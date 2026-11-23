// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "common/bitset_view.h"
#include "common/shm_layout.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace subspace {

// Whether a subscriber on sub_vchan_id receives a message published on
// msg_vchan_id.  -1 is the multiplexer itself: a subscriber to it receives
// every virtual channel, and a message published to it reaches every
// subscriber.
inline bool VirtualChannelsMatch(int sub_vchan_id, int msg_vchan_id) {
  return sub_vchan_id == -1 || msg_vchan_id == -1 ||
         sub_vchan_id == msg_vchan_id;
}

// The regions of a mapped channel control block, and the slot operations that
// every client shares.  It does not own the mapping.
class ChannelView {
public:
  ChannelView() = default;
  ChannelView(ChannelControlBlock *ccb, int num_slots,
              uint64_t queue_arena_size)
      : ccb_(ccb), num_slots_(num_slots), queue_arena_size_(queue_arena_size) {}

  ChannelControlBlock *Ccb() const { return ccb_; }
  int NumSlots() const { return num_slots_; }

  MessageSlot *Slot(int id) const { return &ccb_->slots[id]; }

  BitsetView RetiredSlots() const {
    return BitsetView(Base() + RetiredSlotsOffset(num_slots_), num_slots_);
  }
  BitsetView FreeSlots() const {
    return BitsetView(Base() + FreeSlotsOffset(num_slots_), num_slots_);
  }
  BitsetView AvailableSlots(int sub_id) const {
    return BitsetView(Base() + AvailableSlotsOffset(num_slots_, sub_id),
                      num_slots_);
  }
  BitsetView Subscribers() const {
    return BitsetView(&ccb_->subscribers, kMaxSlotOwners);
  }

  AvailableSlotQueueIndex *QueueIndex() const {
    return reinterpret_cast<AvailableSlotQueueIndex *>(
        Base() + SlotQueueIndexOffset(num_slots_));
  }

  // The subscriber's queue, or null if it has none or the queue does not fit
  // in the arena.
  InPlaceSlotQueue *Queue(int sub_id) const {
    const uint64_t offset =
        QueueIndex()->offsets[sub_id].load(std::memory_order_acquire);
    if (offset == kInvalidSlotQueueOffset || offset > queue_arena_size_ ||
        queue_arena_size_ - offset < sizeof(InPlaceSlotQueue)) {
      return nullptr;
    }
    auto *queue = reinterpret_cast<InPlaceSlotQueue *>(
        Base() + SlotQueueArenaOffset(num_slots_) + offset);
    const uint64_t room = queue_arena_size_ - offset - sizeof(InPlaceSlotQueue);
    if (queue->Capacity() > room / sizeof(SlotQueueEntry)) {
      return nullptr;
    }
    return queue;
  }

  int NumSubscribers(int vchan_id) const {
    return ccb_->num_subs.NumSubscribers(vchan_id);
  }

  uint64_t CleanupGeneration(int vchan_id) const {
    return ccb_->subscriber_cleanup_generation.Get(vchan_id);
  }

  // Whether `refs`, read from `slot`, show a slot that holds no message or one
  // that every subscriber delivered at publish time has finished with. A
  // slot's retired bit is set after that countdown reaches zero, so a publisher
  // can reuse the slot in between and the bit then stands for a message nobody
  // has seen. Check the refs before taking a slot for its retired bit.
  bool RefsShowRetired(const MessageSlot *slot, uint64_t refs) const {
    if ((refs & (kPubOwned | kRefsMask)) != 0) {
      return false;
    }
    if (slot->ordinal.load(std::memory_order_relaxed) == 0) {
      return true;
    }
    return PendingCount(refs) == 0;
  }

  // Whether retirement listeners are told that slot_id retired.  Activation
  // messages are not reported.  A bridged message names a slot on the other
  // side of the bridge, which this channel may not have.
  bool ReportsRetirement(int slot_id) const {
    return slot_id < 0 || slot_id >= num_slots_ ||
           (Slot(slot_id)->flags.load(std::memory_order_relaxed) &
            kMessageIsActivation) == 0;
  }

  // Sets the slot's retired bit if every subscriber has seen and released its
  // message.  Returns true if this call set it.
  bool TryRetireSlot(MessageSlot *slot) const {
    if (slot->ordinal.load(std::memory_order_relaxed) == 0) {
      return false;
    }
    const uint64_t refs = slot->refs.load(std::memory_order_acquire);
    if ((refs & kPubOwned) != 0) {
      return false;
    }
    const uint64_t ref_count = refs & kRefCountMask;
    const uint64_t reliable_ref_count =
        (refs >> kReliableRefCountShift) & kRefCountMask;
    if (ref_count != 0 || reliable_ref_count != 0 || PendingCount(refs) != 0) {
      return false;
    }
    return RetiredSlots().SetWasClear(slot->id);
  }

  // Drop subscriber `owner`'s obligation to the message in `slot` and, when
  // `drop_ref` is set, one reference. Returns false when the slot is
  // publisher-owned or holds a different message. Does not set the retired
  // bit; the caller uses TryRetireSlot.
  bool ReleaseSubscriberObligation(MessageSlot *slot, int owner, bool reliable,
                                   bool drop_ref, uint64_t ordinal,
                                   int vchan_id) const {
    const uint64_t want_ord = ordinal & kOrdinalMask;
    for (;;) {
      uint64_t ref = slot->refs.load(std::memory_order_acquire);
      if ((ref & kPubOwned) != 0) {
        if (drop_ref) {
          slot->sub_owners.Clear(owner);
        }
        return false;
      }
      const uint64_t ref_ord = (ref >> kOrdinalShift) & kOrdinalMask;
      const int ref_vchan_id = RefsVchanId(ref);
      if (want_ord != 0 && ref_ord != 0 && ref_ord != want_ord) {
        if (drop_ref) {
          slot->sub_owners.Clear(owner);
        }
        return false;
      }
      if (want_ord != 0 && ref_ord != 0 && ref_vchan_id != vchan_id) {
        if (drop_ref) {
          slot->sub_owners.Clear(owner);
        }
        return false;
      }

      // Take the obligation before the CAS so two releasers cannot both
      // decrement. If the slot changes before the CAS lands, put the bit back
      // when it now belongs to a committed newer message.
      const bool owed = slot->pending_subs.ClearWasSet(owner);
      const uint64_t again = slot->refs.load(std::memory_order_acquire);
      if (again != ref) {
        if (owed) {
          // The bit we cleared may belong to the message a publisher is
          // installing. Put it back. ClearAll during that publish wipes a
          // restore that still belongs to the previous message, and a restore
          // of a bit the publisher just set matches the count it recorded.
          slot->pending_subs.Set(owner);
        }
        if (drop_ref &&
            ((again & kPubOwned) != 0 ||
             ((again >> kOrdinalShift) & kOrdinalMask) != want_ord)) {
          slot->sub_owners.Clear(owner);
          return false;
        }
        continue;
      }

      uint64_t new_refs = ref & kRefCountMask;
      uint64_t new_reliable_refs =
          (ref >> kReliableRefCountShift) & kRefCountMask;
      const bool drop_a_ref = drop_ref && new_refs != 0;
      if (drop_a_ref) {
        new_refs -= 1;
        if (reliable && new_reliable_refs > 0) {
          new_reliable_refs -= 1;
        }
      }
      int pending = PendingCount(ref);
      if (owed && pending > 0) {
        pending--;
      }
      if (!owed && !drop_a_ref) {
        if (drop_ref) {
          slot->sub_owners.Clear(owner);
        }
        return true;
      }
      const uint64_t new_ref =
          BuildRefsBitField(ref_ord, ref_vchan_id, pending) |
          (new_reliable_refs << kReliableRefCountShift) | new_refs;
      if (slot->refs.compare_exchange_weak(ref, new_ref,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
        if (drop_ref) {
          slot->sub_owners.Clear(owner);
        }
        return true;
      }
      if (owed) {
        slot->pending_subs.Set(owner);
      }
    }
  }

  // A subscriber removed while this slot was still publisher-owned could not
  // decrement pending. Drop those obligations now that the message is
  // committed, including an ID reused by a subscriber on a virtual channel
  // that was not delivered this message.
  void ReleaseDepartedObligations(MessageSlot *slot) const {
    const uint64_t ordinal = slot->ordinal.load(std::memory_order_relaxed);
    const int vchan_id = slot->vchan_id.load(std::memory_order_relaxed);
    if (ordinal == 0) {
      return;
    }
    // Release during the walk. Traverse shifts past the bit it just reported
    // before loading again, and the release only clears that subscriber's bit.
    slot->pending_subs.Traverse([this, slot, ordinal, vchan_id](size_t sub_id) {
      if (ccb_->subscribers.IsSet(sub_id)) {
        // User IDs are first-fit and shared by every virtual channel on a mux.
        // A subscriber removed while this slot was publisher-owned can be
        // reissued to a virtual channel that was not delivered the message
        // before this commit. That subscriber must not keep the countdown.
        const int sub_vchan = ccb_->sub_vchan_ids[sub_id];
        if (sub_vchan == -1 || vchan_id == -1 || sub_vchan == vchan_id) {
          return;
        }
      }
      ReleaseSubscriberObligation(slot, static_cast<int>(sub_id),
                                  /*reliable=*/false, /*drop_ref=*/false,
                                  ordinal, vchan_id);
    });
  }

  // Adds inc to the slot's reference counts, and to its reliable count if
  // reliable, while the slot holds the message with this ordinal and vchan_id.
  // An ordinal of 0 matches any message.  With retire, also counts a
  // subscriber that has finished with the message, and sets *retired if that
  // retired the slot.  Returns false if the slot no longer holds the message
  // or a publisher owns it.
  bool AtomicIncRefCount(MessageSlot *slot, bool reliable, int inc,
                         uint64_t ordinal, int vchan_id, bool retire,
                         bool *retired) const {
    if (retired != nullptr) {
      *retired = false;
    }
    ordinal &= kOrdinalMask;
    for (;;) {
      uint64_t ref = slot->refs.load(std::memory_order_relaxed);
      if ((ref & kPubOwned) != 0) {
        return false;
      }
      // An ordinal of 0 in refs means the slot was free.
      const uint64_t ref_ordinal = (ref >> kOrdinalShift) & kOrdinalMask;
      const int ref_vchan_id = RefsVchanId(ref);
      if (ref_ordinal != 0 && ordinal != 0 &&
          (ref_ordinal != ordinal || ref_vchan_id != vchan_id)) {
        return false;
      }
      uint64_t refs = ref & kRefCountMask;
      uint64_t reliable_refs = (ref >> kReliableRefCountShift) & kRefCountMask;
      if (inc < 0 && refs == 0) {
        return true;
      }
      refs = static_cast<uint64_t>(static_cast<int64_t>(refs) + inc);
      if (reliable) {
        reliable_refs =
            static_cast<uint64_t>(static_cast<int64_t>(reliable_refs) + inc);
      }
      int pending = PendingCount(ref);
      if (retire && pending > 0) {
        pending--;
      }
      const uint64_t new_ref =
          BuildRefsBitField(ref_ordinal, ref_vchan_id, pending) |
          (reliable_refs << kReliableRefCountShift) | refs;
      if (slot->refs.compare_exchange_weak(ref, new_ref,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
        // A reference dropping to zero retires the slot once every subscriber
        // who was delivered the message has finished with it. `retire` covers
        // a caller that decremented pending without also dropping a reference.
        if ((retire || inc < 0) && refs == 0 && reliable_refs == 0 &&
            pending == 0 && RetiredSlots().SetWasClear(slot->id) &&
            retired != nullptr) {
          *retired = true;
        }
        return true;
      }
      // Another subscriber, or a publisher, changed the refs.
    }
  }

  // Clears the bit in `bits` for a message evicted from that subscriber's
  // queue, if the slot still holds it.  Returns false while the message is
  // still being published, because its slot cannot be pinned until then.  If
  // the slot retires when it is unpinned, calls `retired` with the slot to
  // name to retirement listeners.
  template <typename Retired>
  bool ForgetEvictedSlot(BitsetView bits, const QueuedSlot &evicted,
                         Retired &&retired) const {
    if (evicted.slot_id < 0 || evicted.slot_id >= num_slots_ ||
        evicted.ordinal == 0) {
      return true;
    }
    MessageSlot *slot = Slot(evicted.slot_id);
    const uint64_t refs = slot->refs.load(std::memory_order_acquire);
    if ((refs & kPubOwned) != 0) {
      return slot->ordinal.load(std::memory_order_relaxed) != evicted.ordinal;
    }
    if (((refs >> kOrdinalShift) & kOrdinalMask) !=
        (evicted.ordinal & kOrdinalMask)) {
      return true;
    }
    const int vchan_id = RefsVchanId(refs);
    // While pinned the slot cannot be reused, so its bit cannot yet belong to
    // a newer message.
    if (!AtomicIncRefCount(slot, /*reliable=*/false, 1, evicted.ordinal,
                           vchan_id, /*retire=*/false, nullptr)) {
      return true;
    }
    if (slot->ordinal.load(std::memory_order_relaxed) == evicted.ordinal) {
      bits.Clear(slot->id);
    }
    const int32_t retirement_slot_id =
        slot->bridged_slot_id.load(std::memory_order_relaxed);
    // A subscriber that retires the slot while it is pinned leaves the ref
    // count above zero, so the unpin has to retire it.
    const uint64_t unpinned =
        slot->refs.fetch_sub(1, std::memory_order_acq_rel) - 1;
    if ((unpinned & kRefCountMask) == 0 &&
        ((unpinned >> kReliableRefCountShift) & kRefCountMask) == 0 &&
        PendingCount(unpinned) == 0 &&
        RetiredSlots().SetWasClear(slot->id)) {
      retired(retirement_slot_id);
    }
    return true;
  }

protected:
  ChannelControlBlock *ccb_ = nullptr;
  int num_slots_ = 0;
  uint64_t queue_arena_size_ = 0;

private:
  char *Base() const { return reinterpret_cast<char *>(ccb_); }
};

} // namespace subspace
