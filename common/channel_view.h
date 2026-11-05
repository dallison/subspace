// Copyright 2026 David Allison
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
  // that every subscriber has seen and released.  A slot's retired bit is set
  // after the subscriber retiring it releases it, so a publisher can reuse the
  // slot in between and the bit then stands for a message nobody has seen.
  // Check the refs before taking a slot for its retired bit.
  bool RefsShowRetired(const MessageSlot *slot, uint64_t refs) const {
    if ((refs & (kPubOwned | kRefsMask)) != 0) {
      return false;
    }
    if (slot->ordinal.load(std::memory_order_relaxed) == 0) {
      return true;
    }
    const uint64_t retired_refs =
        (refs >> kRetiredRefsShift) & kRetiredRefsMask;
    return retired_refs >=
           static_cast<uint64_t>(NumSubscribers(RefsVchanId(refs)));
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
    const uint64_t retired_refs =
        (refs >> kRetiredRefsShift) & kRetiredRefsMask;
    if (ref_count != 0 || reliable_ref_count != 0 ||
        retired_refs <
            static_cast<uint64_t>(NumSubscribers(RefsVchanId(refs)))) {
      return false;
    }
    return RetiredSlots().SetWasClear(slot->id);
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
      uint64_t retired_refs = (ref >> kRetiredRefsShift) & kRetiredRefsMask;
      if (retire) {
        retired_refs++;
      }
      const uint64_t new_ref =
          BuildRefsBitField(ref_ordinal, ref_vchan_id,
                            static_cast<int>(retired_refs)) |
          (reliable_refs << kReliableRefCountShift) | refs;
      if (slot->refs.compare_exchange_weak(ref, new_ref,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
        if (retire && refs == 0 && reliable_refs == 0 &&
            static_cast<int64_t>(retired_refs) >=
                NumSubscribers(ref_vchan_id) &&
            RetiredSlots().SetWasClear(slot->id) && retired != nullptr) {
          *retired = true;
        }
        return true;
      }
      // Another subscriber, or a publisher, changed the refs.
    }
  }

protected:
  ChannelControlBlock *ccb_ = nullptr;
  int num_slots_ = 0;
  uint64_t queue_arena_size_ = 0;

private:
  char *Base() const { return reinterpret_cast<char *>(ccb_); }
};

} // namespace subspace
