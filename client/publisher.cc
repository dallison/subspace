// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/publisher.h"
#include "client/checksum.h"
#include "client_channel.h"
#include "toolbelt/clock.h"
#include <atomic>
namespace subspace {
namespace details {

absl::Status PublisherImpl::CreateOrAttachBuffers(uint64_t final_slot_size) {
  if (final_slot_size == 0) {
    // If we are being asked for a slot size of 0, we will just use 64 bytes.
    // This is the minimum slot size we can use.
    final_slot_size = 64;
  }
  size_t final_buffer_size = size_t(SlotSizeToBufferSize(final_slot_size));
  uint64_t current_slot_size = 0;
  int num_buffers = ccb_->num_buffers.load(std::memory_order_relaxed);

  for (;;) {
    while (current_slot_size < final_slot_size ||
           buffers_.size() < size_t(num_buffers)) {
      size_t buffer_index = buffers_.size();
      auto shm_fd = CreateBuffer(buffer_index, final_buffer_size);
      if (!shm_fd.ok()) {
        return shm_fd.status();
      }
      if (!shm_fd->Valid()) {
        // This means that the file in /dev/shm already exists so we need to
        // attach to it.
        shm_fd = OpenBuffer(buffer_index);
        if (!shm_fd.ok()) {
          return shm_fd.status();
        }
        auto size = GetBufferSize(*shm_fd, buffer_index);
        if (!size.ok()) {
          return size.status();
        }
        absl::StatusOr<char *> addr;
        current_slot_size = BufferSizeToSlotSize(*size);
        if (current_slot_size > 0) {
          addr = MapBuffer(*shm_fd, *size, BufferMapMode::kReadWrite);
          if (!addr.ok()) {
            return addr.status();
          }
        } else {
          addr = nullptr;
        }
        buffers_.emplace_back(
            std::make_unique<BufferSet>(*size, current_slot_size, *addr));
        bcb_->sizes[buffers_.size()].store(final_buffer_size,
                                           std::memory_order_relaxed);
      } else {
        // We successfully created the /dev/shm file.
        bcb_->sizes[buffers_.size()].store(final_buffer_size,
                                           std::memory_order_relaxed);
        auto addr =
            MapBuffer(*shm_fd, final_buffer_size, BufferMapMode::kReadWrite);
        if (!addr.ok()) {
          return addr.status();
        }
        buffers_.emplace_back(std::make_unique<BufferSet>(
            final_buffer_size, final_slot_size, *addr));
        current_slot_size = final_slot_size;
      }
    }
    int new_num_buffers = int(buffers_.size());
    // Update the atomic numBuffers in the CCB.  If this fails it means
    // something else got there before us and we just go back and remap any new
    // buffers.
    if (ccb_->num_buffers.compare_exchange_strong(num_buffers, new_num_buffers,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed)) {
      // We successfully updated the number of buffers in the CCB.
      break;
    }
    // Another thread has updated the number of buffers in the CCB.  We need to
    // retry.
  }
  return absl::OkStatus();
}

void PublisherImpl::SetSlotToBiggestBuffer(MessageSlot *slot) {
  if (slot == nullptr) {
    return;
  }
  if (slot->buffer_index != -1) {
    // If the slot has a buffer (it's not in the free list), decrement the
    // refs for the buffer.
    DecrementBufferRefs(slot->buffer_index);
  }
  slot->buffer_index = buffers_.size() - 1; // Use biggest buffer.
  IncrementBufferRefs(slot->buffer_index);
}

MessageSlot *PublisherImpl::FindFreeSlotUnreliable(int owner) {
  int retries = num_slots_ * 1000;
  MessageSlot *slot = nullptr;
  embargoed_slots_.ClearAll();
  constexpr int max_cas_retries = 1000;
  int cas_retries = 0;
  int retired_slot = -1;
  int free_slot = -1;
  // See PublisherOptions::SetPreferRetiredSlots() for the rationale.
  // When true (the default) we prefer recycling a retired slot over
  // pulling a fresh slot from the never-used pool.  Both are equally
  // valid for an unreliable publisher (a retired slot has been seen
  // by every current subscriber and dropped), but a retired slot's
  // pages are already cache-hot from the recent publish/consume
  // cycle; a slot out of FreeSlots will demand-fault the kernel into
  // allocating new physical pages on first write, which dominates
  // wall-time for large payloads.  In steady state the publisher
  // cycles through a tiny working set of retired slots without ever
  // consuming from FreeSlots, while still being able to burst into
  // FreeSlots if the subscriber falls behind.
  const bool retired_first = options_.PreferRetiredSlots();
  for (;;) {
    CheckReload();
    bool tried_first = false;
    if (retired_first) {
      retired_slot = RetiredSlots().FindFirstSet();
      tried_first = (retired_slot != -1);
    } else if (!ccb_->free_slots_exhausted.load(std::memory_order_relaxed)) {
      free_slot = FreeSlots().FindFirstSet();
      tried_first = (free_slot != -1);
    }

    if (tried_first && retired_first) {
      // Claim the retired slot.
      if (embargoed_slots_.IsSet(retired_slot)) {
        continue;
      }
      if (!RetiredSlots().ClearWasSet(retired_slot)) {
        retired_slot = -1;
        continue;
      }
      slot = &ccb_->slots[retired_slot];
    } else if (tried_first && !retired_first) {
      // Claim the free slot.
      if (!FreeSlots().ClearWasSet(free_slot)) {
        free_slot = -1;
        continue;
      }
      slot = &ccb_->slots[free_slot];
      if (FreeSlots().IsEmpty()) {
        ccb_->free_slots_exhausted.store(true, std::memory_order_relaxed);
      }
    } else if (retired_first &&
               !ccb_->free_slots_exhausted.load(std::memory_order_relaxed) &&
               (free_slot = FreeSlots().FindFirstSet()) != -1) {
      // Retired pool empty, fall back to FreeSlots.
      if (!FreeSlots().ClearWasSet(free_slot)) {
        free_slot = -1;
        continue;
      }
      slot = &ccb_->slots[free_slot];
      if (FreeSlots().IsEmpty()) {
        ccb_->free_slots_exhausted.store(true, std::memory_order_relaxed);
      }
    } else if (!retired_first &&
               (retired_slot = RetiredSlots().FindFirstSet()) != -1) {
      // FreeSlots exhausted (legacy order), fall back to RetiredSlots.
      if (embargoed_slots_.IsSet(retired_slot)) {
        continue;
      }
      if (!RetiredSlots().ClearWasSet(retired_slot)) {
        retired_slot = -1;
        continue;
      }
      slot = &ccb_->slots[retired_slot];
    } else {
      // Find the slot with refs == 0 and the oldest message.
      uint64_t earliest_timestamp = -1ULL;
      for (int i = 0; i < num_slots_; i++) {
        if (embargoed_slots_.IsSet(i)) {
          continue;
        }
        MessageSlot *s = &ccb_->slots[i];
        uint64_t refs = s->refs.load(std::memory_order_relaxed);
        if ((refs & kPubOwned) != 0) {
          continue;
        }
        if ((refs & kRefsMask) == 0 && s->timestamp < earliest_timestamp) {
          slot = s;
          earliest_timestamp = s->timestamp;
        }
      }
    }

    if (slot == nullptr) {
      // We are guaranteed to find a slot, but let's not go into an infinite
      // loop if something goes wrong.
      if (retries-- == 0) {
        return nullptr;
      }
      continue;
    }
    // Claim the slot by setting the kPubOwned bit and our owner.
    uint64_t old_refs = slot->refs.load(std::memory_order_relaxed);
    uint64_t ref = kPubOwned | owner;
    uint64_t expected = BuildRefsBitField(
        slot->ordinal, (old_refs >> kVchanIdShift) & kVchanIdMask,
        (old_refs >> kRetiredRefsShift) & kRetiredRefsMask);
    if (slot->refs.compare_exchange_weak(expected, ref,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed)) {
      if (!ValidateSlotBuffer(slot)) {
        // No buffer for the slot.  Embargo the slot so we don't see it again
        // this loop and try again.
        embargoed_slots_.Set(slot->id);
        slot->refs.store(old_refs, std::memory_order_relaxed);
        continue;
      }
      break;
    }
    if (++cas_retries >= max_cas_retries) {
      // Rather than spinning forever, let's just give up and return nullptr.
      return nullptr;
    }
  }
  slot->ordinal = 0;
  slot->timestamp = 0;
  slot->vchan_id = vchan_id_;
  SetSlotToBiggestBuffer(slot);

  MessagePrefix *p = Prefix(slot);
  p->flags = 0;
  p->vchan_id = vchan_id_;

  // We have a slot.  Clear it in all the subscriber bitsets.
  ccb_->subscribers.Traverse([this, slot](int sub_id) {
    int vid = GetSubVchanId(sub_id);
    if (vid != -1 && slot->vchan_id != -1 && vid != slot->vchan_id) {
      return;
    }

    GetAvailableSlots(sub_id).Clear(slot->id);
  });

  // If we took a slot that wasn't retired we must trigger the retirement fd.
  // This happens when we recycle a slot that has not yet been seen by all
  // subscribers.
  if (free_slot == -1 && retired_slot == -1) {
    TriggerRetirement(slot->id);
  }
  return slot;
}

MessageSlot *PublisherImpl::FindFreeSlotReliable(int owner) {
  MessageSlot *slot = nullptr;
  int retired_slot = -1;
  int free_slot = -1;
  embargoed_slots_.ClearAll();
  for (;;) {
    CheckReload();

    // Put all free slots into the active_slots vector.
    active_slots_.clear();
    if (!ccb_->free_slots_exhausted.load(std::memory_order_relaxed) &&
        (free_slot = FreeSlots().FindFirstSet()) != -1) {
      // FindFirstSet uses relaxed loads; only the publisher whose
      // ClearWasSet returns true actually owns this bit. See
      // FindFreeSlotUnreliable for details.
      if (!FreeSlots().ClearWasSet(free_slot)) {
        free_slot = -1;
        continue;
      }
      if (FreeSlots().IsEmpty()) {
        ccb_->free_slots_exhausted.store(true, std::memory_order_relaxed);
      }
      MessageSlot *s = &ccb_->slots[free_slot];

      ActiveSlot active_slot = {s, s->ordinal, s->timestamp};
      active_slots_.push_back(active_slot);
    } else if (!ForTunnel() && (retired_slot = RetiredSlots().FindFirstSet()) != -1) {
      if (embargoed_slots_.IsSet(retired_slot)) {
        continue;
      }
      if (!RetiredSlots().ClearWasSet(retired_slot)) {
        retired_slot = -1;
        continue;
      }
      MessageSlot *s = &ccb_->slots[retired_slot];

      ActiveSlot active_slot = {s, s->ordinal, s->timestamp};
      active_slots_.push_back(active_slot);
    } else {
      for (int i = 0; i < NumSlots(); i++) {
        if (embargoed_slots_.IsSet(i)) {
          continue;
        }
        MessageSlot *s = &ccb_->slots[i];
        uint64_t refs = s->refs.load(std::memory_order_relaxed);
        if ((refs & kPubOwned) == 0) {
          ActiveSlot active_slot = {s, s->ordinal, s->timestamp};
          active_slots_.push_back(active_slot);
        }
      }
    }

    // Sort the active slots by timestamp.
    // std::stable_sort gives consistently better performance than std::sort and
    // also is more deterministic in slot ordering.
    std::stable_sort(active_slots_.begin(), active_slots_.end(),
                     [](const ActiveSlot &a, const ActiveSlot &b) {
                       return a.timestamp < b.timestamp;
                     });

    // Look for a slot with zero refs but don't go past one with non-zero
    // reliable ref count.
    for (auto &s : active_slots_) {
      uint64_t refs = s.slot->refs.load(std::memory_order_relaxed);
      if (((refs >> kReliableRefCountShift) & kRefCountMask) != 0) {
        break;
      }
      // Don't go past one without the kMessageSeen flag set.
      if (s.ordinal != 0 && (s.slot->flags & kMessageSeen) == 0) {
        break;
      }
      // If the refs have no references we can claim it.
      if ((refs & kRefsMask) == 0) {
        slot = s.slot;
        break;
      }
    }
    if (slot == nullptr) {
      return nullptr;
    }
    // Claim the slot by setting the kPubOwned bit.
    uint64_t old_refs = slot->refs.load(std::memory_order_relaxed);
    uint64_t ref = kPubOwned | owner;
    uint64_t expected = BuildRefsBitField(
        slot->ordinal, (old_refs >> kVchanIdShift) & kVchanIdMask,
        (old_refs >> kRetiredRefsShift) & kRetiredRefsMask);
    if (slot->refs.compare_exchange_weak(expected, ref,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed)) {
      if (!ValidateSlotBuffer(slot)) {
        // No buffer for the slot.  Embargo the slot so we don't see it again
        // this loop and try again.
        embargoed_slots_.Set(slot->id);
        slot->refs.store(old_refs, std::memory_order_relaxed);
        continue;
      }
      RetiredSlots().Clear(slot->id);
      break;
    }
  }
  slot->ordinal = 0;
  slot->timestamp = 0;
  slot->vchan_id = vchan_id_;
  SetSlotToBiggestBuffer(slot);

  MessagePrefix *p = Prefix(slot);
  p->flags = 0;
  p->vchan_id = vchan_id_;

  // We have a slot.  Clear it in all the subscriber bitsets.
  ccb_->subscribers.Traverse([this, slot](int sub_id) {
    int vid = GetSubVchanId(sub_id);
    if (vid != -1 && slot->vchan_id != -1 && vid != slot->vchan_id) {
      return;
    }
    GetAvailableSlots(sub_id).Clear(slot->id);
  });

  // If we took a slot that wasn't retired we must trigger the retirement fd.
  // This happens when we recycle a slot that has not yet been seen by all
  // subscribers.
  if (free_slot == -1 && retired_slot == -1) {
    TriggerRetirement(slot->id);
  }
  return slot;
}

Channel::PublishedMessage PublisherImpl::ActivateSlotAndGetAnother(
    MessageSlot *slot, bool reliable, bool is_activation, int owner,
    bool omit_prefix, bool use_prefix_slot_id, bool for_tunnel) {
  void *buffer = GetBufferAddress(slot);
  MessagePrefix *prefix = reinterpret_cast<MessagePrefix *>(
      static_cast<char *>(buffer) - PrefixSize());

  slot->ordinal = ccb_->ordinals.Next(slot->vchan_id);
  slot->timestamp = toolbelt::Now();
  slot->flags = 0;

  // Copy message parameters into message prefix in buffer.
  if (omit_prefix) {
    if (for_tunnel) {
      prefix->SetIsCrossMachine();
    }
    slot->timestamp = prefix->timestamp;
    slot->vchan_id = prefix->vchan_id;
    // The bridged_slot_id is the slot is used for the retirement notification.
    slot->bridged_slot_id = use_prefix_slot_id ? prefix->slot_id : slot->id;
  } else {
    prefix->message_size = slot->message_size;
    prefix->ordinal = slot->ordinal;
    prefix->timestamp = slot->timestamp;
    prefix->vchan_id = slot->vchan_id;
    prefix->checksum_size = static_cast<uint16_t>(ChecksumSize());
    prefix->metadata_size = static_cast<uint16_t>(MetadataSize());
    prefix->flags = 0;
    prefix->slot_id = slot->id;
    slot->bridged_slot_id = slot->id;
    if (is_activation) {
      prefix->SetIsActivation();
      slot->flags |= kMessageIsActivation;
      ccb_->activation_tracker.Activate(slot->vchan_id);
    }
    if (for_tunnel) {
      prefix->SetIsCrossMachine();
    }
    if (options_.Checksum()) {
      prefix->SetHasChecksum();
      auto data = GetMessageChecksumData(prefix, buffer, slot->message_size,
                                         ChecksumSize(), MetadataSize());
      absl::Span<std::byte> cksum = GetChecksumSpan(prefix, ChecksumSize());
      if (checksum_callback_ != nullptr) {
        checksum_callback_(data, cksum);
      } else {
        CalculateCRC32Checksum(data, cksum);
      }
    }
  }

  // Set the refs to the ordinal with no refs.
  slot->refs.store(BuildRefsBitField(slot->ordinal, vchan_id_, 0),
                   std::memory_order_release);

  // Tell all subscribers that the slot is available, BEFORE bumping
  // total_messages.  SubscriberImpl::NextSlot() uses total_messages as
  // a version stamp for its cached active_slots_ snapshot: a subscriber
  // that observes a bumped total_messages must also observe every
  // preceding bits.Set() so its CollectVisibleSlots() snapshot can't
  // miss the just-published slot.  bits.Set() is relaxed, but the
  // following total_messages++ is seq_cst, so the relaxed bit writes
  // are sequenced-before the seq_cst increment and therefore
  // happens-before any subscriber's seq_cst load of total_messages
  // that observes the new value.  If we incremented total_messages
  // first, a subscriber could read the new total, run
  // CollectVisibleSlots() before the bit was visible, cache that
  // snapshot under next_slot_cached_total_, and then reuse the stale
  // cache forever (no further total bump arrives to invalidate it).
  ccb_->subscribers.Traverse([this, slot](int sub_id) {
    if (vchan_id_ != -1 && GetSubVchanId(sub_id) != -1 &&
        vchan_id_ != GetSubVchanId(sub_id)) {
      return;
    }
    GetAvailableSlots(sub_id).Set(slot->id);
  });

  // Update counters AFTER setting the available-slot bits (see above).
  if (!is_activation) {
    ccb_->total_bytes += slot->message_size;
    if (slot->message_size > ccb_->max_message_size) {
      ccb_->max_message_size = slot->message_size;
    }
    ccb_->total_messages++;
  }

  // A reliable publisher doesn't allocate a slot until it is asked for.
  if (reliable) {
    CheckReload();
    return {nullptr, 0, 0};
  }
  // Find a new slot.x
  MessageSlot *new_slot = FindFreeSlotUnreliable(owner);

  return {new_slot, prefix->ordinal, prefix->timestamp};
}

} // namespace details
} // namespace subspace
