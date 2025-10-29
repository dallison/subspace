// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/publisher.h"
#include "client_channel.h"
#include "toolbelt/clock.h"

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
  DynamicBitSet embargoed_slots(NumSlots());
  constexpr int max_cas_retries = 1000;
  int cas_retries = 0;
  int retired_slot = -1;
  for (;;) {
    CheckReload();
    // Look at the first retired slot.  If there are no retired
    // slots, look at all slots for the earliest unreferenced one.
    retired_slot = RetiredSlots().FindFirstSet();
    if (retired_slot != -1) {
      // We have a retired slot.
      if (embargoed_slots.IsSet(retired_slot)) {
        continue;
      }
      RetiredSlots().Clear(retired_slot);
      slot = &ccb_->slots[retired_slot];
    } else {
      // Find the slot with refs == 0 and the oldest message.
      uint64_t earliest_timestamp = -1ULL;
      for (int i = 0; i < num_slots_; i++) {
        if (embargoed_slots.IsSet(i)) {
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
        DumpSlots(std::cout);
        return nullptr;
      }
      continue;
    }
    // Claim the slot by setting the refs to kPubOwned with our owner in the
    // bottom bits.
    uint64_t old_refs = slot->refs.load(std::memory_order_relaxed);
    uint64_t ref = kPubOwned | owner;
    uint64_t expected = BuildRefsBitField(
        slot->ordinal, (old_refs >> kVchanIdShift) & kVchanIdMask,
        (old_refs >> kRetiredRefsShift) & kRetiredRefsMask);
    if (slot->refs.compare_exchange_weak(expected, ref,
                                         std::memory_order_relaxed)) {
      if (!ValidateSlotBuffer(slot)) {
        // No buffer for the slot.  Embargo the slot so we don't see it again
        // this loop and try again.
        embargoed_slots.Set(slot->id);
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
  ccb_->subscribers.Traverse(
      [this, slot](int sub_id) { GetAvailableSlots(sub_id).Clear(slot->id); });

  // If we took a slot that wasn't retired we must trigger the retirement fd.
  // This happens when we recycle a slot that has not yet been seen by all
  // subscribers.
  if (retired_slot == -1) {
    TriggerRetirement(slot->id);
  }
  return slot;
}

MessageSlot *PublisherImpl::FindFreeSlotReliable(int owner) {
  MessageSlot *slot = nullptr;
  std::vector<ActiveSlot> active_slots;
  active_slots.reserve(NumSlots());
  DynamicBitSet embargoed_slots(NumSlots());
  int retired_slot = -1;
  for (;;) {
    CheckReload();

    // Put all free slots into the active_slots vector.
    active_slots.clear();
    retired_slot = RetiredSlots().FindFirstSet();
    if (retired_slot != -1) { // We have a retired slot.
      if (embargoed_slots.IsSet(retired_slot)) {
        continue;
      }
      RetiredSlots().Clear(retired_slot);
      MessageSlot *s = &ccb_->slots[retired_slot];

      ActiveSlot active_slot = {s, s->ordinal, s->timestamp};
      active_slots.push_back(active_slot);
    } else {
      for (int i = 0; i < NumSlots(); i++) {
        if (embargoed_slots.IsSet(i)) {
          continue;
        }
        MessageSlot *s = &ccb_->slots[i];
        uint64_t refs = s->refs.load(std::memory_order_relaxed);
        if ((refs & kPubOwned) == 0) {
          ActiveSlot active_slot = {s, s->ordinal, s->timestamp};
          active_slots.push_back(active_slot);
        }
      }
    }
    // Sort the active slots by timestamp.
    // std::stable_sort gives consistently better performance than std::sort and
    // also is more deterministic in slot ordering.
    std::stable_sort(active_slots.begin(), active_slots.end(),
                     [](const ActiveSlot &a, const ActiveSlot &b) {
                       return a.timestamp < b.timestamp;
                     });

    // Look for a slot with zero refs but don't go past one with non-zero
    // reliable ref count.
    for (auto &s : active_slots) {
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
                                         std::memory_order_relaxed)) {
      if (!ValidateSlotBuffer(slot)) {
        // No buffer for the slot.  Embargo the slot so we don't see it again
        // this loop and try again.
        embargoed_slots.Set(slot->id);
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
  ccb_->subscribers.Traverse(
      [this, slot](int sub_id) { GetAvailableSlots(sub_id).Clear(slot->id); });

  // If we took a slot that wasn't retired we must trigger the retirement fd.
  // This happens when we recycle a slot that has not yet been seen by all
  // subscribers.
  if (retired_slot == -1) {
    TriggerRetirement(slot->id);
  }
  return slot;
}

Channel::PublishedMessage PublisherImpl::ActivateSlotAndGetAnother(
    MessageSlot *slot, bool reliable, bool is_activation, int owner,
    bool omit_prefix, bool use_prefix_slot_id) {
  void *buffer = GetBufferAddress(slot);
  MessagePrefix *prefix = reinterpret_cast<MessagePrefix *>(buffer) - 1;

  slot->ordinal = ccb_->ordinals.Next(slot->vchan_id);
  slot->timestamp = toolbelt::Now();
  slot->flags = 0;

  // Copy message parameters into message prefix in buffer.
  if (omit_prefix) {
    slot->timestamp = prefix->timestamp;
    slot->vchan_id = prefix->vchan_id;
    // The bridged_slot_id is the slot is used for the retirement notification.
    slot->bridged_slot_id = use_prefix_slot_id ? prefix->slot_id : slot->id;
  } else {
    prefix->message_size = slot->message_size;
    prefix->ordinal = slot->ordinal;
    prefix->timestamp = slot->timestamp;
    prefix->vchan_id = slot->vchan_id;
    prefix->flags = 0;
    prefix->slot_id = slot->id;
    slot->bridged_slot_id = slot->id;
    if (is_activation) {
      prefix->flags |= kMessageActivate;
      slot->flags |= kMessageIsActivation;
      ccb_->activation_tracker.Activate(slot->vchan_id);
    }
  }

  // Update counters.
  if (!is_activation) {
    ccb_->total_messages++;
    ccb_->total_bytes += slot->message_size;

    if (slot->message_size > ccb_->max_message_size) {
      ccb_->max_message_size = slot->message_size;
    }
  }

  // Set the refs to the ordinal with no refs.
  slot->refs.store(BuildRefsBitField(slot->ordinal, vchan_id_, 0),
                   std::memory_order_release);

  // Tell all subscribers that the slot is available.
  ccb_->subscribers.Traverse([this, slot](int sub_id) {
    if (vchan_id_ != -1 && ccb_->sub_vchan_ids[sub_id] != -1 &&
        vchan_id_ != ccb_->sub_vchan_ids[sub_id]) {
      return;
    }
    GetAvailableSlots(sub_id).Set(slot->id);
  });

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
