#include "client/publisher.h"
#include "toolbelt/clock.h"

namespace subspace {
namespace details {

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

MessageSlot *
PublisherImpl::FindFreeSlotUnreliable(int owner, std::function<bool()> reload) {
  ReloadIfNecessary(reload);

  int retries = num_slots_ * 1000;
  MessageSlot *slot = nullptr;

  for (;;) {
    // Find the slot with refs == 0 and the lowest ordinal.
    uint64_t lowest_ordinal = -1ULL;
    for (int i = 0; i < num_slots_; i++) {
      MessageSlot *s = &ccb_->slots[i];
      uint32_t refs = s->refs.load(std::memory_order_relaxed);
      if ((refs & kPubOwned) != 0) {
        continue;
      }
      if ((refs & kRefsMask) == 0 &&
          s->ordinal < lowest_ordinal) {
        slot = s;
        lowest_ordinal = s->ordinal;
      }
    }
    if (slot == nullptr) {
      // We are guaranteed to find a slot, but let's not go into an infinite
      // loop if something goes wrong.
      if (retries-- == 0) {
        return nullptr;
      }
      DumpSlots();
      return nullptr;
      // continue;
    }
    // Claim the slot by setting the refs to kPubOwned with our owner in the
    // bottom bits.
    uint32_t ref = kPubOwned | owner;
    uint32_t expected = slot->refs & ~kOrdinalMask;
    if (slot->refs.compare_exchange_weak(expected, ref,
                                         std::memory_order_relaxed)) {
      break;
    }
  }
  slot->ordinal = 0;
  SetSlotToBiggestBuffer(slot);

  Prefix(slot, reload)->flags = 0;

  // We have a slot.  Clear it in all the subscriber bitsets.
  ccb_->subscribers.Traverse(
      [this, slot](int sub_id) { GetAvailableSlots(sub_id).Clear(slot->id); });
  return slot;
}

MessageSlot *PublisherImpl::FindFreeSlotReliable(int owner,
                                                 std::function<bool()> reload) {
  ReloadIfNecessary(reload);
  MessageSlot *slot = nullptr;
  std::vector<ActiveSlot> active_slots;
  active_slots.reserve(NumSlots());
  for (;;) {
    // Put all free slots into the active_slots vector.
    for (int i = 0; i < NumSlots(); i++) {
      MessageSlot *s = &ccb_->slots[i];
      uint32_t refs = s->refs.load(std::memory_order_relaxed);
      if ((refs & kPubOwned) == 0) {
        ActiveSlot active_slot = {s, s->ordinal};
        active_slots.push_back(active_slot);
      }
    }

    // Sort the active slots by ordinal.
    std::sort(active_slots.begin(), active_slots.end(),
              [](const ActiveSlot &a, const ActiveSlot &b) {
                return a.ordinal < b.ordinal;
              });

    // Look for a slot with zero refs but don't go past one with non-zero
    // reliable ref count.
    for (auto &s : active_slots) {
      uint32_t refs = s.slot->refs.load(std::memory_order_relaxed);
      if (((refs >> kReliableRefCountShift) & kRefCountMask) != 0) {
        break;
      }
      // Don't go past one without the kMessageSeen flag set.
      if (s.ordinal != 0 && (Prefix(s.slot, reload)->flags & kMessageSeen) == 0) {
        break;
      }
      // If the refs is zero we can claim it.
      if (refs == 0) {
        slot = s.slot;
        break;
      }
    }
    if (slot == nullptr) {
      return nullptr;
    }

    // Claim the slot by setting the kPubOwned bit.
    uint32_t ref = kPubOwned | owner;
    uint32_t expected = 0;
    if (slot->refs.compare_exchange_weak(expected, ref,
                                         std::memory_order_relaxed)) {
      break;
    }
  }
  slot->ordinal = 0;
  SetSlotToBiggestBuffer(slot);

  Prefix(slot, reload)->flags = 0;

  // We have a slot.  Clear it in all the subscriber bitsets.
  ccb_->subscribers.Traverse(
      [this, slot](int sub_id) { GetAvailableSlots(sub_id).Clear(slot->id); });

  return slot;
}

Channel::PublishedMessage PublisherImpl::ActivateSlotAndGetAnother(
    MessageSlot *slot, bool reliable, bool is_activation, int owner,
    bool omit_prefix, bool *notify, std::function<bool()> reload) {
  if (notify != nullptr) {
    *notify = true; // TODO: remove notify.
  }

  void *buffer = GetBufferAddress(slot);
  MessagePrefix *prefix = reinterpret_cast<MessagePrefix *>(buffer) - 1;

  slot->ordinal = ccb_->next_ordinal++;

  // Copy message parameters into message prefix in buffer.
  if (omit_prefix) {
    slot->ordinal = prefix->ordinal; // Copy ordinal from prefix.
  } else {
    prefix->message_size = slot->message_size;
    prefix->ordinal = slot->ordinal;
    prefix->timestamp = toolbelt::Now();
    prefix->flags = 0;
    if (is_activation) {
      prefix->flags |= kMessageActivate;
    }
  }

  // Update counters.
  ccb_->total_messages++;
  ccb_->total_bytes += slot->message_size;

  // Set the refs to the ordinal with no refs.
  slot->refs.store((slot->ordinal & kOrdinalMask) << kOrdinalShift, std::memory_order_release);

  // Tell all subscribers that the slot is available.
  ccb_->subscribers.Traverse(
      [this, slot](int sub_id) { GetAvailableSlots(sub_id).Set(slot->id); });

  // A reliable publisher doesn't allocate a slot until it is asked for.
  if (reliable) {
    ReloadIfNecessary(reload);
    return {nullptr, 0, 0};
  }
  // Find a new slot.x
  MessageSlot *new_slot = FindFreeSlotUnreliable(owner, reload);

  return {new_slot, prefix->ordinal, prefix->timestamp};
}

} // namespace details
} // namespace subspace
