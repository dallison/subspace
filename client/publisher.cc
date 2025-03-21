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
  DynamicBitSet embargoed_slots(NumSlots());

  for (;;) {
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
    if (slot == nullptr) {
      // We are guaranteed to find a slot, but let's not go into an infinite
      // loop if something goes wrong.
      if (retries-- == 0) {
        return nullptr;
      }
      DumpSlots();
      return nullptr;
    }
    // Claim the slot by setting the refs to kPubOwned with our owner in the
    // bottom bits.
    uint64_t old_refs = slot->refs.load(std::memory_order_relaxed);
    uint64_t ref = kPubOwned | owner;
    uint64_t expected =
        BuildOrdinalAndVchanIdBitField(slot->ordinal, slot->vchan_id);
    if (slot->refs.compare_exchange_weak(expected, ref,
                                         std::memory_order_relaxed)) {
      if (!ValidateSlotBuffer(slot, reload)) {
        // No buffer for the slot.  Embargo the slot so we don't see it again
        // this loop and try again.
        embargoed_slots.Set(slot->id);
        slot->refs.store(old_refs, std::memory_order_relaxed);
        continue;
      }
      break;
    }
  }
  slot->ordinal = 0;
  slot->timestamp = 0;
  slot->vchan_id = vchan_id_;
  SetSlotToBiggestBuffer(slot);

  MessagePrefix *p = Prefix(slot, reload);
  p->flags = 0;
  p->vchan_id = vchan_id_;

  // We have a slot.  Clear it in all the subscriber bitsets.
  ccb_->subscribers.Traverse([this, slot](int sub_id) {
    if (vchan_id_ != -1 && ccb_->subVchanIds[sub_id] != -1 &&
        vchan_id_ != ccb_->subVchanIds[sub_id]) {
      return;
    }
    GetAvailableSlots(sub_id).Clear(slot->id);
  });
  return slot;
}

MessageSlot *PublisherImpl::FindFreeSlotReliable(int owner,
                                                 std::function<bool()> reload) {
  MessageSlot *slot = nullptr;
  std::vector<ActiveSlot> active_slots;
  active_slots.reserve(NumSlots());
  DynamicBitSet embargoed_slots(NumSlots());
  for (;;) {
    ReloadIfNecessary(reload);

    // Put all free slots into the active_slots vector.
    active_slots.clear();
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
      if (s.ordinal != 0 &&
          (Prefix(s.slot, reload)->flags & kMessageSeen) == 0) {
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
    uint64_t expected =
        BuildOrdinalAndVchanIdBitField(slot->ordinal, vchan_id_);
    if (slot->refs.compare_exchange_weak(expected, ref,
                                         std::memory_order_relaxed)) {
      if (!ValidateSlotBuffer(slot, reload)) {
        // No buffer for the slot.  Embargo the slot so we don't see it again
        // this loop and try again.
        embargoed_slots.Set(slot->id);
        slot->refs.store(old_refs, std::memory_order_relaxed);
        continue;
      }
      break;
    }
  }
  slot->ordinal = 0;
  slot->timestamp = 0;
  slot->vchan_id = vchan_id_;
  SetSlotToBiggestBuffer(slot);

  MessagePrefix *p = Prefix(slot, reload);
  p->flags = 0;
  p->vchan_id = vchan_id_;

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

  slot->ordinal = NextOrdinal(ccb_, slot->vchan_id);
  slot->timestamp = toolbelt::Now();
  // std::cerr << "Published message in slot " << slot->id << " with ordinal "
  //           << slot->ordinal << " vchan " << slot->vchan_id << "\n";

  // Copy message parameters into message prefix in buffer.
  if (omit_prefix) {
    slot->ordinal = prefix->ordinal; // Copy ordinal from prefix.
    slot->timestamp = prefix->timestamp;
    slot->vchan_id = prefix->vchan_id;
  } else {
    prefix->message_size = slot->message_size;
    prefix->ordinal = slot->ordinal;
    prefix->timestamp = slot->timestamp;
    prefix->vchan_id = slot->vchan_id;
    prefix->flags = 0;
    if (is_activation) {
      prefix->flags |= kMessageActivate;
    }
  }

  // Update counters.
  ccb_->total_messages++;
  ccb_->total_bytes += slot->message_size;

  // Set the refs to the ordinal with no refs.
  slot->refs.store(BuildOrdinalAndVchanIdBitField(slot->ordinal, vchan_id_),
                   std::memory_order_release);

  // Tell all subscribers that the slot is available.
  ccb_->subscribers.Traverse([this, slot](int sub_id) {
    if (vchan_id_ != -1 && ccb_->subVchanIds[sub_id] != -1 &&
        vchan_id_ != ccb_->subVchanIds[sub_id]) {
      return;
    }
    GetAvailableSlots(sub_id).Set(slot->id);
  });

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
