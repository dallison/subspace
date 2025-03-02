#include "client/subscriber.h"

namespace subspace {
namespace details {

// For non-virtual channels both the slots' vchan_id and the subsriber's
// are -1.  This is the common case.
// For virtual subscribers, if the slot's vchan_id is -1 it means that
// the message was published direcly to the multiplexer and all subscribers
// should see it.

// If the subscriber's vchan_id is -1 it means that the subscriber is on the
// multiplexer and should see all messages, regardless of the vchan_id
static inline bool VirtualChannelIdMatch(MessageSlot *slot, int vchan_id) {
  return vchan_id == -1 || slot->vchan_id == -1 || slot->vchan_id == vchan_id;
}

bool SubscriberImpl::AddActiveMessage(MessageSlot *slot) {
  int old = num_active_messages_.fetch_add(1);
  if (old >= options_.MaxActiveMessages()) {
    num_active_messages_.fetch_sub(1);
    return false;
  }
  return true;
}

void SubscriberImpl::RemoveActiveMessage(MessageSlot *slot) {
  // std::cerr << "remove active message " << slot->id << " " << slot->ordinal
  //           << "\n";
  slot->sub_owners.Clear(subscriber_id_);
  AtomicIncRefCount(slot, IsReliable(), -1, slot->ordinal & kOrdinalMask);

  if (num_active_messages_-- == options_.MaxActiveMessages()) {
    Trigger();
    if (IsReliable()) {
      TriggerReliablePublishers();
    }
  }
}

void SubscriberImpl::PopulateActiveSlots(InPlaceAtomicBitset &bits) {
  uint64_t num_messages = 0;
  do {
    num_messages = ccb_->total_messages;
    bits.ClearAll();

    for (int i = 0; i < NumSlots(); i++) {
      MessageSlot *s = &ccb_->slots[i];
      uint32_t refs = s->refs.load(std::memory_order_relaxed);
      if (VirtualChannelIdMatch(s, vchan_id_) && s->ordinal != 0 &&
          (refs & kPubOwned) == 0) {
        bits.Set(i);
      }
    }
  } while (num_messages != ccb_->total_messages);
}

SubscriberImpl::OrdinalTracker &SubscriberImpl::GetOrdinalTracker(int vchan_id) {
  auto it = ordinal_trackers_.find(vchan_id);
  if (it != ordinal_trackers_.end()) {
    return *it->second;
  }
  auto[it2, _] = ordinal_trackers_.emplace(
      vchan_id, std::make_unique<OrdinalTracker>());

  return *it2->second;
}

int SubscriberImpl::DetectDrops(int vchan_id) {
  std::vector<uint64_t> ordinals;
  auto &tracker = GetOrdinalTracker(vchan_id);
  ordinals.reserve(tracker.ordinals.Size());
  tracker.ordinals.Traverse([&tracker, &ordinals](uint64_t o) {
    if (o >= tracker.last_ordinal_seen) {
      ordinals.push_back(o);
    }
  });
  if (ordinals.empty()) {
    return 0;
  }
  std::stable_sort(ordinals.begin(), ordinals.end());
  tracker.last_ordinal_seen = ordinals.back();

  // Look for gaps in the ordinals.
  int drops = 0;
  for (size_t i = 1; i < ordinals.size(); i++) {
    if (ordinals[i] - ordinals[i - 1] == 1) {
      continue;
    }
    drops += static_cast<int>(ordinals[i] - ordinals[i - 1] - 1);
  }
  return drops;
}

void SubscriberImpl::RememberOrdinal(uint64_t ordinal, int vchan_id) {
  auto &tracker = GetOrdinalTracker(vchan_id);
  tracker.ordinals.Insert(ordinal);
}

const ActiveSlot *
SubscriberImpl::FindUnseenOrdinal(const std::vector<ActiveSlot> &active_slots,
                                  int vchan_id) {
  // Traverse the active slots looking for the first ordinal that is not zero
  // and has not been seen by a subscriber.
  auto &tracker = GetOrdinalTracker(vchan_id);
  for (auto &s : active_slots) {
    if (s.ordinal != 0 && !tracker.ordinals.Contains(s.ordinal)) {
      return &s;
    }
  }
  return nullptr;
}

void SubscriberImpl::ClaimSlot(MessageSlot *slot,
                               std::function<bool()> reload) {
  slot->sub_owners.Set(subscriber_id_);
  // Clear the bit in the subscriber bitset.
  GetAvailableSlots(subscriber_id_).Clear(slot->id);
  RememberOrdinal(slot->ordinal, slot->vchan_id);
  Prefix(slot, reload)->flags |= kMessageSeen;
}

void SubscriberImpl::CollectVisibleSlots(InPlaceAtomicBitset &bits,
                                         std::vector<ActiveSlot> &active_slots,
                                         const DynamicBitSet &embargoed_slots) {
  uint64_t num_messages = 0;
  do {
    num_messages = ccb_->total_messages;
    active_slots.clear();

    // Traverse the bits and add an active slot for each bit set.
    bits.Traverse([this, &active_slots, &embargoed_slots](int i) {
      if (embargoed_slots.IsSet(i)) {
        return;
      }
      MessageSlot *s = &ccb_->slots[i];
      if (!VirtualChannelIdMatch(s, vchan_id_)) {
        return;
      }
      // std::cerr << "collected slot " << s->id << " ordinal " << s->ordinal
      //           << " vchan " << s->vchan_id << "\n";
      ActiveSlot active_slot = {s, s->ordinal, s->timestamp};
      active_slots.push_back(active_slot);
    });
  } while (num_messages != ccb_->total_messages);
}

MessageSlot *SubscriberImpl::NextSlot(MessageSlot *slot, bool reliable,
                                      int owner, std::function<bool()> reload) {
  std::vector<ActiveSlot> active_slots;
  active_slots.reserve(NumSlots());
  InPlaceAtomicBitset &bits = GetAvailableSlots(owner);

  DynamicBitSet embargoed_slots(NumSlots());

  for (;;) {
    ReloadIfNecessary(reload);
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }

    CollectVisibleSlots(bits, active_slots, embargoed_slots);

    // Sort the active slots by timestamp.
    std::stable_sort(active_slots.begin(), active_slots.end(),
                     [](const ActiveSlot &a, const ActiveSlot &b) {
                       return a.timestamp < b.timestamp;
                     });

    const ActiveSlot *new_slot =
        FindUnseenOrdinal(active_slots, vchan_id_);
    if (new_slot == nullptr) {
      return nullptr;
    }
    // std::cerr << "sub looking at slot " << new_slot->slot->id << " ordinal "
    //           << new_slot->ordinal << "\n";
    // We have a new slot, see if we can increment the ref count.  If we can't
    // we just go back and try again.
    if (AtomicIncRefCount(new_slot->slot, reliable, 1,
                          new_slot->ordinal & kOrdinalMask)) {
      if (!ValidateSlotBuffer(new_slot->slot, reload) ||
          new_slot->slot->buffer_index == -1) {
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots.Set(new_slot->slot->id);
        AtomicIncRefCount(new_slot->slot, reliable, -1,
                          new_slot->ordinal & kOrdinalMask);
        continue;
      }
      // std::cerr << "sub got slot " << new_slot->slot->id << " ordinal "
      //           << new_slot->ordinal << " vchan " << new_slot->slot->vchan_id
      //           << "\n";
      return new_slot->slot;
    }
  }
}

MessageSlot *SubscriberImpl::LastSlot(MessageSlot *slot, bool reliable,
                                      int owner, std::function<bool()> reload) {
  std::vector<ActiveSlot> active_slots;
  active_slots.reserve(NumSlots());
  InPlaceAtomicBitset &bits = GetAvailableSlots(owner);

  DynamicBitSet embargoed_slots(NumSlots());

  for (;;) {
    ReloadIfNecessary(reload);
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }
    CollectVisibleSlots(bits, active_slots, embargoed_slots);

    // Sort the active slots by timestamp.
    std::stable_sort(active_slots.begin(), active_slots.end(),
                     [](const ActiveSlot &a, const ActiveSlot &b) {
                       return a.timestamp < b.timestamp;
                     });

    ActiveSlot *new_slot = nullptr;
    if (!active_slots.empty()) {
      new_slot = &active_slots.back();

      if (slot != nullptr && slot == new_slot->slot) {
        // Same slot, nothing changes.
        new_slot = nullptr;
      }
    }
    if (new_slot == nullptr) {
      return nullptr;
    }

    // Increment the ref count.
    if (AtomicIncRefCount(new_slot->slot, reliable, 1,
                          new_slot->ordinal & kOrdinalMask)) {
      if (!ValidateSlotBuffer(new_slot->slot, reload) ||
          new_slot->slot->buffer_index == -1) {
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots.Set(new_slot->slot->id);
        AtomicIncRefCount(new_slot->slot, reliable, -1,
                          new_slot->ordinal & kOrdinalMask);
        continue;
      }
      return new_slot->slot;
    }
  }
}

MessageSlot *SubscriberImpl::FindActiveSlotByTimestamp(
    MessageSlot *old_slot, uint64_t timestamp, bool reliable, int owner,
    std::vector<ActiveSlot> &buffer, std::function<bool()> reload) {
  DynamicBitSet embargoed_slots(NumSlots());

  for (;;) {
    ReloadIfNecessary(reload);
    buffer.clear();
    buffer.reserve(NumSlots());

    // Prepopulate the search buffer.
    for (int i = 0; i < NumSlots(); i++) {
      if (embargoed_slots.IsSet(i)) {
        continue;
      }
      MessageSlot *s = &ccb_->slots[i];
      uint32_t refs = s->refs.load(std::memory_order_relaxed);
      if (s->ordinal != 0 && (refs & kPubOwned) == 0) {
        buffer.push_back({s, 0, Prefix(s, reload)->timestamp});
      }
    }
    // Sort by timestamp.
    std::stable_sort(buffer.begin(), buffer.end(),
                     [](const ActiveSlot &a, const ActiveSlot &b) {
                       return a.timestamp < b.timestamp;
                     });

    // Apparently, lower_bound will return the first in the range if
    // the value is less than the whole range.  That's unexpected.
    if (buffer.empty() || timestamp < buffer.front().timestamp) {
      return nullptr;
    }

    // Binary search the search buffer.
    auto it = std::lower_bound(
        buffer.begin(), buffer.end(), timestamp,
        [](ActiveSlot &s, uint64_t t) { return s.timestamp < t; });
    if (it == buffer.end()) {
      // Not found, nothing changes.
      return nullptr;
    }

    // Try to increment the ref count.
    if (AtomicIncRefCount(it->slot, reliable, 1, it->ordinal & kOrdinalMask)) {
      if (!ValidateSlotBuffer(it->slot, reload) ||
          it->slot->buffer_index == -1) {
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots.Set(it->slot->id);
        AtomicIncRefCount(it->slot, reliable, -1, it->ordinal & kOrdinalMask);
        continue;
      }
      Prefix(it->slot, reload)->flags |= kMessageSeen;
      it->slot->sub_owners.Set(owner);
      return it->slot;
    }
    // Publisher got there first, try again.
  }
}
} // namespace details
} // namespace subspace
