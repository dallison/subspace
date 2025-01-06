#include "client/subscriber.h"

namespace subspace {
namespace details {

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
      if (s->ordinal != 0 && (refs & kPubOwned) == 0) {
        bits.Set(i);
      }
    }
  } while (num_messages != ccb_->total_messages);
}

int SubscriberImpl::DetectDrops() {
  std::vector<uint64_t> ordinals;
  ordinals.reserve(seen_ordinals_.Size());
  seen_ordinals_.Traverse([this, &ordinals](uint64_t o) {
    if (o >= last_ordinal_seen_) {
      ordinals.push_back(o);
    }
  });
  if (ordinals.empty()) {
    return 0;
  }
  std::stable_sort(ordinals.begin(), ordinals.end());
  last_ordinal_seen_ = ordinals.back();

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

const ActiveSlot *
SubscriberImpl::FindUnseenOrdinal(const std::vector<ActiveSlot> &active_slots) {
  // Traverse the active slots looking for the first ordinal that is not zero
  // and has not been seen by a subscriber.
  for (auto &s : active_slots) {
    if (s.ordinal != 0 && !seen_ordinals_.Contains(s.ordinal)) {
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
  RememberOrdinal(slot->ordinal);
  Prefix(slot, reload)->flags |= kMessageSeen;
}

void SubscriberImpl::CollectVisibleSlots(
    InPlaceAtomicBitset &bits, std::vector<ActiveSlot> &active_slots) {
  uint64_t num_messages = 0;
  do {
    num_messages = ccb_->total_messages;
    active_slots.clear();

    // Traverse the bits and add an active slot for each bit set.
    bits.Traverse([this, &active_slots](int i) {
      MessageSlot *s = &ccb_->slots[i];
      ActiveSlot active_slot = {s, s->ordinal, 0};
      active_slots.push_back(active_slot);
    });
  } while (num_messages != ccb_->total_messages);
}

MessageSlot *SubscriberImpl::NextSlot(MessageSlot *slot, bool reliable,
                                      int owner, std::function<bool()> reload) {
  std::vector<ActiveSlot> active_slots;
  active_slots.reserve(NumSlots());
  InPlaceAtomicBitset &bits = GetAvailableSlots(owner);

  ReloadIfNecessary(reload);

  for (;;) {
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }

    CollectVisibleSlots(bits, active_slots);

    // Sort the active slots by ordinal.
    std::stable_sort(active_slots.begin(), active_slots.end(),
              [](const ActiveSlot &a, const ActiveSlot &b) {
                return a.ordinal < b.ordinal;
              });

    const ActiveSlot *new_slot = FindUnseenOrdinal(active_slots);
    if (new_slot == nullptr) {
      return nullptr;
    }
    // std::cerr << "sub looking at slot " << new_slot->slot->id << " ordinal "
    //           << new_slot->ordinal << "\n";
    // We have a new slot, see if we can increment the ref count.  If we can't
    // we just go back and try again.
    if (AtomicIncRefCount(new_slot->slot, reliable, 1,
                          new_slot->ordinal & kOrdinalMask)) {
      return new_slot->slot;
    }
  }
}

MessageSlot *SubscriberImpl::LastSlot(MessageSlot *slot, bool reliable,
                                      int owner, std::function<bool()> reload) {
  std::vector<ActiveSlot> active_slots;
  active_slots.reserve(NumSlots());
  InPlaceAtomicBitset &bits = GetAvailableSlots(owner);

  ReloadIfNecessary(reload);

  for (;;) {
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }
    CollectVisibleSlots(bits, active_slots);

    // Sort the active slots by ordinal.
    std::stable_sort(active_slots.begin(), active_slots.end(),
              [](const ActiveSlot &a, const ActiveSlot &b) {
                return a.ordinal < b.ordinal;
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
      return new_slot->slot;
    }
  }
}

MessageSlot *SubscriberImpl::FindActiveSlotByTimestamp(
    MessageSlot *old_slot, uint64_t timestamp, bool reliable, int owner,
    std::vector<ActiveSlot> &buffer, std::function<bool()> reload) {
  ReloadIfNecessary(reload);

  for (;;) {
    buffer.clear();
    buffer.reserve(NumSlots());

    // Prepopulate the search buffer.
    for (int i = 0; i < NumSlots(); i++) {
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
      Prefix(it->slot, reload)->flags |= kMessageSeen;
      it->slot->sub_owners.Set(owner);
      return it->slot;
    }
    // Publisher got there first, try again.
  }
}
} // namespace details
} // namespace subspace
