#include "client/subscriber.h"

namespace subspace {
namespace details {

void SubscriberImpl::PopulateActiveSlots(InPlaceAtomicBitset &bits) {
  for (int i = 0; i < NumSlots(); i++) {
    MessageSlot *s = &ccb_->slots[i];
    uint32_t refs = s->refs.load(std::memory_order_relaxed);
    if (s->ordinal != 0 && (refs & kPubOwned) == 0) {
      bits.Set(i);
    }
  }
}

int SubscriberImpl::DetectDrops() {
  std::vector<uint64_t> ordinals;
  ordinals.reserve(seen_ordinals_.Size());
  seen_ordinals_.Traverse([this, &ordinals](uint64_t o) {
    if (o > last_ordinal_seen_) {
      ordinals.push_back(o);
    }
  });
  if (ordinals.empty()) {
    return 0;
  }
  std::sort(ordinals.begin(), ordinals.end());
  last_ordinal_seen_ = ordinals.back();

  // Look for gaps in the ordinals.
  int drops = 0;
  for (size_t i = 1; i < ordinals.size(); i++) {
    drops += static_cast<int>(ordinals[i] - ordinals[i - 1] - 1);
  }
  return drops;
}

MessageSlot *
SubscriberImpl::FindUnseenOrdinal(const std::vector<ActiveSlot> &active_slots) {
  // Traverse the active slots looking for the first ordinal that is not zero
  // and has not been seen by a subscriber.
  for (auto &s : active_slots) {
    if (s.ordinal != 0 && !seen_ordinals_.Contains(s.ordinal)) {
      return s.slot;
    }
  }
  return nullptr;
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

    // Traverse the bits and add an active slot for each bit set.
    bits.Traverse([this, &active_slots](int i) {
      MessageSlot *s = &ccb_->slots[i];
      ActiveSlot active_slot = {s, s->ordinal};
      active_slots.push_back(active_slot);
    });

    // Sort the active slots by ordinal.
    std::sort(active_slots.begin(), active_slots.end(),
              [](const ActiveSlot &a, const ActiveSlot &b) {
                return a.ordinal < b.ordinal;
              });

    MessageSlot *new_slot = FindUnseenOrdinal(active_slots);
    if (new_slot == nullptr) {
      return nullptr;
    }
    // We have a new slot, see if we can increment the ref count.  If we can't
    // we just go back and try again.
    if (AtomicIncRefCount(new_slot, reliable, 1)) {
      new_slot->sub_owners.Set(owner);
      seen_ordinals_.Insert(new_slot->ordinal);
      Prefix(new_slot)->flags |= kMessageSeen;

      return new_slot;
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
    // Traverse the bits and add an active slot for each bit set.
    bits.Traverse([&](int i) {
      MessageSlot *s = &ccb_->slots[i];
      ActiveSlot active_slot = {s, s->ordinal};
      active_slots.push_back(active_slot);
    });

    // Sort the active slots by ordinal.
    std::sort(active_slots.begin(), active_slots.end(),
              [](const ActiveSlot &a, const ActiveSlot &b) {
                return a.ordinal < b.ordinal;
              });

    MessageSlot *new_slot = nullptr;
    if (!active_slots.empty()) {
      new_slot = active_slots.back().slot;

      if (slot != nullptr && slot == new_slot) {
        // Same slot, nothing changes.
        new_slot = nullptr;
      }
    }
    if (new_slot == nullptr) {
      return nullptr;
    }

    // Increment the ref count.
    if (AtomicIncRefCount(new_slot, reliable, 1)) {
      new_slot->sub_owners.Set(owner);
      seen_ordinals_.Insert(new_slot->ordinal);
      Prefix(new_slot)->flags |= kMessageSeen;
      return new_slot;
    }
  }
}

} // namespace details
} // namespace subspace
