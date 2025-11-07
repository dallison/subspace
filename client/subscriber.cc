// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

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
  // std::cerr << "adding active message " << slot->id << " " << slot->ordinal
  //           << "\n";
  int old = num_active_messages_.fetch_add(1);
  if (old >= options_.MaxActiveMessages() && !IsBridge()) {
    // std::cerr << "exceeded max active messages\n";
    num_active_messages_.fetch_sub(1);
    return false;
  }
  return true;
}

void SubscriberImpl::RemoveActiveMessage(MessageSlot *slot) {
  // std::cerr << this << " remove active message " << slot->id << " "
  //           << slot->ordinal << " refs " << std::hex << slot->refs.load() <<
  //           std::dec << "\n";
  slot->sub_owners.Clear(subscriber_id_);
  AtomicIncRefCount(slot, IsReliable(), -1, slot->ordinal, slot->vchan_id, true,
                    [this, slot]() {
                      // When a slot retires we want to use the slot id that was
                      // originally used for the message.  If the message came
                      // in from a bridge we want to notify the original sender
                      // of the message, not the bridge publisher.
                      //
                      // The original slot id is in the message prefix and is
                      // copied into the slot when the bridge publisher
                      // publishes the message.

                      // Enable this for debugging slot retirement.
                      // std::string details = absl::StrFormat(
                      //   "%d: RemoveActiveMessage: %s retiring slot %d ordinal %d vchan_id %d\n", getpid(), Name(), slot->bridged_slot_id, slot->ordinal, slot->vchan_id);
                      // std::cerr << details;
                      TriggerRetirement(slot->bridged_slot_id);
                    });
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

SubscriberImpl::OrdinalTracker &
SubscriberImpl::GetOrdinalTracker(int vchan_id) {
  auto it = ordinal_trackers_.find(vchan_id);
  if (it != ordinal_trackers_.end()) {
    return *it->second;
  }
  auto [it2, _] =
      ordinal_trackers_.emplace(vchan_id, std::make_unique<OrdinalTracker>());

  return *it2->second;
}

int SubscriberImpl::DetectDrops(int vchan_id) {
  std::vector<OrdinalAndVchanId> ordinals;
  auto &tracker = GetOrdinalTracker(vchan_id_);
  ordinals.reserve(tracker.ordinals.Size());
  tracker.ordinals.Traverse(
      [&tracker, &ordinals, vchan_id](const OrdinalAndVchanId &o) {
        if (vchan_id == o.vchan_id && o.ordinal >= tracker.last_ordinal_seen) {
          ordinals.push_back(o);
        }
      });
  if (ordinals.empty()) {
    return 0;
  }
  std::stable_sort(ordinals.begin(), ordinals.end());
  tracker.last_ordinal_seen = ordinals.back().ordinal;

  // Look for gaps in the ordinals.
  int drops = 0;
  for (size_t i = 1; i < ordinals.size(); i++) {
    if (ordinals[i].vchan_id != vchan_id) {
      // Must be same vchan_id as ordinals are per vchan.
      continue;
    }
    if (ordinals[i].ordinal - ordinals[i - 1].ordinal == 1) {
      continue;
    }
    drops +=
        static_cast<int>(ordinals[i].ordinal - ordinals[i - 1].ordinal - 1);
  }
  return drops;
}

void SubscriberImpl::RememberOrdinal(uint64_t ordinal, int vchan_id) {
  auto &tracker = GetOrdinalTracker(vchan_id_);
  tracker.ordinals.Insert(OrdinalAndVchanId{ordinal, vchan_id});
}

const ActiveSlot *SubscriberImpl::FindUnseenOrdinal() {
  // Traverse the active slots looking for the first ordinal that is not zero
  // and has not been seen by a subscriber.
  auto &tracker = GetOrdinalTracker(vchan_id_);
  for (auto &s : active_slots_) {
    if (s.ordinal != 0 &&
        !tracker.ordinals.Contains(OrdinalAndVchanId{s.ordinal, s.vchan_id})) {
      return &s;
    }
  }
  return nullptr;
}

void SubscriberImpl::ClaimSlot(MessageSlot *slot, int vchan_id,
                               bool was_newest) {
  slot->sub_owners.Set(subscriber_id_);
  if (was_newest) {
    // We read the newest slot so there can't be any other messages for this
    // subscriber.
    GetAvailableSlots(subscriber_id_).ClearAll();
  } else {
    // Clear the bit in the subscriber bitset.
    GetAvailableSlots(subscriber_id_).Clear(slot->id);
  }
  RememberOrdinal(slot->ordinal, vchan_id);
  slot->flags |= kMessageSeen;
}

void SubscriberImpl::CollectVisibleSlots(InPlaceAtomicBitset &bits) {
  uint64_t num_messages = 0;
  do {
    num_messages = ccb_->total_messages;
    active_slots_.clear();

    // Traverse the bits and add an active slot for each bit set.
    bits.Traverse([this](int i) {
      if (embargoed_slots_.IsSet(i)) {
        return;
      }
      MessageSlot *s = &ccb_->slots[i];
      if (!VirtualChannelIdMatch(s, vchan_id_)) {
        return;
      }
      if (s->buffer_index == -1) {
        return;
      }
      ActiveSlot active_slot = {s, s->ordinal, s->timestamp, s->vchan_id};
      active_slots_.push_back(active_slot);
    });
  } while (num_messages != ccb_->total_messages);
}

MessageSlot *SubscriberImpl::NextSlot(MessageSlot *slot, bool reliable,
                                      int owner) {

  InPlaceAtomicBitset &bits = GetAvailableSlots(owner);

  embargoed_slots_.ClearAll();

  constexpr int kMaxRetries = 1000;
  int retries = 0;

  while (retries++ < kMaxRetries) {
#ifndef NDEBUG
    const bool print_errors = retries >= kMaxRetries - 1;
#else
    const bool print_errors = false;
#endif
    CheckReload();
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }

    CollectVisibleSlots(bits);

    // Sort the active slots by timestamp.
    std::stable_sort(active_slots_.begin(), active_slots_.end(),
                     [](const ActiveSlot &a, const ActiveSlot &b) {
                       return a.timestamp < b.timestamp;
                     });

    const ActiveSlot *new_slot = FindUnseenOrdinal();
    if (new_slot == nullptr) {
      return nullptr;
    }
    if (print_errors) {
      std::cerr << "Warning: subscriber for " << Name()
                << " has reached the max retries for reference counter "
                   "increment on slot "
                << new_slot->slot->id << " ordinal " << new_slot->ordinal
                << "; this may indicate heavy use of the channel\n";
    }
    // We have a new slot, see if we can increment the ref count.  If we can't
    // we just go back and try again.
    if (AtomicIncRefCount(new_slot->slot, reliable, 1, new_slot->ordinal,
                          new_slot->vchan_id, false)) {
      if (!ValidateSlotBuffer(new_slot->slot) ||
          new_slot->slot->buffer_index == -1) {
        if (print_errors) {
          std::cerr << "Subscriber for " << Name()
                    << " detected buffer failure on slot: "
                    << new_slot->slot->id
                    << " buffer index: " << new_slot->slot->buffer_index;
          new_slot->slot->Dump(std::cerr);
        }
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots_.Set(new_slot->slot->id);
        AtomicIncRefCount(new_slot->slot, reliable, -1, new_slot->ordinal,
                          new_slot->vchan_id, false);
        continue;
      }
      return new_slot->slot;
    }
  }
  return nullptr;
}

MessageSlot *SubscriberImpl::LastSlot(MessageSlot *slot, bool reliable,
                                      int owner) {

  InPlaceAtomicBitset &bits = GetAvailableSlots(owner);

  embargoed_slots_.ClearAll();
  for (;;) {
    CheckReload();
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }
    CollectVisibleSlots(bits);

    // Sort the active slots by timestamp.
    std::stable_sort(active_slots_.begin(), active_slots_.end(),
                     [](const ActiveSlot &a, const ActiveSlot &b) {
                       return a.timestamp < b.timestamp;
                     });

    ActiveSlot *new_slot = nullptr;
    if (!active_slots_.empty()) {
      new_slot = &active_slots_.back();

      if (slot != nullptr && slot == new_slot->slot) {
        // Same slot, nothing changes.
        new_slot = nullptr;
      }
    }
    if (new_slot == nullptr) {
      return nullptr;
    }

    // Increment the ref count.
    if (AtomicIncRefCount(new_slot->slot, reliable, 1, new_slot->ordinal,
                          new_slot->vchan_id, false)) {
      if (!ValidateSlotBuffer(new_slot->slot) ||
          new_slot->slot->buffer_index == -1) {
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots_.Set(new_slot->slot->id);
        AtomicIncRefCount(new_slot->slot, reliable, -1, new_slot->ordinal,
                          new_slot->vchan_id, false);
        continue;
      }
      return new_slot->slot;
    }
  }
}

MessageSlot *SubscriberImpl::FindActiveSlotByTimestamp(
    MessageSlot *old_slot, uint64_t timestamp, bool reliable, int owner,
    std::vector<ActiveSlot> &buffer) {
  embargoed_slots_.ClearAll();
  for (;;) {
    CheckReload();
    buffer.clear();
    buffer.reserve(NumSlots());

    // Prepopulate the search buffer.
    for (int i = 0; i < NumSlots(); i++) {
      if (embargoed_slots_.IsSet(i)) {
        continue;
      }
      MessageSlot *s = &ccb_->slots[i];
      uint32_t refs = s->refs.load(std::memory_order_relaxed);
      if (s->ordinal != 0 && (refs & kPubOwned) == 0) {
        buffer.push_back({s, 0, Prefix(s)->timestamp});
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
    if (AtomicIncRefCount(it->slot, reliable, 1, it->ordinal, it->vchan_id,
                          false)) {
      if (!ValidateSlotBuffer(it->slot) || it->slot->buffer_index == -1) {
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots_.Set(it->slot->id);
        AtomicIncRefCount(it->slot, reliable, -1, it->ordinal, it->vchan_id,
                          false);
        continue;
      }
      it->slot->flags |= kMessageSeen;
      it->slot->sub_owners.Set(owner);
      return it->slot;
    }
    // Publisher got there first, try again.
  }
}
} // namespace details
} // namespace subspace
