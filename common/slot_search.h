// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

// Finding and claiming a slot for a publisher's next message, shared by every
// client.

#pragma once

#include "common/bitset_view.h"
#include "common/channel_view.h"
#include "common/shm_layout.h"

#include <atomic>
#include <cstdint>
#include <sched.h>

namespace subspace {

// A client's part in a slot search.  A client derives from this, keeps or
// replaces the defaults, and adds:
//
//   // Gives the claimed slot the buffer its next message goes in.
//   void AssignBuffer(MessageSlot *slot);
//   MessagePrefix *Prefix(MessageSlot *slot);
//   // The claimed slot held a message that a subscriber might not have read.
//   void ForcedReuse(MessageSlot *slot);
//
// The defaults suit a client that maps all of a channel's buffers when it
// opens the channel.
class SlotSearchHooks {
public:
  // Called before each attempt to find a slot, for example to map buffers
  // that another publisher added.
  void BeforeAttempt() {}
  // Whether the publisher can use a slot it has claimed.  The search gives a
  // slot it can't use back and embargoes it for the rest of the search.
  bool UsableSlot(MessageSlot * /*slot*/) { return true; }
  void Embargo(int /*slot_id*/) {}
  bool Embargoed(int /*slot_id*/) const { return false; }
};

struct SlotSearch {
  // Stored in refs, with kPubOwned, while the publisher owns the slot.
  int owner = 0;
  int vchan_id = -1;
  // An unreliable publisher takes a retired slot before a free one while
  // anything subscribes.  See PublisherOptions::SetPreferRetiredSlots().
  bool prefer_retired = false;
  // Whether a reliable publisher takes retired slots.  A tunnel publisher
  // doesn't.
  bool reuse_retired = true;
};

// Clears every subscriber's available bit for slot, whose message is gone.
inline void ForgetMessage(const ChannelView &channel, const MessageSlot *slot) {
  channel.Subscribers().Traverse(
      [&](int sub_id) { channel.AvailableSlots(sub_id).Clear(slot->id); });
}

// Resets a slot the publisher has just claimed for a message on vchan_id.
template <typename Hooks>
void ResetClaimedSlot(const ChannelView &channel, MessageSlot *slot,
                      int vchan_id, Hooks &hooks) {
  slot->ordinal.store(0, std::memory_order_relaxed);
  slot->timestamp.store(0, std::memory_order_relaxed);
  slot->vchan_id.store(static_cast<int16_t>(vchan_id),
                       std::memory_order_relaxed);
  hooks.AssignBuffer(slot);
  MessagePrefix *prefix = hooks.Prefix(slot);
  prefix->flags = 0;
  prefix->vchan_id = vchan_id;
  ForgetMessage(channel, slot);
}

// The retired slot to reuse next, or -1.
//
// Normally a slot is only retired once every subscriber has seen and dropped
// it, so the retired pool stays small and taking the lowest-index slot keeps
// the publisher on the cache-hot working set that SetPreferRetiredSlots()
// exists to produce.
//
// With nothing subscribed every slot is retired the instant it is published,
// so the retired pool is the whole ring and the lowest-index slot is the one
// holding the oldest message.  Reusing that slot puts it straight back at the
// front of the pool, so the publisher would overwrite it forever while the
// rest of the ring stayed frozen on the messages that happened to be
// published first, and a subscriber attaching later would read those rather
// than recent history.  With oldest_first, recycling the oldest message
// rotates through the ring instead.
template <typename Hooks>
int RetiredSlotToReuse(const ChannelView &channel, const Hooks &hooks,
                       bool oldest_first) {
  BitsetView retired = channel.RetiredSlots();
  if (!oldest_first) {
    const int first = retired.FindFirstSet();
    if (first == -1 || !hooks.Embargoed(first)) {
      return first;
    }
    for (int i = first + 1; i < channel.NumSlots(); i++) {
      if (retired.IsSet(i) && !hooks.Embargoed(i)) {
        return i;
      }
    }
    return -1;
  }
  int oldest = -1;
  uint64_t oldest_timestamp = ~uint64_t{0};
  for (int i = 0; i < channel.NumSlots(); i++) {
    if (!retired.IsSet(i) || hooks.Embargoed(i)) {
      continue;
    }
    const uint64_t timestamp =
        channel.Slot(i)->timestamp.load(std::memory_order_relaxed);
    if (timestamp < oldest_timestamp) {
      oldest_timestamp = timestamp;
      oldest = i;
    }
  }
  return oldest;
}

namespace slot_search_internal {

enum class Pick { kTaken, kNone, kRetry };

// Takes a slot that has never held a message.
inline Pick TakeFreeSlot(const ChannelView &channel, int &slot_id) {
  ChannelControlBlock *ccb = channel.Ccb();
  if (ccb->free_slots_exhausted.load(std::memory_order_relaxed)) {
    return Pick::kNone;
  }
  BitsetView free_slots = channel.FreeSlots();
  slot_id = free_slots.FindFirstSet();
  if (slot_id == -1) {
    return Pick::kNone;
  }
  // FindFirstSet uses relaxed loads, so only the publisher whose ClearWasSet
  // returns true owns the bit.
  if (!free_slots.ClearWasSet(slot_id)) {
    slot_id = -1;
    return Pick::kRetry;
  }
  if (free_slots.IsEmpty()) {
    ccb->free_slots_exhausted.store(true, std::memory_order_relaxed);
  }
  return Pick::kTaken;
}

// Takes a slot whose message every subscriber has released.  The caller
// checks the refs, because the bit can be stale.
template <typename Hooks>
Pick TakeRetiredSlot(const ChannelView &channel, const Hooks &hooks,
                     bool oldest_first, int &slot_id) {
  slot_id = RetiredSlotToReuse(channel, hooks, oldest_first);
  if (slot_id == -1) {
    return Pick::kNone;
  }
  if (!channel.RetiredSlots().ClearWasSet(slot_id)) {
    slot_id = -1;
    return Pick::kRetry;
  }
  return Pick::kTaken;
}

// Takes ownership of slot if its refs are still old_refs with no references.
//
// Strong, not weak: a weak exchange may fail spuriously (on ARM's
// load/store-exclusive, a page fault between the two clears the reservation,
// which the first write to a slot's page always takes), and the claim loops
// do not retry the same slot. They go back to selecting one, having already
// cleared this slot's free or retired bit, so a spurious failure leaks the
// slot from that pool.
inline bool TryClaimSlot(MessageSlot *slot, int owner, uint64_t old_refs) {
  uint64_t expected = BuildRefsBitField(
      slot->ordinal.load(std::memory_order_relaxed),
      static_cast<int>((old_refs >> kVchanIdShift) & kVchanIdMask),
      static_cast<int>((old_refs >> kRetiredRefsShift) & kRetiredRefsMask));
  return slot->refs.compare_exchange_strong(
      expected, kPubOwned | static_cast<uint64_t>(owner),
      std::memory_order_acquire, std::memory_order_relaxed);
}

// The slot with the oldest message that no subscriber holds.
template <typename Hooks>
MessageSlot *OldestUnreferencedSlot(const ChannelView &channel,
                                    const Hooks &hooks) {
  MessageSlot *oldest = nullptr;
  uint64_t oldest_timestamp = ~uint64_t{0};
  for (int i = 0; i < channel.NumSlots(); i++) {
    if (hooks.Embargoed(i)) {
      continue;
    }
    MessageSlot *s = channel.Slot(i);
    const uint64_t refs = s->refs.load(std::memory_order_relaxed);
    if ((refs & kPubOwned) != 0) {
      continue;
    }
    const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
    if ((refs & kRefsMask) == 0 && timestamp < oldest_timestamp) {
      oldest = s;
      oldest_timestamp = timestamp;
    }
  }
  return oldest;
}

// Slots are ordered by timestamp, then by id.
inline bool Earlier(uint64_t timestamp, int id, uint64_t other_timestamp,
                    int other_id) {
  if (timestamp != other_timestamp) {
    return timestamp < other_timestamp;
  }
  return id < other_id;
}

// Whether a reliable subscriber still needs the slot's message: it holds the
// message, or reliable subscribers exist and none has seen it.
inline bool NeededByReliable(const MessageSlot *slot, uint64_t refs,
                             bool require_reliable_seen) {
  return ((refs >> kReliableRefCountShift) & kRefCountMask) != 0 ||
         (require_reliable_seen &&
          slot->ordinal.load(std::memory_order_relaxed) != 0 &&
          (slot->flags.load(std::memory_order_relaxed) &
           kMessageSeenByReliable) == 0);
}

inline bool ReliableClaimable(const MessageSlot *slot,
                              bool require_reliable_seen) {
  const uint64_t refs = slot->refs.load(std::memory_order_relaxed);
  return !NeededByReliable(slot, refs, require_reliable_seen) &&
         (refs & kRefsMask) == 0;
}

// The oldest slot that no subscriber holds and that is older than every slot
// a reliable subscriber still needs, so a reliable publisher never overtakes
// a reliable subscriber.
template <typename Hooks>
MessageSlot *OldestReliableCandidate(const ChannelView &channel,
                                     const Hooks &hooks,
                                     bool require_reliable_seen) {
  int stop = -1;
  uint64_t stop_timestamp = 0;
  for (int i = 0; i < channel.NumSlots(); i++) {
    if (hooks.Embargoed(i)) {
      continue;
    }
    const MessageSlot *s = channel.Slot(i);
    const uint64_t refs = s->refs.load(std::memory_order_relaxed);
    if ((refs & kPubOwned) != 0 ||
        !NeededByReliable(s, refs, require_reliable_seen)) {
      continue;
    }
    const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
    if (stop == -1 || Earlier(timestamp, i, stop_timestamp, stop)) {
      stop = i;
      stop_timestamp = timestamp;
    }
  }
  MessageSlot *best = nullptr;
  uint64_t best_timestamp = 0;
  for (int i = 0; i < channel.NumSlots(); i++) {
    if (hooks.Embargoed(i)) {
      continue;
    }
    MessageSlot *s = channel.Slot(i);
    const uint64_t refs = s->refs.load(std::memory_order_relaxed);
    if ((refs & (kPubOwned | kRefsMask)) != 0) {
      continue;
    }
    const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
    if (stop != -1 && !Earlier(timestamp, i, stop_timestamp, stop)) {
      continue;
    }
    if (best == nullptr || Earlier(timestamp, i, best_timestamp, best->id)) {
      best = s;
      best_timestamp = timestamp;
    }
  }
  return best;
}

constexpr int kMaxCasRetries = 1000;

// Counts a failed claim.  Returns false when the search should give up.
inline bool BackOff(int &cas_retries, int &retries) {
  if (++cas_retries < kMaxCasRetries) {
    return true;
  }
  if (retries-- == 0) {
    return false;
  }
  cas_retries = 0;
  (void)sched_yield();
  return true;
}

} // namespace slot_search_internal

// Claims a slot for an unreliable publisher's next message and resets it.
// Returns null only if the search keeps failing.
//
// With prefer_retired a retired slot comes before a free one.  Both are
// equally valid for an unreliable publisher (a retired slot has been seen by
// every current subscriber and dropped), but a retired slot's pages are
// already cache-hot from the recent publish/consume cycle; a free slot will
// demand-fault the kernel into allocating new physical pages on first write,
// which dominates wall-time for large payloads.  In steady state the
// publisher cycles through a tiny working set of retired slots without ever
// consuming free slots, while still being able to burst into them if the
// subscriber falls behind.
//
// None of that holds with nothing subscribed: a slot is then retired the
// moment it is published, so the publisher would take the slot it just wrote
// back out of the retired pool on the very next publish and never touch the
// free slots at all.  The ring would hold one message no matter how deep it
// is configured.  So free slots come first while idle, and together with the
// oldest-first recycling in RetiredSlotToReuse the ring becomes a rolling
// window of recent messages for whoever attaches next.
template <typename Hooks>
MessageSlot *ClaimSlotUnreliable(const ChannelView &channel,
                                 const SlotSearch &search, Hooks &hooks) {
  using namespace slot_search_internal;
  int retries = channel.NumSlots() * 1000;
  int cas_retries = 0;
  MessageSlot *slot = nullptr;
  int free_slot = -1;
  int retired_slot = -1;
  for (;;) {
    slot = nullptr;
    free_slot = -1;
    retired_slot = -1;
    hooks.BeforeAttempt();
    // Read the subscriber count on each attempt, so a subscriber attaching
    // while we retry is seen.
    const bool no_subscribers = channel.NumSubscribers(search.vchan_id) == 0;
    Pick pick;
    if (search.prefer_retired && !no_subscribers) {
      pick = TakeRetiredSlot(channel, hooks, /*oldest_first=*/false,
                             retired_slot);
      if (pick == Pick::kNone) {
        pick = TakeFreeSlot(channel, free_slot);
      }
    } else {
      pick = TakeFreeSlot(channel, free_slot);
      if (pick == Pick::kNone) {
        pick = TakeRetiredSlot(channel, hooks, /*oldest_first=*/no_subscribers,
                               retired_slot);
      }
    }
    if (pick == Pick::kRetry) {
      continue;
    }
    slot = pick == Pick::kTaken
               ? channel.Slot(free_slot != -1 ? free_slot : retired_slot)
               : OldestUnreferencedSlot(channel, hooks);
    if (slot == nullptr) {
      // There is always a slot, but don't loop forever if something is wrong.
      if (retries-- == 0) {
        return nullptr;
      }
      continue;
    }
    const uint64_t old_refs = slot->refs.load(std::memory_order_relaxed);
    if (retired_slot != -1 && !channel.RefsShowRetired(slot, old_refs)) {
      // A stale retired bit, now cleared.  The slot may hold the newest
      // message.
      continue;
    }
    if (TryClaimSlot(slot, search.owner, old_refs)) {
      if (!hooks.UsableSlot(slot)) {
        slot->refs.store(old_refs, std::memory_order_relaxed);
        hooks.Embargo(slot->id);
        continue;
      }
      break;
    }
    if (!BackOff(cas_retries, retries)) {
      return nullptr;
    }
  }
  ResetClaimedSlot(channel, slot, search.vchan_id, hooks);
  if (free_slot == -1 && retired_slot == -1) {
    hooks.ForcedReuse(slot);
  }
  return slot;
}

// Claims a slot for a reliable publisher's next message and resets it.
// Returns null when every slot holds a message that a reliable subscriber
// still needs; a reliable subscriber triggers the publisher when it releases
// one.
template <typename Hooks>
MessageSlot *ClaimSlotReliable(const ChannelView &channel,
                               const SlotSearch &search,
                               const ChannelCounters &counters, Hooks &hooks) {
  using namespace slot_search_internal;
  int retries = channel.NumSlots() * 1000;
  int cas_retries = 0;
  MessageSlot *slot = nullptr;
  int free_slot = -1;
  int retired_slot = -1;
  for (;;) {
    slot = nullptr;
    free_slot = -1;
    retired_slot = -1;
    hooks.BeforeAttempt();
    const bool require_reliable_seen = counters.num_reliable_subs != 0;
    Pick pick = TakeFreeSlot(channel, free_slot);
    if (pick == Pick::kNone && search.reuse_retired) {
      pick = TakeRetiredSlot(channel, hooks, /*oldest_first=*/false,
                             retired_slot);
    }
    if (pick == Pick::kRetry) {
      continue;
    }
    if (pick == Pick::kTaken) {
      slot = channel.Slot(free_slot != -1 ? free_slot : retired_slot);
      const uint64_t refs = slot->refs.load(std::memory_order_relaxed);
      if (retired_slot != -1 && !channel.RefsShowRetired(slot, refs)) {
        // A stale retired bit, now cleared.
        continue;
      }
      if (!ReliableClaimable(slot, require_reliable_seen)) {
        return nullptr;
      }
    } else {
      slot = OldestReliableCandidate(channel, hooks, require_reliable_seen);
      if (slot == nullptr) {
        return nullptr;
      }
    }
    const uint64_t old_refs = slot->refs.load(std::memory_order_relaxed);
    if (TryClaimSlot(slot, search.owner, old_refs)) {
      if (!hooks.UsableSlot(slot)) {
        slot->refs.store(old_refs, std::memory_order_relaxed);
        hooks.Embargo(slot->id);
        continue;
      }
      channel.RetiredSlots().Clear(slot->id);
      break;
    }
    if (!BackOff(cas_retries, retries)) {
      return nullptr;
    }
  }
  ResetClaimedSlot(channel, slot, search.vchan_id, hooks);
  if (free_slot == -1 && retired_slot == -1) {
    hooks.ForcedReuse(slot);
  }
  return slot;
}

} // namespace subspace
