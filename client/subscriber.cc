// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/subscriber.h"

#include <limits>

namespace subspace {
namespace details {

static bool ActiveSlotLess(const ActiveSlot &a, const ActiveSlot &b) {
  if (a.timestamp != b.timestamp) {
    return a.timestamp < b.timestamp;
  }
  return a.ordinal < b.ordinal;
}

void SubscriberImpl::InitActiveMessages() {
  active_messages_.resize(NumSlots());
  int slot_id = 0;
  for (auto &m : active_messages_) {
    m = std::make_shared<ActiveMessage>(shared_from_this(), &ccb_->slots[slot_id]);
    slot_id++;
  }
}

void SubscriberImpl::ResetDeliveryState() {
  ClearActiveMessage();
  SetSlot(nullptr);
  active_slots_.clear();
  search_buffer_.clear();
  newest_snapshot_.clear();
  embargoed_slots_.ClearAll();
  ordinal_trackers_.clear();
  (void)GetOrdinalTracker(vchan_id_);
  next_slot_cached_total_ = 0;
  next_slot_cursor_ = 0;
  next_slot_cache_valid_ = false;
  poll_drain_exhausted_ = false;
  queue_bitset_fallback_ = false;
  pending_queue_drops_ = 0;
  queue_drain_tail_ = 0;
  queue_drain_tail_valid_ = false;
}

// For non-virtual channels both the slots' vchan_id and the subsriber's
// are -1.  This is the common case.
// For virtual subscribers, if the slot's vchan_id is -1 it means that
// the message was published direcly to the multiplexer and all subscribers
// should see it.

// If the subscriber's vchan_id is -1 it means that the subscriber is on the
// multiplexer and should see all messages, regardless of the vchan_id
static inline bool VirtualChannelIdMatch(MessageSlot *slot, int vchan_id) {
  const int slot_vchan_id =
      slot->vchan_id.load(std::memory_order_relaxed);
  return vchan_id == -1 || slot_vchan_id == -1 ||
         slot_vchan_id == vchan_id;
}

bool SubscriberImpl::AddActiveMessage([[maybe_unused]] MessageSlot *slot) {
  // std::cerr << "adding active message " << slot->id << " " << slot->ordinal
  //           << "\n";
  int old = num_active_messages_.fetch_add(1);
  if (old >= options_.MaxActiveMessages() && !IsBridge()) {
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
  AtomicIncRefCount(slot, IsReliable(), -1,
                    slot->ordinal.load(std::memory_order_relaxed),
                    slot->vchan_id.load(std::memory_order_relaxed), true,
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
                      //   "%d: RemoveActiveMessage: %s retiring slot %d ordinal "
                      //   "%d vchan_id %d\n", getpid(), Name(),
                      //   slot->bridged_slot_id, slot->ordinal,
                      //   slot->vchan_id);
                      // std::cerr << details;
                      TriggerRetirement(
                          slot->bridged_slot_id.load(std::memory_order_relaxed));
                    });
  if (--num_active_messages_ < options_.MaxActiveMessages()) {
    Trigger();
    if (IsReliable()) {
      TriggerReliablePublishers();
    }
  }
}

void SubscriberImpl::PopulateActiveSlots(InPlaceAtomicBitset &bits) {
  uint64_t num_messages = 0;
  do {
    num_messages = ccb_->total_messages.load(std::memory_order_seq_cst);
    bits.ClearAll();

    for (int i = 0; i < NumSlots(); i++) {
      MessageSlot *s = &ccb_->slots[i];
      uint64_t refs = s->refs.load(std::memory_order_acquire);
      if (VirtualChannelIdMatch(s, vchan_id_) &&
          s->ordinal.load(std::memory_order_relaxed) != 0 &&
          (refs & kPubOwned) == 0) {
        bits.Set(i);
      }
    }
  } while (num_messages !=
           ccb_->total_messages.load(std::memory_order_seq_cst));
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
  auto &tracker = GetOrdinalTracker(vchan_id);
  const uint64_t ordinal = CurrentOrdinal();
  if (ordinal == 0 || ordinal <= tracker.last_ordinal_seen) {
    return 0;
  }
  const uint64_t last_seen = tracker.last_ordinal_seen;
  tracker.last_ordinal_seen = ordinal;
  if (last_seen == 0 || ordinal == last_seen + 1) {
    return 0;
  }
  return static_cast<int>(ordinal - last_seen - 1);
}

void SubscriberImpl::RememberOrdinal(uint64_t ordinal, int vchan_id) {
  auto &tracker = GetOrdinalTracker(vchan_id);
  if (ordinal > tracker.last_ordinal_seen) {
    tracker.last_ordinal_seen = ordinal;
  }
  tracker.ordinals.Insert(OrdinalAndVchanId{ordinal, vchan_id});
}

const ActiveSlot *SubscriberImpl::FindUnseenOrdinal() {
  // Traverse the active slots looking for the first ordinal that is not zero
  // and has not been seen by a subscriber.
  int cached_vchan_id = std::numeric_limits<int>::min();
  OrdinalTracker *cached_tracker = nullptr;
  for (auto &s : active_slots_) {
    if (s.vchan_id != cached_vchan_id) {
      cached_vchan_id = s.vchan_id;
      cached_tracker = &GetOrdinalTracker(s.vchan_id);
    }
    if (s.ordinal > cached_tracker->last_ordinal_seen &&
        !cached_tracker->ordinals.Contains(OrdinalAndVchanId{s.ordinal, s.vchan_id})) {
      // std::cerr << absl::StrFormat("Found unseen ordinal %d in slot %d\n", s.ordinal, s.slot->id);
      return &s;
    }
  }
  return nullptr;
}

void SubscriberImpl::ClaimSlot(MessageSlot *slot, int vchan_id,
                               bool was_newest) {
  slot->sub_owners.Set(subscriber_id_);
  if (was_newest) {
    InPlaceAtomicBitset &bits = GetAvailableSlots(subscriber_id_);
    for (const ActiveSlot &snapshot : newest_snapshot_) {
      bool pinned = snapshot.slot == slot;
      if (!pinned) {
        pinned = AtomicIncRefCount(snapshot.slot, IsReliable(), 1,
                                   snapshot.ordinal, snapshot.vchan_id, false);
      }
      if (!pinned) {
        // The slot was recycled after the ReadNewest snapshot. Its current bit
        // belongs to the new generation and must remain set.
        continue;
      }
      bits.Clear(snapshot.slot->id);
      RememberOrdinal(snapshot.ordinal, snapshot.vchan_id);
      if (snapshot.slot != slot) {
        AtomicIncRefCount(snapshot.slot, IsReliable(), -1, snapshot.ordinal,
                          snapshot.vchan_id, false);
      }
    }
    newest_snapshot_.clear();
  } else {
    // Clear the bit in the subscriber bitset.
    GetAvailableSlots(subscriber_id_).Clear(slot->id);
  }
  RememberOrdinal(slot->ordinal.load(std::memory_order_relaxed), vchan_id);
  slot->flags.fetch_or(kMessageSeen, std::memory_order_relaxed);
  if (IsReliable()) {
    slot->flags.fetch_or(kMessageSeenByReliable, std::memory_order_relaxed);
  }
}

void SubscriberImpl::UnreadSlot(MessageSlot *slot) {
  DecrementSlotRef(slot, false);
  // A queued hint has already been consumed by NextSlot(). If delivery is
  // rejected (for example at max_active_messages), the slot remains unread in
  // the authoritative bitset but is no longer present in the queue. Stay on
  // the ordinal-sorted bitset path until that backlog has been recovered;
  // otherwise the next queue entry would be delivered first and permanently
  // skip this ordinal.
  if (SubscriberQueueSize() > 0) {
    queue_bitset_fallback_ = true;
  }
  // NextSlot()'s cache advanced next_slot_cursor_ past this slot when it
  // returned, on the assumption that ReadMessageInternal would either
  // ClaimSlot() it (recording the ordinal in the tracker) or accept that it
  // had been delivered.  When SetActiveMessage() hits max_active_messages
  // we end up here instead, with the slot's bit still set and the ordinal
  // never recorded.  Without invalidation the next NextSlot() would reuse
  // the cached, sorted active_slots_ and walk straight past this entry,
  // delivering a later ordinal first.  Force a fresh CollectVisibleSlots()
  // snapshot so we revisit this slot.
  next_slot_cache_valid_ = false;
}

uint64_t SubscriberImpl::CollectVisibleSlots(InPlaceAtomicBitset &bits) {
  uint64_t num_messages = 0;
  do {
    num_messages = ccb_->total_messages.load(std::memory_order_seq_cst);
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
      if (s->buffer_index.load(std::memory_order_relaxed) == -1) {
        return;
      }
      ActiveSlot active_slot = {
          s, s->ordinal.load(std::memory_order_relaxed),
          s->timestamp.load(std::memory_order_relaxed),
          s->vchan_id.load(std::memory_order_relaxed)};
      active_slots_.push_back(active_slot);
    });
  } while (num_messages !=
           ccb_->total_messages.load(std::memory_order_seq_cst));
  return num_messages;
}

std::optional<ClaimedQueuedSlot>
SubscriberImpl::FindNextQueuedSlot(uint64_t max_queue_position) {
  InPlaceSlotQueue *queue = GetAvailableSlotQueueAddress(subscriber_id_);
  if (queue == nullptr || queue->Capacity() == 0) {
    return std::nullopt;
  }

  int cached_vchan_id = std::numeric_limits<int>::min();
  OrdinalTracker *cached_tracker = nullptr;

  QueuedSlot queued;
  for (size_t i = 0; i < queue->Capacity(); i++) {
    if (queue->Head() >= max_queue_position) {
      return std::nullopt;
    }
    if (!queue->TryPeek(queued)) {
      return std::nullopt;
    }
    if (queued.slot_id < 0 || queued.slot_id >= NumSlots()) {
      queue->DropFront();
      continue;
    }
    QueuedSlot popped;
    if (!queue->TryPop(popped)) {
      continue;
    }
    queued = popped;
    if (queued.slot_id < 0 || queued.slot_id >= NumSlots()) {
      continue;
    }
    MessageSlot *s = &ccb_->slots[queued.slot_id];
    const uint64_t refs = s->refs.load(std::memory_order_acquire);
    if ((refs & kPubOwned) != 0) {
      continue;
    }
    const uint64_t ref_ordinal = (refs >> kOrdinalShift) & kOrdinalMask;
    int ref_vchan_id = (refs >> kVchanIdShift) & kVchanIdMask;
    if (ref_vchan_id == kVchanIdMask) {
      ref_vchan_id = -1;
    }
    if (queued.ordinal == 0 ||
        (queued.ordinal & kOrdinalMask) != ref_ordinal ||
        (vchan_id_ != -1 && ref_vchan_id != -1 &&
         vchan_id_ != ref_vchan_id)) {
      continue;
    }
    if (ref_vchan_id != cached_vchan_id) {
      cached_vchan_id = ref_vchan_id;
      cached_tracker = &GetOrdinalTracker(ref_vchan_id);
    }
    const OrdinalAndVchanId queued_key{queued.ordinal, ref_vchan_id};
    if (queued.ordinal <= cached_tracker->last_ordinal_seen &&
        cached_tracker->ordinals.Contains(queued_key)) {
      continue;
    }
    if (options_.SubscriberQueueSize() == 0 &&
        queued.ordinal > cached_tracker->last_ordinal_seen &&
        queued.ordinal - cached_tracker->last_ordinal_seen > 1) {
      // Queue reservations from concurrent publishers need not follow ordinal
      // order. Before accepting a gap, including the first queued ordinal,
      // recover any older authoritative bit in ordinal order. If an older
      // queue entry arrives after a newer one was already delivered, the exact
      // ordinal tracker above still permits that unseen entry instead of
      // silently discarding it.
      InPlaceAtomicBitset &bits = GetAvailableSlots(subscriber_id_);
      if (FindNextVisibleSlot(bits, queued.ordinal - 1) != nullptr) {
        queue_bitset_fallback_ = true;
        next_slot_cache_valid_ = false;
        return std::nullopt;
      }
    }
    if (!AtomicIncRefCount(s, /*reliable=*/false, 1, queued.ordinal,
                           ref_vchan_id, false)) {
      continue;
    }
    // The CAS above validates only the low kOrdinalBits stored in refs. After
    // those bits wrap, a stale queue entry can therefore claim a newer slot
    // generation. Verify the full ordinal while holding the reference and roll
    // it back if the queue entry was an alias.
    if (s->ordinal.load(std::memory_order_relaxed) != queued.ordinal ||
        s->vchan_id.load(std::memory_order_relaxed) != ref_vchan_id) {
      AtomicIncRefCount(s, /*reliable=*/false, -1, queued.ordinal,
                        ref_vchan_id, false);
      continue;
    }
    return ClaimedQueuedSlot{
        .slot = s, .ordinal = queued.ordinal, .vchan_id = ref_vchan_id};
  }

  return std::nullopt;
}

MessageSlot *SubscriberImpl::FindNextVisibleSlot(InPlaceAtomicBitset &bits,
                                                 uint64_t max_ordinal) {
  MessageSlot *best_slot = nullptr;
  uint64_t best_ordinal = 0;
  int cached_vchan_id = std::numeric_limits<int>::min();
  OrdinalTracker *cached_tracker = nullptr;

  bits.Traverse([this, &best_slot, &best_ordinal, &cached_vchan_id,
                 &cached_tracker, max_ordinal](size_t i) {
    if (embargoed_slots_.IsSet(i)) {
      return;
    }
    MessageSlot *s = &ccb_->slots[i];
    const uint64_t refs = s->refs.load(std::memory_order_acquire);
    if ((refs & kPubOwned) != 0) {
      return;
    }
    const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
    const int slot_vchan_id =
        s->vchan_id.load(std::memory_order_relaxed);
    if (ordinal == 0 || ordinal > max_ordinal ||
        (vchan_id_ != -1 && slot_vchan_id != -1 &&
         slot_vchan_id != vchan_id_) ||
        s->buffer_index.load(std::memory_order_relaxed) == -1) {
      return;
    }
    if (slot_vchan_id != cached_vchan_id) {
      cached_vchan_id = slot_vchan_id;
      cached_tracker = &GetOrdinalTracker(slot_vchan_id);
    }
    if (ordinal <= cached_tracker->last_ordinal_seen) {
      return;
    }
    if (best_slot == nullptr || ordinal < best_ordinal) {
      best_slot = s;
      best_ordinal = ordinal;
    }
  });

  return best_slot;
}

MessageSlot *SubscriberImpl::NextSlot(MessageSlot *slot, bool reliable,
                                      int owner) {

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
    InPlaceAtomicBitset &bits = GetAvailableSlots(owner);
    const bool stable_poll_drain = PollDrainPending();
    if (stable_poll_drain && poll_drain_exhausted_) {
      if (ccb_->total_messages.load(std::memory_order_seq_cst) !=
          next_slot_cached_total_) {
        poll_drain_exhausted_ = false;
        queue_drain_tail_valid_ = false;
        next_slot_cache_valid_ = false;
      } else {
        return nullptr;
      }
    }
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }

    if (!reliable && SubscriberQueueSize() > 0) {
      InPlaceSlotQueue *queue =
          GetAvailableSlotQueueAddress(subscriber_id_);
      uint32_t queue_overflow_baseline = 0;
      if (queue != nullptr) {
        const uint32_t queue_drops = queue->ConsumeOverflow();
        const bool insertion_failed = queue->ConsumeInsertionFailure();
        queue_overflow_baseline = queue->OverflowCount();
        const bool recover_overflow =
            options_.SubscriberQueueSize() == 0 &&
            (queue_drops != 0 || queue_overflow_baseline != 0);
        if (options_.DetectDroppedMessages() && !recover_overflow) {
          pending_queue_drops_ += static_cast<int>(queue_drops);
        }
        if (recover_overflow || insertion_failed) {
          // An evicted hint can refer to an older slot that remains readable in
          // the authoritative bitset. For an inherited default queue, preserve
          // the legacy no-drop behavior by delivering that backlog in ordinal
          // order before accepting newer queue entries. Explicit queue sizes
          // retain their requested bounded/drop-oldest semantics.
          queue_bitset_fallback_ = true;
          next_slot_cache_valid_ = false;
        }
      }
      if (stable_poll_drain && !queue_drain_tail_valid_) {
        next_slot_cached_total_ = CollectVisibleSlots(bits);
        std::sort(active_slots_.begin(), active_slots_.end(), ActiveSlotLess);
        next_slot_cursor_ = 0;
        next_slot_cache_valid_ = true;
        queue_drain_tail_ = queue == nullptr ? 0 : queue->Tail();
        queue_drain_tail_valid_ = true;
      }
      const uint64_t max_queue_position =
          stable_poll_drain ? queue_drain_tail_
                            : std::numeric_limits<uint64_t>::max();
      std::optional<ClaimedQueuedSlot> claimed =
          queue_bitset_fallback_
              ? std::nullopt
              : FindNextQueuedSlot(max_queue_position);
      if (claimed.has_value()) {
        MessageSlot *new_slot = claimed->slot;
        const uint64_t ordinal = claimed->ordinal;
        const int vchan_id = claimed->vchan_id;
        if (queue != nullptr && queue->InsertionFailed()) {
          AtomicIncRefCount(new_slot, reliable, -1, ordinal, vchan_id, false);
          queue->ConsumeInsertionFailure();
          queue_bitset_fallback_ = true;
          next_slot_cache_valid_ = false;
          continue;
        }
        if (queue != nullptr && options_.SubscriberQueueSize() == 0 &&
            queue->OverflowCount() != queue_overflow_baseline) {
          AtomicIncRefCount(new_slot, reliable, -1, ordinal, vchan_id, false);
          queue->ConsumeOverflow();
          queue_bitset_fallback_ = true;
          next_slot_cache_valid_ = false;
          continue;
        }
        if (queue != nullptr && options_.SubscriberQueueSize() != 0) {
          const uint32_t concurrent_drops = queue->ConsumeOverflow();
          if (options_.DetectDroppedMessages()) {
            pending_queue_drops_ += static_cast<int>(concurrent_drops);
          }
        }
        const int buffer_index =
            new_slot->buffer_index.load(std::memory_order_relaxed);
        if (!ValidateSlotBuffer(new_slot) || buffer_index == -1) {
          if (print_errors) {
            std::cerr << "Subscriber for " << Name()
                      << " detected buffer failure on slot: " << new_slot->id
                      << " buffer index: " << buffer_index;
            new_slot->Dump(std::cerr);
          }
          embargoed_slots_.Set(new_slot->id);
          AtomicIncRefCount(new_slot, reliable, -1, ordinal, vchan_id, false);
          continue;
        }
        if (!stable_poll_drain) {
          next_slot_cache_valid_ = false;
        }
        return new_slot;
      }
      // Push() may fail after a peer dies or loses a bounded CAS race. The
      // publisher always records the slot in the bitset, so continue below and
      // recover it through the authoritative path.
    }

    // Fast path: if the publisher hasn't appended any new messages since the
    // last successful NextSlot() call, the cached, already-sorted
    // active_slots_ list is still valid. We just need to scan forward from
    // next_slot_cursor_ to find the next ordinal we haven't yet delivered.
    //
    // This avoids the O(K) bitset traversal and O(K log K) timestamp sort on
    // every receive, which dominates throughput when the queue is deep.
    //
    // If the caller explicitly requested the poll fd, keep a stable snapshot
    // for each drain even if publishers append more messages while the
    // subscriber is draining it. Poll-driven callers commonly hold the fd for
    // the subscriber lifetime, drain until ReadMessage() returns an empty
    // message, then yield back to their event loop. Chasing concurrently
    // published messages here can keep a slow reliable subscriber in the drain
    // loop forever and starve unrelated work in that process. Direct
    // ReadMessage() callers that never request the fd keep the live
    // total_messages refresh behavior.
    //
    // Correctness invariant: when we observe total_messages == T we must
    // also observe every bits.Set() done by the publisher for ordinals
    // <= T. PublisherImpl::ActivateSlotAndGetAnother sets the
    // available-slot bit BEFORE incrementing total_messages, and the
    // total_messages increment is seq_cst, so the relaxed bit write is
    // happens-before this seq_cst load and visible to the relaxed
    // bits.Traverse() inside CollectVisibleSlots().
    const uint64_t total =
        ccb_->total_messages.load(std::memory_order_seq_cst);
    if (!next_slot_cache_valid_ ||
        (!stable_poll_drain && total != next_slot_cached_total_)) {
      const uint64_t snapshot_total = CollectVisibleSlots(bits);
      if (queue_bitset_fallback_) {
        std::sort(active_slots_.begin(), active_slots_.end(),
                  [](const ActiveSlot &a, const ActiveSlot &b) {
                    return a.ordinal < b.ordinal;
                  });
      } else {
        std::sort(active_slots_.begin(), active_slots_.end(), ActiveSlotLess);
      }
      next_slot_cached_total_ = snapshot_total;
      next_slot_cursor_ = 0;
      next_slot_cache_valid_ = true;
    }

    // Walk forward from the cursor, skipping anything we've embargoed in
    // this NextSlot() invocation or already delivered to this subscriber.
    const ActiveSlot *new_slot = nullptr;
    int cached_vchan_id = std::numeric_limits<int>::min();
    OrdinalTracker *cached_tracker = nullptr;
    while (next_slot_cursor_ < active_slots_.size()) {
      const ActiveSlot &s = active_slots_[next_slot_cursor_];
      if (embargoed_slots_.IsSet(s.slot->id)) {
        ++next_slot_cursor_;
        continue;
      }
      if (s.vchan_id != cached_vchan_id) {
        cached_vchan_id = s.vchan_id;
        cached_tracker = &GetOrdinalTracker(s.vchan_id);
      }
      if (s.ordinal > cached_tracker->last_ordinal_seen &&
          !cached_tracker->ordinals.Contains(
              OrdinalAndVchanId{s.ordinal, s.vchan_id})) {
        new_slot = &s;
        break;
      }
      ++next_slot_cursor_;
    }
    if (new_slot == nullptr) {
      if (queue_bitset_fallback_) {
        if (InPlaceSlotQueue *queue =
                GetAvailableSlotQueueAddress(subscriber_id_);
            queue != nullptr) {
          queue->DiscardAll();
        }
        queue_bitset_fallback_ = false;
      }
      if (stable_poll_drain) {
        poll_drain_exhausted_ = true;
      } else {
        next_slot_cache_valid_ = false;
      }
      // If we suppressed newer messages to keep a stable poll-drain snapshot,
      // re-arm the trigger so the next poll/Wait wakes promptly and
      // re-snapshots them. ClearPollFd() during the drain may already have
      // drained the publisher trigger for those messages, so without this a
      // caller that drains until empty and then re-waits (e.g. the bridge
      // transmitter) can block forever on the final message of a batch.
      if (stable_poll_drain &&
          ccb_->total_messages.load(std::memory_order_seq_cst) !=
              next_slot_cached_total_) {
        Trigger();
      }
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
      const int buffer_index =
          new_slot->slot->buffer_index.load(std::memory_order_relaxed);
      if (!ValidateSlotBuffer(new_slot->slot) || buffer_index == -1) {
        if (print_errors) {
          std::cerr << "Subscriber for " << Name()
                    << " detected buffer failure on slot: "
                    << new_slot->slot->id
                    << " buffer index: " << buffer_index;
          new_slot->slot->Dump(std::cerr);
        }
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again. The cache is now stale wrt
        // the embargo; force a rebuild on the next iteration so subsequent
        // NextSlot() calls don't permanently skip past the embargoed entry.
        embargoed_slots_.Set(new_slot->slot->id);
        AtomicIncRefCount(new_slot->slot, reliable, -1, new_slot->ordinal,
                          new_slot->vchan_id, false);
        if (stable_poll_drain) {
          ++next_slot_cursor_;
        } else {
          next_slot_cache_valid_ = false;
        }
        continue;
      }
      // Successful claim. Advance the cursor so the next NextSlot() call
      // picks up the next ordinal in the cached, sorted list.
      ++next_slot_cursor_;
      return new_slot->slot;
    }
    // CAS failed: another subscriber raced us, or the slot was retired and
    // overwritten with a new ordinal. Drop the cache and re-snapshot.
    if (stable_poll_drain) {
      ++next_slot_cursor_;
    } else {
      next_slot_cache_valid_ = false;
    }
  }
  return nullptr;
}

MessageSlot *SubscriberImpl::LastSlot(MessageSlot *slot, bool reliable,
                                      int owner) {

  embargoed_slots_.ClearAll();
  for (;;) {
    CheckReload();
    InPlaceAtomicBitset &bits = GetAvailableSlots(owner);
    if (!reliable && SubscriberQueueSize() > 0) {
      if (InPlaceSlotQueue *queue =
              GetAvailableSlotQueueAddress(subscriber_id_);
          queue != nullptr) {
        const uint32_t queue_drops = queue->ConsumeOverflow();
        if (options_.DetectDroppedMessages()) {
          pending_queue_drops_ += static_cast<int>(queue_drops);
        }
        queue->ConsumeInsertionFailure();
      }
    }
    if (slot == nullptr) {
      // Prepopulate the active slots.
      PopulateActiveSlots(bits);
    }
    (void)CollectVisibleSlots(bits);

    // Sort the active slots by timestamp.
    std::sort(active_slots_.begin(), active_slots_.end(), ActiveSlotLess);
    newest_snapshot_ = active_slots_;

    ActiveSlot *new_slot = nullptr;
    if (!active_slots_.empty()) {
      new_slot = &active_slots_.back();

      if (slot != nullptr && slot == new_slot->slot) {
        // Same slot, nothing changes.
        new_slot = nullptr;
      }
    }
    if (new_slot == nullptr) {
      newest_snapshot_.clear();
      return nullptr;
    }

    // Increment the ref count.
    if (AtomicIncRefCount(new_slot->slot, reliable, 1, new_slot->ordinal,
                          new_slot->vchan_id, false)) {
      const int buffer_index =
          new_slot->slot->buffer_index.load(std::memory_order_relaxed);
      if (!ValidateSlotBuffer(new_slot->slot) || buffer_index == -1) {
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots_.Set(new_slot->slot->id);
        AtomicIncRefCount(new_slot->slot, reliable, -1, new_slot->ordinal,
                          new_slot->vchan_id, false);
        continue;
      }
      return new_slot->slot;
    }
    newest_snapshot_.clear();
  }
}

MessageSlot *SubscriberImpl::FindActiveSlotByTimestamp(
    [[maybe_unused]] MessageSlot *old_slot, uint64_t timestamp, bool reliable, int owner,
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
      uint64_t refs = s->refs.load(std::memory_order_acquire);
      const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
      if (ordinal != 0 && (refs & kPubOwned) == 0) {
        buffer.push_back(
            {s, ordinal, Prefix(s)->timestamp,
             s->vchan_id.load(std::memory_order_relaxed)});
      }
    }
    // Sort by timestamp.
    std::sort(buffer.begin(), buffer.end(), ActiveSlotLess);

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
    if (it->timestamp != timestamp) {
      return nullptr;
    }

    // Try to increment the ref count.
    if (AtomicIncRefCount(it->slot, reliable, 1, it->ordinal, it->vchan_id,
                          false)) {
      if (!ValidateSlotBuffer(it->slot) ||
          it->slot->buffer_index.load(std::memory_order_relaxed) == -1) {
        // Failed to get a buffer for the slot.  Embargo the slot so we don't
        // see it again this loop and try again.
        embargoed_slots_.Set(it->slot->id);
        AtomicIncRefCount(it->slot, reliable, -1, it->ordinal, it->vchan_id,
                          false);
        continue;
      }
      it->slot->flags.fetch_or(kMessageSeen, std::memory_order_relaxed);
      if (reliable) {
        it->slot->flags.fetch_or(kMessageSeenByReliable,
                                 std::memory_order_relaxed);
      }
      it->slot->sub_owners.Set(owner);
      return it->slot;
    }
    // Publisher got there first, try again.
  }
}
} // namespace details
} // namespace subspace
