// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Publishing a message into a slot, shared by every client.

#pragma once

#include "common/atomic_bitset.h"
#include "common/bitset_view.h"
#include "common/channel_view.h"
#include "common/shm_layout.h"

#include <atomic>
#include <cstdint>

namespace subspace {

// A publisher counts itself in active_publishers while it may write to
// subscriber queues.  The server does not reuse a retired queue while a
// publisher that could still see it is counted.
inline void BeginSubscriberQueuePublish(AvailableSlotQueueIndex *index,
                                        int publisher_id) {
  index->active_publishers[publisher_id].fetch_add(1,
                                                   std::memory_order_seq_cst);
}

inline void EndSubscriberQueuePublish(AvailableSlotQueueIndex *index,
                                      int publisher_id) {
  // The server zeroes the count when it removes the publisher, so never
  // decrement past zero.
  std::atomic<uint32_t> &counter = index->active_publishers[publisher_id];
  uint32_t active = counter.load(std::memory_order_seq_cst);
  while (active != 0 &&
         !counter.compare_exchange_strong(active, active - 1,
                                          std::memory_order_seq_cst,
                                          std::memory_order_seq_cst)) {
  }
}

// Counts a publisher in active_publishers for its lifetime.
class SubscriberQueuePublishScope {
public:
  SubscriberQueuePublishScope(AvailableSlotQueueIndex *index, int publisher_id)
      : index_(index), publisher_id_(publisher_id) {
    BeginSubscriberQueuePublish(index_, publisher_id_);
  }
  ~SubscriberQueuePublishScope() {
    EndSubscriberQueuePublish(index_, publisher_id_);
  }
  SubscriberQueuePublishScope(const SubscriberQueuePublishScope &) = delete;
  SubscriberQueuePublishScope &
  operator=(const SubscriberQueuePublishScope &) = delete;

private:
  AvailableSlotQueueIndex *index_;
  int publisher_id_;
};

// Gives the message the publisher wrote into slot the next ordinal on the
// slot's virtual channel.
inline void StampSlot(ChannelControlBlock *ccb, MessageSlot *slot,
                      uint64_t timestamp) {
  slot->ordinal.store(
      ccb->ordinals.Next(slot->vchan_id.load(std::memory_order_relaxed)),
      std::memory_order_relaxed);
  slot->timestamp.store(timestamp, std::memory_order_relaxed);
  slot->flags.store(0, std::memory_order_relaxed);
}

// Fills in the prefix of a stamped slot.  The caller adds any other flags,
// then the checksum.
inline void FillPrefix(ChannelControlBlock *ccb, MessageSlot *slot,
                       MessagePrefix *prefix, int32_t checksum_size,
                       int32_t metadata_size, bool is_activation) {
  prefix->message_size = slot->message_size.load(std::memory_order_relaxed);
  prefix->ordinal = slot->ordinal.load(std::memory_order_relaxed);
  prefix->timestamp = slot->timestamp.load(std::memory_order_relaxed);
  prefix->vchan_id = slot->vchan_id.load(std::memory_order_relaxed);
  prefix->checksum_size = static_cast<uint16_t>(checksum_size);
  prefix->metadata_size = static_cast<uint16_t>(metadata_size);
  prefix->flags = 0;
  prefix->slot_id = slot->id;
  slot->bridged_slot_id.store(slot->id, std::memory_order_relaxed);
  if (is_activation) {
    prefix->SetIsActivation();
    slot->flags.fetch_or(kMessageIsActivation, std::memory_order_relaxed);
    ccb->activation_tracker.Activate(
        slot->vchan_id.load(std::memory_order_relaxed));
  }
}

struct PublishedSlot {
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
  // The slot to name to retirement listeners.  For a bridged message this is
  // the slot on the other side of the bridge.
  int32_t retirement_slot_id = -1;
  // Whether the commit retired the slot because a subscriber left while it
  // was being published.  The caller tells retirement listeners.
  bool retired = false;
};

// Makes the stamped message in slot, which the publisher owns, available to
// the subscribers on vchan_id and then readable.  `publishing` must count the
// publisher, because the commit writes to subscriber queues.
inline PublishedSlot
CommitPublishedSlot(const ChannelView &channel, MessageSlot *slot,
                    int vchan_id, bool is_activation,
                    const SubscriberQueuePublishScope & /*publishing*/) {
  ChannelControlBlock *ccb = channel.Ccb();
  PublishedSlot published;
  published.ordinal = slot->ordinal.load(std::memory_order_relaxed);
  published.timestamp = slot->timestamp.load(std::memory_order_relaxed);
  published.retirement_slot_id =
      slot->bridged_slot_id.load(std::memory_order_relaxed);
  const uint64_t cleanup_generation = channel.CleanupGeneration(vchan_id);

  // Tell every subscriber that the slot is available while it is still
  // publisher owned.  The kPubOwned bit is the commit barrier: subscribers
  // keep the delivery record but cannot claim the slot, and server cleanup
  // cannot retire it, until the refs store below.
  //
  // The available-slot bitset is authoritative when a queue rejects or evicts
  // a hint.  A subscriber can leave after the traversal sees its bit, so check
  // membership again after setting the delivery bit.  Either the recheck
  // clears a stale write, or the server's later ClearWasSet sees it.
  const BitsetView subscribers = channel.Subscribers();
  AtomicBitSet<kMaxSlotOwners> failed_queues;
  subscribers.Traverse(
      [&](int sub_id) {
        if (!VirtualChannelsMatch(ccb->sub_vchan_ids[sub_id], vchan_id)) {
          return;
        }
        BitsetView available = channel.AvailableSlots(sub_id);
        available.Set(slot->id);
        if (!subscribers.IsSetSeqCst(sub_id)) {
          available.Clear(slot->id);
          // The subscriber ID may have been reused after the first check.
          // Registration publishes membership before seeding this bit, so
          // restore the bit if the new subscriber is already visible.
          if (!subscribers.IsSetSeqCst(sub_id)) {
            return;
          }
          available.Set(slot->id);
        }
        InPlaceSlotQueue *queue = channel.Queue(sub_id);
        if (queue != nullptr &&
            !queue->Push(slot->id, published.ordinal,
                         /*report_insertion_failure=*/false)) {
          failed_queues.Set(sub_id);
        }
      },
      std::memory_order_seq_cst);

  // Finish the slot and queue bookkeeping before making the slot claimable.
  if (!is_activation) {
    const uint64_t message_size =
        slot->message_size.load(std::memory_order_relaxed);
    ccb->total_bytes += message_size;
    if (message_size > ccb->max_message_size) {
      ccb->max_message_size = message_size;
    }
  }
  // A subscriber that left since its push failed has no queue to mark.  A new
  // subscriber with the same ID that is marked only rescans its bitset.
  failed_queues.Traverse([&channel](size_t sub_id) {
    InPlaceSlotQueue *queue = channel.Queue(static_cast<int>(sub_id));
    if (queue != nullptr) {
      queue->MarkInsertionFailure();
    }
  });

  // Commit.  A subscriber that sees the total_messages increment also sees
  // this release and every delivery record above.  Subscribers use
  // total_messages as a version stamp for their delivery snapshots.
  slot->refs.store(BuildRefsBitField(published.ordinal, vchan_id, 0),
                   std::memory_order_release);
  ccb->total_messages.fetch_add(1, std::memory_order_seq_cst);

  // Subscriber removal and the commit race safely: whichever happens second
  // re-evaluates retirement with the current subscriber count.
  if (!is_activation &&
      channel.CleanupGeneration(vchan_id) != cleanup_generation) {
    published.retired = channel.TryRetireSlot(slot);
  }
  return published;
}

} // namespace subspace
