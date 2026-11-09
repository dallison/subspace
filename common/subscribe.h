// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

// The steps of reading a message that every client shares: taking and
// releasing a reference, recording the subscriber as an owner, and marking a
// message seen.

#pragma once

#include "common/bitset_view.h"
#include "common/channel_view.h"
#include "common/shm_layout.h"

#include <atomic>
#include <cstdint>

namespace subspace {

// Messages are delivered in timestamp order, then ordinal order.
inline bool DeliveredBefore(uint64_t timestamp, uint64_t ordinal,
                            uint64_t other_timestamp, uint64_t other_ordinal) {
  if (timestamp != other_timestamp) {
    return timestamp < other_timestamp;
  }
  return ordinal < other_ordinal;
}

// The ordinal of the published message in slot, or 0 if the slot holds none
// or a publisher owns it.  A publisher reusing the slot zeroes its ordinal and
// timestamp before its bit is cleared, and a claim with ordinal 0 matches
// whatever is published there next, so a slot counts only while it holds the
// message its refs were committed with.
inline uint64_t CommittedOrdinal(const MessageSlot *slot) {
  const uint64_t refs = slot->refs.load(std::memory_order_acquire);
  const uint64_t ordinal = slot->ordinal.load(std::memory_order_relaxed);
  if ((refs & kPubOwned) != 0 || ordinal == 0 ||
      (ordinal & kOrdinalMask) != ((refs >> kOrdinalShift) & kOrdinalMask)) {
    return 0;
  }
  return ordinal;
}

// Takes a reference to the message with this ordinal and vchan_id in slot,
// and records the subscriber as an owner so that the server can release the
// reference if the process dies.  Returns false if the slot no longer holds
// the message.
inline bool ClaimMessageReference(const ChannelView &channel,
                                  MessageSlot *slot, int subscriber_id,
                                  bool reliable, uint64_t ordinal,
                                  int vchan_id) {
  if (!channel.AtomicIncRefCount(slot, reliable, 1, ordinal, vchan_id,
                                 /*retire=*/false, nullptr)) {
    return false;
  }
  slot->sub_owners.Set(subscriber_id);
  return true;
}

// Gives back a reference from ClaimMessageReference for a message the
// subscriber is not delivering, so the message stays unread.
inline void UnclaimMessageReference(const ChannelView &channel,
                                    MessageSlot *slot, int subscriber_id,
                                    bool reliable) {
  if (slot->sub_owners.ClearWasSet(subscriber_id)) {
    // A publisher evicting a queued slot can clear its bit just before the
    // subscriber pops it.  Restore the bit while the slot is still held.
    channel.AvailableSlots(subscriber_id).Set(slot->id);
    (void)channel.AtomicIncRefCount(
        slot, reliable, -1, slot->ordinal.load(std::memory_order_relaxed),
        slot->vchan_id.load(std::memory_order_relaxed), /*retire=*/false,
        nullptr);
  }
}

struct ReleasedMessage {
  // The subscriber held a reference to the message.
  bool released = false;
  // The release retired the slot.  The caller tells retirement listeners
  // about retirement_slot_id: for a bridged message, the slot on the other
  // side of the bridge.
  bool retired = false;
  int32_t retirement_slot_id = -1;
};

// Releases the subscriber's reference to the message in slot, which it has
// finished with.
inline ReleasedMessage ReleaseMessageReference(const ChannelView &channel,
                                               MessageSlot *slot,
                                               int subscriber_id,
                                               bool reliable) {
  ReleasedMessage result;
  if (!slot->sub_owners.ClearWasSet(subscriber_id)) {
    return result;
  }
  result.released = true;
  (void)channel.AtomicIncRefCount(
      slot, reliable, -1, slot->ordinal.load(std::memory_order_relaxed),
      slot->vchan_id.load(std::memory_order_relaxed), /*retire=*/true,
      &result.retired);
  if (result.retired) {
    result.retirement_slot_id =
        slot->bridged_slot_id.load(std::memory_order_relaxed);
  }
  return result;
}

// A reliable publisher doesn't overtake a message until a reliable subscriber
// has seen it.
inline void MarkMessageSeen(MessageSlot *slot, bool reliable) {
  slot->flags.fetch_or(reliable ? kMessageSeen | kMessageSeenByReliable
                                : kMessageSeen,
                       std::memory_order_relaxed);
}

// Marks a message that the subscriber is skipping, because it read a newer
// one, as read.  The reference held meanwhile stops a publisher recycling the
// slot from losing the bit for its new message.  A reliable subscriber marks
// the message seen, so that a reliable publisher doesn't wait for it to be
// read; the caller then triggers reliable publishers.  Returns false if the
// slot no longer holds the message.
inline bool SkipUnreadMessage(const ChannelView &channel, MessageSlot *slot,
                              int subscriber_id, bool reliable,
                              uint64_t ordinal, int vchan_id) {
  if (!channel.AtomicIncRefCount(slot, reliable, 1, ordinal, vchan_id,
                                 /*retire=*/false, nullptr)) {
    return false;
  }
  channel.AvailableSlots(subscriber_id).Clear(slot->id);
  if (reliable) {
    MarkMessageSeen(slot, reliable);
  }
  (void)channel.AtomicIncRefCount(slot, reliable, -1, ordinal, vchan_id,
                                  /*retire=*/false, nullptr);
  return true;
}

} // namespace subspace
