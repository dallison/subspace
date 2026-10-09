// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// The reading side of the shared memory protocol.  This follows
// SubscriberImpl::NextSlot, LastSlot, ClaimSlot and RemoveActiveMessage in
// client/subscriber.cc, using the subscriber's available-slot bitset as the
// record of unread messages.

#include "asil_client/subscriber.h"

#include "asil_client/checksum.h"
#include "asil_client/client.h"

#include <cerrno>
#include <cstring>
#include <poll.h>

namespace subspace {
namespace asil {

namespace {

constexpr int kMaxClaimAttempts = 1000;

// Messages are ordered by timestamp, then ordinal.
bool Before(uint64_t timestamp, uint64_t ordinal, uint64_t other_timestamp,
            uint64_t other_ordinal) {
  if (timestamp != other_timestamp) {
    return timestamp < other_timestamp;
  }
  return ordinal < other_ordinal;
}

BitsetView Owners(shm::MessageSlot *slot) {
  return BitsetView(&slot->sub_owners, shm::kMaxSlotOwners);
}

} // namespace

Error Subscriber::Open(Client &client, const char *channel_name,
                       const SubscriberOptions &options) {
  if (client_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  if (!client.Initialized()) {
    return Error::kNotInitialized;
  }
  if (Error e = internal::CopyName(channel_name, name_); e != Error::kOk) {
    return e;
  }
  SubscriberRequest request;
  request.channel_name = name_;
  request.type = options.type;
  request.max_active_messages = 1;
  SubscriberReply reply;
  reply.reliable_publisher_triggers = &reliable_publishers_;
  reply.retirement_triggers = &retirement_triggers_;
  if (Error e = client.connection_->CreateSubscriber(request, reply);
      e != Error::kOk) {
    Reset();
    return e;
  }
  client_ = &client;
  options_ = options;
  channel_id_ = reply.channel_id;
  subscriber_id_ = reply.subscriber_id;
  num_pub_updates_ = static_cast<uint16_t>(reply.num_pub_updates);
  trigger_ = static_cast<UniqueFd &&>(reply.trigger);
  poll_ = static_cast<UniqueFd &&>(reply.poll);

  Error e = Error::kOk;
  if (reply.vchan_id != -1 || reply.use_split_buffers ||
      reply.subscriber_queue_size != 0 ||
      reply.subscriber_queue_arena_size != 0 || channel_id_ < 0 ||
      channel_id_ >= shm::kMaxChannels || subscriber_id_ < 0 ||
      subscriber_id_ >= shm::kMaxSlotOwners) {
    e = Error::kUnsupported;
  }
  if (e == Error::kOk) {
    e = memory_.Map(client.scb_, reply.ccb.Get(), reply.bcb.Get(),
                    reply.num_slots, reply.checksum_size, reply.metadata_size);
  }
  if (e == Error::kOk) {
    e = memory_.AttachBuffer(name_, client.session_id_);
  }
  if (e != Error::kOk) {
    (void)Close();
    return e;
  }
  // Wake the reader for messages published before it joined.
  internal::Trigger(trigger_.Get());
  return Error::kOk;
}

void Subscriber::Reset() {
  memory_.Unmap();
  reliable_publishers_.Clear();
  retirement_triggers_.Clear();
  trigger_.Reset();
  poll_.Reset();
  client_ = nullptr;
  held_ = nullptr;
  last_ordinal_ = 0;
  channel_id_ = -1;
  subscriber_id_ = -1;
}

Error Subscriber::Close() {
  if (client_ == nullptr) {
    return Error::kOk;
  }
  ReleaseMessage();
  const Error e =
      client_->connection_->RemoveSubscriber(name_, subscriber_id_);
  Reset();
  return e;
}

Error Subscriber::Wait(int timeout_ms) {
  if (client_ == nullptr) {
    return Error::kNotInitialized;
  }
  struct pollfd fd = {poll_.Get(), POLLIN, 0};
  for (;;) {
    const int n = ::poll(&fd, 1, timeout_ms < 0 ? -1 : timeout_ms);
    if (n > 0) {
      return Error::kOk;
    }
    if (n == 0) {
      return Error::kTimeout;
    }
    if (errno != EINTR) {
      return Error::kInvalidArgument;
    }
  }
}

void Subscriber::ReleaseMessage() {
  if (held_ != nullptr) {
    ReleaseSlot(held_);
    held_ = nullptr;
  }
}

void Subscriber::ReleaseSlot(shm::MessageSlot *slot) {
  if (!Owners(slot).ClearWasSet(subscriber_id_)) {
    return;
  }
  bool retired = false;
  (void)memory_.AtomicIncRefCount(
      slot, -1, slot->ordinal.load(std::memory_order_relaxed),
      slot->vchan_id.load(std::memory_order_relaxed), /*retire=*/true,
      &retired);
  if (retired) {
    memory_.TriggerRetirement(
        retirement_triggers_,
        slot->bridged_slot_id.load(std::memory_order_relaxed));
  }
}

Error Subscriber::RefreshTriggers() {
  const uint16_t updates = memory_.Counters(channel_id_).num_pub_updates;
  if (updates == num_pub_updates_) {
    return Error::kOk;
  }
  TriggersReply reply;
  reply.reliable_publisher_triggers = &reliable_publishers_;
  reply.retirement_triggers = &retirement_triggers_;
  const Error e = client_->connection_->GetTriggers(name_, reply);
  if (e == Error::kOk) {
    num_pub_updates_ = updates;
  }
  return e;
}

void Subscriber::TriggerReliablePublishers() const {
  for (int i = 0; i < reliable_publishers_.Size(); i++) {
    internal::Trigger(reliable_publishers_.Get(i));
  }
}

// Finds the oldest unread message and takes a reference to it.
Subscriber::Find Subscriber::FindNext(shm::MessageSlot *&slot) {
  BitsetView available = memory_.AvailableSlots(subscriber_id_);
  for (int attempt = 0; attempt < kMaxClaimAttempts; attempt++) {
    shm::MessageSlot *best = nullptr;
    uint64_t best_ordinal = 0;
    uint64_t best_timestamp = 0;
    available.Traverse([&](int i) {
      shm::MessageSlot *s = memory_.Slot(i);
      if ((s->refs.load(std::memory_order_acquire) & shm::kPubOwned) != 0) {
        return;
      }
      const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
      if (ordinal == 0 ||
          s->buffer_index.load(std::memory_order_relaxed) != 0) {
        return;
      }
      const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
      if (best == nullptr ||
          Before(timestamp, ordinal, best_timestamp, best_ordinal)) {
        best = s;
        best_ordinal = ordinal;
        best_timestamp = timestamp;
      }
    });
    if (best == nullptr) {
      return Find::kNone;
    }
    if (memory_.AtomicIncRefCount(
            best, 1, best_ordinal,
            best->vchan_id.load(std::memory_order_relaxed), false, nullptr)) {
      // Record ownership so that the server can release the reference if this
      // process dies.
      Owners(best).Set(subscriber_id_);
      slot = best;
      return Find::kFound;
    }
    // A publisher reclaimed the slot.  Look again.
  }
  return Find::kNone;
}

// Finds the newest unread message and takes a reference to it.
Subscriber::Find Subscriber::FindNewest(shm::MessageSlot *&slot) {
  BitsetView available = memory_.AvailableSlots(subscriber_id_);
  for (int attempt = 0; attempt < kMaxClaimAttempts; attempt++) {
    shm::MessageSlot *best = nullptr;
    uint64_t best_ordinal = 0;
    uint64_t best_timestamp = 0;
    available.Traverse([&](int i) {
      shm::MessageSlot *s = memory_.Slot(i);
      if ((s->refs.load(std::memory_order_acquire) & shm::kPubOwned) != 0) {
        return;
      }
      const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
      if (ordinal == 0 ||
          s->buffer_index.load(std::memory_order_relaxed) != 0) {
        return;
      }
      const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
      if (best == nullptr ||
          Before(best_timestamp, best_ordinal, timestamp, ordinal)) {
        best = s;
        best_ordinal = ordinal;
        best_timestamp = timestamp;
      }
    });
    if (best == nullptr) {
      return Find::kNone;
    }
    if (memory_.AtomicIncRefCount(
            best, 1, best_ordinal,
            best->vchan_id.load(std::memory_order_relaxed), false, nullptr)) {
      Owners(best).Set(subscriber_id_);
      slot = best;
      return Find::kFound;
    }
  }
  return Find::kNone;
}

// Marks every unread message older than newest as read.  Each slot is pinned
// while its bit is cleared so that a publisher recycling it can't lose the
// bit for the new message.
void Subscriber::ClearOlder(const shm::MessageSlot *newest) {
  BitsetView available = memory_.AvailableSlots(subscriber_id_);
  const uint64_t newest_ordinal =
      newest->ordinal.load(std::memory_order_relaxed);
  const uint64_t newest_timestamp =
      newest->timestamp.load(std::memory_order_relaxed);
  available.Traverse([&](int i) {
    if (i == newest->id) {
      return;
    }
    shm::MessageSlot *s = memory_.Slot(i);
    if ((s->refs.load(std::memory_order_acquire) & shm::kPubOwned) != 0) {
      return;
    }
    const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
    const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
    if (ordinal == 0 ||
        !Before(timestamp, ordinal, newest_timestamp, newest_ordinal)) {
      return;
    }
    const int vchan_id = s->vchan_id.load(std::memory_order_relaxed);
    if (memory_.AtomicIncRefCount(s, 1, ordinal, vchan_id, false, nullptr)) {
      available.Clear(i);
      (void)memory_.AtomicIncRefCount(s, -1, ordinal, vchan_id, false,
                                      nullptr);
    }
  });
}

bool Subscriber::ChecksumValid(const shm::MessageSlot *slot,
                               size_t length) const {
  const shm::MessagePrefix *prefix = memory_.Prefix(slot);
  const auto *base = reinterpret_cast<const uint8_t *>(prefix);
  uint32_t crc = 0xFFFFFFFF;
  crc = Crc32(crc, base + offsetof(shm::MessagePrefix, slot_id),
              offsetof(shm::MessagePrefix, checksum) -
                  offsetof(shm::MessagePrefix, slot_id));
  crc = Crc32(crc,
              base + offsetof(shm::MessagePrefix, checksum) +
                  memory_.ChecksumSize(),
              static_cast<size_t>(memory_.MetadataSize()));
  crc = Crc32(crc, reinterpret_cast<const uint8_t *>(memory_.Payload(slot)),
              length);
  uint32_t stored;
  std::memcpy(&stored, &prefix->checksum, sizeof(stored));
  return stored == ~crc;
}

Error Subscriber::ReadMessage(Message &message, ReadMode mode) {
  message = Message();
  if (client_ == nullptr) {
    return Error::kNotInitialized;
  }
  internal::ClearTrigger(poll_.Get());
  ReleaseMessage();
  if (Error e = RefreshTriggers(); e != Error::kOk) {
    return e;
  }
  if (Error e = memory_.AttachBuffer(name_, client_->session_id_);
      e != Error::kOk) {
    return e;
  }
  if (!memory_.HasBuffer()) {
    return Error::kOk;
  }
  BitsetView available = memory_.AvailableSlots(subscriber_id_);
  for (int attempt = 0; attempt < kMaxClaimAttempts; attempt++) {
    shm::MessageSlot *slot = nullptr;
    const Find found = mode == ReadMode::kReadNext ? FindNext(slot)
                                                   : FindNewest(slot);
    if (found == Find::kNone) {
      // Out of messages: tell reliable publishers there is room.
      TriggerReliablePublishers();
      return Error::kOk;
    }
    const shm::MessagePrefix *prefix = memory_.Prefix(slot);
    const uint64_t ordinal = slot->ordinal.load(std::memory_order_relaxed);
    const size_t length = static_cast<size_t>(
        slot->message_size.load(std::memory_order_relaxed));
    const bool is_activation = (prefix->flags & shm::kMessageActivate) != 0;

    if (mode == ReadMode::kReadNewest) {
      ClearOlder(slot);
    }
    available.Clear(slot->id);
    slot->flags.fetch_or(shm::kMessageSeen, std::memory_order_relaxed);

    if (length == 0 || (is_activation && !options_.pass_activation)) {
      if (ordinal > last_ordinal_) {
        last_ordinal_ = ordinal;
      }
      ReleaseSlot(slot);
      continue;
    }
    if (mode == ReadMode::kReadNext && last_ordinal_ != 0 &&
        ordinal > last_ordinal_ + 1) {
      message.dropped = static_cast<uint32_t>(ordinal - last_ordinal_ - 1);
      memory_.Ccb()->total_drops += message.dropped;
    }
    if (ordinal > last_ordinal_) {
      last_ordinal_ = ordinal;
    }
    const bool checksum_error =
        options_.checksum && (prefix->flags & shm::kMessageHasChecksum) != 0 &&
        !ChecksumValid(slot, length);
    if (checksum_error && !options_.pass_checksum_errors) {
      ReleaseSlot(slot);
      message = Message();
      return Error::kChecksumMismatch;
    }
    held_ = slot;
    message.data = memory_.Payload(slot);
    message.length = length;
    message.ordinal = ordinal;
    message.timestamp = slot->timestamp.load(std::memory_order_relaxed);
    message.slot_id = slot->id;
    message.is_activation = is_activation;
    message.checksum_error = checksum_error;
    if (memory_.MetadataSize() > 0) {
      message.metadata = reinterpret_cast<const char *>(&prefix->checksum) +
                         memory_.ChecksumSize();
      message.metadata_length = static_cast<size_t>(memory_.MetadataSize());
    }
    return Error::kOk;
  }
  return Error::kOk;
}

} // namespace asil
} // namespace subspace
