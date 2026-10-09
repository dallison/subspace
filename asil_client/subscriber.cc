// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// The reading side of the shared memory protocol.  This follows
// SubscriberImpl::NextSlot, LastSlot, ClaimSlot and RemoveActiveMessage in
// client/subscriber.cc, using the subscriber's available-slot bitset as the
// record of unread messages.  A subscriber queue, when the channel has one,
// only holds hints, so the subscriber leaves it to the publishers.

#include "asil_client/subscriber.h"

#include "asil_client/checksum.h"
#include "asil_client/client.h"

#include <cstring>

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

bool ValidVchanId(int vchan_id) {
  return vchan_id >= -1 && vchan_id < shm::kMaxVchanId;
}

} // namespace

SubscriberRequest Subscriber::BuildRequest() const {
  SubscriberRequest request;
  request.channel_name = name_;
  request.type = options_.type;
  request.max_active_messages = options_.max_active_messages;
  request.reliable = options_.reliable;
  request.mux = options_.mux;
  request.vchan_id = options_.vchan_id;
  return request;
}

Error Subscriber::Open(Client &client, const char *channel_name,
                       const SubscriberOptions &options) {
  if (client_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  if (!client.Initialized()) {
    return Error::kNotInitialized;
  }
  if (options.max_active_messages < 1 ||
      options.max_active_messages > kMaxActiveMessages ||
      !ValidVchanId(options.vchan_id)) {
    return Error::kInvalidArgument;
  }
  if (Error e = internal::CopyName(channel_name, name_); e != Error::kOk) {
    return e;
  }
  if (Error e = internal::CopyName(
          options.mux != nullptr ? options.mux : channel_name, resolved_name_);
      e != Error::kOk) {
    return e;
  }
  options_ = options;
  SubscriberReply reply;
  reply.reliable_publisher_triggers = &reliable_publishers_;
  reply.retirement_triggers = &retirement_triggers_;
  if (Error e = client.connection_->CreateSubscriber(BuildRequest(), reply);
      e != Error::kOk) {
    Reset();
    return e;
  }
  client_ = &client;
  channel_id_ = reply.channel_id;
  subscriber_id_ = reply.subscriber_id;
  vchan_id_ = reply.vchan_id;
  num_pub_updates_ = static_cast<uint16_t>(reply.num_pub_updates);
  trigger_ = static_cast<UniqueFd &&>(reply.trigger);
  poll_ = static_cast<UniqueFd &&>(reply.poll);
  buffer_context_.channel_name = resolved_name_;
  buffer_context_.session_id = client.session_id_;
  buffer_context_.user_id = client.user_id_;
  buffer_context_.group_id = client.group_id_;
  buffer_context_.connection = client.connection_;

  Error e = Error::kOk;
  if (channel_id_ < 0 || channel_id_ >= shm::kMaxChannels ||
      subscriber_id_ < 0 || subscriber_id_ >= shm::kMaxSlotOwners ||
      !ValidVchanId(vchan_id_)) {
    e = Error::kProtocolError;
  }
  if (e == Error::kOk) {
    // A channel without a layout has no buffers either.
    e = reply.num_slots > 0 ? MapChannel(reply) : Error::kNoBuffers;
  }
  if (e != Error::kOk) {
    (void)Close();
    return e;
  }
  // Wake the reader for messages published before it joined.
  internal::Trigger(trigger_.Get());
  return Error::kOk;
}

Error Subscriber::MapChannel(SubscriberReply &reply) {
  if (Error e =
          memory_.Map(client_->scb_, reply.ccb.Get(), reply.bcb.Get(),
                      reply.num_slots, reply.checksum_size, reply.metadata_size,
                      reply.subscriber_queue_arena_size, &buffer_context_);
      e != Error::kOk) {
    return e;
  }
  if (reply.use_split_buffers) {
    return memory_.AttachSplitBuffers(
        /*writable=*/false, options_.split_slots, options_.split_slot_capacity,
        options_.split_allocator);
  }
  return memory_.AttachBuffer(/*writable=*/false);
}

void Subscriber::Reset() {
  memory_.Unmap();
  reliable_publishers_.Clear();
  retirement_triggers_.Clear();
  trigger_.Reset();
  poll_.Reset();
  client_ = nullptr;
  num_held_ = 0;
  for (uint64_t &ordinal : last_ordinals_) {
    ordinal = 0;
  }
  channel_id_ = -1;
  subscriber_id_ = -1;
  vchan_id_ = -1;
}

Error Subscriber::Close() {
  if (client_ == nullptr) {
    return Error::kOk;
  }
  ReleaseMessage();
  const Error e = client_->connection_->RemoveSubscriber(name_, subscriber_id_);
  Reset();
  return e;
}

Error Subscriber::Wait(int timeout_ms) {
  if (client_ == nullptr) {
    return Error::kNotInitialized;
  }
  return internal::WaitReadable(poll_.Get(), timeout_ms);
}

void Subscriber::ReleaseHeld(int index) {
  ReleaseSlot(held_[index].slot);
  held_[index] = held_[--num_held_];
  held_[num_held_] = Held();
}

void Subscriber::ReleaseMessage() {
  while (num_held_ > 0) {
    ReleaseHeld(num_held_ - 1);
  }
}

Error Subscriber::ReleaseMessage(const Message &message) {
  for (int i = 0; i < num_held_; i++) {
    if (held_[i].slot->id == message.slot_id &&
        held_[i].ordinal == message.ordinal) {
      ReleaseHeld(i);
      return Error::kOk;
    }
  }
  return Error::kInvalidArgument;
}

void Subscriber::ReleaseSlot(shm::MessageSlot *slot) {
  if (!Owners(slot).ClearWasSet(subscriber_id_)) {
    return;
  }
  bool retired = false;
  (void)memory_.AtomicIncRefCount(
      slot, options_.reliable, -1,
      slot->ordinal.load(std::memory_order_relaxed),
      slot->vchan_id.load(std::memory_order_relaxed), /*retire=*/true,
      &retired);
  if (retired) {
    memory_.TriggerRetirement(
        retirement_triggers_,
        slot->bridged_slot_id.load(std::memory_order_relaxed));
  }
  if (options_.reliable) {
    // A reliable publisher may be waiting for this slot.
    TriggerReliablePublishers();
  }
}

// Fetches new trigger fds when publishers have changed.
Error Subscriber::RefreshTriggers() {
  const uint16_t updates = client_->scb_->counters[channel_id_].num_pub_updates;
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

// Whether slot holds a published message for this subscriber's virtual
// channel.  A subscriber to a multiplexer sees every virtual channel.
bool Subscriber::Visible(const shm::MessageSlot *slot) const {
  if ((slot->refs.load(std::memory_order_acquire) & shm::kPubOwned) != 0 ||
      slot->ordinal.load(std::memory_order_relaxed) == 0) {
    return false;
  }
  const int slot_vchan_id = slot->vchan_id.load(std::memory_order_relaxed);
  return vchan_id_ == -1 || slot_vchan_id == -1 || slot_vchan_id == vchan_id_;
}

// Takes a reference to slot and records it so that the server can release
// the reference if this process dies.
bool Subscriber::Claim(shm::MessageSlot *slot, uint64_t ordinal) {
  if (!memory_.AtomicIncRefCount(slot, options_.reliable, 1, ordinal,
                                 slot->vchan_id.load(std::memory_order_relaxed),
                                 false, nullptr)) {
    return false;
  }
  Owners(slot).Set(subscriber_id_);
  return true;
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
      if (!Visible(s)) {
        return;
      }
      const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
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
    if (Claim(best, best_ordinal)) {
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
      if (!Visible(s)) {
        return;
      }
      const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
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
    if (Claim(best, best_ordinal)) {
      slot = best;
      return Find::kFound;
    }
  }
  return Find::kNone;
}

// Marks every unread message older than newest as read.  Each slot is pinned
// while its bit is cleared so that a publisher recycling it can't lose the
// bit for the new message.  A reliable subscriber also marks the messages
// seen so that a reliable publisher doesn't wait for it to read them.
void Subscriber::ClearOlder(const shm::MessageSlot *newest) {
  BitsetView available = memory_.AvailableSlots(subscriber_id_);
  const uint64_t newest_ordinal =
      newest->ordinal.load(std::memory_order_relaxed);
  const uint64_t newest_timestamp =
      newest->timestamp.load(std::memory_order_relaxed);
  bool marked_seen = false;
  available.Traverse([&](int i) {
    if (i == newest->id) {
      return;
    }
    shm::MessageSlot *s = memory_.Slot(i);
    if (!Visible(s)) {
      return;
    }
    const uint64_t ordinal = s->ordinal.load(std::memory_order_relaxed);
    const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
    if (!Before(timestamp, ordinal, newest_timestamp, newest_ordinal)) {
      return;
    }
    const int vchan_id = s->vchan_id.load(std::memory_order_relaxed);
    if (memory_.AtomicIncRefCount(s, options_.reliable, 1, ordinal, vchan_id,
                                  false, nullptr)) {
      available.Clear(i);
      if (options_.reliable) {
        s->flags.fetch_or(shm::kMessageSeen | shm::kMessageSeenByReliable,
                          std::memory_order_relaxed);
        marked_seen = true;
      }
      (void)memory_.AtomicIncRefCount(s, options_.reliable, -1, ordinal,
                                      vchan_id, false, nullptr);
    }
  });
  if (marked_seen) {
    TriggerReliablePublishers();
  }
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
  const bool single = options_.max_active_messages == 1;
  // A reliable subscriber keeps its last message until it has the next one,
  // which stops a reliable publisher overtaking it.
  const bool keep_previous = single && options_.reliable;
  if (single && !keep_previous) {
    ReleaseMessage();
  } else if (!single && num_held_ >= options_.max_active_messages) {
    return Error::kActiveMessageLimit;
  }
  if (Error e = RefreshTriggers(); e != Error::kOk) {
    return e;
  }
  BitsetView available = memory_.AvailableSlots(subscriber_id_);
  for (int attempt = 0; attempt < kMaxClaimAttempts; attempt++) {
    shm::MessageSlot *slot = nullptr;
    const Find found =
        mode == ReadMode::kReadNext ? FindNext(slot) : FindNewest(slot);
    if (found == Find::kNone) {
      // Out of messages: tell reliable publishers there is room.
      TriggerReliablePublishers();
      return Error::kOk;
    }
    const uint64_t ordinal = slot->ordinal.load(std::memory_order_relaxed);
    const size_t length =
        static_cast<size_t>(slot->message_size.load(std::memory_order_relaxed));
    const int vchan_id = slot->vchan_id.load(std::memory_order_relaxed);
    const bool mapped = memory_.InMappedBuffer(slot);

    if (mode == ReadMode::kReadNewest) {
      ClearOlder(slot);
    }
    available.Clear(slot->id);
    slot->flags.fetch_or(options_.reliable
                             ? shm::kMessageSeen | shm::kMessageSeenByReliable
                             : shm::kMessageSeen,
                         std::memory_order_relaxed);

    uint64_t unused_last_ordinal = 0;
    uint64_t &last_ordinal = ValidVchanId(vchan_id)
                                 ? last_ordinals_[vchan_id + 1]
                                 : unused_last_ordinal;
    if (!mapped) {
      if (ordinal > last_ordinal) {
        last_ordinal = ordinal;
      }
      ReleaseSlot(slot);
      return Error::kBufferNotMapped;
    }
    const shm::MessagePrefix *prefix = memory_.Prefix(slot);
    const bool is_activation = (prefix->flags & shm::kMessageActivate) != 0;
    if (length == 0 || (is_activation && !options_.pass_activation)) {
      if (ordinal > last_ordinal) {
        last_ordinal = ordinal;
      }
      ReleaseSlot(slot);
      continue;
    }
    if (mode == ReadMode::kReadNext && last_ordinal != 0 &&
        ordinal > last_ordinal + 1) {
      message.dropped = static_cast<uint32_t>(ordinal - last_ordinal - 1);
      memory_.Ccb()->total_drops += message.dropped;
    }
    if (ordinal > last_ordinal) {
      last_ordinal = ordinal;
    }
    const bool checksum_error =
        options_.checksum && (prefix->flags & shm::kMessageHasChecksum) != 0 &&
        !ChecksumValid(slot, length);
    if (checksum_error && !options_.pass_checksum_errors) {
      ReleaseSlot(slot);
      message = Message();
      return Error::kChecksumMismatch;
    }
    if (keep_previous) {
      ReleaseMessage();
    }
    held_[num_held_++] = Held{slot, ordinal};
    message.data = memory_.Payload(slot);
    message.length = length;
    message.ordinal = ordinal;
    message.timestamp = slot->timestamp.load(std::memory_order_relaxed);
    message.slot_id = slot->id;
    message.vchan_id = vchan_id;
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
