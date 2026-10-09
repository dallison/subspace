// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// The publishing side of the shared memory protocol.  This follows
// PublisherImpl::FindFreeSlotUnreliable, PublisherImpl::FindFreeSlotReliable
// and PublisherImpl::ActivateSlotAndGetAnother in client/publisher.cc.

#include "asil_client/publisher.h"

#include "asil_client/checksum.h"
#include "asil_client/client.h"

#include <cstring>
#include <sched.h>

namespace subspace {
namespace asil {

namespace {

bool ValidLayoutSize(int32_t size) { return size >= 0 && size <= 0xFFFF; }

// Slots are ordered by timestamp, then by id, as a stable sort by timestamp
// orders them.
bool Earlier(uint64_t timestamp, int id, uint64_t other_timestamp,
             int other_id) {
  if (timestamp != other_timestamp) {
    return timestamp < other_timestamp;
  }
  return id < other_id;
}

} // namespace

Error Publisher::Open(Client &client, const char *channel_name,
                      const PublisherOptions &options) {
  if (client_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  if (!client.Initialized()) {
    return Error::kNotInitialized;
  }
  if (options.slot_size <= 0 || options.num_slots <= 0 ||
      !ValidLayoutSize(options.checksum_size) ||
      !ValidLayoutSize(options.metadata_size) || options.vchan_id < -1 ||
      options.vchan_id >= shm::kMaxVchanId) {
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
  const int64_t slot_size = shm::Aligned64(options.slot_size);

  PublisherRequest request;
  request.channel_name = name_;
  request.slot_size = slot_size;
  request.num_slots = options.num_slots;
  request.type = options.type;
  request.checksum_size = options.checksum_size;
  request.metadata_size = options.metadata_size;
  request.reliable = options.reliable;
  request.mux = options.mux;
  request.vchan_id = options.vchan_id;
  request.subscriber_queue_arena_size = options.subscriber_queue_arena_size;
  request.use_split_buffers = options.use_split_buffers;
  PublisherReply reply;
  reply.subscriber_triggers = &subscriber_triggers_;
  reply.retirement_triggers = &retirement_triggers_;
  if (Error e = client.connection_->CreatePublisher(request, reply);
      e != Error::kOk) {
    Reset();
    return e;
  }
  client_ = &client;
  channel_id_ = reply.channel_id;
  publisher_id_ = reply.publisher_id;
  vchan_id_ = reply.vchan_id;
  num_sub_updates_ = static_cast<uint16_t>(reply.num_sub_updates);
  checksum_ = options.checksum;
  reliable_ = options.reliable;
  poll_ = static_cast<UniqueFd &&>(reply.poll);
  buffer_context_.channel_name = resolved_name_;
  buffer_context_.session_id = client.session_id_;
  buffer_context_.user_id = client.user_id_;
  buffer_context_.group_id = client.group_id_;
  buffer_context_.connection = client.connection_;

  Error e = Error::kOk;
  if (channel_id_ < 0 || channel_id_ >= shm::kMaxChannels ||
      publisher_id_ < 0 || publisher_id_ >= shm::kMaxSlotOwners ||
      vchan_id_ < -1 || vchan_id_ >= shm::kMaxVchanId ||
      (vchan_id_ != -1) != (options.mux != nullptr)) {
    e = Error::kProtocolError;
  }
  if (e == Error::kOk) {
    const int num_slots =
        reply.num_slots > 0 ? reply.num_slots : options.num_slots;
    e = memory_.Map(client.scb_, reply.ccb.Get(), reply.bcb.Get(), num_slots,
                    options.checksum_size, options.metadata_size,
                    reply.subscriber_queue_arena_size, &buffer_context_);
  }
  if (e == Error::kOk) {
    e = options.use_split_buffers
            ? memory_.AttachSplitBuffers(
                  /*writable=*/true, options.split_slots,
                  options.split_slot_capacity, options.split_allocator)
            : memory_.CreateOrAttachBuffer(slot_size);
  }
  if (e == Error::kOk && memory_.SlotSize() < slot_size) {
    e = Error::kLayoutMismatch;
  }
  if (e == Error::kOk) {
    slot_ = FindFreeSlot();
    if (slot_ == nullptr) {
      e = Error::kNoSlot;
    }
  }
  if (e != Error::kOk) {
    (void)Close();
    return e;
  }
  BitsetView activations(&memory_.Ccb()->activation_tracker,
                         shm::kMaxVchanId + 1);
  if (reliable_ || (options.activate && !activations.IsSet(vchan_id_ + 1))) {
    // Reliable subscribers hold the activation message, which stops a
    // reliable publisher overtaking them.
    slot_->message_size.store(1, std::memory_order_relaxed);
    PublishSlot(slot_, /*is_activation=*/true, nullptr);
    slot_ = reliable_ ? nullptr : FindFreeSlot();
  }
  TriggerSubscribers();
  return Error::kOk;
}

void Publisher::Reset() {
  memory_.Unmap();
  subscriber_triggers_.Clear();
  retirement_triggers_.Clear();
  poll_.Reset();
  client_ = nullptr;
  slot_ = nullptr;
  channel_id_ = -1;
  publisher_id_ = -1;
  vchan_id_ = -1;
  reliable_ = false;
}

Error Publisher::Close() {
  if (client_ == nullptr) {
    return Error::kOk;
  }
  // The server releases the slot this publisher holds.
  const Error e = client_->connection_->RemovePublisher(name_, publisher_id_);
  Reset();
  return e;
}

Error Publisher::Wait(int timeout_ms) {
  if (client_ == nullptr) {
    return Error::kNotInitialized;
  }
  return internal::WaitReadable(poll_.Get(), timeout_ms);
}

bool Publisher::HoldSlot() {
  if (slot_ != nullptr) {
    return true;
  }
  if (client_ == nullptr) {
    return false;
  }
  if (reliable_) {
    // A subscriber releasing a message after this makes PollFd() readable.
    internal::ClearTrigger(poll_.Get());
    // With no subscribers nothing would stop the publisher taking every
    // slot, and a subscriber that joined later would miss those messages.
    if (memory_.NumSubscribers(vchan_id_) == 0) {
      return false;
    }
  }
  slot_ = FindFreeSlot();
  return slot_ != nullptr;
}

void *Publisher::Buffer() {
  return HoldSlot() ? memory_.Payload(slot_) : nullptr;
}

void *Publisher::Metadata() {
  if (memory_.MetadataSize() == 0 || !HoldSlot()) {
    return nullptr;
  }
  return reinterpret_cast<char *>(&memory_.Prefix(slot_)->checksum) +
         memory_.ChecksumSize();
}

Error Publisher::Publish(size_t message_size, PublishedMessage *published) {
  if (client_ == nullptr) {
    return Error::kNotInitialized;
  }
  if (message_size == 0) {
    return Error::kInvalidArgument;
  }
  if (message_size > static_cast<size_t>(memory_.SlotSize())) {
    return Error::kMessageTooLarge;
  }
  if (slot_ == nullptr) {
    // Buffer() found no slot, so there is nothing to publish.
    return Error::kNoSlot;
  }
  if (Error e = RefreshTriggers(); e != Error::kOk) {
    return e;
  }
  slot_->message_size.store(message_size, std::memory_order_relaxed);
  PublishSlot(slot_, /*is_activation=*/false, published);
  // A reliable publisher takes its next slot when Buffer() asks for one.
  slot_ = reliable_ ? nullptr : FindFreeSlot();
  TriggerSubscribers();
  return Error::kOk;
}

Error Publisher::RefreshTriggers() {
  const uint16_t updates = memory_.Counters(channel_id_).num_sub_updates;
  if (updates == num_sub_updates_) {
    return Error::kOk;
  }
  TriggersReply reply;
  reply.subscriber_triggers = &subscriber_triggers_;
  reply.retirement_triggers = &retirement_triggers_;
  const Error e = client_->connection_->GetTriggers(name_, reply);
  if (e == Error::kOk) {
    num_sub_updates_ = updates;
  }
  return e;
}

void Publisher::TriggerSubscribers() const {
  for (int i = 0; i < subscriber_triggers_.Size(); i++) {
    internal::Trigger(subscriber_triggers_.Get(i));
  }
}

shm::MessageSlot *Publisher::FindFreeSlot() {
  return reliable_ ? FindFreeSlotReliable() : FindFreeSlotUnreliable();
}

int Publisher::FindRetiredSlotToReuse(bool oldest_first) const {
  BitsetView retired = memory_.RetiredSlots();
  if (!oldest_first) {
    return retired.FindFirstSet();
  }
  // With no subscribers every slot retires as soon as it is published, so
  // reuse the oldest to keep a window of recent messages for late joiners.
  int oldest = -1;
  uint64_t oldest_timestamp = ~uint64_t{0};
  for (int i = 0; i < memory_.NumSlots(); i++) {
    if (!retired.IsSet(i)) {
      continue;
    }
    const uint64_t timestamp =
        memory_.Slot(i)->timestamp.load(std::memory_order_relaxed);
    if (timestamp < oldest_timestamp) {
      oldest_timestamp = timestamp;
      oldest = i;
    }
  }
  return oldest;
}

// Takes ownership of slot if no other publisher or subscriber has it.
bool Publisher::ClaimSlot(shm::MessageSlot *slot) {
  const uint64_t owner = shm::kPubOwned | static_cast<uint64_t>(publisher_id_);
  const uint64_t old_refs = slot->refs.load(std::memory_order_relaxed);
  uint64_t expected = shm::BuildRefsBitField(
      slot->ordinal.load(std::memory_order_relaxed),
      static_cast<int>((old_refs >> shm::kVchanIdShift) & shm::kVchanIdMask),
      (old_refs >> shm::kRetiredRefsShift) & shm::kRetiredRefsMask);
  return slot->refs.compare_exchange_weak(
      expected, owner, std::memory_order_acquire, std::memory_order_relaxed);
}

shm::MessageSlot *Publisher::FindFreeSlotUnreliable() {
  shm::ChannelControlBlock *ccb = memory_.Ccb();
  BitsetView free_slots = memory_.FreeSlots();
  BitsetView retired_slots = memory_.RetiredSlots();
  constexpr int kMaxCasRetries = 1000;
  int retries = memory_.NumSlots() * 1000;
  int cas_retries = 0;
  shm::MessageSlot *slot = nullptr;
  int free_slot = -1;
  int retired_slot = -1;
  for (;;) {
    slot = nullptr;
    free_slot = -1;
    retired_slot = -1;
    const bool no_subscribers = memory_.NumSubscribers(vchan_id_) == 0;
    if (!ccb->free_slots_exhausted.load(std::memory_order_relaxed) &&
        (free_slot = free_slots.FindFirstSet()) != -1) {
      if (!free_slots.ClearWasSet(free_slot)) {
        continue;
      }
      slot = memory_.Slot(free_slot);
      if (free_slots.IsEmpty()) {
        ccb->free_slots_exhausted.store(true, std::memory_order_relaxed);
      }
    } else if ((retired_slot = FindRetiredSlotToReuse(no_subscribers)) != -1) {
      if (!retired_slots.ClearWasSet(retired_slot)) {
        continue;
      }
      slot = memory_.Slot(retired_slot);
    } else {
      // Recycle the oldest message that no subscriber holds.
      uint64_t earliest = ~uint64_t{0};
      for (int i = 0; i < memory_.NumSlots(); i++) {
        shm::MessageSlot *s = memory_.Slot(i);
        const uint64_t refs = s->refs.load(std::memory_order_relaxed);
        if ((refs & shm::kPubOwned) != 0) {
          continue;
        }
        const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
        if ((refs & shm::kRefsMask) == 0 && timestamp < earliest) {
          slot = s;
          earliest = timestamp;
        }
      }
    }
    if (slot == nullptr) {
      if (retries-- == 0) {
        return nullptr;
      }
      continue;
    }
    if (ClaimSlot(slot)) {
      break;
    }
    if (++cas_retries >= kMaxCasRetries) {
      if (retries-- == 0) {
        return nullptr;
      }
      cas_retries = 0;
      (void)::sched_yield();
    }
  }
  PrepareClaimedSlot(slot, free_slot == -1 && retired_slot == -1);
  return slot;
}

// A reliable subscriber still needs a slot that it holds, and, when reliable
// subscribers exist, a message that none of them has seen.
bool Publisher::ReliableClaimable(shm::MessageSlot *slot,
                                  bool require_reliable_seen) const {
  const uint64_t refs = slot->refs.load(std::memory_order_relaxed);
  if (((refs >> shm::kReliableRefCountShift) & shm::kRefCountMask) != 0) {
    return false;
  }
  if (require_reliable_seen &&
      slot->ordinal.load(std::memory_order_relaxed) != 0 &&
      (slot->flags.load(std::memory_order_relaxed) &
       shm::kMessageSeenByReliable) == 0) {
    return false;
  }
  return (refs & shm::kRefsMask) == 0;
}

// The oldest slot that no subscriber holds and that is older than every slot
// a reliable subscriber still needs.
shm::MessageSlot *
Publisher::OldestReliableCandidate(bool require_reliable_seen) const {
  int stop = -1;
  uint64_t stop_timestamp = 0;
  for (int i = 0; i < memory_.NumSlots(); i++) {
    shm::MessageSlot *s = memory_.Slot(i);
    const uint64_t refs = s->refs.load(std::memory_order_relaxed);
    if ((refs & shm::kPubOwned) != 0) {
      continue;
    }
    const bool needed =
        ((refs >> shm::kReliableRefCountShift) & shm::kRefCountMask) != 0 ||
        (require_reliable_seen &&
         s->ordinal.load(std::memory_order_relaxed) != 0 &&
         (s->flags.load(std::memory_order_relaxed) &
          shm::kMessageSeenByReliable) == 0);
    const uint64_t timestamp = s->timestamp.load(std::memory_order_relaxed);
    if (needed && (stop == -1 || Earlier(timestamp, i, stop_timestamp, stop))) {
      stop = i;
      stop_timestamp = timestamp;
    }
  }
  shm::MessageSlot *best = nullptr;
  uint64_t best_timestamp = 0;
  for (int i = 0; i < memory_.NumSlots(); i++) {
    shm::MessageSlot *s = memory_.Slot(i);
    const uint64_t refs = s->refs.load(std::memory_order_relaxed);
    if ((refs & (shm::kPubOwned | shm::kRefsMask)) != 0) {
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

shm::MessageSlot *Publisher::FindFreeSlotReliable() {
  shm::ChannelControlBlock *ccb = memory_.Ccb();
  BitsetView free_slots = memory_.FreeSlots();
  BitsetView retired_slots = memory_.RetiredSlots();
  constexpr int kMaxCasRetries = 1000;
  int retries = memory_.NumSlots() * 1000;
  int cas_retries = 0;
  shm::MessageSlot *slot = nullptr;
  int free_slot = -1;
  int retired_slot = -1;
  for (;;) {
    slot = nullptr;
    free_slot = -1;
    retired_slot = -1;
    const bool require_reliable_seen =
        memory_.Counters(channel_id_).num_reliable_subs != 0;
    if (!ccb->free_slots_exhausted.load(std::memory_order_relaxed) &&
        (free_slot = free_slots.FindFirstSet()) != -1) {
      if (!free_slots.ClearWasSet(free_slot)) {
        continue;
      }
      if (free_slots.IsEmpty()) {
        ccb->free_slots_exhausted.store(true, std::memory_order_relaxed);
      }
      // A free slot has never held a message.
      slot = memory_.Slot(free_slot);
    } else if ((retired_slot = retired_slots.FindFirstSet()) != -1) {
      if (!retired_slots.ClearWasSet(retired_slot)) {
        continue;
      }
      slot = memory_.Slot(retired_slot);
      if (!ReliableClaimable(slot, require_reliable_seen)) {
        // The slot is still retired; it just can't be reused yet.
        retired_slots.Set(retired_slot);
        return nullptr;
      }
    } else {
      slot = OldestReliableCandidate(require_reliable_seen);
      if (slot == nullptr) {
        return nullptr;
      }
    }
    if (ClaimSlot(slot)) {
      retired_slots.Clear(slot->id);
      break;
    }
    if (++cas_retries >= kMaxCasRetries) {
      if (retries-- == 0) {
        return nullptr;
      }
      cas_retries = 0;
      (void)::sched_yield();
    }
  }
  PrepareClaimedSlot(slot, free_slot == -1 && retired_slot == -1);
  return slot;
}

// Resets a slot this publisher has just claimed.  forced_reuse is true when
// the slot held a message that a subscriber might not have seen.
void Publisher::PrepareClaimedSlot(shm::MessageSlot *slot, bool forced_reuse) {
  slot->ordinal.store(0, std::memory_order_relaxed);
  slot->timestamp.store(0, std::memory_order_relaxed);
  slot->vchan_id.store(static_cast<int16_t>(vchan_id_),
                       std::memory_order_relaxed);
  SetSlotBuffer(slot);
  shm::MessagePrefix *prefix = memory_.Prefix(slot);
  prefix->flags = 0;
  prefix->vchan_id = vchan_id_;

  // The old message is gone, so no subscriber should look for it.
  memory_.Subscribers().Traverse([this, slot](int sub_id) {
    memory_.AvailableSlots(sub_id).Clear(slot->id);
  });

  if (forced_reuse) {
    memory_.TriggerRetirement(retirement_triggers_, slot->id);
  }
}

void Publisher::SetSlotBuffer(shm::MessageSlot *slot) {
  shm::BufferControlBlock *bcb = memory_.Bcb();
  const int old_index = slot->buffer_index.load(std::memory_order_relaxed);
  if (old_index >= 0 && old_index < shm::kMaxBuffers &&
      bcb->refs[old_index].load(std::memory_order_relaxed) > 0) {
    bcb->refs[old_index]--;
  }
  const int index = memory_.BufferIndex();
  slot->buffer_index.store(static_cast<int16_t>(index),
                           std::memory_order_relaxed);
  bcb->refs[index]++;
}

void Publisher::PublishSlot(shm::MessageSlot *slot, bool is_activation,
                            PublishedMessage *published) {
  shm::ChannelControlBlock *ccb = memory_.Ccb();
  shm::MessagePrefix *prefix = memory_.Prefix(slot);
  const uint64_t ordinal = ccb->ordinals[vchan_id_ + 1].fetch_add(1);
  const uint64_t timestamp = internal::Now();
  const uint64_t message_size =
      slot->message_size.load(std::memory_order_relaxed);
  slot->ordinal.store(ordinal, std::memory_order_relaxed);
  slot->timestamp.store(timestamp, std::memory_order_relaxed);
  slot->flags.store(0, std::memory_order_relaxed);

  prefix->message_size = message_size;
  prefix->ordinal = ordinal;
  prefix->timestamp = timestamp;
  prefix->vchan_id = vchan_id_;
  prefix->checksum_size = static_cast<uint16_t>(memory_.ChecksumSize());
  prefix->metadata_size = static_cast<uint16_t>(memory_.MetadataSize());
  prefix->flags = 0;
  prefix->slot_id = slot->id;
  slot->bridged_slot_id.store(slot->id, std::memory_order_relaxed);
  if (is_activation) {
    prefix->flags |= shm::kMessageActivate;
    slot->flags.fetch_or(shm::kMessageIsActivation, std::memory_order_relaxed);
    BitsetView(&ccb->activation_tracker, shm::kMaxVchanId + 1)
        .Set(vchan_id_ + 1);
  }
  if (checksum_) {
    prefix->flags |= shm::kMessageHasChecksum;
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
                message_size);
    crc = ~crc;
    std::memcpy(&prefix->checksum, &crc, sizeof(crc));
  }

  const uint64_t cleanup_generation = memory_.CleanupGeneration(vchan_id_);

  // The server uses this count to tell when no publisher can be writing to a
  // subscriber queue.
  std::atomic<uint32_t> &active_publishers =
      memory_.QueueIndex()->active_publishers[publisher_id_];
  active_publishers.fetch_add(1, std::memory_order_seq_cst);

  // Mark the message available to every subscriber on the publisher's
  // virtual channel while the slot is still publisher owned.  A subscriber
  // can leave between the traversal and the set, so check membership again
  // afterwards.  The bitset is the authoritative record, so a queue that is
  // full only loses a hint.
  memory_.Subscribers().Traverse(
      [this, slot, ordinal](int sub_id) {
        const int sub_vchan_id = memory_.Ccb()->sub_vchan_ids[sub_id];
        if (vchan_id_ != -1 && sub_vchan_id != -1 &&
            sub_vchan_id != vchan_id_) {
          return;
        }
        BitsetView available = memory_.AvailableSlots(sub_id);
        BitsetView subscribers = memory_.Subscribers();
        available.Set(slot->id);
        if (!subscribers.IsSetSeqCst(sub_id)) {
          available.Clear(slot->id);
          if (!subscribers.IsSetSeqCst(sub_id)) {
            return;
          }
          available.Set(slot->id);
        }
        shm::SlotQueue *queue = memory_.Queue(sub_id);
        if (queue != nullptr &&
            !internal::PushSlotQueue(queue, slot->id, ordinal)) {
          internal::MarkSlotQueueInsertionFailure(queue);
        }
      },
      std::memory_order_seq_cst);

  if (!is_activation) {
    ccb->total_bytes += message_size;
    if (message_size > ccb->max_message_size) {
      ccb->max_message_size = message_size;
    }
  }

  // Releasing publisher ownership makes the message readable.
  slot->refs.store(shm::BuildRefsBitField(ordinal, vchan_id_, 0),
                   std::memory_order_release);
  ccb->total_messages.fetch_add(1, std::memory_order_seq_cst);

  // A subscriber removed while publishing lowers the retirement threshold.
  if (!is_activation &&
      memory_.CleanupGeneration(vchan_id_) != cleanup_generation &&
      memory_.TryRetireSlot(slot)) {
    memory_.TriggerRetirement(retirement_triggers_, slot->id);
  }

  uint32_t active = active_publishers.load(std::memory_order_seq_cst);
  while (active != 0 && !active_publishers.compare_exchange_strong(
                            active, active - 1, std::memory_order_seq_cst)) {
  }
  if (published != nullptr) {
    published->ordinal = ordinal;
    published->timestamp = timestamp;
  }
}

} // namespace asil
} // namespace subspace
