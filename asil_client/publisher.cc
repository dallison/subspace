// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

// The publishing side of the shared memory protocol.  Finding a slot uses
// common/slot_search.h and publishing one uses common/publish.h, as the
// standard client does.

#include "asil_client/publisher.h"

#include "asil_client/client.h"
#include "common/message_checksum.h"
#include "common/publish.h"
#include "common/slot_search.h"

namespace subspace {
namespace asil {

namespace {

bool ValidLayoutSize(int32_t size) { return size >= 0 && size <= 0xFFFF; }

} // namespace

Error Publisher::Open(Client &client, const char *channel_name,
                      const PublisherOptions &options) {
  if (client_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  if (!client.Initialized()) {
    return Error::kNotInitialized;
  }
  if (options.server_defined) {
    if (options.slot_size != 0 || options.num_slots != 0 ||
        options.checksum_size != 0 || options.metadata_size != 0 ||
        options.subscriber_queue_arena_size != 0 || options.use_split_buffers) {
      return Error::kInvalidArgument;
    }
  } else if (options.slot_size <= 0 || options.num_slots <= 0 ||
             !ValidLayoutSize(options.checksum_size) ||
             !ValidLayoutSize(options.metadata_size)) {
    return Error::kInvalidArgument;
  }
  if (options.vchan_id < -1 || options.vchan_id >= shm::kMaxVchanId) {
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
  PublisherRequest request;
  request.channel_name = name_;
  request.server_defined = options.server_defined;
  request.slot_size =
      options.server_defined ? 0 : shm::Aligned64(options.slot_size);
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

  int64_t slot_size = request.slot_size;
  int32_t checksum_size = options.checksum_size;
  int32_t metadata_size = options.metadata_size;
  bool use_split_buffers = options.use_split_buffers;
  if (options.server_defined) {
    slot_size = reply.slot_size;
    checksum_size = reply.checksum_size;
    metadata_size = reply.metadata_size;
    use_split_buffers = reply.use_split_buffers;
  }

  Error e = Error::kOk;
  if (channel_id_ < 0 || channel_id_ >= shm::kMaxChannels ||
      publisher_id_ < 0 || publisher_id_ >= shm::kMaxSlotOwners ||
      vchan_id_ < -1 || vchan_id_ >= shm::kMaxVchanId ||
      (vchan_id_ != -1) != (options.mux != nullptr) || slot_size <= 0 ||
      (options.server_defined && reply.num_slots <= 0) ||
      !ValidLayoutSize(checksum_size) || !ValidLayoutSize(metadata_size)) {
    e = Error::kProtocolError;
  }
  if (e == Error::kOk) {
    const int num_slots =
        reply.num_slots > 0 ? reply.num_slots : options.num_slots;
    e = memory_.Map(client.scb_, reply.ccb.Get(), reply.bcb.Get(), num_slots,
                    checksum_size, metadata_size,
                    reply.subscriber_queue_arena_size, &buffer_context_);
  }
  if (e == Error::kOk) {
    e = use_split_buffers
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
  const bool activated =
      memory_.Ccb()->activation_tracker.IsActivated(vchan_id_);
  if (reliable_ || (options.activate && !activated)) {
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

// The publisher maps the channel's buffer when it opens the channel, so every
// claimed slot is usable.
class Publisher::SearchHooks : public SlotSearchHooks {
public:
  explicit SearchHooks(Publisher &publisher) : publisher_(publisher) {}

  void AssignBuffer(shm::MessageSlot *slot) { publisher_.SetSlotBuffer(slot); }
  shm::MessagePrefix *Prefix(shm::MessageSlot *slot) {
    return publisher_.memory_.Prefix(slot);
  }
  void ForcedReuse(shm::MessageSlot *slot) {
    publisher_.memory_.TriggerRetirement(publisher_.retirement_triggers_,
                                         slot->id);
  }

private:
  Publisher &publisher_;
};

shm::MessageSlot *Publisher::FindFreeSlot() {
  SlotSearch search;
  search.owner = publisher_id_;
  search.vchan_id = vchan_id_;
  SearchHooks hooks(*this);
  if (reliable_) {
    return ClaimSlotReliable(memory_, search, memory_.Counters(channel_id_),
                             hooks);
  }
  return ClaimSlotUnreliable(memory_, search, hooks);
}

void Publisher::SetSlotBuffer(shm::MessageSlot *slot) {
  shm::BufferControlBlock *bcb = memory_.Bcb();
  const int old_index = slot->buffer_index.load(std::memory_order_relaxed);
  if (old_index >= 0 && old_index < static_cast<int>(shm::kMaxBuffers) &&
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
  shm::MessagePrefix *prefix = memory_.Prefix(slot);
  StampSlot(memory_.Ccb(), slot, internal::Now());
  FillPrefix(memory_.Ccb(), slot, prefix, memory_.ChecksumSize(),
             memory_.MetadataSize(), is_activation);
  if (checksum_) {
    prefix->SetHasChecksum();
    SetMessageChecksum(prefix, memory_.Payload(slot),
                       slot->message_size.load(std::memory_order_relaxed),
                       memory_.ChecksumSize(), memory_.MetadataSize());
  }

  PublishedSlot result;
  {
    SubscriberQueuePublishScope publishing(memory_.QueueIndex(),
                                           publisher_id_);
    result = CommitPublishedSlot(
        memory_, slot, vchan_id_, is_activation, publishing,
        [this](int32_t retirement_slot_id) {
          memory_.TriggerRetirement(retirement_triggers_, retirement_slot_id);
        });
  }
  if (result.retired) {
    memory_.TriggerRetirement(retirement_triggers_, result.retirement_slot_id);
  }
  if (published != nullptr) {
    published->ordinal = result.ordinal;
    published->timestamp = result.timestamp;
  }
}

} // namespace asil
} // namespace subspace
