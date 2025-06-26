// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/client.h"
#include "absl/strings/str_format.h"
#include "proto/subspace.pb.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/mutex.h"
#include "toolbelt/sockets.h"
#include <cerrno>
#include <inttypes.h>

namespace subspace {

using ClientChannel = details::ClientChannel;
using SubscriberImpl = details::SubscriberImpl;
using PublisherImpl = details::PublisherImpl;

absl::Status ClientImpl::CheckConnected() const {
  if (!socket_.Connected()) {
    return absl::InternalError(
        "Client is not connected to the server; have you called Init()?");
  }
  return absl::OkStatus();
}

absl::Status ClientImpl::Init(const std::string &server_socket,
                              const std::string &client_name) {
  if (socket_.Connected()) {
    return absl::InternalError("Client is already connected to the server; "
                               "Init() called twice perhaps?");
  }
  absl::Status status = socket_.Connect(server_socket);
  if (!status.ok()) {
    return status;
  }

  name_ = client_name;
  socket_name_ = server_socket;
  Request req;
  req.mutable_init()->set_client_name(client_name);
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  status = SendRequestReceiveResponse(req, resp, fds);
  if (!status.ok()) {
    return status;
  }
  scb_fd_ = std::move(fds[resp.init().scb_fd_index()]);
  session_id_ = resp.init().session_id();
  return absl::OkStatus();
}

void ClientImpl::RegisterDroppedMessageCallback(
    SubscriberImpl *subscriber,
    std::function<void(SubscriberImpl *, int64_t)> callback) {
  dropped_message_callbacks_[subscriber] = std::move(callback);
}

absl::Status
ClientImpl::UnregisterDroppedMessageCallback(SubscriberImpl *subscriber) {
  auto it = dropped_message_callbacks_.find(subscriber);
  if (it == dropped_message_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "No dropped message callback has been registered for channel %s\n",
        subscriber->Name()));
  }
  dropped_message_callbacks_.erase(it);
  return absl::OkStatus();
}

absl::Status ClientImpl::RegisterMessageCallback(
    SubscriberImpl *subscriber,
    std::function<void(SubscriberImpl *, Message)> callback) {
  auto it = message_callbacks_.find(subscriber);
  if (it != message_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "A message callback has already been registered for channel %s\n",
        subscriber->Name()));
  }
  message_callbacks_[subscriber] = std::move(callback);
  return absl::OkStatus();
}

absl::Status ClientImpl::UnregisterMessageCallback(SubscriberImpl *subscriber) {
  auto it = message_callbacks_.find(subscriber);
  if (it == message_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "No message callback has been registered for channel %s\n",
        subscriber->Name()));
  }
  message_callbacks_.erase(it);
  return absl::OkStatus();
}

void ClientImpl::RegisterResizeCallback(
    PublisherImpl *publisher,
    std::function<absl::Status(PublisherImpl *, int32_t, int32_t)> callback) {
  resize_callbacks_[publisher] = std::move(callback);
}

absl::Status ClientImpl::UnregisterResizeCallback(PublisherImpl *publisher) {
  auto it = resize_callbacks_.find(publisher);
  if (it == resize_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "No resize callback has been registered for channel %s\n",
        publisher->Name()));
  }
  resize_callbacks_.erase(it);
  return absl::OkStatus();
}

absl::Status ClientImpl::ProcessAllMessages(details::SubscriberImpl *subscriber,
                                            ReadMode mode) {
  auto it = message_callbacks_.find(subscriber);
  if (it == message_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "No message callback has been registered for channel %s\n",
        subscriber->Name()));
  }
  for (;;) {
    absl::StatusOr<Message> msg = ReadMessage(subscriber, mode);
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length == 0) {
      break;
    }
    it->second(subscriber, std::move(*msg));
  }
  return absl::OkStatus();
}

absl::StatusOr<Publisher>
ClientImpl::CreatePublisher(const std::string &channel_name,
                            const PublisherOptions &opts) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  Request req;
  auto *cmd = req.mutable_create_publisher();
  cmd->set_channel_name(channel_name);
  cmd->set_slot_size(Aligned(opts.slot_size));
  cmd->set_num_slots(opts.num_slots);
  cmd->set_is_local(opts.IsLocal());
  cmd->set_is_reliable(opts.IsReliable());
  cmd->set_is_bridge(opts.IsBridge());
  cmd->set_is_fixed_size(opts.IsFixedSize());
  cmd->set_type(opts.Type());
  cmd->set_mux(opts.Mux());
  cmd->set_vchan_id(opts.VchanId());

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = SendRequestReceiveResponse(req, resp, fds);
      !status.ok()) {
    return status;
  }
  auto &pub_resp = resp.create_publisher();
  if (!pub_resp.error().empty()) {
    return absl::InternalError(pub_resp.error());
  }

  // Make a local ClientChannel object and map in the shared memory allocated
  // by the server.
  std::shared_ptr<PublisherImpl> channel = std::make_shared<PublisherImpl>(
      channel_name, opts.num_slots, pub_resp.channel_id(),
      pub_resp.publisher_id(), pub_resp.vchan_id(), session_id_,
      pub_resp.type(), opts);

  SharedMemoryFds channel_fds(std::move(fds[pub_resp.ccb_fd_index()]),
                              std::move(fds[pub_resp.bcb_fd_index()]));
  if (absl::Status status = channel->Map(std::move(channel_fds), scb_fd_);
      !status.ok()) {
    return status;
  }

  if (absl::Status status =
          channel->CreateOrAttachBuffers(Aligned(opts.slot_size));
      !status.ok()) {
    return status;
  }
  channel->SetTriggerFd(std::move(fds[pub_resp.pub_trigger_fd_index()]));
  channel->SetPollFd(std::move(fds[pub_resp.pub_poll_fd_index()]));

  // Add all subscriber triggers fds to the publisher channel.
  channel->ClearSubscribers();
  for (auto index : pub_resp.sub_trigger_fd_indexes()) {
    channel->AddSubscriber(std::move(fds[index]));
  }

  channel->SetNumUpdates(pub_resp.num_sub_updates());

  if (!opts.IsReliable()) {
    // A publisher needs a slot.  Allocate one.
    MessageSlot *slot = channel->FindFreeSlotUnreliable(
        channel->GetPublisherId(), [ this, channel = channel.get() ]() {
          absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(channel);
          if (!ok.ok()) {
            return false;
          }
          return *ok;
        });
    if (slot == nullptr) {
      return absl::InternalError("No slot available for publisher");
    }
    channel->SetSlot(slot);
    if (opts.Activate()) {
      if (absl::Status status = ActivateChannel(channel.get()); !status.ok()) {
        return status;
      }
    }
  } else {
    // Send a single activation message to the channel.
    absl::Status status = ActivateReliableChannel(channel.get());
    if (!status.ok()) {
      return status;
    }
  }
  channel->TriggerSubscribers();
  // channel->Dump();
  channels_.insert(channel);
  return Publisher(shared_from_this(), channel);
}

absl::StatusOr<Publisher>
ClientImpl::CreatePublisher(const std::string &channel_name, int slot_size,
                            int num_slots, const PublisherOptions &opts) {
  PublisherOptions options = opts;
  options.slot_size = slot_size;
  options.num_slots = num_slots;
  return CreatePublisher(channel_name, options);
}
absl::StatusOr<Subscriber>
ClientImpl::CreateSubscriber(const std::string &channel_name,
                             const SubscriberOptions &opts) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  if (opts.MaxActiveMessages() < 1) {
    return absl::InvalidArgumentError(
        "MaxActiveMessages must be at least 1 for a subscriber");
  }
  Request req;
  auto *cmd = req.mutable_create_subscriber();
  cmd->set_channel_name(channel_name);
  cmd->set_subscriber_id(-1); // New subscriber is being created.
  cmd->set_is_reliable(opts.IsReliable());
  cmd->set_is_bridge(opts.IsBridge());
  cmd->set_type(opts.Type());
  cmd->set_max_active_messages(opts.MaxActiveMessages());
  cmd->set_mux(opts.Mux());
  cmd->set_vchan_id(opts.VchanId());

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = SendRequestReceiveResponse(req, resp, fds);
      !status.ok()) {
    return status;
  }

  auto &sub_resp = resp.create_subscriber();
  if (!sub_resp.error().empty()) {
    return absl::InternalError(sub_resp.error());
  }

  // Make a local Subscriber object and map in the shared memory allocated
  // by the server.
  std::shared_ptr<SubscriberImpl> channel = std::make_shared<SubscriberImpl>(
      channel_name, sub_resp.num_slots(), sub_resp.channel_id(),
      sub_resp.subscriber_id(), sub_resp.vchan_id(), session_id_,
      sub_resp.type(), opts);

  channel->SetNumSlots(sub_resp.num_slots());

  SharedMemoryFds channel_fds(std::move(fds[sub_resp.ccb_fd_index()]),
                              std::move(fds[sub_resp.bcb_fd_index()]));
  if (absl::Status status = channel->Map(std::move(channel_fds), scb_fd_);
      !status.ok()) {
    return status;
  }

  if (absl::Status status = channel->AttachBuffers(); !status.ok()) {
    return status;
  }
  channel->SetTriggerFd(std::move(fds[sub_resp.trigger_fd_index()]));
  channel->SetPollFd(std::move(fds[sub_resp.poll_fd_index()]));

  // Add all publisher triggers fds to the subscriber channel.
  channel->ClearPublishers();
  for (auto index : sub_resp.reliable_pub_trigger_fd_indexes()) {
    channel->AddPublisher(std::move(fds[index]));
  }

  channel->SetNumUpdates(sub_resp.num_pub_updates());

  // Trigger the subscriber to pick up all existing messages.
  channel->Trigger();

  // channel->Dump();

  channels_.insert(channel);
  return Subscriber(shared_from_this(), channel);
}

absl::StatusOr<void *> ClientImpl::GetMessageBuffer(PublisherImpl *publisher,
                                                    int32_t max_size) {
  if (publisher->IsReliable()) {
    publisher->ClearPollFd();
  }

  int32_t slot_size = publisher->SlotSize();
  if (max_size != -1 && max_size > slot_size) {
    int32_t new_slot_size = slot_size;
    assert(new_slot_size > 0);
    while (new_slot_size <= slot_size || new_slot_size < max_size) {
      new_slot_size *= 2;
    }

    if (absl::Status status = ResizeChannel(publisher, new_slot_size);
        !status.ok()) {
      return status;
    }
    publisher->SetSlotToBiggestBuffer(publisher->CurrentSlot());
  }

  if (absl::Status status = ReloadSubscribersIfNecessary(publisher);
      !status.ok()) {
    return status;
  }

  if (publisher->IsReliable() && publisher->CurrentSlot() == nullptr) {
    // We are a reliable publisher and don't have a slot yet.  Try to allocate
    // one now.  If we fail, we return nullptr so that the caller knows to try
    // again.
    //
    // If there are no subscribers to the channel, don't allow a message to
    // be published yet.  This is because since there are no subscribers
    // there are no slots with reliable_ref_count > 0 and therefore nothing
    // to stop the publisher taking all the slots.  An incoming subscriber
    // would miss all those messages and that's not reliable.
    if (publisher->NumSubscribers(publisher->VirtualChannelId()) == 0) {
      return nullptr;
    }
    MessageSlot *slot = publisher->FindFreeSlotReliable(
        publisher->GetPublisherId(), [this, publisher]() {
          absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(publisher);
          if (!ok.ok()) {
            return false;
          }
          return *ok;
        });
    if (slot == nullptr) {
      return nullptr;
    }
    publisher->SetSlot(slot);
  }

  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Channel %s has no buffer", publisher->Name()));
  }
  return buffer;
}

absl::StatusOr<const Message>
ClientImpl::PublishMessage(PublisherImpl *publisher, int64_t message_size) {
  return PublishMessageInternal(publisher, message_size, /*omit_prefix=*/false);
}

absl::StatusOr<const Message>
ClientImpl::PublishMessageInternal(PublisherImpl *publisher,
                                   int64_t message_size, bool omit_prefix) {
  // Check if there are any new subscribers and if so, load their trigger fds.
  if (absl::Status status = ReloadSubscribersIfNecessary(publisher);
      !status.ok()) {
    return status;
  }
  publisher->SetMessageSize(message_size);
  MessageSlot *old_slot = publisher->CurrentSlot();
  if (debug_) {
    if (old_slot != nullptr) {
      printf("publish old slot: %d: %" PRId64 "\n", old_slot->id,
             old_slot->ordinal);
    }
  }

  bool notify = false;
  Channel::PublishedMessage msg = publisher->ActivateSlotAndGetAnother(
      publisher->IsReliable(), /*is_activation=*/false, omit_prefix, &notify,
      [this, publisher]() {
        absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(publisher);
        if (!ok.ok()) {
          return false;
        }
        return *ok;
      });

  // Prevent use of old_slot.
  old_slot = nullptr;

  publisher->SetSlot(msg.new_slot);

  // Only trigger subscribers if we need to.
  // We could trigger for every message, but that is unnecessary and
  // slower.  It would basically mean a write to pipe for every
  // message sent.  That's fast, but if we can avoid it, things
  // would be faster.
  if (notify) {
    publisher->TriggerSubscribers();
    publisher->UnmapUnusedBuffers();
  }

  if (msg.new_slot == nullptr) {
    if (publisher->IsReliable()) {
      // Reliable publishers don't get a slot until it's asked for.
      return Message(message_size, nullptr, msg.ordinal, msg.timestamp,
                     publisher->VirtualChannelId(), false);
    }
    return absl::InternalError(
        absl::StrFormat("Out of slots for channel %s", publisher->Name()));
  }

  if (debug_) {
    printf("publish new slot: %d: %" PRId64 "\n", msg.new_slot->id,
           msg.new_slot->ordinal);
  }

  return Message(message_size, nullptr, msg.ordinal, msg.timestamp,
                 publisher->VirtualChannelId(), false);
}

absl::Status ClientImpl::WaitForReliablePublisher(PublisherImpl *publisher,
                                                  co::Coroutine *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  if (!publisher->IsReliable()) {
    return absl::InternalError("Unreliable publishers can't wait");
  }
  // Check if there are any new subscribers and if so, load their trigger fds.
  if (absl::Status status = ReloadSubscribersIfNecessary(publisher);
      !status.ok()) {
    return status;
  }
  if (c != nullptr) {
    c->Wait(publisher->GetPollFd().Fd(), POLLIN);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    co_->Wait(publisher->GetPollFd().Fd(), POLLIN);
  } else {
    struct pollfd fd = {.fd = publisher->GetPollFd().Fd(), .events = POLLIN};
    int e = ::poll(&fd, 1, -1);
    // Since we are waiting forever will can only get the value 1 from the poll.
    // We will never get 0 since there is no timeout.  Anything else (can only
    // be -1) will be an error.
    if (e != 1) {
      return absl::InternalError(
          absl::StrFormat("Error from poll waiting for reliable publisher: %s",
                          strerror(errno)));
    }
  }
  return absl::OkStatus();
}

absl::Status ClientImpl::WaitForSubscriber(SubscriberImpl *subscriber,
                                           co::Coroutine *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }

  if (c != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    c->Wait(subscriber->GetPollFd().Fd(), POLLIN);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    co_->Wait(subscriber->GetPollFd().Fd(), POLLIN);
  } else {
    struct pollfd fd = {.fd = subscriber->GetPollFd().Fd(), .events = POLLIN};
    int e = ::poll(&fd, 1, -1);
    // Since we are waiting forever will can only get the value 1 from the poll.
    // We will never get 0 since there is no timeout.  Anything else (can only
    // be -1) will be an error.
    if (e != 1) {
      return absl::InternalError(absl::StrFormat(
          "Error from poll waiting for subscriber: %s", strerror(errno)));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<Message>
ClientImpl::ReadMessageInternal(SubscriberImpl *subscriber, ReadMode mode,
                                bool pass_activation, bool clear_trigger) {
  if (clear_trigger) {
    subscriber->ClearPollFd();
  }

  MessageSlot *new_slot = nullptr;
  MessageSlot *old_slot = subscriber->CurrentSlot();
  int64_t last_ordinal = -1;
  if (old_slot != nullptr) {
    last_ordinal = old_slot->ordinal;
    if (debug_) {
      printf("read old slot: %d: %" PRId64 "\n", old_slot->id, last_ordinal);
    }
  }

  switch (mode) {
  case ReadMode::kReadNext:
    new_slot = subscriber->NextSlot([this, subscriber]() {
      absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(subscriber);
      if (!ok.ok()) {
        return false;
      }
      return *ok;
    });
    break;
  case ReadMode::kReadNewest:
    new_slot = subscriber->LastSlot([this, subscriber]() {
      absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(subscriber);
      if (!ok.ok()) {
        return false;
      }
      return *ok;
    });
    break;
  }

  // At this point, old_slot may have been reused so don't reference it
  // for any data.
  old_slot = nullptr; // Prevent any accidental use.

  if (new_slot == nullptr) {
    // I'm out of messages to read, trigger the publishers to give me
    // some more.  This is only for reliable publishers.
    subscriber->TriggerReliablePublishers();
    subscriber->UnmapUnusedBuffers();
    return Message();
  }
  subscriber->SetSlot(new_slot);

  if (debug_) {
    printf("read new_slot: %d: %" PRId64 "\n", new_slot->id, new_slot->ordinal);
  }

  if (mode == ReadMode::kReadNext && last_ordinal != -1) {
    int drops = subscriber->DetectDrops(new_slot->vchan_id);
    if (drops > 0) {
      // We dropped a message.  If we have a callback registered for this
      // channel, call it with the number of dropped messages.
      auto it = dropped_message_callbacks_.find(subscriber);
      if (it != dropped_message_callbacks_.end()) {
        it->second(subscriber, drops);
      }
      if (subscriber->options_.log_dropped_messages) {
        logger_.Log(toolbelt::LogLevel::kWarning,
                    "Dropped %d message%s on channel %s", drops,
                    drops == 1 ? "" : "s", subscriber->Name().c_str());
      }
    }
  }

  MessagePrefix *prefix = subscriber->Prefix(new_slot, [this, subscriber]() {
    absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(subscriber);
    if (!ok.ok()) {
      return false;
    }
    return *ok;
  });

  bool is_activation = false;
  if (prefix != nullptr) {
    if ((prefix->flags & kMessageActivate) != 0) {
      is_activation = true;
      if (!pass_activation) {
        subscriber->IgnoreActivation(new_slot);
        if (subscriber->IsReliable()) {
          subscriber->TriggerReliablePublishers();
        }
        return ReadMessageInternal(subscriber, mode,
                                   /* pass_activation=*/false,
                                   /* clear_trigger=*/false);
      }
    }
  }
  // We have a new slot, clear the subscriber's slot.
  subscriber->ClearActiveMessage();

  // Allocate a new active message for the slot.
  auto msg = subscriber->SetActiveMessage(
      new_slot->message_size, new_slot, subscriber->GetCurrentBufferAddress(),
      subscriber->CurrentOrdinal(), subscriber->Timestamp(), new_slot->vchan_id,
      is_activation);

  // If we are unable to allocate a new message (due to message limits)
  // restore the slot so that we pick it up next time.
  if (msg->length == 0) {
    subscriber->DecrementSlotRef(new_slot, false);
    // Subscriber does not have a slot now but the slot it had is still active.
  } else {
    // We have a slot, claim it.
    subscriber->ClaimSlot(
        new_slot,
        [this, subscriber]() {
          absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(subscriber);
          if (!ok.ok()) {
            return false;
          }
          return *ok;
        },
        subscriber->VirtualChannelId(), mode == ReadMode::kReadNewest);
  }
  return Message(msg);
}

absl::StatusOr<Message> ClientImpl::ReadMessage(SubscriberImpl *subscriber,
                                                ReadMode mode) {
  // If the channel is a placeholder (no publishers present), look
  // in the SCB to see if a new publisher has been created and if so,
  // talk to the server to get the information to reload the shared
  // memory.  If there still isn't a publisher, we will still be a placeholder.
  if (subscriber->IsPlaceholder()) {
    absl::Status status = ReloadSubscriber(subscriber);
    if (!status.ok() || subscriber->IsPlaceholder()) {
      subscriber->ClearPollFd();
      return Message();
    }
  }

  // Check if there are any new reliable publishers and if so, load their
  // trigger fds.
  absl::Status status = ReloadReliablePublishersIfNecessary(subscriber);
  if (!status.ok()) {
    return status;
  }

  return ReadMessageInternal(subscriber, mode,
                             subscriber->options_.pass_activation,
                             /*clear_trigger=*/true);
}

absl::StatusOr<Message>
ClientImpl::FindMessageInternal(SubscriberImpl *subscriber,
                                uint64_t timestamp) {

  MessageSlot *new_slot = subscriber->FindMessage(timestamp);
  if (new_slot == nullptr) {
    // Not found.
    return Message();
  }
  return Message(new_slot->message_size, subscriber->GetCurrentBufferAddress(),
                 subscriber->CurrentOrdinal(), subscriber->Timestamp(),
                 subscriber->VirtualChannelId(), false);
}

absl::StatusOr<Message> ClientImpl::FindMessage(SubscriberImpl *subscriber,
                                                uint64_t timestamp) {
  // If the channel is a placeholder (no publishers present), contact the
  // server to see if there is now a publisher.  This will reload the shared
  // memory.  If there still isn't a publisher, we will still be a placeholder.
  if (subscriber->IsPlaceholder()) {
    absl::Status status = ReloadSubscriber(subscriber);
    if (!status.ok() || subscriber->IsPlaceholder()) {
      subscriber->ClearPollFd();
      return Message();
    }
  }

  // Check if there are any new reliable publishers and if so, load their
  // trigger fds.
  absl::Status status = ReloadReliablePublishersIfNecessary(subscriber);
  if (!status.ok()) {
    return status;
  }
  return FindMessageInternal(subscriber, timestamp);
}

struct pollfd ClientImpl::GetPollFd(SubscriberImpl *subscriber) const {
  struct pollfd fd = {.fd = subscriber->GetPollFd().Fd(), .events = POLLIN};
  return fd;
}

struct pollfd ClientImpl::GetPollFd(PublisherImpl *publisher) const {
  static struct pollfd fd { .fd = -1, .events = POLLIN };
  if (!publisher->IsReliable()) {
    return fd;
  }
  fd = {.fd = publisher->GetPollFd().Fd(), .events = POLLIN};
  return fd;
}

toolbelt::FileDescriptor
ClientImpl::GetFileDescriptor(SubscriberImpl *subscriber) const {
  return subscriber->GetPollFd();
}

toolbelt::FileDescriptor
ClientImpl::GetFileDescriptor(PublisherImpl *publisher) const {
  if (!publisher->IsReliable()) {
    return toolbelt::FileDescriptor();
  }
  return publisher->GetPollFd();
}

int64_t ClientImpl::GetCurrentOrdinal(SubscriberImpl *sub) const {
  MessageSlot *slot = sub->CurrentSlot();
  if (slot == nullptr) {
    return -1;
  }
  return slot->ordinal;
}

absl::StatusOr<bool>
ClientImpl::ReloadBuffersIfNecessary(ClientChannel *channel) {
  if (!channel->BuffersChanged()) {
    return false;
  }
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }

  if (absl::Status status = channel->AttachBuffers(); !status.ok()) {
    return status;
  }
  return true;
}

absl::Status ClientImpl::ReloadSubscriber(SubscriberImpl *subscriber) {
  // Check if there are any updates to the publishers
  // since that last time we checked.
  SystemControlBlock *scb = subscriber->GetScb();
  int updates = scb->counters[subscriber->GetChannelId()].num_pub_updates;

  if (subscriber->NumUpdates() == updates) {
    return absl::OkStatus();
  }
  subscriber->SetNumUpdates(updates);

  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  Request req;
  auto *cmd = req.mutable_create_subscriber();
  cmd->set_channel_name(subscriber->Name());
  cmd->set_subscriber_id(subscriber->GetSubscriberId());
  cmd->set_mux(subscriber->options_.mux);

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = SendRequestReceiveResponse(req, resp, fds);
      !status.ok()) {
    return status;
  }

  auto &sub_resp = resp.create_subscriber();
  if (!sub_resp.error().empty()) {
    return absl::InternalError(sub_resp.error());
  }

  // Unmap the channel memory.
  subscriber->Unmap();

  if (!sub_resp.type().empty()) {
    subscriber->SetType(sub_resp.type());
  }
  subscriber->SetNumSlots(sub_resp.num_slots());

  SharedMemoryFds channel_fds(std::move(fds[sub_resp.ccb_fd_index()]),
                              std::move(fds[sub_resp.bcb_fd_index()]));

  // subscriber->SetSlots(sub_resp.slot_size(), sub_resp.num_slots());

  if (absl::Status status = subscriber->Map(std::move(channel_fds), scb_fd_);
      !status.ok()) {
    return status;
  }
  if (absl::Status status = subscriber->AttachBuffers(); !status.ok()) {
    return status;
  }
  subscriber->SetTriggerFd(fds[sub_resp.trigger_fd_index()]);
  subscriber->SetPollFd(fds[sub_resp.poll_fd_index()]);

  // Add all publisher trigger fds to the subscriber channel.
  subscriber->ClearPublishers();
  for (auto index : sub_resp.reliable_pub_trigger_fd_indexes()) {
    subscriber->AddPublisher(fds[index]);
  }
  // subscriber->Dump();
  return absl::OkStatus();
}

absl::Status
ClientImpl::ReloadSubscribersIfNecessary(PublisherImpl *publisher) {
  SystemControlBlock *scb = publisher->GetScb();
  int updates = scb->counters[publisher->GetChannelId()].num_sub_updates;
  if (publisher->NumUpdates() == updates) {
    return absl::OkStatus();
  }
  publisher->SetNumUpdates(updates);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }

  // We do have updates, get a new list of subscriber for
  // the channel.
  Request req;
  auto *cmd = req.mutable_get_triggers();
  cmd->set_channel_name(publisher->Name());

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = SendRequestReceiveResponse(req, resp, fds);
      !status.ok()) {
    return status;
  }

  auto &sub_resp = resp.get_triggers();
  // Add all subscriber triggers fds to the publisher channel.
  publisher->ClearSubscribers();
  for (auto index : sub_resp.sub_trigger_fd_indexes()) {
    publisher->AddSubscriber(fds[index]);
  }
  return absl::OkStatus();
}

absl::Status
ClientImpl::ReloadReliablePublishersIfNecessary(SubscriberImpl *subscriber) {
  // Check if there are any updates to the publishers
  // since that last time we checked.
  SystemControlBlock *scb = subscriber->GetScb();
  int updates = scb->counters[subscriber->GetChannelId()].num_pub_updates;
  if (subscriber->NumUpdates() == updates) {
    return absl::OkStatus();
  }
  subscriber->SetNumUpdates(updates);

  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  // We do have updates, get a new list of subscriber for
  // the channel.
  Request req;
  auto *cmd = req.mutable_get_triggers();
  cmd->set_channel_name(subscriber->Name());

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = SendRequestReceiveResponse(req, resp, fds);
      !status.ok()) {
    return status;
  }

  auto &sub_resp = resp.get_triggers();
  // Add all subscriber triggers fds to the publisher channel.
  subscriber->ClearPublishers();
  for (auto index : sub_resp.reliable_pub_trigger_fd_indexes()) {
    subscriber->AddPublisher(fds[index]);
  }
  return absl::OkStatus();
}

// A reliable publisher always sends a single activation message when it
// is created.  This is to ensure that the reliable subscribers see
// on message and thus keep a reference to it.
absl::Status ClientImpl::ActivateReliableChannel(PublisherImpl *publisher) {
  MessageSlot *slot = publisher->FindFreeSlotReliable(
      publisher->GetPublisherId(), [this, publisher]() {
        absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(publisher);
        if (!ok.ok()) {
          return false;
        }
        return *ok;
      });
  if (slot == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Channel %s has no free slots", publisher->Name()));
  }
  publisher->SetSlot(slot);

  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Channel %s has no buffer", publisher->Name()));
  }
  slot->message_size = 1;

  publisher->ActivateSlotAndGetAnother(
      /*reliable=*/true,
      /*is_activation=*/true,
      /*omit_prefix=*/false, /*notify=*/nullptr, [this, publisher]() {
        absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(publisher);
        if (!ok.ok()) {
          return false;
        }
        return *ok;
      });
  publisher->SetSlot(nullptr);
  publisher->TriggerSubscribers();

  return absl::OkStatus();
}

absl::Status ClientImpl::ActivateChannel(PublisherImpl *publisher) {
  if (publisher->IsActivated(publisher->VirtualChannelId())) {
    return absl::OkStatus();
  }

  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Channel %s has no buffer", publisher->Name()));
  }
  MessageSlot *slot = publisher->CurrentSlot();
  slot->message_size = 1;

  Channel::PublishedMessage msg = publisher->ActivateSlotAndGetAnother(
      /*reliable=*/false,
      /*is_activation=*/true,
      /*omit_prefix=*/false, /*notify=*/nullptr, [this, publisher]() {
        absl::StatusOr<bool> ok = ReloadBuffersIfNecessary(publisher);
        if (!ok.ok()) {
          return false;
        }
        return *ok;
      });
  publisher->SetSlot(msg.new_slot);
  publisher->TriggerSubscribers();

  return absl::OkStatus();
}

absl::Status ClientImpl::RemoveChannel(ClientChannel *channel) {
  if (!channels_.contains(channel)) {
    return absl::InternalError(
        absl::StrFormat("Channel %s not found\n", channel->Name()));
  }
  channels_.erase(channel);
  return absl::OkStatus();
}

absl::Status ClientImpl::RemovePublisher(PublisherImpl *publisher) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  Request req;
  auto *cmd = req.mutable_remove_publisher();
  cmd->set_channel_name(publisher->Name());
  cmd->set_publisher_id(publisher->GetPublisherId());

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = SendRequestReceiveResponse(req, response, fds);
      !status.ok()) {
    return status;
  }

  auto &resp = response.remove_publisher();
  if (!resp.error().empty()) {
    return absl::InternalError(resp.error());
  }
  return RemoveChannel(publisher);
}

absl::Status ClientImpl::RemoveSubscriber(SubscriberImpl *subscriber) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  Request req;
  auto *cmd = req.mutable_remove_subscriber();
  cmd->set_channel_name(subscriber->Name());
  cmd->set_subscriber_id(subscriber->GetSubscriberId());

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = SendRequestReceiveResponse(req, response, fds);
      !status.ok()) {
    return status;
  }

  auto &resp = response.remove_subscriber();
  if (!resp.error().empty()) {
    return absl::InternalError(resp.error());
  }
  return RemoveChannel(subscriber);
}

const ChannelCounters &ClientImpl::GetChannelCounters(ClientChannel *channel) {
  return channel->GetCounters();
}

absl::Status ClientImpl::ResizeChannel(PublisherImpl *publisher,
                                       int32_t new_slot_size) {
  if (publisher->IsFixedSize()) {
    return absl::InternalError(absl::StrFormat(
        "Channel %s is fixed size at %d bytes; can't increase it to %d bytes",
        publisher->Name(), publisher->SlotSize(), new_slot_size));
  }

  // Call the resize callback if one has been registered.  If this returns
  // an error, we don't perform the resize.
  auto it = resize_callbacks_.find(publisher);
  if (it != resize_callbacks_.end()) {
    if (absl::Status s =
            it->second(publisher, publisher->SlotSize(), new_slot_size);
        !s.ok()) {
      return s;
    }
  }

  return publisher->CreateOrAttachBuffers(Aligned(new_slot_size));
}

absl::Status ClientImpl::SendRequestReceiveResponse(
    const Request &req, Response &response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  // SendMessage needs 4 bytes before the buffer passed to
  // use for the length.
  char *sendbuf = buffer_ + sizeof(int32_t);
  constexpr size_t kSendBufLen = sizeof(buffer_) - sizeof(int32_t);

  if (!req.SerializeToArray(sendbuf, kSendBufLen)) {
    return absl::InternalError("Failed to serialize request");
  }

  size_t length = req.ByteSizeLong();
  absl::StatusOr<ssize_t> n = socket_.SendMessage(sendbuf, length, co_);
  if (!n.ok()) {
    socket_.Close();
    return n.status();
  }

  // Wait for response and put it in the same buffer we used for send.
  n = socket_.ReceiveMessage(buffer_, sizeof(buffer_), co_);
  if (!n.ok()) {
    socket_.Close();
    return n.status();
  }
  if (!response.ParseFromArray(buffer_, static_cast<int>(*n))) {
    socket_.Close();
    return absl::InternalError("Failed to parse response");
  }

  absl::Status s = socket_.ReceiveFds(fds, co_);
  if (!s.ok()) {
    socket_.Close();
  }

  return s;
}

} // namespace subspace
