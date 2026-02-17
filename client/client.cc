// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/client.h"
#include "absl/strings/str_format.h"
#include "client.h"
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

// Get the current thread as a 64 bit number.
static uint64_t GetThreadId() {
  return reinterpret_cast<uint64_t>(pthread_self());
}

ClientImpl::ClientLockGuard::ClientLockGuard(ClientImpl *client,
                                             LockMode lock_mode)
    : client_(client), lock_mode_(lock_mode) {
  if (!client_->thread_safe_) {
    return;
  }
  switch (lock_mode_) {
  case LockMode::kAutoLock:
    Lock();
    break;
  case LockMode::kMaybeLocked:
  case LockMode::kDeferredLock: {
    uint64_t old_thread_id = GetThreadId();
    uint64_t new_thread_id = old_thread_id;
    // If we the current owner of the lock we allow to continue without
    // relocking. If we are not the current owner, we lock the mutex.
    if (!client_->owner_thread_id_.compare_exchange_strong(
            old_thread_id, new_thread_id, std::memory_order_relaxed)) {
      Lock();
    }
    client_->owner_thread_id_.store(new_thread_id, std::memory_order_relaxed);
    break;
  }
  }
}

ClientImpl::ClientLockGuard::~ClientLockGuard() {
  if (!client_->thread_safe_) {
    return;
  }
  switch (lock_mode_) {
  case LockMode::kAutoLock:
    Unlock();
    break;
  case LockMode::kDeferredLock:
    if (!committed_) {
      Unlock();
    }
    break;
  case LockMode::kMaybeLocked:
    Unlock();
    break;
  }
}

void ClientImpl::ClientLockGuard::Lock() {
  client_->mutex_.lock();
  locked_ = true;
}

void ClientImpl::ClientLockGuard::Unlock() {
  client_->mutex_.unlock();
  locked_ = false;
  client_->owner_thread_id_.store(0, std::memory_order_relaxed);
}

void ClientImpl::ClientLockGuard::CommitLock() {
  if (lock_mode_ != LockMode::kDeferredLock) {
    return;
  }
  committed_ = true;
}

absl::Status ClientImpl::CheckConnected() const {
  if (!socket_.Connected()) {
    return absl::InternalError(
        "Client is not connected to the server; have you called Init()?");
  }
  return absl::OkStatus();
}

absl::Status ClientImpl::Init(const std::string &server_socket,
                              const std::string &client_name) {
  ClientLockGuard guard(this);
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
  server_user_id_ = resp.init().user_id();
  server_group_id_ = resp.init().group_id();
  return absl::OkStatus();
}

absl::Status ClientImpl::RegisterDroppedMessageCallback(
    SubscriberImpl *subscriber,
    std::function<void(SubscriberImpl *, int64_t)> callback) {
  ClientLockGuard guard(this);
  if (dropped_message_callbacks_.find(subscriber) !=
      dropped_message_callbacks_.end()) {
    return absl::InternalError(
        absl::StrFormat("A dropped message callback has already been "
                        "registered for channel %s\n",
                        subscriber->Name()));
  }
  dropped_message_callbacks_[subscriber] = std::move(callback);
  return absl::OkStatus();
}

absl::Status
ClientImpl::UnregisterDroppedMessageCallback(SubscriberImpl *subscriber) {
  ClientLockGuard guard(this);
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
  ClientLockGuard guard(this);
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
  ClientLockGuard guard(this);
  auto it = message_callbacks_.find(subscriber);
  if (it == message_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "No message callback has been registered for channel %s\n",
        subscriber->Name()));
  }
  message_callbacks_.erase(it);
  return absl::OkStatus();
}

absl::Status ClientImpl::RegisterResizeCallback(
    PublisherImpl *publisher,
    std::function<absl::Status(PublisherImpl *, int32_t, int32_t)> callback) {
  ClientLockGuard guard(this);
  if (resize_callbacks_.find(publisher) != resize_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "A resize callback has already been registered for channel %s\n",
        publisher->Name()));
  }
  resize_callbacks_[publisher] = std::move(callback);
  return absl::OkStatus();
}

absl::Status ClientImpl::UnregisterResizeCallback(PublisherImpl *publisher) {
  ClientLockGuard guard(this);
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
  std::function<void(details::SubscriberImpl *, Message)> callback;
  {
    ClientLockGuard guard(this);
    auto it = message_callbacks_.find(subscriber);
    if (it == message_callbacks_.end()) {
      return absl::InternalError(absl::StrFormat(
          "No message callback has been registered for channel %s\n",
          subscriber->Name()));
    }
    callback = it->second;
  }

  for (;;) {
    absl::StatusOr<Message> msg = ReadMessage(subscriber, mode);
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length == 0) {
      break;
    }
    callback(subscriber, std::move(*msg));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<Message>>
ClientImpl::GetAllMessages(details::SubscriberImpl *subscriber, ReadMode mode) {
  std::vector<Message> r;
  for (;;) {
    absl::StatusOr<Message> msg = ReadMessage(subscriber, mode);
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length == 0) {
      break;
    }
    r.push_back(std::move(*msg));
  }
  // We we are out of unique_ptrs, untrigger the subscriber so that it will be
  // retriggered when a unique_ptr is released.
  if (subscriber->NumActiveMessages() == subscriber->MaxActiveMessages()) {
    // std::cerr << "read {} messages, untriggering subscriber\n"_format(
    //         subscriber->numActiveMessages());
    subscriber->Untrigger();
    return r;
  }

  if (!r.empty()) {
    subscriber->Trigger();
  }
  return r;
}

absl::StatusOr<Publisher>
ClientImpl::CreatePublisher(const std::string &channel_name,
                            const PublisherOptions &opts) {
  ClientLockGuard guard(this);
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
  cmd->set_notify_retirement(opts.notify_retirement);

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
      pub_resp.type(), opts,
      [this](Channel *c) {
        return CheckReload(static_cast<ClientChannel *>(c));
      },
      server_user_id_, server_group_id_);

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

  // An unreliable publisher always needs a slot but if we are a bridge we
  // don't active the channels as the original publisher's activation message
  // will be used.
  if (!opts.IsReliable()) {
    // A publisher needs a slot.  Allocate one.
    MessageSlot *slot =
        channel->FindFreeSlotUnreliable(channel->GetPublisherId());
    if (slot == nullptr) {
      return absl::InternalError("No slot available for publisher");
    }
    channel->SetSlot(slot);
    if (!opts.IsBridge() && opts.Activate()) {
      if (absl::Status status = ActivateChannel(channel.get()); !status.ok()) {
        return status;
      }
    }
  } else if (!opts.IsBridge()) {
    // Send a single activation message to the channel.
    absl::Status status = ActivateReliableChannel(channel.get());
    if (!status.ok()) {
      return status;
    }
  }
  // Retirement fds.
  if (pub_resp.retirement_fd_index() != -1) {
    channel->SetRetirementFd(
        std::move(fds[size_t(pub_resp.retirement_fd_index())]));
  }

  channel->ClearRetirementTriggers();
  for (auto index : pub_resp.retirement_fd_indexes()) {
    channel->AddRetirementTrigger(fds[size_t(index)]);
  }

  channel->TriggerSubscribers();
  if (absl::Status status = channel->UnmapUnusedBuffers(); !status.ok()) {
    return status;
  }
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
  ClientLockGuard guard(this);
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
      sub_resp.type(), opts,
      [this](Channel *c) {
        return CheckReload(static_cast<ClientChannel *>(c));
      },
      server_user_id_, server_group_id_);

  channel->SetNumSlots(sub_resp.num_slots());

  SharedMemoryFds channel_fds(std::move(fds[sub_resp.ccb_fd_index()]),
                              std::move(fds[sub_resp.bcb_fd_index()]));
  if (absl::Status status = channel->Map(std::move(channel_fds), scb_fd_);
      !status.ok()) {
    return status;
  }

  channel->InitActiveMessages();

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

  // Retirement fds.
  channel->ClearRetirementTriggers();
  for (auto index : sub_resp.retirement_fd_indexes()) {
    channel->AddRetirementTrigger(fds[size_t(index)]);
  }

  channel->SetNumUpdates(sub_resp.num_pub_updates());

  // Trigger the subscriber to pick up all existing messages.
  channel->Trigger();

  // channel->Dump();

  channels_.insert(channel);
  auto sub = Subscriber(shared_from_this(), channel);
  return sub;
}

static uint64_t ExpandSlotSize(uint64_t slotSize) {
  // smaller than 4K, double the size
  // 4K..16K, times 1.5
  // 16K..64K, times 1.25
  // 64K..256K, times 1.125
  // 256K..1M, times 1.0625
  // 1M..., times 1.03125
  static constexpr double multipliers[] = {2.0,   1.5,    1.25,
                                           1.125, 1.0625, 1.03125};
  static constexpr size_t sizeRanges[] = {4096, 16384, 65536, 262144, 1048576};
  size_t i = 0;
  for (; i < std::size(sizeRanges); i++) {
    if (slotSize <= sizeRanges[i]) {
      break;
    }
  }
  // i will be the index of the multiplier to use.  It might be one past the
  // end of the sizeRanges array.
  return Aligned(uint64_t(double(slotSize) * multipliers[i]));
}

absl::StatusOr<void *> ClientImpl::GetMessageBuffer(PublisherImpl *publisher,
                                                    int32_t max_size,
                                                    bool lock) {
  auto span_or_status = GetMessageBufferSpan(publisher, max_size, lock);
  if (!span_or_status.ok()) {
    return span_or_status.status();
  }
  if (absl::Span<std::byte> span = span_or_status.value(); !span.empty()) {
    return span.data();
  }
  return nullptr;
}

absl::StatusOr<absl::Span<std::byte>>
ClientImpl::GetMessageBufferSpan(PublisherImpl *publisher, int32_t max_size,
                                 bool lock) {
  // If the current thread is calling this while it already owns the mutex we
  // allow it to continue without locking.  If another t thread is trying to
  // call this lock the mutex until the current thread releases it.
  ClientLockGuard guard(this, lock ? LockMode::kDeferredLock
                                   : LockMode::kMaybeLocked);
  if (publisher->IsReliable()) {
    publisher->ClearPollFd();
  }

  int32_t slot_size = publisher->SlotSize();
  size_t span_size = size_t(slot_size);
  if (max_size != -1 && max_size > slot_size) {
    int32_t new_slot_size = slot_size;
    assert(new_slot_size > 0);
    while (new_slot_size <= slot_size || new_slot_size < max_size) {
      new_slot_size = ExpandSlotSize(new_slot_size);
      span_size = size_t(new_slot_size);
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
      return absl::Span<std::byte>();
    }
    MessageSlot *slot =
        publisher->FindFreeSlotReliable(publisher->GetPublisherId());
    if (slot == nullptr) {
      return absl::Span<std::byte>();
    }
    publisher->SetSlot(slot);
  }

  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Channel %s has no buffer.  This should never happen.",
                        publisher->Name()));
  }
  // If we are returning a valid message buffer we commit the lock so that we
  // can hold onto it until the message is published or cancelled.
  if (span_size > 0) {
    guard.CommitLock();
  }
  return absl::Span<std::byte>(reinterpret_cast<std::byte *>(buffer),
                               span_size);
}

absl::StatusOr<const Message>
ClientImpl::PublishMessage(PublisherImpl *publisher, int64_t message_size) {
  return PublishMessageInternal(publisher, message_size, /*omit_prefix=*/false,
                                /*use_prefix_slot_id=*/false);
}

absl::StatusOr<const Message>
ClientImpl::PublishMessageInternal(PublisherImpl *publisher,
                                   int64_t message_size, bool omit_prefix,
                                   bool use_prefix_slot_id) {
  // Lock is already held by the call to GetMessageBufferSpan.  This RAII
  // instance wil relesas the lock when we return from this function.
  ClientLockGuard guard(this, LockMode::kMaybeLocked);

  // Check if there are any new subscribers and if so, load their trigger fds.
  if (absl::Status status = ReloadSubscribersIfNecessary(publisher);
      !status.ok()) {
    return status;
  }

  if (message_size == 0) {
    return absl::InternalError("Message size must be greater than 0");
  }

  int32_t old_slot_id = publisher->CurrentSlotId();

  if (publisher->on_send_callback_ != nullptr) {
    absl::StatusOr<int64_t> status_or_size = publisher->on_send_callback_(
        publisher->GetCurrentBufferAddress(), message_size);
    if (!status_or_size.ok()) {
      return status_or_size.status();
    }
    message_size = status_or_size.value();
  }
  publisher->SetMessageSize(message_size);
  MessageSlot *old_slot = publisher->CurrentSlot();
  if (debug_) {
    if (old_slot != nullptr) {
      printf("publish old slot: %d: %" PRId64 "\n", old_slot->id,
             old_slot->ordinal);
    }
  }

  Channel::PublishedMessage msg = publisher->ActivateSlotAndGetAnother(
      publisher->IsReliable(), /*is_activation=*/false, omit_prefix,
      use_prefix_slot_id);

  // Prevent use of old_slot.
  old_slot = nullptr;

  publisher->SetSlot(msg.new_slot);

  publisher->TriggerSubscribers();
  if (absl::Status status = publisher->UnmapUnusedBuffers(); !status.ok()) {
    return status;
  }

  if (msg.new_slot == nullptr) {
    if (publisher->IsReliable()) {
      // Reliable publishers don't get a slot until it's asked for.
      return Message(message_size, nullptr, msg.ordinal, msg.timestamp,
                     publisher->VirtualChannelId(), false, -1, false);
    }
    return absl::InternalError(
        absl::StrFormat("Out of slots for channel %s", publisher->Name()));
  }

  if (debug_) {
    printf("publish new slot: %d: %" PRId64 "\n", msg.new_slot->id,
           msg.new_slot->ordinal);
  }

  return Message(message_size, nullptr, msg.ordinal, msg.timestamp,
                 publisher->VirtualChannelId(), false, old_slot_id, false);
}

void ClientImpl::CancelPublish(PublisherImpl *publisher) {
  // Creating this object will unlock the mutex when it goes out of scope.
  ClientLockGuard guard(this, LockMode::kMaybeLocked);
}

absl::Status
ClientImpl::WaitForReliablePublisher(PublisherImpl *publisher,
                                     std::chrono::nanoseconds timeout,
                                     const co::Coroutine *c) {
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
  uint64_t timeout_ns = timeout.count();
  int result = -1;
  if (c != nullptr) {
    result = c->Wait(publisher->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result = co_->Wait(publisher->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else {
    struct pollfd fd = {.fd = publisher->GetPollFd().Fd(), .events = POLLIN};
    int e = ::poll(&fd, 1, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    // Since we are waiting forever will can only get the value 1 from the poll.
    // We will never get 0 since there is no timeout.  Anything else (can only
    // be -1) will be an error.
    if (timeout_ns != 0 && e == 0) {
      result = -1;
    } else if (e != 1) {
      return absl::InternalError(
          absl::StrFormat("Error from poll waiting for reliable publisher: %s",
                          strerror(errno)));
    } else {
      result = fd.fd;
    }
  }
  if (result == -1) {
    return absl::InternalError("Timeout waiting for reliable publisher");
  }
  return absl::OkStatus();
}

absl::StatusOr<int>
ClientImpl::WaitForReliablePublisher(PublisherImpl *publisher,
                                     const toolbelt::FileDescriptor &fd,
                                     std::chrono::nanoseconds timeout,

                                     const co::Coroutine *c) {
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
  int result = -1;
  uint64_t timeout_ns = timeout.count();
  if (c != nullptr) {
    result =
        c->Wait({publisher->GetPollFd().Fd(), fd.Fd()}, POLLIN, timeout_ns);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result =
        co_->Wait({publisher->GetPollFd().Fd(), fd.Fd()}, POLLIN, timeout_ns);
  } else {
    struct pollfd fds[2] = {
        {.fd = publisher->GetPollFd().Fd(), .events = POLLIN},
        {.fd = fd.Fd(), .events = POLLIN}};
    int e = ::poll(fds, 2, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    if (timeout_ns == 0 && e == 0) {
      return absl::InternalError("Timeout waiting for reliable publisher");
    }
    if (e < 0) {
      return absl::InternalError(
          absl::StrFormat("Error from poll waiting for reliable publisher: %s",
                          strerror(errno)));
    }
    if (fds[0].revents & (POLLIN | POLLHUP)) {
      result = fds[0].fd; // The publisher's poll fd triggered.
    } else if (fds[1].revents & (POLLIN | POLLHUP)) {
      result = fds[1].fd; // The passed in fd triggered.
    } else {
      return absl::InternalError(
          "Unexpected poll result, neither publisher nor passed in fd "
          "triggered");
    }
  }
  return result;
}

absl::Status ClientImpl::WaitForSubscriber(SubscriberImpl *subscriber,
                                           std::chrono::nanoseconds timeout,
                                           const co::Coroutine *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }

  uint64_t timeout_ns = timeout.count();
  int result = -1;
  if (c != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result = c->Wait(subscriber->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result = co_->Wait(subscriber->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else {
    struct pollfd fd = {.fd = subscriber->GetPollFd().Fd(), .events = POLLIN};
    int e = ::poll(&fd, 1, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    // Since we are waiting forever will can only get the value 1 from the poll.
    // We will never get 0 since there is no timeout.  Anything else (can only
    // be -1) will be an error.
    if (timeout_ns != 0 && e == 0) {
      result = -1;
    } else if (e != 1) {
      return absl::InternalError(absl::StrFormat(
          "Error from poll waiting for subscriber: %s", strerror(errno)));
    } else {
      result = fd.fd;
    }
  }
  if (result == -1) {
    return absl::InternalError("Timeout waiting for subscriber");
  }
  return absl::OkStatus();
}

absl::StatusOr<int> ClientImpl::WaitForSubscriber(
    SubscriberImpl *subscriber, const toolbelt::FileDescriptor &fd,
    std::chrono::nanoseconds timeout, const co::Coroutine *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  int result = -1;
  uint64_t timeout_ns = timeout.count();
  if (c != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result =
        c->Wait({subscriber->GetPollFd().Fd(), fd.Fd()}, POLLIN, timeout_ns);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result =
        co_->Wait({subscriber->GetPollFd().Fd(), fd.Fd()}, POLLIN, timeout_ns);
  } else {
    struct pollfd fds[2] = {
        {.fd = subscriber->GetPollFd().Fd(), .events = POLLIN},
        {.fd = fd.Fd(), .events = POLLIN}};
    int e = ::poll(fds, 2, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    if (timeout_ns == 0 && e == 0) {
      return absl::InternalError("Timeout waiting for subscriber");
    }
    if (e < 0) {
      return absl::InternalError(absl::StrFormat(
          "Error from poll waiting for subscriber: %s", strerror(errno)));
    }
    if (fds[0].revents & (POLLIN | POLLHUP)) {
      // The subscriber's poll fd triggered.
      return fds[0].fd;
    } else if (fds[1].revents & (POLLIN | POLLHUP)) {
      // The passed in fd triggered.
      return fds[1].fd;
    } else {
      return absl::InternalError(
          "Unexpected poll result, neither subscriber nor passed in fd "
          "triggered");
    }
  }
  return result;
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
    new_slot = subscriber->NextSlot();
    break;
  case ReadMode::kReadNewest:
    new_slot = subscriber->LastSlot();
    break;
  }

  // At this point, old_slot may have been reused so don't reference it
  // for any data.
  old_slot = nullptr; // Prevent any accidental use.

  if (new_slot == nullptr) {
    // I'm out of messages to read, trigger the publishers to give me
    // some more.  This is only for reliable publishers.
    subscriber->TriggerReliablePublishers();
    if (absl::Status status = subscriber->UnmapUnusedBuffers(); !status.ok()) {
      return status;
    }
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
      subscriber->RecordDroppedMessages(drops);
      if (subscriber->options_.log_dropped_messages) {
        logger_.Log(toolbelt::LogLevel::kWarning,
                    "Dropped %d message%s on channel %s", drops,
                    drops == 1 ? "" : "s", subscriber->Name().c_str());
      }
    }
  }

  MessagePrefix *prefix = subscriber->Prefix(new_slot);

  bool is_activation = false;
  bool checksum_error = false;
  if (prefix != nullptr) {
    if (prefix->HasChecksum()) {
      auto data =
          GetMessageChecksumData(prefix, subscriber->GetCurrentBufferAddress(),
                                 new_slot->message_size);
      checksum_error = !subscriber->ValidateChecksum(data, prefix->checksum);
    }
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
  // Call the on receive callback.
  if (subscriber->on_receive_callback_ != nullptr) {
    absl::StatusOr<int64_t> status_or_size = subscriber->on_receive_callback_(
        subscriber->GetCurrentBufferAddress(), new_slot->message_size);
    if (!status_or_size.ok()) {
      return status_or_size.status();
    }
    new_slot->message_size = status_or_size.value();
  }
  if (new_slot->message_size <= 0) {
    return Message();
  }
  // We have a new slot, clear the subscriber's slot.
  subscriber->ClearActiveMessage();

  // Allocate a new active message for the slot.
  auto msg = subscriber->SetActiveMessage(
      new_slot->message_size, new_slot, subscriber->GetCurrentBufferAddress(),
      subscriber->CurrentOrdinal(), subscriber->Timestamp(new_slot),
      new_slot->vchan_id, is_activation, checksum_error);

  // If we are unable to allocate a new message (due to message limits)
  // restore the slot so that we pick it up next time.
  if (msg->length == 0) {
    subscriber->UnreadSlot(new_slot);
    // Subscriber does not have a slot now but the slot it had is still active.
  } else {
    // We have a slot, claim it.
    subscriber->ClaimSlot(new_slot, subscriber->VirtualChannelId(),
                          mode == ReadMode::kReadNewest);
  }
  auto ret_msg = Message(msg);
  if (subscriber->IsBridge()) {
    // Bridge subscribers don't hold onto the message in the subscriber to allow
    // timely slot retirement.  If they hold onto it, the slot would not be
    // retired until the next message is read.  This only affect bridge
    // subscribers which are used to send data between servers.
    subscriber->ClearActiveMessage();
  }
  if (checksum_error && !subscriber->PassChecksumErrors()) {
    return absl::InternalError("Checksum verification failed");
  }
  // A checksum error that is ignored results in a valid message with the
  // checksum_error flag set.
  return ret_msg;
}

absl::StatusOr<Message> ClientImpl::ReadMessage(SubscriberImpl *subscriber,
                                                ReadMode mode) {

  ClientLockGuard guard(this);
  // If the channel is a placeholder (no publishers present), look
  // in the SCB to see if a new publisher has been created and if so,
  // talk to the server to get the information to reload the shared
  // memory.  If there still isn't a publisher, we will still be a
  // placeholder.
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
                 subscriber->VirtualChannelId(), false, new_slot->id, false);
}

absl::StatusOr<Message> ClientImpl::FindMessage(SubscriberImpl *subscriber,
                                                uint64_t timestamp) {

  ClientLockGuard guard(this);

  // If the channel is a placeholder (no publishers present), contact the
  // server to see if there is now a publisher.  This will reload the shared
  // memory.  If there still isn't a publisher, we will still be a
  // placeholder.
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

int64_t ClientImpl::GetCurrentOrdinal(SubscriberImpl *sub) {
  ClientLockGuard guard(this);
  MessageSlot *slot = sub->CurrentSlot();
  if (slot == nullptr) {
    return -1;
  }
  return slot->ordinal;
}

bool ClientImpl::CheckReload(ClientChannel *channel) {
  auto reloaded = ReloadBuffersIfNecessary(channel);
  if (!reloaded.ok()) {
    return false;
  }
  return *reloaded;
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
  subscriber->InitActiveMessages();

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

  // Retirement fds.
  subscriber->ClearRetirementTriggers();
  for (auto index : sub_resp.retirement_fd_indexes()) {
    subscriber->AddRetirementTrigger(fds[size_t(index)]);
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

  publisher->ClearRetirementTriggers();
  for (auto index : sub_resp.retirement_fd_indexes()) {
    publisher->AddRetirementTrigger(fds[size_t(index)]);
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

  subscriber->ClearRetirementTriggers();
  for (auto index : sub_resp.retirement_fd_indexes()) {
    subscriber->AddRetirementTrigger(fds[size_t(index)]);
  }

  return absl::OkStatus();
}

// A reliable publisher always sends a single activation message when it
// is created.  This is to ensure that the reliable subscribers see
// on message and thus keep a reference to it.
absl::Status ClientImpl::ActivateReliableChannel(PublisherImpl *publisher) {
  MessageSlot *slot =
      publisher->FindFreeSlotReliable(publisher->GetPublisherId());
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
      /*omit_prefix=*/false, /*use_prefix_slot_id=*/false);
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
        absl::StrFormat("3 Channel %s has no buffer", publisher->Name()));
  }
  MessageSlot *slot = publisher->CurrentSlot();
  slot->message_size = 1;

  Channel::PublishedMessage msg = publisher->ActivateSlotAndGetAnother(
      /*reliable=*/false,
      /*is_activation=*/true,
      /*omit_prefix=*/false, /*use_prefix_slot_id=*/false);
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
  ClientLockGuard guard(this);
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
  ClientLockGuard guard(this);
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

absl::StatusOr<const ChannelCounters>
ClientImpl::GetChannelCounters(const std::string &channel_name) const {
  // TODO: find a better way to search for a channel.
  for (auto it = channels_.begin(); it != channels_.end(); ++it) {
    if ((*it)->Name() == channel_name) {
      return (*it)->GetCounters();
    }
  }
  return absl::InternalError(
      absl::StrFormat("Channel %s doesn't exist", channel_name));
}

const ChannelCounters &ClientImpl::GetChannelCounters(ClientChannel *channel) {
  return channel->GetCounters();
}

absl::StatusOr<const ChannelInfo>
ClientImpl::GetChannelInfo(const std::string &channel) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  Request req;
  auto *cmd = req.mutable_get_channel_info();
  cmd->set_channel_name(channel);

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = SendRequestReceiveResponse(req, response, fds);
      !status.ok()) {
    return status;
  }

  auto &resp = response.get_channel_info();
  if (!resp.error().empty()) {
    return absl::InternalError(resp.error());
  }
  // There will be one channel information in the response.
  if (resp.channels_size() != 1) {
    return absl::InternalError("Invalid response for getChannelInfo");
  }
  ChannelInfo result;
  const ChannelInfoProto &info = resp.channels()[0];
  result.channel_name = info.name();
  result.num_publishers = info.num_pubs();
  result.num_subscribers = info.num_subs();
  result.num_bridge_pubs = info.num_bridge_pubs();
  result.num_bridge_subs = info.num_bridge_subs();
  result.reliable = info.is_reliable();
  result.type = info.type();
  result.slot_size = info.slot_size();
  result.num_slots = info.num_slots();
  return result;
}

absl::StatusOr<const std::vector<ChannelInfo>> ClientImpl::GetChannelInfo() {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  Request req;
  [[maybe_unused]] auto cmd = req.mutable_get_channel_info();

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = SendRequestReceiveResponse(req, response, fds);
      !status.ok()) {
    return status;
  }
  auto &resp = response.get_channel_info();
  if (!resp.error().empty()) {
    return absl::InternalError(resp.error());
  }
  std::vector<ChannelInfo> r;
  for (auto &info : resp.channels()) {
    ChannelInfo result;
    result.channel_name = info.name();
    result.num_publishers = info.num_pubs();
    result.num_subscribers = info.num_subs();
    result.num_bridge_pubs = info.num_bridge_pubs();
    result.num_bridge_subs = info.num_bridge_subs();
    result.reliable = info.is_reliable();
    result.type = info.type();
    result.slot_size = info.slot_size();
    result.num_slots = info.num_slots();
    r.push_back(result);
  }
  return r;
}

absl::StatusOr<bool> ClientImpl::ChannelExists(const std::string &channelName) {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  absl::StatusOr<const ChannelInfo> info = GetChannelInfo(channelName);
  return info.ok();
}

absl::StatusOr<const ChannelStats>
ClientImpl::GetChannelStats(const std::string &channel) {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  Request req;
  auto *cmd = req.mutable_get_channel_stats();
  cmd->set_channel_name(channel);

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = SendRequestReceiveResponse(req, response, fds);
      !status.ok()) {
    return status;
  }
  auto &resp = response.get_channel_stats();
  if (!resp.error().empty()) {
    return absl::InternalError(resp.error());
  }
  // There will be one channel information in the response.
  if (resp.channels_size() != 1) {
    return absl::InternalError("Invalid response for getChannelStats");
  }
  ChannelStats result;
  const ChannelStatsProto &stats = resp.channels()[0];
  result.channel_name = stats.channel_name();
  result.total_bytes = stats.total_bytes();
  result.total_messages = stats.total_messages();
  result.max_message_size = stats.max_message_size();
  return result;
}

absl::StatusOr<const std::vector<ChannelStats>> ClientImpl::GetChannelStats() {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }

  Request req;
  [[maybe_unused]] auto cmd = req.mutable_get_channel_stats();

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = SendRequestReceiveResponse(req, response, fds);
      !status.ok()) {
    return status;
  }
  auto &resp = response.get_channel_stats();
  if (!resp.error().empty()) {
    return absl::InternalError(resp.error());
  }
  std::vector<ChannelStats> r;
  for (auto &stats : resp.channels()) {
    ChannelStats result;
    result.channel_name = stats.channel_name();
    result.total_bytes = stats.total_bytes();
    result.total_messages = stats.total_messages();
    result.max_message_size = stats.max_message_size();
    r.push_back(result);
  }
  return r;
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

  // std::cerr << "Sending request " << req.DebugString() << "\n";
  {
    // SendMessage needs 4 bytes before the buffer passed to
    // use for the length.
    size_t msg_len = req.ByteSizeLong();
    std::vector<char> send_msg(sizeof(int32_t) + msg_len);
    char *sendbuf = send_msg.data() + sizeof(int32_t);

    if (!req.SerializeToArray(sendbuf, msg_len)) {
      return absl::InternalError("Failed to serialize request");
    }

    absl::StatusOr<ssize_t> n = socket_.SendMessage(sendbuf, msg_len, co_);
    if (!n.ok()) {
      socket_.Close();
      return n.status();
    }
  }

  // Wait for response and any fds.
  absl::StatusOr<std::vector<char>> recv_msg =
      socket_.ReceiveVariableLengthMessage(co_);
  if (!recv_msg.ok()) {
    socket_.Close();
    return recv_msg.status();
  }
  if (!response.ParseFromArray(recv_msg->data(),
                               static_cast<int>(recv_msg->size()))) {
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
