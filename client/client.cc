// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/client.h"
#include "absl/strings/str_format.h"
#include "client.h"
#include "common/syscall_shim.h"
#include "proto/subspace.pb.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/mutex.h"
#include "toolbelt/sockets.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <inttypes.h>
#include <unistd.h>
#if defined(__QNX__) || defined(__QNXNTO__)
#include <sys/neutrino.h>
#endif
namespace subspace {

namespace {
void FillInitRequest(InitRequest *init, const std::string &client_name) {
  init->set_client_name(client_name);
  init->set_max_channels(kMaxChannels);
  init->set_max_slot_owners(kMaxSlotOwners);
  init->set_max_buffers(static_cast<int32_t>(kMaxBuffers));
}

std::atomic<bool> default_use_split_buffers{false};

#if defined(__QNX__) || defined(__QNXNTO__)
void ConfigureQnxClockPeriod() {
  static std::atomic<bool> attempted{false};
  bool expected = false;
  if (!attempted.compare_exchange_strong(expected, true,
                                         std::memory_order_relaxed)) {
    return;
  }

  struct _clockperiod period;
  period.nsec = 10000U;
  period.fract = 0;
  (void)ClockPeriod(CLOCK_REALTIME, &period, nullptr, 0);
}
#endif

ClientBufferAllocatorKind FromProtoAllocator(ClientBufferAllocator allocator) {
  switch (allocator) {
  case CLIENT_BUFFER_ALLOCATOR_ANDROID_MEMFD:
    return ClientBufferAllocatorKind::kAndroidMemfd;
  case CLIENT_BUFFER_ALLOCATOR_SPLIT_SHM:
    return ClientBufferAllocatorKind::kSplitShm;
  case CLIENT_BUFFER_ALLOCATOR_SPLIT_CALLBACK:
    return ClientBufferAllocatorKind::kSplitCallback;
  case CLIENT_BUFFER_ALLOCATOR_SPLIT_BUFFER_FREE_TEST:
    return ClientBufferAllocatorKind::kSplitBufferFreeTest;
  case CLIENT_BUFFER_ALLOCATOR_UNSPECIFIED:
  default:
    return ClientBufferAllocatorKind::kUnspecified;
  }
}

ClientBufferAllocator ToProtoAllocator(ClientBufferAllocatorKind allocator) {
  switch (allocator) {
  case ClientBufferAllocatorKind::kAndroidMemfd:
    return CLIENT_BUFFER_ALLOCATOR_ANDROID_MEMFD;
  case ClientBufferAllocatorKind::kSplitShm:
    return CLIENT_BUFFER_ALLOCATOR_SPLIT_SHM;
  case ClientBufferAllocatorKind::kSplitCallback:
    return CLIENT_BUFFER_ALLOCATOR_SPLIT_CALLBACK;
  case ClientBufferAllocatorKind::kSplitBufferFreeTest:
    return CLIENT_BUFFER_ALLOCATOR_SPLIT_BUFFER_FREE_TEST;
  case ClientBufferAllocatorKind::kUnspecified:
  default:
    return CLIENT_BUFFER_ALLOCATOR_UNSPECIFIED;
  }
}
} // namespace

using ClientChannel = details::ClientChannel;
using SubscriberImpl = details::SubscriberImpl;
using PublisherImpl = details::PublisherImpl;

void SetDefaultUseSplitBuffers(bool use_split_buffers) {
  default_use_split_buffers.store(use_split_buffers, std::memory_order_relaxed);
}

bool DefaultUseSplitBuffers() {
  return default_use_split_buffers.load(std::memory_order_relaxed);
}

// Get the current thread as a 64 bit number.  pthread_t is integral on some
// platforms (Linux glibc, QNX) and a pointer on others (macOS); on QNX it is
// only 32 bits wide.  Use memcpy so the conversion is well-defined regardless
// of the underlying type.
static uint64_t GetThreadId() {
  pthread_t tid = pthread_self();
  uint64_t id = 0;
  std::memcpy(&id, &tid, sizeof(tid) < sizeof(id) ? sizeof(tid) : sizeof(id));
  return id;
}

static void ToProto(const ClientBufferHandleMetadata &metadata,
                    ClientBufferHandleMetadataProto *proto) {
  proto->set_channel_name(metadata.channel_name);
  proto->set_session_id(metadata.session_id);
  proto->set_buffer_index(metadata.buffer_index);
  proto->set_slot_id(metadata.slot_id);
  proto->set_is_prefix(metadata.is_prefix);
  proto->set_full_size(metadata.full_size);
  proto->set_allocation_size(metadata.allocation_size);
  proto->set_handle(static_cast<uint64_t>(metadata.handle));
  proto->set_shadow_file(metadata.shadow_file);
  proto->set_object_name(metadata.object_name);
  proto->set_allocator(ToProtoAllocator(metadata.allocator));
  proto->set_map_offset(metadata.map_offset);
}

static ClientBufferHandleMetadata
FromProto(const ClientBufferHandleMetadataProto &proto) {
  ClientBufferHandleMetadata metadata;
  metadata.channel_name = proto.channel_name();
  metadata.session_id = proto.session_id();
  metadata.buffer_index = proto.buffer_index();
  metadata.slot_id = proto.slot_id();
  metadata.is_prefix = proto.is_prefix();
  metadata.full_size = proto.full_size();
  metadata.allocation_size = proto.allocation_size();
  metadata.handle = static_cast<uintptr_t>(proto.handle());
  metadata.shadow_file = proto.shadow_file();
  metadata.object_name = proto.object_name();
  metadata.allocator = FromProtoAllocator(proto.allocator());
  metadata.map_offset = proto.map_offset();
  return metadata;
}

static absl::Status
CheckFdIndex(const std::vector<toolbelt::FileDescriptor> &fds, int index,
             const char *field, const char *response_name) {
  if (index < 0 || static_cast<size_t>(index) >= fds.size()) {
    return absl::InternalError(absl::StrFormat(
        "%s references fd index %d for %s, but response included %zu fds",
        response_name, index, field, fds.size()));
  }
  if (!fds[static_cast<size_t>(index)].Valid()) {
    return absl::InternalError(
        absl::StrFormat("%s references invalid fd at index %d for %s",
                        response_name, index, field));
  }
  return absl::OkStatus();
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
  client_->owner_thread_id_.store(0, std::memory_order_relaxed);
  client_->mutex_.unlock();
  locked_ = false;
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

async::Task<absl::Status>
ClientImpl::Init(const std::string &server_socket,
                              const std::string &client_name) {
#if defined(__QNX__) || defined(__QNXNTO__)
  ConfigureQnxClockPeriod();
#endif
  ClientLockGuard guard(this);
  if (socket_.Connected()) {
    ASYNC_RETURN absl::InternalError("Client is already connected to the server; "
                               "Init() called twice perhaps?");
  }
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // A client without a yield_context is not driven by a coroutine: use blocking
  // socket I/O so it works without an io_context (the common case).
  socket_.SetBlocking(!IsCooperative());
#endif
  absl::Status status = socket_.Connect(server_socket);
  if (!status.ok()) {
    ASYNC_RETURN status;
  }

  name_ = client_name;
  socket_name_ = server_socket;
  Request req;
  FillInitRequest(req.mutable_init(), client_name);
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
  if (!status.ok()) {
    ASYNC_RETURN status;
  }
  if (!resp.init().error().empty()) {
    ASYNC_RETURN absl::InternalError(resp.init().error());
  }
  if (absl::Status fd_status = CheckFdIndex(fds, resp.init().scb_fd_index(),
                                            "scb_fd", "InitResponse");
      !fd_status.ok()) {
    ASYNC_RETURN fd_status;
  }
  scb_fd_ = std::move(fds[resp.init().scb_fd_index()]);
  session_id_ = resp.init().session_id();
  server_user_id_ = resp.init().user_id();
  server_group_id_ = resp.init().group_id();
  was_connected_ = true;
  ASYNC_RETURN absl::OkStatus();
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

absl::Status ClientImpl::RegisterStuckSubscriberCallback(
    SubscriberImpl *subscriber,
    std::function<void(SubscriberImpl *, double)> callback) {
  ClientLockGuard guard(this);
  if (stuck_subscriber_callbacks_.find(subscriber) !=
      stuck_subscriber_callbacks_.end()) {
    return absl::InternalError(
        absl::StrFormat("A stuck subscriber callback has already been "
                        "registered for channel %s\n",
                        subscriber->Name()));
  }
  stuck_subscriber_callbacks_[subscriber] = std::move(callback);
  return absl::OkStatus();
}

absl::Status
ClientImpl::UnregisterStuckSubscriberCallback(SubscriberImpl *subscriber) {
  ClientLockGuard guard(this);
  auto it = stuck_subscriber_callbacks_.find(subscriber);
  if (it == stuck_subscriber_callbacks_.end()) {
    return absl::InternalError(absl::StrFormat(
        "No stuck subscriber callback has been registered for channel %s\n",
        subscriber->Name()));
  }
  stuck_subscriber_callbacks_.erase(it);
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
    std::function<absl::Status(PublisherImpl *, int64_t, int64_t)> callback) {
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

async::Task<absl::Status>
ClientImpl::ProcessAllMessages(details::SubscriberImpl *subscriber,
                                            ReadMode mode) {
  std::function<void(details::SubscriberImpl *, Message)> callback;
  {
    ClientLockGuard guard(this);
    auto it = message_callbacks_.find(subscriber);
    if (it == message_callbacks_.end()) {
      ASYNC_RETURN absl::InternalError(absl::StrFormat(
          "No message callback has been registered for channel %s\n",
          subscriber->Name()));
    }
    callback = it->second;
  }

  for (;;) {
    absl::StatusOr<Message> msg = ASYNC_AWAIT(ReadMessage(subscriber, mode));
    if (!msg.ok()) {
      ASYNC_RETURN msg.status();
    }
    if (msg->length == 0) {
      break;
    }
    callback(subscriber, std::move(*msg));
  }
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::StatusOr<std::vector<Message>>>
ClientImpl::GetAllMessages(details::SubscriberImpl *subscriber, ReadMode mode) {
  std::vector<Message> r;
  for (;;) {
    absl::StatusOr<Message> msg = ASYNC_AWAIT(ReadMessage(subscriber, mode));
    if (!msg.ok()) {
      ASYNC_RETURN msg.status();
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
    ASYNC_RETURN r;
  }

  if (!r.empty()) {
    subscriber->Trigger();
  }
  ASYNC_RETURN r;
}

// Tells the server to drop a publisher it created for us when the client
// can't finish setting it up.  Best effort: the original failure is what the
// caller needs to see.
async::Task<void> ClientImpl::RemoveServerPublisher(
    const std::string &channel_name, int32_t publisher_id) {
  Request remove_req;
  auto *cmd = remove_req.mutable_remove_publisher();
  cmd->set_channel_name(channel_name);
  cmd->set_publisher_id(publisher_id);

  Response remove_resp;
  std::vector<toolbelt::FileDescriptor> remove_fds;
  (void)ASYNC_AWAIT(
      SendRequestReceiveResponse(remove_req, remove_resp, remove_fds));
  ASYNC_RETURN;
}

async::Task<absl::StatusOr<Publisher>>
ClientImpl::CreatePublisher(const std::string &channel_name,
                            const PublisherOptions &opts) {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  if (opts.MaxOutstandingSlotLeases() < 1 ||
      opts.MaxOutstandingSlotLeases() > opts.NumSlots()) {
    ASYNC_RETURN absl::InvalidArgumentError(
        "MaxOutstandingSlotLeases must be between 1 and NumSlots");
  }
  if (opts.max_slot_size < 0) {
    ASYNC_RETURN absl::InvalidArgumentError(absl::StrFormat(
        "MaxSlotSize must be non-negative, not %d", opts.max_slot_size));
  }
  if (opts.max_slot_size > 0 && opts.slot_size > opts.max_slot_size) {
    ASYNC_RETURN absl::InvalidArgumentError(absl::StrFormat(
        "Slot size %d for channel %s exceeds its maximum slot size of %d bytes",
        opts.slot_size, channel_name, opts.max_slot_size));
  }
  Request req;
  FillCreatePublisherRequest(req.mutable_create_publisher(), channel_name, opts,
                             -1);

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  auto &pub_resp = resp.create_publisher();
  if (!pub_resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(pub_resp.error());
  }
  // A publisher may join an existing channel with fewer slots than it has.
  // The CCB layout is sized by the channel's slot count, so map with that and
  // keep it in the options so that reregistration asks for the same count.
  // Servers that predate num_slots in the response send zero.
  PublisherOptions channel_opts = opts;
  if (pub_resp.num_slots() > 0) {
    channel_opts.num_slots = pub_resp.num_slots();
  }
  std::shared_ptr<PublisherImpl> channel = std::make_shared<PublisherImpl>(
      channel_name, channel_opts.num_slots, pub_resp.subscriber_queue_size(),
      pub_resp.subscriber_queue_arena_size(), pub_resp.channel_id(),
      pub_resp.publisher_id(), pub_resp.vchan_id(), session_id_,
      pub_resp.type(), channel_opts,
      [this](Channel *c) -> async::Task<bool> {
        ASYNC_RETURN ASYNC_AWAIT(CheckReload(static_cast<ClientChannel *>(c)));
      },
      server_user_id_, server_group_id_);
  // The channel awaits these, so they run on the caller's coroutine.
  channel->SetClientBufferRegistrationCallback(
      [this](const ClientBufferHandleMetadata &metadata,
             const toolbelt::FileDescriptor *fd)
          -> async::Task<absl::Status> {
        ASYNC_RETURN ASYNC_AWAIT(RegisterClientBuffer(metadata, fd));
      });
  channel->SetClientBufferLookupCallback(
      [this](const std::string &channel_name, uint64_t session_id,
             uint32_t buffer_index)
          -> async::Task<absl::StatusOr<std::vector<RegisteredClientBuffer>>> {
        ASYNC_RETURN ASYNC_AWAIT(
            GetClientBuffers(channel_name, session_id, buffer_index));
      });
  channel->SetClientBufferUnregistrationCallback(
      [this](const std::string &channel_name, uint64_t session_id,
             uint32_t buffer_index) -> async::Task<absl::Status> {
        ASYNC_RETURN ASYNC_AWAIT(
            UnregisterClientBuffer(channel_name, session_id, buffer_index));
      });
  int32_t cs = opts.ChecksumSize() > 0 ? opts.ChecksumSize() : 4;
  int32_t ms = opts.MetadataSize() > 0 ? opts.MetadataSize() : 0;
  channel->SetChecksumSize(cs);
  channel->SetMetadataSize(ms);
  channel->SetPrefixSize(Channel::ComputePrefixSize(cs, ms));

  if (absl::Status fd_status = CheckFdIndex(
          fds, pub_resp.ccb_fd_index(), "ccb_fd", "CreatePublisherResponse");
      !fd_status.ok()) {
    ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
    ASYNC_RETURN fd_status;
  }
  if (absl::Status fd_status = CheckFdIndex(
          fds, pub_resp.bcb_fd_index(), "bcb_fd", "CreatePublisherResponse");
      !fd_status.ok()) {
    ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
    ASYNC_RETURN fd_status;
  }
  SharedMemoryFds channel_fds(std::move(fds[pub_resp.ccb_fd_index()]),
                              std::move(fds[pub_resp.bcb_fd_index()]));
  if (absl::Status status = channel->Map(std::move(channel_fds), scb_fd_);
      !status.ok()) {
    ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
    ASYNC_RETURN status;
  }

  if (absl::Status status =
          ASYNC_AWAIT(channel->CreateOrAttachBuffers(Aligned(opts.slot_size)));
      !status.ok()) {
    ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
    ASYNC_RETURN status;
  }

  ApplyPublisherResponseFds(channel.get(), pub_resp, fds);

  if (!opts.IsReliable()) {
    MessageSlot *slot =
        ASYNC_AWAIT(channel->FindFreeSlotUnreliable(channel->GetPublisherId()));
    if (slot == nullptr) {
      ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
      ASYNC_RETURN absl::InternalError("No slot available for publisher");
    }
    channel->SetSlot(slot);
    if (!opts.IsBridge() && opts.Activate()) {
      if (absl::Status status = ASYNC_AWAIT(ActivateChannel(channel.get())); !status.ok()) {
        ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
        ASYNC_RETURN status;
      }
    }
  } else if (!opts.IsBridge() && !opts.ForTunnel()) {
    // Send a single activation message to the channel.  Don't activate if
    // this is a tunnel publisher because there will already be an existing
    // reliable publisher that has been activated.
    absl::Status status = ASYNC_AWAIT(ActivateReliableChannel(channel.get()));
    if (!status.ok()) {
      ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
      ASYNC_RETURN status;
    }
  }

  channel->TriggerSubscribers();
  if (absl::Status status = ASYNC_AWAIT(channel->UnmapUnusedBuffers()); !status.ok()) {
    ASYNC_AWAIT(RemoveServerPublisher(channel_name, pub_resp.publisher_id()));
    ASYNC_RETURN status;
  }
  channels_.insert(channel);
  ASYNC_RETURN Publisher(shared_from_this(), channel);
}

async::Task<absl::StatusOr<Publisher>>
ClientImpl::CreatePublisher(const std::string &channel_name, int64_t slot_size,
                            int num_slots, const PublisherOptions &opts) {
  PublisherOptions options = opts;
  options.slot_size = slot_size;
  options.num_slots = num_slots;
  ASYNC_RETURN ASYNC_AWAIT(CreatePublisher(channel_name, options));
}

async::Task<absl::StatusOr<Subscriber>>
ClientImpl::CreateSubscriber(const std::string &channel_name,
                             const SubscriberOptions &opts) {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  if (opts.MaxActiveMessages() < 1) {
    ASYNC_RETURN absl::InvalidArgumentError(
        "MaxActiveMessages must be at least 1 for a subscriber");
  }
  if (!std::isfinite(opts.StuckWarningGracePeriod()) ||
      opts.StuckWarningGracePeriod() < 0) {
    ASYNC_RETURN absl::InvalidArgumentError(absl::StrFormat(
        "StuckWarningGracePeriod must be a finite number of seconds that is "
        "at least 0, not %g",
        opts.StuckWarningGracePeriod()));
  }
  Request req;
  FillCreateSubscriberRequest(req.mutable_create_subscriber(), channel_name,
                              opts, -1);

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }

  auto &sub_resp = resp.create_subscriber();
  if (!sub_resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(sub_resp.error());
  }

  SubscriberOptions subscriber_options = opts;
  subscriber_options.use_split_buffers = sub_resp.use_split_buffers();
  // Telemetry subscribers map the hidden server channel while retaining the
  // requested target name for reload, trigger, and removal requests.
  const std::string mapped_channel_name =
      sub_resp.resolved_channel_name().empty()
          ? channel_name
          : sub_resp.resolved_channel_name();

  std::shared_ptr<SubscriberImpl> channel = std::make_shared<SubscriberImpl>(
      mapped_channel_name, sub_resp.num_slots(),
      sub_resp.default_subscriber_queue_size(),
      sub_resp.subscriber_queue_arena_size(),
      sub_resp.subscriber_queue_size(), sub_resp.channel_id(),
      sub_resp.subscriber_id(), sub_resp.vchan_id(), session_id_, sub_resp.type(),
      subscriber_options,
      [this](Channel *c) -> async::Task<bool> {
        ASYNC_RETURN ASYNC_AWAIT(CheckReload(static_cast<ClientChannel *>(c)));
      },
      server_user_id_, server_group_id_);
  channel->SetRequestName(channel_name);
  channel->SetClientBufferLookupCallback(
      [this](const std::string &channel_name, uint64_t session_id,
             uint32_t buffer_index)
          -> async::Task<absl::StatusOr<std::vector<RegisteredClientBuffer>>> {
        ASYNC_RETURN ASYNC_AWAIT(
            GetClientBuffers(channel_name, session_id, buffer_index));
      });

  channel->SetNumSlots(sub_resp.num_slots());
  channel->SetEffectiveSubscriberQueueSize(sub_resp.subscriber_queue_size());
  {
    int32_t cs = sub_resp.checksum_size() > 0 ? sub_resp.checksum_size() : 4;
    int32_t ms = sub_resp.metadata_size() > 0 ? sub_resp.metadata_size() : 0;
    channel->SetChecksumSize(cs);
    channel->SetMetadataSize(ms);
    channel->SetPrefixSize(Channel::ComputePrefixSize(cs, ms));
    channel->AllocateChecksumBuffer();
  }

  if (absl::Status fd_status = CheckFdIndex(
          fds, sub_resp.ccb_fd_index(), "ccb_fd", "CreateSubscriberResponse");
      !fd_status.ok()) {
    ASYNC_RETURN fd_status;
  }
  if (absl::Status fd_status = CheckFdIndex(
          fds, sub_resp.bcb_fd_index(), "bcb_fd", "CreateSubscriberResponse");
      !fd_status.ok()) {
    ASYNC_RETURN fd_status;
  }
  SharedMemoryFds channel_fds(std::move(fds[sub_resp.ccb_fd_index()]),
                              std::move(fds[sub_resp.bcb_fd_index()]));
  if (absl::Status status = channel->Map(std::move(channel_fds), scb_fd_);
      !status.ok()) {
    ASYNC_RETURN status;
  }

  channel->InitActiveMessages();

  if (absl::Status status = ASYNC_AWAIT(channel->AttachBuffers()); !status.ok()) {
    ASYNC_RETURN status;
  }
  channel->RememberPublishedOrdinals();

  ApplySubscriberResponseFds(channel.get(), sub_resp, fds);

  channel->Trigger();

  channels_.insert(channel);
  ASYNC_RETURN Subscriber(shared_from_this(), channel);
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

async::Task<absl::StatusOr<absl::Span<std::byte>>>
ClientImpl::GetMessageBufferSpan(PublisherImpl *publisher, int64_t max_size,
                                 bool lock) {
  // If the current thread is calling this while it already owns the mutex we
  // allow it to continue without locking.  If another t thread is trying to
  // call this lock the mutex until the current thread releases it.
  ClientLockGuard guard(this, lock ? LockMode::kDeferredLock
                                   : LockMode::kMaybeLocked);
  if (publisher->IsReliable()) {
    publisher->ClearPollFd();
  }

  int64_t slot_size = publisher->SlotSize();
  size_t span_size = size_t(slot_size);
  if (max_size != -1 && max_size > slot_size) {
    const int64_t max_slot_size = publisher->MaxSlotSize();
    if (max_slot_size != 0 && max_size > max_slot_size) {
      ASYNC_RETURN absl::InvalidArgumentError(absl::StrFormat(
          "Requested buffer of %d bytes on channel %s exceeds its maximum slot "
          "size of %d bytes",
          max_size, publisher->Name(), max_slot_size));
    }
    int64_t new_slot_size = slot_size;
    assert(new_slot_size > 0);
    while (new_slot_size <= slot_size || new_slot_size < max_size) {
      new_slot_size = ExpandSlotSize(new_slot_size);
    }
    // The growth multiplier can overshoot the cap.  Clamping is safe because
    // max_size <= max_slot_size was checked above, so the clamped size still
    // satisfies the request and is still larger than the current slot size.
    if (max_slot_size != 0 && new_slot_size > max_slot_size) {
      new_slot_size = max_slot_size;
    }
    span_size = size_t(new_slot_size);

    if (absl::Status status = ASYNC_AWAIT(ResizeChannel(publisher, new_slot_size));
        !status.ok()) {
      ASYNC_RETURN status;
    }
    publisher->SetSlotToBiggestBuffer(publisher->CurrentSlot());
  }

  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
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
      ASYNC_RETURN absl::Span<std::byte>();
    }
    MessageSlot *slot =
        ASYNC_AWAIT(publisher->FindFreeSlotReliable(publisher->GetPublisherId()));
    if (slot == nullptr) {
      ASYNC_RETURN absl::Span<std::byte>();
    }
    publisher->SetSlot(slot);
  }

  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    ASYNC_RETURN absl::InternalError(
        absl::StrFormat("Channel %s has no buffer.  This should never happen.",
                        publisher->Name()));
  }
  // If we are returning a valid message buffer we commit the lock so that we
  // can hold onto it until the message is published or cancelled.
  if (span_size > 0) {
    guard.CommitLock();
  }
  ASYNC_RETURN absl::Span<std::byte>(reinterpret_cast<std::byte *>(buffer),
                               span_size);
}

async::Task<absl::StatusOr<PublisherBufferLease>>
ClientImpl::AcquirePublisherBuffer(PublisherImpl *publisher) {
  ClientLockGuard guard(this);
  if (publisher->NumLeases() >= size_t(publisher->MaxOutstandingSlotLeases())) {
    ASYNC_RETURN PublisherBufferLease{};
  }
  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }

  MessageSlot *slot = publisher->CurrentSlot();
  if (slot != nullptr) {
    publisher->SetSlot(nullptr);
  } else {
    if (publisher->IsReliable() &&
        publisher->NumSubscribers(publisher->VirtualChannelId()) == 0) {
      ASYNC_RETURN PublisherBufferLease{};
    }
    slot = publisher->IsReliable()
               ? ASYNC_AWAIT(publisher->FindFreeSlotReliable(publisher->GetPublisherId()))
               : ASYNC_AWAIT(publisher->FindFreeSlotUnreliable(publisher->GetPublisherId()));
  }
  if (slot == nullptr) {
    ASYNC_RETURN PublisherBufferLease{};
  }
  uint64_t lease_id = publisher->RegisterLease(slot);
  ASYNC_RETURN PublisherBufferLease{
      .buffer = publisher->GetBufferAddress(slot),
      .buffer_size = size_t(publisher->SlotSize(slot)),
      .slot_id = slot->id,
      .lease_id = lease_id,
  };
}

async::Task<absl::StatusOr<PublisherBufferLease>>
ClientImpl::ReclaimPublisherBuffer(PublisherImpl *publisher, int32_t slot_id) {
  ClientLockGuard guard(this);
  if (publisher->NumLeases() >= size_t(publisher->MaxOutstandingSlotLeases())) {
    ASYNC_RETURN PublisherBufferLease{};
  }
  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }
  MessageSlot *slot = publisher->ClaimRetiredSlot(slot_id);
  if (slot == nullptr) {
    ASYNC_RETURN PublisherBufferLease{};
  }
  uint64_t lease_id = publisher->RegisterLease(slot);
  ASYNC_RETURN PublisherBufferLease{
      .buffer = publisher->GetBufferAddress(slot),
      .buffer_size = size_t(publisher->SlotSize(slot)),
      .slot_id = slot->id,
      .lease_id = lease_id,
  };
}

async::Task<absl::StatusOr<PublisherBufferLease>>
ClientImpl::ReclaimAnyPublisherBuffer(PublisherImpl *publisher) {
  ClientLockGuard guard(this);
  if (publisher->NumLeases() >=
      size_t(publisher->MaxOutstandingSlotLeases())) {
    ASYNC_RETURN PublisherBufferLease{};
  }
  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }
  MessageSlot *slot = publisher->ClaimAnyRetiredSlot();
  if (slot == nullptr) {
    ASYNC_RETURN PublisherBufferLease{};
  }
  uint64_t lease_id = publisher->RegisterLease(slot);
  ASYNC_RETURN PublisherBufferLease{
      .buffer = publisher->GetBufferAddress(slot),
      .buffer_size = size_t(publisher->SlotSize(slot)),
      .slot_id = slot->id,
      .lease_id = lease_id,
  };
}

async::Task<absl::StatusOr<const Message>>
ClientImpl::PublishPublisherBuffer(PublisherImpl *publisher,
                                   const PublisherBufferLease &lease,
                                   int64_t message_size) {
  ClientLockGuard guard(this);
  ASYNC_RETURN ASYNC_AWAIT(PublishPublisherBufferInternal(publisher, lease, message_size,
                                        /*payload=*/nullptr));
}

async::Task<absl::StatusOr<const Message>>
ClientImpl::PublishPublisherBufferCopy(PublisherImpl *publisher,
                                       const PublisherBufferLease &lease,
                                       absl::Span<const std::byte> payload) {
  ClientLockGuard guard(this);
  ASYNC_RETURN ASYNC_AWAIT(PublishPublisherBufferInternal(
      publisher, lease, static_cast<int64_t>(payload.size()), payload.data()));
}

async::Task<absl::StatusOr<const Message>>
ClientImpl::PublishPublisherBufferInternal(
    PublisherImpl *publisher, const PublisherBufferLease &lease,
    int64_t message_size, const std::byte *payload) {
  if (message_size <= 0) {
    ASYNC_RETURN absl::InvalidArgumentError("Message size must be greater than 0");
  }
  MessageSlot *slot = publisher->FindLease(lease.slot_id, lease.lease_id);
  if (slot == nullptr) {
    ASYNC_RETURN absl::FailedPreconditionError("Invalid or stale publisher lease");
  }
  if (message_size > publisher->SlotSize(slot)) {
    ASYNC_RETURN absl::InvalidArgumentError("Message size exceeds leased slot size");
  }
  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }
  if (payload != nullptr) {
    std::memmove(publisher->GetBufferAddress(slot), payload,
                 static_cast<size_t>(message_size));
  }
  if (publisher->on_send_callback_ != nullptr) {
    absl::StatusOr<int64_t> status_or_size = publisher->on_send_callback_(
        publisher->GetBufferAddress(slot), message_size);
    if (!status_or_size.ok()) {
      ASYNC_RETURN status_or_size.status();
    }
    message_size = status_or_size.value();
  }
  slot->message_size = message_size;
  Channel::PublishedMessage msg = ASYNC_AWAIT(publisher->ActivateSlotAndGetAnother(
      slot, publisher->IsReliable(), /*is_activation=*/false,
      publisher->GetPublisherId(), /*omit_prefix=*/false,
      /*use_prefix_slot_id=*/false, publisher->ForTunnel(),
      /*acquire_next=*/false));
  publisher->RemoveLease(lease.slot_id);
  publisher->TriggerSubscribers();
  if (publisher->NumSubscribers(publisher->VirtualChannelId()) == 0) {
    publisher->RetirePublishedSlotImmediately(slot);
  }
  if (absl::Status status = ASYNC_AWAIT(publisher->UnmapUnusedBuffers()); !status.ok()) {
    ASYNC_RETURN status;
  }
  ASYNC_RETURN Message(message_size, nullptr, msg.ordinal, msg.timestamp,
                 publisher->VirtualChannelId(), false, lease.slot_id, false);
}

absl::Status
ClientImpl::ReleasePublisherBuffer(PublisherImpl *publisher,
                                   const PublisherBufferLease &lease) {
  ClientLockGuard guard(this);
  MessageSlot *slot = publisher->FindLease(lease.slot_id, lease.lease_id);
  if (slot == nullptr) {
    return absl::FailedPreconditionError("Invalid or stale publisher lease");
  }
  if (!publisher->ReleaseLeasedSlot(slot)) {
    return absl::InternalError("Failed to release publisher lease");
  }
  publisher->RemoveLease(lease.slot_id);
  return absl::OkStatus();
}

absl::Span<std::byte>
ClientImpl::GetPublisherBufferMetadata(PublisherImpl *publisher,
                                       const PublisherBufferLease &lease) {
  ClientLockGuard guard(this);
  MessageSlot *slot = publisher->FindLease(lease.slot_id, lease.lease_id);
  return publisher->GetMetadata(slot);
}

async::Task<absl::StatusOr<const Message>>
ClientImpl::PublishMessage(PublisherImpl *publisher, int64_t message_size,
                           bool notify_subscribers) {
  return PublishMessageInternal(publisher, message_size, /*omit_prefix=*/false,
                                /*use_prefix_slot_id=*/false,
                                /*use_prefix_vchan_id=*/false,
                                notify_subscribers);
}

void ClientImpl::NotifySubscribers(PublisherImpl *publisher) {
  publisher->TriggerSubscribers();
}

async::Task<absl::StatusOr<const Message>>
ClientImpl::PublishMessageInternal(PublisherImpl *publisher,
                                   int64_t message_size, bool omit_prefix,
                                   bool use_prefix_slot_id,
                                   bool use_prefix_vchan_id,
                                   bool notify_subscribers) {
  // Lock is already held by the call to GetMessageBufferSpan.  This RAII
  // instance wil relesas the lock when we return from this function.
  ClientLockGuard guard(this, LockMode::kMaybeLocked);

  // Check if there are any new subscribers and if so, load their trigger fds.
  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }

  if (message_size == 0) {
    ASYNC_RETURN absl::InternalError("Message size must be greater than 0");
  }

  int32_t old_slot_id = publisher->CurrentSlotId();

  if (publisher->on_send_callback_ != nullptr) {
    absl::StatusOr<int64_t> status_or_size = publisher->on_send_callback_(
        publisher->GetCurrentBufferAddress(), message_size);
    if (!status_or_size.ok()) {
      ASYNC_RETURN status_or_size.status();
    }
    message_size = status_or_size.value();
  }
  publisher->SetMessageSize(message_size);
  MessageSlot *old_slot = publisher->CurrentSlot();
  if (debug_) {
    if (old_slot != nullptr) {
      printf("publish old slot: %d: %" PRId64 "\n", old_slot->id,
             old_slot->ordinal.load(std::memory_order_relaxed));
    }
  }

  Channel::PublishedMessage msg = ASYNC_AWAIT(publisher->ActivateSlotAndGetAnother(
      publisher->IsReliable(), /*is_activation=*/false, omit_prefix,
      use_prefix_slot_id, publisher->ForTunnel(), use_prefix_vchan_id));

  // Prevent use of old_slot.
  old_slot = nullptr;

  publisher->SetSlot(msg.new_slot);

  if (notify_subscribers) {
    publisher->TriggerSubscribers();
  }
  if (absl::Status status = ASYNC_AWAIT(publisher->UnmapUnusedBuffers()); !status.ok()) {
    ASYNC_RETURN status;
  }

  if (msg.new_slot == nullptr) {
    if (publisher->IsReliable()) {
      // Reliable publishers don't get a slot until it's asked for.
      ASYNC_RETURN Message(message_size, nullptr, msg.ordinal, msg.timestamp,
                     publisher->VirtualChannelId(), false, -1, false);
    }
    ASYNC_RETURN absl::InternalError(
        absl::StrFormat("Out of slots for channel %s", publisher->Name()));
  }

  if (debug_) {
    printf("publish new slot: %d: %" PRId64 "\n", msg.new_slot->id,
           msg.new_slot->ordinal.load(std::memory_order_relaxed));
  }

  ASYNC_RETURN Message(message_size, nullptr, msg.ordinal, msg.timestamp,
                 publisher->VirtualChannelId(), false, old_slot_id, false);
}

void ClientImpl::CancelPublish(PublisherImpl * /*publisher*/) {
  // Creating this object will unlock the mutex when it goes out of scope.
  ClientLockGuard guard(this, LockMode::kMaybeLocked);
}

async::Task<absl::Status>
ClientImpl::WaitForReliablePublisher(PublisherImpl *publisher,
                                     std::chrono::nanoseconds timeout,
                                     const toolbelt::AsyncWaiter *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  if (!publisher->IsReliable()) {
    ASYNC_RETURN absl::InternalError("Unreliable publishers can't wait");
  }
  // Check if there are any new subscribers and if so, load their trigger fds.
  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }
  uint64_t timeout_ns = timeout.count();
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  if (IsCooperative()) {
    absl::Status s =
        async::WaitReadable(SocketContext(), publisher->GetPollFd().Fd(),
                            std::chrono::nanoseconds(timeout_ns));
    if (absl::IsDeadlineExceeded(s)) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for reliable publisher");
    }
    ASYNC_RETURN s;
  }
#endif
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // Called from an Async API: suspend this coroutine rather than blocking.
  if (c == nullptr && SocketContext().Suspends()) {
    absl::Status s = ASYNC_AWAIT(
        async::WaitReadable(SocketContext(), publisher->GetPollFd().Fd(),
                            std::chrono::nanoseconds(timeout_ns)));
    if (absl::IsDeadlineExceeded(s)) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for reliable publisher");
    }
    ASYNC_RETURN s;
  }
#endif
  int result = -1;
  if (c != nullptr) {
    result = c->Wait(publisher->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result = co_->Wait(publisher->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else {
    struct pollfd fd = {.fd = publisher->GetPollFd().Fd(), .events = POLLIN};
    int e = GetSyscallShim().poll_fn(
        &fd, 1, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    // Since we are waiting forever will can only get the value 1 from the poll.
    // We will never get 0 since there is no timeout.  Anything else (can only
    // be -1) will be an error.
    if (timeout_ns != 0 && e == 0) {
      result = -1;
    } else if (e != 1) {
      ASYNC_RETURN absl::InternalError(
          absl::StrFormat("Error from poll waiting for reliable publisher: %s",
                          strerror(errno)));
    } else {
      result = fd.fd;
    }
  }
  if (result == -1) {
    ASYNC_RETURN absl::InternalError("Timeout waiting for reliable publisher");
  }
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::StatusOr<int>>
ClientImpl::WaitForReliablePublisher(PublisherImpl *publisher,
                                     const toolbelt::FileDescriptor &fd,
                                     std::chrono::nanoseconds timeout,

                                     const toolbelt::AsyncWaiter *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  if (!publisher->IsReliable()) {
    ASYNC_RETURN absl::InternalError("Unreliable publishers can't wait");
  }
  // Check if there are any new subscribers and if so, load their trigger fds.
  if (SubscribersChanged(publisher)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadSubscribersIfNecessary(publisher));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }
  int result = -1;
  uint64_t timeout_ns = timeout.count();
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  if (IsCooperative()) {
    absl::StatusOr<int> r = async::WaitEither(
        SocketContext(), publisher->GetPollFd().Fd(), fd.Fd(), timeout);
    if (absl::IsDeadlineExceeded(r.status())) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for reliable publisher");
    }
    if (!r.ok()) {
      ASYNC_RETURN r.status();
    }
    ASYNC_RETURN *r;
  }
#endif
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // Called from an Async API: suspend this coroutine rather than blocking.
  if (c == nullptr && SocketContext().Suspends()) {
    absl::StatusOr<int> r = ASYNC_AWAIT(async::WaitEither(
        SocketContext(), publisher->GetPollFd().Fd(), fd.Fd(), timeout));
    if (absl::IsDeadlineExceeded(r.status())) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for reliable publisher");
    }
    if (!r.ok()) {
      ASYNC_RETURN r.status();
    }
    ASYNC_RETURN *r;
  }
#endif
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
    int e = GetSyscallShim().poll_fn(
        fds, 2, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    if (timeout_ns != 0 && e == 0) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for reliable publisher");
    }
    if (e < 0) {
      ASYNC_RETURN absl::InternalError(
          absl::StrFormat("Error from poll waiting for reliable publisher: %s",
                          strerror(errno)));
    }
    if (fds[0].revents & (POLLIN | POLLHUP)) {
      result = fds[0].fd; // The publisher's poll fd triggered.
    } else if (fds[1].revents & (POLLIN | POLLHUP)) {
      result = fds[1].fd; // The passed in fd triggered.
    } else {
      ASYNC_RETURN absl::InternalError(
          "Unexpected poll result, neither publisher nor passed in fd "
          "triggered");
    }
  }
  ASYNC_RETURN result;
}

async::Task<absl::Status>
ClientImpl::WaitForSubscriber(SubscriberImpl *subscriber,
                                           std::chrono::nanoseconds timeout,
                                           const toolbelt::AsyncWaiter *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }

  uint64_t timeout_ns = timeout.count();
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  if (IsCooperative()) {
    absl::Status s =
        async::WaitReadable(SocketContext(), subscriber->GetPollFd().Fd(),
                            std::chrono::nanoseconds(timeout_ns));
    if (absl::IsDeadlineExceeded(s)) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for subscriber");
    }
    ASYNC_RETURN s;
  }
#endif
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // Called from an Async API: suspend this coroutine rather than blocking.
  if (c == nullptr && SocketContext().Suspends()) {
    absl::Status s = ASYNC_AWAIT(
        async::WaitReadable(SocketContext(), subscriber->GetPollFd().Fd(),
                            std::chrono::nanoseconds(timeout_ns)));
    if (absl::IsDeadlineExceeded(s)) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for subscriber");
    }
    ASYNC_RETURN s;
  }
#endif
  int result = -1;
  if (c != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result = c->Wait(subscriber->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else if (co_ != nullptr) {
    // Coroutine aware.  Yield control back until the poll fd is triggered.
    result = co_->Wait(subscriber->GetPollFd().Fd(), POLLIN, timeout_ns);
  } else {
    struct pollfd fd = {.fd = subscriber->GetPollFd().Fd(), .events = POLLIN};
    int e = GetSyscallShim().poll_fn(
        &fd, 1, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    // Since we are waiting forever will can only get the value 1 from the poll.
    // We will never get 0 since there is no timeout.  Anything else (can only
    // be -1) will be an error.
    if (timeout_ns != 0 && e == 0) {
      result = -1;
    } else if (e != 1) {
      ASYNC_RETURN absl::InternalError(absl::StrFormat(
          "Error from poll waiting for subscriber: %s", strerror(errno)));
    } else {
      result = fd.fd;
    }
  }
  if (result == -1) {
    ASYNC_RETURN absl::InternalError("Timeout waiting for subscriber");
  }
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::StatusOr<int>>
ClientImpl::WaitForSubscriber(
    SubscriberImpl *subscriber, const toolbelt::FileDescriptor &fd,
    std::chrono::nanoseconds timeout, const toolbelt::AsyncWaiter *c) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  int result = -1;
  uint64_t timeout_ns = timeout.count();
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  if (IsCooperative()) {
    absl::StatusOr<int> r = async::WaitEither(
        SocketContext(), subscriber->GetPollFd().Fd(), fd.Fd(), timeout);
    if (absl::IsDeadlineExceeded(r.status())) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for subscriber");
    }
    if (!r.ok()) {
      ASYNC_RETURN r.status();
    }
    ASYNC_RETURN *r;
  }
#endif
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // Called from an Async API: suspend this coroutine rather than blocking.
  if (c == nullptr && SocketContext().Suspends()) {
    absl::StatusOr<int> r = ASYNC_AWAIT(async::WaitEither(
        SocketContext(), subscriber->GetPollFd().Fd(), fd.Fd(), timeout));
    if (absl::IsDeadlineExceeded(r.status())) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for subscriber");
    }
    if (!r.ok()) {
      ASYNC_RETURN r.status();
    }
    ASYNC_RETURN *r;
  }
#endif
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
    int e = GetSyscallShim().poll_fn(
        fds, 2, timeout_ns == 0 ? -1 : timeout_ns / 1000000);
    if (timeout_ns != 0 && e == 0) {
      ASYNC_RETURN absl::InternalError("Timeout waiting for subscriber");
    }
    if (e < 0) {
      ASYNC_RETURN absl::InternalError(absl::StrFormat(
          "Error from poll waiting for subscriber: %s", strerror(errno)));
    }
    if (fds[0].revents & (POLLIN | POLLHUP)) {
      // The subscriber's poll fd triggered.
      ASYNC_RETURN fds[0].fd;
    } else if (fds[1].revents & (POLLIN | POLLHUP)) {
      // The passed in fd triggered.
      ASYNC_RETURN fds[1].fd;
    } else {
      ASYNC_RETURN absl::InternalError(
          "Unexpected poll result, neither subscriber nor passed in fd "
          "triggered");
    }
  }
  ASYNC_RETURN result;
}

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
// Asio overloads taking an async::Context (yield_context) directly.  Unlike the
// AsyncWaiter overloads above, which fall back to the client's stored
// context (SocketContext()), these wait cooperatively on the caller-supplied
// context.  This lets an Asio caller wait from within an arbitrary coroutine,
// mirroring how the co backend accepts a co::Coroutine*.
absl::Status
ClientImpl::WaitForReliablePublisher(PublisherImpl *publisher,
                                     std::chrono::nanoseconds timeout,
                                     async::Context ctx) {
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
  absl::Status s =
      async::WaitReadable(ctx, publisher->GetPollFd().Fd(), timeout);
  if (absl::IsDeadlineExceeded(s)) {
    return absl::InternalError("Timeout waiting for reliable publisher");
  }
  return s;
}

absl::StatusOr<int> ClientImpl::WaitForReliablePublisher(
    PublisherImpl *publisher, const toolbelt::FileDescriptor &fd,
    std::chrono::nanoseconds timeout, async::Context ctx) {
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
  absl::StatusOr<int> r =
      async::WaitEither(ctx, publisher->GetPollFd().Fd(), fd.Fd(), timeout);
  if (absl::IsDeadlineExceeded(r.status())) {
    return absl::InternalError("Timeout waiting for reliable publisher");
  }
  return r;
}

absl::Status ClientImpl::WaitForSubscriber(SubscriberImpl *subscriber,
                                           std::chrono::nanoseconds timeout,
                                           async::Context ctx) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  absl::Status s =
      async::WaitReadable(ctx, subscriber->GetPollFd().Fd(), timeout);
  if (absl::IsDeadlineExceeded(s)) {
    return absl::InternalError("Timeout waiting for subscriber");
  }
  return s;
}

absl::StatusOr<int> ClientImpl::WaitForSubscriber(
    SubscriberImpl *subscriber, const toolbelt::FileDescriptor &fd,
    std::chrono::nanoseconds timeout, async::Context ctx) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    return status;
  }
  absl::StatusOr<int> r =
      async::WaitEither(ctx, subscriber->GetPollFd().Fd(), fd.Fd(), timeout);
  if (absl::IsDeadlineExceeded(r.status())) {
    return absl::InternalError("Timeout waiting for subscriber");
  }
  return r;
}
#endif

async::Task<absl::StatusOr<Message>>
ClientImpl::ReadMessageInternal(SubscriberImpl *subscriber, ReadMode mode,
                                bool pass_activation, bool clear_trigger) {
  if (clear_trigger) {
    subscriber->ClearPollFd();
  }

  MessageSlot *new_slot = nullptr;
  MessageSlot *old_slot = subscriber->CurrentSlot();
  if (debug_ && old_slot != nullptr) {
    printf("read old slot: %d: %" PRId64 "\n", old_slot->id,
           static_cast<int64_t>(
               old_slot->ordinal.load(std::memory_order_relaxed)));
  }

  switch (mode) {
  case ReadMode::kReadNext:
    new_slot = ASYNC_AWAIT(subscriber->NextSlot());
    break;
  case ReadMode::kReadNewest:
    new_slot = ASYNC_AWAIT(subscriber->LastSlot());
    break;
  }

  // At this point, old_slot may have been reused so don't reference it
  // for any data.
  old_slot = nullptr; // Prevent any accidental use.

  if (new_slot == nullptr) {
    subscriber->NoteUnstuck();
    // I'm out of messages to read, trigger the publishers to give me
    // some more.  This is only for reliable publishers.
    subscriber->TriggerReliablePublishers();
    if (absl::Status status = ASYNC_AWAIT(subscriber->UnmapUnusedBuffers()); !status.ok()) {
      ASYNC_RETURN status;
    }
    ASYNC_RETURN Message();
  }
  subscriber->SetSlot(new_slot);
  int64_t delivered_message_size =
      static_cast<int64_t>(
          new_slot->message_size.load(std::memory_order_relaxed));

  if (debug_) {
    printf("read new_slot: %d: %" PRId64 "\n", new_slot->id,
           new_slot->ordinal.load(std::memory_order_relaxed));
  }

  MessagePrefix *prefix = subscriber->Prefix(new_slot);

  bool is_activation = false;
  bool checksum_error = false;
  if (prefix != nullptr) {
    if (prefix->HasChecksum()) {
      auto data =
          GetMessageChecksumData(prefix, subscriber->GetCurrentBufferAddress(),
                                 delivered_message_size,
                                 subscriber->ChecksumSize(),
                                 subscriber->MetadataSize());
      absl::Span<const std::byte> cksum =
          GetChecksumSpan(prefix, subscriber->ChecksumSize());
      checksum_error = !subscriber->ValidateChecksum(data.Spans(), cksum);
    }
    if ((prefix->flags & kMessageActivate) != 0) {
      is_activation = true;
      if (!pass_activation) {
        subscriber->IgnoreActivation(new_slot);
        if (subscriber->IsReliable()) {
          subscriber->TriggerReliablePublishers();
        }
        ASYNC_RETURN ASYNC_AWAIT(ReadMessageInternal(subscriber, mode,
                                   /* pass_activation=*/false,
                                   /* clear_trigger=*/false));
      }
    }
  }
  // Call the on receive callback.
  if (subscriber->on_receive_callback_ != nullptr) {
    absl::StatusOr<int64_t> status_or_size = subscriber->on_receive_callback_(
        subscriber->GetCurrentBufferAddress(), delivered_message_size);
    if (!status_or_size.ok()) {
      subscriber->UnreadSlot(new_slot);
      subscriber->SetSlot(nullptr);
      ASYNC_RETURN status_or_size.status();
    }
    delivered_message_size = status_or_size.value();
  }
  if (delivered_message_size <= 0) {
    subscriber->UnreadSlot(new_slot);
    subscriber->SetSlot(nullptr);
    ASYNC_RETURN Message();
  }
  // A FindMessage may already hold this slot.  Share that hold instead of
  // counting the message twice; the read takes over releasing its delivery
  // obligation.
  bool rejected = false;
  auto msg = subscriber->HoldExistingMessage(new_slot, /*consume=*/true,
                                             &rejected);
  const bool shared = msg != nullptr;
  if (rejected) {
    // The read still owns its pin and unread bit.  Restore them so the next
    // read picks the message up.
    subscriber->UnreadSlot(new_slot);
    subscriber->SetSlot(nullptr);
    ASYNC_RETURN Message();
  }
  if (shared) {
    subscriber->RetainActiveMessage(msg);
  } else {
    // We have a new slot, clear the subscriber's slot.
    subscriber->ClearActiveMessage();

    // Allocate a new active message for the slot.
    msg = subscriber->SetActiveMessage(
        delivered_message_size, new_slot, subscriber->GetCurrentBufferAddress(),
        subscriber->CurrentOrdinal(), subscriber->Timestamp(new_slot),
        new_slot->vchan_id.load(std::memory_order_relaxed), is_activation,
        checksum_error);

    // If we are unable to allocate a new message (due to message limits)
    // restore the slot so that we pick it up next time.
    if (msg->length == 0) {
      subscriber->UnreadSlot(new_slot);
      // Subscriber does not have a slot now but the slot it had is still
      // active. Do not wrap the reusable ActiveMessage in an empty Message. A
      // caller may retain that empty handle while this slot is retried, which
      // would keep an extra reference after the ActiveMessage becomes valid
      // and prevent its active-message count from being released.
      ReportIfSubscriberStuck(subscriber);
      ASYNC_RETURN Message();
    }
  }
  subscriber->NoteUnstuck();
  if (mode == ReadMode::kReadNext &&
      subscriber->options_.DetectDroppedMessages()) {
    int drops = subscriber->ConsumeQueueDrops();
    // Explicit bounded queues report their own evictions. Their MPSC
    // reservation order can differ from ordinal assignment under concurrent
    // publishers, so an ordinal gap is not evidence of a dropped message.
    // Queue-disabled and inherited-default subscribers retain ordinal-based
    // detection because they deliver through the ordered bitset path.
    if (subscriber->options_.SubscriberQueueSize() == 0) {
      drops = std::max(
          drops, subscriber->DetectDrops(
                     new_slot->vchan_id.load(std::memory_order_relaxed)));
    }
    if (drops > 0) {
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
  // We have a slot, claim it.
  subscriber->ClaimSlot(
      new_slot, new_slot->vchan_id.load(std::memory_order_relaxed),
      mode == ReadMode::kReadNewest);
  if (shared) {
    msg->length = static_cast<size_t>(delivered_message_size);
    msg->is_activation = is_activation;
    msg->checksum_error = checksum_error;
  }
  auto ret_msg = Message(msg);
  if (shared) {
    // Message took its own reference.  Drop the one HoldExistingMessage used
    // to keep the slot alive across this function.
    msg->DecRef();
  }
  if (subscriber->IsBridge()) {
    // Bridge subscribers don't hold onto the message in the subscriber to allow
    // timely slot retirement.  If they hold onto it, the slot would not be
    // retired until the next message is read.  This only affect bridge
    // subscribers which are used to send data between servers.
    subscriber->ClearActiveMessage();
  }
  if (checksum_error && !subscriber->PassChecksumErrors()) {
    ASYNC_RETURN absl::InternalError("Checksum verification failed");
  }
  // A checksum error that is ignored results in a valid message with the
  // checksum_error flag set.
  ASYNC_RETURN ret_msg;
}

async::Task<absl::StatusOr<Message>>
ClientImpl::ReadMessage(SubscriberImpl *subscriber,
                                                ReadMode mode,
                                                ClearTrigger clear_trigger) {

  ClientLockGuard guard(this);
  const bool should_clear_trigger =
      clear_trigger == ClearTrigger::kClearTrigger;
  // If the channel is a placeholder (no publishers present), look
  // in the SCB to see if a new publisher has been created and if so,
  // talk to the server to get the information to reload the shared
  // memory.  If there still isn't a publisher, we will still be a
  // placeholder.
  //
  // Clear the trigger before checking for a publisher.  A publisher that
  // arrives after the check triggers the poll fd, and clearing after the
  // check would discard that trigger and leave the subscriber waiting.
  if (subscriber->IsPlaceholder()) {
    if (should_clear_trigger) {
      subscriber->ClearPollFd();
    }
    absl::Status status = ASYNC_AWAIT(ReloadSubscriber(subscriber));
    if (placeholder_check_hook_ != nullptr) {
      placeholder_check_hook_();
    }
    if (!status.ok() || subscriber->IsPlaceholder()) {
      ASYNC_RETURN Message();
    }
    subscriber->TriggerReliablePublishers();
  }

  // Check if there are any new reliable publishers and if so, load their
  // trigger fds.
  if (PublishersChanged(subscriber)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadReliablePublishersIfNecessary(subscriber));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }

  ASYNC_RETURN ASYNC_AWAIT(ReadMessageInternal(subscriber, mode,
                             subscriber->options_.pass_activation,
                             should_clear_trigger));
}

absl::StatusOr<std::shared_ptr<Telemetry>>
Subscriber::ReadTelemetryMessage(ReadMode mode) {
  absl::StatusOr<Message> message = ReadMessage(mode);
  if (!message.ok()) {
    return message.status();
  }
  if (message->length == 0) {
    return std::shared_ptr<Telemetry>();
  }

  auto telemetry = std::make_shared<Telemetry>();
  if (!telemetry->ParseFromArray(message->buffer,
                                 static_cast<int>(message->length))) {
    return absl::DataLossError("Failed to parse telemetry message");
  }
  return telemetry;
}

void ClientImpl::ReportIfSubscriberStuck(SubscriberImpl *subscriber) {
  auto callback = stuck_subscriber_callbacks_.find(subscriber);
  const bool has_callback = callback != stuck_subscriber_callbacks_.end();
  std::optional<double> stuck_seconds = subscriber->NoteStuckRead(
      toolbelt::Now(), has_callback ? details::kMaxStuckSubscriberCallbacks
                                    : details::kMaxStuckSubscriberWarnings);
  if (!stuck_seconds.has_value()) {
    return;
  }
  if (has_callback) {
    // Call a copy so that, on a client that isn't thread safe, the callback can
    // unregister itself.
    auto report = callback->second;
    report(subscriber, *stuck_seconds);
    return;
  }
  const bool silencing =
      subscriber->StuckReports() >= details::kMaxStuckSubscriberWarnings;
  logger_.Log(toolbelt::LogLevel::kError,
              "Subscriber on channel %s has been unable to read messages for "
              "%.1f seconds because it holds its maximum of %d active "
              "messages%s",
              subscriber->Name().c_str(), *stuck_seconds,
              subscriber->MaxActiveMessages(),
              silencing ? "; this warning is silenced until it reads again"
                        : "");
}

async::Task<absl::StatusOr<Message>>
ClientImpl::ReadMessageFromBatch(SubscriberImpl *subscriber, ReadMode mode) {
  ClientLockGuard guard(this);
  ASYNC_RETURN ASYNC_AWAIT(ReadMessageInternal(subscriber, mode,
                             subscriber->options_.pass_activation,
                             /*clear_trigger=*/false));
}

async::Task<absl::StatusOr<Message>>
ClientImpl::FindMessageInternal(SubscriberImpl *subscriber,
                                uint64_t timestamp) {
  MessageSlot *new_slot = ASYNC_AWAIT(subscriber->FindMessage(timestamp));
  if (new_slot == nullptr) {
    // Not found.
    ASYNC_RETURN Message();
  }
  // FindMessage pinned the slot.  The returned Message owns that pin and
  // releases it when dropped, without consuming the delivery obligation, so a
  // later ReadMessage can still deliver it.
  bool rejected = false;
  if (auto held = subscriber->HoldExistingMessage(new_slot, /*consume=*/false,
                                                  &rejected)) {
    Message msg(held);
    held->DecRef();
    ASYNC_RETURN msg;
  }
  if (rejected) {
    subscriber->DropLookupPin(new_slot);
    subscriber->SetSlot(nullptr);
    ASYNC_RETURN Message();
  }
  auto msg = subscriber->ActivateLookup(new_slot);
  if (msg == nullptr) {
    ReportIfSubscriberStuck(subscriber);
    ASYNC_RETURN Message();
  }
  subscriber->NoteUnstuck();
  ASYNC_RETURN Message(msg);
}

async::Task<absl::StatusOr<Message>>
ClientImpl::FindMessage(SubscriberImpl *subscriber,
                                                uint64_t timestamp) {

  ClientLockGuard guard(this);

  // If the channel is a placeholder (no publishers present), contact the
  // server to see if there is now a publisher.  This will reload the shared
  // memory.  If there still isn't a publisher, we will still be a
  // placeholder.
  if (subscriber->IsPlaceholder()) {
    // As in ReadMessage, clear before the check so a publisher arriving after
    // it leaves the poll fd triggered.
    subscriber->ClearPollFd();
    absl::Status status = ASYNC_AWAIT(ReloadSubscriber(subscriber));
    if (placeholder_check_hook_ != nullptr) {
      placeholder_check_hook_();
    }
    if (!status.ok() || subscriber->IsPlaceholder()) {
      ASYNC_RETURN Message();
    }
  }

  // Check if there are any new reliable publishers and if so, load their
  // trigger fds.
  if (PublishersChanged(subscriber)) {
    if (absl::Status status =
            ASYNC_AWAIT(ReloadReliablePublishersIfNecessary(subscriber));
        !status.ok()) {
      ASYNC_RETURN status;
    }
  }

  ASYNC_RETURN ASYNC_AWAIT(FindMessageInternal(subscriber, timestamp));
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
  return slot->ordinal.load(std::memory_order_relaxed);
}

async::Task<bool> ClientImpl::CheckReload(ClientChannel *channel) {
  auto reloaded = ASYNC_AWAIT(ReloadBuffersIfNecessary(channel));
  if (!reloaded.ok()) {
    ASYNC_RETURN false;
  }
  ASYNC_RETURN *reloaded;
}

async::Task<absl::StatusOr<bool>>
ClientImpl::ReloadBuffersIfNecessary(ClientChannel *channel) {
  if (!channel->BuffersChanged()) {
    ASYNC_RETURN false;
  }
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  if (absl::Status status = ASYNC_AWAIT(channel->AttachBuffers()); !status.ok()) {
    ASYNC_RETURN status;
  }
  ASYNC_RETURN true;
}

async::Task<absl::Status>
ClientImpl::ReloadSubscriber(SubscriberImpl *subscriber) {
  // Check if there are any updates to the publishers
  // since that last time we checked.
  SystemControlBlock *scb = subscriber->GetScb();
  int updates = scb->counters[subscriber->GetChannelId()].num_pub_updates;

  if (subscriber->NumUpdates() == updates) {
    ASYNC_RETURN absl::OkStatus();
  }
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  Request req;
  FillCreateSubscriberRequest(req.mutable_create_subscriber(),
                              subscriber->RequestName(), subscriber->options_,
                              subscriber->GetSubscriberId());

  // Send request to server and wait for response.
  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }

  auto &sub_resp = resp.create_subscriber();
  if (!sub_resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(sub_resp.error());
  }

  // A subscriber-created placeholder is the only case where the server
  // replaces the CCB. Once num_slots is non-zero, publisher updates retain the
  // existing CCB and only require refreshed descriptors and buffers.
  const bool remap_ccb = subscriber->NumSlots() == 0;
  if (remap_ccb) {
    subscriber->ResetDeliveryState();
    // Leave the system control block mapped. Its counters are read from
    // other threads, and every channel in this client shares the mapping.
    ASYNC_AWAIT(subscriber->UnmapForReload());
  }

  if (!sub_resp.type().empty()) {
    subscriber->SetType(sub_resp.type());
  }
  subscriber->options_.use_split_buffers = sub_resp.use_split_buffers();
  subscriber->SetNumSlots(sub_resp.num_slots());
  subscriber->SetSubscriberQueueSize(
      sub_resp.default_subscriber_queue_size());
  subscriber->SetSubscriberQueueArenaSize(
      sub_resp.subscriber_queue_arena_size());
  subscriber->SetEffectiveSubscriberQueueSize(
      sub_resp.subscriber_queue_size());
  {
    int32_t cs = sub_resp.checksum_size() > 0 ? sub_resp.checksum_size() : 4;
    int32_t ms = sub_resp.metadata_size() > 0 ? sub_resp.metadata_size() : 0;
    subscriber->SetChecksumSize(cs);
    subscriber->SetMetadataSize(ms);
    subscriber->SetPrefixSize(Channel::ComputePrefixSize(cs, ms));
    subscriber->AllocateChecksumBuffer();
  }

  if (remap_ccb) {
    SharedMemoryFds channel_fds(std::move(fds[sub_resp.ccb_fd_index()]),
                                std::move(fds[sub_resp.bcb_fd_index()]));
    if (absl::Status status = subscriber->Map(std::move(channel_fds), scb_fd_);
        !status.ok()) {
      ASYNC_RETURN status;
    }
    subscriber->InitActiveMessages();
  }

  if (absl::Status status = ASYNC_AWAIT(subscriber->AttachBuffers()); !status.ok()) {
    ASYNC_RETURN status;
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

  subscriber->SetNumUpdates(updates);
  // subscriber->Dump();
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::Status>
ClientImpl::ReloadSubscribersIfNecessary(PublisherImpl *publisher) {
  SystemControlBlock *scb = publisher->GetScb();
  int updates = scb->counters[publisher->GetChannelId()].num_sub_updates;
  if (publisher->NumUpdates() == updates) {
    ASYNC_RETURN absl::OkStatus();
  }
  publisher->SetNumUpdates(updates);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
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
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
      !status.ok()) {
    ASYNC_RETURN status;
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
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::Status>
ClientImpl::ReloadReliablePublishersIfNecessary(SubscriberImpl *subscriber) {
  // Check if there are any updates to the publishers
  // since that last time we checked.
  SystemControlBlock *scb = subscriber->GetScb();
  int updates = scb->counters[subscriber->GetChannelId()].num_pub_updates;
  if (subscriber->NumUpdates() == updates) {
    ASYNC_RETURN absl::OkStatus();
  }
  subscriber->SetNumUpdates(updates);

  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
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
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
      !status.ok()) {
    ASYNC_RETURN status;
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

  ASYNC_RETURN absl::OkStatus();
}

// A reliable publisher always sends a single activation message when it
// is created.  This is to ensure that the reliable subscribers see
// on message and thus keep a reference to it.
async::Task<absl::Status> ClientImpl::ActivateReliableChannel(PublisherImpl *publisher) {
  MessageSlot *slot =
      ASYNC_AWAIT(publisher->FindFreeSlotReliable(publisher->GetPublisherId()));
  if (slot == nullptr) {
    ASYNC_RETURN absl::InternalError(
        absl::StrFormat("Channel %s has no free slots", publisher->Name()));
  }
  publisher->SetSlot(slot);

  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    ASYNC_RETURN absl::InternalError(
        absl::StrFormat("Channel %s has no buffer", publisher->Name()));
  }
  slot->message_size.store(1, std::memory_order_relaxed);

  ASYNC_AWAIT(publisher->ActivateSlotAndGetAnother(
      /*reliable=*/true,
      /*is_activation=*/true,
      /*omit_prefix=*/false, /*use_prefix_slot_id=*/false));
  publisher->SetSlot(nullptr);
  publisher->TriggerSubscribers();

  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::Status> ClientImpl::ActivateChannel(PublisherImpl *publisher) {
  if (publisher->IsActivated(publisher->VirtualChannelId())) {
    ASYNC_RETURN absl::OkStatus();
  }

  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    ASYNC_RETURN absl::InternalError(
        absl::StrFormat("3 Channel %s has no buffer", publisher->Name()));
  }
  MessageSlot *slot = publisher->CurrentSlot();
  slot->message_size.store(1, std::memory_order_relaxed);

  Channel::PublishedMessage msg = ASYNC_AWAIT(publisher->ActivateSlotAndGetAnother(
      /*reliable=*/false,
      /*is_activation=*/true,
      /*omit_prefix=*/false, /*use_prefix_slot_id=*/false));
  publisher->SetSlot(msg.new_slot);
  publisher->TriggerSubscribers();

  ASYNC_RETURN absl::OkStatus();
}

absl::Status ClientImpl::RemoveChannel(ClientChannel *channel) {
  if (!channels_.contains(channel)) {
    return absl::InternalError(
        absl::StrFormat("Channel %s not found\n", channel->Name()));
  }
  channels_.erase(channel);
  return absl::OkStatus();
}

async::Task<absl::Status>
ClientImpl::RemovePublisher(PublisherImpl *publisher) {
  ClientLockGuard guard(this);
  if (!socket_.Connected()) {
    ASYNC_RETURN RemoveChannel(publisher);
  }
  Request req;
  auto *cmd = req.mutable_remove_publisher();
  cmd->set_channel_name(publisher->Name());
  cmd->set_publisher_id(publisher->GetPublisherId());

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds));
      !status.ok()) {
    ASYNC_RETURN RemoveChannel(publisher);
  }

  auto &resp = response.remove_publisher();
  if (!resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(resp.error());
  }
  ASYNC_RETURN RemoveChannel(publisher);
}

async::Task<absl::Status>
ClientImpl::RemoveSubscriber(SubscriberImpl *subscriber) {
  ClientLockGuard guard(this);
  if (!socket_.Connected()) {
    ASYNC_RETURN RemoveChannel(subscriber);
  }
  Request req;
  auto *cmd = req.mutable_remove_subscriber();
  cmd->set_channel_name(subscriber->RequestName());
  cmd->set_subscriber_id(subscriber->GetSubscriberId());
  cmd->set_telemetry(subscriber->options_.Telemetry());

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds));
      !status.ok()) {
    ASYNC_RETURN RemoveChannel(subscriber);
  }

  auto &resp = response.remove_subscriber();
  if (!resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(resp.error());
  }
  ASYNC_RETURN RemoveChannel(subscriber);
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

async::Task<absl::StatusOr<const ChannelInfo>>
ClientImpl::GetChannelInfo(const std::string &channel) {
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  Request req;
  auto *cmd = req.mutable_get_channel_info();
  cmd->set_channel_name(channel);

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }

  auto &resp = response.get_channel_info();
  if (!resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(resp.error());
  }
  // There will be one channel information in the response.
  if (resp.channels_size() != 1) {
    ASYNC_RETURN absl::InternalError("Invalid response for getChannelInfo");
  }
  ChannelInfo result;
  const ChannelInfoProto &info = resp.channels()[0];
  result.channel_name = info.name();
  result.num_publishers = info.num_pubs();
  result.num_subscribers = info.num_subs();
  result.num_bridge_pubs = info.num_bridge_pubs();
  result.num_bridge_subs = info.num_bridge_subs();
  result.num_tunnel_pubs = info.num_tunnel_pubs();
  result.num_tunnel_subs = info.num_tunnel_subs();
  result.reliable = info.is_reliable();
  result.type = info.type();
  result.slot_size = info.slot_size();
  result.num_slots = info.num_slots();
  result.subscriber_queue_size = info.subscriber_queue_size();
  result.subscriber_queue_arena_size =
      info.subscriber_queue_arena_size();
  result.is_local = info.is_local();
  ASYNC_RETURN result;
}

async::Task<absl::StatusOr<const std::vector<ChannelInfo>>>
ClientImpl::GetChannelInfo() {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  Request req;
  [[maybe_unused]] auto cmd = req.mutable_get_channel_info();

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  auto &resp = response.get_channel_info();
  if (!resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(resp.error());
  }
  std::vector<ChannelInfo> r;
  for (auto &info : resp.channels()) {
    ChannelInfo result;
    result.channel_name = info.name();
    result.num_publishers = info.num_pubs();
    result.num_subscribers = info.num_subs();
    result.num_bridge_pubs = info.num_bridge_pubs();
    result.num_bridge_subs = info.num_bridge_subs();
    result.num_tunnel_pubs = info.num_tunnel_pubs();
    result.num_tunnel_subs = info.num_tunnel_subs();
    result.reliable = info.is_reliable();
    result.type = info.type();
    result.slot_size = info.slot_size();
    result.num_slots = info.num_slots();
    result.subscriber_queue_size = info.subscriber_queue_size();
    result.subscriber_queue_arena_size =
        info.subscriber_queue_arena_size();
    result.is_local = info.is_local();
    r.push_back(result);
  }
  ASYNC_RETURN r;
}

async::Task<absl::StatusOr<bool>>
ClientImpl::ChannelExists(const std::string &channelName) {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  absl::StatusOr<const ChannelInfo> info = ASYNC_AWAIT(GetChannelInfo(channelName));
  ASYNC_RETURN info.ok();
}

async::Task<absl::StatusOr<const ChannelStats>>
ClientImpl::GetChannelStats(const std::string &channel) {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }
  Request req;
  auto *cmd = req.mutable_get_channel_stats();
  cmd->set_channel_name(channel);

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  auto &resp = response.get_channel_stats();
  if (!resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(resp.error());
  }
  // There will be one channel information in the response.
  if (resp.channels_size() != 1) {
    ASYNC_RETURN absl::InternalError("Invalid response for getChannelStats");
  }
  ChannelStats result;
  const ChannelStatsProto &stats = resp.channels()[0];
  result.channel_name = stats.channel_name();
  result.total_bytes = stats.total_bytes();
  result.total_messages = stats.total_messages();
  result.max_message_size = stats.max_message_size();
  result.is_local = stats.is_local();
  ASYNC_RETURN result;
}

async::Task<absl::StatusOr<const std::vector<ChannelStats>>>
ClientImpl::GetChannelStats() {
  ClientLockGuard guard(this);
  if (absl::Status status = CheckConnected(); !status.ok()) {
    ASYNC_RETURN status;
  }

  Request req;
  [[maybe_unused]] auto cmd = req.mutable_get_channel_stats();

  // Send request to server and wait for response.
  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  auto &resp = response.get_channel_stats();
  if (!resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(resp.error());
  }
  std::vector<ChannelStats> r;
  for (auto &stats : resp.channels()) {
    ChannelStats result;
    result.channel_name = stats.channel_name();
    result.total_bytes = stats.total_bytes();
    result.total_messages = stats.total_messages();
    result.max_message_size = stats.max_message_size();
    result.is_local = stats.is_local();
    r.push_back(result);
  }
  ASYNC_RETURN r;
}

async::Task<absl::Status>
ClientImpl::ResizeChannel(PublisherImpl *publisher,
                                       int64_t new_slot_size) {
  if (publisher->IsFixedSize()) {
    ASYNC_RETURN absl::InternalError(absl::StrFormat(
        "Channel %s is fixed size at %d bytes; can't increase it to %d bytes",
        publisher->Name(), publisher->SlotSize(), new_slot_size));
  }
  if (int64_t max_slot_size = publisher->MaxSlotSize();
      max_slot_size != 0 && new_slot_size > max_slot_size) {
    ASYNC_RETURN absl::InvalidArgumentError(absl::StrFormat(
        "Channel %s has a maximum slot size of %d bytes; can't increase it to "
        "%d bytes",
        publisher->Name(), max_slot_size, new_slot_size));
  }

  // Call the resize callback if one has been registered.  If this returns
  // an error, we don't perform the resize.
  auto it = resize_callbacks_.find(publisher);
  if (it != resize_callbacks_.end()) {
    if (absl::Status s =
            it->second(publisher, publisher->SlotSize(), new_slot_size);
        !s.ok()) {
      ASYNC_RETURN s;
    }
  }

  ASYNC_RETURN ASYNC_AWAIT(publisher->CreateOrAttachBuffers(Aligned(new_slot_size)));
}

void ClientImpl::FillCreatePublisherRequest(CreatePublisherRequest *cmd,
                                            const std::string &channel_name,
                                            const PublisherOptions &opts,
                                            int publisher_id) {
  cmd->set_channel_name(channel_name);
  cmd->set_slot_size(Aligned(opts.slot_size));
  cmd->set_num_slots(opts.num_slots);
  cmd->set_is_local(opts.IsLocal());
  cmd->set_is_reliable(opts.IsReliable());
  cmd->set_is_bridge(opts.IsBridge());
  cmd->set_for_tunnel(opts.ForTunnel());
  cmd->set_is_fixed_size(opts.IsFixedSize());
  cmd->set_type(opts.Type());
  cmd->set_mux(opts.Mux());
  cmd->set_vchan_id(opts.VchanId());
  cmd->set_notify_retirement(opts.notify_retirement);
  cmd->set_checksum_size(opts.ChecksumSize());
  cmd->set_metadata_size(opts.MetadataSize());
  cmd->set_publisher_id(publisher_id);
  cmd->set_max_publishers(opts.MaxPublishers());
  cmd->set_use_split_buffers(opts.UseSplitBuffers());
  cmd->set_split_buffers_over_bridge(opts.SplitBuffersOverBridge());
  cmd->set_subscriber_queue_arena_size(opts.SubscriberQueueArenaSize());
  cmd->set_process_id(static_cast<uint64_t>(getpid()));
  cmd->set_max_outstanding_slot_leases(opts.MaxOutstandingSlotLeases());
  // Align the cap the same way as the slot size so that a publisher asking for
  // slot_size == max_slot_size isn't rejected by its own limit.
  cmd->set_max_slot_size(opts.max_slot_size > 0 ? Aligned(opts.max_slot_size)
                                                : 0);
}

void ClientImpl::ApplyPublisherResponseFds(
    PublisherImpl *publisher, const CreatePublisherResponse &resp,
    std::vector<toolbelt::FileDescriptor> &fds) {
  publisher->SetTriggerFd(std::move(fds[resp.pub_trigger_fd_index()]));
  publisher->SetPollFd(std::move(fds[resp.pub_poll_fd_index()]));

  publisher->ClearSubscribers();
  for (auto index : resp.sub_trigger_fd_indexes()) {
    publisher->AddSubscriber(std::move(fds[index]));
  }

  if (resp.retirement_fd_index() != -1) {
    publisher->SetRetirementFd(
        std::move(fds[size_t(resp.retirement_fd_index())]));
  }

  publisher->ClearRetirementTriggers();
  for (auto index : resp.retirement_fd_indexes()) {
    publisher->AddRetirementTrigger(fds[size_t(index)]);
  }

  publisher->SetNumUpdates(resp.num_sub_updates());
}

void ClientImpl::FillCreateSubscriberRequest(CreateSubscriberRequest *cmd,
                                             const std::string &channel_name,
                                             const SubscriberOptions &opts,
                                             int subscriber_id) {
  cmd->set_channel_name(channel_name);
  cmd->set_subscriber_id(subscriber_id);
  cmd->set_is_reliable(opts.IsReliable());
  cmd->set_is_bridge(opts.IsBridge());
  cmd->set_for_tunnel(opts.ForTunnel());
  cmd->set_telemetry(opts.Telemetry());
  cmd->set_type(opts.Type());
  cmd->set_max_active_messages(opts.MaxActiveMessages());
  cmd->set_max_subscribers(opts.MaxSubscribers());
  cmd->set_mux(opts.Mux());
  cmd->set_vchan_id(opts.VchanId());
  cmd->set_subscriber_queue_size(opts.SubscriberQueueSize());
  cmd->set_process_id(static_cast<uint64_t>(getpid()));
  cmd->set_is_local(opts.IsLocal());
}

void ClientImpl::ApplySubscriberResponseFds(
    SubscriberImpl *subscriber, const CreateSubscriberResponse &resp,
    std::vector<toolbelt::FileDescriptor> &fds) {
  subscriber->SetTriggerFd(std::move(fds[resp.trigger_fd_index()]));
  subscriber->SetPollFd(std::move(fds[resp.poll_fd_index()]));

  subscriber->ClearPublishers();
  for (auto index : resp.reliable_pub_trigger_fd_indexes()) {
    subscriber->AddPublisher(std::move(fds[index]));
  }

  subscriber->ClearRetirementTriggers();
  for (auto index : resp.retirement_fd_indexes()) {
    subscriber->AddRetirementTrigger(fds[size_t(index)]);
  }

  subscriber->SetNumUpdates(resp.num_pub_updates());
}

async::Task<absl::Status>
ClientImpl::Reconnect() {
  reconnecting_ = true;
  auto cleanup = [this]() { reconnecting_ = false; };

  socket_.Close();
  socket_ = async::UnixSocket();
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  socket_.SetBlocking(!IsCooperative());
#endif

  absl::Status status = socket_.Connect(socket_name_);
  if (!status.ok()) {
    cleanup();
    ASYNC_RETURN status;
  }

  Request init_req;
  FillInitRequest(init_req.mutable_init(), name_);
  Response init_resp;
  std::vector<toolbelt::FileDescriptor> init_fds;
  init_fds.reserve(100);
  status = ASYNC_AWAIT(SendRequestReceiveResponse(init_req, init_resp, init_fds));
  if (!status.ok()) {
    cleanup();
    ASYNC_RETURN status;
  }
  if (!init_resp.init().error().empty()) {
    cleanup();
    ASYNC_RETURN absl::InternalError(init_resp.init().error());
  }
  scb_fd_ = std::move(init_fds[init_resp.init().scb_fd_index()]);
  session_id_ = init_resp.init().session_id();
  server_user_id_ = init_resp.init().user_id();
  server_group_id_ = init_resp.init().group_id();

  for (auto &channel : channels_) {
    if (auto *pub = dynamic_cast<PublisherImpl *>(channel.get())) {
      status = ASYNC_AWAIT(ReregisterPublisher(pub));
      if (!status.ok()) {
        cleanup();
        ASYNC_RETURN status;
      }
    } else if (auto *sub = dynamic_cast<SubscriberImpl *>(channel.get())) {
      status = ASYNC_AWAIT(ReregisterSubscriber(sub));
      if (!status.ok()) {
        cleanup();
        ASYNC_RETURN status;
      }
    }
  }

  cleanup();
  ASYNC_RETURN absl::OkStatus();
}

bool ClientImpl::ShouldReconnect(const absl::Status &failure) const {
  // A cancelled wait means the coroutine running this client is being
  // stopped, not that the server has gone.  Reconnecting then can block in
  // connect() for good if the server has stopped accepting connections, as it
  // does while it shuts down.
  if (!was_connected_ || reconnecting_ || absl::IsCancelled(failure)) {
    return false;
  }
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // send()/recv() can fail synchronously (for example EPIPE when the server's
  // client handler exited on shutdown first) without reaching a wait that sees
  // the coroutine's interrupt fd.  Check the interrupt fd directly too.
  if (co20::Coroutine *co = SocketContext().Coroutine(); co != nullptr) {
    if (co->IsAborted()) {
      return false;
    }
    if (int ifd = co->GetInterruptFd(); ifd != -1) {
      struct pollfd pfd = {.fd = ifd, .events = POLLIN, .revents = 0};
      if (GetSyscallShim().poll_fn(&pfd, 1, 0) > 0 &&
          (pfd.revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
        return false;
      }
    }
  }
#endif
  return true;
}

async::Task<absl::Status>
ClientImpl::ReregisterPublisher(PublisherImpl *publisher) {
  Request req;
  FillCreatePublisherRequest(req.mutable_create_publisher(), publisher->Name(),
                             publisher->options_,
                             publisher->GetPublisherId());
  req.mutable_create_publisher()->set_active_queue_publish_depth(
      publisher->ActiveQueuePublishDepth());

  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  auto &pub_resp = resp.create_publisher();
  if (!pub_resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(pub_resp.error());
  }

  ApplyPublisherResponseFds(publisher, pub_resp, fds);
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::Status>
ClientImpl::ReregisterSubscriber(SubscriberImpl *subscriber) {
  Request req;
  FillCreateSubscriberRequest(req.mutable_create_subscriber(),
                              subscriber->Name(), subscriber->options_,
                              subscriber->GetSubscriberId());

  Response resp;
  std::vector<toolbelt::FileDescriptor> fds;
  fds.reserve(100);
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, resp, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  auto &sub_resp = resp.create_subscriber();
  if (!sub_resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(sub_resp.error());
  }

  ApplySubscriberResponseFds(subscriber, sub_resp, fds);
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::Status>
ClientImpl::SendRequestReceiveResponse(
    const Request &req, Response &response,
    std::vector<toolbelt::FileDescriptor> &fds,
    const std::vector<toolbelt::FileDescriptor> &send_fds) {

  {
    size_t msg_len = req.ByteSizeLong();
    std::vector<char> send_msg(sizeof(int32_t) + msg_len);
    char *sendbuf = send_msg.data() + sizeof(int32_t);

    if (!req.SerializeToArray(sendbuf, msg_len)) {
      ASYNC_RETURN absl::InternalError("Failed to serialize request");
    }

    absl::StatusOr<ssize_t> n =
        ASYNC_AWAIT(socket_.SendMessage(sendbuf, msg_len, SocketContext()));
    if (!n.ok()) {
      socket_.Close();
      if (ShouldReconnect(n.status())) {
        if (absl::Status rs = ASYNC_AWAIT(Reconnect()); rs.ok()) {
          ASYNC_RETURN ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds, send_fds));
        } else {
          ASYNC_RETURN rs;
        }
      }
      ASYNC_RETURN n.status();
    }
    if (!send_fds.empty()) {
      absl::Status status =
          ASYNC_AWAIT(socket_.SendFds(send_fds, SocketContext()));
      if (!status.ok()) {
        socket_.Close();
        ASYNC_RETURN status;
      }
    }
  }

  absl::StatusOr<std::vector<char>> recv_msg =
      ASYNC_AWAIT(socket_.ReceiveVariableLengthMessage(SocketContext()));
  if (!recv_msg.ok()) {
    socket_.Close();
    if (ShouldReconnect(recv_msg.status())) {
      if (absl::Status rs = ASYNC_AWAIT(Reconnect()); rs.ok()) {
        ASYNC_RETURN ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds, send_fds));
      } else {
        ASYNC_RETURN rs;
      }
    }
    ASYNC_RETURN recv_msg.status();
  }
  if (!response.ParseFromArray(recv_msg->data(),
                               static_cast<int>(recv_msg->size()))) {
    socket_.Close();
    ASYNC_RETURN absl::InternalError("Failed to parse response");
  }

  absl::Status s = ASYNC_AWAIT(socket_.ReceiveFds(fds, SocketContext()));
  if (!s.ok()) {
    socket_.Close();
  }

  ASYNC_RETURN s;
}

async::Task<absl::Status>
ClientImpl::SendOneWayRequest(
    const Request &req, const std::vector<toolbelt::FileDescriptor> &fds) {
  size_t msg_len = req.ByteSizeLong();
  std::vector<char> send_msg(sizeof(int32_t) + msg_len);
  char *sendbuf = send_msg.data() + sizeof(int32_t);

  if (!req.SerializeToArray(sendbuf, msg_len)) {
    ASYNC_RETURN absl::InternalError("Failed to serialize one-way request");
  }

  absl::StatusOr<ssize_t> n =
      ASYNC_AWAIT(socket_.SendMessage(sendbuf, msg_len, SocketContext()));
  if (!n.ok()) {
    socket_.Close();
    if (ShouldReconnect(n.status())) {
      if (absl::Status rs = ASYNC_AWAIT(Reconnect()); rs.ok()) {
        ASYNC_RETURN ASYNC_AWAIT(SendOneWayRequest(req, fds));
      } else {
        ASYNC_RETURN rs;
      }
    }
    ASYNC_RETURN n.status();
  }
  if (!fds.empty()) {
    absl::Status status = ASYNC_AWAIT(socket_.SendFds(fds, SocketContext()));
    if (!status.ok()) {
      socket_.Close();
      ASYNC_RETURN status;
    }
  }
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::Status>
ClientImpl::RegisterClientBuffer(const ClientBufferHandleMetadata &metadata,
                                 const toolbelt::FileDescriptor *fd) {
  Request req;
  auto *register_buffer = req.mutable_register_client_buffer();
  ToProto(metadata, register_buffer->mutable_metadata());
  std::vector<toolbelt::FileDescriptor> fds;
  if (fd != nullptr && fd->Valid()) {
    register_buffer->set_has_fd(true);
    register_buffer->set_fd_index(0);
    fds.push_back(*fd);
  }
  Response response;
  std::vector<toolbelt::FileDescriptor> response_fds;
  if (absl::Status status =
          ASYNC_AWAIT(SendRequestReceiveResponse(req, response, response_fds, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  if (!response.register_client_buffer().error().empty()) {
    ASYNC_RETURN absl::InternalError(response.register_client_buffer().error());
  }
  ASYNC_RETURN absl::OkStatus();
}

async::Task<absl::StatusOr<std::vector<RegisteredClientBuffer>>>
ClientImpl::GetClientBuffers(const std::string &channel_name,
                             uint64_t session_id, uint32_t buffer_index) {
  Request req;
  auto *get = req.mutable_get_client_buffers();
  get->set_channel_name(channel_name);
  get->set_session_id(session_id);
  get->set_buffer_index(buffer_index);

  Response response;
  std::vector<toolbelt::FileDescriptor> fds;
  if (absl::Status status = ASYNC_AWAIT(SendRequestReceiveResponse(req, response, fds));
      !status.ok()) {
    ASYNC_RETURN status;
  }
  const auto &get_resp = response.get_client_buffers();
  if (!get_resp.error().empty()) {
    ASYNC_RETURN absl::InternalError(get_resp.error());
  }
  if (get_resp.metadata_size() != get_resp.fd_indexes_size()) {
    ASYNC_RETURN absl::InternalError("Malformed client buffer response");
  }
  std::vector<RegisteredClientBuffer> result;
  result.reserve(get_resp.metadata_size());
  for (int i = 0; i < get_resp.metadata_size(); ++i) {
    RegisteredClientBuffer buffer{
        .metadata = FromProto(get_resp.metadata(i)),
    };
    int fd_index = get_resp.fd_indexes(i);
    if (fd_index >= 0 && static_cast<size_t>(fd_index) < fds.size()) {
      buffer.fd = std::move(fds[size_t(fd_index)]);
    }
    result.push_back(std::move(buffer));
  }
  ASYNC_RETURN result;
}

async::Task<absl::Status>
ClientImpl::UnregisterClientBuffer(const std::string &channel_name,
                                                uint64_t session_id,
                                                uint32_t buffer_index) {
  Request req;
  auto *unregister = req.mutable_unregister_client_buffer();
  unregister->set_channel_name(channel_name);
  unregister->set_session_id(session_id);
  unregister->set_buffer_index(buffer_index);
  ASYNC_RETURN ASYNC_AWAIT(SendOneWayRequest(req));
}

} // namespace subspace
