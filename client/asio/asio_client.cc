// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/asio/asio_client.h"

#include "absl/strings/str_format.h"
#include "client/checksum.h"
#include "common/channel.h"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

namespace subspace {
namespace {

boost::asio::local::stream_protocol::endpoint
MakeEndpoint(const std::string &pathname) {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  address.sun_path[0] = '\0';
  std::memcpy(address.sun_path + 1, pathname.data(), pathname.size());
  boost::asio::local::stream_protocol::endpoint endpoint;
  address.sun_path[sizeof(address.sun_path) - 1] = '\1';
  std::memcpy(endpoint.data(), &address, sizeof(address));
  endpoint.resize(sizeof(address));
  reinterpret_cast<sockaddr_un *>(endpoint.data())
      ->sun_path[sizeof(address.sun_path) - 1] = '\0';
  return endpoint;
}

absl::Status IoError(std::string_view operation,
                     const boost::system::error_code &error) {
  return absl::InternalError(
      absl::StrFormat("%s: %s", operation, error.message()));
}

absl::Status CheckFd(const std::vector<toolbelt::FileDescriptor> &fds,
                     int index, std::string_view name) {
  if (index < 0 || static_cast<size_t>(index) >= fds.size() ||
      !fds[static_cast<size_t>(index)].Valid()) {
    return absl::InternalError(
        absl::StrFormat("invalid %s fd index %d", name, index));
  }
  return absl::OkStatus();
}

uint64_t ExpandSlotSize(uint64_t slot_size) {
  static constexpr double multipliers[] = {2.0,   1.5,    1.25,
                                           1.125, 1.0625, 1.03125};
  static constexpr size_t ranges[] = {4096, 16384, 65536, 262144, 1048576};
  size_t index = 0;
  while (index < std::size(ranges) && slot_size > ranges[index]) {
    ++index;
  }
  return Aligned(static_cast<uint64_t>(slot_size * multipliers[index]));
}

} // namespace

AsioClient::AsioClient(Executor executor)
    : executor_(std::move(executor)), strand_(executor_), socket_(executor_),
      control_buffer_(CMSG_SPACE(4096 * sizeof(int)), '\0') {}

AsioClient::~AsioClient() {
  boost::system::error_code ignored;
  socket_.close(ignored);
  Fail(absl::CancelledError("AsioClient destroyed"));
}

void AsioClient::Init(std::string socket, std::string name,
                      std::function<void(absl::Status)> completion) {
  name_ = std::move(name);
  auto self = shared_from_this();
  socket_.async_connect(
      MakeEndpoint(socket),
      boost::asio::bind_executor(
          strand_, [self, completion = std::move(completion)](
                       const boost::system::error_code &error) mutable {
            if (error) {
              completion(IoError("connect", error));
              return;
            }
            Request request;
            auto *init = request.mutable_init();
            init->set_client_name(self->name_);
            init->set_max_channels(kMaxChannels);
            init->set_max_slot_owners(kMaxSlotOwners);
            init->set_max_buffers(static_cast<int32_t>(kMaxBuffers));
            self->Enqueue(
                std::move(request),
                [self, completion = std::move(completion)](
                    absl::StatusOr<RpcResult> result) mutable {
                  if (!result.ok()) {
                    completion(result.status());
                    return;
                  }
                  const auto &response = result->response.init();
                  if (!response.error().empty()) {
                    completion(absl::InternalError(response.error()));
                    return;
                  }
                  if (absl::Status status =
                          CheckFd(result->fds, response.scb_fd_index(), "scb");
                      !status.ok()) {
                    completion(status);
                    return;
                  }
                  self->scb_fd_ =
                      std::move(result->fds[response.scb_fd_index()]);
                  self->session_id_ = response.session_id();
                  self->server_user_id_ = response.user_id();
                  self->server_group_id_ = response.group_id();
                  completion(absl::OkStatus());
                });
          }));
}

void AsioClient::Enqueue(Request request, ControlCompletion completion) {
  const size_t size = request.ByteSizeLong();
  ControlCommand command;
  command.outbound.resize(sizeof(uint32_t) + size);
  const uint32_t network_size = htonl(static_cast<uint32_t>(size));
  std::memcpy(command.outbound.data(), &network_size, sizeof(network_size));
  if (!request.SerializeToArray(command.outbound.data() + sizeof(uint32_t),
                                static_cast<int>(size))) {
    completion(absl::InternalError("failed to serialize request"));
    return;
  }
  command.completion = std::move(completion);
  auto self = shared_from_this();
  boost::asio::post(strand_, [self, command = std::move(command)]() mutable {
    self->commands_.push_back(std::move(command));
    self->StartNext();
  });
}

void AsioClient::StartNext() {
  if (active_ || commands_.empty()) {
    return;
  }
  active_ = true;
  auto self = shared_from_this();
  boost::asio::async_write(
      socket_, boost::asio::buffer(commands_.front().outbound),
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error, size_t) {
            if (error) {
              self->Finish(IoError("control write", error));
              return;
            }
            self->ReadResponseLength();
          }));
}

void AsioClient::ReadResponseLength() {
  auto self = shared_from_this();
  boost::asio::async_read(
      socket_, boost::asio::buffer(length_),
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error, size_t) {
            if (error) {
              self->Finish(IoError("response length read", error));
              return;
            }
            uint32_t network_size = 0;
            std::memcpy(&network_size, self->length_.data(),
                        sizeof(network_size));
            self->response_size_ = ntohl(network_size);
            self->ReadResponseBody();
          }));
}

void AsioClient::ReadResponseBody() {
  response_body_.assign(response_size_, '\0');
  auto self = shared_from_this();
  boost::asio::async_read(
      socket_, boost::asio::buffer(response_body_),
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error, size_t) {
            if (error) {
              self->Finish(IoError("response body read", error));
              return;
            }
            self->result_ = RpcResult();
            if (!self->result_.response.ParseFromArray(
                    self->response_body_.data(),
                    static_cast<int>(self->response_body_.size()))) {
              self->Finish(absl::DataLossError("failed to parse response"));
              return;
            }
            self->expected_fds_ = -1;
            self->ReceiveFds();
          }));
}

void AsioClient::ReceiveFds() {
  auto self = shared_from_this();
  socket_.async_wait(
      boost::asio::socket_base::wait_read,
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error) {
            if (error) {
              self->Finish(IoError("fd wait", error));
              return;
            }
            int32_t total = 0;
            iovec iov{&total, sizeof(total)};
            msghdr message{};
            message.msg_iov = &iov;
            message.msg_iovlen = 1;
            message.msg_control = self->control_buffer_.data();
            message.msg_controllen = self->control_buffer_.size();
            const ssize_t received = ::recvmsg(self->socket_.native_handle(),
                                               &message, MSG_DONTWAIT);
            if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
              self->ReceiveFds();
              return;
            }
            if (received <= 0) {
              self->Finish(absl::InternalError("failed to receive fds"));
              return;
            }
            if ((message.msg_flags & MSG_CTRUNC) != 0) {
              self->Finish(absl::ResourceExhaustedError(
                  "ancillary fd message was truncated"));
              return;
            }
            if (self->expected_fds_ < 0) {
              self->expected_fds_ = total;
            }
            for (cmsghdr *cmsg = CMSG_FIRSTHDR(&message); cmsg != nullptr;
                 cmsg = CMSG_NXTHDR(&message, cmsg)) {
              if (cmsg->cmsg_level != SOL_SOCKET ||
                  cmsg->cmsg_type != SCM_RIGHTS) {
                continue;
              }
              const size_t count = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
              const int *fds = reinterpret_cast<int *>(CMSG_DATA(cmsg));
              for (size_t i = 0; i < count; ++i) {
                self->result_.fds.emplace_back(fds[i]);
              }
            }
            if (static_cast<int>(self->result_.fds.size()) >=
                self->expected_fds_) {
              self->Finish(std::move(self->result_));
            } else {
              self->ReceiveFds();
            }
          }));
}

void AsioClient::Finish(absl::StatusOr<RpcResult> result) {
  if (commands_.empty()) {
    return;
  }
  auto completion = std::move(commands_.front().completion);
  commands_.pop_front();
  active_ = false;
  completion(std::move(result));
  StartNext();
}

void AsioClient::Fail(absl::Status status) {
  while (!commands_.empty()) {
    auto completion = std::move(commands_.front().completion);
    commands_.pop_front();
    completion(status);
  }
  active_ = false;
}

void AsioClient::FillPublisherRequest(CreatePublisherRequest *request,
                                      const std::string &name,
                                      const PublisherOptions &options) {
  request->set_channel_name(name);
  request->set_slot_size(Aligned(options.slot_size));
  request->set_num_slots(options.num_slots);
  request->set_is_local(options.IsLocal());
  request->set_is_reliable(options.IsReliable());
  request->set_is_bridge(options.IsBridge());
  request->set_for_tunnel(options.ForTunnel());
  request->set_is_fixed_size(options.IsFixedSize());
  request->set_type(options.Type());
  request->set_mux(options.Mux());
  request->set_vchan_id(options.VchanId());
  request->set_notify_retirement(options.notify_retirement);
  request->set_checksum_size(options.ChecksumSize());
  request->set_metadata_size(options.MetadataSize());
  request->set_publisher_id(-1);
  request->set_max_publishers(options.MaxPublishers());
  request->set_use_split_buffers(options.UseSplitBuffers());
  request->set_split_buffers_over_bridge(options.SplitBuffersOverBridge());
  request->set_subscriber_queue_arena_size(options.SubscriberQueueArenaSize());
  request->set_process_id(static_cast<uint64_t>(getpid()));
  request->set_max_outstanding_slot_leases(options.MaxOutstandingSlotLeases());
  request->set_max_slot_size(
      options.max_slot_size > 0 ? Aligned(options.max_slot_size) : 0);
}

void AsioClient::FillSubscriberRequest(CreateSubscriberRequest *request,
                                       const std::string &name,
                                       const SubscriberOptions &options) {
  request->set_channel_name(name);
  request->set_subscriber_id(-1);
  request->set_is_reliable(options.IsReliable());
  request->set_is_bridge(options.IsBridge());
  request->set_for_tunnel(options.ForTunnel());
  request->set_telemetry(options.Telemetry());
  request->set_type(options.Type());
  request->set_max_active_messages(options.MaxActiveMessages());
  request->set_max_subscribers(options.MaxSubscribers());
  request->set_mux(options.Mux());
  request->set_vchan_id(options.VchanId());
  request->set_subscriber_queue_size(options.SubscriberQueueSize());
  request->set_process_id(static_cast<uint64_t>(getpid()));
  request->set_is_local(options.IsLocal());
}

absl::StatusOr<AsioPublisher>
AsioClient::BuildPublisher(const std::string &name,
                           const PublisherOptions &requested_options,
                           RpcResult result) {
  const auto &response = result.response.create_publisher();
  if (!response.error().empty()) {
    return absl::InternalError(response.error());
  }
  PublisherOptions options = requested_options;
  if (response.num_slots() > 0) {
    options.num_slots = response.num_slots();
  }
  auto impl = std::make_shared<details::PublisherImpl>(
      name, options.num_slots, response.subscriber_queue_size(),
      response.subscriber_queue_arena_size(), response.channel_id(),
      response.publisher_id(), response.vchan_id(), session_id_,
      response.type(), options, [](Channel *) { return false; },
      server_user_id_, server_group_id_);
  impl->SetChecksumSize(options.ChecksumSize() > 0 ? options.ChecksumSize()
                                                   : 4);
  impl->SetMetadataSize(options.MetadataSize());
  impl->SetPrefixSize(
      Channel::ComputePrefixSize(impl->ChecksumSize(), impl->MetadataSize()));
  if (absl::Status status = CheckFd(result.fds, response.ccb_fd_index(), "ccb");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = CheckFd(result.fds, response.bcb_fd_index(), "bcb");
      !status.ok()) {
    return status;
  }
  SharedMemoryFds fds(std::move(result.fds[response.ccb_fd_index()]),
                      std::move(result.fds[response.bcb_fd_index()]));
  if (absl::Status status = impl->Map(std::move(fds), scb_fd_); !status.ok()) {
    return status;
  }
  if (absl::Status status =
          impl->CreateOrAttachBuffers(Aligned(options.slot_size));
      !status.ok()) {
    return status;
  }
  impl->SetTriggerFd(std::move(result.fds[response.pub_trigger_fd_index()]));
  impl->SetPollFd(std::move(result.fds[response.pub_poll_fd_index()]));
  for (int index : response.sub_trigger_fd_indexes()) {
    impl->AddSubscriber(std::move(result.fds[index]));
  }
  if (response.retirement_fd_index() >= 0) {
    impl->SetRetirementFd(
        std::move(result.fds[response.retirement_fd_index()]));
  }
  for (int index : response.retirement_fd_indexes()) {
    impl->AddRetirementTrigger(result.fds[index]);
  }
  impl->SetNumUpdates(response.num_sub_updates());
  if (options.IsReliable() && !options.IsBridge() && !options.ForTunnel()) {
    MessageSlot *slot = impl->FindFreeSlotReliable(impl->GetPublisherId());
    if (slot == nullptr) {
      return absl::ResourceExhaustedError(
          "reliable publisher has no activation slot");
    }
    impl->SetSlot(slot);
    if (impl->GetCurrentBufferAddress() == nullptr) {
      return absl::InternalError("reliable publisher has no activation buffer");
    }
    slot->message_size.store(1, std::memory_order_relaxed);
    impl->ActivateSlotAndGetAnother(true, true, false, false);
    impl->SetSlot(nullptr);
  } else if (!options.IsReliable()) {
    auto *slot = impl->FindFreeSlotUnreliable(impl->GetPublisherId());
    if (slot == nullptr) {
      return absl::ResourceExhaustedError("publisher has no free slot");
    }
    impl->SetSlot(slot);
  }
  impl->TriggerSubscribers();
  return AsioPublisher(shared_from_this(), std::move(impl));
}

absl::StatusOr<std::shared_ptr<AsioSubscriber>>
AsioClient::BuildSubscriber(const std::string &name,
                            const SubscriberOptions &requested_options,
                            RpcResult result) {
  const auto &response = result.response.create_subscriber();
  if (!response.error().empty()) {
    return absl::InternalError(response.error());
  }
  SubscriberOptions options = requested_options;
  options.use_split_buffers = response.use_split_buffers();
  const std::string mapped_name = response.resolved_channel_name().empty()
                                      ? name
                                      : response.resolved_channel_name();
  auto impl = std::make_shared<details::SubscriberImpl>(
      mapped_name, response.num_slots(),
      response.default_subscriber_queue_size(),
      response.subscriber_queue_arena_size(), response.subscriber_queue_size(),
      response.channel_id(), response.subscriber_id(), response.vchan_id(),
      session_id_, response.type(), options, [](Channel *) { return false; },
      server_user_id_, server_group_id_);
  impl->SetRequestName(name);
  impl->SetNumSlots(response.num_slots());
  impl->SetEffectiveSubscriberQueueSize(response.subscriber_queue_size());
  impl->SetChecksumSize(response.checksum_size() > 0 ? response.checksum_size()
                                                     : 4);
  impl->SetMetadataSize(std::max(0, response.metadata_size()));
  impl->SetPrefixSize(
      Channel::ComputePrefixSize(impl->ChecksumSize(), impl->MetadataSize()));
  impl->AllocateChecksumBuffer();
  if (absl::Status status = CheckFd(result.fds, response.ccb_fd_index(), "ccb");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = CheckFd(result.fds, response.bcb_fd_index(), "bcb");
      !status.ok()) {
    return status;
  }
  SharedMemoryFds fds(std::move(result.fds[response.ccb_fd_index()]),
                      std::move(result.fds[response.bcb_fd_index()]));
  if (absl::Status status = impl->Map(std::move(fds), scb_fd_); !status.ok()) {
    return status;
  }
  impl->InitActiveMessages();
  if (absl::Status status = impl->AttachBuffers(); !status.ok()) {
    return status;
  }
  impl->SetTriggerFd(std::move(result.fds[response.trigger_fd_index()]));
  impl->SetPollFd(std::move(result.fds[response.poll_fd_index()]));
  for (int index : response.reliable_pub_trigger_fd_indexes()) {
    impl->AddPublisher(std::move(result.fds[index]));
  }
  for (int index : response.retirement_fd_indexes()) {
    impl->AddRetirementTrigger(result.fds[index]);
  }
  impl->SetNumUpdates(response.num_pub_updates());
  impl->Trigger();
  return std::shared_ptr<AsioSubscriber>(
      new AsioSubscriber(shared_from_this(), std::move(impl)));
}

absl::StatusOr<absl::Span<std::byte>>
AsioClient::GetMessageBuffer(details::PublisherImpl *publisher,
                             SlotSizeType max_size) {
  if (publisher->IsReliable()) {
    publisher->ClearPollFd();
  }
  if (max_size > publisher->SlotSize()) {
    if (publisher->IsFixedSize()) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "channel %s has fixed-size slots", publisher->Name()));
    }
    if (const int64_t maximum = publisher->MaxSlotSize();
        maximum != 0 && max_size > maximum) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "requested size %d exceeds maximum slot size %d", max_size, maximum));
    }
    int64_t new_size = publisher->SlotSize();
    while (new_size < max_size) {
      new_size = ExpandSlotSize(new_size);
    }
    if (const int64_t maximum = publisher->MaxSlotSize();
        maximum != 0 && new_size > maximum) {
      new_size = maximum;
    }
    if (absl::Status status = publisher->CreateOrAttachBuffers(new_size);
        !status.ok()) {
      return status;
    }
    publisher->SetSlotToBiggestBuffer(publisher->CurrentSlot());
  }
  if (publisher->IsReliable() && publisher->CurrentSlot() == nullptr) {
    if (publisher->NumSubscribers(publisher->VirtualChannelId()) == 0) {
      return absl::Span<std::byte>();
    }
    publisher->SetSlot(
        publisher->FindFreeSlotReliable(publisher->GetPublisherId()));
    if (publisher->CurrentSlot() == nullptr) {
      return absl::Span<std::byte>();
    }
  }
  void *buffer = publisher->GetCurrentBufferAddress();
  if (buffer == nullptr) {
    return absl::InternalError("publisher has no current buffer");
  }
  return absl::Span<std::byte>(static_cast<std::byte *>(buffer),
                               publisher->SlotSize());
}

void AsioClient::AcquireMessageBuffer(
    std::shared_ptr<details::PublisherImpl> publisher, SlotSizeType max_size,
    std::function<void(absl::StatusOr<absl::Span<std::byte>>)> completion) {
  RefreshPublisher(publisher, [self = shared_from_this(), publisher, max_size,
                               completion = std::move(completion)](
                                  absl::Status status) mutable {
    if (!status.ok()) {
      completion(status);
      return;
    }
    auto result = self->GetMessageBuffer(publisher.get(), max_size);
    if (!result.ok() || !result->empty() || !publisher->IsReliable() ||
        publisher->NumSubscribers(publisher->VirtualChannelId()) == 0) {
      completion(std::move(result));
      return;
    }
    const int fd = ::dup(publisher->GetPollFd().Fd());
    if (fd < 0) {
      completion(absl::InternalError(
          absl::StrFormat("dup publisher poll fd: %s", std::strerror(errno))));
      return;
    }
    auto descriptor = std::make_shared<boost::asio::posix::stream_descriptor>(
        self->executor_, fd);
    descriptor->async_wait(
        boost::asio::posix::stream_descriptor::wait_read,
        boost::asio::bind_executor(
            self->strand_, [self, publisher = std::move(publisher), max_size,
                            descriptor, completion = std::move(completion)](
                               const boost::system::error_code &error) mutable {
              if (error) {
                completion(IoError("publisher wait", error));
                return;
              }
              self->AcquireMessageBuffer(std::move(publisher), max_size,
                                         std::move(completion));
            }));
  });
}

absl::StatusOr<Message>
AsioClient::PublishMessage(details::PublisherImpl *publisher,
                           int64_t message_size) {
  if (message_size <= 0 || message_size > publisher->SlotSize()) {
    return absl::InvalidArgumentError("invalid message size");
  }
  const int old_slot_id = publisher->CurrentSlotId();
  publisher->SetMessageSize(message_size);
  auto published = publisher->ActivateSlotAndGetAnother(
      publisher->IsReliable(), false, false, false, publisher->ForTunnel());
  publisher->SetSlot(published.new_slot);
  publisher->TriggerSubscribers();
  return Message(message_size, nullptr, published.ordinal, published.timestamp,
                 publisher->VirtualChannelId(), false, old_slot_id, false);
}

void AsioClient::RefreshPublisher(
    std::shared_ptr<details::PublisherImpl> publisher,
    std::function<void(absl::Status)> completion) {
  SystemControlBlock *scb = publisher->GetScb();
  const int updates = scb->counters[publisher->GetChannelId()].num_sub_updates;
  if (publisher->NumUpdates() == updates) {
    completion(absl::OkStatus());
    return;
  }
  Request request;
  request.mutable_get_triggers()->set_channel_name(publisher->Name());
  Enqueue(std::move(request), [publisher = std::move(publisher), updates,
                               completion = std::move(completion)](
                                  absl::StatusOr<RpcResult> result) mutable {
    if (!result.ok()) {
      completion(result.status());
      return;
    }
    const auto &response = result->response.get_triggers();
    if (!response.error().empty()) {
      completion(absl::InternalError(response.error()));
      return;
    }
    publisher->ClearSubscribers();
    for (int index : response.sub_trigger_fd_indexes()) {
      publisher->AddSubscriber(std::move(result->fds[index]));
    }
    publisher->ClearRetirementTriggers();
    for (int index : response.retirement_fd_indexes()) {
      publisher->AddRetirementTrigger(result->fds[index]);
    }
    publisher->SetNumUpdates(updates);
    completion(absl::OkStatus());
  });
}

void AsioClient::RefreshSubscriber(
    std::shared_ptr<details::SubscriberImpl> subscriber,
    std::function<void(absl::Status, bool)> completion) {
  SystemControlBlock *scb = subscriber->GetScb();
  const int updates = scb->counters[subscriber->GetChannelId()].num_pub_updates;
  if (subscriber->NumUpdates() == updates) {
    completion(absl::OkStatus(), false);
    return;
  }
  Request request;
  FillSubscriberRequest(request.mutable_create_subscriber(),
                        subscriber->RequestName(), subscriber->options_);
  request.mutable_create_subscriber()->set_subscriber_id(
      subscriber->GetSubscriberId());
  Enqueue(std::move(request), [this, subscriber = std::move(subscriber),
                               updates, completion = std::move(completion)](
                                  absl::StatusOr<RpcResult> result) mutable {
    if (!result.ok()) {
      completion(result.status(), false);
      return;
    }
    const auto &response = result->response.create_subscriber();
    if (!response.error().empty()) {
      completion(absl::InternalError(response.error()), false);
      return;
    }
    const bool remap = subscriber->IsPlaceholder();
    if (remap) {
      if (absl::Status status =
              CheckFd(result->fds, response.ccb_fd_index(), "ccb");
          !status.ok()) {
        completion(status, false);
        return;
      }
      if (absl::Status status =
              CheckFd(result->fds, response.bcb_fd_index(), "bcb");
          !status.ok()) {
        completion(status, false);
        return;
      }
      subscriber->ResetDeliveryState();
      subscriber->Unmap();
    }
    if (!response.type().empty()) {
      subscriber->SetType(response.type());
    }
    subscriber->options_.use_split_buffers = response.use_split_buffers();
    subscriber->SetNumSlots(response.num_slots());
    subscriber->SetSubscriberQueueSize(
        response.default_subscriber_queue_size());
    subscriber->SetSubscriberQueueArenaSize(
        response.subscriber_queue_arena_size());
    subscriber->SetEffectiveSubscriberQueueSize(
        response.subscriber_queue_size());
    subscriber->SetChecksumSize(
        response.checksum_size() > 0 ? response.checksum_size() : 4);
    subscriber->SetMetadataSize(std::max(0, response.metadata_size()));
    subscriber->SetPrefixSize(Channel::ComputePrefixSize(
        subscriber->ChecksumSize(), subscriber->MetadataSize()));
    subscriber->AllocateChecksumBuffer();
    if (remap) {
      SharedMemoryFds fds(std::move(result->fds[response.ccb_fd_index()]),
                          std::move(result->fds[response.bcb_fd_index()]));
      if (absl::Status status = subscriber->Map(std::move(fds), scb_fd_);
          !status.ok()) {
        completion(status, false);
        return;
      }
      subscriber->InitActiveMessages();
    }
    if (absl::Status status = subscriber->AttachBuffers(); !status.ok()) {
      completion(status, false);
      return;
    }
    if (absl::Status status =
            CheckFd(result->fds, response.trigger_fd_index(), "trigger");
        !status.ok()) {
      completion(status, false);
      return;
    }
    if (absl::Status status =
            CheckFd(result->fds, response.poll_fd_index(), "poll");
        !status.ok()) {
      completion(status, false);
      return;
    }
    subscriber->SetTriggerFd(
        std::move(result->fds[response.trigger_fd_index()]));
    subscriber->SetPollFd(std::move(result->fds[response.poll_fd_index()]));
    subscriber->ClearPublishers();
    for (int index : response.reliable_pub_trigger_fd_indexes()) {
      subscriber->AddPublisher(std::move(result->fds[index]));
    }
    subscriber->ClearRetirementTriggers();
    for (int index : response.retirement_fd_indexes()) {
      subscriber->AddRetirementTrigger(result->fds[index]);
    }
    subscriber->SetNumUpdates(updates);
    completion(absl::OkStatus(), true);
  });
}

absl::StatusOr<Message>
AsioClient::ReadMessage(details::SubscriberImpl *subscriber, ReadMode mode,
                        ClearTrigger clear_trigger) {
  if (clear_trigger == ClearTrigger::kClearTrigger) {
    subscriber->ClearPollFd();
  }
  if (subscriber->IsPlaceholder()) {
    return Message();
  }
  MessageSlot *slot = mode == ReadMode::kReadNext ? subscriber->NextSlot()
                                                  : subscriber->LastSlot();
  if (slot == nullptr) {
    subscriber->TriggerReliablePublishers();
    if (absl::Status status = subscriber->UnmapUnusedBuffers(); !status.ok()) {
      return status;
    }
    return Message();
  }
  subscriber->SetSlot(slot);
  MessagePrefix *prefix = subscriber->Prefix(slot);
  const bool activation =
      prefix != nullptr && (prefix->flags & kMessageActivate) != 0;
  bool checksum_error = false;
  size_t size = slot->message_size.load(std::memory_order_relaxed);
  if (prefix != nullptr && prefix->HasChecksum()) {
    auto checksum_data = GetMessageChecksumData(
        prefix, subscriber->GetCurrentBufferAddress(), size,
        subscriber->ChecksumSize(), subscriber->MetadataSize());
    checksum_error = !subscriber->ValidateChecksum(
        checksum_data.Spans(),
        GetChecksumSpan(prefix, subscriber->ChecksumSize()));
  }
  if (activation && !subscriber->options_.pass_activation) {
    subscriber->IgnoreActivation(slot);
    if (subscriber->IsReliable()) {
      subscriber->TriggerReliablePublishers();
    }
    return ReadMessage(subscriber, mode, ClearTrigger::kNoClearTrigger);
  }
  if (subscriber->on_receive_callback_ != nullptr) {
    absl::StatusOr<int64_t> delivered = subscriber->on_receive_callback_(
        subscriber->GetCurrentBufferAddress(), static_cast<int64_t>(size));
    if (!delivered.ok()) {
      subscriber->UnreadSlot(slot);
      subscriber->SetSlot(nullptr);
      return delivered.status();
    }
    if (*delivered <= 0) {
      subscriber->UnreadSlot(slot);
      subscriber->SetSlot(nullptr);
      return Message();
    }
    size = static_cast<size_t>(*delivered);
  }
  subscriber->ClearActiveMessage();
  auto active = subscriber->SetActiveMessage(
      size, slot, subscriber->GetCurrentBufferAddress(),
      subscriber->CurrentOrdinal(), subscriber->Timestamp(slot),
      slot->vchan_id.load(std::memory_order_relaxed), activation,
      checksum_error);
  if (active->length == 0) {
    subscriber->UnreadSlot(slot);
    return Message();
  }
  subscriber->ClaimSlot(slot, slot->vchan_id.load(std::memory_order_relaxed),
                        mode == ReadMode::kReadNewest);
  Message message(active);
  if (subscriber->IsBridge()) {
    subscriber->ClearActiveMessage();
  }
  if (checksum_error && !subscriber->PassChecksumErrors()) {
    return absl::InternalError("checksum verification failed");
  }
  return message;
}

void AsioClient::RemovePublisher(
    std::shared_ptr<details::PublisherImpl> publisher) {
  Request request;
  auto *remove = request.mutable_remove_publisher();
  remove->set_channel_name(publisher->Name());
  remove->set_publisher_id(publisher->GetPublisherId());
  Enqueue(std::move(request), [publisher = std::move(publisher)](auto) mutable {
    publisher.reset();
  });
}

void AsioClient::RemoveSubscriber(
    std::shared_ptr<details::SubscriberImpl> subscriber) {
  Request request;
  auto *remove = request.mutable_remove_subscriber();
  remove->set_channel_name(subscriber->RequestName());
  remove->set_subscriber_id(subscriber->GetSubscriberId());
  remove->set_telemetry(subscriber->options_.Telemetry());
  Enqueue(std::move(request), [subscriber = std::move(subscriber)](
                                  auto) mutable { subscriber.reset(); });
}

AsioPublisher::AsioPublisher(std::shared_ptr<AsioClient> client,
                             std::shared_ptr<details::PublisherImpl> impl)
    : client_(std::move(client)), impl_(std::move(impl)) {}

AsioPublisher::~AsioPublisher() {
  if (impl_ != nullptr) {
    client_->RemovePublisher(std::move(impl_));
  }
}

absl::StatusOr<absl::Span<std::byte>>
AsioPublisher::GetMessageBuffer(SlotSizeType max_size) {
  return client_->GetMessageBuffer(impl_.get(), max_size);
}

absl::StatusOr<Message> AsioPublisher::PublishMessage(int64_t message_size) {
  return client_->PublishMessage(impl_.get(), message_size);
}

bool AsioPublisher::IsReliable() const { return impl_->IsReliable(); }
int32_t AsioPublisher::NumSubscribers() const {
  return impl_->NumSubscribers(impl_->VirtualChannelId());
}
std::string AsioPublisher::Name() const { return impl_->Name(); }
struct pollfd AsioPublisher::GetPollFd() const {
  pollfd fd{impl_->GetPollFd().Fd(), POLLIN, 0};
  return fd;
}

AsioSubscriber::AsioSubscriber(std::shared_ptr<AsioClient> client,
                               std::shared_ptr<details::SubscriberImpl> impl)
    : client_(std::move(client)), impl_(std::move(impl)),
      event_fd_(client_->GetExecutor(), ::dup(impl_->GetPollFd().Fd())) {}

AsioSubscriber::~AsioSubscriber() {
  boost::system::error_code ignored;
  event_fd_.cancel(ignored);
  if (impl_ != nullptr) {
    client_->RemoveSubscriber(std::move(impl_));
  }
}

void AsioSubscriber::Start(std::function<void(Message &&)> on_message) {
  if (running_.exchange(true)) {
    return;
  }
  on_message_ = std::move(on_message);
  Drain();
}

void AsioSubscriber::Shutdown() {
  running_ = false;
  boost::system::error_code ignored;
  event_fd_.cancel(ignored);
}

void AsioSubscriber::ResetEventFd() {
  boost::system::error_code ignored;
  event_fd_.cancel(ignored);
  event_fd_.close(ignored);
  event_fd_.assign(::dup(impl_->GetPollFd().Fd()), ignored);
}

void AsioSubscriber::WaitForNotification() {
  auto self = shared_from_this();
  event_fd_.async_wait(boost::asio::posix::stream_descriptor::wait_read,
                       [self](const boost::system::error_code &error) {
                         if (!error && self->running_) {
                           self->Drain();
                         }
                       });
}

void AsioSubscriber::Drain() {
  if (!running_) {
    return;
  }
  AsyncReadMessage(ReadMode::kReadNext,
                   [self = shared_from_this()](absl::StatusOr<Message> result) {
                     if (!result.ok()) {
                       self->Shutdown();
                       return;
                     }
                     if (result->buffer == nullptr) {
                       self->WaitForNotification();
                       return;
                     }
                     self->on_message_(std::move(result).value());
                     self->Drain();
                   });
}

std::string AsioSubscriber::Name() const { return impl_->RequestName(); }
std::string_view AsioSubscriber::Type() const { return impl_->Type(); }
int32_t AsioSubscriber::NumSlots() const { return impl_->NumSlots(); }
SlotSizeType AsioSubscriber::SlotSize() const { return impl_->SlotSize(); }
int32_t AsioSubscriber::ChecksumSize() const { return impl_->ChecksumSize(); }
int32_t AsioSubscriber::MetadataSize() const { return impl_->MetadataSize(); }
ChannelCounters AsioSubscriber::GetChannelCounters() const {
  return impl_->GetCounters();
}
struct pollfd AsioSubscriber::GetPollFd() const {
  pollfd fd{impl_->GetPollFd().Fd(), POLLIN, 0};
  return fd;
}

} // namespace subspace
