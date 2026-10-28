// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/asio/asio_client.h"

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "client/checksum.h"
#include "client/protocol.h"
#include "common/channel.h"
#include "toolbelt/clock.h"

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
#include <unordered_set>

namespace subspace {
namespace {

constexpr uint32_t kMaxControlResponseSize = 64 * 1024 * 1024;

absl::StatusOr<boost::asio::local::stream_protocol::endpoint>
MakeEndpoint(const std::string &pathname) {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
#ifdef __linux__
  if (pathname.size() > sizeof(address.sun_path) - 2) {
    return absl::InvalidArgumentError("abstract Unix socket name is too long");
  }
  address.sun_path[0] = '\0';
  std::memcpy(address.sun_path + 1, pathname.data(), pathname.size());
#else
  if (pathname.size() > sizeof(address.sun_path) - 1) {
    return absl::InvalidArgumentError("Unix socket path is too long");
  }
  std::memcpy(address.sun_path, pathname.data(), pathname.size());
#endif
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

template <typename Indices>
absl::Status CheckFds(const std::vector<toolbelt::FileDescriptor> &fds,
                      const Indices &indices, std::string_view name) {
  for (int index : indices) {
    if (absl::Status status = CheckFd(fds, index, name); !status.ok()) {
      return status;
    }
  }
  return absl::OkStatus();
}

template <typename... IndexGroups>
absl::Status CheckDistinctFds(std::initializer_list<int> scalar_indices,
                              const IndexGroups &...index_groups) {
  std::unordered_set<int> seen;
  auto add = [&seen](int index) -> absl::Status {
    if (index >= 0 && !seen.insert(index).second) {
      return absl::InternalError(
          absl::StrFormat("descriptor index %d is used more than once", index));
    }
    return absl::OkStatus();
  };
  for (int index : scalar_indices) {
    if (absl::Status status = add(index); !status.ok()) {
      return status;
    }
  }
  absl::Status status = absl::OkStatus();
  auto add_group = [&](const auto &indices) {
    if (!status.ok()) {
      return;
    }
    for (int index : indices) {
      if (status = add(index); !status.ok()) {
        return;
      }
    }
  };
  (add_group(index_groups), ...);
  return status;
}

} // namespace

AsioClient::AsioClient(Executor executor, std::string server_socket,
                       std::string client_name)
    : state_(std::shared_ptr<AsioClientState>(
          new AsioClientState(std::move(executor)))) {
  state_->Init(std::move(server_socket), std::move(client_name));
}

bool AsioClient::IsConnected() const { return state_->IsConnected(); }

const AsioClient::Executor &AsioClient::GetExecutor() const {
  return state_->GetExecutor();
}

void AsioClient::SetDebug(bool enabled) { state_->SetDebug(enabled); }

AsioClientState::AsioClientState(Executor executor)
    : executor_(std::move(executor)), strand_(executor_), socket_(executor_),
      control_buffer_(CMSG_SPACE(4096 * sizeof(int)), '\0') {}

AsioClientState::~AsioClientState() {
  is_connected_.store(false, std::memory_order_release);
  boost::system::error_code ignored;
  socket_.close(ignored);
  Fail(absl::CancelledError("AsioClient destroyed"));
}

void AsioClientState::Init(std::string socket, std::string name) {
  name_ = std::move(name);
  auto endpoint = MakeEndpoint(socket);
  if (!endpoint.ok()) {
    Fail(endpoint.status());
    return;
  }
  auto self = shared_from_this();
  socket_.async_connect(
      std::move(*endpoint),
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error) {
            if (error) {
              self->FailTransport(IoError("connect", error));
              return;
            }
            self->transport_connected_ = true;
            Request request;
            auto *init = request.mutable_init();
            init->set_client_name(self->name_);
            init->set_max_channels(kMaxChannels);
            init->set_max_slot_owners(kMaxSlotOwners);
            init->set_max_buffers(static_cast<int32_t>(kMaxBuffers));
            self->Enqueue(
                std::move(request),
                [self](absl::StatusOr<RpcResult> result) {
                  if (!result.ok()) {
                    return;
                  }
                  const auto &response = result->response.init();
                  if (!response.error().empty()) {
                    self->Fail(absl::InternalError(response.error()));
                    return;
                  }
                  if (absl::Status status =
                          CheckFd(result->fds, response.scb_fd_index(), "scb");
                      !status.ok()) {
                    self->Fail(status);
                    return;
                  }
                  self->scb_fd_ =
                      std::move(result->fds[response.scb_fd_index()]);
                  self->session_id_ = response.session_id();
                  self->server_user_id_ = response.user_id();
                  self->server_group_id_ = response.group_id();
                  self->is_connected_.store(true, std::memory_order_release);
                },
                {}, true, true);
          }));
}

void AsioClientState::Enqueue(
    Request request, ControlCompletion completion,
    std::vector<toolbelt::FileDescriptor> outbound_fds, bool expects_response,
    bool prioritize) {
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
  command.outbound_fds = std::move(outbound_fds);
  command.expects_response = expects_response;
  command.completion = std::move(completion);
  auto self = shared_from_this();
  auto enqueue = [self, command = std::move(command), prioritize]() mutable {
    if (!self->terminal_status_.ok()) {
      command.completion(self->terminal_status_);
      return;
    }
    if (prioritize) {
      self->commands_.push_front(std::move(command));
    } else {
      self->commands_.push_back(std::move(command));
    }
    self->StartNext();
  };
  if (strand_.running_in_this_thread()) {
    enqueue();
  } else {
    boost::asio::post(strand_, std::move(enqueue));
  }
}

void AsioClientState::StartNext() {
  if (!transport_connected_ || active_ || commands_.empty()) {
    return;
  }
  active_ = true;
  auto self = shared_from_this();
  boost::asio::async_write(
      socket_, boost::asio::buffer(commands_.front().outbound),
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error, size_t) {
            if (error) {
              self->FailTransport(IoError("control write", error));
              return;
            }
            if (!self->commands_.front().outbound_fds.empty()) {
              self->SendRequestFds();
            } else if (self->commands_.front().expects_response) {
              self->ReadResponseLength();
            } else {
              self->Finish(RpcResult());
            }
          }));
}

void AsioClientState::SendRequestFds() {
  const auto &fds = commands_.front().outbound_fds;
  int32_t count = static_cast<int32_t>(fds.size());
  std::vector<char> control(CMSG_SPACE(fds.size() * sizeof(int)), '\0');
  iovec iov{&count, sizeof(count)};
  msghdr message{};
  message.msg_iov = &iov;
  message.msg_iovlen = 1;
  message.msg_control = control.data();
  message.msg_controllen = control.size();
  cmsghdr *cmsg = CMSG_FIRSTHDR(&message);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(fds.size() * sizeof(int));
  int *raw_fds = reinterpret_cast<int *>(CMSG_DATA(cmsg));
  for (size_t index = 0; index < fds.size(); ++index) {
    raw_fds[index] = fds[index].Fd();
  }
  message.msg_controllen = cmsg->cmsg_len;
  int flags = MSG_DONTWAIT;
#ifdef MSG_NOSIGNAL
  flags |= MSG_NOSIGNAL;
#endif
  const ssize_t sent = ::sendmsg(socket_.native_handle(), &message, flags);
  if (sent < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      auto self = shared_from_this();
      socket_.async_wait(
          boost::asio::socket_base::wait_write,
          boost::asio::bind_executor(
              strand_, [self](const boost::system::error_code &error) {
                if (error) {
                  self->FailTransport(IoError("fd write wait", error));
                  return;
                }
                self->SendRequestFds();
              }));
      return;
    }
    FailTransport(absl::InternalError(
        absl::StrFormat("failed to send fds: %s", std::strerror(errno))));
    return;
  }
  if (sent != sizeof(count)) {
    FailTransport(absl::DataLossError("short descriptor-count write"));
    return;
  }
  if (commands_.front().expects_response) {
    ReadResponseLength();
  } else {
    Finish(RpcResult());
  }
}

void AsioClientState::ReadResponseLength() {
  auto self = shared_from_this();
  boost::asio::async_read(
      socket_, boost::asio::buffer(length_),
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error, size_t) {
            if (error) {
              self->FailTransport(IoError("response length read", error));
              return;
            }
            uint32_t network_size = 0;
            std::memcpy(&network_size, self->length_.data(),
                        sizeof(network_size));
            self->response_size_ = ntohl(network_size);
            if (self->response_size_ > kMaxControlResponseSize) {
              self->FailTransport(absl::ResourceExhaustedError(
                  "control response exceeds maximum size"));
              return;
            }
            self->ReadResponseBody();
          }));
}

void AsioClientState::ReadResponseBody() {
  response_body_.assign(response_size_, '\0');
  auto self = shared_from_this();
  boost::asio::async_read(
      socket_, boost::asio::buffer(response_body_),
      boost::asio::bind_executor(
          strand_, [self](const boost::system::error_code &error, size_t) {
            if (error) {
              self->FailTransport(IoError("response body read", error));
              return;
            }
            self->result_ = RpcResult();
            if (!self->result_.response.ParseFromArray(
                    self->response_body_.data(),
                    static_cast<int>(self->response_body_.size()))) {
              self->FailTransport(
                  absl::DataLossError("failed to parse response"));
              return;
            }
            self->expected_fds_ = -1;
            self->fd_count_received_ = 0;
            self->ReceiveFds();
          }));
}

void AsioClientState::ReceiveFds() {
  auto self = shared_from_this();
  socket_.async_wait(
      boost::asio::socket_base::wait_read,
      boost::asio::bind_executor(strand_, [self](const boost::system::error_code
                                                     &error) {
        if (error) {
          self->FailTransport(IoError("fd wait", error));
          return;
        }
        std::fill(self->control_buffer_.begin(), self->control_buffer_.end(),
                  '\0');
        iovec iov{self->fd_count_.data() + self->fd_count_received_,
                  self->fd_count_.size() - self->fd_count_received_};
        msghdr message{};
        message.msg_iov = &iov;
        message.msg_iovlen = 1;
        message.msg_control = self->control_buffer_.data();
        message.msg_controllen = self->control_buffer_.size();
        const ssize_t received =
            ::recvmsg(self->socket_.native_handle(), &message, MSG_DONTWAIT);
        if (received < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            self->ReceiveFds();
            return;
          }
          self->FailTransport(absl::InternalError(absl::StrFormat(
              "failed to receive fds: %s", std::strerror(errno))));
          return;
        }
        if (received == 0) {
          self->FailTransport(absl::DataLossError("EOF while receiving fds"));
          return;
        }
        self->fd_count_received_ += static_cast<size_t>(received);
        for (cmsghdr *cmsg = CMSG_FIRSTHDR(&message); cmsg != nullptr;
             cmsg = CMSG_NXTHDR(&message, cmsg)) {
          if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
            continue;
          }
          if (cmsg->cmsg_len < CMSG_LEN(0) ||
              (cmsg->cmsg_len - CMSG_LEN(0)) % sizeof(int) != 0) {
            self->FailTransport(
                absl::DataLossError("invalid SCM_RIGHTS control length"));
            return;
          }
          const size_t count = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
          const int *fds = reinterpret_cast<int *>(CMSG_DATA(cmsg));
          for (size_t i = 0; i < count; ++i) {
            self->result_.fds.emplace_back(fds[i]);
          }
        }
        if ((message.msg_flags & MSG_CTRUNC) != 0) {
          self->FailTransport(absl::ResourceExhaustedError(
              "ancillary fd message was truncated"));
          return;
        }
        if (self->fd_count_received_ < self->fd_count_.size()) {
          self->ReceiveFds();
          return;
        }
        int32_t total = 0;
        std::memcpy(&total, self->fd_count_.data(), sizeof(total));
        self->fd_count_received_ = 0;
        if (total < 0 || total > 4096) {
          self->FailTransport(absl::DataLossError("invalid descriptor count"));
          return;
        }
        if (self->expected_fds_ < 0) {
          self->expected_fds_ = total;
        } else if (self->expected_fds_ != total) {
          self->FailTransport(
              absl::DataLossError("descriptor count changed between batches"));
          return;
        }
        if (static_cast<int>(self->result_.fds.size()) == self->expected_fds_) {
          self->Finish(std::move(self->result_));
        } else if (static_cast<int>(self->result_.fds.size()) >
                   self->expected_fds_) {
          self->FailTransport(
              absl::DataLossError("received too many descriptors"));
        } else {
          self->ReceiveFds();
        }
      }));
}

void AsioClientState::Finish(absl::StatusOr<RpcResult> result) {
  if (commands_.empty()) {
    return;
  }
  auto completion = std::move(commands_.front().completion);
  commands_.pop_front();
  active_ = false;
  completion(std::move(result));
  StartNext();
}

void AsioClientState::FailTransport(absl::Status status) {
  is_connected_.store(false, std::memory_order_release);
  transport_connected_ = false;
  boost::system::error_code ignored;
  socket_.close(ignored);
  result_ = RpcResult();
  response_body_.clear();
  Fail(std::move(status));
}

void AsioClientState::Fail(absl::Status status) {
  is_connected_.store(false, std::memory_order_release);
  terminal_status_ = status;
  while (!commands_.empty()) {
    auto completion = std::move(commands_.front().completion);
    commands_.pop_front();
    completion(status);
  }
  active_ = false;
}

void AsioClientState::FillPublisherRequest(CreatePublisherRequest *request,
                                           const std::string &name,
                                           const PublisherOptions &options) {
  details::FillCreatePublisherRequest(request, name, options, -1);
}

void AsioClientState::FillSubscriberRequest(CreateSubscriberRequest *request,
                                            const std::string &name,
                                            const SubscriberOptions &options) {
  details::FillCreateSubscriberRequest(request, name, options, -1);
}

absl::StatusOr<AsioPublisher>
AsioClientState::BuildPublisher(const std::string &name,
                                const PublisherOptions &requested_options,
                                RpcResult result) {
  const auto &response = result.response.create_publisher();
  if (!response.error().empty()) {
    return absl::InternalError(response.error());
  }
  auto rollback =
      absl::MakeCleanup([this, name, publisher_id = response.publisher_id()] {
        Request request;
        auto *remove = request.mutable_remove_publisher();
        remove->set_channel_name(name);
        remove->set_publisher_id(publisher_id);
        Enqueue(std::move(request), [](absl::StatusOr<RpcResult>) {});
      });
  PublisherOptions options = requested_options;
  if (response.num_slots() > 0) {
    options.num_slots = response.num_slots();
  }
  auto impl = std::make_shared<details::PublisherImpl>(
      name, options.num_slots, response.subscriber_queue_size(),
      response.subscriber_queue_arena_size(), response.channel_id(),
      response.publisher_id(), response.vchan_id(), session_id_,
      response.type(), options,
      [](Channel *channel) {
        auto *client_channel = static_cast<details::ClientChannel *>(channel);
        return client_channel->BuffersChanged() &&
               client_channel->AttachBuffers().ok();
      },
      server_user_id_, server_group_id_);
  std::weak_ptr<AsioClientState> weak = shared_from_this();
  impl->SetClientBufferRegistrationCallback(
      [weak](const ClientBufferHandleMetadata &metadata,
             const toolbelt::FileDescriptor *fd) {
        auto client = weak.lock();
        return client != nullptr ? client->RegisterClientBuffer(metadata, fd)
                                 : absl::CancelledError("AsioClient destroyed");
      });
  impl->SetClientBufferLookupCallback([weak](const std::string &channel_name,
                                             uint64_t session_id,
                                             uint32_t buffer_index) {
    auto client = weak.lock();
    return client != nullptr
               ? client->GetClientBuffers(channel_name, session_id,
                                          buffer_index)
               : absl::StatusOr<std::vector<RegisteredClientBuffer>>(
                     absl::CancelledError("AsioClient destroyed"));
  });
  impl->SetClientBufferUnregistrationCallback(
      [weak](const std::string &channel_name, uint64_t session_id,
             uint32_t buffer_index) {
        auto client = weak.lock();
        return client != nullptr ? client->UnregisterClientBuffer(
                                       channel_name, session_id, buffer_index)
                                 : absl::CancelledError("AsioClient destroyed");
      });
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
  if (absl::Status status = CheckFd(result.fds, response.pub_trigger_fd_index(),
                                    "publisher trigger");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          CheckFd(result.fds, response.pub_poll_fd_index(), "publisher poll");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = CheckFds(
          result.fds, response.sub_trigger_fd_indexes(), "subscriber trigger");
      !status.ok()) {
    return status;
  }
  if (response.retirement_fd_index() >= 0) {
    if (absl::Status status =
            CheckFd(result.fds, response.retirement_fd_index(),
                    "retirement notification");
        !status.ok()) {
      return status;
    }
  }
  if (absl::Status status = CheckFds(
          result.fds, response.retirement_fd_indexes(), "retirement trigger");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = CheckDistinctFds(
          {response.ccb_fd_index(), response.bcb_fd_index(),
           response.pub_trigger_fd_index(), response.pub_poll_fd_index(),
           response.retirement_fd_index()},
          response.sub_trigger_fd_indexes(), response.retirement_fd_indexes());
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
    if (!options.IsBridge() && options.Activate() &&
        !impl->IsActivated(impl->VirtualChannelId())) {
      if (impl->GetCurrentBufferAddress() == nullptr) {
        return absl::InternalError("publisher has no activation buffer");
      }
      slot->message_size.store(1, std::memory_order_relaxed);
      auto activation = impl->ActivateSlotAndGetAnother(
          false, true, false, false, impl->ForTunnel());
      impl->SetSlot(activation.new_slot);
      if (impl->CurrentSlot() == nullptr) {
        return absl::ResourceExhaustedError(
            "publisher has no slot after activation");
      }
    }
  }
  impl->TriggerSubscribers();
  if (absl::Status status = impl->UnmapUnusedBuffers(); !status.ok()) {
    return status;
  }
  std::shared_ptr<boost::asio::posix::stream_descriptor> poll_descriptor;
  if (impl->IsReliable()) {
    const int fd = ::dup(impl->GetPollFd().Fd());
    if (fd < 0) {
      return absl::InternalError(
          absl::StrFormat("dup publisher poll fd: %s", std::strerror(errno)));
    }
    poll_descriptor =
        std::make_shared<boost::asio::posix::stream_descriptor>(executor_, fd);
  }
  std::move(rollback).Cancel();
  return AsioPublisher(shared_from_this(), std::move(impl),
                       std::move(poll_descriptor));
}

absl::StatusOr<std::shared_ptr<AsioSubscriber>>
AsioClientState::BuildSubscriber(const std::string &name,
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
      session_id_, response.type(), options,
      [](Channel *channel) {
        auto *client_channel = static_cast<details::ClientChannel *>(channel);
        return client_channel->BuffersChanged() &&
               client_channel->AttachBuffers().ok();
      },
      server_user_id_, server_group_id_);
  std::weak_ptr<AsioClientState> weak = shared_from_this();
  impl->SetClientBufferLookupCallback([weak](const std::string &channel_name,
                                             uint64_t session_id,
                                             uint32_t buffer_index) {
    auto client = weak.lock();
    return client != nullptr
               ? client->GetClientBuffers(channel_name, session_id,
                                          buffer_index)
               : absl::StatusOr<std::vector<RegisteredClientBuffer>>(
                     absl::CancelledError("AsioClient destroyed"));
  });
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
  if (absl::Status status = CheckFd(result.fds, response.trigger_fd_index(),
                                    "subscriber trigger");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          CheckFd(result.fds, response.poll_fd_index(), "subscriber poll");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          CheckFds(result.fds, response.reliable_pub_trigger_fd_indexes(),
                   "reliable publisher trigger");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = CheckFds(
          result.fds, response.retirement_fd_indexes(), "retirement trigger");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = CheckDistinctFds(
          {response.ccb_fd_index(), response.bcb_fd_index(),
           response.trigger_fd_index(), response.poll_fd_index()},
          response.reliable_pub_trigger_fd_indexes(),
          response.retirement_fd_indexes());
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
  const int event_fd = ::dup(impl->GetPollFd().Fd());
  if (event_fd < 0) {
    return absl::InternalError(
        absl::StrFormat("dup subscriber poll fd: %s", std::strerror(errno)));
  }
  auto descriptor = std::make_shared<boost::asio::posix::stream_descriptor>(
      executor_, event_fd);
  return std::shared_ptr<AsioSubscriber>(new AsioSubscriber(
      shared_from_this(), std::move(impl), std::move(descriptor)));
}

absl::StatusOr<absl::Span<std::byte>>
AsioClientState::GetMessageBuffer(details::PublisherImpl *publisher,
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
      new_size = details::ExpandSlotSize(new_size);
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

void AsioClientState::AcquireMessageBuffer(
    std::shared_ptr<details::PublisherImpl> publisher, SlotSizeType max_size,
    std::shared_ptr<boost::asio::posix::stream_descriptor> poll_descriptor,
    std::shared_ptr<std::atomic_bool> cancelled,
    std::function<void(absl::StatusOr<absl::Span<std::byte>>)> completion) {
  RefreshPublisher(publisher, [self = shared_from_this(), publisher, max_size,
                               poll_descriptor, cancelled,
                               completion = std::move(completion)](
                                  absl::Status status) mutable {
    if (cancelled->load(std::memory_order_acquire)) {
      completion(
          absl::CancelledError("publisher buffer acquisition cancelled"));
      return;
    }
    if (!status.ok()) {
      completion(status);
      return;
    }
    boost::asio::post(
        boost::asio::system_executor(),
        [self, publisher = std::move(publisher), max_size, poll_descriptor,
         cancelled, completion = std::move(completion)]() mutable {
          auto result = self->GetMessageBuffer(publisher.get(), max_size);
          if (cancelled->load(std::memory_order_acquire)) {
            completion(
                absl::CancelledError("publisher buffer acquisition cancelled"));
            return;
          }
          if (!result.ok() || !result->empty() || !publisher->IsReliable() ||
              publisher->NumSubscribers(publisher->VirtualChannelId()) == 0) {
            completion(std::move(result));
            return;
          }
          if (poll_descriptor == nullptr) {
            completion(
                absl::InternalError("reliable publisher has no poll fd"));
            return;
          }
          boost::asio::post(
              self->strand_, [self, publisher = std::move(publisher), max_size,
                              poll_descriptor, cancelled,
                              completion = std::move(completion)]() mutable {
                if (cancelled->load(std::memory_order_acquire)) {
                  completion(absl::CancelledError(
                      "publisher buffer acquisition cancelled"));
                  return;
                }
                poll_descriptor->async_wait(
                    boost::asio::posix::stream_descriptor::wait_read,
                    boost::asio::bind_executor(
                        self->strand_,
                        [self, publisher = std::move(publisher), max_size,
                         poll_descriptor, cancelled,
                         completion = std::move(completion)](
                            const boost::system::error_code &error) mutable {
                          if (error) {
                            completion(
                                error == boost::asio::error::operation_aborted
                                    ? absl::Status(absl::CancelledError(
                                          "publisher buffer acquisition "
                                          "cancelled"))
                                    : IoError("publisher wait", error));
                            return;
                          }
                          self->AcquireMessageBuffer(
                              std::move(publisher), max_size,
                              std::move(poll_descriptor), std::move(cancelled),
                              std::move(completion));
                        }));
              });
        });
  });
}

absl::StatusOr<Message>
AsioClientState::PublishMessage(details::PublisherImpl *publisher,
                                int64_t message_size) {
  if (publisher->CurrentSlot() == nullptr) {
    return absl::FailedPreconditionError(
        "GetMessageBuffer must complete before PublishMessage");
  }
  if (message_size <= 0 || message_size > publisher->SlotSize()) {
    return absl::InvalidArgumentError("invalid message size");
  }
  if (publisher->on_send_callback_ != nullptr) {
    absl::StatusOr<int64_t> delivered = publisher->on_send_callback_(
        publisher->GetCurrentBufferAddress(), message_size);
    if (!delivered.ok()) {
      return delivered.status();
    }
    message_size = *delivered;
    if (message_size <= 0 || message_size > publisher->SlotSize()) {
      return absl::InvalidArgumentError(
          "send callback returned an invalid message size");
    }
  }
  const int old_slot_id = publisher->CurrentSlotId();
  publisher->SetMessageSize(message_size);
  auto published = publisher->ActivateSlotAndGetAnother(
      publisher->IsReliable(), false, false, false, publisher->ForTunnel());
  publisher->SetSlot(published.new_slot);
  publisher->TriggerSubscribers();
  if (absl::Status status = publisher->UnmapUnusedBuffers(); !status.ok()) {
    return status;
  }
  if (published.new_slot == nullptr) {
    if (publisher->IsReliable()) {
      return Message(message_size, nullptr, published.ordinal,
                     published.timestamp, publisher->VirtualChannelId(), false,
                     -1, false);
    }
    return absl::ResourceExhaustedError(
        absl::StrFormat("out of slots for channel %s", publisher->Name()));
  }
  return Message(message_size, nullptr, published.ordinal, published.timestamp,
                 publisher->VirtualChannelId(), false, old_slot_id, false);
}

void AsioClientState::RefreshPublisher(
    std::shared_ptr<details::PublisherImpl> publisher,
    std::function<void(absl::Status)> completion) {
  auto self = shared_from_this();
  boost::asio::post(strand_, [self, publisher = std::move(publisher),
                              completion = std::move(completion)]() mutable {
    SystemControlBlock *scb = publisher->GetScb();
    const int updates =
        scb->counters[publisher->GetChannelId()].num_sub_updates;
    if (publisher->NumUpdates() == updates) {
      completion(absl::OkStatus());
      return;
    }
    Request request;
    request.mutable_get_triggers()->set_channel_name(publisher->Name());
    self->Enqueue(
        std::move(request), [publisher = std::move(publisher), updates,
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
          if (absl::Status status =
                  CheckFds(result->fds, response.sub_trigger_fd_indexes(),
                           "subscriber trigger");
              !status.ok()) {
            completion(status);
            return;
          }
          if (absl::Status status =
                  CheckFds(result->fds, response.retirement_fd_indexes(),
                           "retirement trigger");
              !status.ok()) {
            completion(status);
            return;
          }
          if (absl::Status status =
                  CheckDistinctFds({}, response.sub_trigger_fd_indexes(),
                                   response.retirement_fd_indexes());
              !status.ok()) {
            completion(status);
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
  });
}

void AsioClientState::RefreshSubscriber(
    std::shared_ptr<details::SubscriberImpl> subscriber,
    ClearTrigger clear_trigger,
    std::function<void(absl::Status, bool)> completion) {
  if (!strand_.running_in_this_thread()) {
    boost::asio::post(
        strand_, [self = shared_from_this(), subscriber = std::move(subscriber),
                  clear_trigger, completion = std::move(completion)]() mutable {
          self->RefreshSubscriber(std::move(subscriber), clear_trigger,
                                  std::move(completion));
        });
    return;
  }
  if (clear_trigger == ClearTrigger::kClearTrigger) {
    subscriber->ClearPollFd();
  }
  SystemControlBlock *scb = subscriber->GetScb();
  const int updates = scb->counters[subscriber->GetChannelId()].num_pub_updates;
  if (subscriber->NumUpdates() == updates) {
    completion(absl::OkStatus(), false);
    return;
  }
  Request request;
  if (!subscriber->IsPlaceholder()) {
    request.mutable_get_triggers()->set_channel_name(subscriber->Name());
    Enqueue(std::move(request), [subscriber = std::move(subscriber), updates,
                                 completion = std::move(completion)](
                                    absl::StatusOr<RpcResult> result) mutable {
      if (!result.ok()) {
        completion(result.status(), false);
        return;
      }
      const auto &response = result->response.get_triggers();
      if (!response.error().empty()) {
        completion(absl::InternalError(response.error()), false);
        return;
      }
      if (absl::Status status =
              CheckFds(result->fds, response.reliable_pub_trigger_fd_indexes(),
                       "reliable publisher trigger");
          !status.ok()) {
        completion(status, false);
        return;
      }
      if (absl::Status status =
              CheckFds(result->fds, response.retirement_fd_indexes(),
                       "retirement trigger");
          !status.ok()) {
        completion(status, false);
        return;
      }
      if (absl::Status status =
              CheckDistinctFds({}, response.reliable_pub_trigger_fd_indexes(),
                               response.retirement_fd_indexes());
          !status.ok()) {
        completion(status, false);
        return;
      }
      subscriber->ClearPublishers();
      for (int index : response.reliable_pub_trigger_fd_indexes()) {
        subscriber->AddPublisher(std::move(result->fds[index]));
      }
      subscriber->ClearRetirementTriggers();
      for (int index : response.retirement_fd_indexes()) {
        subscriber->AddRetirementTrigger(result->fds[index]);
      }
      subscriber->SetNumUpdates(updates);
      completion(absl::OkStatus(), false);
    });
    return;
  }
  FillSubscriberRequest(request.mutable_create_subscriber(),
                        subscriber->RequestName(), subscriber->options_);
  request.mutable_create_subscriber()->set_subscriber_id(
      subscriber->GetSubscriberId());
  Enqueue(std::move(request), [self = shared_from_this(),
                               subscriber = std::move(subscriber), updates,
                               completion = std::move(completion)](
                                  absl::StatusOr<RpcResult> result) mutable {
    if (!result.ok()) {
      completion(result.status(), false);
      return;
    }
    boost::asio::post(boost::asio::system_executor(),
                      [self, subscriber = std::move(subscriber), updates,
                       result = std::move(*result),
                       completion = std::move(completion)]() mutable {
                        self->ApplySubscriberRefresh(std::move(subscriber),
                                                     updates, std::move(result),
                                                     std::move(completion));
                      });
  });
}

void AsioClientState::ApplySubscriberRefresh(
    std::shared_ptr<details::SubscriberImpl> subscriber, int updates,
    RpcResult result, std::function<void(absl::Status, bool)> completion) {
  const auto &response = result.response.create_subscriber();
  if (!response.error().empty()) {
    completion(absl::InternalError(response.error()), false);
    return;
  }
  const bool remap = subscriber->IsPlaceholder();
  if (remap) {
    if (absl::Status status =
            CheckFd(result.fds, response.ccb_fd_index(), "ccb");
        !status.ok()) {
      completion(status, false);
      return;
    }
    if (absl::Status status =
            CheckFd(result.fds, response.bcb_fd_index(), "bcb");
        !status.ok()) {
      completion(status, false);
      return;
    }
  }
  if (absl::Status status =
          CheckFd(result.fds, response.trigger_fd_index(), "trigger");
      !status.ok()) {
    completion(status, false);
    return;
  }
  if (absl::Status status =
          CheckFd(result.fds, response.poll_fd_index(), "poll");
      !status.ok()) {
    completion(status, false);
    return;
  }
  if (absl::Status status =
          CheckFds(result.fds, response.reliable_pub_trigger_fd_indexes(),
                   "reliable publisher trigger");
      !status.ok()) {
    completion(status, false);
    return;
  }
  if (absl::Status status = CheckFds(
          result.fds, response.retirement_fd_indexes(), "retirement trigger");
      !status.ok()) {
    completion(status, false);
    return;
  }
  if (absl::Status status = CheckDistinctFds(
          {response.ccb_fd_index(), response.bcb_fd_index(),
           response.trigger_fd_index(), response.poll_fd_index()},
          response.reliable_pub_trigger_fd_indexes(),
          response.retirement_fd_indexes());
      !status.ok()) {
    completion(status, false);
    return;
  }
  if (remap) {
    subscriber->ResetDeliveryState();
    subscriber->Unmap();
  }
  if (!response.type().empty()) {
    subscriber->SetType(response.type());
  }
  subscriber->options_.use_split_buffers = response.use_split_buffers();
  subscriber->SetNumSlots(response.num_slots());
  subscriber->SetSubscriberQueueSize(response.default_subscriber_queue_size());
  subscriber->SetSubscriberQueueArenaSize(
      response.subscriber_queue_arena_size());
  subscriber->SetEffectiveSubscriberQueueSize(response.subscriber_queue_size());
  subscriber->SetChecksumSize(
      response.checksum_size() > 0 ? response.checksum_size() : 4);
  subscriber->SetMetadataSize(std::max(0, response.metadata_size()));
  subscriber->SetPrefixSize(Channel::ComputePrefixSize(
      subscriber->ChecksumSize(), subscriber->MetadataSize()));
  subscriber->AllocateChecksumBuffer();
  if (remap) {
    SharedMemoryFds fds(std::move(result.fds[response.ccb_fd_index()]),
                        std::move(result.fds[response.bcb_fd_index()]));
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
  subscriber->SetTriggerFd(std::move(result.fds[response.trigger_fd_index()]));
  subscriber->SetPollFd(std::move(result.fds[response.poll_fd_index()]));
  subscriber->ClearPublishers();
  for (int index : response.reliable_pub_trigger_fd_indexes()) {
    subscriber->AddPublisher(std::move(result.fds[index]));
  }
  subscriber->ClearRetirementTriggers();
  for (int index : response.retirement_fd_indexes()) {
    subscriber->AddRetirementTrigger(result.fds[index]);
  }
  subscriber->SetNumUpdates(updates);
  completion(absl::OkStatus(), true);
}

absl::StatusOr<Message>
AsioClientState::ReadMessage(details::SubscriberImpl *subscriber, ReadMode mode,
                             ClearTrigger clear_trigger) {
  if (clear_trigger == ClearTrigger::kClearTrigger) {
    subscriber->ClearPollFd();
  }
  if (subscriber->IsPlaceholder()) {
    return Message();
  }
  int64_t last_ordinal = -1;
  if (MessageSlot *current = subscriber->CurrentSlot(); current != nullptr) {
    last_ordinal = current->ordinal.load(std::memory_order_relaxed);
  }
  MessageSlot *slot = mode == ReadMode::kReadNext ? subscriber->NextSlot()
                                                  : subscriber->LastSlot();
  if (slot == nullptr) {
    subscriber->NoteUnstuck();
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
    subscriber->NoteStuckRead(toolbelt::Now(),
                              details::kMaxStuckSubscriberWarnings);
    return Message();
  }
  subscriber->NoteUnstuck();
  if (mode == ReadMode::kReadNext &&
      subscriber->options_.DetectDroppedMessages()) {
    int drops = subscriber->ConsumeQueueDrops();
    if (last_ordinal != -1 && subscriber->options_.SubscriberQueueSize() == 0) {
      drops = std::max(drops, subscriber->DetectDrops(slot->vchan_id.load(
                                  std::memory_order_relaxed)));
    }
    if (drops > 0) {
      subscriber->RecordDroppedMessages(drops);
    }
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

void AsioClientState::RemovePublisher(
    std::shared_ptr<details::PublisherImpl> publisher) {
  Request request;
  auto *remove = request.mutable_remove_publisher();
  remove->set_channel_name(publisher->Name());
  remove->set_publisher_id(publisher->GetPublisherId());
  Enqueue(std::move(request), [publisher = std::move(publisher)](auto) mutable {
    publisher.reset();
  });
}

void AsioClientState::RemoveSubscriber(
    std::shared_ptr<details::SubscriberImpl> subscriber) {
  Request request;
  auto *remove = request.mutable_remove_subscriber();
  remove->set_channel_name(subscriber->RequestName());
  remove->set_subscriber_id(subscriber->GetSubscriberId());
  remove->set_telemetry(subscriber->options_.Telemetry());
  Enqueue(std::move(request), [subscriber = std::move(subscriber)](
                                  auto) mutable { subscriber.reset(); });
}

absl::StatusOr<AsioClientState::RpcResult> AsioClientState::WaitForControl(
    Request request, std::vector<toolbelt::FileDescriptor> outbound_fds,
    bool expects_response) {
  auto promise = std::make_shared<std::promise<absl::StatusOr<RpcResult>>>();
  std::future<absl::StatusOr<RpcResult>> future = promise->get_future();
  Enqueue(
      std::move(request),
      [promise](absl::StatusOr<RpcResult> result) mutable {
        promise->set_value(std::move(result));
      },
      std::move(outbound_fds), expects_response);
  return future.get();
}

absl::Status
AsioClientState::RegisterClientBuffer(
    const ClientBufferHandleMetadata &metadata,
    const toolbelt::FileDescriptor *fd) {
  Request request;
  auto *register_buffer = request.mutable_register_client_buffer();
  details::ToProto(metadata, register_buffer->mutable_metadata());
  std::vector<toolbelt::FileDescriptor> fds;
  if (fd != nullptr && fd->Valid()) {
    register_buffer->set_has_fd(true);
    register_buffer->set_fd_index(0);
    fds.push_back(*fd);
  }
  absl::StatusOr<RpcResult> result =
      WaitForControl(std::move(request), std::move(fds));
  if (!result.ok()) {
    return result.status();
  }
  const auto &response = result->response.register_client_buffer();
  return response.error().empty() ? absl::OkStatus()
                                  : absl::InternalError(response.error());
}

absl::StatusOr<std::vector<RegisteredClientBuffer>>
AsioClientState::GetClientBuffers(const std::string &channel_name,
                                  uint64_t session_id, uint32_t buffer_index) {
  Request request;
  auto *get = request.mutable_get_client_buffers();
  get->set_channel_name(channel_name);
  get->set_session_id(session_id);
  get->set_buffer_index(buffer_index);
  absl::StatusOr<RpcResult> result = WaitForControl(std::move(request));
  if (!result.ok()) {
    return result.status();
  }
  const auto &response = result->response.get_client_buffers();
  if (!response.error().empty()) {
    return absl::InternalError(response.error());
  }
  if (response.metadata_size() != response.fd_indexes_size()) {
    return absl::DataLossError("malformed client-buffer response");
  }
  for (int fd_index : response.fd_indexes()) {
    if (fd_index >= 0) {
      if (absl::Status status = CheckFd(result->fds, fd_index, "client buffer");
          !status.ok()) {
        return status;
      }
    }
  }
  if (absl::Status status = CheckDistinctFds({}, response.fd_indexes());
      !status.ok()) {
    return status;
  }
  std::vector<RegisteredClientBuffer> buffers;
  buffers.reserve(response.metadata_size());
  for (int index = 0; index < response.metadata_size(); ++index) {
    RegisteredClientBuffer buffer{
        .metadata = details::FromProto(response.metadata(index)),
    };
    const int fd_index = response.fd_indexes(index);
    if (fd_index >= 0) {
      buffer.fd = std::move(result->fds[fd_index]);
    }
    buffers.push_back(std::move(buffer));
  }
  return buffers;
}

absl::Status AsioClientState::UnregisterClientBuffer(
    const std::string &channel_name, uint64_t session_id,
    uint32_t buffer_index) {
  Request request;
  auto *unregister = request.mutable_unregister_client_buffer();
  unregister->set_channel_name(channel_name);
  unregister->set_session_id(session_id);
  unregister->set_buffer_index(buffer_index);
  absl::StatusOr<RpcResult> result =
      WaitForControl(std::move(request), {}, false);
  return result.status();
}

AsioPublisher::AsioPublisher(
    std::shared_ptr<AsioClientState> client,
    std::shared_ptr<details::PublisherImpl> impl,
    std::shared_ptr<boost::asio::posix::stream_descriptor> poll_descriptor)
    : client_(std::move(client)), impl_(std::move(impl)),
      poll_descriptor_(std::move(poll_descriptor)) {}

AsioPublisher &AsioPublisher::operator=(AsioPublisher &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (impl_ != nullptr) {
    client_->RemovePublisher(std::move(impl_));
  }
  client_ = std::move(other.client_);
  impl_ = std::move(other.impl_);
  poll_descriptor_ = std::move(other.poll_descriptor_);
  return *this;
}

AsioPublisher::~AsioPublisher() {
  if (impl_ != nullptr) {
    client_->RemovePublisher(std::move(impl_));
  }
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

AsioSubscriber::AsioSubscriber(
    std::shared_ptr<AsioClientState> client,
    std::shared_ptr<details::SubscriberImpl> impl,
    std::shared_ptr<boost::asio::posix::stream_descriptor> event_fd)
    : client_(std::move(client)), impl_(std::move(impl)),
      strand_(client_->GetExecutor()), event_fd_(std::move(event_fd)) {}

AsioSubscriber::~AsioSubscriber() {
  Shutdown();
  if (impl_ != nullptr) {
    client_->RemoveSubscriber(std::move(impl_));
  }
}

void AsioSubscriber::Start(std::function<void(Message &&)> on_message) {
  if (running_.exchange(true)) {
    return;
  }
  auto self = shared_from_this();
  boost::asio::post(strand_,
                    [self, on_message = std::move(on_message)]() mutable {
                      if (!self->running_) {
                        return;
                      }
                      self->on_message_ = std::move(on_message);
                      self->Drain();
                    });
}

void AsioSubscriber::Shutdown() {
  running_ = false;
  auto event_fd = event_fd_;
  boost::asio::post(strand_, [event_fd = std::move(event_fd)] {
    boost::system::error_code ignored;
    event_fd->cancel(ignored);
  });
}

absl::Status AsioSubscriber::ResetEventFd() {
  const int fd = ::dup(impl_->GetPollFd().Fd());
  if (fd < 0) {
    return absl::InternalError(
        absl::StrFormat("dup subscriber poll fd: %s", std::strerror(errno)));
  }
  boost::system::error_code ignored;
  event_fd_->cancel(ignored);
  event_fd_->close(ignored);
  event_fd_->assign(fd, ignored);
  if (ignored) {
    ::close(fd);
    return IoError("assign subscriber poll fd", ignored);
  }
  return absl::OkStatus();
}

void AsioSubscriber::WaitForNotification() {
  std::weak_ptr<AsioSubscriber> weak = weak_from_this();
  event_fd_->async_wait(
      boost::asio::posix::stream_descriptor::wait_read,
      boost::asio::bind_executor(
          strand_, [weak](const boost::system::error_code &error) {
            if (auto self = weak.lock();
                self != nullptr && !error && self->running_) {
              self->Drain();
            }
          }));
}

void AsioSubscriber::Drain() {
  if (!running_) {
    return;
  }
  AsyncReadMessage(
      ReadMode::kReadNext,
      boost::asio::bind_executor(
          strand_, [self = shared_from_this()](absl::StatusOr<Message> result) {
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
          }));
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
