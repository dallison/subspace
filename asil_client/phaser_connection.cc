// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asil_client/phaser_connection.h"

#include "proto/subspace.phaser.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <string_view>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace subspace {
namespace asil {

namespace wire = ::subspace::phaser;

namespace {

#if defined(MSG_NOSIGNAL)
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

// The server's limit on descriptors in one SCM_RIGHTS message.
constexpr size_t kMaxFdsPerMessage = 252;

std::string_view View(const char *s) {
  return s == nullptr ? std::string_view() : std::string_view(s);
}

bool SendFully(int fd, const char *data, size_t length) {
  while (length > 0) {
    const ssize_t n = ::send(fd, data, length, kSendFlags);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    data += n;
    length -= static_cast<size_t>(n);
  }
  return true;
}

bool ReceiveFully(int fd, char *data, size_t length) {
  while (length > 0) {
    const ssize_t n = ::recv(fd, data, length, 0);
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n <= 0) {
      return false;
    }
    data += n;
    length -= static_cast<size_t>(n);
  }
  return true;
}

// Duplicates the descriptor at index in fds.  Indexes can repeat, so each
// use gets its own descriptor.
Error DupFd(const PhaserServerConnection::ReceivedFds &fds, int32_t index,
            UniqueFd &out) {
  if (index < 0 || index >= fds.Size()) {
    return Error::kProtocolError;
  }
  const int fd = ::fcntl(fds.Get(index), F_DUPFD_CLOEXEC, 0);
  if (fd < 0) {
    return Error::kProtocolError;
  }
  out.Reset(fd);
  return Error::kOk;
}

// index(i) returns the descriptor index of the i'th of count descriptors.
template <typename Index>
Error FillFdList(const PhaserServerConnection::ReceivedFds &fds, size_t count,
                 Index index, TriggerFdList *list) {
  if (list == nullptr) {
    return Error::kOk;
  }
  list->Clear();
  for (size_t i = 0; i < count; i++) {
    UniqueFd fd;
    if (Error e = DupFd(fds, index(i), fd); e != Error::kOk) {
      return e;
    }
    if (!list->Add(static_cast<UniqueFd &&>(fd))) {
      return Error::kCapacityExceeded;
    }
  }
  return Error::kOk;
}

} // namespace

Error PhaserServerConnection::Connect(const char *socket_name) {
  if (socket_name == nullptr) {
    return Error::kInvalidArgument;
  }
  if (socket_.Valid()) {
    return Error::kAlreadyInitialized;
  }
  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  const size_t length = std::strlen(socket_name);
#if defined(__linux__)
  // The server binds in the abstract namespace on Linux.
  if (length > sizeof(addr.sun_path) - 2) {
    return Error::kCapacityExceeded;
  }
  std::memcpy(addr.sun_path + 1, socket_name, length);
#else
  if (length > sizeof(addr.sun_path) - 1) {
    return Error::kCapacityExceeded;
  }
  std::memcpy(addr.sun_path, socket_name, length);
#endif
  UniqueFd fd(::socket(AF_UNIX, SOCK_STREAM, 0));
  if (!fd.Valid()) {
    return Error::kConnectionFailed;
  }
#if defined(SO_NOSIGPIPE)
  const int on = 1;
  (void)::setsockopt(fd.Get(), SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
  if (::connect(fd.Get(), reinterpret_cast<const struct sockaddr *>(&addr),
                sizeof(addr)) != 0) {
    return Error::kConnectionFailed;
  }
  socket_ = static_cast<UniqueFd &&>(fd);
  return Error::kOk;
}

Error PhaserServerConnection::Rejected(const char *message, size_t length) {
  if (length > sizeof(last_error_) - 1) {
    length = sizeof(last_error_) - 1;
  }
  std::memcpy(last_error_, message, length);
  last_error_[length] = '\0';
  return Error::kServerRejected;
}

// Sends the length-prefixed request in wire_.
Error PhaserServerConnection::Send(size_t length) {
  const uint32_t network_length = htonl(static_cast<uint32_t>(length));
  std::memcpy(wire_, &network_length, sizeof(network_length));
  if (!SendFully(socket_.Get(), wire_, sizeof(network_length) + length)) {
    return Error::kConnectionFailed;
  }
  return Error::kOk;
}

// Sends one descriptor the way the server reads them: the descriptor count as
// an int32 with the descriptor as SCM_RIGHTS.
Error PhaserServerConnection::SendFd(int fd) {
  alignas(struct cmsghdr) char control[CMSG_SPACE(sizeof(int))];
  std::memset(control, 0, sizeof(control));
  int32_t count = 1;
  struct iovec iov;
  iov.iov_base = &count;
  iov.iov_len = sizeof(count);
  struct msghdr msg;
  std::memset(&msg, 0, sizeof(msg));
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = control;
  msg.msg_controllen = static_cast<socklen_t>(sizeof(control));
  struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  std::memcpy(CMSG_DATA(cmsg), &fd, sizeof(int));
  for (;;) {
    const ssize_t n = ::sendmsg(socket_.Get(), &msg, kSendFlags);
    if (n == static_cast<ssize_t>(sizeof(count))) {
      return Error::kOk;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    return Error::kConnectionFailed;
  }
}

// Receives a length-prefixed response into wire_.  A response that does not
// fit is read and discarded so that the stream stays in step.
Error PhaserServerConnection::Receive(size_t &length) {
  uint32_t network_length = 0;
  if (!ReceiveFully(socket_.Get(), reinterpret_cast<char *>(&network_length),
                    sizeof(network_length))) {
    return Error::kConnectionFailed;
  }
  length = ntohl(network_length);
  char *const data = wire_ + sizeof(uint32_t);
  if (length <= kWireBufferSize) {
    return ReceiveFully(socket_.Get(), data, length) ? Error::kOk
                                                     : Error::kConnectionFailed;
  }
  for (size_t remaining = length; remaining > 0;) {
    const size_t chunk =
        remaining < kWireBufferSize ? remaining : kWireBufferSize;
    if (!ReceiveFully(socket_.Get(), data, chunk)) {
      return Error::kConnectionFailed;
    }
    remaining -= chunk;
  }
  return Error::kCapacityExceeded;
}

// The server sends one or more messages, each holding the total descriptor
// count as an int32 and some of the descriptors as SCM_RIGHTS.  Descriptors
// beyond kMaxReceivedFds are closed and reported as kCapacityExceeded once
// all of them have been read.
Error PhaserServerConnection::ReceiveFds() {
  alignas(
      struct cmsghdr) char control[CMSG_SPACE(kMaxFdsPerMessage * sizeof(int))];
  int32_t received = 0;
  bool overflow = false;
  for (;;) {
    int32_t total = 0;
    size_t total_bytes = 0;
    bool saw_rights = false;
    int32_t in_message = 0;
    while (total_bytes < sizeof(total)) {
      std::memset(control, 0, sizeof(control));
      struct iovec iov;
      iov.iov_base = reinterpret_cast<char *>(&total) + total_bytes;
      iov.iov_len = sizeof(total) - total_bytes;
      struct msghdr msg;
      std::memset(&msg, 0, sizeof(msg));
      msg.msg_iov = &iov;
      msg.msg_iovlen = 1;
      msg.msg_control = control;
      msg.msg_controllen = static_cast<socklen_t>(sizeof(control));
#if defined(MSG_CMSG_CLOEXEC)
      const ssize_t n = ::recvmsg(socket_.Get(), &msg, MSG_CMSG_CLOEXEC);
#else
      const ssize_t n = ::recvmsg(socket_.Get(), &msg, 0);
#endif
      if (n < 0 && errno == EINTR) {
        continue;
      }
      if (n <= 0) {
        return Error::kConnectionFailed;
      }
      total_bytes += static_cast<size_t>(n);
      if ((msg.msg_flags & MSG_CTRUNC) != 0) {
        return Error::kProtocolError;
      }
      for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr;
           cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
          continue;
        }
        saw_rights = true;
        const size_t data_length = cmsg->cmsg_len - CMSG_LEN(0);
        const int count = static_cast<int>(data_length / sizeof(int));
        const unsigned char *data = CMSG_DATA(cmsg);
        for (int i = 0; i < count; i++) {
          int fd = -1;
          std::memcpy(&fd, data + static_cast<size_t>(i) * sizeof(int),
                      sizeof(int));
          if (!fds_.Add(UniqueFd(fd))) {
            overflow = true;
          }
        }
        in_message += count;
      }
    }
    if (total == 0) {
      return Error::kOk;
    }
    if (!saw_rights) {
      return Error::kProtocolError;
    }
    received += in_message;
    if (received >= total) {
      return overflow ? Error::kCapacityExceeded : Error::kOk;
    }
  }
}

// build(request) fills in the request and read(response) handles the reply.
// A send_fd of 0 or more goes to the server after the request.
template <typename Build, typename Read>
Error PhaserServerConnection::Transact(Build build, Read read, int send_fd) {
  if (!socket_.Valid()) {
    return Error::kNotInitialized;
  }
  fds_.Clear();
  size_t length = 0;
  {
    absl::StatusOr<wire::Request> request =
        wire::Request::TryCreateMutable(message_, sizeof(message_));
    if (!request.ok()) {
      return Error::kCapacityExceeded;
    }
    build(*request);
    if (request->AllocationFailed()) {
      return Error::kCapacityExceeded;
    }
    ::phaser::ProtoBuffer out(wire_ + sizeof(uint32_t), kWireBufferSize);
    if (!request->Serialize(out).ok()) {
      return Error::kCapacityExceeded;
    }
    length = out.Size();
  }

  Error e = Send(length);
  if (e == Error::kOk && send_fd >= 0) {
    e = SendFd(send_fd);
  }
  if (e == Error::kOk) {
    e = Receive(length);
  }
  if (e == Error::kOk || e == Error::kCapacityExceeded) {
    // The descriptors follow even a response that did not fit.
    const Error fds_error = ReceiveFds();
    if (fds_error != Error::kOk &&
        (e == Error::kOk || fds_error != Error::kCapacityExceeded)) {
      e = fds_error;
    }
  }
  if (e == Error::kConnectionFailed || e == Error::kProtocolError) {
    // The stream position is unknown after a failure.
    socket_.Reset();
  }
  if (e != Error::kOk) {
    fds_.Clear();
    return e;
  }

  absl::StatusOr<wire::Response> response =
      wire::Response::TryCreateMutable(message_, sizeof(message_));
  if (!response.ok()) {
    fds_.Clear();
    return Error::kCapacityExceeded;
  }
  ::phaser::ProtoBuffer in(static_cast<const char *>(wire_ + sizeof(uint32_t)),
                           length);
  if (absl::Status status = response->Deserialize(in); !status.ok()) {
    fds_.Clear();
    return status.code() == absl::StatusCode::kResourceExhausted
               ? Error::kCapacityExceeded
               : Error::kProtocolError;
  }
  e = read(static_cast<const wire::Response &>(*response));
  fds_.Clear();
  return e;
}

Error PhaserServerConnection::Init(const char *client_name, InitReply &reply) {
  return Transact(
      [&](wire::Request &request) {
        request.mutable_init()->set_client_name(View(client_name));
      },
      [&](const wire::Response &response) {
        if (!response.has_init()) {
          return Error::kProtocolError;
        }
        const wire::InitResponse &init = response.init();
        if (Error e = DupFd(fds_, init.scb_fd_index(), reply.scb);
            e != Error::kOk) {
          return e;
        }
        reply.session_id = static_cast<uint64_t>(init.session_id());
        reply.user_id = init.user_id();
        reply.group_id = init.group_id();
        return Error::kOk;
      });
}

Error PhaserServerConnection::CreatePublisher(const PublisherRequest &request,
                                              PublisherReply &reply) {
  return Transact(
      [&](wire::Request &req) {
        wire::CreatePublisherRequest *cmd = req.mutable_create_publisher();
        cmd->set_channel_name(View(request.channel_name));
        cmd->set_slot_size(request.slot_size);
        cmd->set_num_slots(request.num_slots);
        cmd->set_is_fixed_size(true);
        cmd->set_is_reliable(request.reliable);
        if (request.type != nullptr) {
          cmd->set_type(View(request.type));
        }
        if (request.mux != nullptr) {
          cmd->set_mux(View(request.mux));
        }
        cmd->set_vchan_id(request.vchan_id);
        cmd->set_subscriber_queue_arena_size(
            request.subscriber_queue_arena_size);
        cmd->set_checksum_size(request.checksum_size);
        cmd->set_metadata_size(request.metadata_size);
        cmd->set_use_split_buffers(request.use_split_buffers);
        cmd->set_publisher_id(-1);
        cmd->set_process_id(static_cast<uint64_t>(::getpid()));
        cmd->set_max_outstanding_slot_leases(1);
      },
      [&](const wire::Response &response) {
        if (!response.has_create_publisher()) {
          return Error::kProtocolError;
        }
        const wire::CreatePublisherResponse &pub = response.create_publisher();
        if (!pub.error().empty()) {
          return Rejected(pub.error().data(), pub.error().size());
        }
        reply.channel_id = pub.channel_id();
        reply.publisher_id = pub.publisher_id();
        reply.num_slots = pub.num_slots();
        reply.vchan_id = pub.vchan_id();
        reply.num_sub_updates = pub.num_sub_updates();
        reply.subscriber_queue_arena_size = pub.subscriber_queue_arena_size();
        if (Error e = DupFd(fds_, pub.ccb_fd_index(), reply.ccb);
            e != Error::kOk) {
          return e;
        }
        if (Error e = DupFd(fds_, pub.bcb_fd_index(), reply.bcb);
            e != Error::kOk) {
          return e;
        }
        if (Error e = DupFd(fds_, pub.pub_poll_fd_index(), reply.poll);
            e != Error::kOk) {
          return e;
        }
        if (Error e = FillFdList(
                fds_, pub.sub_trigger_fd_indexes_size(),
                [&](size_t i) { return pub.sub_trigger_fd_indexes(i); },
                reply.subscriber_triggers);
            e != Error::kOk) {
          return e;
        }
        return FillFdList(
            fds_, pub.retirement_fd_indexes_size(),
            [&](size_t i) { return pub.retirement_fd_indexes(i); },
            reply.retirement_triggers);
      });
}

Error PhaserServerConnection::CreateSubscriber(const SubscriberRequest &request,
                                               SubscriberReply &reply) {
  return Transact(
      [&](wire::Request &req) {
        wire::CreateSubscriberRequest *cmd = req.mutable_create_subscriber();
        cmd->set_channel_name(View(request.channel_name));
        cmd->set_subscriber_id(-1);
        cmd->set_is_reliable(request.reliable);
        if (request.type != nullptr) {
          cmd->set_type(View(request.type));
        }
        cmd->set_max_active_messages(request.max_active_messages);
        if (request.mux != nullptr) {
          cmd->set_mux(View(request.mux));
        }
        cmd->set_vchan_id(request.vchan_id);
        cmd->set_process_id(static_cast<uint64_t>(::getpid()));
      },
      [&](const wire::Response &response) {
        if (!response.has_create_subscriber()) {
          return Error::kProtocolError;
        }
        const wire::CreateSubscriberResponse &sub =
            response.create_subscriber();
        if (!sub.error().empty()) {
          return Rejected(sub.error().data(), sub.error().size());
        }
        reply.channel_id = sub.channel_id();
        reply.subscriber_id = sub.subscriber_id();
        reply.num_slots = sub.num_slots();
        reply.vchan_id = sub.vchan_id();
        reply.num_pub_updates = sub.num_pub_updates();
        reply.checksum_size = sub.checksum_size();
        reply.metadata_size = sub.metadata_size();
        reply.subscriber_queue_size = sub.subscriber_queue_size();
        reply.subscriber_queue_arena_size = sub.subscriber_queue_arena_size();
        reply.use_split_buffers = sub.use_split_buffers();
        if (Error e = DupFd(fds_, sub.ccb_fd_index(), reply.ccb);
            e != Error::kOk) {
          return e;
        }
        if (Error e = DupFd(fds_, sub.bcb_fd_index(), reply.bcb);
            e != Error::kOk) {
          return e;
        }
        if (Error e = DupFd(fds_, sub.trigger_fd_index(), reply.trigger);
            e != Error::kOk) {
          return e;
        }
        if (Error e = DupFd(fds_, sub.poll_fd_index(), reply.poll);
            e != Error::kOk) {
          return e;
        }
        if (Error e = FillFdList(
                fds_, sub.reliable_pub_trigger_fd_indexes_size(),
                [&](size_t i) {
                  return sub.reliable_pub_trigger_fd_indexes(i);
                },
                reply.reliable_publisher_triggers);
            e != Error::kOk) {
          return e;
        }
        return FillFdList(
            fds_, sub.retirement_fd_indexes_size(),
            [&](size_t i) { return sub.retirement_fd_indexes(i); },
            reply.retirement_triggers);
      });
}

Error PhaserServerConnection::GetTriggers(const char *channel_name,
                                          TriggersReply &reply) {
  return Transact(
      [&](wire::Request &req) {
        req.mutable_get_triggers()->set_channel_name(View(channel_name));
      },
      [&](const wire::Response &response) {
        if (!response.has_get_triggers()) {
          return Error::kProtocolError;
        }
        const wire::GetTriggersResponse &triggers = response.get_triggers();
        if (!triggers.error().empty()) {
          return Rejected(triggers.error().data(), triggers.error().size());
        }
        if (Error e = FillFdList(
                fds_, triggers.reliable_pub_trigger_fd_indexes_size(),
                [&](size_t i) {
                  return triggers.reliable_pub_trigger_fd_indexes(i);
                },
                reply.reliable_publisher_triggers);
            e != Error::kOk) {
          return e;
        }
        if (Error e = FillFdList(
                fds_, triggers.sub_trigger_fd_indexes_size(),
                [&](size_t i) { return triggers.sub_trigger_fd_indexes(i); },
                reply.subscriber_triggers);
            e != Error::kOk) {
          return e;
        }
        return FillFdList(
            fds_, triggers.retirement_fd_indexes_size(),
            [&](size_t i) { return triggers.retirement_fd_indexes(i); },
            reply.retirement_triggers);
      });
}

Error PhaserServerConnection::RemovePublisher(const char *channel_name,
                                              int32_t publisher_id) {
  return Transact(
      [&](wire::Request &req) {
        wire::RemovePublisherRequest *cmd = req.mutable_remove_publisher();
        cmd->set_channel_name(View(channel_name));
        cmd->set_publisher_id(publisher_id);
      },
      [&](const wire::Response &response) {
        const std::string_view error = response.remove_publisher().error();
        if (!error.empty()) {
          return Rejected(error.data(), error.size());
        }
        return Error::kOk;
      });
}

Error PhaserServerConnection::RemoveSubscriber(const char *channel_name,
                                               int32_t subscriber_id) {
  return Transact(
      [&](wire::Request &req) {
        wire::RemoveSubscriberRequest *cmd = req.mutable_remove_subscriber();
        cmd->set_channel_name(View(channel_name));
        cmd->set_subscriber_id(subscriber_id);
      },
      [&](const wire::Response &response) {
        const std::string_view error = response.remove_subscriber().error();
        if (!error.empty()) {
          return Rejected(error.data(), error.size());
        }
        return Error::kOk;
      });
}

Error PhaserServerConnection::RegisterBuffer(const ClientBuffer &buffer,
                                             int fd) {
  if (fd < 0) {
    return Error::kInvalidArgument;
  }
  return Transact(
      [&](wire::Request &req) {
        wire::RegisterClientBufferRequest *cmd =
            req.mutable_register_client_buffer();
        wire::ClientBufferHandleMetadataProto *metadata =
            cmd->mutable_metadata();
        metadata->set_channel_name(View(buffer.channel_name));
        metadata->set_session_id(buffer.session_id);
        metadata->set_buffer_index(buffer.buffer_index);
        metadata->set_slot_id(0);
        metadata->set_is_prefix(false);
        metadata->set_full_size(buffer.full_size);
        metadata->set_allocation_size(buffer.full_size);
        metadata->set_handle(static_cast<uint64_t>(fd));
        metadata->set_allocator(wire::CLIENT_BUFFER_ALLOCATOR_ANDROID_MEMFD);
        cmd->set_has_fd(true);
        cmd->set_fd_index(0);
      },
      [&](const wire::Response &response) {
        const std::string_view error =
            response.register_client_buffer().error();
        if (!error.empty()) {
          return Rejected(error.data(), error.size());
        }
        return Error::kOk;
      },
      fd);
}

Error PhaserServerConnection::GetBuffer(const BufferRequest &request,
                                        BufferReply &reply) {
  reply = BufferReply();
  return Transact(
      [&](wire::Request &req) {
        wire::GetClientBuffersRequest *cmd = req.mutable_get_client_buffers();
        cmd->set_channel_name(View(request.channel_name));
        cmd->set_session_id(request.session_id);
        cmd->set_buffer_index(request.buffer_index);
        cmd->set_filter_slot(true);
        cmd->set_is_prefix(request.is_prefix);
        cmd->set_slot_id(request.slot_id);
      },
      [&](const wire::Response &response) {
        if (!response.has_get_client_buffers()) {
          return Error::kProtocolError;
        }
        const wire::GetClientBuffersResponse &buffers =
            response.get_client_buffers();
        if (!buffers.error().empty()) {
          return Rejected(buffers.error().data(), buffers.error().size());
        }
        if (buffers.metadata_size() != buffers.fd_indexes_size()) {
          return Error::kProtocolError;
        }
        for (size_t i = 0; i < buffers.metadata_size(); i++) {
          const wire::ClientBufferHandleMetadataProto metadata =
              buffers.metadata(i);
          if (metadata.is_prefix() != request.is_prefix ||
              metadata.slot_id() != request.slot_id) {
            continue;
          }
          if (buffers.fd_indexes(i) >= 0) {
            if (Error e = DupFd(fds_, buffers.fd_indexes(i), reply.fd);
                e != Error::kOk) {
              return e;
            }
          }
          reply.found = true;
          reply.full_size = metadata.full_size();
          reply.allocation_size = metadata.allocation_size();
          reply.handle = metadata.handle();
          reply.map_offset = metadata.map_offset();
          reply.allocator = static_cast<BufferAllocator>(metadata.allocator());
          return Error::kOk;
        }
        return Error::kOk;
      });
}

} // namespace asil
} // namespace subspace
