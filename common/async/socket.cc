// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/socket.h"

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include "absl/strings/str_format.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace subspace::async {

namespace {

boost::asio::io_context &IocFromYield(Context yield) {
  return static_cast<boost::asio::io_context &>(
      yield.get_executor().context());
}

// Wait until `fd` is readable, yielding the coroutine.  Operates on a dup so it
// never closes the caller's fd.
absl::Status WaitFdReadable(Context yield, int fd) {
  int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    return absl::InternalError("dup failed");
  }
  boost::asio::posix::stream_descriptor sd(IocFromYield(yield), dup_fd);
  boost::system::error_code ec;
  sd.async_wait(boost::asio::posix::stream_descriptor::wait_read, yield[ec]);
  if (ec) {
    return absl::InternalError(
        absl::StrFormat("wait_read error: %s", ec.message()));
  }
  return absl::OkStatus();
}

absl::Status WaitFdWritable(Context yield, int fd) {
  int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    return absl::InternalError("dup failed");
  }
  boost::asio::posix::stream_descriptor sd(IocFromYield(yield), dup_fd);
  boost::system::error_code ec;
  sd.async_wait(boost::asio::posix::stream_descriptor::wait_write, yield[ec]);
  if (ec) {
    return absl::InternalError(
        absl::StrFormat("wait_write error: %s", ec.message()));
  }
  return absl::OkStatus();
}

// Fill a sockaddr_storage from a SocketAddress.  Returns the length.
absl::StatusOr<socklen_t> AddrToSockaddr(const SocketAddress &addr,
                                         sockaddr_storage *out, int *family) {
  std::memset(out, 0, sizeof(*out));
  switch (addr.Type()) {
  case SocketAddress::kAddressInet: {
    const sockaddr_in &sa = addr.GetInetAddress().GetAddress();
    std::memcpy(out, &sa, sizeof(sa));
    *family = AF_INET;
    return static_cast<socklen_t>(sizeof(sa));
  }
  case SocketAddress::kAddressVirtual: {
    const sockaddr_vm &sa = addr.GetVirtualAddress().GetAddress();
    std::memcpy(out, &sa, sizeof(sa));
    *family = AF_VSOCK;
    return static_cast<socklen_t>(sizeof(sa));
  }
  default:
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported address type for asio stream socket: %s",
        addr.ToString()));
  }
}

void SetFdNonBlocking(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags >= 0) {
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  }
}

SocketAddress SockaddrToAddr(const sockaddr_storage &ss) {
  if (ss.ss_family == AF_VSOCK) {
    return SocketAddress(*reinterpret_cast<const sockaddr_vm *>(&ss));
  }
  return SocketAddress(*reinterpret_cast<const sockaddr_in *>(&ss));
}

}  // namespace

struct StreamSocket::Impl {
  int fd = -1;
  bool nonblocking = false;
  ~Impl() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
};

absl::StatusOr<StreamSocket> MakeConnectedStreamSocket(
    std::shared_ptr<StreamSocket::Impl> impl) {
  StreamSocket s;
  s.impl_ = std::move(impl);
  return s;
}

absl::Status StreamSocket::Bind(const SocketAddress &addr, bool listen) {
  sockaddr_storage ss;
  int family = 0;
  absl::StatusOr<socklen_t> len = AddrToSockaddr(addr, &ss, &family);
  if (!len.ok()) {
    return len.status();
  }
  int fd = ::socket(family, SOCK_STREAM, 0);
  if (fd < 0) {
    return absl::InternalError(
        absl::StrFormat("socket() failed: %s", strerror(errno)));
  }
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (::bind(fd, reinterpret_cast<sockaddr *>(&ss), *len) < 0) {
    int e = errno;
    ::close(fd);
    return absl::InternalError(absl::StrFormat("bind() failed: %s", strerror(e)));
  }
  if (listen && ::listen(fd, SOMAXCONN) < 0) {
    int e = errno;
    ::close(fd);
    return absl::InternalError(
        absl::StrFormat("listen() failed: %s", strerror(e)));
  }
  // Record the actual bound address (e.g. for ephemeral ports).
  sockaddr_storage bound;
  socklen_t blen = sizeof(bound);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &blen) == 0) {
    bound_address_ = SockaddrToAddr(bound);
  }
  SetFdNonBlocking(fd);
  impl_ = std::make_shared<Impl>();
  impl_->fd = fd;
  impl_->nonblocking = true;
  return absl::OkStatus();
}

absl::Status StreamSocket::Connect(const SocketAddress &addr) {
  sockaddr_storage ss;
  int family = 0;
  absl::StatusOr<socklen_t> len = AddrToSockaddr(addr, &ss, &family);
  if (!len.ok()) {
    return len.status();
  }
  int fd = ::socket(family, SOCK_STREAM, 0);
  if (fd < 0) {
    return absl::InternalError(
        absl::StrFormat("socket() failed: %s", strerror(errno)));
  }
  if (::connect(fd, reinterpret_cast<sockaddr *>(&ss), *len) < 0) {
    int e = errno;
    ::close(fd);
    return absl::InternalError(
        absl::StrFormat("connect() failed: %s", strerror(e)));
  }
  // The server's network paths run all I/O through the coroutine, so make the
  // socket non-blocking up front (matches the explicit SetNonBlocking() the
  // bridge does, and keeps the single-threaded io_context responsive).
  SetFdNonBlocking(fd);
  impl_ = std::make_shared<Impl>();
  impl_->fd = fd;
  impl_->nonblocking = true;
  return absl::OkStatus();
}

absl::StatusOr<StreamSocket> StreamSocket::Accept(Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Accept on unbound socket");
  }
  for (;;) {
    sockaddr_storage ss;
    socklen_t slen = sizeof(ss);
    int newfd = ::accept(impl_->fd, reinterpret_cast<sockaddr *>(&ss), &slen);
    if (newfd >= 0) {
      SetFdNonBlocking(newfd);
      auto new_impl = std::make_shared<Impl>();
      new_impl->fd = newfd;
      new_impl->nonblocking = true;
      return MakeConnectedStreamSocket(std::move(new_impl));
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      if (absl::Status s = WaitFdReadable(ctx, impl_->fd); !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("accept() failed: %s", strerror(errno)));
  }
}

absl::Status StreamSocket::SetNonBlocking() {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("SetNonBlocking on closed socket");
  }
  int flags = ::fcntl(impl_->fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(impl_->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return absl::InternalError(
        absl::StrFormat("fcntl O_NONBLOCK failed: %s", strerror(errno)));
  }
  impl_->nonblocking = true;
  return absl::OkStatus();
}

absl::StatusOr<ssize_t> StreamSocket::Send(const char *buffer, size_t length,
                                           Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Send on closed socket");
  }
  size_t sent = 0;
  while (sent < length) {
    ssize_t n = ::send(impl_->fd, buffer + sent, length - sent, MSG_NOSIGNAL);
    if (n > 0) {
      sent += static_cast<size_t>(n);
      continue;
    }
    if (n == 0) {
      break;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      if (absl::Status s = WaitFdWritable(ctx, impl_->fd); !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("send() failed: %s", strerror(errno)));
  }
  return static_cast<ssize_t>(sent);
}

absl::StatusOr<ssize_t> StreamSocket::Receive(char *buffer, size_t buflen,
                                              Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Receive on closed socket");
  }
  for (;;) {
    ssize_t n = ::recv(impl_->fd, buffer, buflen, 0);
    if (n >= 0) {
      return n;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      if (absl::Status s = WaitFdReadable(ctx, impl_->fd); !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("recv() failed: %s", strerror(errno)));
  }
}

// Read exactly `n` bytes (yielding as needed).  Returns the number read, which
// is < n only on EOF.
static absl::StatusOr<size_t> ReadExactly(int fd, Context ctx, char *buffer,
                                          size_t n) {
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::recv(fd, buffer + got, n - got, 0);
    if (r > 0) {
      got += static_cast<size_t>(r);
      continue;
    }
    if (r == 0) {
      return got;  // EOF
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      if (absl::Status s = WaitFdReadable(ctx, fd); !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("recv() failed: %s", strerror(errno)));
  }
  return got;
}

absl::StatusOr<ssize_t> StreamSocket::SendMessage(char *buffer, size_t length,
                                                  Context ctx) {
  // toolbelt convention: write the 4-byte big-endian length immediately before
  // `buffer` and send length+4 bytes from buffer-4 in one logical message.
  char *start = buffer - sizeof(int32_t);
  uint32_t net_len = htonl(static_cast<uint32_t>(length));
  std::memcpy(start, &net_len, sizeof(net_len));
  absl::StatusOr<ssize_t> n = Send(start, length + sizeof(int32_t), ctx);
  if (!n.ok()) {
    return n.status();
  }
  return static_cast<ssize_t>(length);
}

absl::StatusOr<ssize_t> StreamSocket::ReceiveMessage(char *buffer, size_t buflen,
                                                     Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("ReceiveMessage on closed socket");
  }
  char lenbuf[sizeof(int32_t)];
  absl::StatusOr<size_t> got = ReadExactly(impl_->fd, ctx, lenbuf, sizeof(lenbuf));
  if (!got.ok()) {
    return got.status();
  }
  if (*got == 0) {
    return static_cast<ssize_t>(0);  // clean EOF
  }
  if (*got < sizeof(lenbuf)) {
    return absl::InternalError("short read on message length");
  }
  uint32_t net_len;
  std::memcpy(&net_len, lenbuf, sizeof(net_len));
  size_t length = ntohl(net_len);
  if (length > buflen) {
    return absl::InternalError(absl::StrFormat(
        "message length %zu exceeds buffer size %zu", length, buflen));
  }
  absl::StatusOr<size_t> payload = ReadExactly(impl_->fd, ctx, buffer, length);
  if (!payload.ok()) {
    return payload.status();
  }
  if (*payload < length) {
    return absl::InternalError("EOF in the middle of a message");
  }
  return static_cast<ssize_t>(length);
}

absl::StatusOr<SocketAddress> StreamSocket::GetPeerName() const {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("GetPeerName on closed socket");
  }
  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  if (::getpeername(impl_->fd, reinterpret_cast<sockaddr *>(&ss), &slen) < 0) {
    return absl::InternalError(
        absl::StrFormat("getpeername() failed: %s", strerror(errno)));
  }
  return SockaddrToAddr(ss);
}

bool StreamSocket::Connected() const {
  return impl_ != nullptr && impl_->fd >= 0;
}

void StreamSocket::Close() {
  if (impl_ != nullptr) {
    impl_.reset();
  }
}

// -------------------------------------------------------------------------
// UDPSocket
// -------------------------------------------------------------------------

struct UDPSocket::Impl {
  int fd = -1;
  ~Impl() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
};

absl::Status UDPSocket::Bind(const InetAddress &addr) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    return absl::InternalError(
        absl::StrFormat("socket() failed: %s", strerror(errno)));
  }
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  const sockaddr_in &sa = addr.GetAddress();
  if (::bind(fd, reinterpret_cast<const sockaddr *>(&sa), sizeof(sa)) < 0) {
    int e = errno;
    ::close(fd);
    return absl::InternalError(absl::StrFormat("bind() failed: %s", strerror(e)));
  }
  int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  sockaddr_in bound;
  socklen_t blen = sizeof(bound);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &blen) == 0) {
    bound_address_ = InetAddress(bound);
  }
  impl_ = std::make_shared<Impl>();
  impl_->fd = fd;
  return absl::OkStatus();
}

absl::Status UDPSocket::SetBroadcast() {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("SetBroadcast on closed socket");
  }
  int one = 1;
  if (::setsockopt(impl_->fd, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one)) < 0) {
    return absl::InternalError(
        absl::StrFormat("SO_BROADCAST failed: %s", strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status UDPSocket::SendTo(const InetAddress &addr, const void *buffer,
                               size_t length, Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("SendTo on closed socket");
  }
  const sockaddr_in &sa = addr.GetAddress();
  for (;;) {
    ssize_t n = ::sendto(impl_->fd, buffer, length, 0,
                         reinterpret_cast<const sockaddr *>(&sa), sizeof(sa));
    if (n >= 0) {
      return absl::OkStatus();
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      if (absl::Status s = WaitFdWritable(ctx, impl_->fd); !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("sendto() failed: %s", strerror(errno)));
  }
}

absl::StatusOr<ssize_t> UDPSocket::ReceiveFrom(InetAddress &sender, void *buffer,
                                               size_t buflen, Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("ReceiveFrom on closed socket");
  }
  for (;;) {
    sockaddr_in from;
    socklen_t flen = sizeof(from);
    ssize_t n = ::recvfrom(impl_->fd, buffer, buflen, 0,
                          reinterpret_cast<sockaddr *>(&from), &flen);
    if (n >= 0) {
      sender = InetAddress(from);
      return n;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      if (absl::Status s = WaitFdReadable(ctx, impl_->fd); !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("recvfrom() failed: %s", strerror(errno)));
  }
}

}  // namespace subspace::async

#endif  // SUBSPACE_CORO_BACKEND_ASIO
