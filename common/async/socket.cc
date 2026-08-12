// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/socket.h"

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include "absl/strings/str_format.h"
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <netinet/in.h>
#include <poll.h>
#include <stddef.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace subspace::async {

namespace {

constexpr bool kSocketDebugEnabled = false;

boost::asio::io_context &IocFromYield(Context yield) {
  return static_cast<boost::asio::io_context &>(
      yield.get_executor().context());
}

// Wait until `fd` is ready for `cond` (wait_read / wait_write), yielding the
// coroutine.  The stream_descriptor is given the caller's real fd and
// `release()`d before it is destroyed so it never closes the caller's fd.  We
// deliberately do NOT ::dup() the fd: dup'ing churns ephemeral fd numbers
// through the shared process fd table, and when two io_contexts (e.g. an
// in-process server and client) recycle those numbers across their separate
// reactors a completion can resume a coroutine owned by the other io_context's
// thread.  Using the stable real fd keeps each descriptor registered in
// exactly one reactor.
//
// The coroutine suspends on a sentinel steady_timer (`notifier`), never
// directly on the descriptor.  This is essential for graceful shutdown: a
// cancellation emitted onto the coroutine can complete a *descriptor* wait
// synchronously, re-entering and unwinding this coroutine (destroying this
// frame's descriptor) while asio is still walking the cancellation chain - a
// use-after-free.  Cancelling a steady_timer always posts its completion, so
// the coroutine only ever resumes on a clean stack; the descriptor uses a
// plain handler that merely wakes the notifier, so a cancellation never reaches
// it directly.
absl::Status WaitFdReady(Context yield, int fd,
                         boost::asio::posix::descriptor_base::wait_type cond,
                         const char *what) {
  boost::asio::io_context &ioc = IocFromYield(yield);
  // Fast path: if the fd is already ready, return immediately without
  // suspending.  This keeps a persistently-ready fd from racing a
  // graceful-shutdown cancellation that could otherwise repeatedly wake us
  // before the reactor reports readiness, livelocking the coroutine.
  {
    const short ev =
        (cond == boost::asio::posix::stream_descriptor::wait_read) ? POLLIN
                                                                   : POLLOUT;
    struct pollfd pfd = {.fd = fd, .events = ev, .revents = 0};
    if (::poll(&pfd, 1, 0) > 0 && (pfd.revents & (ev | POLLHUP)) != 0) {
      return absl::OkStatus();
    }
  }
  auto sd = std::make_shared<boost::asio::posix::stream_descriptor>(ioc, fd);
  auto fd_ready = std::make_shared<bool>(false);
  auto notified = std::make_shared<bool>(false);

  // Shared so the cancellation handler installed below can hold it alive and
  // safely poke it even if it fires after this frame unwinds.
  auto notifier = std::make_shared<boost::asio::steady_timer>(ioc);
  notifier->expires_at(boost::asio::steady_timer::time_point::max());

  // Single, idempotent wake: fd-ready and graceful-shutdown cancellation both
  // resume the coroutine through here, so notifier->cancel() is called exactly
  // once and never races asio's per-operation timer cancellation.
  auto wake = [notifier, notified]() {
    if (!*notified) {
      *notified = true;
      notifier->cancel();
    }
  };

  // Bind the descriptor completion to the coroutine's own executor (its
  // strand).  Under a multi-threaded io_context the reactor would otherwise run
  // this handler on an arbitrary thread the instant the fd is ready - possibly
  // before the coroutine below has registered notifier->async_wait().  The
  // wake() would then cancel nothing, and the subsequent async_wait on a
  // never-expiring timer would hang forever (a missed wakeup).  Posting the
  // handler onto the coroutine's strand serializes it with the async_wait
  // registration, so the wake can never be lost.
  sd->async_wait(
      cond, boost::asio::bind_executor(
                yield.get_executor(),
                [sd, fd_ready, wake](const boost::system::error_code &ec) {
                  if (!ec) {
                    *fd_ready = true;
                  }
                  wake();
                }));

  // Graceful shutdown routes through the same idempotent wake() rather than
  // cancelling the notifier op directly: emitting a cancellation onto the
  // notifier's timer operation races its natural completion and corrupts asio's
  // timer queue (a use-after-free during shutdown).  We assign our own handler
  // to the coroutine's cancellation slot and wait on the notifier with an
  // *unconnected* slot so asio installs no per-operation timer cancellation.
  auto exec = yield.get_executor();
  auto cancel_slot = yield.get_cancellation_slot();
  if (cancel_slot.is_connected()) {
    cancel_slot.assign([exec, wake](boost::asio::cancellation_type) {
      boost::asio::post(exec, [wake]() { wake(); });
    });
  }

  boost::system::error_code ec;
  notifier->async_wait(
      boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot(),
                                          yield[ec]));

  // Clear our handler so a late re-emit cannot poke a stale wake, then cancel
  // the pending descriptor wait and release the real fd so destroying the
  // shared descriptor does not close it.
  if (cancel_slot.is_connected()) {
    cancel_slot.clear();
  }
  boost::system::error_code ignored;
  sd->cancel(ignored);
  sd->release();

  if (!*fd_ready) {
    // Resumed without the fd becoming ready: the wait was cancelled (e.g.
    // graceful shutdown).
    return absl::InternalError(
        absl::StrFormat("%s error: Operation canceled", what));
  }
  return absl::OkStatus();
}

absl::Status WaitFdReadable(Context yield, int fd) {
  return WaitFdReady(yield, fd,
                     boost::asio::posix::stream_descriptor::wait_read,
                     "wait_read");
}

absl::Status WaitFdWritable(Context yield, int fd) {
  return WaitFdReady(yield, fd,
                     boost::asio::posix::stream_descriptor::wait_write,
                     "wait_write");
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
  // NB: intentionally do NOT set SO_REUSEADDR here.  toolbelt's TCPSocket::Bind
  // (the co backend) does not set it by default, and the bridge port-range
  // logic relies on a bind to an already-occupied port failing.  Setting
  // SO_REUSEADDR would let a specific-IP bind coexist with a wildcard listener
  // on the same port, breaking that behavior.
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

// -------------------------------------------------------------------------
// UnixSocket
// -------------------------------------------------------------------------

namespace {

// Replicates toolbelt::BuildUnixSocketName so the Asio client/server addresses
// the same socket as a co peer (Linux abstract namespace; filesystem path
// elsewhere).
struct sockaddr_un BuildUnixSocketName(const std::string &pathname) {
  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
#ifdef __linux__
  addr.sun_path[0] = '\0';
  std::memcpy(addr.sun_path + 1, pathname.c_str(),
              std::min(pathname.size(), sizeof(addr.sun_path) - 2));
#else
  std::memcpy(addr.sun_path, pathname.c_str(),
              std::min(pathname.size(), sizeof(addr.sun_path) - 1));
#endif
  return addr;
}

// Send `length` bytes from `buffer`, yielding the coroutine on EAGAIN.
absl::StatusOr<ssize_t> SendAll(int fd, Context ctx, const char *buffer,
                                size_t length) {
  size_t sent = 0;
  while (sent < length) {
    ssize_t n = ::send(fd, buffer + sent, length - sent, MSG_NOSIGNAL);
    if (n > 0) {
      sent += static_cast<size_t>(n);
      continue;
    }
    if (n == 0) {
      break;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      if (absl::Status s = WaitFdWritable(ctx, fd); !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("send() failed: %s", strerror(errno)));
  }
  return static_cast<ssize_t>(sent);
}

}  // namespace

struct UnixSocket::Impl {
  int fd = -1;
  bool connected = false;
  // When true the fd is left blocking and socket I/O never yields.
  bool blocking = false;
  ~Impl() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
};

UnixSocket::UnixSocket() {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  impl_ = std::make_shared<Impl>();
  impl_->fd = fd;
}

void UnixSocket::SetBlocking(bool blocking) {
  if (impl_ != nullptr) {
    impl_->blocking = blocking;
  }
}

UnixSocket::UnixSocket(int fd, bool connected) {
  impl_ = std::make_shared<Impl>();
  impl_->fd = fd;
  impl_->connected = connected;
  if (fd >= 0) {
    SetFdNonBlocking(fd);
  }
}

absl::Status UnixSocket::Bind(const std::string &pathname, bool listen) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Bind on invalid unix socket");
  }
  struct sockaddr_un addr = BuildUnixSocketName(pathname);
  if (::bind(impl_->fd, reinterpret_cast<const sockaddr *>(&addr),
             sizeof(addr)) < 0) {
    return absl::InternalError(absl::StrFormat(
        "Failed to bind unix socket to %s: %s", pathname, strerror(errno)));
  }
  if (listen && ::listen(impl_->fd, 10) < 0) {
    return absl::InternalError(
        absl::StrFormat("listen() failed: %s", strerror(errno)));
  }
  if (!impl_->blocking) {
    SetFdNonBlocking(impl_->fd);
  }
  bound_address_ = pathname;
  return absl::OkStatus();
}

absl::Status UnixSocket::Connect(const std::string &pathname) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Connect on invalid unix socket");
  }
  struct sockaddr_un addr = BuildUnixSocketName(pathname);
  // Blocking connect (matches toolbelt); switch to non-blocking afterwards so
  // all subsequent I/O cooperates with the io_context.
  if (::connect(impl_->fd, reinterpret_cast<const sockaddr *>(&addr),
                sizeof(addr)) < 0) {
    return absl::InternalError(absl::StrFormat(
        "Failed to connect unix socket to %s: %s", pathname, strerror(errno)));
  }
  if (!impl_->blocking) {
    SetFdNonBlocking(impl_->fd);
  }
  impl_->connected = true;
  return absl::OkStatus();
}

absl::StatusOr<UnixSocket> UnixSocket::Accept(Context ctx) const {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Accept on invalid unix socket");
  }
  for (;;) {
    struct sockaddr_un sender;
    socklen_t slen = sizeof(sender);
    int newfd =
        ::accept(impl_->fd, reinterpret_cast<sockaddr *>(&sender), &slen);
    if (newfd >= 0) {
      return UnixSocket(newfd, /*connected=*/true);
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

absl::StatusOr<ssize_t> UnixSocket::Send(const char *buffer, size_t length,
                                         Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Send on closed socket");
  }
  return SendAll(impl_->fd, ctx, buffer, length);
}

absl::StatusOr<ssize_t> UnixSocket::Receive(char *buffer, size_t buflen,
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

absl::StatusOr<ssize_t> UnixSocket::SendMessage(char *buffer, size_t length,
                                                Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("SendMessage on closed socket");
  }
  char *start = buffer - sizeof(int32_t);
  uint32_t net_len = htonl(static_cast<uint32_t>(length));
  std::memcpy(start, &net_len, sizeof(net_len));
  absl::StatusOr<ssize_t> n = SendAll(impl_->fd, ctx, start,
                                      length + sizeof(int32_t));
  if (!n.ok()) {
    return n.status();
  }
  return static_cast<ssize_t>(length);
}

absl::StatusOr<ssize_t> UnixSocket::ReceiveMessage(char *buffer, size_t buflen,
                                                   Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("ReceiveMessage on closed socket");
  }
  char lenbuf[sizeof(int32_t)];
  absl::StatusOr<size_t> got =
      ReadExactly(impl_->fd, ctx, lenbuf, sizeof(lenbuf));
  if (!got.ok()) {
    return got.status();
  }
  if (*got == 0) {
    return static_cast<ssize_t>(0);
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

absl::StatusOr<std::vector<char>>
UnixSocket::ReceiveVariableLengthMessage(Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("Receive on closed socket");
  }
  char lenbuf[sizeof(int32_t)];
  absl::StatusOr<size_t> got =
      ReadExactly(impl_->fd, ctx, lenbuf, sizeof(lenbuf));
  if (!got.ok()) {
    return got.status();
  }
  if (*got < sizeof(lenbuf)) {
    return absl::InternalError(
        absl::StrFormat("Failed to read length from socket: socket closed"));
  }
  uint32_t net_len;
  std::memcpy(&net_len, lenbuf, sizeof(net_len));
  size_t length = ntohl(net_len);
  std::vector<char> buffer(length);
  absl::StatusOr<size_t> payload =
      ReadExactly(impl_->fd, ctx, buffer.data(), buffer.size());
  if (!payload.ok()) {
    return payload.status();
  }
  if (*payload < length) {
    return absl::InternalError("EOF in the middle of a message");
  }
  return buffer;
}

absl::Status
UnixSocket::SendFds(const std::vector<toolbelt::FileDescriptor> &fds,
                    Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("SendFds on closed socket");
  }
  constexpr size_t kMaxFds = 252;
  std::vector<char> control_buf(CMSG_SPACE(kMaxFds * sizeof(int)));

  size_t remaining_fds = fds.size();
  size_t first_fd = 0;
  // At least one message is sent even when there are no fds (matches toolbelt).
  do {
    std::fill(control_buf.begin(), control_buf.end(), 0);
    int32_t num_fds = static_cast<int32_t>(fds.size());
    size_t fds_to_send = remaining_fds > kMaxFds ? kMaxFds : remaining_fds;
    struct iovec iov = {.iov_base = reinterpret_cast<void *>(&num_fds),
                        .iov_len = sizeof(int32_t)};
    size_t fds_size = fds_to_send * sizeof(int);
    if constexpr (kSocketDebugEnabled) {
      fprintf(stderr,
              "SUBSPACE_SHM_DEBUG: SendFds socket=%d total=%zu first=%zu "
              "count=%zu values=",
              impl_->fd, fds.size(), first_fd, fds_to_send);
      for (size_t i = first_fd; i < first_fd + fds_to_send; i++) {
        fprintf(stderr, "%s%d", i == first_fd ? "" : ",", fds[i].Fd());
      }
      fprintf(stderr, "\n");
    }
    struct msghdr msg = {};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control_buf.data();
    msg.msg_controllen = static_cast<socklen_t>(CMSG_SPACE(fds_size));
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(fds_size);
    int *fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
    for (size_t i = first_fd; i < first_fd + fds_to_send; i++) {
      *fdptr++ = fds[i].Fd();
    }
    for (;;) {
      ssize_t e = ::sendmsg(impl_->fd, &msg, MSG_NOSIGNAL);
      if (e >= 0) {
        break;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        if (absl::Status s = WaitFdWritable(ctx, impl_->fd); !s.ok()) {
          return s;
        }
        continue;
      }
      return absl::InternalError(absl::StrFormat(
          "Failed to write fds to unix socket: %s", strerror(errno)));
    }
    remaining_fds -= fds_to_send;
    first_fd += fds_to_send;
  } while (remaining_fds > 0);
  return absl::OkStatus();
}

absl::Status UnixSocket::ReceiveFds(std::vector<toolbelt::FileDescriptor> &fds,
                                    Context ctx) {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("ReceiveFds on closed socket");
  }
  constexpr size_t kMaxFds = 252;
  std::vector<char> control_buf(CMSG_SPACE(kMaxFds * sizeof(int)));

  int32_t num_fds_received = 0;
  for (;;) {
    std::fill(control_buf.begin(), control_buf.end(), 0);
    int32_t total_fds = 0;
    struct iovec iov = {.iov_base = reinterpret_cast<void *>(&total_fds),
                        .iov_len = sizeof(int32_t)};
    struct msghdr msg = {};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control_buf.data();
    msg.msg_controllen = static_cast<socklen_t>(control_buf.size());

    ssize_t n;
    for (;;) {
      n = ::recvmsg(impl_->fd, &msg, 0);
      if (n >= 0) {
        break;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        if (absl::Status s = WaitFdReadable(ctx, impl_->fd); !s.ok()) {
          return s;
        }
        continue;
      }
      return absl::InternalError(absl::StrFormat(
          "Failed to read fds from unix socket: %s", strerror(errno)));
    }
    if (n == 0) {
      return absl::InternalError("EOF from socket while reading fds");
    }
    if constexpr (kSocketDebugEnabled) {
      fprintf(stderr,
              "SUBSPACE_SHM_DEBUG: ReceiveFds socket=%d n=%zd total_fds=%d "
              "msg_flags=0x%x msg_controllen=%zu\n",
              impl_->fd, n, total_fds, msg.msg_flags,
              static_cast<size_t>(msg.msg_controllen));
    }
    if ((msg.msg_flags & MSG_CTRUNC) != 0) {
      return absl::InternalError(
          "Control data was truncated while reading fds from unix socket");
    }

    bool saw_rights = false;
    for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr;
         cmsg = CMSG_NXTHDR(&msg, cmsg)) {
      if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        if constexpr (kSocketDebugEnabled) {
          fprintf(stderr,
                  "SUBSPACE_SHM_DEBUG: ReceiveFds skipping cmsg level=%d "
                  "type=%d len=%zu\n",
                  cmsg->cmsg_level, cmsg->cmsg_type,
                  static_cast<size_t>(cmsg->cmsg_len));
        }
        continue;
      }
      saw_rights = true;
      if (cmsg->cmsg_len < CMSG_LEN(0)) {
        return absl::InternalError(absl::StrFormat(
            "Invalid SCM_RIGHTS control length %zu while reading fds",
            static_cast<size_t>(cmsg->cmsg_len)));
      }
      size_t data_len = cmsg->cmsg_len - CMSG_LEN(0);
      if constexpr (kSocketDebugEnabled) {
        fprintf(stderr,
                "SUBSPACE_SHM_DEBUG: ReceiveFds SCM_RIGHTS len=%zu "
                "data_len=%zu\n",
                static_cast<size_t>(cmsg->cmsg_len), data_len);
      }
      if (data_len % sizeof(int) != 0) {
        return absl::InternalError(absl::StrFormat(
            "Misaligned SCM_RIGHTS control length %zu while reading fds",
            static_cast<size_t>(cmsg->cmsg_len)));
      }
      int *fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
      int num_fds = static_cast<int>(data_len / sizeof(int));
      for (int i = 0; i < num_fds; i++) {
        if constexpr (kSocketDebugEnabled) {
          fprintf(stderr, "SUBSPACE_SHM_DEBUG: ReceiveFds fd[%d]=%d\n",
                  num_fds_received + i, fdptr[i]);
        }
        fds.emplace_back(fdptr[i]);
      }
      num_fds_received += num_fds;
    }
    if (!saw_rights && total_fds > 0) {
      return absl::InternalError(absl::StrFormat(
          "Expected %d fds from unix socket but received no SCM_RIGHTS message",
          total_fds));
    }
    if (num_fds_received >= total_fds) {
      break;
    }
  }
  return absl::OkStatus();
}

absl::Status UnixSocket::SetNonBlocking() {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("SetNonBlocking on closed socket");
  }
  SetFdNonBlocking(impl_->fd);
  return absl::OkStatus();
}

absl::Status UnixSocket::SetCloseOnExec() {
  if (impl_ == nullptr || impl_->fd < 0) {
    return absl::FailedPreconditionError("SetCloseOnExec on closed socket");
  }
  int flags = ::fcntl(impl_->fd, F_GETFD, 0);
  if (flags < 0 || ::fcntl(impl_->fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
    return absl::InternalError(
        absl::StrFormat("fcntl FD_CLOEXEC failed: %s", strerror(errno)));
  }
  return absl::OkStatus();
}

toolbelt::FileDescriptor UnixSocket::GetFileDescriptor() const {
  if (impl_ == nullptr || impl_->fd < 0) {
    return toolbelt::FileDescriptor();
  }
  // Non-owning: the UnixSocket's Impl owns the fd.
  return toolbelt::FileDescriptor(impl_->fd, /*owned=*/false);
}

bool UnixSocket::Connected() const {
  return impl_ != nullptr && impl_->fd >= 0 && impl_->connected;
}

void UnixSocket::Close() {
  if (impl_ != nullptr) {
    impl_.reset();
  }
}

}  // namespace subspace::async

#endif  // SUBSPACE_CORO_BACKEND_ASIO
