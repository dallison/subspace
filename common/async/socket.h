// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_SOCKET_H
#define _COMMON_ASYNC_SOCKET_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/async/context.h"
#include "common/async/task.h"
#include "toolbelt/sockets.h"
#include <chrono>

// Networking facade used by the server's discovery and bridging paths.  The
// address types (InetAddress / VirtualAddress / SocketAddress) are pure value
// holders and are shared by all backends, so they are re-exported from
// toolbelt unchanged.  The socket types differ:
//
//   co   backend: StreamSocket/UDPSocket/UnixSocket are thin aliases over the
//                 cpp_toolbelt sockets.  Because the co Context is
//                 `const co::Coroutine *` (exactly what toolbelt methods take)
//                 call sites pass the Context straight through and behavior is
//                 identical to the pre-existing code.
//
//   asio and co20 backends: StreamSocket/UDPSocket/UnixSocket are wrappers
//                 over non-blocking raw fds that wait through the backend
//                 when an operation would block.  TCP and vsock share a
//                 single code path.  The length-delimited framing of
//                 SendMessage/ReceiveMessage and the fd protocol of
//                 SendFds/ReceiveFds match toolbelt exactly, so the wire
//                 format is unchanged.  Under co20 the methods that wait
//                 return tasks (see task.h); a null Context blocks the thread.

namespace subspace::async {

using InetAddress = toolbelt::InetAddress;
using VirtualAddress = toolbelt::VirtualAddress;
using SocketAddress = toolbelt::SocketAddress;

}  // namespace subspace::async

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO ||                     \
    SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20

#include "toolbelt/fd.h"
#include <memory>
#include <string>
#include <vector>

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
#include <boost/asio/generic/datagram_protocol.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#endif

namespace subspace::async {

// A stream socket (TCP or vsock) over a raw fd.  Mirrors the subset of
// toolbelt::StreamSocket the server uses.
class StreamSocket {
public:
  StreamSocket() = default;
  StreamSocket(StreamSocket &&) = default;
  StreamSocket &operator=(StreamSocket &&) = default;
  StreamSocket(const StreamSocket &) = delete;
  StreamSocket &operator=(const StreamSocket &) = delete;

  // Bind a listening socket to `addr` (the only mode the server uses).
  absl::Status Bind(const SocketAddress &addr, bool listen);

  // Connect to `addr` (synchronous; matches toolbelt's blocking Connect).
  absl::Status Connect(const SocketAddress &addr);

  // Accept an incoming connection, yielding the coroutine until one arrives.
  Task<absl::StatusOr<StreamSocket>> Accept(Context ctx);

  absl::Status SetNonBlocking();

  // Raw send/receive.
  Task<absl::StatusOr<ssize_t>> Send(const char *buffer, size_t length,
                                     Context ctx);
  Task<absl::StatusOr<ssize_t>> Receive(char *buffer, size_t buflen,
                                        Context ctx);

  // Length-delimited message framing identical to toolbelt: SendMessage writes
  // a 4-byte big-endian length immediately before `buffer` (so `buffer` must
  // point 4 bytes into a length+4 region), then the payload.  ReceiveMessage
  // reads the 4-byte length then that many payload bytes and returns the
  // payload length (0 on clean EOF).
  Task<absl::StatusOr<ssize_t>> SendMessage(char *buffer, size_t length,
                                            Context ctx);
  Task<absl::StatusOr<ssize_t>> ReceiveMessage(char *buffer, size_t buflen,
                                               Context ctx);

  SocketAddress BoundAddress() const { return bound_address_; }
  absl::StatusOr<SocketAddress> GetPeerName() const;

  bool Connected() const;
  void Close();

  // Takes ownership of `fd`, which must be a non-blocking stream socket.
  static StreamSocket Adopt(int fd, SocketAddress bound_address = {});

  // The socket's file descriptor, or -1 if it is closed.
  int Fd() const;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
  SocketAddress bound_address_;

  friend absl::StatusOr<StreamSocket> MakeConnectedStreamSocket(
      std::shared_ptr<Impl> impl);
};

// An IPv4 UDP socket over a raw fd.
class UDPSocket {
public:
  UDPSocket() = default;
  UDPSocket(UDPSocket &&) = default;
  UDPSocket &operator=(UDPSocket &&) = default;
  UDPSocket(const UDPSocket &) = delete;
  UDPSocket &operator=(const UDPSocket &) = delete;

  absl::Status Bind(const InetAddress &addr);
  absl::Status SetBroadcast();
  Task<absl::Status> SendTo(const InetAddress &addr, const void *buffer,
                            size_t length, Context ctx);
  Task<absl::StatusOr<ssize_t>> ReceiveFrom(InetAddress &sender, void *buffer,
                                            size_t buflen, Context ctx);

  const InetAddress &BoundAddress() const { return bound_address_; }

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
  InetAddress bound_address_;
};

// A Unix-domain stream socket with SCM_RIGHTS fd passing.  Mirrors the subset
// of toolbelt::UnixSocket the client RPC and the server's client handler use,
// and replicates toolbelt's wire framing exactly (4-byte big-endian length
// prefix for messages; the fd protocol of SendFds/ReceiveFds) so a client and
// server built with different backends interoperate.
class UnixSocket {
public:
  // Like toolbelt::UnixSocket, the default constructor creates the underlying
  // AF_UNIX/SOCK_STREAM fd so Bind()/Connect() can operate on it.
  UnixSocket();
  explicit UnixSocket(int fd, bool connected = false);
  UnixSocket(UnixSocket &&) = default;
  UnixSocket &operator=(UnixSocket &&) = default;
  UnixSocket(const UnixSocket &) = delete;
  UnixSocket &operator=(const UnixSocket &) = delete;

  absl::Status Bind(const std::string &pathname, bool listen);
  absl::Status Connect(const std::string &pathname);
  Task<absl::StatusOr<UnixSocket>> Accept(Context ctx) const;

  // Put the socket in blocking mode.  In blocking mode the underlying fd is
  // left blocking so socket calls never return EAGAIN and therefore never
  // yield (the Context passed to the I/O methods is ignored).  This lets a
  // client that is not driven by a coroutine (no yield_context) use the socket
  // synchronously, exactly like the co backend's null-coroutine path.  Must be
  // called before Connect()/Bind().
  void SetBlocking(bool blocking);

  Task<absl::StatusOr<ssize_t>> Send(const char *buffer, size_t length,
                                     Context ctx);
  Task<absl::StatusOr<ssize_t>> Receive(char *buffer, size_t buflen,
                                        Context ctx);
  Task<absl::StatusOr<ssize_t>> SendMessage(char *buffer, size_t length,
                                            Context ctx);
  Task<absl::StatusOr<ssize_t>> ReceiveMessage(char *buffer, size_t buflen,
                                               Context ctx);
  Task<absl::StatusOr<std::vector<char>>>
  ReceiveVariableLengthMessage(Context ctx);

  Task<absl::Status> SendFds(const std::vector<toolbelt::FileDescriptor> &fds,
                             Context ctx);
  Task<absl::Status> ReceiveFds(std::vector<toolbelt::FileDescriptor> &fds,
                                Context ctx);

  absl::Status SetNonBlocking();
  absl::Status SetCloseOnExec();
  toolbelt::FileDescriptor GetFileDescriptor() const;

  bool Connected() const;
  void Close();
  const std::string &BoundAddress() const { return bound_address_; }

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
  std::string bound_address_;
};

}  // namespace subspace::async

#else  // SUBSPACE_CORO_BACKEND_CO

namespace subspace::async {

// The co backend reuses the toolbelt sockets directly.  The Context
// (const co::Coroutine *) is exactly the argument these methods accept.
using StreamSocket = toolbelt::StreamSocket;
using UDPSocket = toolbelt::UDPSocket;
using UnixSocket = toolbelt::UnixSocket;

}  // namespace subspace::async

#endif

namespace subspace::async {

// Connects a TCP socket to `addr` without blocking the runtime.  A non-zero
// timeout returns DeadlineExceededError if the connection isn't made in time;
// a zero timeout leaves it to the kernel.  The socket is non-blocking.
Task<absl::StatusOr<StreamSocket>>
ConnectStream(const InetAddress &addr, std::chrono::nanoseconds timeout,
              Context ctx);

// Binds a listening TCP socket to `addr` with SO_REUSEADDR set, so a
// restarted server can rebind a fixed port while connections from its
// previous run are in TIME_WAIT.
absl::StatusOr<StreamSocket> ListenStream(const InetAddress &addr);

// The socket's file descriptor, or -1 if it is closed.
int StreamSocketFd(const StreamSocket &socket);

}  // namespace subspace::async

#endif  // _COMMON_ASYNC_SOCKET_H
