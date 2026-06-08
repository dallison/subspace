// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_SOCKET_H
#define _COMMON_ASYNC_SOCKET_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/async/context.h"
#include "toolbelt/sockets.h"

// Networking facade used by the server's discovery and bridging paths.  The
// address types (InetAddress / VirtualAddress / SocketAddress) are pure value
// holders and are shared by both backends, so they are re-exported from
// toolbelt unchanged.  The socket types differ:
//
//   co   backend: StreamSocket/UDPSocket/UnixSocket are thin aliases over the
//                 cpp_toolbelt sockets.  Because the co Context is
//                 `const co::Coroutine *` (exactly what toolbelt methods take)
//                 call sites pass the Context straight through and behavior is
//                 identical to the pre-existing code.
//
//   asio backend: StreamSocket/UDPSocket are real wrappers over Boost.Asio.
//                 TCP and vsock share a single code path via
//                 boost::asio::generic::stream_protocol with a sockaddr_vm
//                 endpoint for vsock (Asio has no native vsock type).  The
//                 length-delimited framing of SendMessage/ReceiveMessage
//                 matches toolbelt exactly so the wire format is unchanged.

namespace subspace::async {

using InetAddress = toolbelt::InetAddress;
using VirtualAddress = toolbelt::VirtualAddress;
using SocketAddress = toolbelt::SocketAddress;

}  // namespace subspace::async

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include <boost/asio/generic/datagram_protocol.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <memory>

namespace subspace::async {

// A stream socket (TCP, vsock or Unix-stream) over Boost.Asio.  Mirrors the
// subset of toolbelt::StreamSocket the server uses.
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
  absl::StatusOr<StreamSocket> Accept(Context ctx);

  absl::Status SetNonBlocking();

  // Raw send/receive.
  absl::StatusOr<ssize_t> Send(const char *buffer, size_t length, Context ctx);
  absl::StatusOr<ssize_t> Receive(char *buffer, size_t buflen, Context ctx);

  // Length-delimited message framing identical to toolbelt: SendMessage writes
  // a 4-byte big-endian length immediately before `buffer` (so `buffer` must
  // point 4 bytes into a length+4 region), then the payload.  ReceiveMessage
  // reads the 4-byte length then that many payload bytes and returns the
  // payload length (0 on clean EOF).
  absl::StatusOr<ssize_t> SendMessage(char *buffer, size_t length, Context ctx);
  absl::StatusOr<ssize_t> ReceiveMessage(char *buffer, size_t buflen,
                                         Context ctx);

  SocketAddress BoundAddress() const { return bound_address_; }
  absl::StatusOr<SocketAddress> GetPeerName() const;

  bool Connected() const;
  void Close();

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
  SocketAddress bound_address_;

  friend absl::StatusOr<StreamSocket> MakeConnectedStreamSocket(
      std::shared_ptr<Impl> impl);
};

// A UDP socket over Boost.Asio (generic datagram protocol so it can support
// IPv4 today and other families later).
class UDPSocket {
public:
  UDPSocket() = default;
  UDPSocket(UDPSocket &&) = default;
  UDPSocket &operator=(UDPSocket &&) = default;
  UDPSocket(const UDPSocket &) = delete;
  UDPSocket &operator=(const UDPSocket &) = delete;

  absl::Status Bind(const InetAddress &addr);
  absl::Status SetBroadcast();
  absl::Status SendTo(const InetAddress &addr, const void *buffer,
                      size_t length, Context ctx);
  absl::StatusOr<ssize_t> ReceiveFrom(InetAddress &sender, void *buffer,
                                      size_t buflen, Context ctx);

  const InetAddress &BoundAddress() const { return bound_address_; }

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
  InetAddress bound_address_;
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

#endif  // _COMMON_ASYNC_SOCKET_H
