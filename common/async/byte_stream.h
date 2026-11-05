// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_BYTE_STREAM_H
#define _COMMON_ASYNC_BYTE_STREAM_H

#include "common/async/mtls.h"
#include "common/async/socket.h"
#include <chrono>
#include <memory>
#include <optional>

namespace subspace::async {

// A connected stream.  Without an mTLS handshake it is the socket itself.
// After HandshakeTls, Send and Receive carry plaintext and BoringSSL (or
// OpenSSL, in the CMake build) carries the records on the socket.
class ByteStream {
public:
  ByteStream();
  explicit ByteStream(StreamSocket socket);
  ~ByteStream();

  ByteStream(ByteStream &&) noexcept;
  ByteStream &operator=(ByteStream &&) noexcept;
  ByteStream(const ByteStream &) = delete;
  ByteStream &operator=(const ByteStream &) = delete;

  absl::Status SetNonBlocking();
  StreamSocket &Socket() { return socket_; }
  const StreamSocket &Socket() const { return socket_; }
  int Fd() const;
  void Close();

  // Client when this side connected, server when this side accepted.
  // `timeout` of zero waits until the handshake finishes or the peer closes.
  absl::Status HandshakeTls(const std::shared_ptr<MtlsContext> &mtls,
                            bool server_role, Context ctx,
                            std::chrono::nanoseconds timeout);

  absl::StatusOr<ssize_t> Send(const char *buffer, size_t length, Context ctx);
  absl::StatusOr<ssize_t> Receive(char *buffer, size_t buflen, Context ctx);
  absl::Status SendAll(const void *data, size_t length, Context ctx);
  // Reads exactly `length` bytes.  A deadline uses the non-blocking
  // descriptor directly, because the co backend's Receive waits until its
  // whole buffer is full.
  absl::Status ReadFully(
      void *buffer, size_t length, Context ctx,
      std::optional<std::chrono::steady_clock::time_point> deadline =
          std::nullopt);

  // Length-delimited framing identical to StreamSocket::SendMessage.
  // `buffer` must have 4 bytes of writable padding immediately before it.
  absl::StatusOr<ssize_t> SendMessage(char *buffer, size_t length, Context ctx);
  // Returns 0 on a clean close.
  absl::StatusOr<ssize_t> ReceiveMessage(char *buffer, size_t buflen,
                                         Context ctx);

private:
  struct Tls;
  using Deadline = std::chrono::steady_clock::time_point;

  absl::Status SendRaw(Context ctx, const char *data, size_t length,
                       std::optional<Deadline> deadline);
  absl::StatusOr<ssize_t> RecvRaw(Context ctx, char *buffer, size_t length,
                                  std::optional<Deadline> deadline);
  absl::Status FlushTls(Context ctx, std::optional<Deadline> deadline);
  absl::Status FillTls(Context ctx, std::optional<Deadline> deadline);
  std::optional<Deadline> DeadlineFrom(std::chrono::nanoseconds timeout) const;

  StreamSocket socket_;
  std::unique_ptr<Tls> tls_;
  // Set when a handshake fails, so later sends cannot fall back to cleartext.
  bool broken_ = false;
};

} // namespace subspace::async

#endif // _COMMON_ASYNC_BYTE_STREAM_H
