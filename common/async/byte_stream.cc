// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/byte_stream.h"

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "common/async/wait.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#if SUBSPACE_ENABLE_MTLS
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#endif

namespace subspace::async {
namespace {

constexpr size_t kTlsRecordBuf = 16 * 1024;
constexpr size_t kMaxReceiveChunk = 16 * 1024 * 1024;

#if defined(MSG_NOSIGNAL)
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

absl::Status PeerClosed() {
  return absl::AbortedError("the peer closed the connection");
}

std::optional<std::chrono::nanoseconds>
Remaining(std::optional<std::chrono::steady_clock::time_point> deadline) {
  if (!deadline.has_value()) {
    return std::chrono::nanoseconds(0);
  }
  std::chrono::nanoseconds remaining =
      *deadline - std::chrono::steady_clock::now();
  if (remaining <= std::chrono::nanoseconds(0)) {
    return std::nullopt;
  }
  return remaining;
}

#if SUBSPACE_ENABLE_MTLS

std::string SslErrors(const char *what) {
  std::string message(what);
  unsigned long err = 0;
  while ((err = ERR_get_error()) != 0) {
    char buf[256];
    ERR_error_string_n(err, buf, sizeof(buf));
    message.append(": ");
    message.append(buf);
  }
  return message;
}

#endif

} // namespace

struct MtlsContext::State {
#if SUBSPACE_ENABLE_MTLS
  SSL_CTX *ctx = nullptr;
  ~State() {
    if (ctx != nullptr) {
      SSL_CTX_free(ctx);
    }
  }
#endif
};

struct ByteStream::Tls {
#if SUBSPACE_ENABLE_MTLS
  std::shared_ptr<MtlsContext> mtls;
  SSL *ssl = nullptr;
  BIO *read_bio = nullptr;
  BIO *write_bio = nullptr;
  ~Tls() {
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
  }
#endif
};

MtlsContext::MtlsContext() = default;
MtlsContext::~MtlsContext() = default;

absl::StatusOr<std::shared_ptr<MtlsContext>>
MtlsContext::Load(const std::string &cert_file, const std::string &key_file,
                  const std::string &ca_file) {
#if !SUBSPACE_ENABLE_MTLS
  (void)cert_file;
  (void)key_file;
  (void)ca_file;
  return absl::UnimplementedError(
      "mTLS is not enabled in this build; rebuild with --//:mtls "
      "(Bazel) or -DSUBSPACE_ENABLE_MTLS=ON (CMake)");
#else
  if (cert_file.empty() || key_file.empty() || ca_file.empty()) {
    return absl::InvalidArgumentError(
        "mTLS needs a certificate, a private key, and a CA certificate");
  }
  OPENSSL_init_ssl(0, nullptr);
  SSL_CTX *ctx = SSL_CTX_new(TLS_method());
  if (ctx == nullptr) {
    return absl::InternalError(SslErrors("SSL_CTX_new failed"));
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                     nullptr);
  SSL_CTX_set_verify_depth(ctx, 2);
  // TLS 1.3 session tickets would make a tunnel receiver write on a
  // connection whose application protocol never sends.  The handshake itself
  // still writes, and the stream flushes that.
  SSL_CTX_set_num_tickets(ctx, 0);
  SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);
  SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION);

  if (SSL_CTX_load_verify_locations(ctx, ca_file.c_str(), nullptr) != 1) {
    std::string message = SslErrors("failed to load the mTLS CA");
    SSL_CTX_free(ctx);
    return absl::InvalidArgumentError(message);
  }
  if (SSL_CTX_use_certificate_file(ctx, cert_file.c_str(), SSL_FILETYPE_PEM) !=
      1) {
    std::string message = SslErrors("failed to load the mTLS certificate");
    SSL_CTX_free(ctx);
    return absl::InvalidArgumentError(message);
  }
  if (SSL_CTX_use_PrivateKey_file(ctx, key_file.c_str(), SSL_FILETYPE_PEM) !=
      1) {
    std::string message = SslErrors("failed to load the mTLS private key");
    SSL_CTX_free(ctx);
    return absl::InvalidArgumentError(message);
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    std::string message = SslErrors("mTLS private key does not match the certificate");
    SSL_CTX_free(ctx);
    return absl::InvalidArgumentError(message);
  }

  auto mtls = std::shared_ptr<MtlsContext>(new MtlsContext());
  mtls->state_ = std::make_unique<State>();
  mtls->state_->ctx = ctx;
  return mtls;
#endif
}

ByteStream::ByteStream() = default;

ByteStream::ByteStream(StreamSocket socket) : socket_(std::move(socket)) {}

ByteStream::~ByteStream() = default;

ByteStream::ByteStream(ByteStream &&other) noexcept
    : socket_(std::move(other.socket_)), tls_(std::move(other.tls_)),
      broken_(other.broken_) {}

ByteStream &ByteStream::operator=(ByteStream &&other) noexcept {
  if (this != &other) {
    socket_ = std::move(other.socket_);
    tls_ = std::move(other.tls_);
    broken_ = other.broken_;
  }
  return *this;
}

absl::Status ByteStream::SetNonBlocking() { return socket_.SetNonBlocking(); }

int ByteStream::Fd() const { return StreamSocketFd(socket_); }

void ByteStream::Close() {
  tls_.reset();
  socket_.Close();
}

std::optional<ByteStream::Deadline>
ByteStream::DeadlineFrom(std::chrono::nanoseconds timeout) const {
  if (timeout <= std::chrono::nanoseconds(0)) {
    return std::nullopt;
  }
  return std::chrono::steady_clock::now() + timeout;
}

absl::Status ByteStream::SendRaw(Context ctx, const char *data, size_t length,
                                 std::optional<Deadline> deadline) {
  const int fd = Fd();
  while (length > 0) {
    ssize_t n = ::send(fd, data, length, kSendFlags);
    if (n > 0) {
      data += n;
      length -= static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      std::optional<std::chrono::nanoseconds> remaining = Remaining(deadline);
      if (deadline.has_value() && !remaining.has_value()) {
        return absl::DeadlineExceededError("timed out sending mTLS records");
      }
      if (absl::Status s = WaitWritable(ctx, fd, remaining.value_or(
                                                     std::chrono::nanoseconds(0)));
          !s.ok()) {
        return s;
      }
      continue;
    }
    return absl::InternalError(
        absl::StrFormat("send failed: %s", strerror(errno)));
  }
  return absl::OkStatus();
}

absl::StatusOr<ssize_t> ByteStream::RecvRaw(Context ctx, char *buffer,
                                            size_t length,
                                            std::optional<Deadline> deadline) {
  const int fd = Fd();
  for (;;) {
    ssize_t n = ::recv(fd, buffer, length, 0);
    if (n > 0) {
      return n;
    }
    if (n == 0) {
      return static_cast<ssize_t>(0);
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      return absl::InternalError(
          absl::StrFormat("recv failed: %s", strerror(errno)));
    }
    std::optional<std::chrono::nanoseconds> remaining = Remaining(deadline);
    if (deadline.has_value() && !remaining.has_value()) {
      return absl::DeadlineExceededError("timed out reading mTLS records");
    }
    if (absl::Status s =
            WaitReadable(ctx, fd, remaining.value_or(std::chrono::nanoseconds(0)));
        !s.ok()) {
      return s;
    }
  }
}

absl::Status ByteStream::FlushTls(Context ctx, std::optional<Deadline> deadline) {
#if !SUBSPACE_ENABLE_MTLS
  (void)ctx;
  (void)deadline;
  return absl::OkStatus();
#else
  if (tls_ == nullptr || tls_->write_bio == nullptr) {
    return absl::OkStatus();
  }
  char buf[kTlsRecordBuf];
  while (BIO_pending(tls_->write_bio) > 0) {
    int n = BIO_read(tls_->write_bio, buf, sizeof(buf));
    if (n <= 0) {
      break;
    }
    if (absl::Status s = SendRaw(ctx, buf, static_cast<size_t>(n), deadline);
        !s.ok()) {
      return s;
    }
  }
  return absl::OkStatus();
#endif
}

absl::Status ByteStream::FillTls(Context ctx, std::optional<Deadline> deadline) {
#if !SUBSPACE_ENABLE_MTLS
  (void)ctx;
  (void)deadline;
  return absl::OkStatus();
#else
  char buf[kTlsRecordBuf];
  absl::StatusOr<ssize_t> n = RecvRaw(ctx, buf, sizeof(buf), deadline);
  if (!n.ok()) {
    return n.status();
  }
  if (*n == 0) {
    return PeerClosed();
  }
  int wrote = BIO_write(tls_->read_bio, buf, static_cast<int>(*n));
  if (wrote != static_cast<int>(*n)) {
    return absl::InternalError(SslErrors("failed to buffer an mTLS record"));
  }
  return absl::OkStatus();
#endif
}

absl::Status ByteStream::HandshakeTls(const std::shared_ptr<MtlsContext> &mtls,
                                      bool server_role, Context ctx,
                                      std::chrono::nanoseconds timeout) {
#if !SUBSPACE_ENABLE_MTLS
  (void)mtls;
  (void)server_role;
  (void)ctx;
  (void)timeout;
  return absl::UnimplementedError(
      "mTLS is not enabled in this build; rebuild with --//:mtls "
      "(Bazel) or -DSUBSPACE_ENABLE_MTLS=ON (CMake)");
#else
  if (mtls == nullptr || mtls->state_ == nullptr || mtls->state_->ctx == nullptr) {
    return absl::InvalidArgumentError("mTLS handshake without credentials");
  }
  if (tls_ != nullptr) {
    return absl::OkStatus();
  }
  if (absl::Status s = socket_.SetNonBlocking(); !s.ok()) {
    return s;
  }
  SSL *ssl = SSL_new(mtls->state_->ctx);
  BIO *read_bio = BIO_new(BIO_s_mem());
  BIO *write_bio = BIO_new(BIO_s_mem());
  if (ssl == nullptr || read_bio == nullptr || write_bio == nullptr) {
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    BIO_free(read_bio);
    BIO_free(write_bio);
    broken_ = true;
    return absl::InternalError(SslErrors("failed to create an mTLS session"));
  }
  // An empty read bio must look like EAGAIN, not a close, so the handshake
  // waits for the next record.  SSL_set_bio takes ownership of both bios.
  BIO_set_mem_eof_return(read_bio, -1);
  SSL_set_bio(ssl, read_bio, write_bio);
  auto state = std::make_unique<Tls>();
  state->mtls = mtls;
  state->ssl = ssl;
  state->read_bio = read_bio;
  state->write_bio = write_bio;
  SSL_set_mode(state->ssl, SSL_MODE_ENABLE_PARTIAL_WRITE |
                               SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  SSL_set_quiet_shutdown(state->ssl, 1);
  if (server_role) {
    SSL_set_accept_state(state->ssl);
  } else {
    SSL_set_connect_state(state->ssl);
  }
  tls_ = std::move(state);

  const std::optional<Deadline> deadline = DeadlineFrom(timeout);
  for (;;) {
    ERR_clear_error();
    int rc = SSL_do_handshake(tls_->ssl);
    if (absl::Status s = FlushTls(ctx, deadline); !s.ok()) {
      broken_ = true;
      tls_.reset();
      return s;
    }
    if (rc == 1) {
      long verify = SSL_get_verify_result(tls_->ssl);
      if (verify != X509_V_OK) {
        broken_ = true;
        tls_.reset();
        return absl::UnauthenticatedError(absl::StrFormat(
            "mTLS peer certificate rejected: %s",
            X509_verify_cert_error_string(verify)));
      }
      return absl::OkStatus();
    }
    int err = SSL_get_error(tls_->ssl, rc);
    if (err == SSL_ERROR_WANT_READ) {
      if (absl::Status s = FillTls(ctx, deadline); !s.ok()) {
        broken_ = true;
        tls_.reset();
        return s;
      }
      continue;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
      continue;
    }
    std::string message = SslErrors("mTLS handshake failed");
    broken_ = true;
    tls_.reset();
    return absl::UnauthenticatedError(message);
  }
#endif
}

absl::StatusOr<ssize_t> ByteStream::Send(const char *buffer, size_t length,
                                         Context ctx) {
  if (broken_) {
    return absl::FailedPreconditionError("mTLS handshake failed");
  }
#if SUBSPACE_ENABLE_MTLS
  if (tls_ != nullptr) {
    size_t off = 0;
    while (off < length) {
      ERR_clear_error();
      int n = SSL_write(tls_->ssl, buffer + off,
                        static_cast<int>(std::min(
                            length - off,
                            static_cast<size_t>(std::numeric_limits<int>::max()))));
      if (absl::Status s = FlushTls(ctx, std::nullopt); !s.ok()) {
        return s;
      }
      if (n > 0) {
        off += static_cast<size_t>(n);
        continue;
      }
      int err = SSL_get_error(tls_->ssl, n);
      if (err == SSL_ERROR_WANT_READ) {
        if (absl::Status s = FillTls(ctx, std::nullopt); !s.ok()) {
          return s;
        }
        continue;
      }
      if (err == SSL_ERROR_WANT_WRITE) {
        continue;
      }
      if (err == SSL_ERROR_ZERO_RETURN) {
        return PeerClosed();
      }
      return absl::UnavailableError(SslErrors("mTLS send failed"));
    }
    return static_cast<ssize_t>(length);
  }
#endif
  return socket_.Send(buffer, length, ctx);
}

absl::StatusOr<ssize_t> ByteStream::Receive(char *buffer, size_t buflen,
                                            Context ctx) {
  if (broken_) {
    return absl::FailedPreconditionError("mTLS handshake failed");
  }
#if SUBSPACE_ENABLE_MTLS
  if (tls_ != nullptr) {
    if (buflen == 0) {
      return static_cast<ssize_t>(0);
    }
    for (;;) {
      ERR_clear_error();
      int n = SSL_read(tls_->ssl, buffer,
                       static_cast<int>(std::min(
                           buflen, static_cast<size_t>(std::numeric_limits<int>::max()))));
      if (absl::Status s = FlushTls(ctx, std::nullopt); !s.ok()) {
        return s;
      }
      if (n > 0) {
        return static_cast<ssize_t>(n);
      }
      int err = SSL_get_error(tls_->ssl, n);
      if (err == SSL_ERROR_WANT_READ) {
        if (absl::Status s = FillTls(ctx, std::nullopt); !s.ok()) {
          return s;
        }
        continue;
      }
      if (err == SSL_ERROR_WANT_WRITE) {
        continue;
      }
      if (err == SSL_ERROR_ZERO_RETURN) {
        return static_cast<ssize_t>(0);
      }
      return absl::UnavailableError(SslErrors("mTLS receive failed"));
    }
  }
#endif
  return socket_.Receive(buffer, buflen, ctx);
}

absl::Status ByteStream::SendAll(const void *data, size_t length, Context ctx) {
  const char *p = static_cast<const char *>(data);
  while (length > 0) {
    absl::StatusOr<ssize_t> n = Send(p, length, ctx);
    if (!n.ok()) {
      return n.status();
    }
    if (*n <= 0) {
      return absl::UnavailableError("connection closed while sending");
    }
    p += *n;
    length -= static_cast<size_t>(*n);
  }
  return absl::OkStatus();
}

absl::Status ByteStream::ReadFully(void *buffer, size_t length, Context ctx,
                                   std::optional<Deadline> deadline) {
  if (broken_) {
    return absl::FailedPreconditionError("mTLS handshake failed");
  }
  char *p = static_cast<char *>(buffer);
#if SUBSPACE_ENABLE_MTLS
  if (tls_ != nullptr) {
    while (length > 0) {
      size_t chunk = std::min(length, kTlsRecordBuf);
      // Receive waits without a deadline.  A deadline has to bound the
      // ciphertext read inside FillTls, so that path calls SSL_read itself.
      if (!deadline.has_value()) {
        absl::StatusOr<ssize_t> got = Receive(p, chunk, ctx);
        if (!got.ok()) {
          return got.status();
        }
        if (*got <= 0) {
          return PeerClosed();
        }
        p += *got;
        length -= static_cast<size_t>(*got);
        continue;
      }
      ERR_clear_error();
      int got = SSL_read(tls_->ssl, p, static_cast<int>(chunk));
      if (absl::Status s = FlushTls(ctx, deadline); !s.ok()) {
        return s;
      }
      if (got > 0) {
        p += got;
        length -= static_cast<size_t>(got);
        continue;
      }
      int err = SSL_get_error(tls_->ssl, got);
      if (err == SSL_ERROR_WANT_READ) {
        if (absl::Status s = FillTls(ctx, deadline); !s.ok()) {
          return s;
        }
        continue;
      }
      if (err == SSL_ERROR_WANT_WRITE) {
        continue;
      }
      if (err == SSL_ERROR_ZERO_RETURN) {
        return PeerClosed();
      }
      return absl::UnavailableError(SslErrors("mTLS receive failed"));
    }
    return absl::OkStatus();
  }
#endif
  if (deadline.has_value()) {
    const int fd = Fd();
    while (length > 0) {
      ssize_t n = ::recv(fd, p, length, 0);
      if (n > 0) {
        p += n;
        length -= static_cast<size_t>(n);
        continue;
      }
      if (n == 0) {
        return PeerClosed();
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        return absl::InternalError(
            absl::StrFormat("recv failed: %s", strerror(errno)));
      }
      std::optional<std::chrono::nanoseconds> remaining = Remaining(deadline);
      if (!remaining.has_value()) {
        return absl::DeadlineExceededError("timed out");
      }
      if (absl::Status s = WaitReadable(ctx, fd, *remaining); !s.ok()) {
        return s;
      }
    }
    return absl::OkStatus();
  }
  while (length > 0) {
    size_t chunk = std::min(length, kMaxReceiveChunk);
    absl::StatusOr<ssize_t> n = socket_.Receive(p, chunk, ctx);
    if (!n.ok()) {
      return n.status();
    }
    if (*n <= 0) {
      return PeerClosed();
    }
    p += *n;
    length -= static_cast<size_t>(*n);
  }
  return absl::OkStatus();
}

absl::StatusOr<ssize_t> ByteStream::SendMessage(char *buffer, size_t length,
                                                Context ctx) {
#if SUBSPACE_ENABLE_MTLS
  if (tls_ != nullptr) {
    char *start = buffer - sizeof(int32_t);
    uint32_t net_len = htonl(static_cast<uint32_t>(length));
    memcpy(start, &net_len, sizeof(net_len));
    if (absl::Status s = SendAll(start, length + sizeof(int32_t), ctx); !s.ok()) {
      return s;
    }
    return static_cast<ssize_t>(length);
  }
#endif
  return socket_.SendMessage(buffer, length, ctx);
}

absl::StatusOr<ssize_t> ByteStream::ReceiveMessage(char *buffer, size_t buflen,
                                                   Context ctx) {
#if SUBSPACE_ENABLE_MTLS
  if (tls_ != nullptr) {
    char lenbuf[sizeof(int32_t)];
    absl::Status got = ReadFully(lenbuf, sizeof(lenbuf), ctx);
    if (!got.ok()) {
      if (absl::IsAborted(got)) {
        return static_cast<ssize_t>(0);
      }
      return got;
    }
    uint32_t net_len = 0;
    memcpy(&net_len, lenbuf, sizeof(net_len));
    size_t length = ntohl(net_len);
    if (length > buflen) {
      return absl::InternalError(absl::StrFormat(
          "message length %zu exceeds buffer size %zu", length, buflen));
    }
    if (length == 0) {
      return static_cast<ssize_t>(0);
    }
    if (absl::Status payload = ReadFully(buffer, length, ctx); !payload.ok()) {
      return payload;
    }
    return static_cast<ssize_t>(length);
  }
#endif
  return socket_.ReceiveMessage(buffer, buflen, ctx);
}

} // namespace subspace::async
