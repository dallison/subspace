// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "phaser/runtime/wireformat.h"
#include "proto/subspace.phaser.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

namespace subspace {

// Requests to and responses from the server, as native Phaser payloads.
// The bytes are Data() / ByteSizeLong(): the message buffer itself, with no
// protobuf transcoding.
//
// A ServerWire copies a request into its wire buffer and loads a response
// from received bytes.  The Buffers policy decides where messages and wire
// bytes live:
//
// FixedServerWireBuffers holds them inside the object and makes no heap
// allocations, including when something fails. Its errors are process-lifetime
// statuses: the text stays readable through Status::message(), and returning
// one only adjusts a refcount. Requests and responses that don't fit fail
// with kResourceExhausted. NewRequest and NewResponse build messages in a
// single message buffer, so a new message replaces the previous one.
//
// DynamicServerWireBuffers grows its wire buffer to fit each request.  Its
// messages are default constructed Phaser messages, which own their storage.
//
// Socket I/O stays with the caller.  The length that precedes each message on
// the socket is the caller's to write, and Serialize leaves room for it just
// before the payload.
inline constexpr size_t kServerWireHeaderSize = sizeof(uint32_t);

template <size_t kMessageSize, size_t kWireSize> class FixedServerWireBuffers {
public:
  static constexpr size_t kMaxWireSize = kWireSize;
  // Report failures with the permanent statuses, which keep their text and
  // do not allocate.
  static constexpr bool kErrorMessages = false;

  // kSize, because kPerformance builds a template of each message type on
  // first use, which allocates from the heap.  The handshake messages are
  // small, so the simple allocator costs nothing noticeable.
  absl::StatusOr<phaser::Request> NewRequest() {
    return phaser::Request::TryCreateMutable(message_, sizeof(message_),
                                             ::phaser::Tuning::kSize);
  }

  absl::StatusOr<phaser::Response> NewResponse() {
    return phaser::Response::TryCreateMutable(message_, sizeof(message_),
                                              ::phaser::Tuning::kSize);
  }

  // Room for the header and length bytes after it, or nullptr.
  char *Wire(size_t length) { return length <= kWireSize ? wire_ : nullptr; }

private:
  alignas(8) char message_[kMessageSize];
  alignas(8) char wire_[kServerWireHeaderSize + kWireSize];
};

// Message and wire storage the caller owns. The wire pointer addresses the
// length header, and wire_size is the payload capacity after that header.
class ProvidedServerWireBuffers {
public:
  static constexpr bool kErrorMessages = false;

  ProvidedServerWireBuffers() = default;
  ProvidedServerWireBuffers(char *message, size_t message_size, char *wire,
                            size_t wire_size)
      : message_(message),
        message_size_(message_size),
        wire_(wire),
        wire_size_(wire_size) {}

  absl::StatusOr<phaser::Request> NewRequest() {
    return phaser::Request::TryCreateMutable(message_, message_size_,
                                             ::phaser::Tuning::kSize);
  }

  absl::StatusOr<phaser::Response> NewResponse() {
    return phaser::Response::TryCreateMutable(message_, message_size_,
                                              ::phaser::Tuning::kSize);
  }

  char *Wire(size_t length) { return length <= wire_size_ ? wire_ : nullptr; }

private:
  char *message_ = nullptr;
  size_t message_size_ = 0;
  char *wire_ = nullptr;
  size_t wire_size_ = 0;
};

class DynamicServerWireBuffers {
public:
  static constexpr bool kErrorMessages = true;

  char *Wire(size_t length) {
    wire_.resize(kServerWireHeaderSize + length);
    return wire_.data();
  }

private:
  std::vector<char> wire_;
};

namespace server_wire_internal {

// Statuses for the fixed-buffer failures. Built once, on the first call,
// which the ASIL connection makes before main. Later calls copy a refcount.
inline absl::Status PermanentError(absl::StatusCode code,
                                  absl::string_view message) {
  static const absl::Status build(absl::StatusCode::kResourceExhausted,
                                  "Message is too large to build");
  static const absl::Status send(absl::StatusCode::kResourceExhausted,
                                 "Message is too large to send");
  static const absl::Status not_phaser(absl::StatusCode::kInvalidArgument,
                                       "Not a phaser payload");
  static const absl::Status too_large(absl::StatusCode::kResourceExhausted,
                                      "Phaser payload is too large");
  if (message == "Message is too large to build") {
    return build;
  }
  if (message == "Message is too large to send") {
    return send;
  }
  if (message == "Not a phaser payload") {
    return not_phaser;
  }
  if (message == "Phaser payload is too large") {
    return too_large;
  }
  // A message this function doesn't know. An empty message is inlined, so
  // this still doesn't allocate; the text is not available.
  return absl::Status(code, absl::string_view());
}

} // namespace server_wire_internal

template <typename Buffers> class ServerWire {
public:
  ServerWire() = default;
  explicit ServerWire(Buffers buffers) : buffers_(std::move(buffers)) {}

  absl::StatusOr<phaser::Request> NewRequest() { return buffers_.NewRequest(); }
  absl::StatusOr<phaser::Response> NewResponse() {
    return buffers_.NewResponse();
  }

  // Copies request's native payload into the wire buffer and returns its
  // length.  The bytes are at Payload(), with kServerWireHeaderSize bytes
  // before them.
  absl::StatusOr<size_t> Serialize(const phaser::Request &request) {
    return CopyPayload(request);
  }

  absl::StatusOr<size_t> Serialize(const phaser::Response &response) {
    return CopyPayload(response);
  }

  // The bytes from the last Serialize.
  char *Payload() const { return payload_; }

  // Room for a response of length bytes, or nullptr if it doesn't fit.
  char *ResponseBuffer(size_t length) {
    char *wire = buffers_.Wire(length);
    return wire == nullptr ? nullptr : wire + kServerWireHeaderSize;
  }

  // Loads a native payload into an owned message.
  static absl::Status Decode(const char *data, size_t length,
                             phaser::Response &response) {
    return LoadPayload(data, length, response);
  }

  static absl::Status Decode(const char *data, size_t length,
                             phaser::Request &request) {
    return LoadPayload(data, length, request);
  }

private:
  // An error with message. Fixed buffers reuse a permanent status so the
  // text is visible and reporting the error does not allocate.
  static absl::Status Error(absl::StatusCode code, absl::string_view message) {
    if (!Buffers::kErrorMessages) {
      return server_wire_internal::PermanentError(code, message);
    }
    return absl::Status(code, message);
  }

  template <typename Message>
  absl::StatusOr<size_t> CopyPayload(const Message &message) {
    if (message.AllocationFailed()) {
      return Error(absl::StatusCode::kResourceExhausted,
                   "Message is too large to build");
    }
    const size_t length = message.ByteSizeLong();
    char *wire = buffers_.Wire(length);
    if (wire == nullptr) {
      return Error(absl::StatusCode::kResourceExhausted,
                   "Message is too large to send");
    }
    payload_ = wire + kServerWireHeaderSize;
    if (length != 0) {
      std::memcpy(payload_, message.Data(), length);
    }
    return length;
  }

  template <typename Message>
  static absl::Status LoadPayload(const char *data, size_t length,
                                  Message &message) {
    if (data == nullptr ||
        !::phaser::internal::IsStructurallyValidPhaser(
            absl::Span<const char>(data, length))) {
      return Error(absl::StatusCode::kInvalidArgument,
                   "Not a phaser payload");
    }
    Message view = Message::CreateReadonly(data, length);
    message.Clear();
    if (absl::Status status = message.CloneFrom(view); !status.ok()) {
      message.Clear();
      // Phaser's own failure already names the reason, and that status does
      // not allocate. An empty message would hide why this failed.
      if (!status.message().empty()) {
        return status;
      }
      return Error(status.code(), "Phaser payload is too large");
    }
    if (message.AllocationFailed()) {
      message.Clear();
      return Error(absl::StatusCode::kResourceExhausted,
                   "Phaser payload is too large");
    }
    return absl::OkStatus();
  }

  Buffers buffers_;
  char *payload_ = nullptr;
};

} // namespace subspace
