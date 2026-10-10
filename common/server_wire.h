// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/subspace.phaser.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace subspace {

// Requests to and responses from the server, as Phaser messages sent in
// protobuf wire format.  The server, and clients in other languages, see the
// same bytes as from a protobuf client.
//
// A ServerWire serializes requests into its wire buffer and decodes
// responses.  The Buffers policy decides where messages and wire bytes live:
//
// FixedServerWireBuffers holds them inside the object and makes no heap
// allocations.  Requests and responses that don't fit fail with
// kResourceExhausted.  NewRequest and NewResponse build messages in a single
// message buffer, so a new message replaces the previous one.
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

  absl::StatusOr<phaser::Request> NewRequest() {
    return phaser::Request::TryCreateMutable(message_, sizeof(message_));
  }

  absl::StatusOr<phaser::Response> NewResponse() {
    return phaser::Response::TryCreateMutable(message_, sizeof(message_));
  }

  // Room for the header and length bytes after it, or nullptr.
  char *Wire(size_t length) { return length <= kWireSize ? wire_ : nullptr; }

private:
  alignas(8) char message_[kMessageSize];
  alignas(8) char wire_[kServerWireHeaderSize + kWireSize];
};

class DynamicServerWireBuffers {
public:
  char *Wire(size_t length) {
    wire_.resize(kServerWireHeaderSize + length);
    return wire_.data();
  }

private:
  std::vector<char> wire_;
};

template <typename Buffers> class ServerWire {
public:
  absl::StatusOr<phaser::Request> NewRequest() { return buffers_.NewRequest(); }
  absl::StatusOr<phaser::Response> NewResponse() {
    return buffers_.NewResponse();
  }

  // Serializes request into the wire buffer and returns its length.  The
  // bytes are at Payload(), with kServerWireHeaderSize bytes before them.
  absl::StatusOr<size_t> Serialize(const phaser::Request &request) {
    if (request.AllocationFailed()) {
      return absl::ResourceExhaustedError("Request is too large to build");
    }
    const size_t length = request.SerializedSize();
    char *wire = buffers_.Wire(length);
    if (wire == nullptr) {
      return absl::ResourceExhaustedError("Request is too large to send");
    }
    payload_ = wire + kServerWireHeaderSize;
    ::phaser::ProtoBuffer out(payload_, length);
    if (absl::Status status = request.Serialize(out); !status.ok()) {
      return status;
    }
    return out.Size();
  }

  // The bytes from the last Serialize.
  char *Payload() const { return payload_; }

  // Room for a response of length bytes, or nullptr if it doesn't fit.
  char *ResponseBuffer(size_t length) {
    char *wire = buffers_.Wire(length);
    return wire == nullptr ? nullptr : wire + kServerWireHeaderSize;
  }

  // Decodes length bytes of a response into response.
  static absl::Status Decode(const char *data, size_t length,
                             phaser::Response &response) {
    ::phaser::ProtoBuffer in(data, length);
    return response.Deserialize(in);
  }

private:
  Buffers buffers_;
  char *payload_ = nullptr;
};

} // namespace subspace
