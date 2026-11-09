// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "asil_client/server_connection.h"
#include "common/server_wire.h"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace subspace {
namespace asil {

// The standard Subspace protocol: native Phaser requests and responses over
// the server's Unix socket, with file descriptors passed as SCM_RIGHTS.
// Requests and responses are Phaser messages held in a ServerWire. The
// default buffers are inside this object, so a handshake makes no heap
// allocations, even when it fails. A larger wire buffer is allocated once,
// in the constructor, and handshakes still do not allocate. A request or
// response that does not fit fails with kCapacityExceeded, and
// LastServerError() says which buffer it did not fit.
//
// The default object holds about 32 KB of buffers, so keep it in static
// storage or inside a long-lived object rather than on a small stack.
class PhaserServerConnection : public ServerConnection {
public:
  // Capacity of the request or response, both as a Phaser message and as the
  // payload copied onto the socket. A larger constructor argument uses this
  // same size for both buffers.
  static constexpr size_t kWireBufferSize = 16 * 1024;

  // wire_buffer_size is the payload capacity of one request or response.
  // Zero selects kWireBufferSize. A size up to kWireBufferSize uses the
  // buffer inside this object. A larger size allocates its storage here.
  explicit PhaserServerConnection(size_t wire_buffer_size = kWireBufferSize);

  size_t WireBufferSize() const { return wire_buffer_size_; }
  // Descriptors in one response: the channel's control blocks, the
  // publisher or subscriber's own triggers, and a trigger and a retirement
  // descriptor for every other publisher or subscriber.
  static constexpr int kMaxReceivedFds = 2 * kMaxTriggerFds + 8;

  ~PhaserServerConnection() override = default;

  PhaserServerConnection(const PhaserServerConnection &) = delete;
  PhaserServerConnection &operator=(const PhaserServerConnection &) = delete;

  Error Connect(const char *socket_name);
  void Close() { socket_.Reset(); }
  bool Connected() const { return socket_.Valid(); }

  Error Init(const char *client_name, InitReply &reply) override;
  Error CreatePublisher(const PublisherRequest &request,
                        PublisherReply &reply) override;
  Error CreateSubscriber(const SubscriberRequest &request,
                         SubscriberReply &reply) override;
  Error GetTriggers(const char *channel_name, TriggersReply &reply) override;
  Error RemovePublisher(const char *channel_name,
                        int32_t publisher_id) override;
  Error RemoveSubscriber(const char *channel_name,
                         int32_t subscriber_id) override;
  Error RegisterBuffer(const ClientBuffer &buffer, int fd) override;
  Error GetBuffer(const BufferRequest &request, BufferReply &reply) override;

  const char *LastServerError() const override { return last_error_; }

  using ReceivedFds = FdList<kMaxReceivedFds>;

private:
  template <typename Build, typename Read>
  Error Transact(Build build, Read read, int send_fd = -1);
  Error Send(size_t length);
  Error SendFd(int fd);
  Error Receive(const char *&data, size_t &length);
  Error ReceiveFds();
  void SetLastError(const char *message, size_t length);
  void Remember(const char *message, size_t length) {
    if (message != nullptr && length != 0) {
      SetLastError(message, length);
    }
  }
  Error Rejected(const char *message, size_t length);

  UniqueFd socket_;
  ReceivedFds fds_;
  char last_error_[256] = {};
  size_t wire_buffer_size_ = kWireBufferSize;
  size_t message_buffer_size_ = kWireBufferSize;
  bool storage_ok_ = true;
  // Holds the message buffer and the wire buffer when they do not fit in
  // the embedded arrays. Empty for the default size.
  std::unique_ptr<char[]> allocated_;
  alignas(8) char embedded_message_[kWireBufferSize];
  alignas(8) char embedded_wire_[kServerWireHeaderSize + kWireBufferSize];
  ServerWire<ProvidedServerWireBuffers> wire_;
};

} // namespace asil
} // namespace subspace
