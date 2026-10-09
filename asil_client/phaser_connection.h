// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "asil_client/server_connection.h"

#include <cstddef>
#include <cstdint>

namespace subspace {
namespace asil {

// The standard Subspace protocol: protobuf requests and responses over the
// server's Unix socket, with file descriptors passed as SCM_RIGHTS.  Requests
// and responses are Phaser messages held in buffers inside this object and
// converted to and from protobuf wire format, so the handshake makes no heap
// allocations.  A request or response that does not fit fails with
// kCapacityExceeded.
//
// The object holds about 60 KB of buffers, so keep it in static storage or
// inside a long-lived object rather than on a small stack.
class PhaserServerConnection : public ServerConnection {
public:
  // Protobuf wire bytes for one request or response.
  static constexpr size_t kWireBufferSize = 16 * 1024;
  // The decoded request or response.
  static constexpr size_t kMessageBufferSize = 32 * 1024;
  // Descriptors in one response: the channel's control blocks, the
  // publisher or subscriber's own triggers, and a trigger and a retirement
  // descriptor for every other publisher or subscriber.
  static constexpr int kMaxReceivedFds = 2 * kMaxTriggerFds + 8;

  PhaserServerConnection() = default;
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
  Error Receive(size_t &length);
  Error ReceiveFds();
  Error Rejected(const char *message, size_t length);

  UniqueFd socket_;
  ReceivedFds fds_;
  char last_error_[256] = {};
  alignas(8) char wire_[sizeof(uint32_t) + kWireBufferSize];
  alignas(8) char message_[kMessageBufferSize];
};

} // namespace asil
} // namespace subspace
