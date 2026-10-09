// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// A Subspace client for safety-related software.
//
// The client publishes and reads messages on fixed-size channels, reliable or
// unreliable, plain or virtual, sharing them with the standard clients.  It
// is written in C++17 using only the standard library and POSIX.  It throws no
// exceptions and returns an Error from every operation that can fail.  It
// makes no heap allocations: buffers and tables have fixed capacities, and
// publishing and reading messages talks to the server only when publishers,
// subscribers or memfd buffers change.
//
//   subspace::asil::PhaserServerConnection connection;
//   connection.Connect("/tmp/subspace");
//   subspace::asil::Client client;
//   client.Init(connection, "my_client");
//   subspace::asil::PublisherOptions options;
//   options.slot_size = 256;
//   options.num_slots = 8;
//   subspace::asil::Publisher publisher;
//   client.CreatePublisher("/chan", options, publisher);

#pragma once

#include "asil_client/error.h"
#include "asil_client/publisher.h"
#include "asil_client/server_connection.h"
#include "asil_client/shm_layout.h"
#include "asil_client/subscriber.h"

#include <cstdint>

namespace subspace {
namespace asil {

// A connection to one Subspace server.  It must outlive its publishers and
// subscribers, and the ServerConnection must outlive it.  It is not thread
// safe.
class Client {
public:
  Client() = default;
  ~Client();
  Client(const Client &) = delete;
  Client &operator=(const Client &) = delete;

  Error Init(ServerConnection &connection, const char *client_name);
  bool Initialized() const { return scb_ != nullptr; }

  Error CreatePublisher(const char *channel_name,
                        const PublisherOptions &options, Publisher &publisher) {
    return publisher.Open(*this, channel_name, options);
  }
  Error CreateSubscriber(const char *channel_name,
                         const SubscriberOptions &options,
                         Subscriber &subscriber) {
    return subscriber.Open(*this, channel_name, options);
  }

  // Why the server rejected the last request that returned kServerRejected.
  const char *LastServerError() const;

private:
  friend class Publisher;
  friend class Subscriber;

  ServerConnection *connection_ = nullptr;
  shm::SystemControlBlock *scb_ = nullptr;
  uint64_t session_id_ = 0;
  int32_t user_id_ = -1;
  int32_t group_id_ = -1;
};

} // namespace asil
} // namespace subspace
