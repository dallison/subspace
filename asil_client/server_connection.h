// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// The handshake between the ASIL client and the Subspace server.
//
// The client talks to the server only to connect, to create and remove
// publishers and subscribers, and to fetch trigger file descriptors when the
// set of publishers or subscribers on a channel changes.  Messages move
// through shared memory without the server.  ServerConnection hides the wire
// format of these requests so that it can be replaced; PhaserServerConnection
// implements the standard protocol.

#pragma once

#include "asil_client/error.h"
#include "asil_client/shm_layout.h"
#include "asil_client/unique_fd.h"

#include <cstdint>

namespace subspace {
namespace asil {

constexpr int kMaxTriggerFds = shm::kMaxSlotOwners;
using TriggerFdList = FdList<kMaxTriggerFds>;

struct InitReply {
  UniqueFd scb;
  uint64_t session_id = 0;
  int32_t user_id = -1;
  int32_t group_id = -1;
};

struct PublisherRequest {
  const char *channel_name = nullptr;
  int64_t slot_size = 0;
  int32_t num_slots = 0;
  const char *type = nullptr;
  int32_t checksum_size = 0;
  int32_t metadata_size = 0;
};

struct PublisherReply {
  int32_t channel_id = -1;
  int32_t publisher_id = -1;
  int32_t num_slots = 0;
  int32_t vchan_id = -1;
  int32_t num_sub_updates = 0;
  uint64_t subscriber_queue_arena_size = 0;
  UniqueFd ccb;
  UniqueFd bcb;
  // Filled when not null.
  TriggerFdList *subscriber_triggers = nullptr;
  TriggerFdList *retirement_triggers = nullptr;
};

struct SubscriberRequest {
  const char *channel_name = nullptr;
  const char *type = nullptr;
  int32_t max_active_messages = 1;
};

struct SubscriberReply {
  int32_t channel_id = -1;
  int32_t subscriber_id = -1;
  int32_t num_slots = 0;
  int32_t vchan_id = -1;
  int32_t num_pub_updates = 0;
  int32_t checksum_size = 0;
  int32_t metadata_size = 0;
  int32_t subscriber_queue_size = 0;
  uint64_t subscriber_queue_arena_size = 0;
  bool use_split_buffers = false;
  UniqueFd ccb;
  UniqueFd bcb;
  UniqueFd trigger;
  UniqueFd poll;
  // Filled when not null.
  TriggerFdList *reliable_publisher_triggers = nullptr;
  TriggerFdList *retirement_triggers = nullptr;
};

struct TriggersReply {
  // Filled when not null.
  TriggerFdList *reliable_publisher_triggers = nullptr;
  TriggerFdList *subscriber_triggers = nullptr;
  TriggerFdList *retirement_triggers = nullptr;
};

class ServerConnection {
public:
  virtual ~ServerConnection() = default;

  virtual Error Init(const char *client_name, InitReply &reply) = 0;
  virtual Error CreatePublisher(const PublisherRequest &request,
                                PublisherReply &reply) = 0;
  virtual Error CreateSubscriber(const SubscriberRequest &request,
                                 SubscriberReply &reply) = 0;
  virtual Error GetTriggers(const char *channel_name, TriggersReply &reply) = 0;
  virtual Error RemovePublisher(const char *channel_name,
                                int32_t publisher_id) = 0;
  virtual Error RemoveSubscriber(const char *channel_name,
                                 int32_t subscriber_id) = 0;

  // The server's message for the last request that returned kServerRejected.
  virtual const char *LastServerError() const = 0;
};

} // namespace asil
} // namespace subspace
