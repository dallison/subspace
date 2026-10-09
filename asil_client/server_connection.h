// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// The handshake between the ASIL client and the Subspace server.
//
// The client talks to the server only to connect, to create and remove
// publishers and subscribers, to fetch trigger file descriptors when the
// set of publishers or subscribers on a channel changes, and, while opening a
// publisher or subscriber, to exchange buffer descriptors.  Messages move
// through shared memory without the server.  ServerConnection hides the wire
// format of these requests so that it can be replaced; PhaserServerConnection
// implements the standard protocol.

#pragma once

#include "asil_client/error.h"
#include "asil_client/shm_layout.h"
#include "asil_client/split_buffer.h"
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
  bool reliable = false;
  // The multiplexer of a virtual channel, or null.
  const char *mux = nullptr;
  int32_t vchan_id = -1;
  uint64_t subscriber_queue_arena_size = 0;
  bool use_split_buffers = false;
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
  // Readable when a reliable subscriber has released a message.
  UniqueFd poll;
  // Filled when not null.
  TriggerFdList *subscriber_triggers = nullptr;
  TriggerFdList *retirement_triggers = nullptr;
};

struct SubscriberRequest {
  const char *channel_name = nullptr;
  const char *type = nullptr;
  int32_t max_active_messages = 1;
  bool reliable = false;
  // The multiplexer of a virtual channel, or null.
  const char *mux = nullptr;
  int32_t vchan_id = -1;
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

// A memfd message buffer.  The server keeps the descriptor so that other
// clients can map the buffer.
struct ClientBuffer {
  // The channel that owns the buffer: the multiplexer of a virtual channel.
  const char *channel_name = nullptr;
  uint64_t session_id = 0;
  uint32_t buffer_index = 0;
  uint64_t full_size = 0;
};

// A buffer registered with the server: buffer_index's single buffer, or the
// prefix or one slot of a split buffer set.
struct BufferRequest {
  // The channel that owns the buffer: the multiplexer of a virtual channel.
  const char *channel_name = nullptr;
  uint64_t session_id = 0;
  uint32_t buffer_index = 0;
  bool is_prefix = false;
  uint32_t slot_id = 0;
};

struct BufferReply {
  // Whether the server has the buffer.
  bool found = false;
  // Invalid when the creator registered no descriptor.
  UniqueFd fd;
  uint64_t full_size = 0;
  uint64_t allocation_size = 0;
  uint64_t handle = 0;
  int64_t map_offset = 0;
  BufferAllocator allocator = BufferAllocator::kUnspecified;
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

  // Gives the server the descriptor of a memfd buffer that this client
  // created.  The client must own a publisher on the channel.
  virtual Error RegisterBuffer(const ClientBuffer &buffer, int fd) = 0;
  // Fetches a buffer that its creator registered.  Returns kOk with
  // reply.found false when it hasn't been registered yet.
  virtual Error GetBuffer(const BufferRequest &request, BufferReply &reply) = 0;

  // The server's message for the last request that returned kServerRejected.
  virtual const char *LastServerError() const = 0;
};

} // namespace asil
} // namespace subspace
