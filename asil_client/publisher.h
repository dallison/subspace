// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "asil_client/channel_memory.h"
#include "asil_client/error.h"
#include "asil_client/server_connection.h"
#include "asil_client/unique_fd.h"

#include <cstddef>
#include <cstdint>

namespace subspace {
namespace asil {

class Client;

struct PublisherOptions {
  // The channel layout.  On a server with a static channel config these must
  // match the configured channel.
  int64_t slot_size = 0;
  int32_t num_slots = 0;
  int32_t checksum_size = 0; // 0 means 4 bytes.
  int32_t metadata_size = 0;
  const char *type = nullptr;
  // Put a checksum in every message.
  bool checksum = false;
  // Publish an activation message if the channel hasn't been activated.  A
  // reliable publisher always publishes one.
  bool activate = false;
  // Never overwrite a message that a reliable subscriber hasn't read.  Buffer()
  // returns null until there is room; wait for PollFd() and try again.
  bool reliable = false;
  // Publish on a virtual channel of this multiplexer.  vchan_id -1 lets the
  // server choose the channel's id.
  const char *mux = nullptr;
  int32_t vchan_id = -1;
  // Bytes in the channel's control block for subscriber queues.  This must
  // match the channel; 0 for a channel without queues.
  uint64_t subscriber_queue_arena_size = 0;
  // The channel has split buffers.  They must exist already: the server
  // creates them for a static channel, otherwise a standard publisher does.
  // split_slots has room for the channel's num_slots slot mappings, and the
  // allocator maps slots that a custom allocator created.
  bool use_split_buffers = false;
  SplitSlot *split_slots = nullptr;
  int32_t split_slot_capacity = 0;
  SplitBufferAllocator split_allocator;
};

struct PublishedMessage {
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
};

// A publisher for a fixed-size channel.  An unreliable publisher always holds
// a slot to write the next message into, recycling the oldest message that no
// subscriber holds.  A reliable publisher takes a slot only when one is free
// of messages that reliable subscribers haven't read.
//
// A publisher is not thread safe and must not outlive its Client.
class Publisher {
public:
  Publisher() = default;
  ~Publisher() { (void)Close(); }
  Publisher(const Publisher &) = delete;
  Publisher &operator=(const Publisher &) = delete;

  Error Open(Client &client, const char *channel_name,
             const PublisherOptions &options);
  // Removes the publisher from the server.  Safe to call more than once.
  Error Close();
  bool IsOpen() const { return client_ != nullptr; }

  // Where to write the next message, or null if no slot is available.  A
  // reliable publisher also returns null while the channel has no
  // subscribers.
  void *Buffer();
  int64_t SlotSize() const { return memory_.SlotSize(); }
  int NumSlots() const { return memory_.NumSlots(); }

  // The user metadata area for the next message, or null if the channel has
  // none or no slot is available.
  void *Metadata();
  size_t MetadataSize() const {
    return static_cast<size_t>(memory_.MetadataSize());
  }

  // Publishes the message_size bytes written to Buffer().  If a subscriber
  // has joined since the last publish this asks the server for its trigger
  // fd first.
  Error Publish(size_t message_size, PublishedMessage *published = nullptr);

  // Readable when a reliable subscriber may have made room for a message.
  int PollFd() const { return poll_.Get(); }
  // Waits until PollFd() is readable.  A negative timeout waits forever.
  Error Wait(int timeout_ms);

  bool IsReliable() const { return reliable_; }
  // The publisher's virtual channel id, or -1.
  int VirtualChannelId() const { return vchan_id_; }
  const char *Name() const { return name_; }

private:
  bool HoldSlot();
  Error RefreshTriggers();
  shm::MessageSlot *FindFreeSlot();
  shm::MessageSlot *FindFreeSlotUnreliable();
  shm::MessageSlot *FindFreeSlotReliable();
  shm::MessageSlot *OldestReliableCandidate(bool require_reliable_seen) const;
  bool ReliableClaimable(shm::MessageSlot *slot,
                         bool require_reliable_seen) const;
  bool ClaimSlot(shm::MessageSlot *slot);
  void PrepareClaimedSlot(shm::MessageSlot *slot, bool forced_reuse);
  int FindRetiredSlotToReuse(bool oldest_first) const;
  void SetSlotBuffer(shm::MessageSlot *slot);
  void PublishSlot(shm::MessageSlot *slot, bool is_activation,
                   PublishedMessage *published);
  void TriggerSubscribers() const;
  void Reset();

  Client *client_ = nullptr;
  internal::ChannelMemory memory_;
  internal::BufferContext buffer_context_;
  char name_[kMaxChannelNameLength + 1] = {};
  // The channel that owns the shared memory: the multiplexer of a virtual
  // channel.
  char resolved_name_[kMaxChannelNameLength + 1] = {};
  int32_t channel_id_ = -1;
  int32_t publisher_id_ = -1;
  int32_t vchan_id_ = -1;
  uint16_t num_sub_updates_ = 0;
  bool checksum_ = false;
  bool reliable_ = false;
  shm::MessageSlot *slot_ = nullptr;
  UniqueFd poll_;
  TriggerFdList subscriber_triggers_;
  TriggerFdList retirement_triggers_;
};

} // namespace asil
} // namespace subspace
