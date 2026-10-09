// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "asil_client/channel_memory.h"
#include "asil_client/error.h"
#include "asil_client/server_connection.h"

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
  // Publish an activation message if the channel hasn't been activated.
  bool activate = false;
};

struct PublishedMessage {
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
};

// An unreliable publisher for a fixed-size channel.  It always holds a slot to
// write the next message into.
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

  // Where to write the next message, or null if every slot is in use.
  void *Buffer();
  int64_t SlotSize() const { return memory_.SlotSize(); }
  int NumSlots() const { return memory_.NumSlots(); }

  // The user metadata area for the next message, or null if the channel has
  // none or every slot is in use.
  void *Metadata();
  size_t MetadataSize() const {
    return static_cast<size_t>(memory_.MetadataSize());
  }

  // Publishes the message_size bytes written to Buffer().  If a subscriber
  // has joined since the last publish this asks the server for its trigger
  // fd first.
  Error Publish(size_t message_size, PublishedMessage *published = nullptr);

  const char *Name() const { return name_; }

private:
  bool HoldSlot();
  Error RefreshTriggers();
  shm::MessageSlot *FindFreeSlot();
  int FindRetiredSlotToReuse(bool oldest_first) const;
  void SetSlotBuffer(shm::MessageSlot *slot);
  void PublishSlot(shm::MessageSlot *slot, bool is_activation,
                   PublishedMessage *published);
  void TriggerSubscribers() const;
  void Reset();

  Client *client_ = nullptr;
  internal::ChannelMemory memory_;
  char name_[kMaxChannelNameLength + 1] = {};
  int32_t channel_id_ = -1;
  int32_t publisher_id_ = -1;
  uint16_t num_sub_updates_ = 0;
  bool checksum_ = false;
  shm::MessageSlot *slot_ = nullptr;
  TriggerFdList subscriber_triggers_;
  TriggerFdList retirement_triggers_;
};

} // namespace asil
} // namespace subspace
