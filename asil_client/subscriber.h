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

// The most messages that one subscriber can hold at once.
constexpr int kMaxActiveMessages = 64;

struct SubscriberOptions {
  const char *type = nullptr;
  // Verify message checksums.
  bool checksum = false;
  // Deliver a message whose checksum is wrong, with checksum_error set,
  // rather than failing the read.
  bool pass_checksum_errors = false;
  // Deliver activation messages.
  bool pass_activation = false;
  // Reliable publishers wait for this subscriber to read every message.
  bool reliable = false;
  // Subscribe to a virtual channel of this multiplexer.  vchan_id -1 lets the
  // server choose the channel's id.  To read every virtual channel, subscribe
  // to the multiplexer itself.
  const char *mux = nullptr;
  int32_t vchan_id = -1;
  // How many messages the subscriber can hold, up to kMaxActiveMessages.
  // With 1, each read releases the previous message.  With more, messages are
  // held until ReleaseMessage().
  int32_t max_active_messages = 1;
  // For a channel with split buffers: room for the channel's num_slots slot
  // mappings, and the allocator for slots that a custom allocator created.
  SplitSlot *split_slots = nullptr;
  int32_t split_slot_capacity = 0;
  SplitBufferAllocator split_allocator;
};

enum class ReadMode {
  kReadNext,
  kReadNewest,
};

// A message in shared memory.  The pointers stay valid until the message is
// released: by the next read when max_active_messages is 1, otherwise by
// ReleaseMessage(), and always by Close().
struct Message {
  const void *data = nullptr;
  size_t length = 0; // 0 when there was no message.
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
  int32_t slot_id = -1;
  // The virtual channel the message was published on, or -1.
  int32_t vchan_id = -1;
  bool is_activation = false;
  bool checksum_error = false;
  // Messages missed since the previous read on the same virtual channel, for
  // kReadNext.
  uint32_t dropped = 0;
  const void *metadata = nullptr;
  size_t metadata_length = 0;
};

// A subscriber to a fixed-size channel.
//
// A subscriber is not thread safe and must not outlive its Client.  It holds
// a table of the last ordinal seen on each virtual channel, about 8 KB.
class Subscriber {
public:
  Subscriber() = default;
  ~Subscriber() { (void)Close(); }
  Subscriber(const Subscriber &) = delete;
  Subscriber &operator=(const Subscriber &) = delete;

  Error Open(Client &client, const char *channel_name,
             const SubscriberOptions &options);
  // Removes the subscriber from the server.  Safe to call more than once.
  Error Close();
  bool IsOpen() const { return client_ != nullptr; }

  // Reads a message.  Returns kOk with message.length == 0 when there is no
  // message, and kActiveMessageLimit when the subscriber already holds
  // max_active_messages messages.  If a publisher has joined since the last
  // read this asks the server for its trigger fds first.  The channel's
  // buffers are mapped by Open(), so this maps nothing.
  Error ReadMessage(Message &message, ReadMode mode = ReadMode::kReadNext);
  // Releases every message the subscriber holds.
  void ReleaseMessage();
  // Releases one message.  Returns kInvalidArgument if the subscriber doesn't
  // hold it.
  Error ReleaseMessage(const Message &message);
  int NumActiveMessages() const { return num_held_; }

  // Readable when there may be messages to read.
  int PollFd() const { return poll_.Get(); }
  // Waits until PollFd() is readable.  A negative timeout waits forever.
  Error Wait(int timeout_ms);

  bool IsReliable() const { return options_.reliable; }
  // The subscriber's virtual channel id, or -1.
  int VirtualChannelId() const { return vchan_id_; }
  const char *Name() const { return name_; }

private:
  enum class Find { kFound, kNone };

  struct Held {
    shm::MessageSlot *slot = nullptr;
    uint64_t ordinal = 0;
  };

  SubscriberRequest BuildRequest() const;
  Error MapChannel(SubscriberReply &reply);
  Error RefreshTriggers();
  bool Visible(const shm::MessageSlot *slot) const;
  Find FindNext(shm::MessageSlot *&slot);
  Find FindNewest(shm::MessageSlot *&slot);
  bool Claim(shm::MessageSlot *slot, uint64_t ordinal);
  void ClearOlder(const shm::MessageSlot *newest);
  void ReleaseSlot(shm::MessageSlot *slot);
  void ReleaseHeld(int index);
  bool ChecksumValid(const shm::MessageSlot *slot, size_t length) const;
  void TriggerReliablePublishers() const;
  void Reset();

  Client *client_ = nullptr;
  internal::ChannelMemory memory_;
  internal::BufferContext buffer_context_;
  char name_[kMaxChannelNameLength + 1] = {};
  // The channel that owns the shared memory: the multiplexer of a virtual
  // channel.
  char resolved_name_[kMaxChannelNameLength + 1] = {};
  SubscriberOptions options_;
  int32_t channel_id_ = -1;
  int32_t subscriber_id_ = -1;
  int32_t vchan_id_ = -1;
  uint16_t num_pub_updates_ = 0;
  UniqueFd trigger_;
  UniqueFd poll_;
  Held held_[kMaxActiveMessages];
  int num_held_ = 0;
  // The newest ordinal read on each virtual channel, indexed by vchan_id + 1.
  uint64_t last_ordinals_[shm::kMaxVchanId + 1] = {};
  TriggerFdList reliable_publishers_;
  TriggerFdList retirement_triggers_;
};

} // namespace asil
} // namespace subspace
