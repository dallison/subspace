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

struct SubscriberOptions {
  const char *type = nullptr;
  // Verify message checksums.
  bool checksum = false;
  // Deliver a message whose checksum is wrong, with checksum_error set,
  // rather than failing the read.
  bool pass_checksum_errors = false;
  // Deliver activation messages.
  bool pass_activation = false;
};

enum class ReadMode {
  kReadNext,
  kReadNewest,
};

// A message in shared memory.  The pointers stay valid until the next read,
// ReleaseMessage() or Close().
struct Message {
  const void *data = nullptr;
  size_t length = 0; // 0 when there was no message.
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
  int32_t slot_id = -1;
  bool is_activation = false;
  bool checksum_error = false;
  // Messages missed since the previous read, for kReadNext.
  uint32_t dropped = 0;
  const void *metadata = nullptr;
  size_t metadata_length = 0;
};

// An unreliable subscriber.  It holds at most one message; reading the next
// message releases the previous one.
//
// A subscriber is not thread safe and must not outlive its Client.
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
  // message.  If a publisher has joined since the last read this asks the
  // server for its trigger fds first.
  Error ReadMessage(Message &message, ReadMode mode = ReadMode::kReadNext);
  // Releases the message from the last read.
  void ReleaseMessage();

  // Readable when there may be messages to read.
  int PollFd() const { return poll_.Get(); }
  // Waits until PollFd() is readable.  A negative timeout waits forever.
  Error Wait(int timeout_ms);

  const char *Name() const { return name_; }

private:
  enum class Find { kFound, kNone };

  Error RefreshTriggers();
  Find FindNext(shm::MessageSlot *&slot);
  Find FindNewest(shm::MessageSlot *&slot);
  void ClearOlder(const shm::MessageSlot *newest);
  void ReleaseSlot(shm::MessageSlot *slot);
  bool ChecksumValid(const shm::MessageSlot *slot, size_t length) const;
  void TriggerReliablePublishers() const;
  void Reset();

  Client *client_ = nullptr;
  internal::ChannelMemory memory_;
  char name_[kMaxChannelNameLength + 1] = {};
  SubscriberOptions options_;
  int32_t channel_id_ = -1;
  int32_t subscriber_id_ = -1;
  uint16_t num_pub_updates_ = 0;
  UniqueFd trigger_;
  UniqueFd poll_;
  shm::MessageSlot *held_ = nullptr;
  uint64_t last_ordinal_ = 0;
  TriggerFdList reliable_publishers_;
  TriggerFdList retirement_triggers_;
};

} // namespace asil
} // namespace subspace
