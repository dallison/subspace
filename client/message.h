// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>

namespace subspace {
struct MessageSlot;

namespace details {
class SubscriberImpl;
}

// This is a message read by ReadMessage.  The 'length' member is the
// length of the message data in bytes and 'buffer' points to the
// start address of the message in shared memory.  If there is no message read,
// the length member will be zero and buffer will be nullptr.
// The ordinal is a monotonically increasing sequence number for all messages
// sent to the channel.
// The timestamp is the nanonsecond monotonic time when the message was
// published in memory.
//
// It is also returned by Publish but only the length, ordinal and timestamp
// members are available.  This can be used to see the information on the
// message just published.
struct ActiveMessage {
  ActiveMessage(std::shared_ptr<details::SubscriberImpl> subr, MessageSlot *s)
      : sub(std::weak_ptr<details::SubscriberImpl>(subr)), slot(s) {}
  ~ActiveMessage();

  // Can't be copied but can be moved.
  ActiveMessage(const ActiveMessage &) = delete;
  ActiveMessage &operator=(const ActiveMessage &) = delete;
  ActiveMessage(ActiveMessage &&) = default;
  ActiveMessage &operator=(ActiveMessage &&) = default;

  void Release();

  void ResetInternal() {
    length = 0;
    buffer = nullptr;
    ordinal = -1;
    timestamp = 0;
    vchan_id = -1;
    is_activation = false;
    checksum_error = false;
  }

  void Reset(size_t len, const void *buf, uint64_t ord, int64_t ts, int vid,
             bool activation, bool checksum_error);

  void IncRef() { 
    refs++;
  }
  void DecRef() {
    refs--;
    Release();
  }

  std::weak_ptr<details::SubscriberImpl>
      sub;                      // Subscriber that read the message.
  size_t length = 0;            // Length of message in bytes.
  MessageSlot *slot;            // Slot for message.
  const void *buffer = nullptr; // Address of message payload.
  uint64_t ordinal = 0;         // Monotonic number of message.
  uint64_t timestamp = 0;       // Nanosecond time message was published.
  int vchan_id = -1;            // Virtual channel ID (or -1 if not used).
  bool is_activation = false;   // Is this an activation message?
  bool checksum_error = false;  // Was there a checksum error?

  // We keep track of the number of references to this active message.  Once it
  // goes to zero we can release the slot.
  std::atomic<int> refs = 0;
};

struct Message {
  Message() = default;
  Message(size_t len, const void *buf, uint64_t ord, int64_t ts, int vid,
          bool activation, int32_t sid, bool checksum_error)
      : length(len), buffer(buf), ordinal(ord), timestamp(ts), vchan_id(vid),
        is_activation(activation), slot_id(sid),
        checksum_error(checksum_error) {}
  Message(std::shared_ptr<ActiveMessage> msg);

  // We can copy this around and move it.  It maintains a reference to the
  // active message. which has a reference count to the slot.

  ~Message() {
    if (active_message != nullptr) {
      active_message->DecRef();
    }
  }

  // Copying requires incrementing reference count for active_mesasge.
  Message(const Message &other)
      : active_message(other.active_message), length(other.length),
        buffer(other.buffer), ordinal(other.ordinal),
        timestamp(other.timestamp), vchan_id(other.vchan_id),
        is_activation(other.is_activation), slot_id(other.slot_id),
        checksum_error(other.checksum_error) {
    if (active_message != nullptr) {
      active_message->IncRef();
    }
  }

  Message &operator=(const Message &other) {
    if (active_message != nullptr) {
      active_message->DecRef();
    }
    active_message = other.active_message;
    if (active_message != nullptr) {
      active_message->IncRef();
    }
    length = other.length;
    buffer = other.buffer;
    ordinal = other.ordinal;
    timestamp = other.timestamp;
    vchan_id = other.vchan_id;
    is_activation = other.is_activation;
    slot_id = other.slot_id;
    checksum_error = other.checksum_error;
    return *this;
  }

  Message(Message &&other)
      : active_message(std::move(other.active_message)), length(other.length),
                       buffer(other.buffer), ordinal(other.ordinal),
                       timestamp(other.timestamp), vchan_id(other.vchan_id),
                       is_activation(other.is_activation),
                       slot_id(other.slot_id),
                       checksum_error(other.checksum_error) {
    other.ResetInternal();
  }

  Message &operator=(Message &&other) {
    active_message = std::move(other.active_message);
    length = other.length;
    buffer = other.buffer;
    ordinal = other.ordinal;
    timestamp = other.timestamp;
    vchan_id = other.vchan_id;
    is_activation = other.is_activation;
    slot_id = other.slot_id;
    checksum_error = other.checksum_error;
    other.ResetInternal();
    return *this;
  }

  void Reset() {
    if (active_message != nullptr) {
      active_message->DecRef();
    }
    ResetInternal();
  }

  void ResetInternal() {
    active_message.reset();
    length = 0;
    buffer = nullptr;
    ordinal = 0;
    timestamp = 0;
    vchan_id = -1;
    is_activation = false;
    checksum_error = false;
    slot_id = -1;
  }

  std::string ChannelType() const;
  int NumSlots() const;
  uint64_t SlotSize() const;

  std::shared_ptr<ActiveMessage> active_message;
  size_t length = 0;
  const void *buffer = nullptr;
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
  int vchan_id = -1;          // Virtual channel ID (or -1 if not used).
  bool is_activation = false; // Is this an activation message?
  int32_t slot_id = -1;
  bool checksum_error = false;
};

} // namespace subspace