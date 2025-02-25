#pragma once

#include <cstddef>
#include <cstdint>
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
  ActiveMessage() = default;
  ActiveMessage(std::shared_ptr<details::SubscriberImpl> sub, size_t len,
          MessageSlot *slot, const void *buf, uint64_t ord, int64_t ts, int vchan_id);
  ActiveMessage(size_t len, uint64_t ord, uint64_t ts, int vchan_id)
      : length(len), ordinal(ord), timestamp(ts), vchan_id(vchan_id) {}
  ~ActiveMessage();

  // Can't be copied but can be moved.
  ActiveMessage(const ActiveMessage &) = delete;
  ActiveMessage &operator=(const ActiveMessage &) = delete;
  ActiveMessage(ActiveMessage &&) = default;
  ActiveMessage &operator=(ActiveMessage &&) = default;

  void Release();

  void ResetInternal() {
    sub.reset();
    length = 0;
    slot = nullptr;
    buffer = nullptr;
    ordinal = -1;
    timestamp = 0;
    vchan_id = -1;
  }

  std::shared_ptr<details::SubscriberImpl>
      sub;                      // Subscriber that read the message.
  size_t length = 0;            // Length of message in bytes.
  MessageSlot *slot = nullptr;  // Slot for message.
  const void *buffer = nullptr; // Address of message payload.
  uint64_t ordinal = 0;         // Monotonic number of message.
  uint64_t timestamp = 0;       // Nanosecond time message was published.
  int vchan_id;                 // Virtual channel ID (or -1 if not used).
};

struct Message {
  Message() = default;
  Message(size_t len, const void *buf, uint64_t ord, int64_t ts, int vchan_id)
      : length(len), buffer(buf), ordinal(ord), timestamp(ts), vchan_id(vchan_id) {}
  Message(std::shared_ptr<ActiveMessage> msg)
      : active_message(std::move(msg)), length(active_message->length),
        buffer(active_message->buffer), ordinal(active_message->ordinal),
        timestamp(active_message->timestamp), vchan_id(active_message->vchan_id) {}
  void Release() { active_message.reset(); }
  std::shared_ptr<ActiveMessage> active_message;
  size_t length = 0;
  const void *buffer = nullptr;
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
  int vchan_id;                 // Virtual channel ID (or -1 if not used).
};

} // namespace subspace