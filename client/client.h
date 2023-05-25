// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __CLIENT_CLIENT_H
#define __CLIENT_CLIENT_H
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client/client_channel.h"
#include "client/options.h"
#include "common/channel.h"
#include "common/fd.h"
#include "common/sockets.h"
#include "common/triggerfd.h"
#include "coroutine.h"
#include <functional>
#include <sys/poll.h>

namespace subspace {

enum class ReadMode {
  kReadNext,
  kReadNewest,
};

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
struct Message {
  Message() = default;
  Message(size_t len, const void *buf, int64_t ordinal, int64_t timestamp)
      : length(len), buffer(buf), ordinal(ordinal), timestamp(timestamp) {}
  size_t length = 0;
  const void *buffer = nullptr;
  int64_t ordinal = -1;
  uint64_t timestamp = 0;
};

template <typename T> class weak_ptr;

template <typename T> class shared_ptr {
public:
  shared_ptr() = default;

  ~shared_ptr() {
    if (*this) {
      IncRefCount(-1);
    }
  }
  shared_ptr(const weak_ptr<T> &p);

  shared_ptr(const shared_ptr &p)
      : sub_(p.sub_), msg_(p.msg_), slot_(p.slot_), ordinal_(p.ordinal_) {
    IncRefCount(+1);
  }
  shared_ptr(shared_ptr &&p)
      : sub_(p.sub_), msg_(p.msg_), slot_(p.slot_), ordinal_(p.ordinal_) {
    p.reset();
  }

  shared_ptr &operator=(const shared_ptr &p) {
    if (*this != p) {
      IncRefCount(-1);
    }
    CopyFrom(p);
    return *this;
  }

  shared_ptr &operator=(shared_ptr &&p) {
    if (*this != p) {
      IncRefCount(-1);
    }
    CopyFrom(p);
    p.reset();
    return *this;
  }

  void reset() {
    sub_ = nullptr;
    slot_ = nullptr;
  }

  T *get() const { return reinterpret_cast<T *>(msg_.buffer); }
  T &operator*() const { return *reinterpret_cast<T *>(msg_.buffer); }
  T *operator->() const { return get(); }
  operator bool() const {
    return sub_ != nullptr && slot_ != nullptr && msg_.length != 0;
  }
  long use_count() const {
    return (sub_ == nullptr || slot_ == nullptr || msg_.length == 0)
               ? 0
               : slot_->ref_count;
  }

private:
  friend class Client;
  void CopyFrom(const shared_ptr &p) {
    sub_ = p.sub_;
    msg_ = p.msg_;
    slot_ = p.slot_;
    ordinal_ = p.ordinal_;
  }

  shared_ptr(Subscriber *sub, const Message &msg)
      : sub_(sub), msg_(msg), slot_(sub->CurrentSlot()),
        ordinal_(sub->CurrentOrdinal()) {
    IncRefCount(+1);
  }
  template <typename M>
  friend bool operator==(const shared_ptr<M> &p1, const shared_ptr<M> &p2);

  friend class weak_ptr<T>;

  void IncRefCount(int inc) {
    slot_->ref_count += inc;
    if (sub_->IsReliable()) {
      slot_->reliable_ref_count += inc;
    }
  }

  Subscriber *sub_ = nullptr;
  Message msg_;
  MessageSlot *slot_ = nullptr;
  int64_t ordinal_ = -1;
};

template <typename M>
bool operator==(const shared_ptr<M> &p1, const shared_ptr<M> &p2) {
  return p1.sub_ == p2.sub_ && p1.slot_ == p2.slot_ &&
         p1.ordinal_ == p2.ordinal_;
}

template <typename T> class weak_ptr {
public:
  weak_ptr(Subscriber *sub, const Message &msg, MessageSlot *slot)
      : sub_(sub), msg_(msg), slot_(slot), ordinal_(slot->ordinal) {}
  ~weak_ptr() {}
  weak_ptr(const weak_ptr &p)
      : sub_(p.sub_), msg_(p.msg_), slot_(p.slot_), ordinal_(p.ordinal_) {}
  weak_ptr(weak_ptr &&p)
      : sub_(p.sub_), msg_(p.msg_), slot_(p.slot_), ordinal_(p.ordinal_) {
    p.reset();
  }
  weak_ptr(const shared_ptr<T> &p)
      : sub_(p.sub_), msg_(p.msg_), slot_(p.slot_), ordinal_(p.ordinal_) {}

  weak_ptr &operator=(const weak_ptr &p) {
    CopyFrom(p);
    return *this;
  }

  weak_ptr &operator=(weak_ptr &&p) {
    CopyFrom(p);
    p.reset();
    return *this;
  }

  void reset() {
    sub_ = nullptr;
    slot_ = nullptr;
  }

  long use_count() const { return msg_.length == 0 ? 0 : slot_->ref_count; }

  bool expired() const {
    return !(sub_->CurrentSlot() == slot_ &&
             sub_->CurrentOrdinal() == ordinal_);
  }

  shared_ptr<T> lock() const {
    return expired() ? shared_ptr<T>() : shared_ptr<T>(*this);
  }

private:
  friend class shared_ptr<T>;

  void CopyFrom(const weak_ptr &p) {
    sub_ = p.sub_;
    msg_ = p.msg_;
    slot_ = p.slot_;
    ordinal_ = p.ordinal_;
  }

  Subscriber *sub_;
  Message msg_;
  MessageSlot *slot_;
  int64_t ordinal_;
};

template <typename T>
inline shared_ptr<T>::shared_ptr(const weak_ptr<T> &p)
    : sub_(p.sub_), msg_(p.msg_), slot_(p.slot_), ordinal_(p.ordinal_) {
  IncRefCount(+1);
}

// This is an Subspace client.  It must be initialized by calling Init() before
// it can be used.  The Init() function connects it to an Subspace server that
// is listening on the same Unix Domain Socket.
class Client {
public:
  // You can pass a Coroutine pointer to the Client and it will run inside
  // a CoroutineScheduler, sharing the CPU with all other coroutines in the
  // same scheduler.
  Client(co::Coroutine *c = nullptr) : co_(c) {}
  ~Client() = default;

  // Initialize the client by connecting to the server.
  absl::Status Init(const std::string &server_socket = "/tmp/subspace",
                    const std::string &client_name = "");

  // Create a publisher for the given channel.  If the channel doesn't exit
  // it will be created with num_slots slots, each of which is slot_size
  // bytes long.  The slots will not be resized.
  absl::StatusOr<Publisher *>
  CreatePublisher(const std::string &channel_name, int slot_size, int num_slots,
                  const PublisherOptions &opts = PublisherOptions());

  // Create a subscriber for the given channel. This can be done before there
  // are any publishers on the channel.
  absl::StatusOr<Subscriber *>
  CreateSubscriber(const std::string &channel_name,
                   const SubscriberOptions &opts = SubscriberOptions());

  // Remove publisher and subscriber.
  absl::Status RemovePublisher(Publisher *publisher);
  absl::Status RemoveSubscriber(Subscriber *subscriber);

  // Get a pointer to the message buffer for the publisher.  The publisher
  // will own this buffer until you call PublishMessage.  The idea is that
  // you fill in the buffer with the message you want to send and then
  // call PublishMessage, at which point the message will be active and
  // subscribers will be able to see it.  A reliable publisher may not
  // be able to get a message buffer if there are no free slots, in which
  // case nullptr is returned.  The publishers's PollFd can be used to
  // detect when another attempt can be made to get a buffer.
  absl::StatusOr<void *> GetMessageBuffer(Publisher *publisher);

  // Publish the message in the publisher's buffer.  The message_size
  // argument specifies the actual size of the message to send.  Returns the
  // information about the message sent with buffer set to nullptr since
  // the publisher cannot access the message once it's been published.
  absl::StatusOr<const Message> PublishMessage(Publisher *publisher,
                                               int64_t message_size);

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  absl::Status WaitForReliablePublisher(Publisher *publisher);

  // Wait until there's a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  absl::Status WaitForSubscriber(Subscriber *subscriber);

  // Read a message from a subscriber.  If there are no available messages
  // the 'length' field of the returned Message will be zero.  The 'buffer'
  // field of the Message is set to the address of the message in shared
  // memory which is read-only.  If the read is triggered by the PollFd,
  // you must read all the avaiable messages from the subscriber as the
  // PollFd is only triggered when a new message is published.
  absl::StatusOr<const Message>
  ReadMessage(Subscriber *subscriber, ReadMode mode = ReadMode::kReadNext);

  // As ReadMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>>
  ReadMessage(Subscriber *subscriber, ReadMode mode = ReadMode::kReadNext);

  // Find a message given a timestamp.
  absl::StatusOr<const Message> FindMessage(Subscriber *subscriber,
                                            uint64_t timestamp);

  // AsFindMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>> FindMessage(Subscriber *subscriber,
                                            uint64_t timestamp);

  // Gets the PollFd for a publisher and subscriber.  PollFds are only
  // available for reliable publishers but a valid pollfd will be returned for
  // an unreliable publisher (it will just never be ready)
  struct pollfd GetPollFd(Publisher *publisher);
  struct pollfd GetPollFd(Subscriber *subscriber);

  // Register a function to be called when a subscriber drops a message.  The
  // function is called with the number of messages that have been missed
  // as its second argument.
  void RegisterDroppedMessageCallback(
      Subscriber *subscriber,
      std::function<void(Subscriber *, int64_t)> callback);
  absl::Status UnregisterDroppedMessageCallback(Subscriber *subscriber);

  int64_t GetCurrentOrdinal(Subscriber *sub) const;

  const ChannelCounters &GetChannelCounters(ClientChannel *channel);

  void SetDebug(bool v) { debug_ = v; }

private:
  friend class Server;

  absl::Status CheckConnected() const;
  absl::Status SendRequestReceiveResponse(const Request &req,
                                          Response &response,
                                          std::vector<FileDescriptor> &fds);

  absl::Status ReloadSubscriber(Subscriber *channel);
  absl::Status ReloadSubscribersIfNecessary(Publisher *publisher);
  absl::Status ReloadReliablePublishersIfNecessary(Subscriber *subscriber);
  absl::Status RemoveChannel(ClientChannel *channel);
  absl::Status ActivateReliableChannel(Publisher *channel);
  absl::StatusOr<const Message> ReadMessageInternal(Subscriber *subscriber,
                                                    ReadMode mode,
                                                    bool pass_activation,
                                                    bool clear_trigger);
  absl::StatusOr<const Message> FindMessageInternal(Subscriber *subscriber,
                                                    uint64_t timestamp);
  absl::StatusOr<const Message> PublishMessageInternal(Publisher *publisher,
                                                       int64_t message_size,
                                                       bool omit_prefix);

  UnixSocket socket_;
  FileDescriptor scb_fd_;    // System control block memory fd.
  char buffer_[kMaxMessage]; // Buffer for comms with server over UDS.

  // The client owns all the publishers and subscribers.
  absl::flat_hash_set<std::unique_ptr<ClientChannel>> channels_;

  // If this is non-nullptr the client is coroutine aware and will cooperate
  // with all other coroutines to share the CPU.
  co::Coroutine *co_; // Does not own the coroutine.

  // Call this function when the given subscriber detects a dropped message.
  // This will only really happen when you have an unreliable subscriber
  // but if there's an unreliable publisher and this subscriber is reliable
  // you might see it called for messages from that publisher.
  absl::flat_hash_map<Subscriber *, std::function<void(Subscriber *, int64_t)>>
      dropped_message_callbacks_;
  bool debug_ = false;
};

// This function returns an subspace::shared_ptr that refers to the message
// just read.  The subscriber will still have a reference to the slot
// until another read is done.  The shared_ptr will keep a reference
// to the original slot until it is reset or destructed.  This will
// prevent the slot referred to by the shared_ptr from being taken
// by a publisher.  Don't hold onto shared_ptr instances long than
// you need to as it may prevent a publisher getting a slot.
template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
Client::ReadMessage(Subscriber *subscriber, ReadMode mode) {
  absl::StatusOr<Message> msg = ReadMessage(subscriber, mode);
  if (!msg.ok()) {
    return msg.status();
  }
  return ::subspace::shared_ptr<T>(subscriber, *msg);
}

template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
Client::FindMessage(Subscriber *subscriber, uint64_t timestamp) {
  absl::StatusOr<Message> msg = FindMessage(subscriber, timestamp);
  if (!msg.ok()) {
    return msg.status();
  }
  return ::subspace::shared_ptr<T>(subscriber, *msg);
}
} // namespace subspace

#endif // __CLIENT_CLIENT_H
