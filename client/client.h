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
#include "common/triggerfd.h"
#include "coroutine.h"
#include "toolbelt/fd.h"
#include "toolbelt/sockets.h"
#include <functional>
#include <string>
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
  size_t length = 0;            // Length of message in bytes.
  const void *buffer = nullptr; // Address of message payload.
  int64_t ordinal = -1;         // Monotonic number of message.
  uint64_t timestamp = 0;       // Nanosecond time message was published.
};

// Shared and weak pointers to messsages as seen by subscriber.  These
// refer to a message in a slot.  A shared_ptr maintains a reference to the
// message until it is destructed or moved to another message.  The message
// referred to by a shared_ptr will never be removed.  A weak_ptr
// is a reference to a slot that might go away.  You need to lock the
// weak_ptr, converting it to a shared_ptr before you can access the
// message.  The lock may fail if the message slot has gone away.
//
// Don't hold onto shared_ptr objects for a long time.   If you do you
// are keeping slots from being reused, thus reducing the channel
// capacity for all other publishers and subscribers.  The best strategy
// is to convert them to a weak_ptr before storing them and then,
// when you want to use the message, lock it and check that it's still
// valid.  You should design your system so that lost messages are not
// an issue.  If you really need to see all messages on a channel, use
// the reliable mode for the publshers and subscribers.
//
// The shared_ptr and weak_ptr have a very similar interface to their
// counterparts in std.  Please see the C++ documentation for how to use
// them.
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

  const Message &GetMessage() const { return msg_; }

private:
  friend class Client;
  void CopyFrom(const shared_ptr &p) {
    sub_ = p.sub_;
    msg_ = p.msg_;
    slot_ = p.slot_;
    ordinal_ = p.ordinal_;
  }

  shared_ptr(details::SubscriberImpl *sub, const Message &msg)
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
    sub_->IncDecSharedPtrCount(inc);
  }

  details::SubscriberImpl *sub_ = nullptr;
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
  weak_ptr(details::SubscriberImpl *sub, const Message &msg, MessageSlot *slot)
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
    if (expired()) {
      return shared_ptr<T>();
    }
    if (!sub_->CheckSharedPtrCount()) {
      return shared_ptr<T>();
    }
    return shared_ptr<T>(*this);
  }

private:
  friend class shared_ptr<T>;

  void CopyFrom(const weak_ptr &p) {
    sub_ = p.sub_;
    msg_ = p.msg_;
    slot_ = p.slot_;
    ordinal_ = p.ordinal_;
  }

  details::SubscriberImpl *sub_;
  Message msg_;
  MessageSlot *slot_;
  int64_t ordinal_;
};

class Publisher;
class Subscriber;

template <typename T>
inline shared_ptr<T>::shared_ptr(const weak_ptr<T> &p)
    : sub_(p.sub_), msg_(p.msg_), slot_(p.slot_), ordinal_(p.ordinal_) {
  IncRefCount(+1);
}

// This is an Subspace client.  It must be initialized by calling Init() before
// it can be used.  The Init() function connects it to an Subspace server that
// is listening on the same Unix Domain Socket.
//
// This is NOT THREAD SAFE so don't use it in a multithreaded program without
// ensuring that two threads can't access it at the same time.
//
// Why not thread safety?  It's really hard to guarantee it in anything
// except trivial programs.  Even if the interface is made thread safe,
// thus slowing everything down, you still wouldn't be able to access the
// same channel from more than one thread at the same time.  To do that,
// each thread would have to have its own publisher and subscriber.
//
// Instead, this client is aware of coroutines.  A coroutine is a way to
// share the CPU without cloning processes that share memory.  They are
// lightweight and very safe.  You don't need to use coroutines for
// the client, but if your client is in a coroutine based program, you can
// allow it to share the CPU with all other coroutines in the program.
//
// For information about the coroutine library used, please see
// https://github.com/dallison/cocpp.
class Client {
public:
  // You can pass a Coroutine pointer to the Client and it will run inside
  // a CoroutineScheduler, sharing the CPU with all other coroutines in the
  // same scheduler.  If c is nullptr, the client will not yield to
  // other coroutines when talking to the server and will block the CPU.
  Client(co::Coroutine *c = nullptr) : co_(c) {}
  ~Client() = default;

  const std::string &GetName() const { return name_; }

  // Initialize the client by connecting to the server.
  absl::Status Init(const std::string &server_socket = "/tmp/subspace",
                    const std::string &client_name = "");

  // Create a publisher for the given channel.  If the channel doesn't exit
  // it will be created with num_slots slots, each of which is slot_size
  // bytes long.
  absl::StatusOr<Publisher>
  CreatePublisher(const std::string &channel_name, int slot_size, int num_slots,
                  const PublisherOptions &opts = PublisherOptions());

  // Create a subscriber for the given channel. This can be done before there
  // are any publishers on the channel.
  absl::StatusOr<Subscriber>
  CreateSubscriber(const std::string &channel_name,
                   const SubscriberOptions &opts = SubscriberOptions());

  // Call with true to turn on some debug information.  Kind of meaningless
  // information unless you know how this works in detail.
  void SetDebug(bool v) { debug_ = v; }

private:
  friend class Server;
  friend class Publisher;
  friend class Subscriber;

  // Get a snapshot of the current number of publishers and subscribers
  // for the given channel (publisher or subscriber)
  const ChannelCounters &GetChannelCounters(details::ClientChannel *channel);

  // Remove publisher and subscriber.
  absl::Status RemovePublisher(details::PublisherImpl *publisher);
  absl::Status RemoveSubscriber(details::SubscriberImpl *subscriber);

  // Get a pointer to the message buffer for the publisher.  The publisher
  // will own this buffer until you call PublishMessage.  The idea is that
  // you fill in the buffer with the message you want to send and then
  // call PublishMessage, at which point the message will be active and
  // subscribers will be able to see it.  A reliable publisher may not
  // be able to get a message buffer if there are no free slots, in which
  // case nullptr is returned.  The publishers's PollFd can be used to
  // detect when another attempt can be made to get a buffer.
  // If max_size is greater than the current buffer size, the buffers
  // will be resized.
  absl::StatusOr<void *> GetMessageBuffer(details::PublisherImpl *publisher,
                                          int32_t max_size);

  // Publish the message in the publisher's buffer.  The message_size
  // argument specifies the actual size of the message to send.  Returns the
  // information about the message sent with buffer set to nullptr since
  // the publisher cannot access the message once it's been published.
  absl::StatusOr<const Message>
  PublishMessage(details::PublisherImpl *publisher, int64_t message_size);

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  absl::Status WaitForReliablePublisher(details::PublisherImpl *publisher);

  // Wait until there's a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  absl::Status WaitForSubscriber(details::SubscriberImpl *subscriber);

  // Read a message from a subscriber.  If there are no available messages
  // the 'length' field of the returned Message will be zero.  The 'buffer'
  // field of the Message is set to the address of the message in shared
  // memory which is read-only.  If the read is triggered by the PollFd,
  // you must read all the avaiable messages from the subscriber as the
  // PollFd is only triggered when a new message is published.
  absl::StatusOr<const Message>
  ReadMessage(details::SubscriberImpl *subscriber,
              ReadMode mode = ReadMode::kReadNext);

  // As ReadMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>>
  ReadMessage(details::SubscriberImpl *subscriber,
              ReadMode mode = ReadMode::kReadNext);

  // Find a message given a timestamp.
  absl::StatusOr<const Message> FindMessage(details::SubscriberImpl *subscriber,
                                            uint64_t timestamp);

  // AsFindMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>> FindMessage(details::SubscriberImpl *subscriber,
                                            uint64_t timestamp);

  // Gets the PollFd for a publisher and subscriber.  PollFds are only
  // available for reliable publishers but a valid pollfd will be returned for
  // an unreliable publisher (it will just never be ready)
  struct pollfd GetPollFd(details::PublisherImpl *publisher);
  struct pollfd GetPollFd(details::SubscriberImpl *subscriber);

  // Register a function to be called when a subscriber drops a message.  The
  // function is called with the number of messages that have been missed
  // as its second argument.
  void RegisterDroppedMessageCallback(
      details::SubscriberImpl *subscriber,
      std::function<void(details::SubscriberImpl *, int64_t)> callback);
  absl::Status
  UnregisterDroppedMessageCallback(details::SubscriberImpl *subscriber);

  // Get the most recently received ordinal for the subscriber.
  int64_t GetCurrentOrdinal(details::SubscriberImpl *sub) const;

  absl::Status CheckConnected() const;
  absl::Status
  SendRequestReceiveResponse(const Request &req, Response &response,
                             std::vector<toolbelt::FileDescriptor> &fds);

  absl::Status ReloadSubscriber(details::SubscriberImpl *channel);
  absl::Status ReloadSubscribersIfNecessary(details::PublisherImpl *publisher);
  absl::Status
  ReloadReliablePublishersIfNecessary(details::SubscriberImpl *subscriber);
  absl::Status RemoveChannel(details::ClientChannel *channel);
  absl::Status ActivateReliableChannel(details::PublisherImpl *channel);
  absl::StatusOr<const Message>
  ReadMessageInternal(details::SubscriberImpl *subscriber, ReadMode mode,
                      bool pass_activation, bool clear_trigger);
  absl::StatusOr<const Message>
  FindMessageInternal(details::SubscriberImpl *subscriber, uint64_t timestamp);
  absl::StatusOr<const Message>
  PublishMessageInternal(details::PublisherImpl *publisher,
                         int64_t message_size, bool omit_prefix);
  absl::Status ResizeChannel(details::PublisherImpl *publisher,
                             int32_t new_slot_size);
  absl::Status ReloadBuffersIfNecessary(details::ClientChannel *channel);

  const std::vector<BufferSet> &
  GetBuffers(details::ClientChannel *channel) const {
    return channel->GetBuffers();
  }

  std::string name_;
  toolbelt::UnixSocket socket_;
  toolbelt::FileDescriptor scb_fd_; // System control block memory fd.
  char buffer_[kMaxMessage];        // Buffer for comms with server over UDS.

  // The client owns all the publishers and subscribers.
  absl::flat_hash_set<std::unique_ptr<details::ClientChannel>> channels_;

  // If this is non-nullptr the client is coroutine aware and will cooperate
  // with all other coroutines to share the CPU.
  co::Coroutine *co_; // Does not own the coroutine.

  // Call this function when the given subscriber detects a dropped message.
  // This will only really happen when you have an unreliable subscriber
  // but if there's an unreliable publisher and this subscriber is reliable
  // you might see it called for messages from that publisher.
  absl::flat_hash_map<details::SubscriberImpl *,
                      std::function<void(details::SubscriberImpl *, int64_t)>>
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
Client::ReadMessage(details::SubscriberImpl *subscriber, ReadMode mode) {
  absl::StatusOr<Message> msg = ReadMessage(subscriber, mode);
  if (!msg.ok()) {
    return msg.status();
  }
  if (msg->length == 0) {
    return ::subspace::shared_ptr<T>(subscriber, *msg);
  }
  if (!subscriber->CheckSharedPtrCount()) {
    return absl::InternalError(
        absl::StrFormat("Too many shared pointers for %s: current: %d, max: %d",
                        subscriber->Name(), subscriber->NumSharedPtrs(),
                        subscriber->MaxSharedPtrs()));
  }
  return ::subspace::shared_ptr<T>(subscriber, *msg);
}

template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
Client::FindMessage(details::SubscriberImpl *subscriber, uint64_t timestamp) {
  absl::StatusOr<Message> msg = FindMessage(subscriber, timestamp);
  if (!msg.ok()) {
    return msg.status();
  }
  if (msg->length == 0) {
    return ::subspace::shared_ptr<T>(subscriber, *msg);
  }
  if (!subscriber->CheckSharedPtrCount()) {
    return absl::InternalError(
        absl::StrFormat("Too many shared pointers for %s: current: %d, max: %d",
                        subscriber->Name(), subscriber->NumSharedPtrs(),
                        subscriber->MaxSharedPtrs()));
  }
  return ::subspace::shared_ptr<T>(subscriber, *msg);
}

// The Publisher and Subscriber classes are the main interface for sending
// and receiving messages.  They can be moved but not copied.
class Publisher {
public:
  ~Publisher() {
    if (client_ != nullptr && impl_ != nullptr) {
      (void)client_->RemovePublisher(impl_);
    }
  }

  Publisher(const Publisher &other) = delete;
  Publisher &operator=(const Publisher &other) = delete;

  Publisher(Publisher &&other) : client_(other.client_), impl_(other.impl_) {
    other.client_ = nullptr;
    other.impl_ = nullptr;
  }

  Publisher &operator=(Publisher &&other) {
    client_ = other.client_;
    impl_ = other.impl_;
    other.client_ = nullptr;
    other.impl_ = nullptr;
    return *this;
  }

  bool operator==(const Publisher &p) const {
    return client_ == p.client_ && impl_ == p.impl_;
  }

  // Get a pointer to the message buffer for the publisher.  The publisher
  // will own this buffer until you call PublishMessage.  The idea is that
  // you fill in the buffer with the message you want to send and then
  // call PublishMessage, at which point the message will be active and
  // subscribers will be able to see it.  A reliable publisher may not
  // be able to get a message buffer if there are no free slots, in which
  // case nullptr is returned.  The publishers's PollFd can be used to
  // detect when another attempt can be made to get a buffer.
  // If max_size is greater than the current buffer size, the buffers
  // will be resized.
  absl::StatusOr<void *> GetMessageBuffer(int32_t max_size = -1) {
    return client_->GetMessageBuffer(impl_, max_size);
  }

  // Publish the message in the publisher's buffer.  The message_size
  // argument specifies the actual size of the message to send.  Returns the
  // information about the message sent with buffer set to nullptr since
  // the publisher cannot access the message once it's been published.
  absl::StatusOr<Message> PublishMessage(int64_t message_size) {
    return client_->PublishMessage(impl_, message_size);
  }

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  absl::Status Wait() { return client_->WaitForReliablePublisher(impl_); }

  struct pollfd GetPollFd() {
    return client_->GetPollFd(impl_);
  }

  const ChannelCounters &GetChannelCounters() {
    return client_->GetChannelCounters(impl_);
  }

  const std::string Type() const { return impl_->Type(); }

  bool IsReliable() const { return impl_->IsReliable(); }
  bool IsLocal() const { return impl_->IsLocal(); }
  bool IsFixedSize() const { return impl_->IsFixedSize(); }

  int32_t SlotSize() const { return impl_->SlotSize(); }

  const std::vector<BufferSet> &GetBuffers() const {
    return client_->GetBuffers(impl_);
  }

private:
  friend class Server;
  friend class Client;

  Publisher(Client *client, details::PublisherImpl *impl)
      : client_(client), impl_(impl) {}

  absl::StatusOr<const Message> PublishMessageInternal(int64_t message_size,
                                                       bool omit_prefix) {
    return client_->PublishMessageInternal(impl_, message_size, omit_prefix);
  }

  Client *client_;
  details::PublisherImpl *impl_;
};

class Subscriber {
public:
  ~Subscriber() {
    if (client_ != nullptr && impl_ != nullptr) {
      (void)client_->RemoveSubscriber(impl_);
    }
  }
  Subscriber(const Subscriber &other) = delete;

  Subscriber &operator=(const Subscriber &other) = delete;

  Subscriber(Subscriber &&other) : client_(other.client_), impl_(other.impl_) {
    other.client_ = nullptr;
    other.impl_ = nullptr;
  }

  Subscriber &operator=(Subscriber &&other) {
    client_ = other.client_;
    impl_ = other.impl_;
    other.client_ = nullptr;
    other.impl_ = nullptr;
    return *this;
  }

  bool operator==(const Subscriber &s) const {
    return client_ == s.client_ && impl_ == s.impl_;
  }

  // Wait until there's a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  absl::Status Wait() { return client_->WaitForSubscriber(impl_); }

  // Read a message from a subscriber.  If there are no available messages
  // the 'length' field of the returned Message will be zero.  The 'buffer'
  // field of the Message is set to the address of the message in shared
  // memory which is read-only.  If the read is triggered by the PollFd,
  // you must read all the avaiable messages from the subscriber as the
  // PollFd is only triggered when a new message is published.
  absl::StatusOr<const Message>
  ReadMessage(ReadMode mode = ReadMode::kReadNext) {
    return client_->ReadMessage(impl_, mode);
  }

  // As ReadMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>>
  ReadMessage(ReadMode mode = ReadMode::kReadNext);

  // Find a message given a timestamp.
  absl::StatusOr<const Message> FindMessage(uint64_t timestamp) {
    return client_->FindMessage(impl_, timestamp);
  }

  // AsFindMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>> FindMessage(uint64_t timestamp);

  struct pollfd GetPollFd() {
    return client_->GetPollFd(impl_);
  }

  int64_t GetCurrentOrdinal() const {
    return client_->GetCurrentOrdinal(impl_);
  }

  const ChannelCounters &GetChannelCounters() {
    return client_->GetChannelCounters(impl_);
  }

  const std::string Type() const { return impl_->Type(); }

  // Register a function to be called when a subscriber drops a message.  The
  // function is called with the number of messages that have been missed
  // as its second argument.
  void RegisterDroppedMessageCallback(
      std::function<void(Subscriber *, int64_t)> callback) {
    client_->RegisterDroppedMessageCallback(impl_, [
      this, callback = std::move(callback)
    ](details::SubscriberImpl * s, int64_t c) { callback(this, c); });
  }

  absl::Status UnregisterDroppedMessageCallback() {
    return client_->UnregisterDroppedMessageCallback(impl_);
  }

  const ChannelCounters &GetCounters() const { return impl_->GetCounters(); }

  int64_t CurrentOrdinal() const { return impl_->CurrentOrdinal(); }
  int64_t Timestamp() const { return impl_->Timestamp(); }
  bool IsReliable() const { return impl_->IsReliable(); }

  int32_t SlotSize() const { return impl_->SlotSize(); }

  const std::vector<BufferSet> &GetBuffers() const {
    return client_->GetBuffers(impl_);
  }

private:
  friend class Server;
  friend class Client;

  Subscriber(Client *client, details::SubscriberImpl *impl)
      : client_(client), impl_(impl) {}

  absl::StatusOr<const Message>
  ReadMessageInternal(ReadMode mode, bool pass_activation, bool clear_trigger) {
    return client_->ReadMessageInternal(impl_, mode, pass_activation,
                                        clear_trigger);
  }

  Client *client_;
  details::SubscriberImpl *impl_;
};

template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
Subscriber::ReadMessage(ReadMode mode) {
  return client_->ReadMessage<T>(impl_, mode);
}

template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
Subscriber::FindMessage(uint64_t timestamp) {
  return client_->FindMessage<T>(impl_, timestamp);
}

} // namespace subspace

#endif // __CLIENT_CLIENT_H
