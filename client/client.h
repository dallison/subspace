// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _CLIENT_CLIENT_H
#define _CLIENT_CLIENT_H
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client/message.h"
#include "client/options.h"
#include "client/publisher.h"
#include "client/subscriber.h"
#include "common/channel.h"
#include "coroutine.h"
#include "toolbelt/fd.h"
#include "toolbelt/logging.h"
#include "toolbelt/sockets.h"
#include "toolbelt/triggerfd.h"
#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <sys/poll.h>

namespace subspace {

enum class ReadMode {
  kReadNext,
  kReadNewest,
};

template <typename T> class weak_ptr;

template <typename T> class shared_ptr {
public:
  shared_ptr() = default;
  shared_ptr(const Message &m) : msg_(m.active_message) {}
  shared_ptr(std::shared_ptr<ActiveMessage> m) : msg_(std::move(m)) {}
  shared_ptr(const shared_ptr &p) : msg_(p.msg_) {}
  shared_ptr(shared_ptr &&p) : msg_(std::move(p.msg_)) {}
  shared_ptr(const weak_ptr<T> &p);

  shared_ptr &operator=(const shared_ptr &p) {
    msg_ = p.msg_;
    return *this;
  }

  shared_ptr &operator=(shared_ptr &&p) {
    msg_ = std::move(p.msg_);
    return *this;
  }

  subspace::Message GetMessage() const { return Message(msg_); }

  T *get() const { return reinterpret_cast<T *>(msg_->buffer); }
  T &operator*() const { return *reinterpret_cast<T *>(msg_->buffer); }
  T *operator->() const { return get(); }
  operator bool() const { return msg_ != nullptr; }
  long use_count() const {
    return msg_.use_count() == 0
               ? 0
               : msg_.use_count() -
                     1; // Subscriber has a reference that we shouldn't count
  }
  void reset() { msg_.reset(); }

  bool operator==(const shared_ptr &p) const { return msg_ == p.msg_; }
  bool operator!=(const shared_ptr &p) const { return msg_ != p.msg_; }

private:
  template <typename M> friend class weak_ptr;

  std::shared_ptr<ActiveMessage> msg_;
};

template <typename T> class weak_ptr {
public:
  weak_ptr() = default;
  weak_ptr(const shared_ptr<T> &p)
      : sub_(p.msg_->sub), slot_(p.msg_->slot), ordinal_(p.msg_->ordinal) {}
  weak_ptr(const weak_ptr &p)
      : sub_(p.sub_), slot_(p.slot_), ordinal_(p.ordinal_) {}
  weak_ptr(weak_ptr &&p)
      : sub_(std::move(p.sub_)), slot_(p.slot_), ordinal_(p.ordinal_) {
    p.slot_ = nullptr;
    p.ordinal_ = 0;
  }

  weak_ptr &operator=(const weak_ptr &p) {
    sub_ = p.sub_;
    slot_ = p.slot_;
    ordinal_ = p.ordinal_;
    return *this;
  }

  weak_ptr &operator=(weak_ptr &&p) {
    sub_ = std::move(p.sub_);
    slot_ = p.slot_;
    ordinal_ = p.ordinal_;
    p.slot_ = nullptr;
    p.ordinal_ = 0;
    return *this;
  }

  shared_ptr<T> lock() const {
    if (sub_ == nullptr) {
      return shared_ptr<T>();
    }
    return sub_->LockWeakMessage(slot_, ordinal_);
  }

  bool expired() const {
    if (sub_ == nullptr || slot_ == nullptr) {
      return true;
    }
    return sub_->SlotExpired(slot_, ordinal_);
  }

  void reset() {
    sub_.reset();
    slot_ = nullptr;
    ordinal_ = 0;
  }

  bool operator==(const weak_ptr &p) const {
    return sub_ == p.sub_ && slot_ == p.slot_ && ordinal_ == p.ordinal_;
  }

  bool operator!=(const weak_ptr &p) const {
    return sub_ != p.sub_ || slot_ != p.slot_ || ordinal_ != p.ordinal_;
  }

private:
  template <typename M> friend class shared_ptr;

  std::shared_ptr<details::SubscriberImpl> sub_;
  MessageSlot *slot_ = nullptr;
  uint64_t ordinal_ = 0;
};

template <typename T>
inline shared_ptr<T>::shared_ptr(const weak_ptr<T> &p) : msg_(p.lock().msg_) {}

class Publisher;
class Subscriber;

// This is an Subspace client.  It must be initialized by calling Init()
// before it can be used.  The Init() function connects it to an Subspace
// server that is listening on the same Unix Domain Socket.
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
// https://github.com/dallison/co.
class ClientImpl : public std::enable_shared_from_this<ClientImpl> {
public:
  // You can pass a Coroutine pointer to the Client and it will run inside
  // a CoroutineScheduler, sharing the CPU with all other coroutines in the
  // same scheduler.  If c is nullptr, the client will not yield to
  // other coroutines when talking to the server and will block the CPU.
  //
  // These are public so that they can be accessed by std::make_shared.
  // You shouldn't create these yourself - create a Client instead.
  ClientImpl(co::Coroutine *c = nullptr) : co_(c) {}
  ~ClientImpl() = default;

private:
  friend class Client;
  friend class Server;
  friend class Publisher;
  friend class Subscriber;

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

  // Create a publisher with the slot size and number of slots set in the
  // options.
  absl::StatusOr<Publisher>
  CreatePublisher(const std::string &channel_name,
                  const PublisherOptions &opts = PublisherOptions());

  // Create a subscriber for the given channel. This can be done before there
  // are any publishers on the channel.
  absl::StatusOr<Subscriber>
  CreateSubscriber(const std::string &channel_name,
                   const SubscriberOptions &opts = SubscriberOptions());

  // Call with true to turn on some debug information.  Kind of meaningless
  // information unless you know how this works in detail.
  void SetDebug(bool v) { debug_ = v; }

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
  absl::Status WaitForReliablePublisher(details::PublisherImpl *publisher,
                                        co::Coroutine *c = nullptr) {
    return WaitForReliablePublisher(publisher, std::chrono::nanoseconds(0), c);
  }

  absl::Status WaitForReliablePublisher(details::PublisherImpl *publisher,
                                        std::chrono::nanoseconds timeout,
                                        co::Coroutine *c = nullptr);

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  absl::StatusOr<int>
  WaitForReliablePublisher(details::PublisherImpl *publisher,
                           const toolbelt::FileDescriptor &fd,
                           co::Coroutine *c = nullptr) {
    return WaitForReliablePublisher(publisher, fd, std::chrono::nanoseconds(0),
                                    c);
  }

  absl::StatusOr<int> WaitForReliablePublisher(
      details::PublisherImpl *publisher, const toolbelt::FileDescriptor &fd,
      std::chrono::nanoseconds timeout, co::Coroutine *c = nullptr);

  // Wait until there's a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  absl::Status WaitForSubscriber(details::SubscriberImpl *subscriber,
                                 co::Coroutine *c = nullptr) {
    return WaitForSubscriber(subscriber, std::chrono::nanoseconds(0), c);
  }

  absl::Status WaitForSubscriber(details::SubscriberImpl *subscriber,
                                 std::chrono::nanoseconds timeout,
                                 co::Coroutine *c = nullptr);

  // Wait until there' s a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  absl::StatusOr<int> WaitForSubscriber(details::SubscriberImpl *subscriber,
                                        const toolbelt::FileDescriptor &fd,
                                        co::Coroutine *c = nullptr) {
    return WaitForSubscriber(subscriber, fd, std::chrono::nanoseconds(0), c);
  }

  absl::StatusOr<int> WaitForSubscriber(details::SubscriberImpl *subscriber,
                                        const toolbelt::FileDescriptor &fd,
                                        std::chrono::nanoseconds timeout,
                                        co::Coroutine *c = nullptr);

  // Read a message from a subscriber.  If there are no available messages
  // the 'length' field of the returned Message will be zero.  The 'buffer'
  // field of the Message is set to the address of the message in shared
  // memory which is read-only.  If the read is triggered by the PollFd,
  // you must read all the avaiable messages from the subscriber as the
  // PollFd is only triggered when a new message is published.
  absl::StatusOr<Message> ReadMessage(details::SubscriberImpl *subscriber,
                                      ReadMode mode = ReadMode::kReadNext);

  // As ReadMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>>
  ReadMessage(details::SubscriberImpl *subscriber,
              ReadMode mode = ReadMode::kReadNext);

  // Find a message given a timestamp.
  absl::StatusOr<Message> FindMessage(details::SubscriberImpl *subscriber,
                                      uint64_t timestamp);
  // AsFindMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>> FindMessage(details::SubscriberImpl *subscriber,
                                            uint64_t timestamp);

  // Gets the PollFd for a publisher and subscriber.  PollFds are only
  // available for reliable publishers but a valid pollfd will be returned for
  // an unreliable publisher (it will just never be ready)
  struct pollfd GetPollFd(details::PublisherImpl *publisher) const;
  struct pollfd GetPollFd(details::SubscriberImpl *subscriber) const;

  toolbelt::FileDescriptor
  GetFileDescriptor(details::PublisherImpl *publisher) const;
  toolbelt::FileDescriptor
  GetFileDescriptor(details::SubscriberImpl *subscriber) const;

  // Register a function to be called when a subscriber drops a message.  The
  // function is called with the number of messages that have been missed
  // as its second argument.
  absl::Status RegisterDroppedMessageCallback(
      details::SubscriberImpl *subscriber,
      std::function<void(details::SubscriberImpl *, int64_t)> callback);
  absl::Status
  UnregisterDroppedMessageCallback(details::SubscriberImpl *subscriber);

  absl::Status RegisterMessageCallback(
      details::SubscriberImpl *subscriber,
      std::function<void(details::SubscriberImpl *, Message)> callback);
  absl::Status UnregisterMessageCallback(details::SubscriberImpl *subscriber);

  // Register a callback that will be called when the publisher wants a
  // channel to be resized.  Note that there is more than one
  // publisher, only the one that causes the resize will cause the
  // callback to be called.  The arguments to the callback are:
  // 1. Publisher
  // 2. old size (bytes)
  // 3. new size (bytes)
  //
  // Returns absl::OkStatus() if resize is OK.  If the callback wants
  // to prevent the resize from happening, return an error.
  absl::Status RegisterResizeCallback(
      details::PublisherImpl *publisher,
      std::function<absl::Status(details::PublisherImpl *, int32_t, int32_t)>
          cb);
  absl::Status UnregisterResizeCallback(details::PublisherImpl *publisher);

  // Call the message callback function for all available messages.  You must
  // have registered a callback function with RegisterMessageCallback for this
  // to work.
  absl::Status ProcessAllMessages(details::SubscriberImpl *subscriber,
                                  ReadMode mode = ReadMode::kReadNext);

  absl::StatusOr<std::vector<Message>>
  GetAllMessages(details::SubscriberImpl *subscriber,
                 ReadMode mode = ReadMode::kReadNext);

  // Get the most recently received ordinal for the subscriber.
  int64_t GetCurrentOrdinal(details::SubscriberImpl *sub) const;

  absl::Status CheckConnected() const;
  absl::Status
  SendRequestReceiveResponse(const Request &req, Response &response,
                             std::vector<toolbelt::FileDescriptor> &fds);

  bool CheckReload(details::ClientChannel *channel);

  absl::Status ReloadSubscriber(details::SubscriberImpl *channel);
  absl::Status ReloadSubscribersIfNecessary(details::PublisherImpl *publisher);
  absl::Status
  ReloadReliablePublishersIfNecessary(details::SubscriberImpl *subscriber);
  absl::Status RemoveChannel(details::ClientChannel *channel);
  absl::Status ActivateReliableChannel(details::PublisherImpl *channel);
  absl::Status ActivateChannel(details::PublisherImpl *channel);
  absl::StatusOr<Message>
  ReadMessageInternal(details::SubscriberImpl *subscriber, ReadMode mode,
                      bool pass_activation, bool clear_trigger);
  absl::StatusOr<Message>
  FindMessageInternal(details::SubscriberImpl *subscriber, uint64_t timestamp);
  absl::StatusOr<const Message>
  PublishMessageInternal(details::PublisherImpl *publisher,
                         int64_t message_size, bool omit_prefix);
  absl::Status ResizeChannel(details::PublisherImpl *publisher,
                             int32_t new_slot_size);
  absl::StatusOr<bool>
  ReloadBuffersIfNecessary(details::ClientChannel *channel);

  const std::vector<std::unique_ptr<details::BufferSet>> &
  GetBuffers(details::ClientChannel *channel) const {
    return channel->GetBuffers();
  }

  std::string name_;
  std::string socket_name_;
  uint64_t session_id_ = 0;

  toolbelt::UnixSocket socket_;
  toolbelt::FileDescriptor scb_fd_; // System control block memory fd.
  char buffer_[kMaxMessage];        // Buffer for comms with server over UDS.

  // The client owns all the publishers and subscribers.
  absl::flat_hash_set<std::shared_ptr<details::ClientChannel>> channels_;

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

  // Callback per subscriber to call when a message is received.  Use the
  // `ProcessAllMessages` function on the Subscriber to call this function for
  // all currently available messages.
  absl::flat_hash_map<details::SubscriberImpl *,
                      std::function<void(details::SubscriberImpl *, Message)>>
      message_callbacks_;

  // Call the function when a publisher causes a channel to be resized.
  absl::flat_hash_map<
      details::PublisherImpl *,
      std::function<absl::Status(details::PublisherImpl *, int32_t, int32_t)>>
      resize_callbacks_;
  bool debug_ = false;
  toolbelt::Logger logger_;
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
ClientImpl::ReadMessage(details::SubscriberImpl *subscriber, ReadMode mode) {
  absl::StatusOr<Message> msg = ReadMessage(subscriber, mode);
  if (!msg.ok()) {
    return msg.status();
  }
  if (msg->length == 0) {
    return ::subspace::shared_ptr<T>();
  }
  return ::subspace::shared_ptr<T>(std::move(*msg));
}

template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
ClientImpl::FindMessage(details::SubscriberImpl *subscriber,
                        uint64_t timestamp) {
  absl::StatusOr<Message> msg = FindMessage(subscriber, timestamp);
  if (!msg.ok()) {
    return msg.status();
  }
  return ::subspace::shared_ptr<T>(std::move(*msg));
}

// The Publisher and Subscriber classes are the main interface for sending
// and receiving messages.  They can be moved but not copied.
class Publisher {
public:
  ~Publisher() {
    if (client_ != nullptr && impl_ != nullptr) {
      (void)client_->RemovePublisher(impl_.get());
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
    return client_->GetMessageBuffer(impl_.get(), max_size);
  }

  // Publish the message in the publisher's buffer.  The message_size
  // argument specifies the actual size of the message to send.  Returns the
  // information about the message sent with buffer set to nullptr since
  // the publisher cannot access the message once it's been published.
  absl::StatusOr<const Message> PublishMessage(int64_t message_size) {
    return client_->PublishMessage(impl_.get(), message_size);
  }

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  absl::Status Wait(co::Coroutine *c = nullptr) {
    return client_->WaitForReliablePublisher(impl_.get(), c);
  }

  absl::Status Wait(std::chrono::nanoseconds timeout,
                    co::Coroutine *c = nullptr) {
    return client_->WaitForReliablePublisher(impl_.get(), timeout, c);
  }

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  // This also takes an additional file descriptor that can be used to interrupt
  // the wait.  Returns the integer fd value of the file descriptor that
  // triggered the wait.
  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           co::Coroutine *c = nullptr) {
    return client_->WaitForReliablePublisher(impl_.get(), fd, c);
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           std::chrono::nanoseconds timeout,
                           co::Coroutine *c = nullptr) {
    return client_->WaitForReliablePublisher(impl_.get(), fd, timeout, c);
  }

  struct pollfd GetPollFd() const { return client_->GetPollFd(impl_.get()); }

  // This is a file descriptor that you can poll on to wait for
  // message slots to be retired.  It is triggered
  const toolbelt::FileDescriptor &GetRetirementFd() const {
    return impl_->GetRetirementFd();
  }

  toolbelt::FileDescriptor GetFileDescriptor() const {
    return client_->GetFileDescriptor(impl_.get());
  }

  const ChannelCounters &GetChannelCounters() {
    return client_->GetChannelCounters(impl_.get());
  }

  std::string Name() const { return impl_->Name(); }
  std::string Type() const { return impl_->Type(); }
  std::string_view TypeView() const { return impl_->TypeView(); }

  void DumpSlots(std::ostream &os) const { impl_->DumpSlots(os); }

  bool IsReliable() const { return impl_->IsReliable(); }
  bool IsLocal() const { return impl_->IsLocal(); }
  bool IsFixedSize() const { return impl_->IsFixedSize(); }

  int32_t SlotSize() const { return impl_->SlotSize(); }
  int32_t NumSlots() const { return impl_->NumSlots(); }

  const std::vector<std::unique_ptr<details::BufferSet>> &GetBuffers() const {
    return client_->GetBuffers(impl_.get());
  }

  // Register a function to be called when the publisher resizes
  // the channel.
  absl::Status RegisterResizeCallback(
      std::function<absl::Status(Publisher *, int, int)> callback) {
    return client_->RegisterResizeCallback(
        impl_.get(),
        [this, callback = std::move(callback)](
            details::PublisherImpl *, int32_t old_size, int32_t new_size)
            -> absl::Status { return callback(this, old_size, new_size); });
  }

  absl::Status UnregisterResizeCallback() {
    return client_->UnregisterResizeCallback(impl_.get());
  }

  int VirtualChannelId() const { return impl_->VirtualChannelId(); }

  int NumSubscribers(int vchan_id = -1) const {
    return impl_->NumSubscribers(vchan_id);
  }

private:
  friend class Server;
  friend class ClientImpl;

  Publisher(std::shared_ptr<ClientImpl> client,
            std::shared_ptr<details::PublisherImpl> impl)
      : client_(client), impl_(impl) {}

  absl::StatusOr<const Message> PublishMessageInternal(int64_t message_size,
                                                       bool omit_prefix) {
    return client_->PublishMessageInternal(impl_.get(), message_size,
                                           omit_prefix);
  }

  std::shared_ptr<ClientImpl> client_;
  std::shared_ptr<details::PublisherImpl> impl_;
};

class Subscriber {
public:
  ~Subscriber() {
    if (client_ != nullptr && impl_ != nullptr) {
      (void)client_->RemoveSubscriber(impl_.get());
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
  absl::Status Wait(co::Coroutine *c = nullptr) {
    return client_->WaitForSubscriber(impl_.get(), c);
  }

  absl::Status Wait(std::chrono::nanoseconds timeout,
                    co::Coroutine *c = nullptr) {
    return client_->WaitForSubscriber(impl_.get(), timeout, c);
  }

  // Wait until there's a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  // This also takes an additional file descriptor that can be used to interrupt
  // the wait.  Returns the integer fd value of the file descriptor that
  // triggered the wait.
  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           co::Coroutine *c = nullptr) {
    return client_->WaitForSubscriber(impl_.get(), fd, c);
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           std::chrono::nanoseconds timeout,
                           co::Coroutine *c = nullptr) {
    return client_->WaitForSubscriber(impl_.get(), fd, timeout, c);
  }

  // Read a message from a subscriber.  If there are no available messages
  // the 'length' field of the returned Message will be zero.  The 'buffer'
  // field of the Message is set to the address of the message in shared
  // memory which is read-only.  If the read is triggered by the PollFd,
  // you must read all the avaiable messages from the subscriber as the
  // PollFd is only triggered when a new message is published.
  absl::StatusOr<Message> ReadMessage(ReadMode mode = ReadMode::kReadNext) {
    return client_->ReadMessage(impl_.get(), mode);
  }

  // As ReadMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>>
  ReadMessage(ReadMode mode = ReadMode::kReadNext);

  // Find a message given a timestamp.
  absl::StatusOr<Message> FindMessage(uint64_t timestamp) {
    return client_->FindMessage(impl_.get(), timestamp);
  }

  // AsFindMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T>
  absl::StatusOr<shared_ptr<T>> FindMessage(uint64_t timestamp);

  struct pollfd GetPollFd() const { return client_->GetPollFd(impl_.get()); }

  toolbelt::FileDescriptor GetFileDescriptor() const {
    return client_->GetFileDescriptor(impl_.get());
  }

  int64_t GetCurrentOrdinal() const {
    return client_->GetCurrentOrdinal(impl_.get());
  }

  const ChannelCounters &GetChannelCounters() {
    return client_->GetChannelCounters(impl_.get());
  }

  std::string Name() const { return impl_->Name(); }
  std::string Type() const { return impl_->Type(); }
  std::string_view TypeView() const { return impl_->TypeView(); }

  // Register a function to be called when a subscriber drops a message.  The
  // function is called with the number of messages that have been missed
  // as its second argument.
  absl::Status RegisterDroppedMessageCallback(
      std::function<void(Subscriber *, int64_t)> callback) {
    return client_->RegisterDroppedMessageCallback(
        impl_.get(),
        [this, callback = std::move(callback)](
            details::SubscriberImpl *, int64_t c) { callback(this, c); });
  }

  absl::Status UnregisterDroppedMessageCallback() {
    return client_->UnregisterDroppedMessageCallback(impl_.get());
  }

  absl::Status
  RegisterMessageCallback(std::function<void(Subscriber *, Message)> callback) {
    return client_->RegisterMessageCallback(
        impl_.get(), [this, callback = std::move(callback)](
                         details::SubscriberImpl *, Message m) {
          callback(this, std::move(m));
        });
  }

  absl::Status UnregisterMessageCallback() {
    return client_->UnregisterMessageCallback(impl_.get());
  }

  absl::Status ProcessAllMessages(ReadMode mode = ReadMode::kReadNext) {
    return client_->ProcessAllMessages(impl_.get(), mode);
  }

  absl::StatusOr<std::vector<Message>>
  GetAllMessages(ReadMode mode = ReadMode::kReadNext) {
    return client_->GetAllMessages(impl_.get(), mode);
  }

  void Trigger() { impl_->Trigger(); }

  const ChannelCounters &GetCounters() const { return impl_->GetCounters(); }

  int64_t CurrentOrdinal() const { return impl_->CurrentOrdinal(); }
  int64_t Timestamp() const { return impl_->Timestamp(); }
  bool IsReliable() const { return impl_->IsReliable(); }

  int32_t SlotSize() const { return impl_->SlotSize(); }
  int32_t NumSlots() const { return impl_->NumSlots(); }

  const std::vector<std::unique_ptr<details::BufferSet>> &GetBuffers() const {
    return client_->GetBuffers(impl_.get());
  }

  int NumActiveMessages() const { return impl_->NumActiveMessages(); }

  void DumpSlots(std::ostream &os) const { impl_->DumpSlots(os); }

  int VirtualChannelId() const { return impl_->VirtualChannelId(); }

  int NumSubscribers(int vchan_id = -1) const {
    return impl_->NumSubscribers(vchan_id);
  }

  // If you don't want to hold on to the current active message in the
  // subscriber, you can call this.
  void ClearActiveMessage() { impl_->ClearActiveMessage(); }

private:
  friend class Server;
  friend class ClientImpl;

  Subscriber(std::shared_ptr<ClientImpl> client,
             std::shared_ptr<details::SubscriberImpl> impl)
      : client_(client), impl_(impl) {}

  absl::StatusOr<Message>
  ReadMessageInternal(ReadMode mode, bool pass_activation, bool clear_trigger) {
    return client_->ReadMessageInternal(impl_.get(), mode, pass_activation,
                                        clear_trigger);
  }

  std::shared_ptr<ClientImpl> client_;
  std::shared_ptr<details::SubscriberImpl> impl_;
};

template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
Subscriber::ReadMessage(ReadMode mode) {
  return client_->ReadMessage<T>(impl_.get(), mode);
}

template <typename T>
inline absl::StatusOr<::subspace::shared_ptr<T>>
Subscriber::FindMessage(uint64_t timestamp) {
  return client_->FindMessage<T>(impl_.get(), timestamp);
}

// This is a wrapper around the ClientImpl that is created as a shared_ptr
// to provide lifetime management with respect to Publisher, Subscriber and
// subspace::shared_ptr objects.
// You can create this anywhere you like and it will automatically allocate
// a std::shared_ptr for the ClientImpl.  Since the ClientImpl won't move
// an instance of this class may be moved around without affecting
// anything referring to the ClientImpl.
//
// You can also copy the object, which isn't something you can do with
// ClientImpl.
class Client {
public:
  static absl::StatusOr<std::shared_ptr<Client>>
  Create(const std::string &server_socket = "/tmp/subspace",
         const std::string &client_name = "", co::Coroutine *c = nullptr) {
    auto client = std::make_shared<Client>(c);
    auto status = client->Init(server_socket, client_name);
    if (!status.ok()) {
      return status;
    }
    return client;
  }

  Client(co::Coroutine *c = nullptr) : impl_(std::make_shared<ClientImpl>(c)) {}
  ~Client() = default;

  const std::string &GetName() const { return impl_->GetName(); }

  // Initialize the client by connecting to the server.
  absl::Status Init(const std::string &server_socket = "/tmp/subspace",
                    const std::string &client_name = "") {
    return impl_->Init(server_socket, client_name);
  }

  // Create a publisher for the given channel.  If the channel doesn't exit
  // it will be created with num_slots slots, each of which is slot_size
  // bytes long.
  absl::StatusOr<Publisher>
  CreatePublisher(const std::string &channel_name, int slot_size, int num_slots,
                  const PublisherOptions &opts = PublisherOptions()) {
    return impl_->CreatePublisher(channel_name, slot_size, num_slots, opts);
  }

  // If you prefer, you can create the publisher with the slot size and
  // number of slots set in the options.
  absl::StatusOr<Publisher>
  CreatePublisher(const std::string &channel_name,
                  const PublisherOptions &opts = PublisherOptions()) {
    return impl_->CreatePublisher(channel_name, opts);
  }

  // Create a subscriber for the given channel. This can be done before there
  // are any publishers on the channel.
  absl::StatusOr<Subscriber>
  CreateSubscriber(const std::string &channel_name,
                   const SubscriberOptions &opts = SubscriberOptions()) {
    return impl_->CreateSubscriber(channel_name, opts);
  }

  // Call with true to turn on some debug information.  Kind of meaningless
  // information unless you know how this works in detail.
  void SetDebug(bool v) { impl_->SetDebug(v); }

private:
  std::shared_ptr<ClientImpl> impl_;
};

} // namespace subspace

#endif // _CLIENT_CLIENT_H
