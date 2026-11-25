// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xCLIENT_CLIENT_H
#define _xCLIENT_CLIENT_H
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

#include "client/message.h"
#include "client/options.h"
#include "client/publisher.h"
#include "client/subscriber.h"
#include "co/coroutine.h"
#include "common/async/context.h"
#include "common/async/socket.h"
#include "common/async/task.h"
#include "common/async/wait.h"
#include "common/channel.h"
#include "common/client_buffer.h"
#include "proto/subspace.pb.h"

#include "toolbelt/async_waiter.h"
#include "toolbelt/fd.h"
#include "toolbelt/logging.h"
#include "toolbelt/sockets.h"
#include "toolbelt/triggerfd.h"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <pthread.h>
#include <string>
#include <sys/poll.h>
#include <thread>
namespace subspace {

#if defined(__ANDROID__)
constexpr const char *kDefaultServerSocket = "/data/local/tmp/subspace";
#else
constexpr const char *kDefaultServerSocket = "/tmp/subspace";
#endif

#ifndef __has_cpp_attribute
#define __has_cpp_attribute(attribute) 0
#endif

#if __has_cpp_attribute(no_unique_address) && !defined(__QNX__)
#define SUBSPACE_NO_UNIQUE_ADDRESS [[no_unique_address]]
#else
#define SUBSPACE_NO_UNIQUE_ADDRESS
#endif

enum class ReadMode {
  kReadNext,
  kReadNewest,
};

// Controls whether ReadMessage consumes the subscriber trigger fd (eventfd
// or pipe). The default, kClearTrigger, reads the fd so a later poll/Wait
// blocks until a new message is published. kNoClearTrigger leaves the fd
// unread, which is useful when the caller is managing it from an external
// event loop.
enum class ClearTrigger {
  kClearTrigger,
  kNoClearTrigger,
};

struct ChannelInfo {
  std::string channel_name;
  int num_publishers;
  int num_subscribers;
  int num_bridge_pubs;
  int num_bridge_subs;
  int num_tunnel_pubs;
  int num_tunnel_subs;
  std::string type;
  uint64_t slot_size;
  int num_slots;
  int subscriber_queue_size;
  uint64_t subscriber_queue_arena_size;
  bool reliable;
  bool is_local = false;
};

struct ChannelStats {
  std::string channel_name;
  uint64_t total_bytes;
  uint64_t total_messages;
  uint64_t max_message_size;
  bool is_local = false;
};

struct DefaultAliaser {
  void Set(const void * /*buffer*/, size_t /*length*/) {}
  void Reset() {}
  template <typename T>
  void AliasTo(const void *buffer, size_t length, T *&dest) const {
    assert(length >= sizeof(T));
    static_assert(std::is_standard_layout_v<T>,
                  "Default aliasing requires a standard layout type! Casting "
                  "is undefined behavior!");
    dest = reinterpret_cast<T *>(buffer);
  }
};

// The shared_ptr / weak_ptr combination allows you to keep pointers with
// the standard semantics into an active message slot that was recently read
// from a channel subscriber. The pointer type `T` must be aliasable in the
// raw shared-memory of the active slot (and, of course, `const`).
// By default, the aliasing is done with reinterpret_cast, which only works
// if `T` is a standard-layout type (e.g., C-struct, primitive type, etc.).
// An aliaser type can be provided to customize how this works, for example,
// to accommodate a complex zero-copy type. An aliaser type should provide
// the functions seen in DefaultAliaser, that is:
//  - Be default-constructible
//  - Have a non-const `Set` function to initialize the aliased object for
//    a given buffer (if needed, e.g., like finding fields and offsets)
//  - Have a `Reset` function to forget whatever was setup in `Set`
//  - Have a const `AliasTo` function that can deliver a valid `T*` pointer
//    that points to an object that is aliasing the given memory
// See TestAliasedMessageAliaser in client_test.cc as an example.

template <typename T, typename Aliaser = DefaultAliaser> class weak_ptr;

template <typename T, typename Aliaser = DefaultAliaser> class shared_ptr {
public:
  shared_ptr() = default;
  shared_ptr(std::shared_ptr<ActiveMessage> m) : msg_(std::move(m)) {
    if (msg_ != nullptr) {
      msg_->IncRef();
      aliaser_.Set(msg_->buffer, msg_->length);
    }
  }
  shared_ptr(const Message &m) : shared_ptr(m.active_message) {}
  shared_ptr(const weak_ptr<T, Aliaser> &p);

  shared_ptr(const shared_ptr &p) : msg_(p.msg_), aliaser_(p.aliaser_) {
    if (msg_ != nullptr) {
      msg_->IncRef();
    }
  }
  shared_ptr &operator=(const shared_ptr &p) {
    msg_ = p.msg_;
    aliaser_ = p.aliaser_;
    if (msg_ != nullptr) {
      msg_->IncRef();
    }
    return *this;
  }

  shared_ptr(shared_ptr &&p) noexcept = default;
  shared_ptr &operator=(shared_ptr &&p) noexcept = default;

  ~shared_ptr() {
    if (msg_ != nullptr) {
      aliaser_.Reset();
      msg_->DecRef();
    }
  }
  subspace::Message GetMessage() const { return Message(msg_); }

  T *get() const {
    if (msg_ == nullptr) {
      return nullptr;
    }
    T *result = nullptr;
    aliaser_.AliasTo(msg_->buffer, msg_->length, result);
    return result;
  }
  T &operator*() const { return *get(); }
  T *operator->() const { return get(); }
  operator bool() const { return msg_ != nullptr; }
  long use_count() const { return msg_ == nullptr ? 0 : msg_->refs.load(); }
  void reset() {
    if (msg_ != nullptr) {
      aliaser_.Reset();
      msg_->DecRef();
    }
    msg_ = nullptr;
  }

  bool operator==(const shared_ptr &p) const { return msg_ == p.msg_; }
  bool operator!=(const shared_ptr &p) const { return msg_ != p.msg_; }

private:
  template <typename M, typename OtherAliaser> friend class weak_ptr;

  std::shared_ptr<ActiveMessage> msg_;
  SUBSPACE_NO_UNIQUE_ADDRESS Aliaser aliaser_;
};

template <typename T, typename Aliaser> class weak_ptr {
public:
  weak_ptr() = default;
  weak_ptr(const shared_ptr<T, Aliaser> &p)
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
  template <typename M, typename OtherAliaser> friend class shared_ptr;

  std::shared_ptr<details::SubscriberImpl> sub_;
  MessageSlot *slot_ = nullptr;
  uint64_t ordinal_ = 0;
};

template <typename T, typename Aliaser>
inline shared_ptr<T, Aliaser>::shared_ptr(const weak_ptr<T, Aliaser> &p)
    : msg_(p.lock().msg_) {}

class Publisher;
class Subscriber;

// An explicitly leased publisher slot. The lease remains publisher-owned until
// it is published or released. lease_id prevents stale slot-id reuse.
struct PublisherBufferLease {
  void *buffer = nullptr;
  size_t buffer_size = 0;
  int32_t slot_id = -1;
  uint64_t lease_id = 0;

  explicit operator bool() const {
    return buffer != nullptr && slot_id >= 0 && lease_id != 0;
  }
};

// RAII owner for an explicitly leased publisher slot. Unless the lease is
// published or explicitly released first, destruction releases it back to the
// publisher. The Publisher must not be moved or destroyed while one of its
// scoped leases is alive.
class ScopedPublisherBufferLease {
public:
  ScopedPublisherBufferLease() = default;
  ~ScopedPublisherBufferLease();

  ScopedPublisherBufferLease(const ScopedPublisherBufferLease &) = delete;
  ScopedPublisherBufferLease &
  operator=(const ScopedPublisherBufferLease &) = delete;
  ScopedPublisherBufferLease(ScopedPublisherBufferLease &&other) noexcept;
  ScopedPublisherBufferLease &
  operator=(ScopedPublisherBufferLease &&other) = delete;

  explicit operator bool() const {
    return publisher_ != nullptr && static_cast<bool>(lease_);
  }
  void *buffer() const { return lease_.buffer; }
  size_t buffer_size() const { return lease_.buffer_size; }
  int32_t slot_id() const { return lease_.slot_id; }
  uint64_t lease_id() const { return lease_.lease_id; }
  absl::Span<std::byte> GetMetadata();
  absl::StatusOr<const Message> Publish(int64_t message_size);
  absl::StatusOr<const Message> PublishCopy(absl::Span<const std::byte> payload);
  absl::Status Release();

private:
  friend class Publisher;
  ScopedPublisherBufferLease(Publisher *publisher, PublisherBufferLease lease)
      : publisher_(publisher), lease_(lease) {}
  void Invalidate() {
    publisher_ = nullptr;
    lease_ = {};
  }

  Publisher *publisher_ = nullptr;
  PublisherBufferLease lease_;
};

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
  // You can pass an AsyncWaiter, such as a co::Coroutine, to the Client and
  // it will wait through it, so a coroutine shares the CPU with all other
  // coroutines in the same scheduler.  If c is nullptr, the client will not
  // yield to other coroutines when talking to the server and will block the
  // CPU.
  //
  // These are public so that they can be accessed by std::make_shared.
  // You shouldn't create these yourself - create a Client instead.
  ClientImpl(const toolbelt::AsyncWaiter *c = nullptr) : co_(c) {}
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // Asio-backed client.  The yield_context drives all cooperative socket I/O.
  // The co coroutine pointer API is preserved for source compatibility but is
  // unused on this backend.
  explicit ClientImpl(async::Context ctx) : co_(nullptr), async_ctx_(ctx) {}
#endif
  ~ClientImpl() = default;

private:
  friend class Client;
  friend class Server;
  friend class Publisher;
  friend class Subscriber;

  enum class LockMode {
    kAutoLock,     // Lock on construction, unlock on destruction
    kDeferredLock, // Locks if not already locked by the current thread, unlocks
                   // if not committed.
    kMaybeLocked,  // Maybe locked by the current thread, lock if not, unlock on
                   // destruction
  };

  // RAII class to conditionally lock a mutex.  This is like std::lock_guard but
  // allows a locking mode to be specified.  The default is be behave like
  // std::lock_guard.  To support holding the lock across GetMessageBuffer and
  // PublishMessage calls it also support deferring the lock and unlock.  This
  // mode also allows for calls to GetMessageBuffer multiple times without
  // publishing.
  class ClientLockGuard {
  public:
    ClientLockGuard(ClientImpl *client,
                    LockMode lock_mode = LockMode::kAutoLock);
    ~ClientLockGuard();
    void Lock();
    void Unlock();

    // If we are using a deferred lock we need to commit it before the object is
    // desctructed if we want to hold onto the lock.
    void CommitLock();

  private:
    ClientImpl *client_;
    LockMode lock_mode_;
    bool locked_ = false;
    bool committed_ = false;
  };

  const std::string &GetName() const { return name_; }

  // Initialize the client by connecting to the server.
  async::Task<absl::Status>
  Init(const std::string &server_socket = kDefaultServerSocket,
                    const std::string &client_name = "");

  // Create a publisher for the given channel.  If the channel doesn't exit
  // it will be created with num_slots slots, each of which is slot_size
  // bytes long.
  async::Task<absl::StatusOr<Publisher>>
  CreatePublisher(const std::string &channel_name, int64_t slot_size,
                  int num_slots,
                  const PublisherOptions &opts = PublisherOptions());

  // Create a publisher with the slot size and number of slots set in the
  // options.
  async::Task<absl::StatusOr<Publisher>>
  CreatePublisher(const std::string &channel_name,
                  const PublisherOptions &opts = PublisherOptions());
  async::Task<void> RemoveServerPublisher(const std::string &channel_name,
                                          int32_t publisher_id);

  // Create a subscriber for the given channel. This can be done before there
  // are any publishers on the channel.
  async::Task<absl::StatusOr<Subscriber>>
  CreateSubscriber(const std::string &channel_name,
                   const SubscriberOptions &opts = SubscriberOptions());

  // Call with true to turn on some debug information.  Kind of meaningless
  // information unless you know how this works in detail.
  void SetDebug(bool v) { debug_ = v; }

  void SetThreadSafe(bool v) { thread_safe_ = v; }

  // Get a snapshot of the current number of publishers and subscribers
  // for the given channel (publisher or subscriber)
  const ChannelCounters &GetChannelCounters(details::ClientChannel *channel);

  absl::StatusOr<const ChannelCounters>
  GetChannelCounters(const std::string &channel_name) const;

  async::Task<absl::StatusOr<const ChannelInfo>>
  GetChannelInfo(const std::string &channelName);
  async::Task<absl::StatusOr<const std::vector<ChannelInfo>>>
  GetChannelInfo();

  async::Task<absl::StatusOr<const ChannelStats>>
  GetChannelStats(const std::string &channelName);
  async::Task<absl::StatusOr<const std::vector<ChannelStats>>>
  GetChannelStats();

  async::Task<absl::StatusOr<bool>>
  ChannelExists(const std::string &channelName);

  // Remove publisher and subscriber.
  async::Task<absl::Status>
  RemovePublisher(details::PublisherImpl *publisher);
  async::Task<absl::Status>
  RemoveSubscriber(details::SubscriberImpl *subscriber);

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
  // (Publisher::GetMessageBuffer is GetMessageBufferSpan plus a conversion, so
  // it needs no coroutine frame of its own.)
  // Get the messsage buffer as a span.  Returns an empty span if there is no
  // buffer available
  async::Task<absl::StatusOr<absl::Span<std::byte>>>
  GetMessageBufferSpan(details::PublisherImpl *publisher, int64_t max_size,
                       bool lock);

  // Publish the message in the publisher's buffer.  The message_size
  // argument specifies the actual size of the message to send.  Returns the
  // information about the message sent with buffer set to nullptr since
  // the publisher cannot access the message once it's been published.
  async::Task<absl::StatusOr<const Message>>
  PublishMessage(details::PublisherImpl *publisher, int64_t message_size,
                 bool notify_subscribers);
  void NotifySubscribers(details::PublisherImpl *publisher);

  async::Task<absl::StatusOr<PublisherBufferLease>>
  AcquirePublisherBuffer(details::PublisherImpl *publisher);
  async::Task<absl::StatusOr<PublisherBufferLease>>
  ReclaimPublisherBuffer(details::PublisherImpl *publisher, int32_t slot_id);
  async::Task<absl::StatusOr<PublisherBufferLease>>
  ReclaimAnyPublisherBuffer(details::PublisherImpl *publisher);
  async::Task<absl::StatusOr<const Message>>
  PublishPublisherBuffer(details::PublisherImpl *publisher,
                         const PublisherBufferLease &lease,
                         int64_t message_size);
  async::Task<absl::StatusOr<const Message>>
  PublishPublisherBufferCopy(details::PublisherImpl *publisher,
                             const PublisherBufferLease &lease,
                             absl::Span<const std::byte> payload);
  async::Task<absl::StatusOr<const Message>>
  PublishPublisherBufferInternal(
      details::PublisherImpl *publisher, const PublisherBufferLease &lease,
      int64_t message_size, const std::byte *payload);
  absl::Status ReleasePublisherBuffer(details::PublisherImpl *publisher,
                                      const PublisherBufferLease &lease);
  absl::Span<std::byte>
  GetPublisherBufferMetadata(details::PublisherImpl *publisher,
                             const PublisherBufferLease &lease);

  // In thread-safe mode, if you don't want to publish the message, you must
  // cancel the publish.  This will release the lock.
  void CancelPublish(details::PublisherImpl *publisher);

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  async::Task<absl::Status>
  WaitForReliablePublisher(details::PublisherImpl *publisher,
                           const toolbelt::AsyncWaiter *c = nullptr) {
    return WaitForReliablePublisher(publisher, std::chrono::nanoseconds(0), c);
  }

  async::Task<absl::Status>
  WaitForReliablePublisher(details::PublisherImpl *publisher,
                                        std::chrono::nanoseconds timeout,
                                        const toolbelt::AsyncWaiter *c = nullptr);

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  async::Task<absl::StatusOr<int>>
  WaitForReliablePublisher(details::PublisherImpl *publisher,
                           const toolbelt::FileDescriptor &fd,
                           const toolbelt::AsyncWaiter *c = nullptr) {
    return WaitForReliablePublisher(publisher, fd, std::chrono::nanoseconds(0),
                                    c);
  }

  async::Task<absl::StatusOr<int>>
  WaitForReliablePublisher(
      details::PublisherImpl *publisher, const toolbelt::FileDescriptor &fd,
      std::chrono::nanoseconds timeout, const toolbelt::AsyncWaiter *c = nullptr);

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // Asio overloads that take an async::Context (yield_context) directly so an
  // Asio caller can cooperatively wait on a caller-supplied coroutine context,
  // exactly as the co backend does with a co::Coroutine*.  On the co backend
  // async::Context is a co::Coroutine*, which the AsyncWaiter overloads above
  // already accept, so these are only present for the Asio backend.
  absl::Status WaitForReliablePublisher(details::PublisherImpl *publisher,
                                        async::Context ctx) {
    return WaitForReliablePublisher(publisher, std::chrono::nanoseconds(0),
                                    ctx);
  }

  absl::Status WaitForReliablePublisher(details::PublisherImpl *publisher,
                                        std::chrono::nanoseconds timeout,
                                        async::Context ctx);

  absl::StatusOr<int>
  WaitForReliablePublisher(details::PublisherImpl *publisher,
                           const toolbelt::FileDescriptor &fd,
                           async::Context ctx) {
    return WaitForReliablePublisher(publisher, fd, std::chrono::nanoseconds(0),
                                    ctx);
  }

  absl::StatusOr<int> WaitForReliablePublisher(
      details::PublisherImpl *publisher, const toolbelt::FileDescriptor &fd,
      std::chrono::nanoseconds timeout, async::Context ctx);
#endif

  // Wait until there's a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  async::Task<absl::Status>
  WaitForSubscriber(details::SubscriberImpl *subscriber,
                    const toolbelt::AsyncWaiter *c = nullptr) {
    return WaitForSubscriber(subscriber, std::chrono::nanoseconds(0), c);
  }

  async::Task<absl::Status>
  WaitForSubscriber(details::SubscriberImpl *subscriber,
                                 std::chrono::nanoseconds timeout,
                                 const toolbelt::AsyncWaiter *c = nullptr);

  // Wait until there' s a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  async::Task<absl::StatusOr<int>>
  WaitForSubscriber(details::SubscriberImpl *subscriber,
                    const toolbelt::FileDescriptor &fd,
                    const toolbelt::AsyncWaiter *c = nullptr) {
    return WaitForSubscriber(subscriber, fd, std::chrono::nanoseconds(0), c);
  }

  async::Task<absl::StatusOr<int>>
  WaitForSubscriber(details::SubscriberImpl *subscriber,
                                        const toolbelt::FileDescriptor &fd,
                                        std::chrono::nanoseconds timeout,
                                        const toolbelt::AsyncWaiter *c = nullptr);

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // Asio async::Context (yield_context) overloads; see the publisher overloads
  // above for why these are Asio-only.
  absl::Status WaitForSubscriber(details::SubscriberImpl *subscriber,
                                 async::Context ctx) {
    return WaitForSubscriber(subscriber, std::chrono::nanoseconds(0), ctx);
  }

  absl::Status WaitForSubscriber(details::SubscriberImpl *subscriber,
                                 std::chrono::nanoseconds timeout,
                                 async::Context ctx);

  absl::StatusOr<int> WaitForSubscriber(details::SubscriberImpl *subscriber,
                                        const toolbelt::FileDescriptor &fd,
                                        async::Context ctx) {
    return WaitForSubscriber(subscriber, fd, std::chrono::nanoseconds(0), ctx);
  }

  absl::StatusOr<int> WaitForSubscriber(details::SubscriberImpl *subscriber,
                                        const toolbelt::FileDescriptor &fd,
                                        std::chrono::nanoseconds timeout,
                                        async::Context ctx);
#endif

  // Read a message from a subscriber.  If there are no available messages
  // the 'length' field of the returned Message will be zero.  The 'buffer'
  // field of the Message is set to the address of the message in shared
  // memory which is read-only.  If the read is triggered by the PollFd,
  // you must read all the avaiable messages from the subscriber as the
  // PollFd is only triggered when a new message is published.
  // Pass ClearTrigger::kNoClearTrigger to leave the subscriber trigger fd
  // unread.
  async::Task<absl::StatusOr<Message>>
  ReadMessage(
      details::SubscriberImpl *subscriber, ReadMode mode = ReadMode::kReadNext,
      ClearTrigger clear_trigger = ClearTrigger::kClearTrigger);
  // Continue draining a batch after ReadMessage has cleared the poll fd and
  // refreshed publisher state. This avoids repeating that work per message.
  async::Task<absl::StatusOr<Message>>
  ReadMessageFromBatch(details::SubscriberImpl *subscriber,
                       ReadMode mode = ReadMode::kReadNext);

  // As ReadMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T, typename Aliaser = DefaultAliaser>
  async::Task<absl::StatusOr<shared_ptr<T, Aliaser>>>
  ReadMessage(details::SubscriberImpl *subscriber,
              ReadMode mode = ReadMode::kReadNext,
              ClearTrigger clear_trigger = ClearTrigger::kClearTrigger);

  // Find a message given a timestamp.
  async::Task<absl::StatusOr<Message>>
  FindMessage(details::SubscriberImpl *subscriber,
                                      uint64_t timestamp);
  // AsFindMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T, typename Aliaser = DefaultAliaser>
  async::Task<absl::StatusOr<shared_ptr<T, Aliaser>>>
  FindMessage(details::SubscriberImpl *subscriber, uint64_t timestamp);

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

  // Register a function to be called when max_active_messages has kept a
  // subscriber from reading a waiting message for the stuck warning grace
  // period.  The function is called with the number of seconds the subscriber
  // has been stuck as its second argument.
  absl::Status RegisterStuckSubscriberCallback(
      details::SubscriberImpl *subscriber,
      std::function<void(details::SubscriberImpl *, double)> callback);
  absl::Status
  UnregisterStuckSubscriberCallback(details::SubscriberImpl *subscriber);

  absl::Status RegisterMessageCallback(
      details::SubscriberImpl *subscriber,
      std::function<void(details::SubscriberImpl *, Message)> callback);
  absl::Status UnregisterMessageCallback(details::SubscriberImpl *subscriber);

  void InvokeMessageCallback(details::SubscriberImpl *subscriber, Message msg) {
    auto it = message_callbacks_.find(subscriber);
    if (it != message_callbacks_.end() && it->second) {
      it->second(subscriber, std::move(msg));
    }
  }

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
      std::function<absl::Status(details::PublisherImpl *, int64_t, int64_t)>
          cb);
  absl::Status UnregisterResizeCallback(details::PublisherImpl *publisher);

  // Call the message callback function for all available messages.  You must
  // have registered a callback function with RegisterMessageCallback for this
  // to work.
  async::Task<absl::Status>
  ProcessAllMessages(details::SubscriberImpl *subscriber,
                                  ReadMode mode = ReadMode::kReadNext);

  async::Task<absl::StatusOr<std::vector<Message>>>
  GetAllMessages(details::SubscriberImpl *subscriber,
                 ReadMode mode = ReadMode::kReadNext);

  // Get the most recently received ordinal for the subscriber.
  int64_t GetCurrentOrdinal(details::SubscriberImpl *sub);

  absl::Status CheckConnected() const;
  async::Task<absl::Status>
  SendRequestReceiveResponse(
      const Request &req, Response &response,
      std::vector<toolbelt::FileDescriptor> &fds,
      const std::vector<toolbelt::FileDescriptor> &send_fds = {});
  async::Task<absl::Status>
  SendOneWayRequest(const Request &req,
                    const std::vector<toolbelt::FileDescriptor> &fds = {});
  async::Task<absl::Status>
  RegisterClientBuffer(const ClientBufferHandleMetadata &metadata,
                       const toolbelt::FileDescriptor *fd = nullptr);
  async::Task<absl::StatusOr<std::vector<RegisteredClientBuffer>>>
  GetClientBuffers(const std::string &channel_name, uint64_t session_id,
                   uint32_t buffer_index);
  async::Task<absl::Status>
  UnregisterClientBuffer(const std::string &channel_name,
                                      uint64_t session_id,
                                      uint32_t buffer_index);

  async::Task<absl::Status>
  Reconnect();
  // Whether a request that failed with `failure` should reconnect and retry.
  bool ShouldReconnect(const absl::Status &failure) const;
  async::Task<absl::Status>
  ReregisterPublisher(details::PublisherImpl *publisher);
  async::Task<absl::Status>
  ReregisterSubscriber(details::SubscriberImpl *subscriber);

  static void FillCreatePublisherRequest(CreatePublisherRequest *cmd,
                                         const std::string &channel_name,
                                         const PublisherOptions &opts,
                                         int publisher_id);
  void ApplyPublisherResponseFds(details::PublisherImpl *publisher,
                                 const CreatePublisherResponse &resp,
                                 std::vector<toolbelt::FileDescriptor> &fds);
  static void FillCreateSubscriberRequest(CreateSubscriberRequest *cmd,
                                          const std::string &channel_name,
                                          const SubscriberOptions &opts,
                                          int subscriber_id);
  void ApplySubscriberResponseFds(details::SubscriberImpl *subscriber,
                                  const CreateSubscriberResponse &resp,
                                  std::vector<toolbelt::FileDescriptor> &fds);

  async::Task<bool> CheckReload(details::ClientChannel *channel);

  // The Async API holds nothing across a suspension, but a thread-safe client
  // keeps a mutex locked while a coroutine is suspended, which would let
  // another coroutine on the same thread deadlock on it.  So the Async API is
  // only available on a client that isn't thread safe.
  absl::Status CheckAsyncAllowed() const {
    if (thread_safe_) {
      return absl::FailedPreconditionError(
          "The Async API can't be used on a thread-safe client");
    }
    return absl::OkStatus();
  }

  // Cheap checks matching the early-outs of ReloadSubscribersIfNecessary and
  // ReloadReliablePublishersIfNecessary.  The hot paths test these first so
  // they only create a coroutine frame when the server has to be consulted.
  bool SubscribersChanged(details::PublisherImpl *publisher) const {
    return publisher->NumUpdates() !=
           publisher->GetScb()
               ->counters[publisher->GetChannelId()]
               .num_sub_updates;
  }
  bool PublishersChanged(details::SubscriberImpl *subscriber) const {
    return subscriber->NumUpdates() !=
           subscriber->GetScb()
               ->counters[subscriber->GetChannelId()]
               .num_pub_updates;
  }

  async::Task<absl::Status>
  ReloadSubscriber(details::SubscriberImpl *channel);
  async::Task<absl::Status>
  ReloadSubscribersIfNecessary(details::PublisherImpl *publisher);
  async::Task<absl::Status>
  ReloadReliablePublishersIfNecessary(details::SubscriberImpl *subscriber);
  absl::Status RemoveChannel(details::ClientChannel *channel);
  async::Task<absl::Status> ActivateReliableChannel(details::PublisherImpl *channel);
  async::Task<absl::Status> ActivateChannel(details::PublisherImpl *channel);
  async::Task<absl::StatusOr<Message>>
  ReadMessageInternal(details::SubscriberImpl *subscriber, ReadMode mode,
                      bool pass_activation, bool clear_trigger);
  void ReportIfSubscriberStuck(details::SubscriberImpl *subscriber);
  async::Task<absl::StatusOr<Message>>
  FindMessageInternal(details::SubscriberImpl *subscriber, uint64_t timestamp);
  async::Task<absl::StatusOr<const Message>>
  PublishMessageInternal(details::PublisherImpl *publisher,
                         int64_t message_size, bool omit_prefix,
                         bool use_prefix_slot_id, bool use_prefix_vchan_id,
                         bool notify_subscribers);
  async::Task<absl::Status>
  ResizeChannel(details::PublisherImpl *publisher,
                             int64_t new_slot_size);
  async::Task<absl::StatusOr<bool>>
  ReloadBuffersIfNecessary(details::ClientChannel *channel);

  const std::vector<std::unique_ptr<details::BufferSet>> &
  GetBuffers(details::ClientChannel *channel) const {
    return channel->GetBuffers();
  }

  std::string name_;
  std::string socket_name_;
  uint64_t session_id_ = 0;

  async::UnixSocket socket_;
  toolbelt::FileDescriptor scb_fd_; // System control block memory fd.

  // The client owns all the publishers and subscribers.
  absl::flat_hash_set<std::shared_ptr<details::ClientChannel>> channels_;

  // If this is non-nullptr the client waits through it, so a coroutine
  // cooperates with all other coroutines to share the CPU.
  const toolbelt::AsyncWaiter *co_; // Does not own the waiter.

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // On the Asio backend the cooperative context is a yield_context supplied at
  // construction (the co pointer above is unused).
  std::optional<async::Context> async_ctx_;

  // The Context passed to socket I/O.  On Asio this is the stored
  // yield_context for a cooperative client; for a blocking client (no
  // yield_context) the socket is in blocking mode and never yields, so we hand
  // it a NullContext() placeholder that is never used.
  async::Context SocketContext() const {
    return async_ctx_.has_value() ? *async_ctx_ : async::NullContext();
  }

  // True when this client cooperates with a coroutine (has a yield_context);
  // false for a blocking client that uses synchronous socket I/O + poll().
  bool IsCooperative() const { return async_ctx_.has_value(); }
#elif SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // Inside an Async call co20::self is the running coroutine, so socket waits
  // suspend it.  The blocking API runs the same code under SyncWait, which
  // hides co20::self, so those waits go through co_ (if there is one) or poll.
  async::Context SocketContext() const {
    if (co20::Coroutine *running = co20::self; running != nullptr) {
      return async::Context(running);
    }
    return async::Context(co_);
  }
#else
  const toolbelt::AsyncWaiter *SocketContext() const { return co_; }
#endif

  // Call this function when the given subscriber detects a dropped message.
  // This will only really happen when you have an unreliable subscriber
  // but if there's an unreliable publisher and this subscriber is reliable
  // you might see it called for messages from that publisher.
  absl::flat_hash_map<details::SubscriberImpl *,
                      std::function<void(details::SubscriberImpl *, int64_t)>>
      dropped_message_callbacks_;

  // Called instead of logging an error when a subscriber is stuck at its
  // max_active_messages limit.
  absl::flat_hash_map<details::SubscriberImpl *,
                      std::function<void(details::SubscriberImpl *, double)>>
      stuck_subscriber_callbacks_;

  // Callback per subscriber to call when a message is received.  Use the
  // `ProcessAllMessages` function on the Subscriber to call this function for
  // all currently available messages.
  absl::flat_hash_map<details::SubscriberImpl *,
                      std::function<void(details::SubscriberImpl *, Message)>>
      message_callbacks_;

  // Call the function when a publisher causes a channel to be resized.
  absl::flat_hash_map<
      details::PublisherImpl *,
      std::function<absl::Status(details::PublisherImpl *, int64_t, int64_t)>>
      resize_callbacks_;
  // Test-only.  Called by ReadMessage and FindMessage on a placeholder
  // subscriber just after it has checked the server for a new publisher.
  std::function<void()> placeholder_check_hook_;
  bool debug_ = false;
  toolbelt::Logger logger_;
  mutable std::mutex mutex_;
  std::atomic<uint64_t> owner_thread_id_ = {};

  // For backward compatibility we default to non-thread-safe because it has
  // implications for the GetMessageBufferSpan function.  You have to publish
  // the message or call CancelPublish in order to release the lock.
  bool thread_safe_ = false;

  // User and group id for the server.  This is used to change the ownership of
  // the shared memory files so that the server can delete them.  Important when
  // the client is running as root and the server isn't
  int server_user_id_ = -1;
  int server_group_id_ = -1;
  bool was_connected_ = false;
  bool reconnecting_ = false;
};

// This function returns an subspace::shared_ptr that refers to the message
// just read.  The subscriber will still have a reference to the slot
// until another read is done.  The shared_ptr will keep a reference
// to the original slot until it is reset or destructed.  This will
// prevent the slot referred to by the shared_ptr from being taken
// by a publisher.  Don't hold onto shared_ptr instances long than
// you need to as it may prevent a publisher getting a slot.
template <typename T, typename Aliaser>
inline async::Task<absl::StatusOr<::subspace::shared_ptr<T, Aliaser>>>
ClientImpl::ReadMessage(details::SubscriberImpl *subscriber, ReadMode mode,
                        ClearTrigger clear_trigger) {
  absl::StatusOr<Message> msg =
      ASYNC_AWAIT(ReadMessage(subscriber, mode, clear_trigger));
  if (!msg.ok()) {
    ASYNC_RETURN msg.status();
  }
  if (msg->length == 0) {
    ASYNC_RETURN ::subspace::shared_ptr<T, Aliaser>();
  }
  ASYNC_RETURN ::subspace::shared_ptr<T, Aliaser>(std::move(*msg));
}

template <typename T, typename Aliaser>
inline async::Task<absl::StatusOr<::subspace::shared_ptr<T, Aliaser>>>
ClientImpl::FindMessage(details::SubscriberImpl *subscriber,
                        uint64_t timestamp) {
  absl::StatusOr<Message> msg = ASYNC_AWAIT(FindMessage(subscriber, timestamp));
  if (!msg.ok()) {
    ASYNC_RETURN msg.status();
  }
  ASYNC_RETURN ::subspace::shared_ptr<T, Aliaser>(std::move(*msg));
}

// The Publisher and Subscriber classes are the main interface for sending
// and receiving messages.  They can be moved but not copied.
class Publisher {
public:
  ~Publisher() {
    if (client_ != nullptr && impl_ != nullptr && !closed_) {
      UnregisterResizeCallback().IgnoreError();
      (void)ASYNC_BLOCK(client_->RemovePublisher(impl_.get()));
    }
  }

  Publisher(const Publisher &other) = delete;
  Publisher &operator=(const Publisher &other) = delete;

  Publisher(Publisher &&other)
      : client_(std::move(other.client_)), impl_(std::move(other.impl_)),
        resize_callback_(std::move(other.resize_callback_)),
        closed_(other.closed_) {
    if (impl_ != nullptr && client_ != nullptr && resize_callback_ != nullptr) {
      client_->UnregisterResizeCallback(impl_.get()).IgnoreError();
      RegisterResizeCallback(resize_callback_).IgnoreError();
    }
    other.client_ = nullptr;
    other.impl_ = nullptr;
  }

  Publisher &operator=(Publisher &&other) {
    client_ = std::move(other.client_);
    impl_ = std::move(other.impl_);
    resize_callback_ = std::move(other.resize_callback_);
    closed_ = other.closed_;
    if (impl_ != nullptr && client_ != nullptr && resize_callback_ != nullptr) {
      client_->UnregisterResizeCallback(impl_.get()).IgnoreError();
      RegisterResizeCallback(resize_callback_).IgnoreError();
    }
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
  //
  // In thread-safe mode, this will hold a lock on the client until you publish
  // the message.  If you don't want to publish the message, you must cancel the
  // publish using CancelPublish.  This will release the lock.
  absl::StatusOr<void *> GetMessageBuffer(SlotSizeType max_size = -1,
                                          bool lock = true) {
    absl::StatusOr<absl::Span<std::byte>> span = ASYNC_BLOCK(
        client_->GetMessageBufferSpan(impl_.get(), max_size, lock));
    if (!span.ok()) {
      return span.status();
    }
    return span->empty() ? nullptr : static_cast<void *>(span->data());
  }

  // Get the messsage buffer as a span.  Returns an empty span if there is no
  // buffer available.  See GetMessageBuffer for details of
  absl::StatusOr<absl::Span<std::byte>>
  GetMessageBufferSpan(SlotSizeType max_size = -1, bool lock = true) {
    return ASYNC_BLOCK(client_->GetMessageBufferSpan(impl_.get(), max_size, lock));
  }

  // Publish the message in the publisher's buffer.  The message_size
  // argument specifies the actual size of the message to send.  Returns the
  // information about the message sent with buffer set to nullptr since
  // the publisher cannot access the message once it's been published.
  //
  // In thread-safe mode, this will release the lock on the client.  If you
  // don't want to publish the message, you must cancel the publish using
  // CancelPublish.
  // notify_subscribers may be false while building a batch, provided a later
  // message in that batch is published with notification enabled.
  absl::StatusOr<const Message>
  PublishMessage(int64_t message_size, bool notify_subscribers = true) {
    return ASYNC_BLOCK(client_->PublishMessage(impl_.get(), message_size,
                                   notify_subscribers));
  }
  // Flush a batch whose messages were published without subscriber
  // notification.
  void NotifySubscribers() { client_->NotifySubscribers(impl_.get()); }

  // Lease multiple unpublished slots without holding the client's thread-safe
  // mutex across the lease lifetime. The maximum is configured by
  // PublisherOptions::SetMaxOutstandingSlotLeases().
  absl::StatusOr<PublisherBufferLease> AcquireBufferLease() {
    return ASYNC_BLOCK(client_->AcquirePublisherBuffer(impl_.get()));
  }

  absl::StatusOr<ScopedPublisherBufferLease> AcquireScopedBufferLease() & {
    auto lease = AcquireBufferLease();
    if (!lease.ok()) {
      return lease.status();
    }
    return ScopedPublisherBufferLease(this, *lease);
  }

  // Reclaim a specific slot reported by the retirement fd.
  absl::StatusOr<PublisherBufferLease> ReclaimBufferLease(int32_t slot_id) {
    return ASYNC_BLOCK(client_->ReclaimPublisherBuffer(impl_.get(), slot_id));
  }

  // Reclaim any slot currently marked retired when fd notifications are stale.
  absl::StatusOr<PublisherBufferLease> ReclaimAnyBufferLease() {
    return ASYNC_BLOCK(client_->ReclaimAnyPublisherBuffer(impl_.get()));
  }

  absl::StatusOr<ScopedPublisherBufferLease>
  ReclaimScopedBufferLease(int32_t slot_id) & {
    auto lease = ReclaimBufferLease(slot_id);
    if (!lease.ok()) {
      return lease.status();
    }
    return ScopedPublisherBufferLease(this, *lease);
  }

  absl::StatusOr<const Message>
  PublishBufferLease(const PublisherBufferLease &lease, int64_t message_size) {
    return ASYNC_BLOCK(client_->PublishPublisherBuffer(impl_.get(), lease, message_size));
  }

  absl::StatusOr<const Message>
  PublishBufferLeaseCopy(const PublisherBufferLease &lease,
                         absl::Span<const std::byte> payload) {
    return ASYNC_BLOCK(client_->PublishPublisherBufferCopy(impl_.get(), lease, payload));
  }

  absl::Status ReleaseBufferLease(const PublisherBufferLease &lease) {
    return client_->ReleasePublisherBuffer(impl_.get(), lease);
  }

  absl::Span<std::byte> GetMetadata(const PublisherBufferLease &lease) {
    return client_->GetPublisherBufferMetadata(impl_.get(), lease);
  }

  // Publish a message that already includes a prefix.  You have the option to
  // use the slot id passed in the prefix for message retirement or use the
  // If the message arrived over a bridge and you want to pass the retirement
  // back over the bridge, use the slot id from the prefix.  If the retirement
  // notification is locally handled, use the slot id from the message.
  //
  // The message is delivered on the virtual channel in the prefix's vchan_id.
  // If use_vchan_id_from_prefix is false, it's delivered on this publisher's
  // virtual channel instead, and the prefix keeps the sender's vchan_id, so a
  // checksum the sender computed over it still verifies.
  //
  // In thread-safe mode, this will release the lock on the client.  If you
  // don't want to publish the message, you must cancel the publish using
  // CancelPublish.
  absl::StatusOr<const Message>
  PublishMessageWithPrefix(int64_t message_size,
                           bool use_slot_id_from_prefix = true,
                           bool use_vchan_id_from_prefix = true) {
    return PublishMessageInternal(message_size, true, use_slot_id_from_prefix,
                                  use_vchan_id_from_prefix);
  }

  // If you don't want to publish the message, you must cancel the publish. This
  // will release the lock.
  void CancelPublish() { client_->CancelPublish(impl_.get()); }

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  absl::Status Wait(const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForReliablePublisher(impl_.get(), c));
  }

  absl::Status Wait(std::chrono::nanoseconds timeout,
                    const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForReliablePublisher(impl_.get(), timeout, c));
  }

  // Wait until a reliable publisher can try again to send a message.  If the
  // client is coroutine-aware, the coroutine will wait.  If it's not,
  // the function will block on a poll until the publisher is triggered.
  // This also takes an additional file descriptor that can be used to interrupt
  // the wait.  Returns the integer fd value of the file descriptor that
  // triggered the wait.
  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForReliablePublisher(impl_.get(), fd, c));
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           std::chrono::nanoseconds timeout,
                           const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForReliablePublisher(impl_.get(), fd, timeout, c));
  }

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // Asio overloads: cooperatively wait on a caller-supplied yield_context, the
  // direct analogue of the AsyncWaiter overloads above.
  absl::Status Wait(async::Context ctx) {
    return client_->WaitForReliablePublisher(impl_.get(), ctx);
  }

  absl::Status Wait(std::chrono::nanoseconds timeout, async::Context ctx) {
    return client_->WaitForReliablePublisher(impl_.get(), timeout, ctx);
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           async::Context ctx) {
    return client_->WaitForReliablePublisher(impl_.get(), fd, ctx);
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           std::chrono::nanoseconds timeout,
                           async::Context ctx) {
    return client_->WaitForReliablePublisher(impl_.get(), fd, timeout, ctx);
  }
#endif

  // Trigger the publisher's reliable event fd, waking anything that is waiting
  // on it (for example a caller blocked in Wait()).  This exposes the trigger
  // side of the publisher's triggerfd so that the caller of PublishMessage can
  // explicitly wake a waiter.
  void TriggerReliableWait() { impl_->TriggerReliable(); }

  struct pollfd GetPollFd() const {
    return client_->GetPollFd(impl_.get());
  }

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
  // Effective upper bound on the slot size, rounded up to the channel's
  // alignment, or 0 if the channel is uncapped.
  SlotSizeType MaxSlotSize() const {
    return static_cast<SlotSizeType>(impl_->MaxSlotSize());
  }
  bool ForTunnel() const { return impl_->ForTunnel(); }
  // True when the channel stores prefixes separately from payload slots.
  // In this mode each payload slot has an allocator-defined handle that can be
  // passed to code outside Subspace. For example, if payloads were allocated
  // from a Qualcomm memory pool, the handle is the value that qcomm-specific
  // code needs to identify/map/free that slot.
  bool UsesSplitBuffers() const { return impl_->UsesSplitBuffers(); }

  SlotSizeType SlotSize() const {
    return static_cast<SlotSizeType>(impl_->SlotSize());
  }
  int32_t NumSlots() const { return impl_->NumSlots(); }
  int32_t SubscriberQueueSize() const { return impl_->SubscriberQueueSize(); }
  uint64_t SubscriberQueueArenaSize() const {
    return impl_->SubscriberQueueArenaSize();
  }

  const std::vector<std::unique_ptr<details::BufferSet>> &GetBuffers() const {
    return client_->GetBuffers(impl_.get());
  }

  bool GetAddresses(void ***addresses, size_t *count) {
    if (addresses != nullptr) {
      *addresses = nullptr;
    }
    if (count != nullptr) {
      *count = 0;
    }
    if (addresses == nullptr || count == nullptr || impl_->IsPlaceholder() ||
        impl_->GetBuffers().empty()) {
      return false;
    }
    const details::BufferSet *latest = impl_->GetBuffers().back().get();
    address_cache_.clear();
    address_cache_.reserve(static_cast<size_t>(impl_->NumSlots()));
    if (latest->IsSplitBuffers()) {
      if (latest->split_slot_buffers.size() <
          static_cast<size_t>(impl_->NumSlots())) {
        return false;
      }
      for (int slot = 0; slot < impl_->NumSlots(); ++slot) {
        address_cache_.push_back(latest->split_slot_buffers[slot]);
      }
    } else {
      if (latest->buffer == nullptr) {
        return false;
      }
      uint64_t stride = impl_->PrefixSize() + Aligned<64>(latest->slot_size);
      for (int slot = 0; slot < impl_->NumSlots(); ++slot) {
        address_cache_.push_back(latest->buffer + stride * slot +
                                 impl_->PrefixSize());
      }
    }
    *addresses = address_cache_.data();
    *count = address_cache_.size();
    return true;
  }

  void GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages,
                        uint64_t &max_message_size, uint32_t &total_drops) {
    impl_->GetStatsCounters(total_bytes, total_messages, max_message_size,
                            total_drops);
  }

  uint64_t GetVirtualMemoryUsage() const {
    return impl_->GetVirtualMemoryUsage();
  }

  const ChannelCounters &GetCounters() const { return impl_->GetCounters(); }

  std::string BufferSharedMemoryName(int buffer_index) const {
    return impl_->BufferSharedMemoryName(buffer_index);
  }

  // Set a custom checksum callback to replace the default CRC32.
  // The callback is invoked for every published message and receives the
  // data spans to checksum plus a writable absl::Span<std::byte> of
  // ChecksumSize() bytes where the checksum should be written.
  void SetChecksumCallback(ChecksumCallback callback) {
    impl_->SetChecksumCallback(std::move(callback));
  }

  // Revert to the built-in CRC32 checksum.
  void ResetChecksumCallback() { impl_->ResetChecksumCallback(); }

  // Register a function to be called when the publisher resizes
  // the channel.
  absl::Status RegisterResizeCallback(
      std::function<absl::Status(Publisher *, SlotSizeType, SlotSizeType)>
          callback) {
    auto status = client_->RegisterResizeCallback(
        impl_.get(),
        [this](details::PublisherImpl *, int64_t old_size,
               int64_t new_size) -> absl::Status {
          return resize_callback_(this, static_cast<SlotSizeType>(old_size),
                                  static_cast<SlotSizeType>(new_size));
        });
    if (!status.ok()) {
      return status;
    }
    resize_callback_ = std::move(callback);
    return absl::OkStatus();
  }

  absl::Status UnregisterResizeCallback() {
    auto status = client_->UnregisterResizeCallback(impl_.get());
    if (!status.ok()) {
      return status;
    }
    resize_callback_ = nullptr;
    return absl::OkStatus();
  }

  void SetOnSendCallback(
      std::function<absl::StatusOr<int64_t>(void *buffer, int64_t size)>
          callback) {
    impl_->SetOnSendCallback(std::move(callback));
  }

  void ClearOnSendCallback() { impl_->SetOnSendCallback(nullptr); }

  int VirtualChannelId() const { return impl_->VirtualChannelId(); }
  std::string Mux() const { return impl_->Mux(); }

  int NumSubscribers(int vchan_id = -1) const {
    return impl_->NumSubscribers(vchan_id);
  }

  int CurrentSlotId() const { return impl_->CurrentSlotId(); }

  MessageSlot *CurrentSlot() const { return impl_->CurrentSlot(); }

  MessagePrefix *Prefix(MessageSlot *slot = nullptr) const {
    return impl_->Prefix(slot != nullptr ? slot : impl_->CurrentSlot());
  }
  // If `address` points at a split payload buffer owned by this publisher,
  // writes that slot's allocator handle to `handle` and returns true. This is
  // useful after GetMessageBuffer() when another API needs the handle for the
  // payload memory instead of the mapped CPU address.
  bool GetSplitBufferHandleFromAddress(const void *address,
                                       uintptr_t *handle) const {
    return impl_->GetSplitBufferHandleFromAddress(address, handle);
  }
  // Returns the handles for the current split-buffer set, one handle per slot.
  // The returned array is owned by the publisher and remains valid until the
  // publisher reloads, resizes, or is destroyed. Handles are meaningful only to
  // the allocator/callbacks that created the payload buffers.
  bool GetSplitBufferHandles(uintptr_t **handles, size_t *count) {
    return impl_->GetSplitBufferHandles(handles, count);
  }

  // Total prefix area size in bytes (always a multiple of 64).
  // Determined by ChecksumSize() and MetadataSize().
  int32_t PrefixSize() const { return impl_->PrefixSize(); }

  // Number of bytes reserved for the checksum (default 4 for CRC32).
  int32_t ChecksumSize() const { return impl_->ChecksumSize(); }

  // Number of bytes of user metadata in the prefix area (default 0).
  int32_t MetadataSize() const { return impl_->MetadataSize(); }

  // Get a writable span over the user metadata area in the current
  // slot's prefix.  Call this between GetMessageBuffer() and
  // PublishMessage() to attach per-message metadata.  Returns an
  // empty span if MetadataSize() is 0.
  absl::Span<std::byte> GetMetadata() { return impl_->GetMetadata(); }

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // ---- Async API (C++20 coroutines) ------------------------------------
  //
  // Each of these does what the function of the same name without "Async"
  // does, but suspends the calling coroutine instead of blocking the thread
  // while it waits for the server or for a reliable publisher.  co_await the
  // result from a coroutine running on a co20::Scheduler.  Don't use the
  // blocking functions from inside such a coroutine: they block the whole
  // scheduler thread.
  //
  // The client must not be thread safe (FailedPrecondition otherwise), and
  // only one Async call should be in progress on a client at a time because
  // they share its socket to the server.  The Publisher must stay in place
  // until the call completes.

  async::Task<absl::StatusOr<void *>>
  GetMessageBufferAsync(SlotSizeType max_size = -1) {
    auto span = ASYNC_AWAIT(GetMessageBufferSpanAsync(max_size));
    if (!span.ok()) {
      co_return span.status();
    }
    co_return span->empty() ? nullptr : static_cast<void *>(span->data());
  }

  async::Task<absl::StatusOr<absl::Span<std::byte>>>
  GetMessageBufferSpanAsync(SlotSizeType max_size = -1) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->GetMessageBufferSpan(impl.get(), max_size, /*lock=*/false));
  }

  async::Task<absl::StatusOr<const Message>>
  PublishMessageAsync(int64_t message_size, bool notify_subscribers = true) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->PublishMessage(impl.get(), message_size, notify_subscribers));
  }

  // The same as PublishMessageWithPrefix, but suspends the calling coroutine.
  async::Task<absl::StatusOr<const Message>>
  PublishMessageWithPrefixAsync(int64_t message_size,
                                bool use_slot_id_from_prefix = true,
                                bool use_vchan_id_from_prefix = true) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(client->PublishMessageInternal(
        impl.get(), message_size, /*omit_prefix=*/true,
        use_slot_id_from_prefix, use_vchan_id_from_prefix,
        /*notify_subscribers=*/true));
  }

  async::Task<absl::StatusOr<PublisherBufferLease>> AcquireBufferLeaseAsync() {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(client->AcquirePublisherBuffer(impl.get()));
  }

  async::Task<absl::StatusOr<PublisherBufferLease>>
  ReclaimBufferLeaseAsync(int32_t slot_id) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(client->ReclaimPublisherBuffer(impl.get(), slot_id));
  }

  async::Task<absl::StatusOr<PublisherBufferLease>>
  ReclaimAnyBufferLeaseAsync() {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(client->ReclaimAnyPublisherBuffer(impl.get()));
  }

  async::Task<absl::StatusOr<const Message>>
  PublishBufferLeaseAsync(PublisherBufferLease lease, int64_t message_size) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->PublishPublisherBuffer(impl.get(), lease, message_size));
  }

  // The payload must stay valid until the call completes.
  async::Task<absl::StatusOr<const Message>>
  PublishBufferLeaseCopyAsync(PublisherBufferLease lease,
                              absl::Span<const std::byte> payload) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->PublishPublisherBufferCopy(impl.get(), lease, payload));
  }

  // Wait until a reliable publisher can try again to send a message.  A
  // timeout of zero waits forever.
  async::Task<absl::Status>
  WaitAsync(std::chrono::nanoseconds timeout = std::chrono::nanoseconds(0)) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->WaitForReliablePublisher(impl.get(), timeout, nullptr));
  }

  // As above, but also returns when fd becomes readable.  Returns the fd that
  // triggered the wait.
  async::Task<absl::StatusOr<int>>
  WaitAsync(const toolbelt::FileDescriptor &fd,
            std::chrono::nanoseconds timeout = std::chrono::nanoseconds(0)) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->WaitForReliablePublisher(impl.get(), fd, timeout, nullptr));
  }

  // Removes the publisher from the server without blocking.  The destructor
  // does the same but blocks, so a publisher used from a coroutine should be
  // closed with this first.  The publisher can't be used afterwards.
  async::Task<absl::Status> CloseAsync() {
    if (client_ == nullptr || impl_ == nullptr || closed_) {
      co_return absl::OkStatus();
    }
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    closed_ = true;
    UnregisterResizeCallback().IgnoreError();
    co_return ASYNC_AWAIT(client->RemovePublisher(impl.get()));
  }
#endif

private:
  friend class Server;
  friend class ClientImpl;

  Publisher(std::shared_ptr<ClientImpl> client,
            std::shared_ptr<details::PublisherImpl> impl)
      : client_(client), impl_(impl) {}

  absl::StatusOr<const Message>
  PublishMessageInternal(int64_t message_size, bool omit_prefix,
                         bool use_prefix_slot_id,
                         bool use_prefix_vchan_id = true) {
    return ASYNC_BLOCK(client_->PublishMessageInternal(
        impl_.get(), message_size, omit_prefix, use_prefix_slot_id,
        use_prefix_vchan_id, /*notify_subscribers=*/true));
  }

  std::shared_ptr<ClientImpl> client_;
  std::shared_ptr<details::PublisherImpl> impl_;
  std::function<absl::Status(Publisher *, SlotSizeType, SlotSizeType)>
      resize_callback_ = nullptr;
  std::vector<void *> address_cache_;
  // Set by CloseAsync so that the destructor doesn't remove the publisher a
  // second time (that removal would block).
  bool closed_ = false;
};

inline ScopedPublisherBufferLease::~ScopedPublisherBufferLease() {
  Release().IgnoreError();
}

inline ScopedPublisherBufferLease::ScopedPublisherBufferLease(
    ScopedPublisherBufferLease &&other) noexcept
    : publisher_(other.publisher_), lease_(other.lease_) {
  other.Invalidate();
}

inline absl::Span<std::byte> ScopedPublisherBufferLease::GetMetadata() {
  if (!*this) {
    return {};
  }
  return publisher_->GetMetadata(lease_);
}

inline absl::StatusOr<const Message>
ScopedPublisherBufferLease::Publish(int64_t message_size) {
  if (!*this) {
    return absl::FailedPreconditionError(
        "publisher buffer lease is not active");
  }
  auto result = publisher_->PublishBufferLease(lease_, message_size);
  if (result.ok()) {
    Invalidate();
  }
  return result;
}

inline absl::StatusOr<const Message>
ScopedPublisherBufferLease::PublishCopy(
    absl::Span<const std::byte> payload) {
  if (!*this) {
    return absl::FailedPreconditionError(
        "publisher buffer lease is not active");
  }
  auto result = publisher_->PublishBufferLeaseCopy(lease_, payload);
  if (result.ok()) {
    Invalidate();
  }
  return result;
}

inline absl::Status ScopedPublisherBufferLease::Release() {
  if (!*this) {
    return absl::OkStatus();
  }
  absl::Status status = publisher_->ReleaseBufferLease(lease_);
  if (status.ok()) {
    Invalidate();
  }
  return status;
}

class Subscriber {
public:
  ~Subscriber() {
    if (client_ != nullptr && impl_ != nullptr && !closed_) {
      UnregisterDroppedMessageCallback().IgnoreError();
      UnregisterStuckSubscriberCallback().IgnoreError();
      UnregisterMessageCallback().IgnoreError();
      (void)ASYNC_BLOCK(client_->RemoveSubscriber(impl_.get()));
    }
  }
  Subscriber(const Subscriber &other) = delete;

  Subscriber &operator=(const Subscriber &other) = delete;

  Subscriber(Subscriber &&other)
      : client_(std::move(other.client_)), impl_(std::move(other.impl_)),
        dropped_message_callback_(std::move(other.dropped_message_callback_)),
        stuck_subscriber_callback_(
            std::move(other.stuck_subscriber_callback_)),
        message_callback_(std::move(other.message_callback_)),
        closed_(other.closed_) {
    if (impl_ != nullptr && client_ != nullptr &&
        dropped_message_callback_ != nullptr) {
      client_->UnregisterDroppedMessageCallback(impl_.get()).IgnoreError();
      RegisterDroppedMessageCallback(dropped_message_callback_).IgnoreError();
    }
    if (impl_ != nullptr && client_ != nullptr &&
        stuck_subscriber_callback_ != nullptr) {
      client_->UnregisterStuckSubscriberCallback(impl_.get()).IgnoreError();
      RegisterStuckSubscriberCallback(stuck_subscriber_callback_)
          .IgnoreError();
    }
    if (impl_ != nullptr && client_ != nullptr &&
        message_callback_ != nullptr) {
      client_->UnregisterMessageCallback(impl_.get()).IgnoreError();
      RegisterMessageCallback(message_callback_).IgnoreError();
    }
    other.client_ = nullptr;
    other.impl_ = nullptr;
  }

  Subscriber &operator=(Subscriber &&other) {
    client_ = std::move(other.client_);
    impl_ = std::move(other.impl_);
    dropped_message_callback_ = std::move(other.dropped_message_callback_);
    stuck_subscriber_callback_ = std::move(other.stuck_subscriber_callback_);
    message_callback_ = std::move(other.message_callback_);
    closed_ = other.closed_;
    if (impl_ != nullptr && client_ != nullptr &&
        dropped_message_callback_ != nullptr) {
      client_->UnregisterDroppedMessageCallback(impl_.get()).IgnoreError();
      RegisterDroppedMessageCallback(dropped_message_callback_).IgnoreError();
    }
    if (impl_ != nullptr && client_ != nullptr &&
        stuck_subscriber_callback_ != nullptr) {
      client_->UnregisterStuckSubscriberCallback(impl_.get()).IgnoreError();
      RegisterStuckSubscriberCallback(stuck_subscriber_callback_)
          .IgnoreError();
    }
    if (impl_ != nullptr && client_ != nullptr &&
        message_callback_ != nullptr) {
      client_->UnregisterMessageCallback(impl_.get()).IgnoreError();
      RegisterMessageCallback(message_callback_).IgnoreError();
    }
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
  absl::Status Wait(const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForSubscriber(impl_.get(), c));
  }

  absl::Status Wait(std::chrono::nanoseconds timeout,
                    const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForSubscriber(impl_.get(), timeout, c));
  }

  // Wait until there's a message available to be read by the
  // subscriber.  If the client is coroutine-aware, the coroutine
  // will wait.  If it's not, the function will block on a poll
  // until the subscriber is triggered.
  // This also takes an additional file descriptor that can be used to interrupt
  // the wait.  Returns the integer fd value of the file descriptor that
  // triggered the wait.
  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForSubscriber(impl_.get(), fd, c));
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           std::chrono::nanoseconds timeout,
                           const toolbelt::AsyncWaiter *c = nullptr) {
    return ASYNC_BLOCK(client_->WaitForSubscriber(impl_.get(), fd, timeout, c));
  }

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // Asio overloads: cooperatively wait on a caller-supplied yield_context, the
  // direct analogue of the AsyncWaiter overloads above.
  absl::Status Wait(async::Context ctx) {
    return client_->WaitForSubscriber(impl_.get(), ctx);
  }

  absl::Status Wait(std::chrono::nanoseconds timeout, async::Context ctx) {
    return client_->WaitForSubscriber(impl_.get(), timeout, ctx);
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           async::Context ctx) {
    return client_->WaitForSubscriber(impl_.get(), fd, ctx);
  }

  absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd,
                           std::chrono::nanoseconds timeout,
                           async::Context ctx) {
    return client_->WaitForSubscriber(impl_.get(), fd, timeout, ctx);
  }
#endif

  // Read a message from a subscriber.  If there are no available messages
  // the 'length' field of the returned Message will be zero.  The 'buffer'
  // field of the Message is set to the address of the message in shared
  // memory which is read-only.  If the read is triggered by the PollFd,
  // you must read all the avaiable messages from the subscriber as the
  // PollFd is only triggered when a new message is published.
  // Pass ClearTrigger::kNoClearTrigger to leave the subscriber trigger fd
  // unread.
  absl::StatusOr<Message>
  ReadMessage(ReadMode mode = ReadMode::kReadNext,
              ClearTrigger clear_trigger = ClearTrigger::kClearTrigger) {
    return ASYNC_BLOCK(client_->ReadMessage(impl_.get(), mode, clear_trigger));
  }
  absl::StatusOr<Message>
  ReadMessageFromBatch(ReadMode mode = ReadMode::kReadNext) {
    return ASYNC_BLOCK(client_->ReadMessageFromBatch(impl_.get(), mode));
  }

  // Read and deserialize a telemetry message. Returns nullptr when no message
  // is currently available.
  absl::StatusOr<std::shared_ptr<Telemetry>>
  ReadTelemetryMessage(ReadMode mode = ReadMode::kReadNext);

  // As ReadMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T, typename Aliaser = DefaultAliaser>
  absl::StatusOr<shared_ptr<T, Aliaser>>
  ReadMessage(ReadMode mode = ReadMode::kReadNext,
              ClearTrigger clear_trigger = ClearTrigger::kClearTrigger);

  bool AddActiveMessage(int32_t slot_id) {
    return impl_->AddActiveMessage(impl_->GetSlot(slot_id));
  }
  void RemoveActiveMessage(int32_t slot_id) {
    impl_->RemoveActiveMessage(impl_->GetSlot(slot_id));
  }

  // Find a message given a timestamp.
  absl::StatusOr<Message> FindMessage(uint64_t timestamp) {
    return ASYNC_BLOCK(client_->FindMessage(impl_.get(), timestamp));
  }

  // AsFindMessage above but returns a shared_ptr to the typed message.
  // NOTE: this is subspace::shared_ptr, not std::shared_ptr.
  template <typename T, typename Aliaser = DefaultAliaser>
  absl::StatusOr<shared_ptr<T, Aliaser>> FindMessage(uint64_t timestamp);

  struct pollfd GetPollFd() const {
    return client_->GetPollFd(impl_.get());
  }

  toolbelt::FileDescriptor GetFileDescriptor() const {
    return client_->GetFileDescriptor(impl_.get());
  }

  int64_t GetCurrentOrdinal() const {
    return client_->GetCurrentOrdinal(impl_.get());
  }

  const ChannelCounters &GetChannelCounters() {
    return client_->GetChannelCounters(impl_.get());
  }

  uint64_t GetVirtualMemoryUsage() const {
    return impl_->GetVirtualMemoryUsage();
  }

  std::string Name() const { return impl_->RequestName(); }
  std::string Type() const { return impl_->Type(); }
  std::string_view TypeView() const { return impl_->TypeView(); }

  // Set a custom checksum callback to replace the default CRC32 verification.
  // The callback receives the data spans and a read-only absl::Span of
  // ChecksumSize() bytes containing the stored checksum.  It should write
  // the computed checksum into the temporary buffer provided by the
  // subscriber for comparison.
  void SetChecksumCallback(ChecksumCallback callback) {
    impl_->SetChecksumCallback(std::move(callback));
  }

  // Revert to the built-in CRC32 checksum verification.
  void ResetChecksumCallback() { impl_->ResetChecksumCallback(); }

  // Register a function to be called when a subscriber drops a message.  The
  // function is called with the number of messages that have been missed
  // as its second argument.
  absl::Status RegisterDroppedMessageCallback(
      std::function<void(Subscriber *, int64_t)> callback) {
    auto status = client_->RegisterDroppedMessageCallback(
        impl_.get(), [this](details::SubscriberImpl *, int64_t c) {
          dropped_message_callback_(this, c);
        });
    if (!status.ok()) {
      return status;
    }
    dropped_message_callback_ = std::move(callback);
    return absl::OkStatus();
  }

  absl::Status UnregisterDroppedMessageCallback() {
    auto status = client_->UnregisterDroppedMessageCallback(impl_.get());
    if (!status.ok()) {
      return status;
    }
    dropped_message_callback_ = nullptr;
    return absl::OkStatus();
  }

  // Register a function to be called when max_active_messages has kept the
  // subscriber from reading a waiting message for the stuck warning grace
  // period.  The function is called with the number of seconds the subscriber
  // has been stuck as its second argument.  It replaces the logged error and
  // is called once until the subscriber reads a message again.  The
  // warn_when_stuck option enables it.
  absl::Status RegisterStuckSubscriberCallback(
      std::function<void(Subscriber *, double)> callback) {
    auto status = client_->RegisterStuckSubscriberCallback(
        impl_.get(), [this](details::SubscriberImpl *, double seconds) {
          stuck_subscriber_callback_(this, seconds);
        });
    if (!status.ok()) {
      return status;
    }
    stuck_subscriber_callback_ = std::move(callback);
    return absl::OkStatus();
  }

  absl::Status UnregisterStuckSubscriberCallback() {
    auto status = client_->UnregisterStuckSubscriberCallback(impl_.get());
    if (!status.ok()) {
      return status;
    }
    stuck_subscriber_callback_ = nullptr;
    return absl::OkStatus();
  }

  absl::Status
  RegisterMessageCallback(std::function<void(Subscriber *, Message)> callback) {
    auto status = client_->RegisterMessageCallback(
        impl_.get(), [this](details::SubscriberImpl *, Message m) {
          message_callback_(this, std::move(m));
        });
    if (!status.ok()) {
      return status;
    }
    message_callback_ = std::move(callback);
    return absl::OkStatus();
  }

  absl::Status UnregisterMessageCallback() {
    auto status = client_->UnregisterMessageCallback(impl_.get());
    if (!status.ok()) {
      return status;
    }
    message_callback_ = nullptr;
    return absl::OkStatus();
  }

  void InvokeMessageCallback(Message msg) {
    client_->InvokeMessageCallback(impl_.get(), std::move(msg));
  }

  void SetOnReceiveCallback(
      std::function<absl::StatusOr<int64_t>(void *buffer, int64_t size)>
          callback) {
    impl_->SetOnReceiveCallback(std::move(callback));
  }

  void ClearOnReceiveCallback() { impl_->SetOnReceiveCallback(nullptr); }

  absl::Status ProcessAllMessages(ReadMode mode = ReadMode::kReadNext) {
    return ASYNC_BLOCK(client_->ProcessAllMessages(impl_.get(), mode));
  }

  absl::StatusOr<std::vector<Message>>
  GetAllMessages(ReadMode mode = ReadMode::kReadNext) {
    return ASYNC_BLOCK(client_->GetAllMessages(impl_.get(), mode));
  }

  void Trigger() { impl_->Trigger(); }
  void Untrigger() { impl_->Untrigger(); }

  bool IsPlaceholder() const { return impl_->IsPlaceholder(); }

  const ChannelCounters &GetCounters() const { return impl_->GetCounters(); }

  int64_t CurrentOrdinal() const { return impl_->CurrentOrdinal(); }
  int64_t Timestamp() const { return impl_->Timestamp(); }
  bool IsReliable() const { return impl_->IsReliable(); }
  bool ForTunnel() const { return impl_->ForTunnel(); }
  // True when the attached channel stores prefixes separately from payload
  // slots. Subscribers learn this from the server response; they do not request
  // it. In this mode each payload slot has an allocator-defined handle, such as
  // a Qualcomm memory-pool handle, that matching map callbacks can use to map
  // the payload into the subscriber process.
  bool UsesSplitBuffers() const { return impl_->UsesSplitBuffers(); }

  SlotSizeType SlotSize() const {
    return static_cast<SlotSizeType>(impl_->SlotSize());
  }
  int32_t NumSlots() const { return impl_->NumSlots(); }
  int32_t SubscriberQueueSize() const { return impl_->SubscriberQueueSize(); }

  const std::vector<std::unique_ptr<details::BufferSet>> &GetBuffers() const {
    return client_->GetBuffers(impl_.get());
  }

  bool GetAddresses(void ***addresses, size_t *count) {
    if (addresses != nullptr) {
      *addresses = nullptr;
    }
    if (count != nullptr) {
      *count = 0;
    }
    if (addresses == nullptr || count == nullptr || impl_->IsPlaceholder() ||
        impl_->GetBuffers().empty()) {
      return false;
    }
    const details::BufferSet *latest = impl_->GetBuffers().back().get();
    address_cache_.clear();
    address_cache_.reserve(static_cast<size_t>(impl_->NumSlots()));
    if (latest->IsSplitBuffers()) {
      if (latest->split_slot_buffers.size() <
          static_cast<size_t>(impl_->NumSlots())) {
        return false;
      }
      for (int slot = 0; slot < impl_->NumSlots(); ++slot) {
        address_cache_.push_back(latest->split_slot_buffers[slot]);
      }
    } else {
      if (latest->buffer == nullptr) {
        return false;
      }
      uint64_t stride = impl_->PrefixSize() + Aligned<64>(latest->slot_size);
      for (int slot = 0; slot < impl_->NumSlots(); ++slot) {
        address_cache_.push_back(latest->buffer + stride * slot +
                                 impl_->PrefixSize());
      }
    }
    *addresses = address_cache_.data();
    *count = address_cache_.size();
    return true;
  }

  int NumActiveMessages() const { return impl_->NumActiveMessages(); }

  void DumpSlots(std::ostream &os) const { impl_->DumpSlots(os); }

  int VirtualChannelId() const { return impl_->VirtualChannelId(); }

  int ConfiguredVchanId() const { return impl_->ConfiguredVchanId(); }

  std::string Mux() const { return impl_->Mux(); }

  int NumSubscribers(int vchan_id = -1) const {
    return impl_->NumSubscribers(vchan_id);
  }

  // If you don't want to hold on to the current active message in the
  // subscriber, you can call this.
  void ClearActiveMessage() { impl_->ClearActiveMessage(); }

  void TriggerReliablePublishers() { impl_->TriggerReliablePublishers(); }

  MessageSlot *GetSlot(int slot_id) const { return impl_->GetSlot(slot_id); }
  MessagePrefix *Prefix(MessageSlot *slot = nullptr) const {
    return impl_->Prefix(slot != nullptr ? slot : impl_->CurrentSlot());
  }
  // If `address` points at a split payload buffer mapped by this subscriber,
  // writes that slot's allocator handle to `handle` and returns true. Use this
  // when handing the payload to another library that understands the allocator
  // handle, not just the mapped CPU address.
  bool GetSplitBufferHandleFromAddress(const void *address,
                                       uintptr_t *handle) const {
    return impl_->GetSplitBufferHandleFromAddress(address, handle);
  }
  // Returns the handles for the current split-buffer set, one handle per slot.
  // The returned array is owned by the subscriber and remains valid until the
  // subscriber reloads, the channel resizes, or the subscriber is destroyed.
  // Handles are meaningful only to the allocator/callbacks that map the payload
  // buffers.
  bool GetSplitBufferHandles(uintptr_t **handles, size_t *count) {
    return impl_->GetSplitBufferHandles(handles, count);
  }

  // Total prefix area size in bytes (always a multiple of 64).
  // Determined by ChecksumSize() and MetadataSize().
  int32_t PrefixSize() const { return impl_->PrefixSize(); }

  // Number of bytes reserved for the checksum (default 4 for CRC32).
  int32_t ChecksumSize() const { return impl_->ChecksumSize(); }

  // Number of bytes of user metadata in the prefix area (default 0).
  int32_t MetadataSize() const { return impl_->MetadataSize(); }

  // Get a read-only span over the user metadata area in the most
  // recently read message's prefix.  Only valid while the message is
  // active (between ReadMessage() and the next ReadMessage() or
  // ClearActiveMessage()).  Returns an empty span if MetadataSize() is 0.
  absl::Span<const std::byte> GetMetadata() { return impl_->GetMetadata(); }

  bool AtomicIncRefCount(int slot_id, int inc) {
    MessageSlot *slot = impl_->GetSlot(slot_id);
    if (slot != nullptr) {
      return impl_->AtomicIncRefCount(
          slot, IsReliable(), inc,
          slot->ordinal.load(std::memory_order_relaxed),
          slot->vchan_id.load(std::memory_order_relaxed), false);
    }
    return false;
  }

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // ---- Async API (C++20 coroutines) ------------------------------------
  //
  // The same as the functions without "Async", but they suspend the calling
  // coroutine instead of blocking the thread.  See the Publisher Async API
  // for the rules on using them.

  async::Task<absl::Status>
  WaitAsync(std::chrono::nanoseconds timeout = std::chrono::nanoseconds(0)) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->WaitForSubscriber(impl.get(), timeout, nullptr));
  }

  async::Task<absl::StatusOr<int>>
  WaitAsync(const toolbelt::FileDescriptor &fd,
            std::chrono::nanoseconds timeout = std::chrono::nanoseconds(0)) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->WaitForSubscriber(impl.get(), fd, timeout, nullptr));
  }

  async::Task<absl::StatusOr<Message>>
  ReadMessageAsync(ReadMode mode = ReadMode::kReadNext,
                   ClearTrigger clear_trigger = ClearTrigger::kClearTrigger) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        client->ReadMessage(impl.get(), mode, clear_trigger));
  }

  template <typename T, typename Aliaser = DefaultAliaser>
  async::Task<absl::StatusOr<shared_ptr<T, Aliaser>>>
  ReadMessageAsync(ReadMode mode = ReadMode::kReadNext,
                   ClearTrigger clear_trigger = ClearTrigger::kClearTrigger) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        (client->ReadMessage<T, Aliaser>(impl.get(), mode, clear_trigger)));
  }

  async::Task<absl::StatusOr<Message>> FindMessageAsync(uint64_t timestamp) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(client->FindMessage(impl.get(), timestamp));
  }

  template <typename T, typename Aliaser = DefaultAliaser>
  async::Task<absl::StatusOr<shared_ptr<T, Aliaser>>>
  FindMessageAsync(uint64_t timestamp) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        (client->FindMessage<T, Aliaser>(impl.get(), timestamp)));
  }

  async::Task<absl::Status>
  ProcessAllMessagesAsync(ReadMode mode = ReadMode::kReadNext) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(client->ProcessAllMessages(impl.get(), mode));
  }

  async::Task<absl::StatusOr<std::vector<Message>>>
  GetAllMessagesAsync(ReadMode mode = ReadMode::kReadNext) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(client->GetAllMessages(impl.get(), mode));
  }

  // Removes the subscriber from the server without blocking.  The destructor
  // does the same but blocks.  The subscriber can't be used afterwards.
  async::Task<absl::Status> CloseAsync() {
    if (client_ == nullptr || impl_ == nullptr || closed_) {
      co_return absl::OkStatus();
    }
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    closed_ = true;
    UnregisterDroppedMessageCallback().IgnoreError();
    UnregisterStuckSubscriberCallback().IgnoreError();
    UnregisterMessageCallback().IgnoreError();
    co_return ASYNC_AWAIT(client->RemoveSubscriber(impl.get()));
  }
#endif

private:
  friend class Server;
  friend class ClientImpl;

  Subscriber(std::shared_ptr<ClientImpl> client,
             std::shared_ptr<details::SubscriberImpl> impl)
      : client_(client), impl_(impl) {}

  absl::StatusOr<Message>
  ReadMessageInternal(ReadMode mode, bool pass_activation, bool clear_trigger) {
    return ASYNC_BLOCK(client_->ReadMessageInternal(
        impl_.get(), mode, pass_activation, clear_trigger));
  }

  // Awaitable form for code on the scheduler, such as the in-process bridge.
  async::Task<absl::StatusOr<Message>>
  ReadMessageInternalAsync(ReadMode mode, bool pass_activation,
                           bool clear_trigger) {
    auto client = client_;
    auto impl = impl_;
    if (absl::Status s = client->CheckAsyncAllowed(); !s.ok()) {
      ASYNC_RETURN s;
    }
    ASYNC_RETURN ASYNC_AWAIT(client->ReadMessageInternal(
        impl.get(), mode, pass_activation, clear_trigger));
  }

  std::shared_ptr<ClientImpl> client_;
  std::shared_ptr<details::SubscriberImpl> impl_;
  std::vector<void *> address_cache_;
  std::function<void(Subscriber *, int64_t)> dropped_message_callback_ =
      nullptr;
  std::function<void(Subscriber *, double)> stuck_subscriber_callback_ =
      nullptr;
  std::function<void(Subscriber *, Message)> message_callback_ = nullptr;
  // Set by CloseAsync so the destructor doesn't remove the subscriber again.
  bool closed_ = false;
};

template <typename T, typename Aliaser>
inline absl::StatusOr<::subspace::shared_ptr<T, Aliaser>>
Subscriber::ReadMessage(ReadMode mode, ClearTrigger clear_trigger) {
  return ASYNC_BLOCK(
      client_->ReadMessage<T, Aliaser>(impl_.get(), mode, clear_trigger));
}

template <typename T, typename Aliaser>
inline absl::StatusOr<::subspace::shared_ptr<T, Aliaser>>
Subscriber::FindMessage(uint64_t timestamp) {
  return ASYNC_BLOCK(client_->FindMessage<T, Aliaser>(impl_.get(), timestamp));
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
  Create(const std::string &server_socket = kDefaultServerSocket,
         const std::string &client_name = "",
         const toolbelt::AsyncWaiter *c = nullptr) {
    auto client = std::make_shared<Client>(c);
    auto status = client->Init(server_socket, client_name);
    if (!status.ok()) {
      return status;
    }
    return client;
  }

  Client(const toolbelt::AsyncWaiter *c = nullptr)
      : impl_(std::make_shared<ClientImpl>(c)) {}
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // The Async API suspends whichever coroutine calls it (co20::self), so a
  // co20 client doesn't need the coroutine it is created in.  The context is
  // accepted so code written for the other backends compiles unchanged.
  explicit Client(async::Context) : impl_(std::make_shared<ClientImpl>()) {}
#endif
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // Asio-backed client: the yield_context drives cooperative socket I/O.
  explicit Client(async::Context ctx)
      : impl_(std::make_shared<ClientImpl>(ctx)) {}
#endif
  ~Client() = default;

  const std::string &GetName() const { return impl_->GetName(); }

  // Initialize the client by connecting to the server.
  absl::Status Init(const std::string &server_socket = kDefaultServerSocket,
                    const std::string &client_name = "") {
    return ASYNC_BLOCK(impl_->Init(server_socket, client_name));
  }

  // Create a publisher for the given channel.  If the channel doesn't exit
  // it will be created with num_slots slots, each of which is slot_size
  // bytes long.
  absl::StatusOr<Publisher>
  CreatePublisher(const std::string &channel_name, SlotSizeType slot_size,
                  int num_slots,
                  const PublisherOptions &opts = PublisherOptions()) {
    return ASYNC_BLOCK(impl_->CreatePublisher(channel_name, slot_size, num_slots, opts));
  }

  // If you prefer, you can create the publisher with the slot size and
  // number of slots set in the options.
  absl::StatusOr<Publisher>
  CreatePublisher(const std::string &channel_name,
                  const PublisherOptions &opts = PublisherOptions()) {
    return ASYNC_BLOCK(impl_->CreatePublisher(channel_name, opts));
  }

  // Create a subscriber for the given channel. This can be done before there
  // are any publishers on the channel.
  absl::StatusOr<Subscriber>
  CreateSubscriber(const std::string &channel_name,
                   const SubscriberOptions &opts = SubscriberOptions()) {
    return ASYNC_BLOCK(impl_->CreateSubscriber(channel_name, opts));
  }

  // Call with true to turn on some debug information.  Kind of meaningless
  // information unless you know how this works in detail.
  void SetDebug(bool v) { impl_->SetDebug(v); }

  void SetThreadSafe(bool v) { impl_->SetThreadSafe(v); }

  // Test-only.  The hook runs inside ReadMessage and FindMessage when a
  // placeholder subscriber has just checked the server for a new publisher,
  // so a test can make a publisher appear at exactly that point.
  void SetPlaceholderCheckHookForTesting(std::function<void()> hook) {
    impl_->placeholder_check_hook_ = std::move(hook);
  }

  absl::StatusOr<const ChannelCounters>
  GetChannelCounters(const std::string &channel_name) const {
    return impl_->GetChannelCounters(channel_name);
  }

  absl::StatusOr<const ChannelInfo>
  GetChannelInfo(const std::string &channelName) {
    return ASYNC_BLOCK(impl_->GetChannelInfo(channelName));
  }
  absl::StatusOr<const std::vector<ChannelInfo>> GetChannelInfo() {
    return ASYNC_BLOCK(impl_->GetChannelInfo());
  }

  absl::StatusOr<const ChannelStats>
  GetChannelStats(const std::string &channelName) {
    return ASYNC_BLOCK(impl_->GetChannelStats(channelName));
  }
  absl::StatusOr<const std::vector<ChannelStats>> GetChannelStats() {
    return ASYNC_BLOCK(impl_->GetChannelStats());
  }

  absl::StatusOr<bool> ChannelExists(const std::string &channelName) {
    return ASYNC_BLOCK(impl_->ChannelExists(channelName));
  }

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
  // ---- Async API (C++20 coroutines) ------------------------------------
  //
  // The same as the functions without "Async", but they suspend the calling
  // coroutine instead of blocking the thread.  See the Publisher Async API
  // for the rules on using them.  The Client must not be thread safe.
  async::Task<absl::Status>
  InitAsync(const std::string &server_socket = kDefaultServerSocket,
            const std::string &client_name = "") {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->Init(server_socket, client_name));
  }

  async::Task<absl::StatusOr<Publisher>>
  CreatePublisherAsync(const std::string &channel_name, SlotSizeType slot_size,
                       int num_slots,
                       const PublisherOptions &opts = PublisherOptions()) {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(
        impl->CreatePublisher(channel_name, slot_size, num_slots, opts));
  }

  async::Task<absl::StatusOr<Publisher>>
  CreatePublisherAsync(const std::string &channel_name,
                       const PublisherOptions &opts = PublisherOptions()) {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->CreatePublisher(channel_name, opts));
  }

  async::Task<absl::StatusOr<Subscriber>>
  CreateSubscriberAsync(const std::string &channel_name,
                        const SubscriberOptions &opts = SubscriberOptions()) {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->CreateSubscriber(channel_name, opts));
  }

  async::Task<absl::StatusOr<const ChannelInfo>>
  GetChannelInfoAsync(const std::string &channelName) {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->GetChannelInfo(channelName));
  }

  async::Task<absl::StatusOr<const std::vector<ChannelInfo>>>
  GetChannelInfoAsync() {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->GetChannelInfo());
  }

  async::Task<absl::StatusOr<const ChannelStats>>
  GetChannelStatsAsync(const std::string &channelName) {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->GetChannelStats(channelName));
  }

  async::Task<absl::StatusOr<const std::vector<ChannelStats>>>
  GetChannelStatsAsync() {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->GetChannelStats());
  }

  async::Task<absl::StatusOr<bool>>
  ChannelExistsAsync(const std::string &channelName) {
    auto impl = impl_;
    if (absl::Status s = impl->CheckAsyncAllowed(); !s.ok()) {
      co_return s;
    }
    co_return ASYNC_AWAIT(impl->ChannelExists(channelName));
  }
#endif

private:
  std::shared_ptr<ClientImpl> impl_;
};

// Convenience functions to create a client and publisher/subscriber in one
// step. The Publisher and Subscriber objects hold a shared_ptr to the
// ClientImpl internally, so the client stays alive as long as the returned
// object does.

inline absl::StatusOr<Publisher>
CreatePublisher(const std::string &channel_name,
                const PublisherOptions &opts = PublisherOptions(),
                const std::string &server_socket = "/tmp/subspace",
                const std::string &client_name = "",
                const toolbelt::AsyncWaiter *c = nullptr) {
  auto client_or = Client::Create(server_socket, client_name, c);
  if (!client_or.ok()) {
    return client_or.status();
  }
  return (*client_or)->CreatePublisher(channel_name, opts);
}

inline absl::StatusOr<Subscriber>
CreateSubscriber(const std::string &channel_name,
                 const SubscriberOptions &opts = SubscriberOptions(),
                 const std::string &server_socket = "/tmp/subspace",
                 const std::string &client_name = "",
                 const toolbelt::AsyncWaiter *c = nullptr) {
  auto client_or = Client::Create(server_socket, client_name, c);
  if (!client_or.ok()) {
    return client_or.status();
  }
  return (*client_or)->CreateSubscriber(channel_name, opts);
}

} // namespace subspace

#endif // _xCLIENT_CLIENT_H
