// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_CLIENT_H_
#define CLIENT_ASIO_ASIO_CLIENT_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client/message.h"
#include "client/options.h"
#include "client/publisher.h"
#include "client/subscriber.h"
#include "client/types.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace subspace {

class AsioClient;

class AsioPublisher {
public:
  ~AsioPublisher();
  AsioPublisher(const AsioPublisher &) = delete;
  AsioPublisher &operator=(const AsioPublisher &) = delete;
  AsioPublisher(AsioPublisher &&) noexcept = default;
  AsioPublisher &operator=(AsioPublisher &&) noexcept = default;

  absl::StatusOr<absl::Span<std::byte>>
  GetMessageBuffer(SlotSizeType max_size = -1);
  absl::StatusOr<Message> PublishMessage(int64_t message_size);

  template <typename CompletionToken>
  auto AsyncGetMessageBuffer(SlotSizeType max_size, CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncPublishMessage(int64_t message_size, CompletionToken &&token);

  bool IsReliable() const;
  int32_t NumSubscribers() const;
  std::string Name() const;
  struct pollfd GetPollFd() const;

private:
  friend class AsioClient;
  AsioPublisher(std::shared_ptr<AsioClient> client,
                std::shared_ptr<details::PublisherImpl> impl);

  std::shared_ptr<AsioClient> client_;
  std::shared_ptr<details::PublisherImpl> impl_;
};

class AsioSubscriber : public std::enable_shared_from_this<AsioSubscriber> {
public:
  ~AsioSubscriber();
  AsioSubscriber(const AsioSubscriber &) = delete;
  AsioSubscriber &operator=(const AsioSubscriber &) = delete;

  void Start(std::function<void(Message &&)> on_message);
  void Shutdown();

  template <typename CompletionToken>
  auto AsyncReadMessage(ReadMode mode, CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncReadMessageWithoutClearingTrigger(ReadMode mode,
                                              CompletionToken &&token);

  std::string Name() const;
  std::string_view Type() const;
  int32_t NumSlots() const;
  SlotSizeType SlotSize() const;
  int32_t ChecksumSize() const;
  int32_t MetadataSize() const;
  ChannelCounters GetChannelCounters() const;
  struct pollfd GetPollFd() const;

private:
  friend class AsioClient;
  AsioSubscriber(std::shared_ptr<AsioClient> client,
                 std::shared_ptr<details::SubscriberImpl> impl);

  template <typename CompletionToken>
  auto AsyncReadMessageImpl(ReadMode mode, ClearTrigger clear_trigger,
                            CompletionToken &&token);
  void ResetEventFd();
  void WaitForNotification();
  void Drain();

  std::shared_ptr<AsioClient> client_;
  std::shared_ptr<details::SubscriberImpl> impl_;
  boost::asio::posix::stream_descriptor event_fd_;
  std::function<void(Message &&)> on_message_;
  std::atomic_bool running_{false};
};

class AsioClient : public std::enable_shared_from_this<AsioClient> {
public:
  using Executor = boost::asio::any_io_executor;

  ~AsioClient();
  AsioClient(const AsioClient &) = delete;
  AsioClient &operator=(const AsioClient &) = delete;

  template <typename CompletionToken>
  static auto AsyncCreate(Executor executor, std::string server_socket,
                          std::string client_name, CompletionToken &&token);

  const Executor &GetExecutor() const { return executor_; }
  void SetDebug(bool enabled) { debug_ = enabled; }

  template <typename CompletionToken>
  auto AsyncCreatePublisher(std::string channel_name, PublisherOptions options,
                            CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncCreateSubscriber(std::string channel_name,
                             SubscriberOptions options,
                             CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncGetChannelInfo(std::string channel_name, CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncGetChannelStats(std::string channel_name, CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncChannelExists(std::string channel_name, CompletionToken &&token);

private:
  friend class AsioPublisher;
  friend class AsioSubscriber;

  struct RpcResult {
    Response response;
    std::vector<toolbelt::FileDescriptor> fds;
  };
  using ControlCompletion = std::function<void(absl::StatusOr<RpcResult>)>;
  struct ControlCommand {
    std::vector<char> outbound;
    ControlCompletion completion;
  };

  explicit AsioClient(Executor executor);
  void Init(std::string socket, std::string name,
            std::function<void(absl::Status)> completion);
  void Enqueue(Request request, ControlCompletion completion);
  void StartNext();
  void ReadResponseLength();
  void ReadResponseBody();
  void ReceiveFds();
  void Finish(absl::StatusOr<RpcResult> result);
  void Fail(absl::Status status);

  absl::StatusOr<AsioPublisher> BuildPublisher(const std::string &name,
                                               const PublisherOptions &options,
                                               RpcResult result);
  absl::StatusOr<std::shared_ptr<AsioSubscriber>>
  BuildSubscriber(const std::string &name, const SubscriberOptions &options,
                  RpcResult result);
  absl::StatusOr<absl::Span<std::byte>>
  GetMessageBuffer(details::PublisherImpl *publisher, SlotSizeType max_size);
  void AcquireMessageBuffer(
      std::shared_ptr<details::PublisherImpl> publisher, SlotSizeType max_size,
      std::function<void(absl::StatusOr<absl::Span<std::byte>>)> completion);
  absl::StatusOr<Message> PublishMessage(details::PublisherImpl *publisher,
                                         int64_t message_size);
  void RefreshPublisher(std::shared_ptr<details::PublisherImpl> publisher,
                        std::function<void(absl::Status)> completion);
  void RefreshSubscriber(std::shared_ptr<details::SubscriberImpl> subscriber,
                         std::function<void(absl::Status, bool)> completion);
  absl::StatusOr<Message> ReadMessage(details::SubscriberImpl *subscriber,
                                      ReadMode mode,
                                      ClearTrigger clear_trigger);
  void RemovePublisher(std::shared_ptr<details::PublisherImpl> publisher);
  void RemoveSubscriber(std::shared_ptr<details::SubscriberImpl> subscriber);

  static void FillPublisherRequest(CreatePublisherRequest *request,
                                   const std::string &name,
                                   const PublisherOptions &options);
  static void FillSubscriberRequest(CreateSubscriberRequest *request,
                                    const std::string &name,
                                    const SubscriberOptions &options);

  Executor executor_;
  boost::asio::strand<Executor> strand_;
  boost::asio::local::stream_protocol::socket socket_;
  std::deque<ControlCommand> commands_;
  bool active_ = false;
  std::array<char, sizeof(uint32_t)> length_{};
  uint32_t response_size_ = 0;
  std::vector<char> response_body_;
  RpcResult result_;
  std::vector<char> control_buffer_;
  int expected_fds_ = 0;
  std::string name_;
  uint64_t session_id_ = 0;
  int server_user_id_ = 0;
  int server_group_id_ = 0;
  toolbelt::FileDescriptor scb_fd_;
  bool debug_ = false;
};

namespace asio_detail {

template <typename Handler, typename Result>
void Complete(const AsioClient::Executor &fallback, Handler handler,
              Result result) {
  auto executor = boost::asio::get_associated_executor(handler, fallback);
  boost::asio::post(executor, [handler = std::move(handler),
                               result = std::move(result)]() mutable {
    std::move(handler)(std::move(result));
  });
}

} // namespace asio_detail

template <typename CompletionToken>
auto AsioClient::AsyncCreate(Executor executor, std::string server_socket,
                             std::string client_name, CompletionToken &&token) {
  using Signature = void(absl::StatusOr<std::shared_ptr<AsioClient>>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [executor = std::move(executor), server_socket = std::move(server_socket),
       client_name = std::move(client_name)](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        auto fallback = executor;
        auto work = boost::asio::make_work_guard(executor);
        auto client =
            std::shared_ptr<AsioClient>(new AsioClient(std::move(executor)));
        client->Init(
            std::move(server_socket), std::move(client_name),
            [client, fallback, work = std::move(work),
             completion = std::move(completion)](absl::Status status) mutable {
              asio_detail::Complete(
                  fallback, std::move(*completion),
                  status.ok()
                      ? absl::StatusOr<std::shared_ptr<AsioClient>>(client)
                      : absl::StatusOr<std::shared_ptr<AsioClient>>(status));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClient::AsyncCreatePublisher(std::string channel_name,
                                      PublisherOptions options,
                                      CompletionToken &&token) {
  using Signature = void(absl::StatusOr<AsioPublisher>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(), channel_name = std::move(channel_name),
       options = std::move(options)](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        Request request;
        FillPublisherRequest(request.mutable_create_publisher(), channel_name,
                             options);
        self->Enqueue(
            std::move(request),
            [self, channel_name = std::move(channel_name),
             options = std::move(options), completion = std::move(completion)](
                absl::StatusOr<RpcResult> result) mutable {
              asio_detail::Complete(
                  self->executor_, std::move(*completion),
                  result.ok() ? self->BuildPublisher(channel_name, options,
                                                     std::move(result).value())
                              : absl::StatusOr<AsioPublisher>(result.status()));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClient::AsyncCreateSubscriber(std::string channel_name,
                                       SubscriberOptions options,
                                       CompletionToken &&token) {
  using Signature = void(absl::StatusOr<std::shared_ptr<AsioSubscriber>>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(), channel_name = std::move(channel_name),
       options = std::move(options)](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        Request request;
        FillSubscriberRequest(request.mutable_create_subscriber(), channel_name,
                              options);
        self->Enqueue(
            std::move(request),
            [self, channel_name = std::move(channel_name),
             options = std::move(options), completion = std::move(completion)](
                absl::StatusOr<RpcResult> result) mutable {
              asio_detail::Complete(
                  self->executor_, std::move(*completion),
                  result.ok() ? self->BuildSubscriber(channel_name, options,
                                                      std::move(result).value())
                              : absl::StatusOr<std::shared_ptr<AsioSubscriber>>(
                                    result.status()));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClient::AsyncGetChannelInfo(std::string channel_name,
                                     CompletionToken &&token) {
  using Signature = void(absl::StatusOr<ChannelInfo>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(),
       channel_name = std::move(channel_name)](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        Request request;
        request.mutable_get_channel_info()->set_channel_name(channel_name);
        self->Enqueue(
            std::move(request), [self, completion = std::move(completion)](
                                    absl::StatusOr<RpcResult> result) mutable {
              absl::StatusOr<ChannelInfo> output =
                  absl::InternalError("invalid channel-info response");
              if (!result.ok()) {
                output = result.status();
              } else {
                const auto &response = result->response.get_channel_info();
                if (!response.error().empty()) {
                  output = absl::InternalError(response.error());
                } else if (response.channels_size() == 1) {
                  const auto &info = response.channels(0);
                  ChannelInfo value{};
                  value.channel_name = info.name();
                  value.num_publishers = info.num_pubs();
                  value.num_subscribers = info.num_subs();
                  value.num_bridge_pubs = info.num_bridge_pubs();
                  value.num_bridge_subs = info.num_bridge_subs();
                  value.num_tunnel_pubs = info.num_tunnel_pubs();
                  value.num_tunnel_subs = info.num_tunnel_subs();
                  value.reliable = info.is_reliable();
                  value.type = info.type();
                  value.slot_size = info.slot_size();
                  value.num_slots = info.num_slots();
                  value.subscriber_queue_size = info.subscriber_queue_size();
                  value.subscriber_queue_arena_size =
                      info.subscriber_queue_arena_size();
                  value.is_local = info.is_local();
                  output = std::move(value);
                }
              }
              asio_detail::Complete(self->executor_, std::move(*completion),
                                    std::move(output));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClient::AsyncGetChannelStats(std::string channel_name,
                                      CompletionToken &&token) {
  using Signature = void(absl::StatusOr<ChannelStats>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(),
       channel_name = std::move(channel_name)](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        Request request;
        request.mutable_get_channel_stats()->set_channel_name(channel_name);
        self->Enqueue(
            std::move(request), [self, completion = std::move(completion)](
                                    absl::StatusOr<RpcResult> result) mutable {
              absl::StatusOr<ChannelStats> output =
                  absl::InternalError("invalid channel-stats response");
              if (!result.ok()) {
                output = result.status();
              } else {
                const auto &response = result->response.get_channel_stats();
                if (!response.error().empty()) {
                  output = absl::InternalError(response.error());
                } else if (response.channels_size() == 1) {
                  const auto &stats = response.channels(0);
                  output = ChannelStats{
                      stats.channel_name(),
                      static_cast<uint64_t>(stats.total_bytes()),
                      static_cast<uint64_t>(stats.total_messages()),
                      static_cast<uint64_t>(stats.max_message_size()),
                      stats.is_local()};
                }
              }
              asio_detail::Complete(self->executor_, std::move(*completion),
                                    std::move(output));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClient::AsyncChannelExists(std::string channel_name,
                                    CompletionToken &&token) {
  using Signature = void(absl::StatusOr<bool>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(),
       channel_name = std::move(channel_name)](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        self->AsyncGetChannelInfo(
            std::move(channel_name),
            [self, completion = std::move(completion)](
                absl::StatusOr<ChannelInfo> result) mutable {
              asio_detail::Complete(self->executor_, std::move(*completion),
                                    absl::StatusOr<bool>(result.ok()));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioPublisher::AsyncGetMessageBuffer(SlotSizeType max_size,
                                          CompletionToken &&token) {
  using Signature = void(absl::StatusOr<absl::Span<std::byte>>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this, max_size](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        auto client = client_;
        auto impl = impl_;
        client->AcquireMessageBuffer(
            impl, max_size,
            [client, completion = std::move(completion)](
                absl::StatusOr<absl::Span<std::byte>> result) mutable {
              asio_detail::Complete(client->GetExecutor(),
                                    std::move(*completion), std::move(result));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioPublisher::AsyncPublishMessage(int64_t message_size,
                                        CompletionToken &&token) {
  using Signature = void(absl::StatusOr<Message>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this, message_size](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        auto client = client_;
        auto impl = impl_;
        client->RefreshPublisher(impl, [client, impl, message_size,
                                        completion = std::move(completion)](
                                           absl::Status status) mutable {
          asio_detail::Complete(
              client->GetExecutor(), std::move(*completion),
              status.ok() ? client->PublishMessage(impl.get(), message_size)
                          : absl::StatusOr<Message>(status));
        });
      },
      token);
}

template <typename CompletionToken>
auto AsioSubscriber::AsyncReadMessage(ReadMode mode, CompletionToken &&token) {
  return AsyncReadMessageImpl(mode, ClearTrigger::kClearTrigger,
                              std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto AsioSubscriber::AsyncReadMessageWithoutClearingTrigger(
    ReadMode mode, CompletionToken &&token) {
  return AsyncReadMessageImpl(mode, ClearTrigger::kNoClearTrigger,
                              std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto AsioSubscriber::AsyncReadMessageImpl(ReadMode mode,
                                          ClearTrigger clear_trigger,
                                          CompletionToken &&token) {
  using Signature = void(absl::StatusOr<Message>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(), mode, clear_trigger](auto handler) mutable {
        auto completion = std::make_shared<std::decay_t<decltype(handler)>>(
            std::move(handler));
        auto client = self->client_;
        auto impl = self->impl_;
        client->RefreshSubscriber(
            impl, [self, client, impl, mode, clear_trigger,
                   completion = std::move(completion)](absl::Status status,
                                                       bool reloaded) mutable {
              if (reloaded) {
                self->ResetEventFd();
              }
              asio_detail::Complete(
                  client->GetExecutor(), std::move(*completion),
                  status.ok()
                      ? client->ReadMessage(impl.get(), mode, clear_trigger)
                      : absl::StatusOr<Message>(status));
            });
      },
      token);
}

} // namespace subspace

#endif // CLIENT_ASIO_ASIO_CLIENT_H_
