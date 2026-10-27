// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_CLIENT_H_
#define CLIENT_ASIO_ASIO_CLIENT_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "client/message.h"
#include "client/options.h"
#include "client/publisher.h"
#include "client/subscriber.h"
#include "client/types.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/system_executor.hpp>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace subspace {

class AsioClient;
class AsioClientState;

class AsioPublisher {
public:
  ~AsioPublisher();
  AsioPublisher(const AsioPublisher &) = delete;
  AsioPublisher &operator=(const AsioPublisher &) = delete;
  AsioPublisher(AsioPublisher &&) noexcept = default;
  AsioPublisher &operator=(AsioPublisher &&other) noexcept;

  template <typename CompletionToken>
  auto AsyncGetMessageBuffer(SlotSizeType max_size, CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncPublishMessage(int64_t message_size, CompletionToken &&token);

  bool IsReliable() const;
  int32_t NumSubscribers() const;
  std::string Name() const;
  struct pollfd GetPollFd() const;

private:
  friend class AsioClientState;
  AsioPublisher(
      std::shared_ptr<AsioClientState> client,
      std::shared_ptr<details::PublisherImpl> impl,
      std::shared_ptr<boost::asio::posix::stream_descriptor> poll_descriptor);

  std::shared_ptr<AsioClientState> client_;
  std::shared_ptr<details::PublisherImpl> impl_;
  std::shared_ptr<boost::asio::posix::stream_descriptor> poll_descriptor_;
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
  friend class AsioClientState;
  AsioSubscriber(
      std::shared_ptr<AsioClientState> client,
      std::shared_ptr<details::SubscriberImpl> impl,
      std::shared_ptr<boost::asio::posix::stream_descriptor> event_fd);

  template <typename CompletionToken>
  auto AsyncReadMessageImpl(ReadMode mode, ClearTrigger clear_trigger,
                            CompletionToken &&token);
  absl::Status ResetEventFd();
  void WaitForNotification();
  void Drain();

  std::shared_ptr<AsioClientState> client_;
  std::shared_ptr<details::SubscriberImpl> impl_;
  boost::asio::strand<boost::asio::any_io_executor> strand_;
  std::shared_ptr<boost::asio::posix::stream_descriptor> event_fd_;
  std::function<void(Message &&)> on_message_;
  std::atomic_bool running_{false};
};

class AsioClient {
public:
  using Executor = boost::asio::any_io_executor;

  AsioClient(Executor executor, std::string server_socket,
             std::string client_name);
  AsioClient(const AsioClient &) = delete;
  AsioClient &operator=(const AsioClient &) = delete;
  AsioClient(AsioClient &&) noexcept = default;
  AsioClient &operator=(AsioClient &&) noexcept = default;

  bool IsConnected() const;
  const Executor &GetExecutor() const;
  void SetDebug(bool enabled);

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
  std::shared_ptr<AsioClientState> state_;
};

class AsioClientState
    : public std::enable_shared_from_this<AsioClientState> {
public:
  using Executor = AsioClient::Executor;

  ~AsioClientState();
  AsioClientState(const AsioClientState &) = delete;
  AsioClientState &operator=(const AsioClientState &) = delete;

  bool IsConnected() const {
    return is_connected_.load(std::memory_order_acquire);
  }
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
  friend class AsioClient;
  friend class AsioPublisher;
  friend class AsioSubscriber;

  struct RpcResult {
    Response response;
    std::vector<toolbelt::FileDescriptor> fds;
  };
  using ControlCompletion = std::function<void(absl::StatusOr<RpcResult>)>;
  struct ControlCommand {
    std::vector<char> outbound;
    std::vector<toolbelt::FileDescriptor> outbound_fds;
    bool expects_response = true;
    ControlCompletion completion;
  };

  explicit AsioClientState(Executor executor);
  void Init(std::string socket, std::string name);
  void Enqueue(Request request, ControlCompletion completion,
               std::vector<toolbelt::FileDescriptor> outbound_fds = {},
               bool expects_response = true, bool prioritize = false);
  void StartNext();
  void SendRequestFds();
  void ReadResponseLength();
  void ReadResponseBody();
  void ReceiveFds();
  void Finish(absl::StatusOr<RpcResult> result);
  void FailTransport(absl::Status status);
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
      std::shared_ptr<boost::asio::posix::stream_descriptor> poll_descriptor,
      std::shared_ptr<std::atomic_bool> cancelled,
      std::function<void(absl::StatusOr<absl::Span<std::byte>>)> completion);
  absl::StatusOr<Message> PublishMessage(details::PublisherImpl *publisher,
                                         int64_t message_size);
  void RefreshPublisher(std::shared_ptr<details::PublisherImpl> publisher,
                        std::function<void(absl::Status)> completion);
  void RefreshSubscriber(std::shared_ptr<details::SubscriberImpl> subscriber,
                         ClearTrigger clear_trigger,
                         std::function<void(absl::Status, bool)> completion);
  void
  ApplySubscriberRefresh(std::shared_ptr<details::SubscriberImpl> subscriber,
                         int updates, RpcResult result,
                         std::function<void(absl::Status, bool)> completion);
  absl::StatusOr<Message> ReadMessage(details::SubscriberImpl *subscriber,
                                      ReadMode mode,
                                      ClearTrigger clear_trigger);
  void RemovePublisher(std::shared_ptr<details::PublisherImpl> publisher);
  void RemoveSubscriber(std::shared_ptr<details::SubscriberImpl> subscriber);
  absl::Status RegisterClientBuffer(const ClientBufferHandleMetadata &metadata,
                                    const toolbelt::FileDescriptor *fd);
  absl::StatusOr<std::vector<RegisteredClientBuffer>>
  GetClientBuffers(const std::string &channel_name, uint64_t session_id,
                   uint32_t buffer_index);
  absl::Status UnregisterClientBuffer(const std::string &channel_name,
                                      uint64_t session_id,
                                      uint32_t buffer_index);
  absl::StatusOr<RpcResult>
  WaitForControl(Request request,
                 std::vector<toolbelt::FileDescriptor> outbound_fds = {},
                 bool expects_response = true);

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
  std::array<char, sizeof(int32_t)> fd_count_{};
  size_t fd_count_received_ = 0;
  std::string name_;
  uint64_t session_id_ = 0;
  int server_user_id_ = 0;
  int server_group_id_ = 0;
  toolbelt::FileDescriptor scb_fd_;
  std::atomic_bool is_connected_{false};
  bool transport_connected_ = false;
  absl::Status terminal_status_;
  bool debug_ = false;
};

namespace asio_detail {

template <typename Handler> struct CompletionState {
  CompletionState(Handler value, const AsioClient::Executor &fallback)
      : handler(std::move(value)),
        executor(boost::asio::get_associated_executor(handler, fallback)),
        work(boost::asio::make_work_guard(executor)) {}

  Handler handler;
  AsioClient::Executor executor;
  boost::asio::executor_work_guard<AsioClient::Executor> work;
};

template <typename Handler>
auto MakeCompletion(Handler handler, const AsioClient::Executor &fallback) {
  return std::make_shared<CompletionState<Handler>>(std::move(handler),
                                                    fallback);
}

template <typename Handler, typename Result,
          typename KeepAlive = std::nullptr_t>
void Complete(std::shared_ptr<CompletionState<Handler>> completion,
              Result result, KeepAlive keep_alive = nullptr) {
  auto executor = completion->executor;
  boost::asio::post(executor, [completion = std::move(completion),
                               result = std::move(result),
                               keep_alive = std::move(keep_alive)]() mutable {
    static_cast<void>(keep_alive);
    completion->work.reset();
    std::move(completion->handler)(std::move(result));
  });
}

} // namespace asio_detail

template <typename CompletionToken>
auto AsioClient::AsyncCreatePublisher(std::string channel_name,
                                      PublisherOptions options,
                                      CompletionToken &&token) {
  return state_->AsyncCreatePublisher(
      std::move(channel_name), std::move(options),
      std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto AsioClient::AsyncCreateSubscriber(std::string channel_name,
                                       SubscriberOptions options,
                                       CompletionToken &&token) {
  return state_->AsyncCreateSubscriber(
      std::move(channel_name), std::move(options),
      std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto AsioClient::AsyncGetChannelInfo(std::string channel_name,
                                     CompletionToken &&token) {
  return state_->AsyncGetChannelInfo(std::move(channel_name),
                                     std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto AsioClient::AsyncGetChannelStats(std::string channel_name,
                                      CompletionToken &&token) {
  return state_->AsyncGetChannelStats(std::move(channel_name),
                                      std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto AsioClient::AsyncChannelExists(std::string channel_name,
                                    CompletionToken &&token) {
  return state_->AsyncChannelExists(std::move(channel_name),
                                    std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto AsioClientState::AsyncCreatePublisher(std::string channel_name,
                                           PublisherOptions options,
                                           CompletionToken &&token) {
  using Signature = void(absl::StatusOr<AsioPublisher>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(), channel_name = std::move(channel_name),
       options = std::move(options)](auto handler) mutable {
        auto completion =
            asio_detail::MakeCompletion(std::move(handler), self->executor_);
        if (options.MaxOutstandingSlotLeases() < 1 ||
            options.MaxOutstandingSlotLeases() > options.NumSlots()) {
          asio_detail::Complete(
              std::move(completion),
              absl::StatusOr<AsioPublisher>(absl::InvalidArgumentError(
                  "MaxOutstandingSlotLeases must be between 1 and NumSlots")));
          return;
        }
        if (options.MaxSlotSize() < 0) {
          asio_detail::Complete(
              std::move(completion),
              absl::StatusOr<AsioPublisher>(absl::InvalidArgumentError(
                  "MaxSlotSize must be non-negative")));
          return;
        }
        if (options.MaxSlotSize() > 0 &&
            options.SlotSize() > options.MaxSlotSize()) {
          asio_detail::Complete(
              std::move(completion),
              absl::StatusOr<AsioPublisher>(
                  absl::InvalidArgumentError("SlotSize exceeds MaxSlotSize")));
          return;
        }
        Request request;
        FillPublisherRequest(request.mutable_create_publisher(), channel_name,
                             options);
        self->Enqueue(
            std::move(request),
            [self, channel_name = std::move(channel_name),
             options = std::move(options), completion = std::move(completion)](
                absl::StatusOr<RpcResult> result) mutable {
              if (!result.ok()) {
                asio_detail::Complete(
                    std::move(completion),
                    absl::StatusOr<AsioPublisher>(result.status()));
                return;
              }
              boost::asio::post(
                  boost::asio::system_executor(),
                  [self, channel_name = std::move(channel_name),
                   options = std::move(options), result = std::move(*result),
                   completion = std::move(completion)]() mutable {
                    asio_detail::Complete(
                        std::move(completion),
                        self->BuildPublisher(channel_name, options,
                                             std::move(result)));
                  });
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClientState::AsyncCreateSubscriber(std::string channel_name,
                                            SubscriberOptions options,
                                            CompletionToken &&token) {
  using Signature = void(absl::StatusOr<std::shared_ptr<AsioSubscriber>>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(), channel_name = std::move(channel_name),
       options = std::move(options)](auto handler) mutable {
        auto completion =
            asio_detail::MakeCompletion(std::move(handler), self->executor_);
        Request request;
        FillSubscriberRequest(request.mutable_create_subscriber(), channel_name,
                              options);
        self->Enqueue(
            std::move(request),
            [self, channel_name = std::move(channel_name),
             options = std::move(options), completion = std::move(completion)](
                absl::StatusOr<RpcResult> result) mutable {
              if (!result.ok()) {
                asio_detail::Complete(
                    std::move(completion),
                    absl::StatusOr<std::shared_ptr<AsioSubscriber>>(
                        result.status()));
                return;
              }
              boost::asio::post(
                  boost::asio::system_executor(),
                  [self, channel_name = std::move(channel_name),
                   options = std::move(options), result = std::move(*result),
                   completion = std::move(completion)]() mutable {
                    asio_detail::Complete(
                        std::move(completion),
                        self->BuildSubscriber(channel_name, options,
                                              std::move(result)));
                  });
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClientState::AsyncGetChannelInfo(std::string channel_name,
                                          CompletionToken &&token) {
  using Signature = void(absl::StatusOr<ChannelInfo>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(),
       channel_name = std::move(channel_name)](auto handler) mutable {
        auto completion =
            asio_detail::MakeCompletion(std::move(handler), self->executor_);
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
                  output =
                      absl::StartsWith(response.error(), "No such channel ")
                          ? absl::StatusOr<ChannelInfo>(
                                absl::NotFoundError(response.error()))
                          : absl::StatusOr<ChannelInfo>(
                                absl::InternalError(response.error()));
                } else if (response.channels_size() == 0) {
                  output = absl::NotFoundError("channel does not exist");
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
              asio_detail::Complete(std::move(completion), std::move(output));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClientState::AsyncGetChannelStats(std::string channel_name,
                                           CompletionToken &&token) {
  using Signature = void(absl::StatusOr<ChannelStats>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(),
       channel_name = std::move(channel_name)](auto handler) mutable {
        auto completion =
            asio_detail::MakeCompletion(std::move(handler), self->executor_);
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
              asio_detail::Complete(std::move(completion), std::move(output));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClientState::AsyncChannelExists(std::string channel_name,
                                         CompletionToken &&token) {
  using Signature = void(absl::StatusOr<bool>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(),
       channel_name = std::move(channel_name)](auto handler) mutable {
        auto completion =
            asio_detail::MakeCompletion(std::move(handler), self->executor_);
        self->AsyncGetChannelInfo(
            std::move(channel_name),
            [self, completion = std::move(completion)](
                absl::StatusOr<ChannelInfo> result) mutable {
              if (result.ok()) {
                asio_detail::Complete(std::move(completion),
                                      absl::StatusOr<bool>(true));
              } else if (absl::IsNotFound(result.status())) {
                asio_detail::Complete(std::move(completion),
                                      absl::StatusOr<bool>(false));
              } else {
                asio_detail::Complete(std::move(completion),
                                      absl::StatusOr<bool>(result.status()));
              }
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioPublisher::AsyncGetMessageBuffer(SlotSizeType max_size,
                                          CompletionToken &&token) {
  using Signature = void(absl::StatusOr<absl::Span<std::byte>>);
  auto client = client_;
  auto impl = impl_;
  auto poll_descriptor = poll_descriptor_;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [client = std::move(client), impl = std::move(impl),
       poll_descriptor = std::move(poll_descriptor),
       max_size](auto handler) mutable {
        auto cancellation_slot =
            boost::asio::get_associated_cancellation_slot(handler);
        auto completion = asio_detail::MakeCompletion(std::move(handler),
                                                      client->GetExecutor());
        auto cancelled = std::make_shared<std::atomic_bool>(false);
        if (cancellation_slot.is_connected()) {
          cancellation_slot.assign([client, cancelled, poll_descriptor](
                                       boost::asio::cancellation_type type) {
            if (type == boost::asio::cancellation_type::none) {
              return;
            }
            cancelled->store(true, std::memory_order_release);
            if (poll_descriptor != nullptr) {
              boost::asio::post(client->GetExecutor(), [poll_descriptor] {
                boost::system::error_code ignored;
                poll_descriptor->cancel(ignored);
              });
            }
          });
        }
        client->AcquireMessageBuffer(
            impl, max_size, poll_descriptor, cancelled,
            [client, impl, cancellation_slot,
             completion = std::move(completion)](
                absl::StatusOr<absl::Span<std::byte>> result) mutable {
              if (cancellation_slot.is_connected()) {
                cancellation_slot.clear();
              }
              asio_detail::Complete(std::move(completion), std::move(result),
                                    std::move(impl));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioPublisher::AsyncPublishMessage(int64_t message_size,
                                        CompletionToken &&token) {
  using Signature = void(absl::StatusOr<Message>);
  auto client = client_;
  auto impl = impl_;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [client = std::move(client), impl = std::move(impl),
       message_size](auto handler) mutable {
        auto completion = asio_detail::MakeCompletion(std::move(handler),
                                                      client->GetExecutor());
        client->RefreshPublisher(impl, [client, impl, message_size,
                                        completion = std::move(completion)](
                                           absl::Status status) mutable {
          if (!status.ok()) {
            asio_detail::Complete(std::move(completion),
                                  absl::StatusOr<Message>(status),
                                  std::move(impl));
            return;
          }
          boost::asio::post(
              boost::asio::system_executor(),
              [client, impl = std::move(impl), message_size,
               completion = std::move(completion)]() mutable {
                auto result = client->PublishMessage(impl.get(), message_size);
                asio_detail::Complete(std::move(completion), std::move(result),
                                      std::move(impl));
              });
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
        auto client = self->client_;
        auto impl = self->impl_;
        auto completion = asio_detail::MakeCompletion(std::move(handler),
                                                      client->GetExecutor());
        client->RefreshSubscriber(
            impl, clear_trigger,
            [self, client, impl, mode, completion = std::move(completion)](
                absl::Status status, bool reloaded) mutable {
              boost::asio::post(
                  self->strand_,
                  [self, client, impl = std::move(impl), mode, status, reloaded,
                   completion = std::move(completion)]() mutable {
                    if (reloaded && status.ok()) {
                      status = self->ResetEventFd();
                    }
                    if (!status.ok()) {
                      asio_detail::Complete(
                          std::move(completion),
                          absl::StatusOr<Message>(std::move(status)),
                          std::move(impl));
                      return;
                    }
                    boost::asio::post(
                        boost::asio::system_executor(),
                        [client, impl = std::move(impl), mode,
                         completion = std::move(completion)]() mutable {
                          auto result = client->ReadMessage(
                              impl.get(), mode, ClearTrigger::kNoClearTrigger);
                          asio_detail::Complete(std::move(completion),
                                                std::move(result),
                                                std::move(impl));
                        });
                  });
            });
      },
      token);
}

} // namespace subspace

#endif // CLIENT_ASIO_ASIO_CLIENT_H_
