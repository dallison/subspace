// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_CLIENT_INL_
#define CLIENT_ASIO_ASIO_CLIENT_INL_

#include "client/asio/asio_publisher.h"
#include "client/asio/asio_subscriber.h"
#include "client/asio/completion.h"

#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>

#include <utility>
#include <vector>

namespace subspace {

template <typename CompletionToken>
auto AsioClient::AsyncCreate(boost::asio::io_context &io,
                             std::string server_socket, std::string client_name,
                             CompletionToken &&token) {
  using Signature = void(absl::StatusOr<std::shared_ptr<AsioClient>>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [&io, server_socket = std::move(server_socket),
       client_name = std::move(client_name)](auto handler) mutable {
        auto client = std::shared_ptr<AsioClient>(new AsioClient(io));
        auto work = asio_client_internal::MakeWorkGuard(io);
        boost::asio::post(
            client->control_pool_,
            [client, server_socket = std::move(server_socket),
             client_name = std::move(client_name), work = std::move(work),
             handler = std::move(handler)]() mutable {
              auto result = Client::Create(server_socket, client_name);
              if (!result.ok()) {
                asio_client_internal::Complete(
                    client->IoContext(), std::move(handler),
                    absl::StatusOr<std::shared_ptr<AsioClient>>(
                        result.status()));
                return;
              }
              client->client_ = std::move(result).value();
              client->client_->SetThreadSafe(true);
              asio_client_internal::Complete(
                  client->IoContext(), std::move(handler),
                  absl::StatusOr<std::shared_ptr<AsioClient>>(client));
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
        auto work = asio_client_internal::MakeWorkGuard(self->IoContext());
        boost::asio::post(
            self->control_pool_,
            [self, channel_name = std::move(channel_name),
             options = std::move(options), work = std::move(work),
             handler = std::move(handler)]() mutable {
              auto result =
                  self->client_->CreatePublisher(channel_name, options);
              if (!result.ok()) {
                asio_client_internal::Complete(
                    self->IoContext(), std::move(handler),
                    absl::StatusOr<AsioPublisher>(result.status()));
                return;
              }
              asio_client_internal::Complete(
                  self->IoContext(), std::move(handler),
                  absl::StatusOr<AsioPublisher>(
                      AsioPublisher(self, std::move(result).value())));
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
        auto work = asio_client_internal::MakeWorkGuard(self->IoContext());
        boost::asio::post(
            self->control_pool_,
            [self, channel_name = std::move(channel_name),
             options = std::move(options), work = std::move(work),
             handler = std::move(handler)]() mutable {
              auto result =
                  self->client_->CreateSubscriber(channel_name, options);
              if (!result.ok()) {
                asio_client_internal::Complete(
                    self->IoContext(), std::move(handler),
                    absl::StatusOr<std::shared_ptr<AsioSubscriber>>(
                        result.status()));
                return;
              }
              auto subscriber = std::shared_ptr<AsioSubscriber>(
                  new AsioSubscriber(self, std::move(result).value()));
              asio_client_internal::Complete(
                  self->IoContext(), std::move(handler),
                  absl::StatusOr<std::shared_ptr<AsioSubscriber>>(subscriber));
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
        auto work = asio_client_internal::MakeWorkGuard(self->IoContext());
        boost::asio::post(
            self->control_pool_,
            [self, channel_name = std::move(channel_name),
             work = std::move(work), handler = std::move(handler)]() mutable {
              auto result = self->client_->GetChannelInfo(channel_name);
              asio_client_internal::Complete(
                  self->IoContext(), std::move(handler),
                  result.ok() ? absl::StatusOr<ChannelInfo>(*result)
                              : absl::StatusOr<ChannelInfo>(result.status()));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClient::AsyncGetChannelInfo(CompletionToken &&token) {
  using Result = std::vector<ChannelInfo>;
  using Signature = void(absl::StatusOr<Result>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this()](auto handler) mutable {
        auto work = asio_client_internal::MakeWorkGuard(self->IoContext());
        boost::asio::post(
            self->control_pool_, [self, work = std::move(work),
                                  handler = std::move(handler)]() mutable {
              auto result = self->client_->GetChannelInfo();
              asio_client_internal::Complete(
                  self->IoContext(), std::move(handler),
                  result.ok() ? absl::StatusOr<Result>(*result)
                              : absl::StatusOr<Result>(result.status()));
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
        auto work = asio_client_internal::MakeWorkGuard(self->IoContext());
        boost::asio::post(
            self->control_pool_,
            [self, channel_name = std::move(channel_name),
             work = std::move(work), handler = std::move(handler)]() mutable {
              auto result = self->client_->GetChannelStats(channel_name);
              asio_client_internal::Complete(
                  self->IoContext(), std::move(handler),
                  result.ok() ? absl::StatusOr<ChannelStats>(*result)
                              : absl::StatusOr<ChannelStats>(result.status()));
            });
      },
      token);
}

template <typename CompletionToken>
auto AsioClient::AsyncGetChannelStats(CompletionToken &&token) {
  using Result = std::vector<ChannelStats>;
  using Signature = void(absl::StatusOr<Result>);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this()](auto handler) mutable {
        auto work = asio_client_internal::MakeWorkGuard(self->IoContext());
        boost::asio::post(
            self->control_pool_, [self, work = std::move(work),
                                  handler = std::move(handler)]() mutable {
              auto result = self->client_->GetChannelStats();
              asio_client_internal::Complete(
                  self->IoContext(), std::move(handler),
                  result.ok() ? absl::StatusOr<Result>(*result)
                              : absl::StatusOr<Result>(result.status()));
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
        auto work = asio_client_internal::MakeWorkGuard(self->IoContext());
        boost::asio::post(
            self->control_pool_,
            [self, channel_name = std::move(channel_name),
             work = std::move(work), handler = std::move(handler)]() mutable {
              auto result = self->client_->ChannelExists(channel_name);
              asio_client_internal::Complete(
                  self->IoContext(), std::move(handler),
                  result.ok() ? absl::StatusOr<bool>(*result)
                              : absl::StatusOr<bool>(result.status()));
            });
      },
      token);
}

} // namespace subspace

#endif // CLIENT_ASIO_ASIO_CLIENT_INL_
