// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_PUBLISHER_INL_
#define CLIENT_ASIO_ASIO_PUBLISHER_INL_

#include "client/asio/completion.h"

#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>

namespace subspace {

template <typename CompletionToken>
auto AsioPublisher::AsyncGetMessageBuffer(SlotSizeType max_size,
                                          CompletionToken &&token) {
  using Result = absl::StatusOr<absl::Span<std::byte>>;
  using Signature = void(Result);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this, max_size](auto handler) mutable {
        auto client = client_;
        auto work = asio_client_internal::MakeWorkGuard(client->IoContext());
        boost::asio::post(client->control_pool_,
                          [this, client, max_size, work = std::move(work),
                           handler = std::move(handler)]() mutable {
                            asio_client_internal::Complete(
                                client->IoContext(), std::move(handler),
                                GetMessageBuffer(max_size));
                          });
      },
      token);
}

template <typename CompletionToken>
auto AsioPublisher::AsyncPublishMessage(int64_t message_size,
                                        CompletionToken &&token) {
  using Result = absl::StatusOr<Message>;
  using Signature = void(Result);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this, message_size](auto handler) mutable {
        auto client = client_;
        auto work = asio_client_internal::MakeWorkGuard(client->IoContext());
        boost::asio::post(client->control_pool_,
                          [this, client, message_size, work = std::move(work),
                           handler = std::move(handler)]() mutable {
                            asio_client_internal::Complete(
                                client->IoContext(), std::move(handler),
                                PublishMessage(message_size));
                          });
      },
      token);
}

} // namespace subspace

#endif // CLIENT_ASIO_ASIO_PUBLISHER_INL_
