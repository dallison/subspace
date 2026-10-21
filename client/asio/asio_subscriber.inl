// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_SUBSCRIBER_INL_
#define CLIENT_ASIO_ASIO_SUBSCRIBER_INL_

#include "client/asio/completion.h"

#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>

namespace subspace {

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
  using Result = absl::StatusOr<Message>;
  using Signature = void(Result);
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [self = shared_from_this(), mode, clear_trigger](auto handler) mutable {
        auto work =
            asio_client_internal::MakeWorkGuard(self->client_->IoContext());
        boost::asio::post(
            self->client_->control_pool_,
            [self, mode, clear_trigger, work = std::move(work),
             handler = std::move(handler)]() mutable {
              asio_client_internal::Complete(
                  self->client_->IoContext(), std::move(handler),
                  self->subscriber_->ReadMessage(mode, clear_trigger));
            });
      },
      token);
}

} // namespace subspace

#endif // CLIENT_ASIO_ASIO_SUBSCRIBER_INL_
