// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/asio/asio_client.h"

#include <boost/asio/post.hpp>

#include <utility>

namespace subspace {

AsioClient::AsioClient(Executor executor) : executor_(std::move(executor)) {}

AsioClient::~AsioClient() { control_pool_.join(); }

void AsioClient::SetDebug(bool enabled) {
  auto self = shared_from_this();
  boost::asio::post(control_pool_,
                    [self, enabled] { self->client_->SetDebug(enabled); });
}

void AsioClient::ReleasePublisher(Publisher publisher) {
  boost::asio::post(control_pool_,
                    [publisher = std::move(publisher)] { (void)publisher; });
}

void AsioClient::ReleaseSubscriber(Subscriber subscriber) {
  boost::asio::post(control_pool_,
                    [subscriber = std::move(subscriber)] { (void)subscriber; });
}

} // namespace subspace
