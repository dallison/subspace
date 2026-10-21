// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/asio/asio_publisher.h"

#include <utility>

namespace subspace {

AsioPublisher::AsioPublisher(std::shared_ptr<AsioClient> client,
                             Publisher publisher)
    : client_(std::move(client)), publisher_(std::move(publisher)) {}

AsioPublisher::~AsioPublisher() { Reset(); }

AsioPublisher::AsioPublisher(AsioPublisher &&other) noexcept
    : client_(std::move(other.client_)),
      publisher_(std::move(other.publisher_)) {
  other.publisher_.reset();
}

AsioPublisher &AsioPublisher::operator=(AsioPublisher &&other) noexcept {
  if (this != &other) {
    Reset();
    client_ = std::move(other.client_);
    publisher_ = std::move(other.publisher_);
    other.publisher_.reset();
  }
  return *this;
}

absl::StatusOr<absl::Span<std::byte>>
AsioPublisher::GetMessageBuffer(SlotSizeType max_size) {
  return publisher_->GetMessageBufferSpan(max_size);
}

absl::StatusOr<Message> AsioPublisher::PublishMessage(int64_t message_size) {
  auto result = publisher_->PublishMessage(message_size);
  if (!result.ok()) {
    return result.status();
  }
  return *result;
}

bool AsioPublisher::IsReliable() const { return publisher_->IsReliable(); }

int32_t AsioPublisher::NumSubscribers() const {
  return publisher_->NumSubscribers();
}

std::string AsioPublisher::Name() const { return publisher_->Name(); }

struct pollfd AsioPublisher::GetPollFd() const {
  return publisher_->GetPollFd();
}

void AsioPublisher::Reset() {
  if (publisher_.has_value()) {
    client_->ReleasePublisher(std::move(*publisher_));
    publisher_.reset();
  }
}

} // namespace subspace
