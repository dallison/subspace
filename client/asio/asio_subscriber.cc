// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/asio/asio_subscriber.h"

#include <boost/asio/error.hpp>

#include <unistd.h>

#include <utility>

namespace subspace {

AsioSubscriber::AsioSubscriber(std::shared_ptr<AsioClient> client,
                               Subscriber subscriber)
    : client_(std::move(client)), subscriber_(std::move(subscriber)),
      event_fd_(client_->GetExecutor(), ::dup(subscriber_->GetPollFd().fd)),
      type_(subscriber_->Type()) {}

AsioSubscriber::~AsioSubscriber() {
  boost::system::error_code ignored;
  event_fd_.cancel(ignored);
  Reset();
}

void AsioSubscriber::Start(std::function<void(Message &&)> on_message) {
  if (running_.exchange(true)) {
    return;
  }
  on_message_ = std::move(on_message);
  ReadAvailableMessages();
}

void AsioSubscriber::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  boost::system::error_code ignored;
  event_fd_.cancel(ignored);
  Reset();
}

std::string AsioSubscriber::Name() const { return subscriber_->Name(); }

std::string_view AsioSubscriber::Type() const { return type_; }

int32_t AsioSubscriber::NumSlots() const { return subscriber_->NumSlots(); }

SlotSizeType AsioSubscriber::SlotSize() const {
  return subscriber_->SlotSize();
}

int32_t AsioSubscriber::ChecksumSize() const {
  return subscriber_->ChecksumSize();
}

int32_t AsioSubscriber::MetadataSize() const {
  return subscriber_->MetadataSize();
}

ChannelCounters AsioSubscriber::GetChannelCounters() const {
  return const_cast<Subscriber &>(*subscriber_).GetChannelCounters();
}

struct pollfd AsioSubscriber::GetPollFd() const {
  return subscriber_->GetPollFd();
}

void AsioSubscriber::WaitForNotification() {
  if (!running_ || !subscriber_.has_value()) {
    return;
  }
  event_fd_.async_wait(
      boost::asio::posix::stream_descriptor::wait_read,
      [self = shared_from_this()](const boost::system::error_code &error) {
        if (error == boost::asio::error::operation_aborted || !self->running_) {
          return;
        }
        if (error) {
          self->Shutdown();
          return;
        }
        self->ReadAvailableMessages();
      });
}

void AsioSubscriber::ReadAvailableMessages() {
  if (!running_ || !subscriber_.has_value()) {
    return;
  }
  AsyncReadMessage(ReadMode::kReadNext,
                   [self = shared_from_this()](auto result) {
                     if (!self->running_) {
                       return;
                     }
                     if (!result.ok()) {
                       self->Shutdown();
                       return;
                     }
                     auto message = std::move(result).value();
                     if (message.buffer == nullptr) {
                       self->WaitForNotification();
                       return;
                     }
                     if (self->on_message_) {
                       self->on_message_(std::move(message));
                     }
                     self->ReadAvailableMessages();
                   });
}

void AsioSubscriber::Reset() {
  if (subscriber_.has_value()) {
    client_->ReleaseSubscriber(std::move(*subscriber_));
    subscriber_.reset();
  }
}

} // namespace subspace
