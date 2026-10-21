// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_SUBSCRIBER_H_
#define CLIENT_ASIO_ASIO_SUBSCRIBER_H_

#include "client/client.h"

#include "absl/status/statusor.h"
#include <boost/asio/posix/stream_descriptor.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace subspace {

class AsioClient;

class AsioSubscriber : public std::enable_shared_from_this<AsioSubscriber> {
public:
  ~AsioSubscriber();

  AsioSubscriber(const AsioSubscriber &) = delete;
  AsioSubscriber &operator=(const AsioSubscriber &) = delete;
  AsioSubscriber(AsioSubscriber &&) = delete;
  AsioSubscriber &operator=(AsioSubscriber &&) = delete;

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

  AsioSubscriber(std::shared_ptr<AsioClient> client, Subscriber subscriber);

  template <typename CompletionToken>
  auto AsyncReadMessageImpl(ReadMode mode, ClearTrigger clear_trigger,
                            CompletionToken &&token);

  void WaitForNotification();
  void ReadAvailableMessages();
  void Reset();

  std::shared_ptr<AsioClient> client_;
  std::optional<Subscriber> subscriber_;
  boost::asio::posix::stream_descriptor event_fd_;
  std::function<void(Message &&)> on_message_;
  std::atomic_bool running_{false};
  std::string type_;
};

} // namespace subspace

#include "client/asio/asio_client.h"
#include "client/asio/asio_client.inl"
#include "client/asio/asio_subscriber.inl"

#endif // CLIENT_ASIO_ASIO_SUBSCRIBER_H_
