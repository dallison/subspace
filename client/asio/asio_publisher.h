// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_PUBLISHER_H_
#define CLIENT_ASIO_ASIO_PUBLISHER_H_

#include "client/client.h"

#include "absl/status/statusor.h"
#include "absl/types/span.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace subspace {

class AsioClient;

class AsioPublisher {
public:
  ~AsioPublisher();

  AsioPublisher(const AsioPublisher &) = delete;
  AsioPublisher &operator=(const AsioPublisher &) = delete;
  AsioPublisher(AsioPublisher &&other) noexcept;
  AsioPublisher &operator=(AsioPublisher &&other) noexcept;

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

  AsioPublisher(std::shared_ptr<AsioClient> client, Publisher publisher);
  void Reset();

  std::shared_ptr<AsioClient> client_;
  std::optional<Publisher> publisher_;
};

} // namespace subspace

#include "client/asio/asio_client.h"
#include "client/asio/asio_client.inl"
#include "client/asio/asio_publisher.inl"

#endif // CLIENT_ASIO_ASIO_PUBLISHER_H_
