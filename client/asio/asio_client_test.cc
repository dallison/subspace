// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/asio/asio_publisher.h"
#include "client/asio/asio_subscriber.h"
#include "client/test_fixture.h"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace subspace {
namespace {

class AsioClientTest : public SubspaceTestBase {};

TEST_F(AsioClientTest, PublishesAndReceives) {
  boost::asio::io_context io;
  std::shared_ptr<AsioClient> client;
  std::shared_ptr<AsioSubscriber> subscriber;
  std::optional<AsioPublisher> publisher;
  std::string payload;
  std::string failure;
  bool channel_exists = false;
  bool channel_info_read = false;
  bool channel_stats_read = false;

  AsioClient::AsyncCreate(
      io, Socket(), "asio-client-test",
      [&](absl::StatusOr<std::shared_ptr<AsioClient>> client_result) {
        if (!client_result.ok()) {
          failure = "create client";
          return;
        }
        client = std::move(client_result).value();
        client->AsyncCreatePublisher(
            "asio_client_test",
            PublisherOptions().SetSlotSize(64).SetNumSlots(4),
            [&](absl::StatusOr<AsioPublisher> publisher_result) {
              if (!publisher_result.ok()) {
                failure = "create publisher";
                return;
              }
              publisher.emplace(std::move(publisher_result).value());
              client->AsyncChannelExists(
                  "asio_client_test", [&](absl::StatusOr<bool> exists_result) {
                    channel_exists = exists_result.ok() && *exists_result;
                  });
              client->AsyncGetChannelInfo(
                  "asio_client_test",
                  [&](absl::StatusOr<ChannelInfo> info_result) {
                    channel_info_read = info_result.ok();
                  });
              client->AsyncGetChannelStats(
                  "asio_client_test",
                  [&](absl::StatusOr<ChannelStats> stats_result) {
                    channel_stats_read = stats_result.ok();
                  });
              client->AsyncCreateSubscriber(
                  "asio_client_test", SubscriberOptions(),
                  [&](absl::StatusOr<std::shared_ptr<AsioSubscriber>>
                          subscriber_result) {
                    if (!subscriber_result.ok()) {
                      failure = "create subscriber";
                      return;
                    }
                    subscriber = std::move(subscriber_result).value();
                    auto buffer = publisher->GetMessageBuffer();
                    if (!buffer.ok()) {
                      failure = "get buffer";
                      return;
                    }
                    constexpr char message[] = "open-source-wire";
                    std::memcpy(buffer->data(), message, sizeof(message) - 1);
                    if (!publisher->PublishMessage(sizeof(message) - 1).ok()) {
                      failure = "publish";
                      return;
                    }
                    auto read_next = std::make_shared<std::function<void()>>();
                    *read_next = [&, read_next] {
                      subscriber->AsyncReadMessage(
                          ReadMode::kReadNext,
                          [&, read_next](absl::StatusOr<Message> result) {
                            if (!result.ok()) {
                              failure = "read";
                              return;
                            }
                            auto received = std::move(result).value();
                            if (received.buffer == nullptr) {
                              (*read_next)();
                              return;
                            }
                            payload.assign(
                                static_cast<const char *>(received.buffer),
                                received.length);
                          });
                    };
                    (*read_next)();
                  });
            });
      });

  io.run();

  EXPECT_EQ(failure, "");
  EXPECT_EQ(payload, "open-source-wire");
  EXPECT_TRUE(channel_exists);
  EXPECT_TRUE(channel_info_read);
  EXPECT_TRUE(channel_stats_read);
  subscriber.reset();
  publisher.reset();
  client.reset();
}

} // namespace
} // namespace subspace
