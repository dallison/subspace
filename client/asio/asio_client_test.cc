// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/asio/asio_publisher.h"
#include "client/asio/asio_subscriber.h"
#include "client/test_fixture.h"

#include <gtest/gtest.h>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>

namespace subspace {
namespace {

class AsioClientTest : public SubspaceTestBase {};

TEST_F(AsioClientTest, RejectsOverlongSocketName) {
  boost::asio::io_context io;
  absl::Status status;
  AsioClient client(io.get_executor(), std::string(256, 'x'),
                    "overlong-socket-test");
  client.AsyncGetChannelInfo(
      "unused",
      [&](absl::StatusOr<ChannelInfo> result) { status = result.status(); });
  io.run();
  EXPECT_FALSE(client.IsConnected());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST_F(AsioClientTest, RejectsInvalidPublisherOptionsBeforeRegistration) {
  boost::asio::io_context io;
  absl::Status status;
  AsioClient client(io.get_executor(), Socket(), "asio-invalid-options-test");
  client.AsyncCreatePublisher(
      "asio_invalid_options",
      PublisherOptions().SetNumSlots(2).SetMaxOutstandingSlotLeases(3),
      [&](absl::StatusOr<AsioPublisher> publisher_result) {
        status = publisher_result.status();
      });
  io.run();
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST_F(AsioClientTest, PublishRequiresAcquiredBuffer) {
  boost::asio::io_context io;
  absl::Status status;
  std::optional<AsioPublisher> publisher;
  AsioClient client(io.get_executor(), Socket(),
                    "asio-publish-without-buffer-test");
  client.AsyncCreatePublisher(
      "asio_publish_without_buffer",
      PublisherOptions().SetSlotSize(64).SetNumSlots(4).SetReliable(true),
      [&](absl::StatusOr<AsioPublisher> publisher_result) {
        ASSERT_TRUE(publisher_result.ok());
        publisher.emplace(std::move(*publisher_result));
        publisher->AsyncPublishMessage(1, [&](absl::StatusOr<Message> result) {
          status = result.status();
        });
      });
  io.run();
  EXPECT_TRUE(absl::IsFailedPrecondition(status));
}

TEST_F(AsioClientTest, CancelsReliableBufferWait) {
  boost::asio::io_context io;
  boost::asio::cancellation_signal cancellation;
  AsioClient client(io.get_executor(), Socket(), "asio-cancel-buffer-test");
  std::shared_ptr<AsioSubscriber> subscriber;
  std::optional<AsioPublisher> publisher;
  absl::Status wait_status;

  client.AsyncCreatePublisher(
      "asio_cancel_buffer",
      PublisherOptions().SetSlotSize(64).SetNumSlots(2).SetReliable(true),
      [&](absl::StatusOr<AsioPublisher> publisher_result) {
        ASSERT_TRUE(publisher_result.ok());
        publisher.emplace(std::move(*publisher_result));
        client.AsyncCreateSubscriber(
            "asio_cancel_buffer", SubscriberOptions().SetReliable(true),
            [&](absl::StatusOr<std::shared_ptr<AsioSubscriber>>
                    subscriber_result) {
              ASSERT_TRUE(subscriber_result.ok());
              subscriber = std::move(*subscriber_result);
              publisher->AsyncGetMessageBuffer(
                  -1, [&](absl::StatusOr<absl::Span<std::byte>> buffer) {
                    ASSERT_TRUE(buffer.ok());
                    (*buffer)[0] = std::byte{1};
                    publisher->AsyncPublishMessage(
                        1, [&](absl::StatusOr<Message> result) {
                          ASSERT_TRUE(result.ok());
                          publisher->AsyncGetMessageBuffer(
                              -1, boost::asio::bind_cancellation_slot(
                                      cancellation.slot(),
                                      [&](absl::StatusOr<absl::Span<std::byte>>
                                              waited) {
                                        wait_status = waited.status();
                                      }));
                          boost::asio::post(io, [&] {
                            cancellation.emit(
                                boost::asio::cancellation_type::all);
                          });
                        });
                  });
            });
      });

  io.run();
  EXPECT_TRUE(absl::IsCancelled(wait_status));
}

TEST_F(AsioClientTest, StartedSubscriberDoesNotSelfOwnWhileIdle) {
  boost::asio::io_context io;
  AsioClient client(io.get_executor(), Socket(),
                    "asio-subscriber-lifetime-test");
  std::weak_ptr<AsioSubscriber> weak_subscriber;

  client.AsyncCreateSubscriber(
      "asio_idle_subscriber", SubscriberOptions(),
      [&](absl::StatusOr<std::shared_ptr<AsioSubscriber>> result) {
        ASSERT_TRUE(result.ok());
        auto subscriber = std::move(*result);
        weak_subscriber = subscriber;
        subscriber->Start([](Message &&) {});
        subscriber.reset();
      });

  io.run();
  EXPECT_TRUE(weak_subscriber.expired());
}

TEST_F(AsioClientTest, PublisherMoveAssignmentRemovesReplacedPublisher) {
  boost::asio::io_context io;
  AsioClient client(io.get_executor(), Socket(), "asio-publisher-move-test");
  std::optional<AsioPublisher> publisher;
  absl::Status removed_status;

  client.AsyncCreatePublisher(
      "asio_replaced_publisher",
      PublisherOptions().SetSlotSize(64).SetNumSlots(4),
      [&](absl::StatusOr<AsioPublisher> first) {
        ASSERT_TRUE(first.ok());
        publisher.emplace(std::move(*first));
        client.AsyncCreatePublisher(
            "asio_retained_publisher",
            PublisherOptions().SetSlotSize(64).SetNumSlots(4),
            [&](absl::StatusOr<AsioPublisher> second) {
              ASSERT_TRUE(second.ok());
              *publisher = std::move(*second);
              client.AsyncGetChannelInfo(
                  "asio_replaced_publisher",
                  [&](absl::StatusOr<ChannelInfo> info) {
                    removed_status = info.status();
                  });
            });
      });

  io.run();
  EXPECT_TRUE(absl::IsNotFound(removed_status));
}

TEST_F(AsioClientTest, PublishesAndReceives) {
  boost::asio::io_context io;
  AsioClient client(io.get_executor(), Socket(), "asio-client-test");
  std::shared_ptr<AsioSubscriber> subscriber;
  std::optional<AsioPublisher> publisher;
  std::string payload;
  std::string failure;
  bool channel_exists = false;
  bool channel_info_read = false;
  bool channel_stats_read = false;
  std::optional<std::future<absl::StatusOr<bool>>> exists_future;

  EXPECT_FALSE(client.IsConnected());
  exists_future.emplace(
      client.AsyncChannelExists("asio_client_test", boost::asio::use_future));
  client.AsyncCreatePublisher(
      "asio_client_test",
      PublisherOptions()
          .SetSlotSize(64)
          .SetNumSlots(4)
          .SetUseSplitBuffers(true),
      [&](absl::StatusOr<AsioPublisher> publisher_result) {
        if (!publisher_result.ok()) {
          failure = "create publisher";
          return;
        }
        publisher.emplace(std::move(publisher_result).value());
        client.AsyncChannelExists(
            "asio_client_test", [&](absl::StatusOr<bool> exists_result) {
              channel_exists = exists_result.ok() && *exists_result;
            });
        client.AsyncGetChannelInfo(
            "asio_client_test",
            [&](absl::StatusOr<ChannelInfo> info_result) {
              channel_info_read = info_result.ok();
            });
        client.AsyncGetChannelStats(
            "asio_client_test",
            [&](absl::StatusOr<ChannelStats> stats_result) {
              channel_stats_read = stats_result.ok();
            });
        client.AsyncCreateSubscriber(
            "asio_client_test", SubscriberOptions(),
            [&](absl::StatusOr<std::shared_ptr<AsioSubscriber>>
                    subscriber_result) {
              if (!subscriber_result.ok()) {
                failure = "create subscriber";
                return;
              }
              subscriber = std::move(subscriber_result).value();
              publisher->AsyncGetMessageBuffer(
                  1024, [&](absl::StatusOr<absl::Span<std::byte>> buffer) {
                    if (!buffer.ok()) {
                      failure = "get buffer";
                      return;
                    }
                    constexpr char message[] = "open-source-wire";
                    std::memcpy(buffer->data(), message, sizeof(message) - 1);
                    publisher->AsyncPublishMessage(
                        sizeof(message) - 1,
                        [&](absl::StatusOr<Message> publish_result) {
                          if (!publish_result.ok()) {
                            failure = "publish";
                            return;
                          }
                          auto read_next =
                              std::make_shared<std::function<void()>>();
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
      });

  io.run();

  EXPECT_EQ(failure, "");
  EXPECT_EQ(payload, "open-source-wire");
  EXPECT_TRUE(channel_exists);
  EXPECT_TRUE(channel_info_read);
  EXPECT_TRUE(channel_stats_read);
  EXPECT_TRUE(client.IsConnected());
  ASSERT_TRUE(exists_future.has_value());
  auto initial_exists = exists_future->get();
  ASSERT_TRUE(initial_exists.ok());
  EXPECT_FALSE(*initial_exists);
  subscriber.reset();
  publisher.reset();
}

TEST_F(AsioClientTest, SubscriberCreatedBeforePublisherReceives) {
  boost::asio::io_context io;
  AsioClient client(io.get_executor(), Socket(), "asio-placeholder-test");
  std::shared_ptr<AsioSubscriber> subscriber;
  std::optional<AsioPublisher> publisher;
  std::string payload;
  std::string failure;

  client.AsyncCreateSubscriber(
      "asio_placeholder_test", SubscriberOptions(),
      [&](absl::StatusOr<std::shared_ptr<AsioSubscriber>> subscriber_result) {
        if (!subscriber_result.ok()) {
          failure = "create subscriber";
          return;
        }
        subscriber = std::move(subscriber_result).value();
        subscriber->Start([&](Message &&message) {
          payload.assign(static_cast<const char *>(message.buffer),
                         message.length);
          subscriber->Shutdown();
        });
        client.AsyncCreatePublisher(
            "asio_placeholder_test",
            PublisherOptions().SetSlotSize(64).SetNumSlots(4),
            [&](absl::StatusOr<AsioPublisher> publisher_result) {
              if (!publisher_result.ok()) {
                failure = "create publisher";
                return;
              }
              publisher.emplace(std::move(publisher_result).value());
              publisher->AsyncGetMessageBuffer(
                  -1, [&](absl::StatusOr<absl::Span<std::byte>> buffer) {
                    if (!buffer.ok()) {
                      failure = "get buffer";
                      return;
                    }
                    constexpr char message[] = "placeholder-wire";
                    std::memcpy(buffer->data(), message, sizeof(message) - 1);
                    publisher->AsyncPublishMessage(
                        sizeof(message) - 1,
                        [&](absl::StatusOr<Message> result) {
                          if (!result.ok()) {
                            failure = "publish";
                          }
                        });
                  });
            });
      });

  io.run();

  EXPECT_EQ(failure, "");
  EXPECT_EQ(payload, "placeholder-wire");
  subscriber.reset();
  publisher.reset();
}

} // namespace
} // namespace subspace
