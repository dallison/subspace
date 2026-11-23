// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

// Stress tests for the ASIL client, in the shape of client/stress_test.cc.
//
// An ASIL client, publisher, or subscriber is used by one thread at a time, so
// each thread owns the objects it publishes or reads with. There is no
// coroutine test and no shared-client thread-safety test: the ASIL client has
// neither interface. Channels are created on demand. A publisher opens before
// its subscriber so the subscriber has buffers to map. Slot counts stay ahead
// of the messages still in the channel, so an unreliable publisher does not
// overwrite a message the subscriber has not read yet.

#include "client/test_fixture.h"

#include "absl/strings/str_format.h"
#include "asil_client/client.h"
#include "asil_client/phaser_connection.h"
#include "asil_client/publisher.h"
#include "asil_client/shm_layout.h"
#include "asil_client/subscriber.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

namespace {

namespace asil = ::subspace::asil;

int Scaled(int normal, int low) {
  static const bool low_resource = [] {
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) == 0 &&
        limit.rlim_cur != RLIM_INFINITY && limit.rlim_cur < 4096) {
      return true;
    }
#ifdef __linux__
    std::ifstream file("/proc/sys/vm/max_map_count");
    uint64_t value = 0;
    if ((file >> value) && value < 262144) {
      return true;
    }
#endif
    return false;
  }();
  return low_resource ? low : normal;
}

struct AsilLink {
  // Destroyed last: publishers and subscribers are declared after these in
  // the groups below, so they close while the connection is still open.
  std::unique_ptr<asil::PhaserServerConnection> connection;
  std::unique_ptr<asil::Client> client;
};

bool ConnectAsil(const std::string &socket, const std::string &name,
                 AsilLink &link) {
  link.connection = std::make_unique<asil::PhaserServerConnection>();
  if (link.connection->Connect(socket.c_str()) != asil::Error::kOk) {
    ADD_FAILURE() << "connect " << name;
    return false;
  }
  link.client = std::make_unique<asil::Client>();
  if (asil::Error error = link.client->Init(*link.connection, name.c_str());
      error != asil::Error::kOk) {
    ADD_FAILURE() << "init " << name << ": " << asil::ErrorString(error)
                  << " " << link.client->LastServerError();
    return false;
  }
  return true;
}

void Fail(std::atomic<int> &failures, const std::string &why) {
  if (failures.fetch_add(1, std::memory_order_relaxed) == 0) {
    ADD_FAILURE() << why;
  }
}

// Publishes sequence numbers 0 .. count-1. A reliable publisher's Buffer()
// is null while a reliable subscriber still holds every slot; wait for one.
bool PublishSequences(asil::Publisher &publisher, int count,
                      std::atomic<int> &failures) {
  for (uint32_t sequence = 0; sequence < static_cast<uint32_t>(count);
       ++sequence) {
    if (failures.load(std::memory_order_relaxed) != 0) {
      return false;
    }
    void *buffer = publisher.Buffer();
    while (buffer == nullptr) {
      if (publisher.Wait(10000) != asil::Error::kOk) {
        Fail(failures, absl::StrFormat("publisher %s waited 10s for a slot",
                                       publisher.Name()));
        return false;
      }
      buffer = publisher.Buffer();
    }
    std::memcpy(buffer, &sequence, sizeof(sequence));
    if (asil::Error error = publisher.Publish(sizeof(sequence));
        error != asil::Error::kOk) {
      Fail(failures, absl::StrFormat("publish %s sequence %u: %s",
                                     publisher.Name(), sequence,
                                     asil::ErrorString(error)));
      return false;
    }
  }
  return true;
}

// Reads count messages whose payloads are 0 .. count-1, in order. Activation
// messages are skipped by ReadMessage.
bool ReadSequences(asil::Subscriber &subscriber, int count,
                   std::atomic<int> &failures) {
  uint32_t expect = 0;
  while (expect < static_cast<uint32_t>(count)) {
    if (failures.load(std::memory_order_relaxed) != 0) {
      return false;
    }
    asil::Message message;
    if (asil::Error error = subscriber.ReadMessage(message);
        error != asil::Error::kOk) {
      Fail(failures, absl::StrFormat("read %s: %s", subscriber.Name(),
                                     asil::ErrorString(error)));
      return false;
    }
    if (message.length == 0) {
      if (subscriber.Wait(10000) != asil::Error::kOk) {
        Fail(failures, absl::StrFormat(
                           "subscriber %s waited 10s, saw %u of %d",
                           subscriber.Name(), expect, count));
        return false;
      }
      continue;
    }
    uint32_t sequence = 0;
    if (message.length != sizeof(sequence)) {
      Fail(failures, absl::StrFormat("subscriber %s length %zu",
                                     subscriber.Name(), message.length));
      return false;
    }
    std::memcpy(&sequence, message.data, sizeof(sequence));
    if (sequence != expect || message.dropped != 0) {
      Fail(failures, absl::StrFormat(
                         "subscriber %s sequence %u, expected %u, dropped %u",
                         subscriber.Name(), sequence, expect, message.dropped));
      return false;
    }
    ++expect;
  }
  return true;
}

asil::PublisherOptions PubOptions(int64_t slot_size, int32_t num_slots) {
  asil::PublisherOptions options;
  options.slot_size = slot_size;
  options.num_slots = num_slots;
  return options;
}

class AsilStressTest : public SubspaceTestBase {};

// Many clients, each with many channels. One thread publishes a client's
// channels and another reads them, through a second ASIL client.
TEST_F(AsilStressTest, Threads) {
  const int kNumClients = Scaled(8, 2);
  const int kNumChannels = Scaled(32, 4);
  const int kNumMessages = Scaled(100, 20);
  const int kNumSlots = kNumMessages + 2;

  struct Group {
    AsilLink link;
    std::vector<std::string> names;
    std::vector<std::unique_ptr<asil::Publisher>> publishers;
    std::vector<std::unique_ptr<asil::Subscriber>> subscribers;
  };
  std::vector<Group> publishers(kNumClients);
  std::vector<Group> subscribers(kNumClients);

  for (int client = 0; client < kNumClients; ++client) {
    ASSERT_TRUE(ConnectAsil(Socket(), absl::StrFormat("asil-pub-%d", client),
                            publishers[client].link));
    ASSERT_TRUE(ConnectAsil(Socket(), absl::StrFormat("asil-sub-%d", client),
                            subscribers[client].link));
    for (int channel = 0; channel < kNumChannels; ++channel) {
      publishers[client].names.push_back(
          absl::StrFormat("/asil_stress/threads/%d/%d", client, channel));
      auto &name = publishers[client].names.back();
      auto publisher = std::make_unique<asil::Publisher>();
      ASSERT_EQ(asil::Error::kOk,
                publishers[client].link.client->CreatePublisher(
                    name.c_str(), PubOptions(64, kNumSlots), *publisher))
          << publishers[client].link.client->LastServerError();
      publishers[client].publishers.push_back(std::move(publisher));

      auto subscriber = std::make_unique<asil::Subscriber>();
      ASSERT_EQ(asil::Error::kOk,
                subscribers[client].link.client->CreateSubscriber(
                    name.c_str(), {}, *subscriber))
          << subscribers[client].link.client->LastServerError();
      subscribers[client].subscribers.push_back(std::move(subscriber));
    }
  }

  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kNumClients * 2);
  for (int client = 0; client < kNumClients; ++client) {
    threads.emplace_back([&, client] {
      for (int channel = 0; channel < kNumChannels; ++channel) {
        if (!PublishSequences(*publishers[client].publishers[channel],
                              kNumMessages, failures)) {
          return;
        }
      }
    });
    threads.emplace_back([&, client] {
      for (int channel = 0; channel < kNumChannels; ++channel) {
        if (!ReadSequences(*subscribers[client].subscribers[channel],
                           kNumMessages, failures)) {
          return;
        }
      }
    });
  }
  for (std::thread &thread : threads) {
    thread.join();
  }
  EXPECT_EQ(0, failures.load());
}

// A reliable publisher blocks while the subscriber holds its messages. The
// subscriber keeps kMaxActive of them, checks that the held payloads stay
// intact, then releases them so the publisher can continue.
TEST_F(AsilStressTest, ActiveMessages) {
  constexpr int kMaxActive = 32;
  const int kNumMessages = Scaled(400, 64);
  const int kNumSlots = kMaxActive + 4;
  constexpr char kChannel[] = "/asil_stress/active";

  AsilLink publisher_link;
  AsilLink subscriber_link;
  ASSERT_TRUE(ConnectAsil(Socket(), "asil-active-pub", publisher_link));
  ASSERT_TRUE(ConnectAsil(Socket(), "asil-active-sub", subscriber_link));

  asil::PublisherOptions options = PubOptions(64, kNumSlots);
  options.reliable = true;
  asil::Publisher publisher;
  ASSERT_EQ(asil::Error::kOk, publisher_link.client->CreatePublisher(
                                  kChannel, options, publisher));
  asil::SubscriberOptions subscriber_options;
  subscriber_options.reliable = true;
  subscriber_options.max_active_messages = kMaxActive;
  asil::Subscriber subscriber;
  ASSERT_EQ(asil::Error::kOk, subscriber_link.client->CreateSubscriber(
                                  kChannel, subscriber_options, subscriber));

  std::atomic<int> failures{0};
  std::thread publisher_thread([&] {
    PublishSequences(publisher, kNumMessages, failures);
  });

  // Each held message with the sequence it carried when it was read.
  struct Held {
    asil::Message message;
    uint32_t sequence;
  };
  std::vector<Held> held;
  held.reserve(kMaxActive);
  uint32_t expect = 0;
  while (expect < static_cast<uint32_t>(kNumMessages) &&
         failures.load(std::memory_order_relaxed) == 0) {
    if (subscriber.NumActiveMessages() == kMaxActive) {
      // The payloads must still hold what they held when they were read: the
      // publisher may not overwrite a message the subscriber holds.
      for (const Held &h : held) {
        uint32_t sequence = 0;
        if (h.message.length != sizeof(sequence)) {
          Fail(failures, "held message has the wrong length");
          break;
        }
        std::memcpy(&sequence, h.message.data, sizeof(sequence));
        if (sequence != h.sequence) {
          Fail(failures, absl::StrFormat("held sequence %u changed to %u",
                                         h.sequence, sequence));
          break;
        }
      }
      subscriber.ReleaseMessage();
      held.clear();
    }
    asil::Message message;
    if (asil::Error error = subscriber.ReadMessage(message);
        error != asil::Error::kOk) {
      Fail(failures, absl::StrFormat("active read: %s", asil::ErrorString(error)));
      break;
    }
    if (message.length == 0) {
      if (subscriber.Wait(10000) != asil::Error::kOk) {
        Fail(failures, absl::StrFormat("active subscriber waited 10s at %u",
                                       expect));
        break;
      }
      continue;
    }
    uint32_t sequence = 0;
    std::memcpy(&sequence, message.data, sizeof(sequence));
    if (sequence != expect || message.dropped != 0) {
      Fail(failures, absl::StrFormat("active sequence %u, expected %u",
                                     sequence, expect));
      break;
    }
    held.push_back(Held{message, sequence});
    ++expect;
  }
  subscriber.ReleaseMessage();
  publisher_thread.join();
  EXPECT_EQ(0, failures.load());
  EXPECT_EQ(static_cast<uint32_t>(kNumMessages), expect);
}

// Publishers on many virtual channels, one subscriber on the multiplexer.
TEST_F(AsilStressTest, VirtualChannels) {
  const int kNumThreads = Scaled(4, 2);
  const int kVchansPerThread = Scaled(16, 4);
  const int kNumMessages = Scaled(20, 8);
  const int kNumVchans = kNumThreads * kVchansPerThread;
  // Every publisher holds one slot besides the messages it has published, and
  // the subscriber may not have read any of them yet.
  const int kNumSlots = kNumVchans * kNumMessages + kNumVchans;
  const std::string mux = "/asil_stress/virtual";

  struct Group {
    AsilLink link;
    std::vector<std::unique_ptr<asil::Publisher>> publishers;
  };
  std::vector<Group> groups(kNumThreads);
  asil::PublisherOptions options = PubOptions(128, kNumSlots);
  options.mux = mux.c_str();
  for (int thread = 0; thread < kNumThreads; ++thread) {
    ASSERT_TRUE(ConnectAsil(Socket(), absl::StrFormat("asil-vpub-%d", thread),
                            groups[thread].link));
    for (int vchan = 0; vchan < kVchansPerThread; ++vchan) {
      const std::string name =
          absl::StrFormat("/asil_stress/virtual/%d/%d", thread, vchan);
      auto publisher = std::make_unique<asil::Publisher>();
      ASSERT_EQ(asil::Error::kOk,
                groups[thread].link.client->CreatePublisher(
                    name.c_str(), options, *publisher))
          << groups[thread].link.client->LastServerError();
      groups[thread].publishers.push_back(std::move(publisher));
    }
  }

  AsilLink subscriber_link;
  ASSERT_TRUE(ConnectAsil(Socket(), "asil-vmux", subscriber_link));
  asil::Subscriber subscriber;
  ASSERT_EQ(asil::Error::kOk, subscriber_link.client->CreateSubscriber(
                                  mux.c_str(), {}, subscriber));

  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int thread = 0; thread < kNumThreads; ++thread) {
    threads.emplace_back([&, thread] {
      for (const std::unique_ptr<asil::Publisher> &publisher :
           groups[thread].publishers) {
        if (!PublishSequences(*publisher, kNumMessages, failures)) {
          return;
        }
      }
    });
  }

  std::vector<int> next(asil::shm::kMaxVchanId + 1, 0);
  const int expected = kNumVchans * kNumMessages;
  int got = 0;
  while (got < expected && failures.load(std::memory_order_relaxed) == 0) {
    asil::Message message;
    if (subscriber.ReadMessage(message) != asil::Error::kOk) {
      Fail(failures, "virtual read failed");
      break;
    }
    if (message.length == 0) {
      if (subscriber.Wait(10000) != asil::Error::kOk) {
        Fail(failures, absl::StrFormat("mux subscriber waited 10s, saw %d of %d",
                                       got, expected));
        break;
      }
      continue;
    }
    uint32_t sequence = 0;
    if (message.length != sizeof(sequence) || message.vchan_id < 0 ||
        message.vchan_id >= asil::shm::kMaxVchanId) {
      Fail(failures, "virtual message has a bad length or vchan");
      break;
    }
    std::memcpy(&sequence, message.data, sizeof(sequence));
    if (sequence != static_cast<uint32_t>(next[message.vchan_id]) ||
        message.dropped != 0) {
      Fail(failures, absl::StrFormat("vchan %d sequence %u, expected %d",
                                     message.vchan_id, sequence,
                                     next[message.vchan_id]));
      break;
    }
    ++next[message.vchan_id];
    ++got;
  }
  for (std::thread &thread : threads) {
    thread.join();
  }
  EXPECT_EQ(0, failures.load());
  EXPECT_EQ(expected, got);
}

// Large slots, one publisher thread for a handful of channels and one
// subscriber thread polling all of them.
TEST_F(AsilStressTest, ManyChannels) {
  const int kNumThreads = Scaled(8, 2);
  const int kChannelsPerThread = Scaled(4, 2);
  const int kNumChannels = kNumThreads * kChannelsPerThread;
  const int kNumMessages = Scaled(30, 8);
  const int kNumSlots = kNumMessages + 2;
  constexpr int kSlotSize = 32768;

  struct Group {
    AsilLink link;
    std::vector<std::string> names;
    std::vector<std::unique_ptr<asil::Publisher>> publishers;
  };
  std::vector<Group> groups(kNumThreads);
  std::vector<std::string> names;
  names.reserve(kNumChannels);
  for (int thread = 0; thread < kNumThreads; ++thread) {
    ASSERT_TRUE(ConnectAsil(Socket(), absl::StrFormat("asil-mpub-%d", thread),
                            groups[thread].link));
    for (int channel = 0; channel < kChannelsPerThread; ++channel) {
      names.push_back(
          absl::StrFormat("/asil_stress/many/%d/%d", thread, channel));
      groups[thread].names.push_back(names.back());
      auto publisher = std::make_unique<asil::Publisher>();
      ASSERT_EQ(asil::Error::kOk,
                groups[thread].link.client->CreatePublisher(
                    names.back().c_str(), PubOptions(kSlotSize, kNumSlots),
                    *publisher));
      groups[thread].publishers.push_back(std::move(publisher));
    }
  }

  AsilLink subscriber_link;
  ASSERT_TRUE(ConnectAsil(Socket(), "asil-many-sub", subscriber_link));
  std::vector<std::unique_ptr<asil::Subscriber>> subscribers;
  subscribers.reserve(kNumChannels);
  for (const std::string &name : names) {
    auto subscriber = std::make_unique<asil::Subscriber>();
    ASSERT_EQ(asil::Error::kOk, subscriber_link.client->CreateSubscriber(
                                    name.c_str(), {}, *subscriber));
    subscribers.push_back(std::move(subscriber));
  }

  std::atomic<int> failures{0};
  std::vector<std::thread> publishers_threads;
  for (int thread = 0; thread < kNumThreads; ++thread) {
    publishers_threads.emplace_back([&, thread] {
      for (int channel = 0; channel < kChannelsPerThread; ++channel) {
        for (uint32_t sequence = 0; sequence < static_cast<uint32_t>(kNumMessages);
             ++sequence) {
          if (failures.load(std::memory_order_relaxed) != 0) {
            return;
          }
          asil::Publisher &publisher = *groups[thread].publishers[channel];
          void *buffer = publisher.Buffer();
          if (buffer == nullptr) {
            Fail(failures, "many-channel publisher has no slot");
            return;
          }
          std::memset(buffer, 0xda, kSlotSize);
          std::memcpy(buffer, &sequence, sizeof(sequence));
          if (publisher.Publish(kSlotSize) != asil::Error::kOk) {
            Fail(failures, "many-channel publish failed");
            return;
          }
          std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
      }
    });
  }

  std::vector<uint32_t> expect(kNumChannels, 0);
  int got = 0;
  const int expected = kNumChannels * kNumMessages;
  std::vector<pollfd> fds;
  fds.reserve(subscribers.size());
  for (const std::unique_ptr<asil::Subscriber> &subscriber : subscribers) {
    fds.push_back(pollfd{subscriber->PollFd(), POLLIN, 0});
  }
  auto last_progress = std::chrono::steady_clock::now();
  while (got < expected && failures.load(std::memory_order_relaxed) == 0) {
    bool progress = false;
    for (int channel = 0; channel < kNumChannels; ++channel) {
      for (;;) {
        asil::Message message;
        if (subscribers[channel]->ReadMessage(message) != asil::Error::kOk) {
          Fail(failures, "many-channel read failed");
          break;
        }
        if (message.length == 0) {
          break;
        }
        uint32_t sequence = 0;
        std::memcpy(&sequence, message.data, sizeof(sequence));
        if (message.length != kSlotSize || sequence != expect[channel] ||
            message.dropped != 0) {
          Fail(failures, absl::StrFormat("channel %d sequence %u, expected %u",
                                         channel, sequence, expect[channel]));
          break;
        }
        ++expect[channel];
        ++got;
        progress = true;
      }
    }
    if (got >= expected || failures.load(std::memory_order_relaxed) != 0) {
      break;
    }
    // Time out on lack of progress, not only on a quiet poll: a readable fd
    // that delivers nothing would otherwise spin forever.
    const auto now = std::chrono::steady_clock::now();
    if (progress) {
      last_progress = now;
    } else if (now - last_progress > std::chrono::seconds(10)) {
      Fail(failures, absl::StrFormat(
                         "many-channel subscriber made no progress for 10s, "
                         "saw %d of %d",
                         got, expected));
      break;
    }
    (void)poll(fds.data(), fds.size(), 100);
  }
  for (std::thread &thread : publishers_threads) {
    thread.join();
  }
  EXPECT_EQ(0, failures.load());
  EXPECT_EQ(expected, got);
}

// ASIL and standard clients publish and read the same channels at once, in
// both directions. A separate channel checks that ASIL publishers fill the
// queues of standard subscribers.
TEST_F(AsilStressTest, MixedWithStandardClient) {
  const int kNumChannels = Scaled(16, 4);
  const int kNumMessages = Scaled(80, 20);
  const int kNumSlots = kNumMessages + 2;
  const int kNumQueuePublishers = Scaled(4, 2);
  const int kNumQueueSubscribers = Scaled(8, 2);
  const int kQueueMessages = Scaled(1000, 100);
  constexpr int kQueueSlots = 128;

  struct Direction {
    std::string name;
    AsilLink asil;
    std::shared_ptr<subspace::Client> standard;
    std::unique_ptr<asil::Publisher> asil_publisher;
    std::unique_ptr<asil::Subscriber> asil_subscriber;
    std::optional<subspace::Publisher> standard_publisher;
    std::optional<subspace::Subscriber> standard_subscriber;
  };

  std::vector<Direction> asil_to_std(kNumChannels);
  std::vector<Direction> std_to_asil(kNumChannels);
  for (int channel = 0; channel < kNumChannels; ++channel) {
    Direction &to_std = asil_to_std[channel];
    to_std.name = absl::StrFormat("/asil_stress/mix/to_std/%d", channel);
    ASSERT_TRUE(ConnectAsil(Socket(), absl::StrFormat("mix-pub-%d", channel),
                            to_std.asil));
    to_std.asil_publisher = std::make_unique<asil::Publisher>();
    ASSERT_EQ(asil::Error::kOk, to_std.asil.client->CreatePublisher(
                                    to_std.name.c_str(),
                                    PubOptions(64, kNumSlots),
                                    *to_std.asil_publisher));
    absl::StatusOr<std::shared_ptr<subspace::Client>> standard_subscriber_client =
        subspace::Client::Create(Socket(),
                                 absl::StrFormat("mix-std-sub-%d", channel));
    ASSERT_OK(standard_subscriber_client);
    to_std.standard = *std::move(standard_subscriber_client);
    subspace::SubscriberOptions subscriber_options;
    subscriber_options.SetLogDroppedMessages(false);
    absl::StatusOr<subspace::Subscriber> standard_subscriber =
        to_std.standard->CreateSubscriber(to_std.name, subscriber_options);
    ASSERT_OK(standard_subscriber);
    to_std.standard_subscriber = *std::move(standard_subscriber);

    Direction &to_asil = std_to_asil[channel];
    to_asil.name = absl::StrFormat("/asil_stress/mix/to_asil/%d", channel);
    absl::StatusOr<std::shared_ptr<subspace::Client>> standard_publisher_client =
        subspace::Client::Create(Socket(),
                                 absl::StrFormat("mix-std-pub-%d", channel));
    ASSERT_OK(standard_publisher_client);
    to_asil.standard = *std::move(standard_publisher_client);
    absl::StatusOr<subspace::Publisher> standard_publisher =
        to_asil.standard->CreatePublisher(
            to_asil.name, subspace::PublisherOptions()
                              .SetSlotSize(64)
                              .SetNumSlots(kNumSlots)
                              .SetFixedSize(true));
    ASSERT_OK(standard_publisher);
    to_asil.standard_publisher = *std::move(standard_publisher);
    ASSERT_TRUE(ConnectAsil(Socket(), absl::StrFormat("mix-sub-%d", channel),
                            to_asil.asil));
    to_asil.asil_subscriber = std::make_unique<asil::Subscriber>();
    ASSERT_EQ(asil::Error::kOk, to_asil.asil.client->CreateSubscriber(
                                    to_asil.name.c_str(), {},
                                    *to_asil.asil_subscriber));
  }

  constexpr char kQueue[] = "/asil_stress/mix/queue";
  struct Payload {
    uint64_t magic;
    uint32_t publisher;
    uint32_t sequence;
    uint64_t checksum;
  };
  constexpr uint64_t kMagic = 0x4153494c51554555ULL;
  std::vector<AsilLink> queue_links(kNumQueuePublishers);
  std::vector<std::unique_ptr<asil::Publisher>> queue_publishers(
      kNumQueuePublishers);
  for (int publisher = 0; publisher < kNumQueuePublishers; ++publisher) {
    ASSERT_TRUE(ConnectAsil(Socket(),
                            absl::StrFormat("mix-qpub-%d", publisher),
                            queue_links[publisher]));
    asil::PublisherOptions options = PubOptions(64, kQueueSlots);
    options.subscriber_queue_arena_size =
        subspace::kDefaultSubscriberQueueArenaSize;
    queue_publishers[publisher] = std::make_unique<asil::Publisher>();
    ASSERT_EQ(asil::Error::kOk,
              queue_links[publisher].client->CreatePublisher(
                  kQueue, options, *queue_publishers[publisher]))
        << queue_links[publisher].client->LastServerError();
  }
  std::vector<std::shared_ptr<subspace::Client>> queue_clients;
  std::vector<subspace::Subscriber> queue_subscribers;
  for (int subscriber = 0; subscriber < kNumQueueSubscribers; ++subscriber) {
    absl::StatusOr<std::shared_ptr<subspace::Client>> queue_client =
        subspace::Client::Create(Socket(),
                                 absl::StrFormat("mix-qsub-%d", subscriber));
    ASSERT_OK(queue_client);
    queue_clients.push_back(*std::move(queue_client));
    subspace::SubscriberOptions options;
    options.SetSubscriberQueueSize(1 << (subscriber % 7));
    options.SetLogDroppedMessages(false);
    absl::StatusOr<subspace::Subscriber> queue_subscriber =
        queue_clients.back()->CreateSubscriber(kQueue, options);
    ASSERT_OK(queue_subscriber);
    queue_subscribers.push_back(*std::move(queue_subscriber));
  }

  std::atomic<int> failures{0};
  std::atomic<bool> queues_done{false};
  std::vector<int> queue_received(kNumQueueSubscribers, 0);
  std::vector<std::thread> threads;

  for (int channel = 0; channel < kNumChannels; ++channel) {
    threads.emplace_back([&, channel] {
      PublishSequences(*asil_to_std[channel].asil_publisher, kNumMessages,
                       failures);
    });
    threads.emplace_back([&, channel] {
      subspace::Subscriber &subscriber =
          *asil_to_std[channel].standard_subscriber;
      uint32_t expect = 0;
      while (expect < static_cast<uint32_t>(kNumMessages) &&
             failures.load(std::memory_order_relaxed) == 0) {
        absl::StatusOr<subspace::Message> message = subscriber.ReadMessage();
        if (!message.ok()) {
          Fail(failures, message.status().ToString());
          return;
        }
        if (message->length == 0) {
          if (!subscriber.Wait().ok()) {
            Fail(failures, "standard subscriber wait failed");
            return;
          }
          continue;
        }
        uint32_t sequence = 0;
        std::memcpy(&sequence, message->buffer, sizeof(sequence));
        if (sequence != expect) {
          Fail(failures, absl::StrFormat("standard subscriber sequence %u",
                                         sequence));
          return;
        }
        ++expect;
      }
    });
    threads.emplace_back([&, channel] {
      subspace::Publisher &publisher = *std_to_asil[channel].standard_publisher;
      for (uint32_t sequence = 0; sequence < static_cast<uint32_t>(kNumMessages);
           ++sequence) {
        if (failures.load(std::memory_order_relaxed) != 0) {
          return;
        }
        absl::StatusOr<void *> buffer = publisher.GetMessageBuffer();
        if (!buffer.ok() || *buffer == nullptr) {
          Fail(failures, "standard publisher has no buffer");
          return;
        }
        std::memcpy(*buffer, &sequence, sizeof(sequence));
        if (!publisher.PublishMessage(sizeof(sequence)).ok()) {
          Fail(failures, "standard publish failed");
          return;
        }
      }
    });
    threads.emplace_back([&, channel] {
      ReadSequences(*std_to_asil[channel].asil_subscriber, kNumMessages,
                    failures);
    });
  }

  for (int subscriber = 0; subscriber < kNumQueueSubscribers; ++subscriber) {
    threads.emplace_back([&, subscriber] {
      std::vector<int64_t> last(kNumQueuePublishers, -1);
      auto take = [&](const subspace::Message &message) {
        if (message.length != sizeof(Payload)) {
          Fail(failures, "queue message has the wrong length");
          return;
        }
        Payload payload;
        std::memcpy(&payload, message.buffer, sizeof(payload));
        const uint64_t checksum =
            payload.magic ^
            (static_cast<uint64_t>(payload.publisher) << 32) ^ payload.sequence;
        if (payload.magic != kMagic || payload.checksum != checksum ||
            payload.publisher >= static_cast<uint32_t>(kNumQueuePublishers)) {
          Fail(failures, "queue message is corrupt");
          return;
        }
        if (static_cast<int64_t>(payload.sequence) <=
            last[payload.publisher]) {
          Fail(failures, "queue sequence went backwards");
          return;
        }
        last[payload.publisher] = payload.sequence;
        ++queue_received[subscriber];
      };
      while (failures.load(std::memory_order_relaxed) == 0) {
        absl::StatusOr<subspace::Message> message =
            queue_subscribers[subscriber].ReadMessage();
        if (!message.ok()) {
          Fail(failures, message.status().ToString());
          return;
        }
        if (message->length == 0) {
          if (queues_done.load(std::memory_order_acquire)) {
            break;
          }
          (void)queue_subscribers[subscriber].Wait(std::chrono::milliseconds(100));
          continue;
        }
        take(*message);
      }
      for (;;) {
        absl::StatusOr<subspace::Message> message =
            queue_subscribers[subscriber].ReadMessage();
        if (!message.ok() || message->length == 0) {
          break;
        }
        take(*message);
      }
    });
  }
  for (int publisher = 0; publisher < kNumQueuePublishers; ++publisher) {
    threads.emplace_back([&, publisher] {
      for (uint32_t sequence = 0;
           sequence < static_cast<uint32_t>(kQueueMessages); ++sequence) {
        if (failures.load(std::memory_order_relaxed) != 0) {
          return;
        }
        asil::Publisher &pub = *queue_publishers[publisher];
        void *buffer = pub.Buffer();
        if (buffer == nullptr) {
          Fail(failures, "queue publisher has no slot");
          return;
        }
        Payload payload = {
            kMagic, static_cast<uint32_t>(publisher), sequence,
            kMagic ^ (static_cast<uint64_t>(publisher) << 32) ^ sequence};
        std::memcpy(buffer, &payload, sizeof(payload));
        if (pub.Publish(sizeof(payload)) != asil::Error::kOk) {
          Fail(failures, "queue publish failed");
          return;
        }
      }
    });
  }

  // Queue subscribers are still running when the publishers finish. Setting
  // queues_done first would let them stop between a publish and the matching
  // read; joining them first deadlocks, because they wait for the flag.
  const int queue_subscriber_begin = static_cast<int>(threads.size()) -
                                     kNumQueueSubscribers - kNumQueuePublishers;
  const int queue_publisher_begin =
      static_cast<int>(threads.size()) - kNumQueuePublishers;
  for (int i = 0; i < queue_subscriber_begin; ++i) {
    threads[i].join();
  }
  for (int i = queue_publisher_begin; i < static_cast<int>(threads.size());
       ++i) {
    threads[i].join();
  }
  queues_done.store(true, std::memory_order_release);
  for (int i = queue_subscriber_begin; i < queue_publisher_begin; ++i) {
    threads[i].join();
  }
  EXPECT_EQ(0, failures.load());
  for (int received : queue_received) {
    EXPECT_GT(received, 0);
  }
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
