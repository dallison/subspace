// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/test_fixture.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <fstream>
#include <inttypes.h>
#include <limits>
#include <sys/resource.h>

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");
ABSL_FLAG(bool, use_split_buffers, false,
          "Run publishers with split-buffer payload storage");

using InetAddress = toolbelt::InetAddress;

class StressTest : public SubspaceTestBase {};

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
static co::CoroutineScheduler *g_scheduler;
#endif

int StressValueForSplitBuffers(int normal_value, int split_buffer_value,
                               int low_resource_value = -1) {
  static const uint64_t open_file_limit = [] {
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) != 0 ||
        limit.rlim_cur == RLIM_INFINITY) {
      return std::numeric_limits<uint64_t>::max();
    }
    return static_cast<uint64_t>(limit.rlim_cur);
  }();
  static const uint64_t map_count_limit = [] {
#ifdef __linux__
    std::ifstream file("/proc/sys/vm/max_map_count");
    uint64_t value = 0;
    if (file >> value) {
      return value;
    }
#endif
    return std::numeric_limits<uint64_t>::max();
  }();

  const bool low_resource_host =
      open_file_limit < 4096 || map_count_limit < 262144;
  if (!absl::GetFlag(FLAGS_use_split_buffers) && !low_resource_host) {
    return normal_value;
  }
  return low_resource_value > 0 ? low_resource_value : split_buffer_value;
}

// For debugging, hit ^\ to dump all coroutines if this test is not working
// properly.
static void SigQuitHandler(int sig) {
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
  std::cout << "\nAll coroutines:" << std::endl;
  g_scheduler->Show();
#endif
  signal(sig, SIG_DFL);
  (void)raise(sig);
}

TEST_F(StressTest, Coroutines) {
  auto oldSig = signal(SIGQUIT, SigQuitHandler);

  const int kNumClients = StressValueForSplitBuffers(10, 4);
  const int kNumChannels =
      StressValueForSplitBuffers(100, 20); // Channels per client.
  const int kNumSlots = kNumChannels * 2 + 1;

  struct Client {
    explicit Client(std::shared_ptr<subspace::Client> client)
        : client(std::move(client)) {}
    std::shared_ptr<subspace::Client> client;
    std::vector<std::string> channels;
    std::vector<subspace::Publisher> pubs;
    std::vector<subspace::Subscriber> subs;
  };

  TestCoroMachine machine;
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
  g_scheduler = &machine.Scheduler();
#endif

  std::vector<Client> clients;
  for (int i = 0; i < kNumClients; i++) {
    clients.emplace_back(EVAL_AND_ASSERT_OK(
        subspace::Client::Create(Socket(), absl::StrFormat("client%d", i))));
    // Create the channel names.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].channels.push_back(absl::StrFormat("%d/%d", i, j));
    }

    // Create the publishers.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].pubs.emplace_back(
          EVAL_AND_ASSERT_OK(clients[i].client->CreatePublisher(
              clients[i].channels[j],
              subspace::PublisherOptions().SetSlotSize(32).SetNumSlots(kNumSlots))));
    }

    // Create the subscribers.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].subs.emplace_back(EVAL_AND_ASSERT_OK(
          clients[i].client->CreateSubscriber(clients[i].channels[j])));
    }
  }

  // Publish messages
  auto publish = [&](TestCoroContext &c, int client_id, int pubId) {
    for (int i = 0; i < kNumSlots - 1; i++) {
      absl::StatusOr<void *> buffer =
          clients[client_id].pubs[pubId].GetMessageBuffer();
      ASSERT_OK(buffer);
      memcpy(*buffer, "foobar", 6);
      absl::StatusOr<const Message> pubStatus =
          clients[client_id].pubs[pubId].PublishMessage(6);
      ASSERT_OK(pubStatus);
    }
  };

  // Publish all messages from all publishers in all clients.
  auto publish_all = [&](TestCoroContext &c, int client_id) {
    for (int i = 0; i < kNumChannels; i++) {
      machine.Spawn([&publish, client_id, i](TestCoroContext &c1) {
        publish(c1, client_id, i);
      });
      c.Yield();
    }
  };

  // Publish messages.
  for (int i = 0; i < kNumClients; i++) {
    machine.Spawn(
        [&publish_all, i](TestCoroContext &c) { publish_all(c, i); });
  }

  // Read all messages from a single subscriber.
  auto read = [&](TestCoroContext &c, int client_id, int subId) {
    int num_messages = 0;
    while (num_messages < kNumSlots - 1) {
      // Wait for notification of a message.
      c.Wait(clients[client_id].subs[subId].GetPollFd().fd);
      // Read all available messages.
      for (;;) {
        absl::StatusOr<Message> m =
            clients[client_id].subs[subId].ReadMessage();
        ASSERT_OK(m);
        auto msg = *m;
        if (msg.length == 0) {
          break;
        }
        num_messages++;
        ASSERT_EQ(6, msg.length);
      }
    }
  };

  // Read all messages in all clients.
  auto readAll = [&](TestCoroContext &c, int client_id) {
    for (int i = 0; i < kNumChannels; i++) {
      machine.Spawn([&read, client_id, i](TestCoroContext &c1) {
        read(c1, client_id, i);
      });
    }
  };

  // Read messages.
  for (int i = 0; i < kNumClients; i++) {
    machine.Spawn([&readAll, i](TestCoroContext &c) { readAll(c, i); });
  }

  machine.Run();

  signal(SIGQUIT, oldSig);
}

TEST_F(StressTest, Threads) {
  auto oldSig = signal(SIGQUIT, SigQuitHandler);

  // Be aware of the static limit to the number of channels.
  //
  // Also be aware that the maximum number of mapped vm segments
  // is about 64K by default.  This limits the number of channels
  // to around 6560 in a single process.
  //
  // Also, also, valgrind isn't happy if we exceed some sort of
  // internal maximum (VG_N_SEGMENTS).  This seems to be 30000.
  //
  // This tests uses threads so will be non-deterministic in the order
  // that things happen.
  //
  const int kNumClients = StressValueForSplitBuffers(10, 4);
  const int kNumChannels =
      StressValueForSplitBuffers(100, 12); // Channels per client.

  constexpr int kMaxActiveMessages = 5;
  // Number of channels accounts for all publishers, subscribers and unique_ptrs
  const int kNumSlots = kNumChannels * (2 + kMaxActiveMessages) + 1;

  struct Client {
    explicit Client(std::shared_ptr<subspace::Client> client)
        : client(std::move(client)) {}
    std::shared_ptr<subspace::Client> client;
    std::vector<std::string> channels;
    std::vector<subspace::Publisher> pubs;
    std::vector<subspace::Subscriber> subs;
  };

  std::vector<Client> clients;
  for (int i = 0; i < kNumClients; i++) {
    clients.emplace_back(EVAL_AND_ASSERT_OK(
        subspace::Client::Create(Socket(), absl::StrFormat("client%d", i))));
    // Create the channel names.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].channels.push_back(absl::StrFormat("%d/%d", i, j));
    }

    // Create the publishers.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].pubs.emplace_back(
          EVAL_AND_ASSERT_OK(clients[i].client->CreatePublisher(
              clients[i].channels[j],
              subspace::PublisherOptions().SetSlotSize(32).SetNumSlots(kNumSlots))));
    }

    // Create the subscribers.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].subs.emplace_back(
          EVAL_AND_ASSERT_OK(clients[i].client->CreateSubscriber(
              clients[i].channels[j],
              subspace::SubscriberOptions().SetMaxActiveMessages(kMaxActiveMessages))));
    }
  }

  std::vector<std::thread> threads;

  // Publish messages
  auto publish = [&](int client_id, int pubId) {
    for (int i = 0; i < kNumSlots - 1; i++) {
      absl::StatusOr<void *> buffer =
          clients[client_id].pubs[pubId].GetMessageBuffer();
      ASSERT_OK(buffer);
      memcpy(*buffer, "foobar", 6);
      absl::StatusOr<const Message> pubStatus =
          clients[client_id].pubs[pubId].PublishMessage(6);
      ASSERT_OK(pubStatus);
    }
  };

  // Publish all messages from all publishers in all clients.
  auto publish_all = [&](int client_id) {
    for (int i = 0; i < kNumChannels; i++) {
      publish(client_id, i);
    }
  };

  // Publish messages.
  for (int i = 0; i < kNumClients; i++) {
    threads.push_back(std::thread([&, i]() { publish_all(i); }));
  }

  // Read all messages from a single subscriber.
  auto read = [&](int client_id, int subId) {
    int num_messages = 0;
    while (num_messages < kNumSlots - 1) {
      // Wait for notification of a message.
      ASSERT_OK(clients[client_id].subs[subId].Wait());

      // Read all available messages up to our active message limit.
      absl::StatusOr<std::vector<::subspace::Message>> msgs =
          clients[client_id].subs[subId].GetAllMessages();
      ASSERT_OK(msgs);
      auto all = *msgs;
      for (auto &msg : all) {
        if (msg.length == 0) {
          break;
        }
        num_messages++;
        ASSERT_EQ(6, msg.length);
      }
    }
  };

  // Read all messages in all clients.
  auto readAll = [&](int client_id) {
    for (int i = 0; i < kNumChannels; i++) {
      read(client_id, i);
    }
  };

  // Read messages.
  for (int i = 0; i < kNumClients; i++) {
    threads.push_back(std::thread([&, i]() { readAll(i); }));
  }

  // Wait for all threads to complete.
  for (auto &t : threads) {
    t.join();
  }
  signal(SIGQUIT, oldSig);
}

TEST_F(StressTest, ThreadSafety) {
  auto oldSig = signal(SIGQUIT, SigQuitHandler);

  // Uses one client with a thread per pub and sub.  Stress test the thread safety of the client.
  const int kNumChannels =
      StressValueForSplitBuffers(100, 24); // Channels per client.

  constexpr int kMaxActiveMessages = 5;
  // Number of channels accounts for all publishers, subscribers and unique_ptrs
  const int kNumSlots = kNumChannels * (2 + kMaxActiveMessages) + 1;

  std::shared_ptr<subspace::Client> client;

  std::vector<std::string> channels;
  std::vector<subspace::Publisher> pubs;
  std::vector<subspace::Subscriber> subs;

  client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), absl::StrFormat("client")));
  client->SetThreadSafe(true);

  // Create the channel names.
  for (int j = 0; j < kNumChannels; j++) {
    channels.push_back(absl::StrFormat("/foobar/%d", j));
  }
  // Create the publishers.
  for (int j = 0; j < kNumChannels; j++) {
    pubs.emplace_back(EVAL_AND_ASSERT_OK(client->CreatePublisher(
        channels[j], subspace::PublisherOptions().SetSlotSize(32).SetNumSlots(kNumSlots))));
  }

  // Create the subscribers.
  for (int j = 0; j < kNumChannels; j++) {
    subs.emplace_back(EVAL_AND_ASSERT_OK(client->CreateSubscriber(
        channels[j], subspace::SubscriberOptions().SetMaxActiveMessages(kMaxActiveMessages))));
  }

  std::vector<std::thread> threads;

  // Publish messages
  auto publish = [&](int pubId) {
    for (int i = 0; i < kNumSlots - 1; i++) {
      absl::StatusOr<void *> buffer = pubs[pubId].GetMessageBuffer();
      ASSERT_OK(buffer);
      memcpy(*buffer, "foobar", 6);
      absl::StatusOr<const Message> pubStatus = pubs[pubId].PublishMessage(6);
      ASSERT_OK(pubStatus);
    }
  };

  // Publish all messages from all publishers in all clients.
  for (int i = 0; i < kNumChannels; i++) {
    threads.push_back(std::thread([&, i]() { publish(i); }));
  }

  // Read all messages from a single subscriber.
  auto read = [&](int subId) {
    int num_messages = 0;
    while (num_messages < kNumSlots - 1) {
      // Wait for notification of a message.
      ASSERT_OK(subs[subId].Wait());

      // Read all available messages up to our active message limit.
      absl::StatusOr<std::vector<::subspace::Message>> msgs =
          subs[subId].GetAllMessages();
      ASSERT_OK(msgs);
      auto all = *msgs;
      for (auto &msg : all) {
        if (msg.length == 0) {
          break;
        }
        num_messages++;
        ASSERT_EQ(6, msg.length);
      }
    }
  };

  // Read all messages in all clients.
  for (int i = 0; i < kNumChannels; i++) {
    threads.push_back(std::thread([&, i]() { read(i); }));
  }

  std::cerr << threads.size() << " threads created" << std::endl;

  // Wait for all threads to complete.
  for (auto &t : threads) {
    t.join();
  }
  signal(SIGQUIT, oldSig);
}

TEST_F(StressTest, SubscriberQueuesManyPublishersAndSubscribers) {
  const int kNumPublishers = StressValueForSplitBuffers(8, 4);
  const int kNumSubscribers = StressValueForSplitBuffers(16, 8);
  const int kMessagesPerPublisher =
      StressValueForSplitBuffers(10000, 2000);
  const int kNumSlots = StressValueForSplitBuffers(256, 128);
  constexpr int kDefaultQueueSize = 64;
  constexpr uint64_t kMagic = 0x5155455545535452;
  constexpr char kChannel[] = "/subscriber_queue_stress";

  struct Payload {
    uint64_t magic;
    uint32_t publisher;
    uint32_t sequence;
    uint64_t checksum;
  };

  std::vector<std::shared_ptr<subspace::Client>> publisher_clients;
  std::vector<subspace::Publisher> publishers;
  publisher_clients.reserve(kNumPublishers);
  publishers.reserve(kNumPublishers);
  for (int i = 0; i < kNumPublishers; ++i) {
    publisher_clients.push_back(
        EVAL_AND_ASSERT_OK(subspace::Client::Create(
            Socket(), absl::StrFormat("queue_publisher_%d", i))));
    publishers.push_back(
        EVAL_AND_ASSERT_OK(publisher_clients.back()->CreatePublisher(
            kChannel,
            subspace::PublisherOptions()
                .SetSlotSize(sizeof(Payload))
                .SetNumSlots(kNumSlots)
                .SetSubscriberQueueSize(kDefaultQueueSize))));
  }

  std::vector<std::shared_ptr<subspace::Client>> subscriber_clients;
  std::vector<subspace::Subscriber> subscribers;
  subscriber_clients.reserve(kNumSubscribers);
  subscribers.reserve(kNumSubscribers);
  for (int i = 0; i < kNumSubscribers; ++i) {
    const int queue_size = 1 << (i % 7);
    subscriber_clients.push_back(
        EVAL_AND_ASSERT_OK(subspace::Client::Create(
            Socket(), absl::StrFormat("queue_subscriber_%d", i))));
    subspace::SubscriberOptions options;
    options.SetSubscriberQueueSize(queue_size);
    options.SetLogDroppedMessages(false);
    subscribers.push_back(
        EVAL_AND_ASSERT_OK(subscriber_clients.back()->CreateSubscriber(
            kChannel, options)));
    ASSERT_EQ(queue_size, subscribers.back().SubscriberQueueSize());
  }

  std::atomic<bool> start = false;
  std::atomic<bool> publishers_done = false;
  std::atomic<int> failures = 0;
  std::vector<int> received(kNumSubscribers, 0);
  std::vector<std::thread> subscriber_threads;
  subscriber_threads.reserve(kNumSubscribers);
  for (int sub_id = 0; sub_id < kNumSubscribers; ++sub_id) {
    subscriber_threads.emplace_back([&, sub_id]() {
      std::vector<int64_t> last_sequence(kNumPublishers, -1);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (;;) {
        absl::StatusOr<Message> message = subscribers[sub_id].ReadMessage();
        if (!message.ok()) {
          ++failures;
          return;
        }
        if (message->length == 0) {
          if (publishers_done.load(std::memory_order_acquire)) {
            return;
          }
          std::this_thread::yield();
          continue;
        }
        if (message->length != sizeof(Payload)) {
          ++failures;
          continue;
        }

        Payload payload;
        memcpy(&payload, message->buffer, sizeof(payload));
        const uint64_t checksum =
            payload.magic ^
            (static_cast<uint64_t>(payload.publisher) << 32) ^
            payload.sequence;
        if (payload.magic != kMagic ||
            payload.publisher >= static_cast<uint32_t>(kNumPublishers) ||
            payload.sequence >=
                static_cast<uint32_t>(kMessagesPerPublisher) ||
            payload.checksum != checksum) {
          ++failures;
          continue;
        }
        if (static_cast<int64_t>(payload.sequence) <=
            last_sequence[payload.publisher]) {
          ++failures;
          continue;
        }
        last_sequence[payload.publisher] = payload.sequence;
        ++received[sub_id];
      }
    });
  }

  std::vector<std::thread> publisher_threads;
  publisher_threads.reserve(kNumPublishers);
  for (int pub_id = 0; pub_id < kNumPublishers; ++pub_id) {
    publisher_threads.emplace_back([&, pub_id]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int sequence = 0; sequence < kMessagesPerPublisher; ++sequence) {
        Payload payload = {
            kMagic,
            static_cast<uint32_t>(pub_id),
            static_cast<uint32_t>(sequence),
            kMagic ^ (static_cast<uint64_t>(pub_id) << 32) ^
                static_cast<uint32_t>(sequence),
        };
        absl::StatusOr<void *> buffer =
            publishers[pub_id].GetMessageBuffer(sizeof(payload));
        if (!buffer.ok()) {
          ++failures;
          return;
        }
        memcpy(*buffer, &payload, sizeof(payload));
        if (!publishers[pub_id].PublishMessage(sizeof(payload)).ok()) {
          ++failures;
          return;
        }
      }
    });
  }

  start.store(true, std::memory_order_release);
  for (auto &thread : publisher_threads) {
    thread.join();
  }
  publishers_done.store(true, std::memory_order_release);
  for (auto &thread : subscriber_threads) {
    thread.join();
  }

  EXPECT_EQ(0, failures.load());
  for (int sub_id = 0; sub_id < kNumSubscribers; ++sub_id) {
    EXPECT_GT(received[sub_id], 0) << "subscriber " << sub_id;
  }
}

TEST_F(StressTest, SubscriberQueueChurnDuringConcurrentPublishing) {
  const int kNumPublishers = StressValueForSplitBuffers(4, 2);
  const int kNumSubscriberThreads = StressValueForSplitBuffers(8, 4);
  const int kCyclesPerThread = StressValueForSplitBuffers(800, 1400);
  constexpr int kDefaultQueueSize = 64;
  constexpr int kNumSlots = 128;
  constexpr char kChannel[] = "/subscriber_queue_churn_stress";

  std::vector<std::shared_ptr<subspace::Client>> publisher_clients;
  std::vector<subspace::Publisher> publishers;
  publisher_clients.reserve(kNumPublishers);
  publishers.reserve(kNumPublishers);
  for (int i = 0; i < kNumPublishers; ++i) {
    publisher_clients.push_back(
        EVAL_AND_ASSERT_OK(subspace::Client::Create(
            Socket(), absl::StrFormat("queue_churn_publisher_%d", i))));
    publishers.push_back(
        EVAL_AND_ASSERT_OK(publisher_clients.back()->CreatePublisher(
            kChannel,
            subspace::PublisherOptions()
                .SetSlotSize(sizeof(uint64_t))
                .SetNumSlots(kNumSlots)
                .SetSubscriberQueueSize(kDefaultQueueSize))));
  }

  std::vector<std::shared_ptr<subspace::Client>> subscriber_clients;
  subscriber_clients.reserve(kNumSubscriberThreads);
  for (int i = 0; i < kNumSubscriberThreads; ++i) {
    subscriber_clients.push_back(
        EVAL_AND_ASSERT_OK(subspace::Client::Create(
            Socket(), absl::StrFormat("queue_churn_subscriber_%d", i))));
  }

  std::atomic<bool> start = false;
  std::atomic<bool> stop_publishers = false;
  std::atomic<int> failures = 0;
  std::atomic<int> messages_published = 0;
  std::atomic<int> messages_received = 0;

  std::vector<std::thread> publisher_threads;
  publisher_threads.reserve(kNumPublishers);
  for (int pub_id = 0; pub_id < kNumPublishers; ++pub_id) {
    publisher_threads.emplace_back([&, pub_id]() {
      uint64_t sequence = static_cast<uint64_t>(pub_id) << 56;
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      while (!stop_publishers.load(std::memory_order_acquire)) {
        absl::StatusOr<void *> buffer =
            publishers[pub_id].GetMessageBuffer(sizeof(sequence));
        if (!buffer.ok()) {
          ++failures;
          return;
        }
        memcpy(*buffer, &sequence, sizeof(sequence));
        if (!publishers[pub_id].PublishMessage(sizeof(sequence)).ok()) {
          ++failures;
          return;
        }
        ++sequence;
        ++messages_published;
      }
    });
  }

  std::vector<std::thread> subscriber_threads;
  subscriber_threads.reserve(kNumSubscriberThreads);
  for (int thread_id = 0; thread_id < kNumSubscriberThreads; ++thread_id) {
    subscriber_threads.emplace_back([&, thread_id]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int cycle = 0; cycle < kCyclesPerThread; ++cycle) {
        const int queue_size = 1 << ((thread_id + cycle) % 6);
        subspace::SubscriberOptions options;
        options.SetSubscriberQueueSize(queue_size);
        options.SetLogDroppedMessages(false);
        absl::StatusOr<subspace::Subscriber> subscriber =
            subscriber_clients[thread_id]->CreateSubscriber(
                kChannel, options);
        if (!subscriber.ok()) {
          ++failures;
          return;
        }
        if (subscriber->SubscriberQueueSize() != queue_size) {
          ++failures;
          return;
        }

        for (int attempt = 0; attempt < 32; ++attempt) {
          const subspace::ReadMode mode =
              (cycle + attempt) % 2 == 0
                  ? subspace::ReadMode::kReadNext
                  : subspace::ReadMode::kReadNewest;
          absl::StatusOr<Message> message = subscriber->ReadMessage(mode);
          if (!message.ok()) {
            ++failures;
            return;
          }
          if (message->length == sizeof(uint64_t)) {
            ++messages_received;
            break;
          }
          std::this_thread::yield();
        }
      }
    });
  }

  start.store(true, std::memory_order_release);
  for (auto &thread : subscriber_threads) {
    thread.join();
  }
  stop_publishers.store(true, std::memory_order_release);
  for (auto &thread : publisher_threads) {
    thread.join();
  }

  EXPECT_EQ(0, failures.load());
  EXPECT_GT(messages_published.load(), 0);
  EXPECT_GT(messages_received.load(), 0);
}

TEST_F(StressTest, ActiveMessages) {
  auto oldSig = signal(SIGQUIT, SigQuitHandler);

  constexpr int kMaxActiveMessages = 51; // Including the subscriber.

  constexpr int kNumPubs = 1;
  constexpr int kNumSpare = 2;
  constexpr int kNumSlots = kMaxActiveMessages + kNumPubs + kNumSpare;
  constexpr int kNumMessages = kNumSlots * 10000;

  struct Client {
    explicit Client(std::shared_ptr<subspace::Client> client)
        : client(std::move(client)) {}
    std::shared_ptr<subspace::Client> client;
    std::string channel;
    std::unique_ptr<subspace::Publisher> pub;
    std::unique_ptr<subspace::Subscriber> sub;
  };

  Client client(EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket())));

  client.channel = "/foobar";
  client.pub = std::make_unique<subspace::Publisher>(
      EVAL_AND_ASSERT_OK(client.client->CreatePublisher(
          client.channel, subspace::PublisherOptions().SetSlotSize(32).SetNumSlots(kNumSlots))));
  client.sub = std::make_unique<subspace::Subscriber>(
      EVAL_AND_ASSERT_OK(client.client->CreateSubscriber(
          client.channel, ([] { subspace::SubscriberOptions opts; opts.SetMaxActiveMessages(kMaxActiveMessages); opts.SetLogDroppedMessages(false); return opts; }()))));

  struct Receiver {
    std::thread thread;
    toolbelt::SharedPtrPipe<subspace::Message> pipe;
  };

  std::vector<Receiver> receivers;
  receivers.reserve(kMaxActiveMessages);
  std::atomic<int> num_messages_received = 0;
  std::atomic<int> num_dropped_messages = 0;

  std::cerr << "expecting " << kNumMessages << " messages" << std::endl;
  // Create the receivers and their pipes first to avoid vector reallocation
  // while threads are running (use-after-free from dangling references).
  for (int i = 0; i < kMaxActiveMessages; i++) {
    receivers.push_back(Receiver());
    auto pipe = toolbelt::SharedPtrPipe<subspace::Message>::Create();
    ASSERT_OK(pipe);
    receivers[i].pipe = *pipe;
  }
  for (int i = 0; i < kMaxActiveMessages; i++) {
    receivers[i].thread = std::thread([&, i]() {
      while (num_messages_received <
             kNumMessages - num_dropped_messages.load()) {
        auto msg = receivers[i].pipe.Read();
        num_messages_received++;
        std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
        msg.IgnoreError();
      }
    });
  }

  std::vector<std::thread> threads;

  // Publish messages
  auto publish = [&]() {
    for (int i = 0; i < kNumMessages; i++) {
      absl::StatusOr<void *> buffer = client.pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      memcpy(*buffer, "foobar", 6);
      absl::StatusOr<const Message> pubStatus = client.pub->PublishMessage(6);
      ASSERT_OK(pubStatus);
      // Sleep for a small amount of time to allow the receiver to read the
      // message.
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 20));
    }
  };

  std::atomic<int> total_messages_received = 0;

  // Read all messages from a single subscriber and write to random receivers.
  auto read = [&]() {
    uint64_t last_ordinal = 0;
    int32_t last_slot = -1;
    while (total_messages_received <
           kNumMessages - num_dropped_messages.load()) {
      // Read all available messages up to our active message limit.
      absl::StatusOr<std::vector<::subspace::Message>> msgs =
          client.sub->GetAllMessages();
      ASSERT_OK(msgs);
      auto all = *msgs;
      for (auto &msg : all) {
        if (msg.length == 0) {
          break;
        }
        // We are sending data too fast for the dropped message detector
        // in the subscriber to correctly count the them.  We will count
        // them here instead.
        if (last_ordinal > 0) {
          if (msg.ordinal <= last_ordinal) {
            std::cerr << "ordinal ordering issue " << msg.ordinal << " in slot "
                      << msg.slot_id << " last " << last_ordinal << " in slot "
                      << last_slot << std::endl;
            client.sub->DumpSlots(std::cerr);
          }
          ASSERT_GT(msg.ordinal, last_ordinal);
          num_dropped_messages += msg.ordinal - last_ordinal - 1;
        }
        last_ordinal = msg.ordinal;
        last_slot = msg.slot_id;
        int receiver = rand() % receivers.size();
        ASSERT_OK(receivers[receiver].pipe.Write(
            std::make_shared<subspace::Message>(std::move(msg))));
        total_messages_received++;
        std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
      }
    }
  };

  // Read messages.
  threads.push_back(std::thread([&]() { read(); }));

  // Publish messages.
  threads.push_back(std::thread([&]() { publish(); }));

  // Wait for all threads to complete.
  for (auto &t : threads) {
    t.join();
  }
  std::cerr << "subscribers received " << total_messages_received << " messages"
            << std::endl;
  std::cerr << "we dropped " << num_dropped_messages << " messages"
            << std::endl;
  // Send one more messages to wake up the receivers.
  for (auto &r : receivers) {
    subspace::Message msg;
    ASSERT_OK(
        r.pipe.Write(std::make_shared<subspace::Message>(std::move(msg))));
  }

  // Wait for receivers to stop.
  for (auto &r : receivers) {
    r.thread.join();
  }
  std::cerr << "receivers received "
            << (num_messages_received - receivers.size() + 1) << " messages"
            << std::endl;

  signal(SIGQUIT, oldSig);
}

TEST_F(StressTest, ActiveMessages2) {
  auto oldSig = signal(SIGQUIT, SigQuitHandler);

  constexpr int kMaxActiveMessages = 5; // Including the subscriber.

  constexpr int kNumPubs = 1;
  constexpr int kNumSpare = 2;
  constexpr int kNumSlots = kMaxActiveMessages + kNumPubs + kNumSpare;
  constexpr int kNumMessages = kNumSlots * 1000;

  struct Client {
    explicit Client(std::shared_ptr<subspace::Client> client)
        : client(std::move(client)) {}
    std::shared_ptr<subspace::Client> client;
    std::string channel;
    std::unique_ptr<subspace::Publisher> pub;
    std::unique_ptr<subspace::Subscriber> sub;
  };

  Client client(EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket())));

  client.channel = "/foobar";
  client.pub = std::make_unique<subspace::Publisher>(
      EVAL_AND_ASSERT_OK(client.client->CreatePublisher(
          client.channel, subspace::PublisherOptions().SetSlotSize(32).SetNumSlots(kNumSlots))));
  client.sub = std::make_unique<subspace::Subscriber>(
      EVAL_AND_ASSERT_OK(client.client->CreateSubscriber(
          client.channel, ([] { subspace::SubscriberOptions opts; opts.SetMaxActiveMessages(kMaxActiveMessages); opts.SetLogDroppedMessages(false); return opts; }()))));

  struct Receiver {
    std::thread thread;
    toolbelt::SharedPtrPipe<subspace::Message> pipe;
  };

  std::vector<Receiver> receivers;
  receivers.reserve(kMaxActiveMessages);
  std::atomic<int> num_messages_received = 0;
  std::atomic<int> num_dropped_messages = 0;

  std::cerr << "expecting " << kNumMessages << " messages" << std::endl;
  // Create the receivers and their pipes first to avoid vector reallocation
  // while threads are running (use-after-free from dangling references).
  for (int i = 0; i < kMaxActiveMessages; i++) {
    receivers.push_back(Receiver());
    auto pipe = toolbelt::SharedPtrPipe<subspace::Message>::Create();
    ASSERT_OK(pipe);
    receivers[i].pipe = *pipe;
  }
  for (int i = 0; i < kMaxActiveMessages; i++) {
    receivers[i].thread = std::thread([&, i]() {
      while (num_messages_received <
             kNumMessages - num_dropped_messages.load()) {
        auto msg = receivers[i].pipe.Read();
        num_messages_received++;
        std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
        msg.IgnoreError();
      }
    });
  }

  std::vector<std::thread> threads;

  // Publish messages
  auto publish = [&]() {
    for (int i = 0; i < kNumMessages; i++) {
      absl::StatusOr<void *> buffer = client.pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      memcpy(*buffer, "foobar", 6);
      absl::StatusOr<const Message> pubStatus = client.pub->PublishMessage(6);
      ASSERT_OK(pubStatus);
      // Sleep for a small amount of time to allow the receiver to read the
      // message.
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 20));
    }
  };

  std::atomic<int> total_messages_received = 0;

  // Read all messages from a single subscriber and write to random receivers.
  auto read = [&]() {
    uint64_t last_ordinal = 0;
    while (total_messages_received <
           kNumMessages - num_dropped_messages.load()) {
      absl::StatusOr<Message> msg = client.sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length == 0) {
        continue;
      }
      // We are sending data too fast for the dropped message detector
      // in the subscriber to correctly count the them.  We will count
      // them here instead.
      ASSERT_GT(msg->ordinal, last_ordinal);
      num_dropped_messages += msg->ordinal - last_ordinal - 1;
      last_ordinal = msg->ordinal;
      int receiver = rand() % receivers.size();
      ASSERT_OK(receivers[receiver].pipe.Write(
          std::make_shared<subspace::Message>(std::move(*msg))));
      total_messages_received++;
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
    }
  };

  // Read messages.
  threads.push_back(std::thread([&]() { read(); }));

  // Publish messages.
  threads.push_back(std::thread([&]() { publish(); }));

  // Wait for all threads to complete.
  for (auto &t : threads) {
    t.join();
  }
  std::cerr << "subscribers received " << total_messages_received << " messages"
            << std::endl;
  std::cerr << "we dropped " << num_dropped_messages << " messages"
            << std::endl;
  // Send one more messages to wake up the receivers.
  for (auto &r : receivers) {
    subspace::Message msg;
    ASSERT_OK(
        r.pipe.Write(std::make_shared<subspace::Message>(std::move(msg))));
  }

  // Wait for receivers to stop.
  for (auto &r : receivers) {
    r.thread.join();
  }
  std::cerr << "receivers received "
            << (num_messages_received - receivers.size() + 1) << " messages"
            << std::endl;

  signal(SIGQUIT, oldSig);
}

TEST_F(StressTest, VirtualChannels) {
  auto subClient = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  const int kNumSlots = StressValueForSplitBuffers(3000, 160);
  const int kNumThreads = StressValueForSplitBuffers(8, 4);
  const int kVchansPerThread = StressValueForSplitBuffers(125, 10);
  const int kNumVchans = kNumThreads * kVchansPerThread;
  const int kNumMessages = kNumThreads * kNumVchans;

  std::vector<std::shared_ptr<subspace::Client>> clients;
  std::vector<subspace::Publisher> publishers;
  for (int i = 0; i < kNumVchans; i++) {
    clients.push_back(EVAL_AND_ASSERT_OK(
        subspace::Client::Create(Socket(), absl::StrFormat("client%d", i))));
    absl::StatusOr<Publisher> p = clients[i]->CreatePublisher(
        absl::StrFormat("/foobar/%d", i),
        subspace::PublisherOptions().SetSlotSize(64).SetNumSlots(kNumSlots).SetMux("/foobar"));
    ASSERT_OK(p);
    publishers.push_back(std::move(*p));
  }

  // Create a subscriber to the mux.
  absl::StatusOr<Subscriber> s = subClient->CreateSubscriber("/foobar");
  ASSERT_OK(s);

  // Thread to read messages from the mux.
  std::thread sub_thread([&s, kNumMessages]() {
    int num_messages = 0;
    while (num_messages < kNumMessages) {
      // Read all available messages.
      for (;;) {
        absl::StatusOr<Message> m = s->ReadMessage();
        ASSERT_OK(m);
        auto msg = *m;
        if (msg.length == 0) {
          break;
        }
        num_messages++;
        ASSERT_EQ(6, msg.length);
      }
    }
  });

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; i++) {
    std::thread t([&, i]() {
      for (int j = 0; j < kNumMessages; j++) {
        int pubIndex = i * kVchansPerThread + j % kVchansPerThread;
        auto &pub = publishers[pubIndex];
        absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
        ASSERT_OK(buffer);
        memcpy(*buffer, "foobar", 6);
        absl::StatusOr<const Message> pubStatus = pub.PublishMessage(6);
        ASSERT_OK(pubStatus);
        // Sleep for 10 microseconds.
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
    threads.push_back(std::move(t));
  }

  // Wait for the threads to finish.
  for (auto &t : threads) {
    t.join();
  }

  // Wait for the subscriber to finish.
  sub_thread.join();
}

TEST_F(StressTest, ManyChannelsNonMultiplexed) {
  std::vector<std::shared_ptr<subspace::Client>> pub_clients;
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  const int kNumChannels = StressValueForSplitBuffers(200, 200, 16);
  const int knum_slots = StressValueForSplitBuffers(100, 100, 8);
  constexpr int kSlotSize = 32768;
  const int kNumMessages = StressValueForSplitBuffers(200, 200, 50);
  // Memory used ~= kNumChannels * knum_slots * kSlotSize
  std::vector<std::string> channels;

  for (int i = 0; i < kNumChannels; i++) {
    auto pub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
    pub_clients.push_back(std::move(pub_client));
    channels.push_back(absl::StrFormat("/logs/%d", i));
  }

  std::vector<Publisher> pubs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Publisher> pub = pub_clients[i]->CreatePublisher(
        channels[i], subspace::PublisherOptions().SetSlotSize(kSlotSize).SetNumSlots(knum_slots));
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Subscriber> sub = sub_client->CreateSubscriber(
        channels[i], ([] { subspace::SubscriberOptions opts; opts.SetLogDroppedMessages(false); return opts; }()));
    // std::cerr << "sub status " << sub.status() << "\n";
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }

  std::atomic<int> num_messages = 0;

  // Create a thread to read from all subscribers.
  std::thread sub_thread([&subs, &num_messages, kNumChannels, kNumMessages] {
    std::vector<struct pollfd> fds;
    for (auto &sub : subs) {
      struct pollfd fd = sub.GetPollFd();
      fds.push_back(fd);
    }
    int num_dropped = 0;
    std::vector<uint64_t> last_ordinals(kNumChannels, 0);
    while (num_messages < kNumMessages * kNumChannels - num_dropped) {
      poll(fds.data(), fds.size(), -1);
      for (size_t i = 0; i < fds.size(); i++) {
        if (fds[i].revents & POLLIN) {
          for (;;) {
            absl::StatusOr<Message> msg = subs[i].ReadMessage();
            ASSERT_OK(msg);
            if (msg->length > 0) {
              num_dropped += msg->ordinal - last_ordinals[i] - 1;
              last_ordinals[i] = msg->ordinal;
              num_messages++;
            } else {
              break;
            }
          }
        }
      }
    }
  });

  srand(1234);
  std::vector<uint64_t> periods; // In microseconds
  for (int i = 0; i < kNumChannels; i++) {
    periods.push_back(5000 + rand() % 100000);
  }
  // Create a thread for each publisher, sending messages at the periods in the
  // vector.
  std::vector<std::thread> pub_threads;
  for (int i = 0; i < kNumChannels; i++) {
    pub_threads.emplace_back([i, &pubs, &periods, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
        ASSERT_OK(buffer);
        uint64_t send_time = toolbelt::Now();
        memset(*buffer, 0xda, kSlotSize);
        memcpy(*buffer, &send_time, sizeof(send_time));
        absl::StatusOr<const Message> pub_status =
            pubs[i].PublishMessage(sizeof(send_time));
        // std::cerr << "pub status " << pub_status.status() << "\n";
        ASSERT_OK(pub_status);
        j++;
        // Transmit at 1 MHz.
        std::this_thread::sleep_for(std::chrono::microseconds(periods[i]));
      }
    });
  }
  // Wait for threads to exit.
  for (auto &t : pub_threads) {
    t.join();
  }
  // The subscriber might have dropped the last sequence of messages
  // and will therefore not stop.  We need to send enough messages to
  // stop the subscriber.
  for (int i = 0; i < 100; i++) {
    for (int j = 0; j < kNumChannels; j++) {
      absl::StatusOr<void *> buffer = pubs[j].GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pubs[j].PublishMessage(1);
      ASSERT_OK(pub_status);
    }
  }
  sub_thread.join();
  std::cerr << "Received " << num_messages << " messages\n";
  std::cerr << "Dropped " << (kNumMessages * kNumChannels) - num_messages
            << " messages\n";
}

TEST_F(StressTest, ManyChannelsMultiplexed) {
  std::vector<std::shared_ptr<subspace::Client>> pub_clients;
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  constexpr const char *kMux = "/logs/*";
  const int kNumChannels = StressValueForSplitBuffers(200, 40, 4);
  const int knum_slots = StressValueForSplitBuffers(800, 160, 16);
  constexpr int kSlotSize = 32768;
  const int kNumMessages = StressValueForSplitBuffers(200, 100, 50);
  // Memory used ~= knum_slots * kSlotSize
  std::vector<std::string> channels;

  for (int i = 0; i < kNumChannels; i++) {
    auto pub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
    pub_clients.push_back(std::move(pub_client));
    channels.push_back(absl::StrFormat("/logs/%d", i));
  }

  std::vector<Publisher> pubs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Publisher> pub = pub_clients[i]->CreatePublisher(
        channels[i],
        subspace::PublisherOptions().SetSlotSize(kSlotSize).SetNumSlots(knum_slots).SetMux(kMux));
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Subscriber> sub = sub_client->CreateSubscriber(
        channels[i], ([] { subspace::SubscriberOptions opts; opts.SetLogDroppedMessages(false); opts.SetMux(kMux); return opts; }()));
    // std::cerr << "sub status " << sub.status() << "\n";
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }

  std::atomic<int> num_messages = 0;

  // Create a thread to read from all subscribers.
  std::thread sub_thread([&subs, &num_messages, kNumChannels, kNumMessages] {
    std::vector<struct pollfd> fds;
    for (auto &sub : subs) {
      struct pollfd fd = sub.GetPollFd();
      fds.push_back(fd);
    }
    int num_dropped = 0;
    std::vector<uint64_t> last_ordinals(kNumChannels, 0);
    while (num_messages < kNumMessages * kNumChannels - num_dropped) {
      poll(fds.data(), fds.size(), -1);
      for (size_t i = 0; i < fds.size(); i++) {
        if (fds[i].revents & POLLIN) {
          for (;;) {
            absl::StatusOr<Message> msg = subs[i].ReadMessage();
            ASSERT_OK(msg);
            if (msg->length > 0) {
              num_dropped += msg->ordinal - last_ordinals[i] - 1;
              last_ordinals[i] = msg->ordinal;
              num_messages++;
            } else {
              break;
            }
          }
        }
      }
    }
  });

  srand(time(nullptr));
  std::vector<uint64_t> periods; // In microseconds
  for (int i = 0; i < kNumChannels; i++) {
    periods.push_back(5000 + rand() % 100000);
  }
  // Create a thread for each publisher, sending messages at the periods in the
  // vector.
  std::vector<std::thread> pub_threads;
  for (int i = 0; i < kNumChannels; i++) {
    pub_threads.emplace_back([i, &pubs, &periods, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
        ASSERT_OK(buffer);
        uint64_t send_time = toolbelt::Now();
        memset(*buffer, 0xda, kSlotSize);
        memcpy(*buffer, &send_time, sizeof(send_time));
        absl::StatusOr<const Message> pub_status =
            pubs[i].PublishMessage(sizeof(send_time));
        if (!pub_status.ok()) {
          std::cerr << "pub status " << pub_status.status() << "\n";
        }
        ASSERT_OK(pub_status);
        j++;
        std::this_thread::sleep_for(std::chrono::microseconds(periods[i]));
      }
    });
  }
  // Wait for threads to exit.
  for (auto &t : pub_threads) {
    t.join();
  }
  // The subscriber might have dropped the last sequence of messages
  // and will therefore not stop.  We need to send enough messages to
  // stop the subscriber.
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < kNumChannels; j++) {
      absl::StatusOr<void *> buffer = pubs[j].GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pubs[j].PublishMessage(1);
      ASSERT_OK(pub_status);
    }
  }
  sub_thread.join();
  std::cerr << "Received " << num_messages << " messages\n";
  std::cerr << "Dropped " << (kNumMessages * kNumChannels) - num_messages
            << " messages\n";
}

TEST_F(StressTest, ManyChannelsMultiplexedSubscribedToMux) {
  std::vector<std::shared_ptr<subspace::Client>> pub_clients;
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  constexpr const char *kMux = "/logs/*";
  const int kNumChannels = StressValueForSplitBuffers(200, 40, 4);
  const int knum_slots = StressValueForSplitBuffers(1000, 200, 16);
  constexpr int kSlotSize = 32768;
  const int kNumMessages = StressValueForSplitBuffers(200, 100, 50);
  // Memory used ~= knum_slots * kSlotSize
  std::vector<std::string> channels;

  for (int i = 0; i < kNumChannels; i++) {
    auto pub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
    pub_clients.push_back(std::move(pub_client));
    channels.push_back(absl::StrFormat("/logs/%d", i));
  }

  std::vector<Publisher> pubs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Publisher> pub =
        pub_clients[i]->CreatePublisher(channels[i], subspace::PublisherOptions().SetSlotSize(kSlotSize).SetNumSlots(knum_slots).SetActivate(true).SetMux(kMux));
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create subscriber to multiplexer.
  absl::StatusOr<Subscriber> sub =
      sub_client->CreateSubscriber(kMux, ([] { subspace::SubscriberOptions opts; opts.SetLogDroppedMessages(false); return opts; }()));
  // std::cerr << "sub status " << sub.status() << "\n";
  ASSERT_OK(sub);

  std::atomic<int> num_messages = 0;

  // Create a thread to read from the mux subscriber.
  std::thread sub_thread([&sub, &num_messages, kNumChannels, kNumMessages] {
    struct pollfd pfd = sub->GetPollFd();

    int num_dropped = 0;
    std::vector<uint64_t> last_ordinals(kNumChannels, 0);
    while (num_messages < kNumMessages * kNumChannels - num_dropped) {
      poll(&pfd, 1, -1);
      if (pfd.revents & POLLIN) {
        for (;;) {
          absl::StatusOr<Message> msg = sub->ReadMessage();
          ASSERT_OK(msg);
          if (msg->length > 0) {
            num_dropped += msg->ordinal - last_ordinals[msg->vchan_id] - 1;
            last_ordinals[msg->vchan_id] = msg->ordinal;
            num_messages++;
          } else {
            break;
          }
        }
      }
    }
  });

  srand(1234);
  std::vector<uint64_t> periods; // In microseconds
  for (int i = 0; i < kNumChannels; i++) {
    periods.push_back(5000 + rand() % 100000);
  }
  // Create a thread for each publisher, sending messages at the periods in the
  // vector.
  std::vector<std::thread> pub_threads;
  for (int i = 0; i < kNumChannels; i++) {
    pub_threads.emplace_back([i, &pubs, &periods, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
        ASSERT_OK(buffer);
        uint64_t send_time = toolbelt::Now();
        memset(*buffer, 0xda, kSlotSize);
        memcpy(*buffer, &send_time, sizeof(send_time));
        absl::StatusOr<const Message> pub_status =
            pubs[i].PublishMessage(sizeof(send_time));
        if (!pub_status.ok()) {
          std::cerr << "pub status " << pub_status.status() << "\n";
        }
        ASSERT_OK(pub_status);
        j++;
        std::this_thread::sleep_for(std::chrono::microseconds(periods[i]));
      }
    });
  }
  // Wait for threads to exit.
  for (auto &t : pub_threads) {
    t.join();
  }
  // The subscriber might have dropped the last sequence of messages
  // and will therefore not stop.  We need to send enough messages to
  // stop the subscriber.
  for (int i = 0; i < 100; i++) {
    for (int j = 0; j < kNumChannels; j++) {
      absl::StatusOr<void *> buffer = pubs[j].GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pubs[j].PublishMessage(1);
      ASSERT_OK(pub_status);
    }
  }
  sub_thread.join();
  std::cerr << "Received " << num_messages << " messages\n";
  std::cerr << "Dropped " << (kNumMessages * kNumChannels) - num_messages
            << " messages\n";
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  subspace::SetDefaultUseSplitBuffers(absl::GetFlag(FLAGS_use_split_buffers));

  return RUN_ALL_TESTS();
}
