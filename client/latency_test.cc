// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/test_fixture.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <cstdlib>
#include <fstream>
#include <inttypes.h>
#include <limits>
#include <string>
#include <sys/resource.h>

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");
ABSL_FLAG(bool, use_split_buffers, false,
          "Run publishers with split-buffer payload storage");

using InetAddress = toolbelt::InetAddress;

class LatencyTest : public SubspaceTestBase {};

const char *LatencyEnvOrDefault(const char *name, const char *default_value) {
  const char *value = std::getenv(name);
  return value == nullptr || value[0] == '\0' ? default_value : value;
}

void EmitLatencyMetric(const std::string &test, const std::string &series,
                       const std::string &x_name, uint64_t x,
                       const std::string &metric, uint64_t value_ns) {
  std::cerr << "LATENCY_JSON {"
            << "\"test\":\"" << test << "\","
            << "\"series\":\"" << series << "\","
            << "\"x_name\":\"" << x_name << "\","
            << "\"x\":" << x << ","
            << "\"metric\":\"" << metric << "\","
            << "\"value_ns\":" << value_ns << ","
            << "\"os\":\"" << LatencyEnvOrDefault("SUBSPACE_LATENCY_OS", "local")
            << "\","
            << "\"mode\":\""
            << LatencyEnvOrDefault("SUBSPACE_LATENCY_MODE", "local") << "\","
            << "\"revision\":\""
            << LatencyEnvOrDefault("SUBSPACE_LATENCY_REVISION", "current")
            << "\""
            << "}\n";
}

int LatencyValueForSplitBuffers(int normal_value, int split_buffer_value,
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

// Stress test with multiple threads.
TEST_F(LatencyTest, MultithreadedSingleChannel) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr int kNumReceivers = 20;
  const int kNumMessages = LatencyValueForSplitBuffers(2000, 500);

  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("stress", 256, kNumReceivers + 3);
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "stress", subspace::SubscriberOptions().SetMaxActiveMessages(kNumReceivers + 1));
  ASSERT_OK(sub);

  std::vector<std::thread> receivers;
  std::vector<toolbelt::SharedPtrPipe<Message>> pipes;

  std::atomic<int> total_received_messages{0};
  std::atomic<int> num_dropped{0};

  for (size_t i = 0; i < kNumReceivers; i++) {
    pipes.emplace_back(toolbelt::SharedPtrPipe<Message>());
    ASSERT_OK(pipes.back().Open());
  }

  for (size_t i = 0; i < kNumReceivers; i++) {
    receivers.emplace_back(
        [&pipes, i, &total_received_messages, &num_dropped, kNumMessages]() {
          while (total_received_messages + num_dropped < kNumMessages) {
            auto msg = pipes[i].Read();
            ASSERT_OK(msg);
            // std::cerr << "received ordinal " << (*msg)->ordinal << " on "
            //           << i << "\n";
            total_received_messages++;
            // Sleep for random microseconds.
            std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
          }
        });
  }

  // Create a subscriber thread to read from the channel and write to random
  // pipe.
  std::thread sub_thread([&sub, &pipes, &num_dropped, kNumMessages]() {
    uint64_t last_ordinal = 0;

    for (int j = 0; j < kNumMessages; j++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length > 0) {
        ASSERT_GT(msg->ordinal, last_ordinal);
        num_dropped += msg->ordinal - last_ordinal - 1;
        last_ordinal = msg->ordinal;
        int receiver = rand() % kNumReceivers;
        ASSERT_TRUE(pipes[receiver]
                        .Write(std::make_shared<Message>(std::move(*msg)))
                        .ok());
      }
      // Sleep for random microseconds.
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
    }
  });

  // Create a publisher thread.
  std::thread pub_thread([&pub, kNumMessages]() {
    for (int j = 0; j < kNumMessages; j++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      char *buf = reinterpret_cast<char *>(*buffer);
      int len = snprintf(buf, 256, "foobar %d", j);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
      // Sleep for random microseconds.
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
    }
  });

  pub_thread.join();
  sub_thread.join();
  // Send one last message to the receivers to stop them.
  for (size_t i = 0; i < kNumReceivers; i++) {
    ASSERT_OK(pipes[i].Write(std::make_shared<Message>()));
  }
  for (auto &r : receivers) {
    r.join();
  }
}

TEST_F(LatencyTest, MultithreadedSingleChannelReliable) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr int kNumReceivers = 20;
  const int kNumMessages = LatencyValueForSplitBuffers(200000, 20000, 10000);

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "rstress", 256, kNumReceivers + 3, subspace::PublisherOptions().SetReliable(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "rstress", subspace::SubscriberOptions().SetReliable(true).SetMaxActiveMessages(kNumReceivers + 1));
  ASSERT_OK(sub);

  std::vector<std::thread> receivers;
  std::vector<toolbelt::SharedPtrPipe<Message>> pipes;

  std::atomic<int> total_received_messages{0};

  for (size_t i = 0; i < kNumReceivers; i++) {
    pipes.emplace_back(toolbelt::SharedPtrPipe<Message>());
    ASSERT_OK(pipes.back().Open());
  }

  for (size_t i = 0; i < kNumReceivers; i++) {
    receivers.emplace_back([&pipes, i, &total_received_messages, kNumMessages]() {
      while (total_received_messages < kNumMessages) {
        auto msg = pipes[i].Read();
        ASSERT_OK(msg);
        // std::cerr << absl::StrFormat("Received message %d %d\n",
        // (*msg)->slot_id, (*msg)->ordinal);
        total_received_messages++;
        // Sleep for random microseconds.
        std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
      }
    });
  }

  uint64_t start_time = toolbelt::Now();
  // Create a subscriber thread to read from the channel and write to random
  // pipe.
  std::thread sub_thread([&sub, &pipes, kNumMessages]() {
    uint64_t last_ordinal = 0;

    int j = 0;
    ASSERT_OK(sub->Wait());
    while (j < kNumMessages) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length > 0) {
        // std::cerr << absl::StrFormat("Received message %d %d\n", msg->slot_id, msg->ordinal);
        if (last_ordinal != 0) {
          if (msg->ordinal != last_ordinal + 1) {
            std::cerr << absl::StrFormat(
                "Ordinal ordering issue received %d, expected %d\n",
                msg->ordinal, last_ordinal + 1);
            sub->DumpSlots(std::cerr);
            abort();
          }
        }
        last_ordinal = msg->ordinal;
        int receiver = rand() % kNumReceivers;
        ASSERT_TRUE(pipes[receiver]
                        .Write(std::make_shared<Message>(std::move(*msg)))
                        .ok());
        j++;
      }
    }
  });

  // Create a publisher thread.
  std::thread pub_thread([&pub, kNumMessages]() {
    int j = 0;
    while (j < kNumMessages) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      char *buf = reinterpret_cast<char *>(*buffer);
      int len = snprintf(buf, 256, "foobar %d", j);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
      j++;
    }
  });

  pub_thread.join();
  sub_thread.join();
  uint64_t end_time = toolbelt::Now();
  std::cerr << "Average latency: " << (end_time - start_time) / kNumMessages
            << " ns\n";
  // Send one last message to the receivers to stop them.
  for (size_t i = 0; i < kNumReceivers; i++) {
    ASSERT_OK(pipes[i].Write(std::make_shared<Message>()));
  }
  for (auto &r : receivers) {
    r.join();
  }
}

TEST_F(LatencyTest, MultithreadedReliableLatency) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(200000, 20000, 10000);

  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("lstress", 256, 10, subspace::PublisherOptions().SetReliable(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("lstress", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_OK(sub);

  uint64_t start_time = toolbelt::Now();
  // Create a subscriber thread to read from the channel and write to random
  // pipe.
  std::thread sub_thread([&sub, kNumMessages]() {
    int j = 0;
    ASSERT_OK(sub->Wait());
    while (j < kNumMessages) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length > 0) {
        j++;
      } else {
        ASSERT_OK(sub->Wait());
      }
    }
  });

  // Create a publisher thread.
  std::thread pub_thread([&pub, kNumMessages]() {
    int j = 0;
    while (j < kNumMessages) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      char *buf = reinterpret_cast<char *>(*buffer);
      int len = snprintf(buf, 256, "foobar %d", j);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
      j++;
    }
  });

  pub_thread.join();
  sub_thread.join();
  uint64_t end_time = toolbelt::Now();
  std::cerr << "Average latency: " << (end_time - start_time) / kNumMessages
            << " ns\n";
}

TEST_F(LatencyTest, MultithreadedReliableLatencyHistogram) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  std::vector<uint64_t> latencies;

  for (int num_slots = 3;
       num_slots < LatencyValueForSplitBuffers(20000, 4096);
       num_slots *= 2) {
    std::cerr << "num_slots: " << num_slots << "\n";
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "lstress", 256, num_slots, subspace::PublisherOptions().SetReliable(true));
    ASSERT_OK(pub);

    absl::StatusOr<Subscriber> sub =
        sub_client.CreateSubscriber("lstress", subspace::SubscriberOptions().SetReliable(true));
    ASSERT_OK(sub);

    uint64_t start_time = toolbelt::Now();
    // Create a subscriber thread to read from the channel and write to random
    // pipe.
    std::thread sub_thread([&sub, kNumMessages]() {
      int j = 0;
      ASSERT_OK(sub->Wait());
      while (j < kNumMessages) {
        absl::StatusOr<Message> msg = sub->ReadMessage();
        ASSERT_OK(msg);
        if (msg->length > 0) {
          j++;
        } else {
          ASSERT_OK(sub->Wait());
        }
      }
    });

    // Create a publisher thread.
    std::thread pub_thread([&pub, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);
        if (*buffer == nullptr) {
          // Can't send, wait until we can try again.
          ASSERT_OK(pub->Wait());
          continue;
        }
        absl::StatusOr<const Message> pub_status = pub->PublishMessage(1);
        ASSERT_OK(pub_status);
        j++;
      }
    });

    pub_thread.join();
    sub_thread.join();
    uint64_t end_time = toolbelt::Now();
    latencies.push_back((end_time - start_time) / kNumMessages);
  }

  int slot_size = 3;
  uint64_t prev_latency = 0;
  for (auto &latency : latencies) {
    std::cerr << slot_size << ": " << latency << " ns scaling factor: "
              << (prev_latency == 0 ? 0 : (double(latency) / prev_latency))
              << "\n";
    prev_latency = latency;
    slot_size *= 2;
  }
}

TEST_F(LatencyTest, MultithreadedUnreliableLatency) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(2000000, 200000, 50000);

  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("lustress", 256, 10, subspace::PublisherOptions().SetReliable(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "lustress", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
  ASSERT_OK(sub);

  uint64_t start_time = toolbelt::Now();
  // Create a subscriber thread to read from the channel and write to random
  // pipe.
  std::atomic<int> num_dropped{0};
  std::thread sub_thread([&sub, &num_dropped, kNumMessages]() {
    uint64_t last_ordinal = 0;
    int j = 0;
    ASSERT_OK(sub->Wait());
    while (j < kNumMessages - num_dropped) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length > 0) {
        num_dropped += msg->ordinal - last_ordinal - 1;

        last_ordinal = msg->ordinal;
        j++;
      } else {
        // ASSERT_OK(sub->Wait());
      }
    }
    std::cerr << "Received " << j << " messages, dropped " << num_dropped.load()
              << "\n";
  });

  // Create a publisher thread.
  std::thread pub_thread([&pub, kNumMessages]() {
    int j = 0;
    while (j < kNumMessages) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(1);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
      j++;
    }
  });

  pub_thread.join();
  // The subscriber might have dropped the last sequence of messages
  // and will therefore not stop.  We need to send enough messages to
  // stop the subscriber.
  for (int i = 0; i < 100; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    if (*buffer == nullptr) {
      // Can't send, wait until we can try again.
      ASSERT_OK(pub->Wait());
      continue;
    }

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(1);
    // std::cerr << "pub status " << pub_status.status() << "\n";
    ASSERT_OK(pub_status);
  }
  sub_thread.join();
  uint64_t end_time = toolbelt::Now();
  std::cerr << "Average latency: " << (end_time - start_time) / kNumMessages
            << " ns\n";
}

TEST_F(LatencyTest, PublisherLatency) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_slots,latency_with_retirement,latency_no_retirement\n";
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(100000, 20000, 3000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    uint64_t total_time = 0;

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);
    }
    std::cerr << total_time / kNumMessages << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    total_time = 0;
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;
    }
    std::cerr << total_time / kNumMessages << "\n";
  }
}

TEST_F(LatencyTest, PublisherLatencyChecksum) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_slots,latency_with_retirement,latency_no_retirement\n";
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(100000, 20000, 3000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false).SetChecksum(true));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat",
        ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); opts.SetChecksum(true); return opts; }()));
    ASSERT_OK(sub);

    uint64_t total_time = 0;

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);
    }
    std::cerr << total_time / kNumMessages << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    total_time = 0;
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;
    }
    std::cerr << total_time / kNumMessages << "\n";
  }
}

TEST_F(LatencyTest, PublisherLatencyPayload) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(2000, 500);
  constexpr int kMaxPayloadSize = 32 * 1024;

  auto random_payload = [](void *buffer) -> int {
    int size = (rand() % (kMaxPayloadSize - 1)) + 1;
    for (int i = 0; i < size; i++) {
      reinterpret_cast<char *>(buffer)[i] = rand() % 256;
    }
    return size;
  };
  std::cerr << "num_slots,latency_with_retirement,latency_no_retirement\n";
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(10000, 2000, 1000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "publat", kMaxPayloadSize, num_slots, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    uint64_t total_time = 0;

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      int payload_size = random_payload(*buffer);

      absl::StatusOr<const Message> pub_status =
          pub->PublishMessage(payload_size);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(payload_size, msg->length);
    }
    std::cerr << total_time / kNumMessages << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      int payload_size = random_payload(*buffer);
      absl::StatusOr<const Message> pub_status =
          pub->PublishMessage(payload_size);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    total_time = 0;
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      int payload_size = random_payload(*buffer);
      absl::StatusOr<const Message> pub_status =
          pub->PublishMessage(payload_size);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;
    }
    std::cerr << total_time / kNumMessages << "\n";
  }
}

TEST_F(LatencyTest, PublisherLatencyPayloadChecksum) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(2000, 500);
  constexpr int kMaxPayloadSize = 32 * 1024;

  auto random_payload = [](void *buffer) -> int {
    int size = (rand() % (kMaxPayloadSize - 1)) + 1;
    for (int i = 0; i < size; i++) {
      reinterpret_cast<char *>(buffer)[i] = rand() % 256;
    }
    return size;
  };
  std::cerr << "num_slots,latency_with_retirement,latency_no_retirement\n";
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(10000, 2000, 1000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub =
        pub_client.CreatePublisher("publat", kMaxPayloadSize, num_slots,
                                   subspace::PublisherOptions().SetReliable(false).SetChecksum(true));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat",
        ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); opts.SetChecksum(true); return opts; }()));
    ASSERT_OK(sub);

    uint64_t total_time = 0;

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      int payload_size = random_payload(*buffer);

      absl::StatusOr<const Message> pub_status =
          pub->PublishMessage(payload_size);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(payload_size, msg->length);
    }
    std::cerr << total_time / kNumMessages << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      int payload_size = random_payload(*buffer);
      absl::StatusOr<const Message> pub_status =
          pub->PublishMessage(payload_size);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    total_time = 0;
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      int payload_size = random_payload(*buffer);
      absl::StatusOr<const Message> pub_status =
          pub->PublishMessage(payload_size);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;
    }
    std::cerr << total_time / kNumMessages << "\n";
  }
}

TEST_F(LatencyTest, PublisherLatencyHistogram) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_slots,min,median,p99,max,average\n";
  auto show_latencies = [](std::vector<uint64_t> &latencies,
                           const std::string &test, const std::string &series,
                           const std::string &x_name, uint64_t x) {
    // Sort latencies.
    std::sort(latencies.begin(), latencies.end());
    // Min latency.
    uint64_t min = latencies.front();
    std::cerr << min << ",";
    // Median.
    uint64_t median = latencies[latencies.size() / 2];
    std::cerr << median << ",";
    // P99 latency.
    uint64_t p99 = latencies[latencies.size() * 99 / 100];
    std::cerr << p99 << ",";
    // Max latency.
    uint64_t max = latencies.back();
    std::cerr << max << ",";
    // Average latency.
    uint64_t sum = 0;
    for (auto &l : latencies) {
      sum += l;
    }
    uint64_t average = sum / latencies.size();
    std::cerr << average << "\n";
    EmitLatencyMetric(test, series, x_name, x, "min", min);
    EmitLatencyMetric(test, series, x_name, x, "median", median);
    EmitLatencyMetric(test, series, x_name, x, "p99", p99);
    EmitLatencyMetric(test, series, x_name, x, "max", max);
    EmitLatencyMetric(test, series, x_name, x, "average", average);
  };
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(100000, 20000, 3000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    std::vector<uint64_t> latencies;
    latencies.reserve(kNumMessages);

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      latencies.push_back(end - start_time);

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);
    }

    show_latencies(latencies, "PublisherLatencyHistogram", "with_retirement",
                   "num_slots", num_slots);
    latencies.clear();
    std::cerr << num_slots << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      latencies.push_back(end - start_time);
    }
    show_latencies(latencies, "PublisherLatencyHistogram", "no_retirement",
                   "num_slots", num_slots);
  }
}

TEST_F(LatencyTest, PublisherLatencyHistogramThreadSafe) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  pub_client.SetThreadSafe(true);
  sub_client.SetThreadSafe(true);
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_slots,min,median,p99,max,average\n";
  auto show_latencies = [](std::vector<uint64_t> &latencies) {
    // Sort latencies.
    std::sort(latencies.begin(), latencies.end());
    // Min latency.
    std::cerr << latencies.front() << ",";
    // Median.
    std::cerr << latencies[latencies.size() / 2] << ",";
    // P99 latency.
    std::cerr << latencies[latencies.size() * 99 / 100] << ",";
    // Max latency.
    std::cerr << latencies.back() << ",";
    // Average latency.
    uint64_t sum = 0;
    for (auto &l : latencies) {
      sum += l;
    }
    std::cerr << sum / latencies.size() << "\n";
  };
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(100000, 20000, 3000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    std::vector<uint64_t> latencies;
    latencies.reserve(kNumMessages);

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      latencies.push_back(end - start_time);

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);
    }

    show_latencies(latencies);
    latencies.clear();
    std::cerr << num_slots << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      latencies.push_back(end - start_time);
    }
    show_latencies(latencies);
  }
}

TEST_F(LatencyTest, PublisherLatencyMultiSub) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr
      << "num_slots,num_subs,latency_with_retirement,latency_no_retirement\n";
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(10000, 2000, 250);
       num_slots *= 5) {

    for (int num_subs = 1; num_subs < sqrt(num_slots); num_subs *= 2) {
      absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
          "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false));
      ASSERT_OK(pub);

      std::cerr << num_slots << "," << num_subs << ",";
      std::vector<Subscriber> subs;

      for (int i = 0; i < num_subs; i++) {
        absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
            "publat", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
        ASSERT_OK(sub);
        subs.push_back(std::move(*sub));
      }

      uint64_t total_time = 0;

      // Send messages ensuring there is always a retired message.  Measure the
      // total time to send (but not to receive).
      for (int i = 0; i < kNumMessages; i++) {
        // Publish a message.
        uint64_t start_time = toolbelt::Now();
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);
        absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
        ASSERT_OK(pub_status);
        uint64_t end = toolbelt::Now();
        total_time += end - start_time;

        for (int i = 0; i < num_subs; i++) {
          absl::StatusOr<Message> msg = subs[i].ReadMessage();
          ASSERT_OK(msg);
          ASSERT_EQ(100, msg->length);
        }
      }
      std::cerr << total_time / kNumMessages << ",";

      // Now fill the channel.
      for (int i = 0; i < num_slots; i++) {
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);
        if (*buffer == nullptr) {
          // Can't send, wait until we can try again.
          ASSERT_OK(pub->Wait());
          continue;
        }
        absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
        // std::cerr << "pub status " << pub_status.status() << "\n";
        ASSERT_OK(pub_status);
      }

      // Send the same number of messages but with the channel full so that it
      // has to take messages that subscribers have not yet seen.
      total_time = 0;
      for (int i = 0; i < kNumMessages; i++) {
        // Publish a message.
        uint64_t start_time = toolbelt::Now();
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);

        absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
        ASSERT_OK(pub_status);
        uint64_t end = toolbelt::Now();
        total_time += end - start_time;
      }
      std::cerr << total_time / kNumMessages << "\n";
    }
  }
}

TEST_F(LatencyTest, VirtualPublisherLatency) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_slots,latency_with_retirement,latency_no_retirement\n";
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(100000, 20000, 3000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false).SetMux("/foo"));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat",
        ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); opts.SetMux("/foo"); return opts; }()));
    ASSERT_OK(sub);

    uint64_t total_time = 0;

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);
    }
    std::cerr << total_time / kNumMessages << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    total_time = 0;
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;
    }
    std::cerr << total_time / kNumMessages << "\n";
  }
}

TEST_F(LatencyTest, VirtualPublisherLatencyMultiSub) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr
      << "num_slots,num_subs,latency_with_retirement,latency_no_retirement\n";
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(10000, 2000, 250);
       num_slots *= 5) {

    for (int num_subs = 1; num_subs < sqrt(num_slots); num_subs *= 2) {
      absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
          "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false).SetMux("/foo"));
      ASSERT_OK(pub);

      std::cerr << num_slots << "," << num_subs << ",";
      std::vector<Subscriber> subs;

      for (int i = 0; i < num_subs; i++) {
        absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
            "publat",
            ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); opts.SetMux("/foo"); return opts; }()));
        ASSERT_OK(sub);
        subs.push_back(std::move(*sub));
      }

      uint64_t total_time = 0;

      // Send messages ensuring there is always a retired message.  Measure the
      // total time to send (but not to receive).
      for (int i = 0; i < kNumMessages; i++) {
        // Publish a message.
        uint64_t start_time = toolbelt::Now();
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);
        absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
        ASSERT_OK(pub_status);
        uint64_t end = toolbelt::Now();
        total_time += end - start_time;

        for (int i = 0; i < num_subs; i++) {
          absl::StatusOr<Message> msg = subs[i].ReadMessage();
          ASSERT_OK(msg);
          ASSERT_EQ(100, msg->length);
        }
      }
      std::cerr << total_time / kNumMessages << ",";

      // Now fill the channel.
      for (int i = 0; i < num_slots; i++) {
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);
        if (*buffer == nullptr) {
          // Can't send, wait until we can try again.
          ASSERT_OK(pub->Wait());
          continue;
        }
        absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
        // std::cerr << "pub status " << pub_status.status() << "\n";
        ASSERT_OK(pub_status);
      }

      // Send the same number of messages but with the channel full so that it
      // has to take messages that subscribers have not yet seen.
      total_time = 0;
      for (int i = 0; i < kNumMessages; i++) {
        // Publish a message.
        uint64_t start_time = toolbelt::Now();
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);

        absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
        ASSERT_OK(pub_status);
        uint64_t end = toolbelt::Now();
        total_time += end - start_time;
      }
      std::cerr << total_time / kNumMessages << "\n";
    }
  }
}

TEST_F(LatencyTest, VirtualPublisherMuxLatency) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_slots,latency_with_retirement,latency_no_retirement\n";
  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);
  for (int num_slots = 10;
       num_slots < LatencyValueForSplitBuffers(100000, 20000, 3000);
       num_slots = (num_slots)*15 / 10) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "publat", 256, num_slots, subspace::PublisherOptions().SetReliable(false).SetMux("/foo"));
    ASSERT_OK(pub);

    std::cerr << num_slots << ",";
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "publat",
        ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); opts.SetMux("/foo"); return opts; }()));
    ASSERT_OK(sub);

    // Mux subscriber.
    absl::StatusOr<Subscriber> mux_sub = sub_client.CreateSubscriber(
        "/foo", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(mux_sub);

    uint64_t total_time = 0;

    // Send messages ensuring there is always a retired message.  Measure the
    // total time to send (but not to receive).
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;

      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);

      absl::StatusOr<Message> mux_msg = mux_sub->ReadMessage();
      ASSERT_OK(mux_msg);
      ASSERT_EQ(100, mux_msg->length);
    }
    std::cerr << total_time / kNumMessages << ",";

    // Now fill the channel.
    for (int i = 0; i < num_slots; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }

    // Send the same number of messages but with the channel full so that it has
    // to take messages that subscribers have not yet seen.
    total_time = 0;
    for (int i = 0; i < kNumMessages; i++) {
      // Publish a message.
      uint64_t start_time = toolbelt::Now();
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      uint64_t end = toolbelt::Now();
      total_time += end - start_time;
    }
    std::cerr << total_time / kNumMessages << "\n";
  }
}

// This measures unreliable latency by sending as fast as possible.  It will
// drop messages because the publisher will run faster than the subscriber
// most of the time.
TEST_F(LatencyTest, MultithreadedUnreliableLatencyHistogram) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);

  std::vector<uint64_t> latencies;
  for (int num_slots = 3;
       num_slots < LatencyValueForSplitBuffers(20000, 4096);
       num_slots *= 2) {
    std::cerr << "num_slots: " << num_slots << "\n";
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "lustress", 256, num_slots, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);

    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "lustress", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    uint64_t start_time = toolbelt::Now();
    // Create a subscriber thread to read from the channel and write to random
    // pipe.
    std::atomic<int> num_dropped{0};
    std::thread sub_thread([&sub, &num_dropped, kNumMessages]() {
      uint64_t last_ordinal = 0;
      int j = 0;
      while (j < kNumMessages - num_dropped) {
        absl::StatusOr<Message> msg = sub->ReadMessage();
        ASSERT_OK(msg);
        if (msg->length > 0) {
          num_dropped += msg->ordinal - last_ordinal - 1;
          last_ordinal = msg->ordinal;
          j++;
        }
      }
      std::cerr << "Received " << j << " messages, dropped "
                << num_dropped.load() << "\n";
    });

    // Create a publisher thread.
    std::thread pub_thread([&pub, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);
        if (*buffer == nullptr) {
          // Can't send, wait until we can try again.
          ASSERT_OK(pub->Wait());
          continue;
        }

        absl::StatusOr<const Message> pub_status = pub->PublishMessage(1);
        // std::cerr << "pub status " << pub_status.status() << "\n";
        ASSERT_OK(pub_status);
        j++;
      }
    });

    pub_thread.join();

    // The subscriber might have dropped the last sequence of messages
    // and will therefore not stop.  We need to send enough messages to
    // stop the subscriber.
    for (int i = 0; i < 100; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(1);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }
    sub_thread.join();
    uint64_t end_time = toolbelt::Now();
    latencies.push_back((end_time - start_time) / kNumMessages);
  }

  int slot_size = 3;
  uint64_t prev_latency = 0;
  for (auto &latency : latencies) {
    std::cerr << slot_size << ": " << latency << " ns scaling factor: "
              << (prev_latency == 0 ? 0 : (double(latency) / prev_latency))
              << "\n";
    prev_latency = latency;
    slot_size *= 2;
  }
}

TEST_F(LatencyTest, MultithreadedUnreliableLatencyPayload) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(200000, 20000, 10000);

  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("lustress", 256, 100, subspace::PublisherOptions().SetReliable(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "lustress", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
  ASSERT_OK(sub);

  // Create a subscriber thread to read from the channel and write to random
  // pipe.
  std::atomic<int> num_dropped{0};

  std::thread sub_thread([&sub, &num_dropped, kNumMessages]() {
    uint64_t last_ordinal = 0;
    std::vector<uint64_t> latencies;
    latencies.reserve(kNumMessages);
    int j = 0;
    ASSERT_OK(sub->Wait());
    while (j < kNumMessages - num_dropped) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length > 0) {
        uint64_t receive_time = toolbelt::Now();
        num_dropped += msg->ordinal - last_ordinal - 1;
        last_ordinal = msg->ordinal;
        j++;
        const uint64_t send_time =
            *reinterpret_cast<const uint64_t *>(msg->buffer);
        uint64_t latency = receive_time - send_time;
        latencies.push_back(latency);
      } else {
        // ASSERT_OK(sub->Wait());
      }
    }
    std::cerr << "Received " << j << " messages, dropped " << num_dropped.load()
              << "\n";
    if (latencies.empty()) {
      std::cerr << "No messages received\n";
      return;
    }
    // Sort latencies.
    std::sort(latencies.begin(), latencies.end());
    // Min latency.
    std::cerr << "Min latency: " << latencies.front() << " ns\n";
    std::cerr << "Median latency: " << latencies[latencies.size() / 2]
              << " ns\n";
    // P99 latency.
    std::cerr << "P99 latency: " << latencies[latencies.size() * 99 / 100]
              << " ns\n";
    // Max latency.
    std::cerr << "Max latency: " << latencies.back() << " ns\n";
    // Average latency.
    uint64_t sum = 0;
    for (auto &l : latencies) {
      sum += l;
    }
    std::cerr << "Average latency: " << sum / latencies.size() << " ns\n";
  });

  // Create a publisher thread.
  std::thread pub_thread([&pub, kNumMessages]() {
    int j = 0;
    while (j < kNumMessages) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }
      uint64_t send_time = toolbelt::Now();
      memcpy(*buffer, &send_time, sizeof(send_time));
      absl::StatusOr<const Message> pub_status =
          pub->PublishMessage(sizeof(send_time));
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
      j++;
      // Sleep for random microseconds.
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10));
    }
  });

  pub_thread.join();
  // The subscriber might have dropped the last sequence of messages
  // and will therefore not stop.  We need to send enough messages to
  // stop the subscriber.
  for (int i = 0; i < 100; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    if (*buffer == nullptr) {
      // Can't send, wait until we can try again.
      ASSERT_OK(pub->Wait());
      continue;
    }

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(1);
    // std::cerr << "pub status " << pub_status.status() << "\n";
    ASSERT_OK(pub_status);
  }
  sub_thread.join();
}

TEST_F(LatencyTest, MultithreadedUnreliableLatencyPayloadHistogram) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumMessages = LatencyValueForSplitBuffers(20000, 5000);

  struct Stats {
    int num_slots;
    int received;
    int dropped;
    uint64_t min;
    uint64_t max;
    uint64_t p50;
    uint64_t p99;
    uint64_t avg;
  };
  std::vector<Stats> stats;
  for (int num_slots = 3;
       num_slots < LatencyValueForSplitBuffers(20000, 4096);
       num_slots *= 2) {
    std::cerr << "Testing with num_slots: " << num_slots << "\n";
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "lustress", 256, num_slots, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);

    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "lustress", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    // Create a subscriber thread to read from the channel and write to random
    // pipe.
    std::atomic<int> num_dropped{0};

    std::thread sub_thread(
        [&sub, &num_dropped, &stats, num_slots, kNumMessages]() {
      uint64_t last_ordinal = 0;
      std::vector<uint64_t> latencies;
      int j = 0;
      ASSERT_OK(sub->Wait());
      while (j < kNumMessages - num_dropped) {
        absl::StatusOr<Message> msg = sub->ReadMessage();
        ASSERT_OK(msg);
        if (msg->length > 0) {
          uint64_t receive_time = toolbelt::Now();
          num_dropped += msg->ordinal - last_ordinal - 1;
          last_ordinal = msg->ordinal;
          j++;
          const uint64_t send_time =
              *reinterpret_cast<const uint64_t *>(msg->buffer);
          uint64_t latency = receive_time - send_time;
          latencies.push_back(latency);
        } else {
          // ASSERT_OK(sub->Wait());
        }
      };
      if (latencies.empty()) {
        std::cerr << "No messages received\n";
        return;
      }
      // Sort latencies.
      std::sort(latencies.begin(), latencies.end());
      // Add stats.
      Stats s;
      s.num_slots = num_slots;
      s.min = latencies.front();
      s.max = latencies.back();
      s.p50 = latencies[latencies.size() / 2];
      s.p99 = latencies[latencies.size() * 99 / 100];
      s.received = j;
      s.dropped = num_dropped.load();
      uint64_t sum = 0;
      for (auto &l : latencies) {
        sum += l;
      }
      s.avg = sum / latencies.size();
      stats.push_back(s);
    });

    // Create a publisher thread.
    std::thread pub_thread([&pub, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        ASSERT_OK(buffer);
        if (*buffer == nullptr) {
          // Can't send, wait until we can try again.
          ASSERT_OK(pub->Wait());
          continue;
        }
        uint64_t send_time = toolbelt::Now();
        memcpy(*buffer, &send_time, sizeof(send_time));
        absl::StatusOr<const Message> pub_status =
            pub->PublishMessage(sizeof(send_time));
        // std::cerr << "pub status " << pub_status.status() << "\n";
        ASSERT_OK(pub_status);
        j++;
        // Transmit at 10 kHz.
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });

    pub_thread.join();
    // The subscriber might have dropped the last sequence of messages
    // and will therefore not stop.  We need to send enough messages to
    // stop the subscriber.
    for (int i = 0; i < 100; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        // Can't send, wait until we can try again.
        ASSERT_OK(pub->Wait());
        continue;
      }

      absl::StatusOr<const Message> pub_status = pub->PublishMessage(1);
      // std::cerr << "pub status " << pub_status.status() << "\n";
      ASSERT_OK(pub_status);
    }
    sub_thread.join();
  }
  // Print stats
  for (auto &s : stats) {
    std::cerr << "slots: " << s.num_slots;
    std::cerr << ", received: " << s.received;
    std::cerr << ", dropped: " << s.dropped;
    std::cerr << ", min: " << s.min << " ns";
    std::cerr << ", median: " << s.p50 << " ns";
    std::cerr << ", p99: " << s.p99 << " ns";
    std::cerr << ", max: " << s.max << " ns";
    std::cerr << ", average: " << s.avg << " ns\n";
  }
}

TEST_F(LatencyTest, ManyChannelsNonMultiplexed) {
  std::vector<subspace::Client> pub_clients;
  subspace::Client sub_client;
  ASSERT_OK(sub_client.Init(Socket()));

  const int kNumChannels = LatencyValueForSplitBuffers(200, 100, 8);
  const int kNumSlots = LatencyValueForSplitBuffers(100, 80, 16);
  constexpr int kSlotSize = 32768;
  const int kNumMessages = LatencyValueForSplitBuffers(200, 100, 50);
  // Memory used ~= kNumChannels * kNumSlots * kSlotSize
  std::vector<std::string> channels;

  for (int i = 0; i < kNumChannels; i++) {
    subspace::Client pub_client;
    ASSERT_OK(pub_client.Init(Socket()));
    pub_clients.push_back(std::move(pub_client));
    channels.push_back(absl::StrFormat("/logs/%d", i));
  }

  std::vector<Publisher> pubs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Publisher> pub =
        pub_clients[i].CreatePublisher(channels[i], kSlotSize, kNumSlots);
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        channels[i], ([] { subspace::SubscriberOptions opts; opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    // std::cerr << "sub status " << sub.status() << "\n";
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }

  std::cerr << "Total virtual memory: "
            << static_cast<double>(Server()->GetVirtualMemoryUsage()) /
                   (1024 * 1024)
            << " MB" << std::endl;
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
    periods.push_back(5000 + rand() % 1000);
  }
  uint64_t start = toolbelt::Now();

  // Create a thread for each publisher, sending messages at the periods in the
  // vector.
  std::vector<std::thread> pub_threads;
  for (int i = 0; i < kNumChannels; i++) {
    pub_threads.emplace_back([i, &pubs, &periods, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
        // std::cerr << "buffer status " << buffer.status() << "\n";
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
  uint64_t end = toolbelt::Now();
  sub_thread.join();
  std::cerr << "Received " << num_messages << " messages\n";
  std::cerr << "Dropped " << (kNumMessages * kNumChannels) - num_messages
            << " messages\n";
  std::cerr << "Total time: " << end - start << " ns\n";
}

TEST_F(LatencyTest, ManyChannelsMultiplexed) {
  std::vector<subspace::Client> pub_clients;
  subspace::Client sub_client;
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr const char *kMux = "/logs/*";
  const int kNumChannels = LatencyValueForSplitBuffers(200, 40, 4);
  const int kNumSlots = LatencyValueForSplitBuffers(800, 160, 16);
  constexpr int kSlotSize = 32768;
  const int kNumMessages = LatencyValueForSplitBuffers(200, 100, 50);
  // Memory used ~= kNumSlots * kSlotSize
  std::vector<std::string> channels;

  for (int i = 0; i < kNumChannels; i++) {
    subspace::Client pub_client;
    ASSERT_OK(pub_client.Init(Socket()));
    pub_clients.push_back(std::move(pub_client));
    channels.push_back(absl::StrFormat("/logs/%d", i));
  }

  std::vector<Publisher> pubs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Publisher> pub = pub_clients[i].CreatePublisher(
        channels[i], kSlotSize, kNumSlots, subspace::PublisherOptions().SetMux(kMux));
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        channels[i], ([] { subspace::SubscriberOptions opts; opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); opts.SetMux(kMux); return opts; }()));
    // std::cerr << "sub status " << sub.status() << "\n";
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }

  std::cerr << "Total virtual memory: "
            << static_cast<double>(Server()->GetVirtualMemoryUsage()) /
                   (1024 * 1024)
            << " MB" << std::endl;
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

  srand(1024);
  std::vector<uint64_t> periods; // In microseconds
  for (int i = 0; i < kNumChannels; i++) {
    periods.push_back(5000 + rand() % 1000);
  }
  uint64_t start = toolbelt::Now();
  // Create a thread for each publisher, sending messages at the periods in the
  // vector.
  std::vector<std::thread> pub_threads;
  for (int i = 0; i < kNumChannels; i++) {
    pub_threads.emplace_back([i, &pubs, &periods, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
        // std::cerr << "buffer status " << buffer.status() << "\n";
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
  uint64_t end = toolbelt::Now();
  std::cerr << "Received " << num_messages << " messages\n";
  std::cerr << "Dropped " << (kNumMessages * kNumChannels) - num_messages
            << " messages\n";
  std::cerr << "Total time: " << end - start << " ns\n";
}

TEST_F(LatencyTest, ManyChannelsMultiplexedSubscribedToMux) {
  std::vector<subspace::Client> pub_clients;
  subspace::Client sub_client;
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr const char *kMux = "/logs/*";
  const int kNumChannels = LatencyValueForSplitBuffers(200, 40, 4);
  const int kNumSlots = LatencyValueForSplitBuffers(800, 160, 16);
  constexpr int kSlotSize = 32768;
  const int kNumMessages = LatencyValueForSplitBuffers(200, 100, 50);
  // Memory used ~= kNumSlots * kSlotSize
  std::vector<std::string> channels;

  for (int i = 0; i < kNumChannels; i++) {
    subspace::Client pub_client;
    ASSERT_OK(pub_client.Init(Socket()));
    pub_clients.push_back(std::move(pub_client));
    channels.push_back(absl::StrFormat("/logs/%d", i));
  }

  std::vector<Publisher> pubs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Publisher> pub = pub_clients[i].CreatePublisher(
        channels[i], kSlotSize, kNumSlots, subspace::PublisherOptions().SetMux(kMux));
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create subscriber to multiplexer.
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber(kMux, ([] { subspace::SubscriberOptions opts; opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
  // std::cerr << "sub status " << sub.status() << "\n";
  ASSERT_OK(sub);

  std::cerr << "Total virtual memory: "
            << static_cast<double>(Server()->GetVirtualMemoryUsage()) /
                   (1024 * 1024)
            << " MB" << std::endl;

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
    periods.push_back(5000 + rand() % 1000);
  }

  uint64_t start = toolbelt::Now();
  // Create a thread for each publisher, sending messages at the periods in the
  // vector.
  std::vector<std::thread> pub_threads;
  for (int i = 0; i < kNumChannels; i++) {
    pub_threads.emplace_back([i, &pubs, &periods, kNumMessages]() {
      int j = 0;
      while (j < kNumMessages) {
        absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
        // std::cerr << "buffer status " << buffer.status() << "\n";
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
  uint64_t end = toolbelt::Now();
  std::cerr << "Received " << num_messages << " messages\n";
  std::cerr << "Dropped " << (kNumMessages * kNumChannels) - num_messages
            << " messages\n";
  std::cerr << "Total time: " << end - start << " ns\n";
}

TEST_F(LatencyTest, SubscriberLatency) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_slots,avg_latency_ns\n";

  for (int num_slots = 100;
       num_slots < LatencyValueForSplitBuffers(10000, 4000);
       num_slots += 100) {
    // Create publisher.
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        "sublat", 256, num_slots, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);
    // Create subscriber.
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "sublat", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    // Fill channel.
    for (int i = 0; i < num_slots - 1; i++) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
    }

    uint64_t start = toolbelt::Now();
    // Read all messages and calculate average time per message in nanoseconds.
    for (int i = 0; i < num_slots - 1; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);
    }
    uint64_t end = toolbelt::Now();
    uint64_t average = (end - start) / (num_slots - 1);
    std::cerr << num_slots << "," << average << "\n";
    EmitLatencyMetric("SubscriberLatency", "read_messages", "num_slots",
                      num_slots, "average", average);
  }
}

TEST_F(LatencyTest, PubSubLatency) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  std::cerr << "num_messages,avg_latency_ns\n";
  for (int num_messages = 100;
       num_messages <= LatencyValueForSplitBuffers(20000, 5000);
       num_messages += 100) {
    absl::StatusOr<Publisher> pub =
        pub_client.CreatePublisher("pubsublat", 256, 10, subspace::PublisherOptions().SetReliable(false));
    ASSERT_OK(pub);
    // Create subscriber.
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
        "pubsublat", ([] { subspace::SubscriberOptions opts; opts.SetReliable(false); opts.SetLogDroppedMessages(false); opts.SetDetectDroppedMessages(false); return opts; }()));
    ASSERT_OK(sub);

    // Send and receive messages, measuring time taken.
    uint64_t start = toolbelt::Now();
    for (int i = 0; i < num_messages; i++) {
      // Send a message and immediately read it.
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      absl::StatusOr<const Message> pub_status = pub->PublishMessage(100);
      ASSERT_OK(pub_status);
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(100, msg->length);
    }
    uint64_t end = toolbelt::Now();
    uint64_t average = (end - start) / num_messages;
    std::cerr << num_messages << "," << average << "\n";
    EmitLatencyMetric("PubSubLatency", "publish_and_read", "num_messages",
                      num_messages, "average", average);
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  subspace::SetDefaultUseSplitBuffers(absl::GetFlag(FLAGS_use_split_buffers));

  return RUN_ALL_TESTS();
}
