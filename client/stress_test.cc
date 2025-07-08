// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/hash/hash_testing.h"
#include "client/client.h"
#include "coroutine.h"
#include "server/server.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/resource.h>
#include <thread>

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");

void SignalHandler(int sig) { printf("Signal %d", sig); }

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;
using InetAddress = toolbelt::InetAddress;

class StressTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    printf("Starting Subspace server\n");
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    // The server will write to this pipe to notify us when it
    // has started and stopped.  This end of the pipe is blocking.
    (void)pipe(server_pipe_);

    server_ =
        std::make_unique<subspace::Server>(scheduler_, socket_, "", 0, 0,
                                           /*local=*/true, server_pipe_[1]);

    // Start server running in a thread.
    server_thread_ = std::thread([]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });

    // Wait for server to tell us that it's running.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  static void TearDownTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    printf("Stopping Subspace server\n");
    server_->Stop();

    // Wait for server to tell us that it's stopped.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
  void TearDown() override {}

  void InitClient(subspace::Client &client) {
    ASSERT_TRUE(client.Init(Socket()).ok());
  }

  static const std::string &Socket() { return socket_; }

  static subspace::Server *Server() { return server_.get(); }

private:
  static co::CoroutineScheduler scheduler_;
  static std::string socket_;
  static int server_pipe_[2];
  static std::unique_ptr<subspace::Server> server_;
  static std::thread server_thread_;
};

co::CoroutineScheduler StressTest::scheduler_;
std::string StressTest::socket_ = "/tmp/subspace";
int StressTest::server_pipe_[2];
std::unique_ptr<subspace::Server> StressTest::server_;
std::thread StressTest::server_thread_;

static co::CoroutineScheduler *g_scheduler;

// For debugging, hit ^\ to dump all coroutines if this test is not working
// properly.
static void SigQuitHandler(int sig) {
  std::cout << "\nAll coroutines:" << std::endl;
  g_scheduler->Show();
  signal(sig, SIG_DFL);
  (void)raise(sig);
}

#define VAR(a) a##__COUNTER__
#define EVAL_AND_ASSERT_OK(expr) EVAL_AND_ASSERT_OK2(VAR(r_), expr)

#define EVAL_AND_ASSERT_OK2(result, expr)                                      \
  ({                                                                           \
    auto result = (expr);                                                      \
    if (!result.ok()) {                                                        \
      std::cerr << result.status() << std::endl;                               \
    }                                                                          \
    ASSERT_TRUE(result.ok());                                                  \
    std::move(*result);                                                        \
  })

#define ASSERT_OK(e) ASSERT_TRUE(e.ok())

TEST_F(StressTest, Coroutines) {
  auto oldSig = signal(SIGQUIT, SigQuitHandler);

  constexpr int kNumClients = 10;
  constexpr int kNumChannels = 100; // Channels per client.
  constexpr int kNumSlots = kNumChannels * 2 + 1;

  struct Client {
    explicit Client(std::shared_ptr<subspace::Client> client)
        : client(std::move(client)) {}
    std::shared_ptr<subspace::Client> client;
    std::vector<std::string> channels;
    std::vector<subspace::Publisher> pubs;
    std::vector<subspace::Subscriber> subs;
  };

  co::CoroutineScheduler scheduler;
  g_scheduler = &scheduler;

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
              {.slot_size = 32, .num_slots = kNumSlots})));
    }

    // Create the subscribers.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].subs.emplace_back(EVAL_AND_ASSERT_OK(
          clients[i].client->CreateSubscriber(clients[i].channels[j])));
    }
  }

  std::vector<std::unique_ptr<co::Coroutine>> coroutines;

  // Publish messages
  auto publish = [&](co::Coroutine *c, int client_id, int pubId) {
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
  auto publish_all = [&](co::Coroutine *c, int client_id) {
    for (int i = 0; i < kNumChannels; i++) {
      coroutines.push_back(std::make_unique<co::Coroutine>(
          scheduler,
          [&publish, client_id, i](co::Coroutine *c1) {
            publish(c1, client_id, i);
          },
          absl::StrFormat("pub/%d/%d", client_id, i)));
      c->Yield();
    }
  };

  // Publish messages.
  for (int i = 0; i < kNumClients; i++) {
    coroutines.push_back(std::make_unique<co::Coroutine>(
        scheduler, [&publish_all, i](co::Coroutine *c) { publish_all(c, i); },
        absl::StrFormat("publish_all/%d", i)));
  }

  // Read all messages from a single subscriber.
  auto read = [&](co::Coroutine *c, int client_id, int subId) {
    int num_messages = 0;
    while (num_messages < kNumSlots - 1) {
      // Wait for notification of a message.
      ASSERT_OK(clients[client_id].subs[subId].Wait(c));
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
  auto readAll = [&](co::Coroutine *c, int client_id) {
    for (int i = 0; i < kNumChannels; i++) {
      coroutines.push_back(std::make_unique<co::Coroutine>(
          scheduler,
          [&read, client_id, i](co::Coroutine *c1) { read(c1, client_id, i); },
          absl::StrFormat("sub/%d/%d", client_id, i)));
    }
  };

  // Read messages.
  for (int i = 0; i < kNumClients; i++) {
    coroutines.push_back(std::make_unique<co::Coroutine>(
        scheduler, [&readAll, i](co::Coroutine *c) { readAll(c, i); },
        "readall"));
  }

  scheduler.Run();

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
  constexpr int kNumClients = 10;
  constexpr int kNumChannels = 100; // Channels per client.

  constexpr int kMaxActiveMessages = 5;
  // Number of channels accounts for all publishers, subscribers and unique_ptrs
  constexpr int kNumSlots = kNumChannels * (2 + kMaxActiveMessages) + 1;

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
              {.slot_size = 32, .num_slots = kNumSlots})));
    }

    // Create the subscribers.
    for (int j = 0; j < kNumChannels; j++) {
      clients[i].subs.emplace_back(
          EVAL_AND_ASSERT_OK(clients[i].client->CreateSubscriber(
              clients[i].channels[j],
              {.max_active_messages = kMaxActiveMessages})));
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
          client.channel, {.slot_size = 32, .num_slots = kNumSlots})));
  client.sub = std::make_unique<subspace::Subscriber>(
      EVAL_AND_ASSERT_OK(client.client->CreateSubscriber(
          client.channel, {.max_active_messages = kMaxActiveMessages,
                           .log_dropped_messages = false})));

  struct Receiver {
    std::thread thread;
    toolbelt::SharedPtrPipe<subspace::Message> pipe;
  };

  std::vector<Receiver> receivers;
  std::atomic<int> num_messages_received = 0;
  std::atomic<int> num_dropped_messages = 0;

  std::cerr << "expecting " << kNumMessages << " messages" << std::endl;
  // Create the receivers and start them running.
  for (int i = 0; i < kMaxActiveMessages; i++) {
    receivers.push_back(Receiver());
    auto pipe = toolbelt::SharedPtrPipe<subspace::Message>::Create();
    ASSERT_OK(pipe);
    receivers[i].pipe = *pipe;

    receivers[i].thread = std::thread([&, i]() {
      while (num_messages_received <
             kNumMessages - num_dropped_messages.load()) {
        auto msg = receivers[i].pipe.Read();
        num_messages_received++;
        // Just throw the message away.
        // We are interested in the receiver reading the message, not what the
        // message contains.
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
          client.channel, {.slot_size = 32, .num_slots = kNumSlots})));
  client.sub = std::make_unique<subspace::Subscriber>(
      EVAL_AND_ASSERT_OK(client.client->CreateSubscriber(
          client.channel, {.max_active_messages = kMaxActiveMessages,
                           .log_dropped_messages = false})));

  struct Receiver {
    std::thread thread;
    toolbelt::SharedPtrPipe<subspace::Message> pipe;
  };

  std::vector<Receiver> receivers;
  std::atomic<int> num_messages_received = 0;
  std::atomic<int> num_dropped_messages = 0;

  std::cerr << "expecting " << kNumMessages << " messages" << std::endl;
  // Create the receivers and start them running.
  for (int i = 0; i < kMaxActiveMessages; i++) {
    receivers.push_back(Receiver());
    auto pipe = toolbelt::SharedPtrPipe<subspace::Message>::Create();
    ASSERT_OK(pipe);
    receivers[i].pipe = *pipe;

    receivers[i].thread = std::thread([&, i]() {
      while (num_messages_received <
             kNumMessages - num_dropped_messages.load()) {
        auto msg = receivers[i].pipe.Read();
        num_messages_received++;
        // Just throw the message away.
        // We are interested in the receiver reading the message, not what the
        // message contains.
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

  constexpr int kNumSlots = 3000;
  constexpr int kNumThreads = 8;
  constexpr int kVchansPerThread = 125;
  constexpr int kNumVchans = kNumThreads * kVchansPerThread;
  constexpr int kNumMessages = kNumThreads * kNumVchans;

  std::vector<std::shared_ptr<subspace::Client>> clients;
  std::vector<subspace::Publisher> publishers;
  for (int i = 0; i < kNumVchans; i++) {
    clients.push_back(EVAL_AND_ASSERT_OK(
        subspace::Client::Create(Socket(), absl::StrFormat("client%d", i))));
    absl::StatusOr<Publisher> p = clients[i]->CreatePublisher(
        absl::StrFormat("/foobar/%d", i),
        {.slot_size = 64, .num_slots = kNumSlots, .mux = "/foobar"});
    ASSERT_OK(p);
    publishers.push_back(std::move(*p));
  }

  // Create a subscriber to the mux.
  absl::StatusOr<Subscriber> s = subClient->CreateSubscriber("/foobar");
  ASSERT_OK(s);
  int pipes[2];
  ASSERT_EQ(0, pipe(pipes));

  // Thread to read messages from the mux.
  std::thread sub_thread([&s, pipes]() {
    int num_messages = 0;
    while (num_messages < kNumMessages) {
      // Wait for notification of a message.
      struct pollfd fds[2];
      fds[0] = s->GetPollFd();
      fds[1] = {.fd = pipes[0], .events = POLLIN};
      int e = ::poll(fds, 2, -1);
      ASSERT_GT(e, 0);
      if (fds[1].revents & POLLIN) {
        break;
      }
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
  sleep(2);
  // Interrupt the subscriber to cause it to stop.
  close(pipes[1]);

  // Wait for the subscriber to finish.
  sub_thread.join();

  close(pipes[0]);
}

TEST_F(StressTest, ManyChannelsNonMultiplexed) {
  std::vector<std::shared_ptr<subspace::Client>> pub_clients;
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  constexpr int kNumChannels = 200;
  constexpr int knum_slots = 100;
  constexpr int kSlotSize = 32768;
  constexpr int kNumMessages = 200;
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
        channels[i], {.slot_size = kSlotSize, .num_slots = knum_slots});
    ASSERT_TRUE(pub.ok());
    pubs.push_back(std::move(*pub));
  }

  // Create subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Subscriber> sub = sub_client->CreateSubscriber(
        channels[i], {.log_dropped_messages = false});
    // std::cerr << "sub status " << sub.status() << "\n";
    ASSERT_TRUE(sub.ok());
    subs.push_back(std::move(*sub));
  }

  std::atomic<int> num_messages = 0;

  // Create a thread to read from all subscribers.
  std::thread sub_thread([&subs, &num_messages] {
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
            ASSERT_TRUE(msg.ok());
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
    pub_threads.emplace_back([i, &pubs, &periods]() {
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
        ASSERT_TRUE(pub_status.ok());
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
      ASSERT_TRUE(pub_status.ok());
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
  constexpr int kNumChannels = 200;
  constexpr int knum_slots = 800;
  constexpr int kSlotSize = 32768;
  constexpr int kNumMessages = 200;
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
        {.slot_size = kSlotSize, .num_slots = knum_slots, .mux = kMux});
    ASSERT_TRUE(pub.ok());
    pubs.push_back(std::move(*pub));
  }

  // Create subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumChannels; i++) {
    absl::StatusOr<Subscriber> sub = sub_client->CreateSubscriber(
        channels[i], {.log_dropped_messages = false, .mux = kMux});
    // std::cerr << "sub status " << sub.status() << "\n";
    ASSERT_TRUE(sub.ok());
    subs.push_back(std::move(*sub));
  }

  std::atomic<int> num_messages = 0;

  // Create a thread to read from all subscribers.
  std::thread sub_thread([&subs, &num_messages] {
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
            ASSERT_TRUE(msg.ok());
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
    pub_threads.emplace_back([i, &pubs, &periods]() {
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
        ASSERT_TRUE(pub_status.ok());
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
      ASSERT_TRUE(pub_status.ok());
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
  constexpr int kNumChannels = 200;
  constexpr int knum_slots = 1000;
  constexpr int kSlotSize = 32768;
  constexpr int kNumMessages = 200;
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
        pub_clients[i]->CreatePublisher(channels[i], {.slot_size = kSlotSize,
                                                      .num_slots = knum_slots,
                                                      .activate = true,
                                                      .mux = kMux});
    ASSERT_TRUE(pub.ok());
    pubs.push_back(std::move(*pub));
  }

  // Create subscriber to multiplexer.
  absl::StatusOr<Subscriber> sub =
      sub_client->CreateSubscriber(kMux, {.log_dropped_messages = false});
  // std::cerr << "sub status " << sub.status() << "\n";
  ASSERT_TRUE(sub.ok());

  std::atomic<int> num_messages = 0;

  // Create a thread to read from the mux subscriber.
  std::thread sub_thread([&sub, &num_messages] {
    struct pollfd pfd = sub->GetPollFd();

    int num_dropped = 0;
    std::array<uint64_t, kNumChannels> last_ordinals{0};
    while (num_messages < kNumMessages * kNumChannels - num_dropped) {
      poll(&pfd, 1, -1);
      if (pfd.revents & POLLIN) {
        for (;;) {
          absl::StatusOr<Message> msg = sub->ReadMessage();
          ASSERT_TRUE(msg.ok());
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
    pub_threads.emplace_back([i, &pubs, &periods]() {
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
        ASSERT_TRUE(pub_status.ok());
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
      ASSERT_TRUE(pub_status.ok());
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

  return RUN_ALL_TESTS();
}
