// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status_matchers.h"
#include "client/client.h"
#include "co/coroutine.h"
#include "proto/subspace.pb.h"
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

ABSL_FLAG(bool, start_server, true, "Start the subspace servers");
ABSL_FLAG(std::string, server, "", "Path to server executable");
ABSL_FLAG(std::string, log_level, "debug", "Log level");

void SignalHandler(int sig) { printf("Signal %d", sig); }

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;
using InetAddress = toolbelt::InetAddress;

#define VAR(a) a##__COUNTER__
#define EVAL_AND_ASSERT_OK(expr) EVAL_AND_ASSERT_OK2(VAR(r_), expr)

#define EVAL_AND_ASSERT_OK2(result, expr)                                      \
  ({                                                                           \
    auto result = (expr);                                                      \
    if (!result.ok()) {                                                        \
      std::cerr << result.status() << std::endl;                               \
    }                                                                          \
    ASSERT_OK(result);                                                  \
    std::move(*result);                                                        \
  })

#define ASSERT_OK(e) ASSERT_THAT(e, ::absl_testing::IsOk())

class BridgeTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    constexpr int kDiscPorts[2] = {6522, 6523};
    for (int i = 0; i < 2; i++) {
      printf("Starting Subspace server %d\n", i);
      char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
      ::close(mkstemp(&socket_name_template[0]));
      socket_[i] = &socket_name_template[0];

      // The server will write to this pipe to notify us when it
      // has started and stopped.  This end of the pipe is blocking.
      (void)pipe(server_pipe_[i]);

      int peer_port = kDiscPorts[(i + 1) % 2];
      server_[i] = std::make_unique<subspace::Server>(
          scheduler_[i], socket_[i], "", kDiscPorts[i % 2], peer_port,
          /*local=*/false, server_pipe_[i][1]);

      server_[i]->SetLogLevel(absl::GetFlag(FLAGS_log_level));

      auto bridge_pipe = server_[i]->CreateBridgeNotificationPipe();
      ASSERT_OK(bridge_pipe);
      bridge_notification_pipe_[i] = *bridge_pipe;

      // Start server running in a thread.
      server_thread_[i] = std::thread([i]() {
        absl::Status s = server_[i]->Run();
        if (!s.ok()) {
          fprintf(stderr, "Error running Subspace server: %s\n",
                  s.ToString().c_str());
          exit(1);
        }
      });

      // Wait for server to tell us that it's running.
      char buf[8];
      (void)::read(server_pipe_[i][0], buf, 8);
    }
  }

  static void TearDownTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    printf("Stopping Subspace server\n");
    server_[0]->Stop();

    // Wait for server to tell us that it's stopped.
    char buf[8];
    (void)::read(server_pipe_[0][0], buf, 8);
    server_thread_[0].join();

    // Stop server 1.
    server_[1]->Stop();

    // Wait for server to tell us that it's stopped.
    (void)::read(server_pipe_[1][0], buf, 8);
    server_thread_[1].join();
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
  void TearDown() override {}

  void InitClient(subspace::Client &client, int server) {
    ASSERT_OK(client.Init(Socket(server)));
  }

  static const std::string &Socket(int i) { return socket_[i]; }

  static subspace::Server *Server(int i) { return server_[i].get(); }

  static toolbelt::FileDescriptor &BridgeNotificationPipe(int i) {
    return bridge_notification_pipe_[i];
  }

private:
  static co::CoroutineScheduler scheduler_[2];
  static std::string socket_[2];
  static int server_pipe_[2][2];
  static std::unique_ptr<subspace::Server> server_[2];
  static std::thread server_thread_[2];
  static toolbelt::FileDescriptor bridge_notification_pipe_[2];
};

co::CoroutineScheduler BridgeTest::scheduler_[2];
std::string BridgeTest::socket_[2] = {"/tmp/subspace1", "/tmp/subspace2"};
int BridgeTest::server_pipe_[2][2];
std::unique_ptr<subspace::Server> BridgeTest::server_[2];
std::thread BridgeTest::server_thread_[2];
toolbelt::FileDescriptor BridgeTest::bridge_notification_pipe_[2];
static co::CoroutineScheduler *g_scheduler[2];

// For debugging, hit ^\ to dump all coroutines if this test is not working
// properly.
static void SigQuitHandler(int sig) {
  std::cout << "\nAll coroutines:" << std::endl;
  g_scheduler[0]->Show();
  g_scheduler[1]->Show();
  signal(sig, SIG_DFL);
  (void)raise(sig);
}

void WaitForSubscribedMessage(toolbelt::FileDescriptor &bridge_pipe,
                              const std::string &channel_name) {
  std::cerr << "Waiting for bridge notification\n";
  char buffer[4096];
  int32_t length;
  absl::StatusOr<ssize_t> n = bridge_pipe.Read(&length, sizeof(length));
  ASSERT_OK(n);
  ASSERT_EQ(sizeof(int32_t), *n);
  length = ntohl(length); // Length is network byte order.

  n = bridge_pipe.Read(buffer, length);
  ASSERT_OK(n);

  subspace::Subscribed subscribed;
  ASSERT_TRUE(subscribed.ParseFromArray(buffer, *n));
  ASSERT_EQ(subscribed.channel_name(), channel_name);
  std::cerr << "Received bridge notification for channel: "
            << subscribed.channel_name() << std::endl;
}

TEST_F(BridgeTest, Basic) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_channel", {.slot_size = 256, .num_slots = 10, .local = false});
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_channel", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_channel");

  // Send a message on the publisher.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_channel");

  // Receive the message on the subscriber.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());
}

TEST_F(BridgeTest, TwoSubs) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_channel", {.slot_size = 256, .num_slots = 10, .local = false});
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub1 =
      client2.CreateSubscriber("/bridged_channel", {.max_active_messages = 2});
  ASSERT_OK(sub1);

  absl::StatusOr<Subscriber> sub2 =
      client2.CreateSubscriber("/bridged_channel", {.max_active_messages = 2});
  ASSERT_OK(sub2);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_channel");

  // Send a message on the publisher.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_channel");

  // Receive the message on the subscriber.
  absl::StatusOr<Message> msg = sub1->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub1->SlotSize());

  msg = sub2->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub2->SlotSize());
}

TEST_F(BridgeTest, BasicRetirement) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub =
      client1.CreatePublisher("/bridged_channel", {.slot_size = 256,
                                                   .num_slots = 10,
                                                   .local = false,
                                                   .notify_retirement = true});
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_channel", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_channel");

  // Send a message on the publisher.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_channel");

  auto retirement_fd = pub->GetRetirementFd();
  ASSERT_TRUE(retirement_fd.Valid());

  // Receive the message on the subscriber.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());
  // Release the message on the subscriber (on server 2).
  msg->Release();

  std::cerr << "Waiting for retirement notification..." << std::endl;
  // Read the retirement fd.
  int32_t slot_id;
  ssize_t n = ::read(retirement_fd.Fd(), &slot_id, sizeof(slot_id));
  ASSERT_EQ(n, sizeof(slot_id));
  ASSERT_EQ(0, slot_id);
}

TEST_F(BridgeTest, MultipleRetirement) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  constexpr int kNumSlots = 10;

  // Create publisher to consume a slot.  This will check that we received
  // retirement notifications for the correct slot ids.  This will use slot 0
  // so we will never receive a retirement notification for it.
  absl::StatusOr<Publisher> local_pub = client1.CreatePublisher(
      "/bridged_channel",
      {.slot_size = 256, .num_slots = kNumSlots, .local = false});
  ASSERT_OK(local_pub);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub =
      client1.CreatePublisher("/bridged_channel", {.slot_size = 256,
                                                   .num_slots = kNumSlots,
                                                   .local = false,
                                                   .notify_retirement = true});
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_channel", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_channel");

  constexpr int kNumMessages = 7;
  for (int i = 0; i < kNumMessages; i++) {
    // Send a message on the publisher.
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    snprintf(static_cast<char *>(*buffer), 256, "foobar %d", i);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(12 + i);
    ASSERT_OK(pub_status);
  }

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_channel");

  auto retirement_fd = pub->GetRetirementFd();
  ASSERT_TRUE(retirement_fd.Valid());

  // Receive all messages on the subscriber.
  (void)sub->Wait();
  int num_received = 0;
  while (num_received < kNumMessages) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    if (!msg.ok()) {
      break;
    }
    if (msg->length == 0) {
      (void)sub->Wait();
      continue;
    }
    std::cerr << "received message: " << msg->ordinal << " in slot "
              << msg->slot_id << std::endl;
    num_received++;
  }
  sub->ClearActiveMessage();

  std::cerr << "Waiting for retirement notifications..." << std::endl;
  std::set<int> retired_slots;
  int num_notifications = 0;
  while (num_notifications < kNumMessages) {
    // Read the retirement fd.
    int32_t slot_id;
    ssize_t n = ::read(retirement_fd.Fd(), &slot_id, sizeof(slot_id));
    if (n <= 0) {
      break; // No more notifications, we're done.
    }
    std::cerr << "received retirement notification for slot " << slot_id
              << std::endl;
    ASSERT_EQ(n, sizeof(slot_id));
    retired_slots.insert(slot_id);
    num_notifications++;
  }

  // Make sure all slots have been retired.
  // NB: We use i+1 because we have a publisher on slot 0 which will never be
  // used.
  for (int i = 0; i < kNumMessages; i++) {
    ASSERT_TRUE(retired_slots.count(i + 1));
  }
}

TEST_F(BridgeTest, MultipleRetirement2) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  constexpr int kNumSlots = 10;

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub =
      client1.CreatePublisher("/bridged_channel", {.slot_size = 256,
                                                   .num_slots = kNumSlots,
                                                   .local = false,
                                                   .notify_retirement = true});
  ASSERT_OK(pub);

  // Create publisher on the subscriber server.  This wil consume slot 0 on that
  // side.
  absl::StatusOr<Publisher> local_pub = client2.CreatePublisher(
      "/bridged_channel",
      {.slot_size = 256, .num_slots = kNumSlots, .local = false});
  ASSERT_OK(local_pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_channel", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_channel");

  constexpr int kNumMessages = 7;
  for (int i = 0; i < kNumMessages; i++) {
    // Send a message on the publisher.
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    snprintf(static_cast<char *>(*buffer), 256, "foobar %d", i);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(12 + i);
    ASSERT_OK(pub_status);
  }

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_channel");

  auto retirement_fd = pub->GetRetirementFd();
  ASSERT_TRUE(retirement_fd.Valid());

  // Receive all messages on the subscriber.
  (void)sub->Wait();
  int num_received = 0;
  while (num_received < kNumMessages) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    if (!msg.ok()) {
      break;
    }
    if (msg->length == 0) {
      (void)sub->Wait();
      continue;
    }
    std::cerr << "received message: " << msg->ordinal << " in slot "
              << msg->slot_id << std::endl;
    num_received++;
  }
  sub->ClearActiveMessage();

  std::cerr << "Waiting for retirement notifications..." << std::endl;
  std::set<int> retired_slots;
  int num_notifications = 0;
  while (num_notifications < kNumMessages) {
    // Read the retirement fd.
    int32_t slot_id;
    ssize_t n = ::read(retirement_fd.Fd(), &slot_id, sizeof(slot_id));
    if (n <= 0) {
      break; // No more notifications, we're done.
    }
    std::cerr << "received retirement notification for slot " << slot_id
              << std::endl;
    ASSERT_EQ(n, sizeof(slot_id));
    retired_slots.insert(slot_id);
    num_notifications++;
  }

  // Make sure all slots have been retired.
  for (int i = 0; i < kNumMessages; i++) {
    ASSERT_TRUE(retired_slots.count(i));
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
