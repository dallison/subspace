// Copyright 2023-2026 David Allison
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
#include <array>
#include <chrono>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <thread>
#include <unistd.h>

#if defined(__ANDROID__)
#define BRIDGE_TEST_TMP "/data/local/tmp"
#else
#define BRIDGE_TEST_TMP "/tmp"
#endif

ABSL_FLAG(bool, start_server, true, "Start the subspace servers");
ABSL_FLAG(std::string, server, "", "Path to server executable");
ABSL_FLAG(std::string, log_level, "debug", "Log level");
ABSL_FLAG(bool, use_split_buffers, false,
          "Run publishers with split-buffer payload storage");

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
    int lock_fd = ::open(BRIDGE_TEST_TMP "/subspace_bridge_test_port.lock",
                         O_CREAT | O_RDWR, 0666);
    ASSERT_NE(-1, lock_fd);
    ASSERT_EQ(0, ::flock(lock_fd, LOCK_EX));

    std::array<int, 2> disc_ports;
    std::array<toolbelt::UDPSocket, 2> reserved_ports;
    {
      for (int i = 0; i < 2; i++) {
        ASSERT_OK(reserved_ports[i].Bind(
            toolbelt::InetAddress::AnyAddress(/*port=*/0)));
        disc_ports[i] = reserved_ports[i].BoundAddress().Port();
      }
    }
    for (int i = 0; i < 2; i++) {
      printf("Starting Subspace server %d\n", i);
      char socket_name_template[] = BRIDGE_TEST_TMP "/subspaceXXXXXX"; // NOLINT
      ::close(mkstemp(&socket_name_template[0]));
      socket_[i] = &socket_name_template[0];

      // The server will write to this pipe to notify us when it
      // has started and stopped.  This end of the pipe is blocking.
      (void)pipe(server_pipe_[i]);

      int peer_port = disc_ports[(i + 1) % 2];
      reserved_ports[i].Close();
      server_[i] = std::make_unique<subspace::Server>(
          scheduler_[i], socket_[i], "", disc_ports[i], peer_port,
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
    ASSERT_EQ(0, ::flock(lock_fd, LOCK_UN));
    ::close(lock_fd);
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
std::string BridgeTest::socket_[2] = {BRIDGE_TEST_TMP "/subspace1", BRIDGE_TEST_TMP "/subspace2"};
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

absl::StatusOr<Message> ReadMessageEventually(Subscriber &sub) {
  for (int i = 0; i < 100; i++) {
    absl::StatusOr<Message> msg = sub.ReadMessage();
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length != 0) {
      return msg;
    }
    absl::Status status = sub.Wait(std::chrono::milliseconds(100));
    if (!status.ok()) {
      return status;
    }
  }
  return absl::DeadlineExceededError("Timed out waiting for bridge message");
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
  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
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
  absl::StatusOr<Message> msg = ReadMessageEventually(*sub1);
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub1->SlotSize());

  msg = ReadMessageEventually(*sub2);
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
  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());
  // Release the message on the subscriber (on server 2).
  msg->Reset();

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

void WaitForSubscribedMessageWithSizes(
    toolbelt::FileDescriptor &bridge_pipe, const std::string &channel_name,
    int32_t expected_checksum_size, int32_t expected_metadata_size) {
  std::cerr << "Waiting for bridge notification\n";
  char buffer[4096];
  int32_t length;
  absl::StatusOr<ssize_t> n = bridge_pipe.Read(&length, sizeof(length));
  ASSERT_OK(n);
  ASSERT_EQ(sizeof(int32_t), *n);
  length = ntohl(length);
  n = bridge_pipe.Read(buffer, length);
  ASSERT_OK(n);

  subspace::Subscribed subscribed;
  ASSERT_TRUE(subscribed.ParseFromArray(buffer, *n));
  ASSERT_EQ(subscribed.channel_name(), channel_name);
  ASSERT_EQ(expected_checksum_size, subscribed.checksum_size());
  ASSERT_EQ(expected_metadata_size, subscribed.metadata_size());
  std::cerr << "Received bridge notification for channel: "
            << subscribed.channel_name()
            << " checksum_size: " << subscribed.checksum_size()
            << " metadata_size: " << subscribed.metadata_size() << std::endl;
}

subspace::Subscribed
ReadSubscribedMessage(toolbelt::FileDescriptor &bridge_pipe,
                      const std::string &channel_name) {
  std::cerr << "Waiting for bridge notification\n";
  char buffer[4096];
  int32_t length;
  absl::StatusOr<ssize_t> n = bridge_pipe.Read(&length, sizeof(length));
  if (!n.ok()) {
    ADD_FAILURE() << n.status();
    return {};
  }
  EXPECT_EQ(sizeof(int32_t), *n);
  length = ntohl(length);
  n = bridge_pipe.Read(buffer, length);
  if (!n.ok()) {
    ADD_FAILURE() << n.status();
    return {};
  }

  subspace::Subscribed subscribed;
  EXPECT_TRUE(subscribed.ParseFromArray(buffer, *n));
  EXPECT_EQ(subscribed.channel_name(), channel_name);
  std::cerr << "Received bridge notification for channel: "
            << subscribed.channel_name() << std::endl;
  return subscribed;
}

TEST_F(BridgeTest, LargeChecksumBridge) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  // checksum_size=20, metadata_size=50 → prefix = Aligned<64>(48+20+50) = 128.
  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_sizes",
      {.slot_size = 256, .num_slots = 10, .local = false,
       .checksum_size = 20, .metadata_size = 50});
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_sizes", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessageWithSizes(send_bridge_pipe, "/bridged_sizes", 20, 50);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "bridge_sizes", 12);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(12);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessageWithSizes(recv_bridge_pipe, "/bridged_sizes", 20, 50);

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(12, msg->length);
  ASSERT_EQ(0, memcmp("bridge_sizes", msg->buffer, 12));
  ASSERT_EQ(128, sub->PrefixSize());
  ASSERT_EQ(20, sub->ChecksumSize());
  ASSERT_EQ(50, sub->MetadataSize());
}

TEST_F(BridgeTest, DefaultSizesBridge) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_def_sizes",
      {.slot_size = 256, .num_slots = 10, .local = false});
  ASSERT_OK(pub);
  ASSERT_EQ(64, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub = client2.CreateSubscriber(
      "/bridged_def_sizes", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessageWithSizes(send_bridge_pipe, "/bridged_def_sizes",
                                    4, 0);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "default", 7);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessageWithSizes(recv_bridge_pipe, "/bridged_def_sizes",
                                    4, 0);

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(7, msg->length);
  ASSERT_EQ(0, memcmp("default", msg->buffer, 7));
  ASSERT_EQ(64, sub->PrefixSize());
}

// metadata_size=24, checksum_size=4: prefix=128 (48+4+24=76, rounds to 128).
// Verify metadata survives the bridge.
TEST_F(BridgeTest, MetadataBridge) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_meta",
      {.slot_size = 256, .num_slots = 10, .local = false, .metadata_size = 24});
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());
  ASSERT_EQ(24, pub->MetadataSize());

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_meta", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessageWithSizes(send_bridge_pipe, "/bridged_meta", 4, 24);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "bridgemeta", 10);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(24u, meta.size());
  memcpy(meta.data(), "BRIDGE_METADATA_24BYTES!", 24);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(10);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessageWithSizes(recv_bridge_pipe, "/bridged_meta", 4, 24);

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(10, msg->length);
  ASSERT_EQ(0, memcmp("bridgemeta", msg->buffer, 10));
  ASSERT_EQ(128, sub->PrefixSize());
  ASSERT_EQ(24, sub->MetadataSize());

  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(24u, sub_meta.size());
  ASSERT_EQ(0, memcmp("BRIDGE_METADATA_24BYTES!", sub_meta.data(), 24));
}

// metadata_size=100, checksum_size=20: prefix=192 (48+20+100=168, rounds to 192).
TEST_F(BridgeTest, MetadataLargeBridge) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_meta_lg",
      {.slot_size = 256, .num_slots = 10, .local = false,
       .checksum_size = 20, .metadata_size = 100});
  ASSERT_OK(pub);
  ASSERT_EQ(192, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_meta_lg", {.max_active_messages = 2});
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessageWithSizes(send_bridge_pipe, "/bridged_meta_lg",
                                    20, 100);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "large", 5);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(100u, meta.size());
  for (int i = 0; i < 100; i++) {
    meta[i] = static_cast<std::byte>(i ^ 0xAB);
  }

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessageWithSizes(recv_bridge_pipe, "/bridged_meta_lg",
                                    20, 100);

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(5, msg->length);
  ASSERT_EQ(192, sub->PrefixSize());
  ASSERT_EQ(20, sub->ChecksumSize());
  ASSERT_EQ(100, sub->MetadataSize());

  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(100u, sub_meta.size());
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(static_cast<std::byte>(i ^ 0xAB), sub_meta[i]) << "at index " << i;
  }
}

TEST_F(BridgeTest, SplitBufferSourceBridge) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_split_source",
      {.slot_size = 256, .num_slots = 10, .local = false,
       .use_split_buffers = true, .split_buffers_over_bridge = false});
  ASSERT_OK(pub);
  ASSERT_TRUE(pub->UsesSplitBuffers());

  absl::StatusOr<Subscriber> sub = client2.CreateSubscriber(
      "/bridged_split_source", {.max_active_messages = 2});
  ASSERT_OK(sub);

  subspace::Subscribed sent = ReadSubscribedMessage(
      BridgeNotificationPipe(0), "/bridged_split_source");
  ASSERT_TRUE(sent.split_buffers());
  ASSERT_FALSE(sent.split_buffers_over_bridge());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "split-source", 12);
  ASSERT_OK(pub->PublishMessage(12));

  subspace::Subscribed received = ReadSubscribedMessage(
      BridgeNotificationPipe(1), "/bridged_split_source");
  ASSERT_TRUE(received.split_buffers());
  ASSERT_FALSE(received.split_buffers_over_bridge());

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(12, msg->length);
  ASSERT_EQ(0, memcmp("split-source", msg->buffer, 12));
  ASSERT_FALSE(sub->UsesSplitBuffers());
}

TEST_F(BridgeTest, SplitBuffersOverBridge) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_split_remote",
      {.slot_size = 256, .num_slots = 10, .local = false,
       .use_split_buffers = false, .split_buffers_over_bridge = true});
  ASSERT_OK(pub);
  ASSERT_FALSE(pub->UsesSplitBuffers());

  absl::StatusOr<Subscriber> sub = client2.CreateSubscriber(
      "/bridged_split_remote", {.max_active_messages = 2});
  ASSERT_OK(sub);

  subspace::Subscribed sent = ReadSubscribedMessage(
      BridgeNotificationPipe(0), "/bridged_split_remote");
  ASSERT_FALSE(sent.split_buffers());
  ASSERT_TRUE(sent.split_buffers_over_bridge());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "split-remote", 12);
  ASSERT_OK(pub->PublishMessage(12));

  subspace::Subscribed received = ReadSubscribedMessage(
      BridgeNotificationPipe(1), "/bridged_split_remote");
  ASSERT_FALSE(received.split_buffers());
  ASSERT_TRUE(received.split_buffers_over_bridge());

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(12, msg->length);
  ASSERT_EQ(0, memcmp("split-remote", msg->buffer, 12));
  ASSERT_TRUE(sub->UsesSplitBuffers());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  subspace::SetDefaultUseSplitBuffers(absl::GetFlag(FLAGS_use_split_buffers));

  return RUN_ALL_TESTS();
}
