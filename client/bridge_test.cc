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
#include <poll.h>
#include <set>
#include <signal.h>
#include <sys/file.h>
#if defined(__linux__)
#include <sys/socket.h>
#if __has_include(<linux/vm_sockets.h>)
#include <linux/vm_sockets.h>
#endif
#endif
#include <sys/resource.h>
#include <thread>
#include <unistd.h>
#include <vector>

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
  static subspace::async::RuntimeEngine scheduler_[2];
  static std::string socket_[2];
  static int server_pipe_[2][2];
  static std::unique_ptr<subspace::Server> server_[2];
  static std::thread server_thread_[2];
  static toolbelt::FileDescriptor bridge_notification_pipe_[2];
};

subspace::async::RuntimeEngine BridgeTest::scheduler_[2];
std::string BridgeTest::socket_[2] = {BRIDGE_TEST_TMP "/subspace1", BRIDGE_TEST_TMP "/subspace2"};
int BridgeTest::server_pipe_[2][2];
std::unique_ptr<subspace::Server> BridgeTest::server_[2];
std::thread BridgeTest::server_thread_[2];
toolbelt::FileDescriptor BridgeTest::bridge_notification_pipe_[2];
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
static co::CoroutineScheduler *g_scheduler[2];
#endif

// For debugging, hit ^\ to dump all coroutines if this test is not working
// properly.
static void SigQuitHandler(int sig) {
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
  std::cout << "\nAll coroutines:" << std::endl;
  g_scheduler[0]->Show();
  g_scheduler[1]->Show();
#endif
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

absl::StatusOr<Message> ReadMessageEventually(Subscriber &sub, int attempts) {
  for (int i = 0; i < attempts; i++) {
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

absl::StatusOr<Message> ReadMessageEventually(Subscriber &sub) {
  return ReadMessageEventually(sub, 100);
}

struct BridgeRangeConfig {
  int first_port = 0;
  int last_port = 0;
  bool fallback_to_ephemeral = false;
};

std::vector<toolbelt::StreamSocket> ReserveTcpPortRange(int num_ports,
                                                        int *first_port,
                                                        int *last_port) {
  for (int attempt = 0; attempt < 100; ++attempt) {
    std::vector<toolbelt::StreamSocket> sockets;
    toolbelt::StreamSocket first_socket;
    absl::Status status =
        first_socket.Bind(toolbelt::InetAddress::AnyAddress(/*port=*/0), true);
    if (!status.ok()) {
      continue;
    }

    int candidate_first = first_socket.BoundAddress().Port();
    if (candidate_first + num_ports - 1 > 65535) {
      continue;
    }
    sockets.push_back(std::move(first_socket));

    bool reserved = true;
    for (int port = candidate_first + 1;
         port < candidate_first + num_ports; ++port) {
      toolbelt::StreamSocket socket;
      status = socket.Bind(toolbelt::InetAddress::AnyAddress(port), true);
      if (!status.ok()) {
        reserved = false;
        break;
      }
      sockets.push_back(std::move(socket));
    }

    if (reserved) {
      *first_port = candidate_first;
      *last_port = candidate_first + num_ports - 1;
      return sockets;
    }
  }

  ADD_FAILURE() << "Unable to reserve a contiguous TCP port range";
  return {};
}

class ScopedBridgeServerPair {
public:
  ~ScopedBridgeServerPair() { Stop(); }

  void Start(const std::array<BridgeRangeConfig, 2> &ranges,
             int num_asio_threads = 1, bool use_vsock = false,
             uint32_t vsock_cid = 0) {
    int lock_fd = ::open(BRIDGE_TEST_TMP "/subspace_bridge_test_port.lock",
                         O_CREAT | O_RDWR, 0666);
    ASSERT_NE(-1, lock_fd);
    ASSERT_EQ(0, ::flock(lock_fd, LOCK_EX));

    std::array<int, 2> disc_ports;
    std::array<toolbelt::UDPSocket, 2> reserved_ports;
    for (int i = 0; i < 2; i++) {
      ASSERT_OK(reserved_ports[i].Bind(
          toolbelt::InetAddress::AnyAddress(/*port=*/0)));
      disc_ports[i] = reserved_ports[i].BoundAddress().Port();
    }

    for (int i = 0; i < 2; i++) {
      char socket_name_template[] = BRIDGE_TEST_TMP "/subspaceXXXXXX"; // NOLINT
      ::close(mkstemp(&socket_name_template[0]));
      socket_[i] = &socket_name_template[0];

      (void)pipe(server_pipe_[i]);
      int peer_port = disc_ports[(i + 1) % 2];
      server_[i] = std::make_unique<subspace::Server>(
          scheduler_[i], socket_[i], "", disc_ports[i], peer_port,
          /*local=*/false, server_pipe_[i][1]);
      server_[i]->SetLogLevel(absl::GetFlag(FLAGS_log_level));

      if (use_vsock) {
        server_[i]->SetVsockBridging(true, vsock_cid);
      }

      if (ranges[i].first_port != 0) {
        ASSERT_OK(server_[i]->SetBridgePortRange(
            ranges[i].first_port, ranges[i].last_port,
            ranges[i].fallback_to_ephemeral));
      }

      auto bridge_pipe = server_[i]->CreateBridgeNotificationPipe();
      ASSERT_OK(bridge_pipe);
      bridge_notification_pipe_[i] = *bridge_pipe;
    }

    for (auto &port : reserved_ports) {
      port.Close();
    }

    for (int i = 0; i < 2; i++) {
      server_thread_[i] = std::thread([this, i, num_asio_threads]() {
        absl::Status s = server_[i]->Run(num_asio_threads);
        if (!s.ok()) {
          fprintf(stderr, "Error running Subspace server: %s\n",
                  s.ToString().c_str());
          exit(1);
        }
      });

      char buf[8];
      (void)::read(server_pipe_[i][0], buf, 8);
    }
    running_ = true;

    ASSERT_EQ(0, ::flock(lock_fd, LOCK_UN));
    ::close(lock_fd);
  }

  void Stop() {
    if (!running_) {
      return;
    }

    char buf[8];
    for (int i = 0; i < 2; i++) {
      server_[i]->Stop();
      (void)::read(server_pipe_[i][0], buf, 8);
      server_thread_[i].join();
    }
    running_ = false;
  }

  void InitClient(subspace::Client &client, int server) {
    ASSERT_OK(client.Init(socket_[server]));
  }

  const std::string &Socket(int server) const { return socket_[server]; }

  toolbelt::FileDescriptor &BridgeNotificationPipe(int server) {
    return bridge_notification_pipe_[server];
  }

private:
  subspace::async::RuntimeEngine scheduler_[2];
  std::string socket_[2];
  int server_pipe_[2][2];
  std::unique_ptr<subspace::Server> server_[2];
  std::thread server_thread_[2];
  toolbelt::FileDescriptor bridge_notification_pipe_[2];
  bool running_ = false;
};

TEST_F(BridgeTest, Basic) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_basic", subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_basic", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_basic");

  // Send a message on the publisher.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_basic");

  // Receive the message on the subscriber.
  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());
}

TEST(BridgePortRangeTest, StaticBridgePortRangeSucceeds) {
  int first_port0 = 0;
  int last_port0 = 0;
  auto reserved0 =
      ReserveTcpPortRange(/*num_ports=*/4, &first_port0, &last_port0);
  ASSERT_FALSE(reserved0.empty());
  int first_port1 = 0;
  int last_port1 = 0;
  auto reserved1 =
      ReserveTcpPortRange(/*num_ports=*/4, &first_port1, &last_port1);
  ASSERT_FALSE(reserved1.empty());
  reserved0.clear();
  reserved1.clear();

  ScopedBridgeServerPair servers;
  servers.Start({BridgeRangeConfig{first_port0, last_port0},
                 BridgeRangeConfig{first_port1, last_port1}});

  subspace::Client client1;
  servers.InitClient(client1, 0);
  subspace::Client client2;
  servers.InitClient(client2, 1);

  absl::StatusOr<Publisher> pub =
      client1.CreatePublisher("/bridged_static_range",
                              subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_static_range",
                               subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  WaitForSubscribedMessage(servers.BridgeNotificationPipe(0),
                           "/bridged_static_range");

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "static", 6);
  ASSERT_OK(pub->PublishMessage(6));

  WaitForSubscribedMessage(servers.BridgeNotificationPipe(1),
                           "/bridged_static_range");

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
}

TEST(BridgePortRangeTest, FallsBackWhenConfiguredRangeIsBusy) {
  int occupied_port = 0;
  int last_port = 0;
  auto occupied =
      ReserveTcpPortRange(/*num_ports=*/1, &occupied_port, &last_port);
  ASSERT_FALSE(occupied.empty());

  ScopedBridgeServerPair servers;
  servers.Start({BridgeRangeConfig{},
                 BridgeRangeConfig{occupied_port, occupied_port,
                                   /*fallback_to_ephemeral=*/true}});

  subspace::Client client1;
  servers.InitClient(client1, 0);
  subspace::Client client2;
  servers.InitClient(client2, 1);

  absl::StatusOr<Publisher> pub =
      client1.CreatePublisher("/bridged_fallback_range",
                              subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_fallback_range",
                               subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  WaitForSubscribedMessage(servers.BridgeNotificationPipe(0),
                           "/bridged_fallback_range");

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "fallback", 8);
  ASSERT_OK(pub->PublishMessage(8));

  WaitForSubscribedMessage(servers.BridgeNotificationPipe(1),
                           "/bridged_fallback_range");

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(8, msg->length);
}

TEST(BridgePortRangeTest, FailsWhenConfiguredRangeIsBusyAndFallbackDisabled) {
  int occupied_port = 0;
  int last_port = 0;
  auto occupied =
      ReserveTcpPortRange(/*num_ports=*/1, &occupied_port, &last_port);
  ASSERT_FALSE(occupied.empty());

  ScopedBridgeServerPair servers;
  servers.Start({BridgeRangeConfig{},
                 BridgeRangeConfig{occupied_port, occupied_port,
                                   /*fallback_to_ephemeral=*/false}});

  subspace::Client client1;
  servers.InitClient(client1, 0);
  subspace::Client client2;
  servers.InitClient(client2, 1);

  absl::StatusOr<Publisher> pub =
      client1.CreatePublisher("/bridged_exhausted_range",
                              subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_exhausted_range",
                               subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "blocked", 7);
  ASSERT_OK(pub->PublishMessage(7));

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub, 5);
  ASSERT_FALSE(msg.ok());
}

TEST_F(BridgeTest, TwoSubs) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_twosubs", subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub1 =
      client2.CreateSubscriber("/bridged_twosubs", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub1);

  absl::StatusOr<Subscriber> sub2 =
      client2.CreateSubscriber("/bridged_twosubs", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub2);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_twosubs");

  // Send a message on the publisher.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_twosubs");

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
      client1.CreatePublisher("/bridged_basic_retire", subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false).SetNotifyRetirement(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_basic_retire", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_basic_retire");

  // Send a message on the publisher.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  toolbelt::FileDescriptor &recv_bridge_pipe = BridgeNotificationPipe(1);
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_basic_retire");

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

TEST(BridgeRetirementLifetimeTest, ShutdownWithOutstandingRetirementBridge) {
  ScopedBridgeServerPair servers;
  servers.Start({BridgeRangeConfig{}, BridgeRangeConfig{}});

  subspace::Client client1;
  servers.InitClient(client1, 0);
  subspace::Client client2;
  servers.InitClient(client2, 1);

  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/bridged_retire_shutdown",
      subspace::PublisherOptions()
          .SetSlotSize(256)
          .SetNumSlots(10)
          .SetLocal(false)
          .SetNotifyRetirement(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_retire_shutdown",
                               subspace::SubscriberOptions()
                                   .SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  WaitForSubscribedMessage(servers.BridgeNotificationPipe(0),
                           "/bridged_retire_shutdown");

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  ASSERT_OK(pub->PublishMessage(6));

  WaitForSubscribedMessage(servers.BridgeNotificationPipe(1),
                           "/bridged_retire_shutdown");

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  msg->Reset();

  servers.Stop();
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
      "/bridged_retire1",
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(kNumSlots).SetLocal(false));
  ASSERT_OK(local_pub);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub =
      client1.CreatePublisher("/bridged_retire1", subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(kNumSlots).SetLocal(false).SetNotifyRetirement(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_retire1", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_retire1");

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
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_retire1");

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
      client1.CreatePublisher("/bridged_retire2", subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(kNumSlots).SetLocal(false).SetNotifyRetirement(true));
  ASSERT_OK(pub);

  // Create publisher on the subscriber server.  This wil consume slot 0 on that
  // side.
  absl::StatusOr<Publisher> local_pub = client2.CreatePublisher(
      "/bridged_retire2",
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(kNumSlots).SetLocal(false));
  ASSERT_OK(local_pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_retire2", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  toolbelt::FileDescriptor &send_bridge_pipe = BridgeNotificationPipe(0);
  WaitForSubscribedMessage(send_bridge_pipe, "/bridged_retire2");

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
  WaitForSubscribedMessage(recv_bridge_pipe, "/bridged_retire2");

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
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false).SetChecksumSize(20).SetMetadataSize(50));
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_sizes", subspace::SubscriberOptions().SetMaxActiveMessages(2));
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
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false));
  ASSERT_OK(pub);
  ASSERT_EQ(64, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub = client2.CreateSubscriber(
      "/bridged_def_sizes", subspace::SubscriberOptions().SetMaxActiveMessages(2));
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
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false).SetMetadataSize(24));
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());
  ASSERT_EQ(24, pub->MetadataSize());

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_meta", subspace::SubscriberOptions().SetMaxActiveMessages(2));
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
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false).SetChecksumSize(20).SetMetadataSize(100));
  ASSERT_OK(pub);
  ASSERT_EQ(192, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/bridged_meta_lg", subspace::SubscriberOptions().SetMaxActiveMessages(2));
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
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false).SetUseSplitBuffers(true).SetSplitBuffersOverBridge(false));
  ASSERT_OK(pub);
  ASSERT_TRUE(pub->UsesSplitBuffers());

  absl::StatusOr<Subscriber> sub = client2.CreateSubscriber(
      "/bridged_split_source", subspace::SubscriberOptions().SetMaxActiveMessages(2));
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
      subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false).SetUseSplitBuffers(false).SetSplitBuffersOverBridge(true));
  ASSERT_OK(pub);
  ASSERT_FALSE(pub->UsesSplitBuffers());

  absl::StatusOr<Subscriber> sub = client2.CreateSubscriber(
      "/bridged_split_remote", subspace::SubscriberOptions().SetMaxActiveMessages(2));
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

// Self-validating payload sent over the bridge during the multi-threaded stress
// test.  Each message carries its channel index, a per-channel sequence number
// and a checksum computed over the whole payload.  A data race in the
// multi-threaded (Asio) bridge data plane that corrupted a message, mixed up
// channels, or torn-wrote a slot would show up as a checksum mismatch, a wrong
// channel id, or a non-monotonic sequence number on the receiving side.
namespace {

constexpr uint32_t kStressMagic = 0xC0FFEEu;
constexpr int kStressFillWords = 45; // sizeof(StressPayload) == 196 bytes.

struct StressPayload {
  uint32_t magic;
  uint32_t channel;
  uint32_t seq;
  uint32_t fill[kStressFillWords];
  uint32_t checksum;
};

uint32_t StressChecksum(const StressPayload &p) {
  uint32_t c = p.magic ^ (p.channel * 2654435761u) ^ (p.seq * 40503u);
  for (int i = 0; i < kStressFillWords; i++) {
    c = c * 31u + p.fill[i];
  }
  return c;
}

void FillStressPayload(StressPayload &p, uint32_t channel, uint32_t seq) {
  p.magic = kStressMagic;
  p.channel = channel;
  p.seq = seq;
  for (int i = 0; i < kStressFillWords; i++) {
    p.fill[i] = (channel << 24) ^ (seq * 2246822519u) ^ static_cast<uint32_t>(i);
  }
  p.checksum = StressChecksum(p);
}

// Drains "Subscribed" bridge notifications from `pipe` (the same length-prefixed
// framing WaitForSubscribedMessage uses) until every channel in `expected` has
// been seen, or `timeout` elapses.  poll() bounds the wait so a missing
// notification fails the test instead of hanging.  Returns true if all expected
// channels were observed.
bool WaitForBridgesEstablished(toolbelt::FileDescriptor &pipe,
                               std::set<std::string> expected,
                               std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!expected.empty()) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return false;
    }
    int ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
            .count());
    struct pollfd pfd = {pipe.Fd(), POLLIN, 0};
    int r = ::poll(&pfd, 1, ms);
    if (r < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (r == 0) {
      return false; // Timed out.
    }

    int32_t length = 0;
    absl::StatusOr<ssize_t> n = pipe.Read(&length, sizeof(length));
    if (!n.ok() || *n != sizeof(length)) {
      return false;
    }
    length = ntohl(length); // Length is network byte order.
    char buffer[4096];
    if (length <= 0 || length > static_cast<int32_t>(sizeof(buffer))) {
      return false;
    }
    n = pipe.Read(buffer, length);
    if (!n.ok()) {
      return false;
    }
    subspace::Subscribed subscribed;
    if (!subscribed.ParseFromArray(buffer, *n)) {
      continue;
    }
    expected.erase(subscribed.channel_name());
  }
  return true;
}

#if defined(__linux__) && defined(VMADDR_CID_LOCAL)
// Probe whether vsock loopback actually works here, using the *exact* bind the
// server's bridge listener uses: (VMADDR_CID_ANY, port 0), then a connect to it
// via VMADDR_CID_LOCAL.  vsock port 0 is a privileged port, so this bind fails
// with EPERM for an unprivileged process even when vsock loopback is otherwise
// usable; mirroring the server's bind here means the test skips precisely when
// the server would not be able to establish the bridge (e.g. unprivileged CI
// runners) and runs only where it actually works (e.g. running as root or in a
// VM).  Requires a Linux kernel with the vsock_loopback module available.
bool VsockLoopbackAvailable() {
  int lfd = ::socket(AF_VSOCK, SOCK_STREAM, 0);
  if (lfd < 0) {
    return false;
  }
  sockaddr_vm laddr;
  memset(&laddr, 0, sizeof(laddr));
  laddr.svm_family = AF_VSOCK;
  laddr.svm_cid = VMADDR_CID_ANY;
  laddr.svm_port = 0;
  bool ok = ::bind(lfd, reinterpret_cast<sockaddr *>(&laddr), sizeof(laddr)) ==
                0 &&
            ::listen(lfd, 1) == 0;
  socklen_t len = sizeof(laddr);
  if (ok &&
      ::getsockname(lfd, reinterpret_cast<sockaddr *>(&laddr), &len) != 0) {
    ok = false;
  }
  if (ok) {
    int cfd = ::socket(AF_VSOCK, SOCK_STREAM, 0);
    if (cfd < 0) {
      ok = false;
    } else {
      sockaddr_vm caddr;
      memset(&caddr, 0, sizeof(caddr));
      caddr.svm_family = AF_VSOCK;
      caddr.svm_cid = VMADDR_CID_LOCAL;
      caddr.svm_port = laddr.svm_port;
      ok = ::connect(cfd, reinterpret_cast<sockaddr *>(&caddr),
                     sizeof(caddr)) == 0;
      ::close(cfd);
    }
  }
  ::close(lfd);
  return ok;
}
#endif

} // namespace

// Drive many bridged channels concurrently while the servers run their Asio
// io_context on several threads, to flush out data races in the multi-threaded
// bridge path.  On the co backend num_asio_threads is ignored and the servers
// run single-threaded, so this still exercises the bridge but without the
// threading dimension.
TEST(BridgeStressTest, MultiThreadedBridging) {
  using namespace std::chrono;
  constexpr int kNumChannels = 8;
  constexpr int kNumMessages = 1000; // per channel
  // Size the channel so the whole burst fits without the ring buffer wrapping.
  // The point of this test is to stress the multi-threaded bridge data plane
  // (concurrent bridges on their own strands) and verify it never corrupts,
  // reorders or cross-wires messages - not to measure unreliable drop
  // behaviour.  With a small buffer a bridge transmitter that happens to be
  // starved for the whole burst under heavy contention can miss an entire
  // channel and deliver zero messages, which is legitimate unreliable
  // behaviour but makes the per-channel liveness check (received > 0) flaky.
  // A buffer that holds the full burst removes that flake while keeping the
  // checksum / ordering / cross-channel correctness checks fully meaningful.
  constexpr int kNumSlots = kNumMessages + 24;
  constexpr int kSlotSize = 256;
  constexpr int kNumAsioThreads = 4;

  static_assert(sizeof(StressPayload) <= kSlotSize,
                "payload must fit in a slot");

  ScopedBridgeServerPair servers;
  servers.Start({BridgeRangeConfig{}, BridgeRangeConfig{}}, kNumAsioThreads);

  auto channel_name = [](int ch) { return "/stress/" + std::to_string(ch); };

  std::atomic<int> subs_ready{0};
  std::atomic<int> pubs_ready{0};
  std::atomic<bool> go{false};
  std::atomic<bool> pubs_done{false};
  std::atomic<bool> failed{false};
  std::array<std::atomic<int>, kNumChannels> received{};

  std::vector<std::thread> sub_threads;
  std::vector<std::thread> pub_threads;

  // Subscribers (on server 1) come up first so the bridge can discover them
  // before any publishing starts.
  for (int ch = 0; ch < kNumChannels; ch++) {
    sub_threads.emplace_back([&, ch]() {
      subspace::Client client;
      if (absl::Status s = client.Init(servers.Socket(1)); !s.ok()) {
        ADD_FAILURE() << "subscriber Init: " << s;
        failed = true;
        subs_ready++;
        return;
      }
      absl::StatusOr<Subscriber> sub =
          client.CreateSubscriber(channel_name(ch), subspace::SubscriberOptions().SetMaxActiveMessages(8));
      if (!sub.ok()) {
        ADD_FAILURE() << "CreateSubscriber: " << sub.status();
        failed = true;
        subs_ready++;
        return;
      }
      subs_ready++;

      while (!go.load()) {
        std::this_thread::sleep_for(milliseconds(1));
      }

      int64_t last_seq = -1;
      auto last_rx = steady_clock::now();
      const auto start = steady_clock::now();
      for (;;) {
        absl::StatusOr<Message> msg = sub->ReadMessage();
        if (!msg.ok()) {
          ADD_FAILURE() << "channel " << ch << " ReadMessage: " << msg.status();
          failed = true;
          break;
        }
        if (msg->length == 0) {
          // No message available.  Stop once publishing is done and the channel
          // has been idle for a while, or after an overall deadline.
          if (pubs_done.load() && steady_clock::now() - last_rx > seconds(2)) {
            break;
          }
          if (steady_clock::now() - start > seconds(90)) {
            break;
          }
          (void)sub->Wait(milliseconds(100));
          continue;
        }
        if (msg->length != sizeof(StressPayload)) {
          ADD_FAILURE() << "channel " << ch << " bad length " << msg->length;
          failed = true;
          break;
        }
        StressPayload p;
        memcpy(&p, msg->buffer, sizeof(p));
        if (p.magic != kStressMagic || p.channel != static_cast<uint32_t>(ch) ||
            p.checksum != StressChecksum(p)) {
          ADD_FAILURE() << "channel " << ch
                        << " corrupt/cross-channel message: magic=" << p.magic
                        << " channel=" << p.channel << " seq=" << p.seq;
          failed = true;
          break;
        }
        // The bridge preserves order, so even with dropped messages the
        // received sequence numbers must be strictly increasing.
        if (static_cast<int64_t>(p.seq) <= last_seq) {
          ADD_FAILURE() << "channel " << ch << " out-of-order seq " << p.seq
                        << " after " << last_seq;
          failed = true;
          break;
        }
        last_seq = p.seq;
        received[ch]++;
        last_rx = steady_clock::now();
      }
    });
  }

  while (subs_ready.load() < kNumChannels) {
    std::this_thread::sleep_for(milliseconds(1));
  }

  // Publishers on server 0.
  for (int ch = 0; ch < kNumChannels; ch++) {
    pub_threads.emplace_back([&, ch]() {
      subspace::Client client;
      if (absl::Status s = client.Init(servers.Socket(0)); !s.ok()) {
        ADD_FAILURE() << "publisher Init: " << s;
        failed = true;
        pubs_ready++;
        return;
      }
      // Explicitly opt out of split buffers regardless of the global
      // --use_split_buffers default.  This stress test sizes the channel to
      // hold the whole burst (kNumSlots ~= kNumMessages) so the data plane can
      // be exercised without ring-buffer wrap.  Split buffers allocate one
      // shared-memory file per slot, so kNumChannels x kNumSlots files would
      // exhaust the process fd limit (macOS defaults are low) - and this test
      // targets the multi-threaded bridge data plane, not split buffers.
      absl::StatusOr<Publisher> pub = client.CreatePublisher(
          channel_name(ch), subspace::PublisherOptions().SetSlotSize(kSlotSize).SetNumSlots(kNumSlots).SetLocal(false).SetUseSplitBuffers(false));
      if (!pub.ok()) {
        ADD_FAILURE() << "CreatePublisher: " << pub.status();
        failed = true;
        pubs_ready++;
        return;
      }
      pubs_ready++;

      while (!go.load()) {
        std::this_thread::sleep_for(milliseconds(1));
      }

      for (int seq = 0; seq < kNumMessages; seq++) {
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        if (!buffer.ok()) {
          ADD_FAILURE() << "channel " << ch << " GetMessageBuffer: "
                        << buffer.status();
          failed = true;
          break;
        }
        StressPayload p;
        FillStressPayload(p, ch, seq);
        memcpy(*buffer, &p, sizeof(p));
        absl::StatusOr<const Message> st = pub->PublishMessage(sizeof(p));
        if (!st.ok()) {
          ADD_FAILURE() << "channel " << ch << " PublishMessage: "
                        << st.status();
          failed = true;
          break;
        }
        // Pace publishing so the unreliable bridge has time to forward each
        // message before its slot is recycled.  Without this the whole burst
        // can be overwritten before a (cooperative, single-threaded) co-backend
        // server forwards anything.  Spreading the burst over time also keeps
        // sustained, overlapping traffic on all channels, which is what we want
        // to exercise the multi-threaded Asio data plane.
        std::this_thread::sleep_for(microseconds(250));
      }
    });
  }

  while (pubs_ready.load() < kNumChannels) {
    std::this_thread::sleep_for(milliseconds(1));
  }
  // Wait until the send-side bridge transmitter is established for every channel
  // (server 0 emits a Subscribed notification once it discovers the remote
  // subscriber).  This removes the race where publishing starts before the
  // bridge exists and the early burst is dropped.
  std::set<std::string> expected;
  for (int ch = 0; ch < kNumChannels; ch++) {
    expected.insert(channel_name(ch));
  }
  ASSERT_TRUE(WaitForBridgesEstablished(servers.BridgeNotificationPipe(0),
                                        expected, seconds(30)))
      << "timed out waiting for bridges to be established";
  go.store(true);

  for (auto &t : pub_threads) {
    t.join();
  }
  pubs_done.store(true);
  for (auto &t : sub_threads) {
    t.join();
  }

  EXPECT_FALSE(failed.load());
  int total = 0;
  for (int ch = 0; ch < kNumChannels; ch++) {
    int n = received[ch].load();
    EXPECT_GT(n, 0) << "channel " << ch << " received no messages";
    total += n;
  }
  std::cerr << "Bridge stress: received " << total << " of "
            << (kNumChannels * kNumMessages) << " messages across "
            << kNumChannels << " channels (" << kNumAsioThreads
            << " asio threads/server)" << std::endl;
}

#if defined(__linux__) && defined(VMADDR_CID_LOCAL)
// End-to-end bridge over vsock instead of TCP.  Both servers run on the same
// host and advertise VMADDR_CID_LOCAL (the loopback CID), so the publisher-side
// bridge transmitter connects back to the subscriber-side listener over vsock.
// Discovery still runs over UDP/IP exactly as for the TCP bridge tests.
//
// Gated to Linux and skipped at runtime unless vsock loopback is usable (needs
// the vsock_loopback kernel module), so it is a no-op on machines without it.
TEST(VsockBridgeTest, BridgeOverVsockLoopback) {
  if (!VsockLoopbackAvailable()) {
    GTEST_SKIP() << "vsock loopback (VMADDR_CID_LOCAL) is not available; load "
                    "the vsock_loopback kernel module to run this test";
  }

  ScopedBridgeServerPair servers;
  servers.Start({BridgeRangeConfig{}, BridgeRangeConfig{}},
                /*num_asio_threads=*/1, /*use_vsock=*/true,
                /*vsock_cid=*/VMADDR_CID_LOCAL);

  subspace::Client client1;
  servers.InitClient(client1, 0);
  subspace::Client client2;
  servers.InitClient(client2, 1);

  // Non-local publisher on server 0; subscriber on server 1.  Bridging the two
  // requires a vsock data connection between the servers.
  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "/vsock_bridged", subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(10).SetLocal(false));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("/vsock_bridged", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  ASSERT_TRUE(WaitForBridgesEstablished(servers.BridgeNotificationPipe(0),
                                        {"/vsock_bridged"},
                                        std::chrono::seconds(30)))
      << "timed out waiting for vsock bridge to be established";

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "vsock!", 6);
  ASSERT_OK(pub->PublishMessage(6));

  absl::StatusOr<Message> msg = ReadMessageEventually(*sub);
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(0, memcmp("vsock!", msg->buffer, 6));
}
#endif // __linux__ && VMADDR_CID_LOCAL

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  subspace::SetDefaultUseSplitBuffers(absl::GetFlag(FLAGS_use_split_buffers));

  return RUN_ALL_TESTS();
}
