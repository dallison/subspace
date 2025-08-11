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

ABSL_FLAG(bool, start_server, true, "Start the subspace servers");
ABSL_FLAG(std::string, server, "", "Path to server executable");
ABSL_FLAG(std::string, log_level, "debug", "Log level");

void SignalHandler(int sig) { printf("Signal %d", sig); }

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;
using InetAddress = toolbelt::InetAddress;

class BridgeTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    constexpr int kDiscPorts[2] = {7000, 7001};
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
    ASSERT_TRUE(client.Init(Socket(server)).ok());
  }

  static const std::string &Socket(int i) { return socket_[i]; }

  static subspace::Server *Server(int i) { return server_[i].get(); }

private:
  static co::CoroutineScheduler scheduler_[2];
  static std::string socket_[2];
  static int server_pipe_[2][2];
  static std::unique_ptr<subspace::Server> server_[2];
  static std::thread server_thread_[2];
};

co::CoroutineScheduler BridgeTest::scheduler_[2];
std::string BridgeTest::socket_[2] = {"/tmp/subspace1", "/tmp/subspace2"};
int BridgeTest::server_pipe_[2][2];
std::unique_ptr<subspace::Server> BridgeTest::server_[2];
std::thread BridgeTest::server_thread_[2];

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

TEST_F(BridgeTest, Basic) {
  subspace::Client client1;
  InitClient(client1, 0);

  subspace::Client client2;
  InitClient(client2, 1);

  // Create a non-local publisher on client 1.
  absl::StatusOr<Publisher> pub = client1.CreatePublisher(
      "public", {.slot_size = 100, .num_slots = 10, .local = false});
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber> sub =
      client2.CreateSubscriber("public", {.max_active_messages = 2});
  ASSERT_TRUE(sub.ok());

  // Send a message on the publisher.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  // Receive the message on the subscriber.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
