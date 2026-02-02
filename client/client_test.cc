// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status_matchers.h"
#include "client/client.h"
#include "co/coroutine.h"
#include "server/server.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <cstdio>
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/resource.h>
#include <thread>

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");

void SignalHandler(int sig) {
  fprintf(stderr, "Signal %d", sig);
  std::cerr.flush();
  FILE *fp = fopen("/proc/self/maps", "r");
  for (;;) {
    int ch = fgetc(fp);
    if (ch == EOF) {
      break;
    }
    fputc(ch, stderr);
  }
  signal(sig, SIG_DFL);
  raise(sig);
}

void SigQuitHandler(int signum);

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
    ASSERT_OK(result);                                                         \
    std::move(*result);                                                        \
  })

#define ASSERT_OK(e) ASSERT_THAT(e, ::absl_testing::IsOk())

class ClientTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }

    printf("Starting Subspace server\n");
    // subspace::Server::CleanupFilesystem();
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    // The server will write to this pipe to notify us when it
    // has started and stopped.  This end of the pipe is blocking.
    (void)pipe(server_pipe_);

    server_ = std::make_unique<subspace::Server>(scheduler_, socket_, "", 0, 0,
                                                 /*local=*/true,
                                                 server_pipe_[1], 1, true);
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
    server_->CleanupAfterSession();
    // Remove the socket if it exists.
    (void)remove(socket_.c_str());
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
  void TearDown() override {}

  void InitClient(subspace::Client &client) {
    client.SetThreadSafe(true);
    ASSERT_OK(client.Init(Socket()));
  }

  static co::CoroutineScheduler &Scheduler() { return scheduler_; }

  static const std::string &Socket() { return socket_; }

  static subspace::Server *Server() { return server_.get(); }

private:
  static co::CoroutineScheduler scheduler_;
  static std::string socket_;
  static int server_pipe_[2];
  static std::unique_ptr<subspace::Server> server_;
  static std::thread server_thread_;
};

co::CoroutineScheduler ClientTest::scheduler_;
std::string ClientTest::socket_ = "/tmp/subspace";
int ClientTest::server_pipe_[2];
std::unique_ptr<subspace::Server> ClientTest::server_;
std::thread ClientTest::server_thread_;

void SigQuitHandler(int signum) {
  ClientTest::Scheduler().Show();
  signal(signum, SIG_DFL);
  raise(signum);
}

TEST_F(ClientTest, InetAddressSupportsAbslHash) {
  struct sockaddr_in addr = {
#if defined(__APPLE__)
    .sin_len = sizeof(int),
#endif
    .sin_family = AF_INET,
    .sin_port = htons(1234),
    .sin_addr = {.s_addr = htonl(0x12345678)}
  };

  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly({
      toolbelt::InetAddress(),
      toolbelt::InetAddress("1.2.3.4", 2),
      toolbelt::InetAddress("localhost", 3),
      toolbelt::InetAddress(addr),
  }));
}

TEST_F(ClientTest, Init) {
  subspace::Client client;
  InitClient(client);
}

TEST_F(ClientTest, CreatePublisher) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
  std::cerr << pub.status() << std::endl;
  ASSERT_OK(pub);
}

TEST_F(ClientTest, Resize1) {
  subspace::Client client;
  InitClient(client);
  // Initial slot size is 256.
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_OK(pub);

  // No resize.
  absl::StatusOr<void *> buffer1 = pub->GetMessageBuffer(256);
  ASSERT_OK(buffer1);
  ASSERT_EQ(256, pub->SlotSize());

  // Resize to new slot size is 512.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(300);
  ASSERT_OK(buffer2);
  ASSERT_EQ(512, pub->SlotSize());

  // Won't resize.
  absl::StatusOr<void *> buffer3 = pub->GetMessageBuffer(512, false);
  ASSERT_OK(buffer3);
  ASSERT_EQ(512, pub->SlotSize());
}

TEST_F(ClientTest, ResizeCallback) {
  subspace::Client client;
  InitClient(client);
  // Initial slot size is 256.
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_OK(pub);

  int num_resizes = 0;
  absl::Status status = pub->RegisterResizeCallback(
      [&num_resizes](Publisher *cb_pub, int32_t old_size,
                     int32_t new_size) -> absl::Status {
        num_resizes++;
        if (num_resizes < 2) {
          return absl::OkStatus();
        }
        return absl::InternalError("Unable to resize channel");
      });
  ASSERT_OK(status);

  // No resize.
  absl::StatusOr<void *> buffer1 = pub->GetMessageBuffer(256);
  ASSERT_OK(buffer1);
  ASSERT_EQ(256, pub->SlotSize());

  // Resize to new slot size is 512.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(300);
  ASSERT_OK(buffer2);
  ASSERT_EQ(512, pub->SlotSize());

  // Won't resize.
  absl::StatusOr<void *> buffer3 = pub->GetMessageBuffer(512);
  ASSERT_OK(buffer3);
  ASSERT_EQ(512, pub->SlotSize());

  ASSERT_EQ(1, num_resizes);

  // The resize callback will return an error because it only
  // allows one resize.
  absl::StatusOr<void *> buffer4 = pub->GetMessageBuffer(1000);
  ASSERT_FALSE(buffer4.ok());
  // For thread safety support we need to cancel the publish if we don't want to
  // send it.
  pub->CancelPublish();
}

TEST_F(ClientTest, CreatePublisherWithType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_OK(pub);
  ASSERT_EQ("foobar", pub->Type());
}

TEST_F(ClientTest, CreateVirtualPublisherWithType) {
  subspace::Client client;
  InitClient(client);
  {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 100, {.type = "foobar", .mux = "mainmux"});
    std::cerr << pub.status() << std::endl;
    ASSERT_OK(pub);
    ASSERT_EQ("foobar", pub->Type());
  }
  // Mux will be destructed here since there are no virtual
  // channels on it.

  // Create again with different type.
  {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 10, {.type = "foobar1", .mux = "mainmux"});
    ASSERT_OK(pub);
    ASSERT_EQ("foobar1", pub->Type());
  }
}

TEST_F(ClientTest, TooManyVirtualPublishers) {
  subspace::Client client;
  InitClient(client);
  constexpr int kMuxCapacity = 10;

  std::vector<Publisher> pubs;
  for (int i = 0; i < kMuxCapacity - 1; i++) {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 10, {.type = "foobar", .mux = "mainmux"});
    ASSERT_OK(pub);
    ASSERT_EQ("foobar", pub->Type());
    pubs.push_back(std::move(*pub));
  }
  // Publisher on the mux will fail
  absl::StatusOr<Publisher> mux_pub =
      client.CreatePublisher("mainmux", 256, 10, {.type = "foobar"});
  ASSERT_FALSE(mux_pub.ok());

  // One more virtual publisher will fail.
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "dave0", 256, 10, {.type = "foobar", .mux = "mainmux"});
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, CreateVirtualPublisherMuxMismatch) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "dave0", 256, 100, {.type = "foobar", .mux = "mainmux"});
  ASSERT_OK(pub);
  ASSERT_EQ("foobar", pub->Type());

  // Different mux.
  absl::StatusOr<Publisher> pub2 = client.CreatePublisher(
      "dave0", 256, 100, {.type = "foobar", .mux = "diffmux"});
  ASSERT_FALSE(pub2.ok());

  // No mux.
  absl::StatusOr<Publisher> pub3 =
      client.CreatePublisher("dave0", 256, 100, {.type = "foobar"});
  ASSERT_FALSE(pub3.ok());

  // Creating a channel with same name as mux should fail.
  absl::StatusOr<Publisher> pub4 = client.CreatePublisher("mainmux", 256, 100);
  ASSERT_FALSE(pub4.ok());
}

TEST_F(ClientTest, CreateMultipleVirtualPublisherSameVchan) {
  subspace::Client client;
  InitClient(client);
  std::vector<Publisher> pubs;
  int last_vchan = -1;
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 100, {.type = "foobar", .mux = "mainmux"});
    ASSERT_OK(pub);
    if (last_vchan == -1) {
      last_vchan = pub->VirtualChannelId();
    } else {
      ASSERT_EQ(last_vchan, pub->VirtualChannelId());
    }
    pubs.push_back(std::move(*pub));
  }
}

TEST_F(ClientTest, CreateMultipleVirtualPublisherDiffVchan) {
  subspace::Client client;
  InitClient(client);
  std::vector<Publisher> pubs;
  int last_vchan = -1;
  for (int i = 0; i < 10; i++) {
    std::string name = "dave" + std::to_string(i);
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        name, 256, 100, {.type = "foobar", .mux = "mainmux"});
    ASSERT_OK(pub);
    if (last_vchan == -1) {
      last_vchan = pub->VirtualChannelId();
    } else {
      ASSERT_NE(last_vchan, pub->VirtualChannelId());
    }
    pubs.push_back(std::move(*pub));
  }
}

TEST_F(ClientTest, PublisherTypeMismatch) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub1 = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("foobar"));

  ASSERT_OK(pub1);
  ASSERT_EQ("foobar", pub1->Type());

  absl::StatusOr<Publisher> pub2 = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("barfoo"));
  ASSERT_FALSE(pub2.ok());
}

TEST_F(ClientTest, TooManyPublishers) {
  subspace::Client client;
  InitClient(client);
  std::vector<Publisher> pubs;
  for (int i = 0; i < 9; i++) {
    absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }
  // One more will fail.
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, TooManySubscribers) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_OK(pub);

  std::vector<Subscriber> subs;
  for (int i = 0; i < 8; i++) {
    absl::StatusOr<Subscriber> sub = client.CreateSubscriber("dave0");
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }
  // One more will fail.
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber("dave0");
  ASSERT_FALSE(sub.ok());
}

TEST_F(ClientTest, TooManyVirtualSubscribers) {
  subspace::Client client;
  InitClient(client);
  constexpr int kNumSlots = 10;

  // 1 publisher.
  absl::StatusOr<Publisher> pub =
      client.CreatePublisher("dave0", 256, kNumSlots, {.mux = "foobar"});
  ASSERT_OK(pub);

  // 6 subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumSlots - 3; i++) {
    absl::StatusOr<Subscriber> sub =
        client.CreateSubscriber("dave0", {.mux = "foobar"});
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }

  // 1 Multiplexer subscriber.
  absl::StatusOr<Subscriber> mux_sub = client.CreateSubscriber("foobar");
  ASSERT_OK(mux_sub);

  // One more will fail.
  absl::StatusOr<Subscriber> sub =
      client.CreateSubscriber("dave0", {.mux = "foobar"});
  ASSERT_FALSE(sub.ok());
}

// The Push... tests might fail if there are insufficient open
// files configured on the system.
TEST_F(ClientTest, DISABLED_PushChannelLimit) {
  // There are a couple of channels created by the server itself;
  constexpr int kMaxChannels = subspace::kMaxChannels - 2;

  subspace::Client client;
  InitClient(client);
  for (int i = 0; i < kMaxChannels; i++) {
    char name[16];
    snprintf(name, sizeof(name), "dave_%d", i);
    absl::StatusOr<Publisher> pub = client.CreatePublisher(name, 256, 10);
    if (!pub.ok()) {
      printf("%s: %s\n", name, pub.status().ToString().c_str());
    }
    ASSERT_OK(pub);
  }
  // One more will fail.
  absl::StatusOr<Publisher> pub = client.CreatePublisher("mint", 256, 10);
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, DISABLED_PushSubscriberLimit) {
  subspace::Client client;
  InitClient(client);
  for (int i = 0; i < subspace::kMaxSlotOwners; i++) {
    absl::StatusOr<Subscriber> sub = client.CreateSubscriber("dave0");
    if (!sub.ok()) {
      printf("%s\n", sub.status().ToString().c_str());
    }
    ASSERT_OK(sub);
  }
  // One more will fail.
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber("dave0");
  ASSERT_FALSE(sub.ok());
}

TEST_F(ClientTest, BadPublisherParameters) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub1 = client.CreatePublisher("dave0", 256, 10);
  ASSERT_OK(pub1);

  // Different slot size - this is fine due to channel resizing and we are not
  // fixed size.
  absl::StatusOr<Publisher> pub2 = client.CreatePublisher("dave0", 255, 10);
  ASSERT_OK(pub2);

  // Different num slots
  absl::StatusOr<Publisher> pub3 = client.CreatePublisher("dave0", 256, 9);
  ASSERT_TRUE(pub3.ok());

  // Fixed size.
  absl::StatusOr<Publisher> pub4 =
      client.CreatePublisher("dave1", 256, 10, {.fixed_size = true});
  ASSERT_OK(pub4);

  // Not fixed size - mismatch fixed size option, same slot size.
  absl::StatusOr<Publisher> pub5 =
      client.CreatePublisher("dave1", 256, 10, {.fixed_size = false});
  ASSERT_FALSE(pub5.ok());

  // Different slot size - we are fixed size, this will fail.
  absl::StatusOr<Publisher> pub6 =
      client.CreatePublisher("dave1", 512, 10, {.fixed_size = true});
  ASSERT_FALSE(pub6.ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriber) {
  subspace::Client client;
  InitClient(client);
  auto p = client.CreatePublisher("dave1", 256, 10);
  ASSERT_OK(p);

  auto s = client.CreateSubscriber("dave1");
  ASSERT_OK(s);
}

TEST_F(ClientTest, CreateVirtualPublisherThenSubscriber) {
  subspace::Client client;
  InitClient(client);
  auto p = client.CreatePublisher("dave1", 256, 10, {.mux = "foobar"});
  ASSERT_OK(p);

  auto s = client.CreateSubscriber("dave1", {.mux = "foobar"});
  ASSERT_OK(s);
}

TEST_F(ClientTest, CreateVirtualSubscriberThenPublisher) {
  subspace::Client client;
  InitClient(client);

  auto s = client.CreateSubscriber("dave1", {.mux = "foobar"});
  ASSERT_OK(s);
  auto p = client.CreatePublisher("dave1", 256, 10, {.mux = "foobar"});
  ASSERT_OK(p);
}

TEST_F(ClientTest, CreateVirtualPublisherThenSubscriberMuxMismatch) {
  subspace::Client client;
  InitClient(client);
  auto p1 = client.CreatePublisher("dave1", 256, 10, {.mux = "foobar"});
  ASSERT_OK(p1);

  auto s1 = client.CreateSubscriber("dave1", {.mux = "foobar"});
  ASSERT_OK(s1);

  // Different mux.
  auto s2 = client.CreateSubscriber("dave1", {.mux = "diffmux"});
  ASSERT_FALSE(s2.ok());

  // No mux.
  auto s3 = client.CreateSubscriber("dave1");
  ASSERT_FALSE(s3.ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriberSameType) {
  subspace::Client client;
  InitClient(client);
  auto p = client.CreatePublisher(
      "dave1", 256, 10, subspace::PublisherOptions().SetType("foobar"));
  std::cerr << p.status() << std::endl;
  ASSERT_OK(p);
  auto s = client.CreateSubscriber(
      "dave1", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_OK(s);
}

TEST_F(ClientTest, CreatePublisherThenSubscriberWrongType) {
  subspace::Client client;
  InitClient(client);
  auto p = client.CreatePublisher(
      "dave1", 256, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_OK(p);
  auto s = client.CreateSubscriber(
      "dave1", subspace::SubscriberOptions().SetType("barfoo"));
  ASSERT_FALSE(s.ok());
}

TEST_F(ClientTest, CreateSubscriber) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber("dave2");
  ASSERT_OK(s);
}

TEST_F(ClientTest, FileDescriptors) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub1 = client.CreatePublisher("dave0", 256, 10);
  absl::StatusOr<Publisher> pub2 = client.CreatePublisher(
      "dave1", 256, 10, subspace::PublisherOptions().SetReliable(true));
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber("dave1");

  auto pub1_fd = pub1->GetFileDescriptor();
  auto pub2_fd = pub2->GetFileDescriptor();
  auto sub_fd = sub->GetFileDescriptor();

  ASSERT_FALSE(pub1_fd.Valid());
  ASSERT_TRUE(pub2_fd.Valid());
  ASSERT_TRUE(sub_fd.Valid());
}

TEST_F(ClientTest, CreateSubscriberWithType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_OK(s);
  ASSERT_EQ("foobar", s->Type());
}

TEST_F(ClientTest, MismatchedSubscriberType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s1 = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_OK(s1);
  ASSERT_EQ("foobar", s1->Type());

  absl::StatusOr<Subscriber> s2 = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("barfoo"));
  ASSERT_FALSE(s2.ok());
}

TEST_F(ClientTest, CreateSubscriberThenPublisher) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber("dave3");
  ASSERT_OK(s);

  absl::StatusOr<Publisher> p = client.CreatePublisher("dave3", 300, 10);
  ASSERT_OK(p);

  auto &counters = s->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
}

TEST_F(ClientTest, CreateSubscriberThenPublisherSameType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber(
      "dave3", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_OK(s);

  absl::StatusOr<Publisher> p = client.CreatePublisher(
      "dave3", 300, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_OK(p);

  auto &counters = s->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
}

TEST_F(ClientTest, CreateSubscriberThenPublisherWrongType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber(
      "dave3", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_OK(s);

  absl::StatusOr<Publisher> p = client.CreatePublisher(
      "dave3", 300, 10, subspace::PublisherOptions().SetType("bar"));
  ASSERT_FALSE(p.ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriberDifferentClient) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  ASSERT_OK(pub_client.CreatePublisher("dave4", 256, 10));
  ASSERT_OK(sub_client.CreateSubscriber("dave4"));
}

TEST_F(ClientTest, PublishSingleMessage) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave5", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);
}

TEST_F(ClientTest, PublishSingleMessageAndRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessageWithPrefixAndRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .type = "foobar"});
  ASSERT_OK(pub);

  ASSERT_EQ("foobar", pub->TypeView());
  ASSERT_FALSE(pub->IsReliable());
  ASSERT_FALSE(pub->IsFixedSize());
  ASSERT_EQ(256, pub->SlotSize());
  ASSERT_EQ(10, pub->NumSlots());
  ASSERT_EQ(0, pub->CurrentSlotId());
  ASSERT_EQ("", pub->Mux());
  std::stringstream ss;
  pub->DumpSlots(ss);
  ASSERT_NE("", ss.str());
  ASSERT_NE("", pub->BufferSharedMemoryName(0));
  ASSERT_EQ(0, pub->CurrentSlotId());
  ASSERT_NE(nullptr, pub->CurrentSlot());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);

  auto prefix = reinterpret_cast<subspace::MessagePrefix *>(*buffer) - 1;
  prefix->SetIsBridged();
  prefix->timestamp = 1234;

  absl::StatusOr<const Message> pub_status = pub->PublishMessageWithPrefix(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  auto prefix2 =
      reinterpret_cast<const subspace::MessagePrefix *>(msg->buffer) - 1;
  ASSERT_TRUE(prefix2->IsBridged());
  ASSERT_EQ(1234, prefix2->timestamp);
  ASSERT_EQ(1, sub->CurrentOrdinal());
  ASSERT_EQ("dave6", sub->Name());
  ASSERT_EQ("foobar", sub->TypeView());
  ASSERT_FALSE(sub->IsReliable());
  ASSERT_EQ(256, sub->SlotSize());
  ASSERT_EQ(10, sub->NumSlots());
  ASSERT_EQ("", sub->Mux());
  ss.clear();
  sub->DumpSlots(ss);
  ASSERT_NE("", ss.str());
  ASSERT_EQ(1234, sub->Timestamp());
  ASSERT_EQ(1234, msg->timestamp);
  ASSERT_EQ(-1, sub->VirtualChannelId());
  ASSERT_EQ(-1, sub->ConfiguredVchanId());
  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessageAndReadNewest) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);

  // Publish a message.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_OK(sub);

  // Another message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar2", 7);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
    ASSERT_OK(pub_status);
  }

  // Read the newest message.
  absl::StatusOr<Message> msg =
      sub->ReadMessage(subspace::ReadMode::kReadNewest);
  ASSERT_OK(msg);
  ASSERT_EQ(7, msg->length);

  // There are no more messages since we read the newest one.
  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessageAndReadWithActivation) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .activate = true});
  ASSERT_OK(pub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", {.max_active_messages = 2, .pass_activation = true});
  ASSERT_OK(sub);

  // Read activation message.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(1, msg->length);
  ASSERT_TRUE(msg->is_activation);

  // Read the actual message.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessageAndReadWithCallback) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_OK(sub);

  auto status = sub->RegisterMessageCallback(
      [](Subscriber *s, Message msg) { ASSERT_EQ(6, msg.length); });
  ASSERT_OK(status);

  status = sub->ProcessAllMessages();
  ASSERT_OK(status);

  status = sub->UnregisterMessageCallback();
  ASSERT_OK(status);
}

TEST_F(ClientTest, PublishSingleMessageAndReadWithPlugin) {
  ASSERT_OK(Server()->LoadPlugin("NOP", "plugins/nop_plugin.so"));
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
  ASSERT_OK(Server()->UnloadPlugin("NOP"));
}

TEST_F(ClientTest, VirtualPublishSingleMessageAndRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", 256, 10, {.mux = "mainmux"});
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.mux = "mainmux"});
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, VirtualPublishMultiple) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  std::vector<Publisher> pubs;
  std::vector<Subscriber> subs;
  // Create 10 publishers on mux mainmux.
  for (int i = 0; i < 10; i++) {
    std::string name = "dave" + std::to_string(i);
    absl::StatusOr<Publisher> pub =
        pub_client.CreatePublisher(name, 256, 100, {.mux = "mainmux"});
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create 10 subscribers for the same mux and channel names.
  for (int i = 0; i < 10; i++) {
    std::string name = "dave" + std::to_string(i);
    absl::StatusOr<Subscriber> sub =
        sub_client.CreateSubscriber(name, {.mux = "mainmux"});
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }

  // Create a subscriber to the multiplexer.
  absl::StatusOr<Subscriber> mux_sub = sub_client.CreateSubscriber("mainmux");
  ASSERT_OK(mux_sub);

  // Publish a message on all 10 publishers
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pubs[i].PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read all messages using the virtual subscribers.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(subs[i].Wait());
    absl::StatusOr<Message> msg = subs[i].ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(6, msg->length);
    ASSERT_EQ(i, msg->vchan_id);
  }

  // Make sure mux is triggered.
  ASSERT_OK(mux_sub->Wait());

  // Read the all messages using the multiplexer subscriber.
  // This hasn't seen any messages yet so will see them all.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Message> msg = mux_sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(6, msg->length);
    ASSERT_EQ(i, msg->vchan_id);
  }

  // Read another and get 0 length.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Message> msg = subs[i].ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(0, msg->length);
  }
}

TEST_F(ClientTest, PublishAndResize) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_OK(buffer2);
  ASSERT_EQ(4096, pub->SlotSize());

  auto &pub_buffers = pub->GetBuffers();
  ASSERT_EQ(2, pub_buffers.size());

  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_OK(pub_status2);

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_OK(msg2);
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());

  auto &sub_buffers = sub->GetBuffers();
  ASSERT_EQ(2, sub_buffers.size());
}

TEST_F(ClientTest, PublishVirtualAndResize) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", 256, 10, {.mux = "mainmux"});
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", {.max_active_messages = 2, .mux = "mainmux"});
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_OK(buffer2);
  ASSERT_EQ(4096, pub->SlotSize());

  auto &pub_buffers = pub->GetBuffers();
  ASSERT_EQ(2, pub_buffers.size());

  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_OK(pub_status2);

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_OK(msg2);
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());

  auto &sub_buffers = sub->GetBuffers();
  ASSERT_EQ(2, sub_buffers.size());
}

TEST_F(ClientTest, PublishAndResize2) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_OK(buffer2);
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_OK(pub_status2);

  // Now create subscriber and read both messages.
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_OK(msg2);
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());
}

TEST_F(ClientTest, PublishAndResizeUnmapBuffers) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Publish bigger messages.  This will cause a resize.  We take
  // all the slots to free up buffer index 0 and it will be
  // unmapped
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
    ASSERT_OK(buffer2);
    ASSERT_EQ(4096, pub->SlotSize());
    memcpy(*buffer2, "barfoofoobar", 12);

    absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
    ASSERT_OK(pub_status2);
  }

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_TRUE(sub.ok()) << sub.status();

  // Create another publisher after resize.
  absl::StatusOr<Publisher> pub2 = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub2);

  // Read all messages.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
  }

  {
    auto &pub_buffers = pub->GetBuffers();
    ASSERT_EQ(2, pub_buffers.size());
    ASSERT_EQ(nullptr, pub_buffers[0]->buffer);

    auto &sub_buffers = sub->GetBuffers();
    ASSERT_EQ(2, sub_buffers.size());
    ASSERT_EQ(nullptr, sub_buffers[0]->buffer);
    ASSERT_NE(nullptr, sub_buffers[1]->buffer);
  }

  // Publish one more that will check for free buffers and will unmap
  // them.  We only check for unused buffers when we will also notify
  // subscribers, which is done when we are publishing a message
  // immediatly after one that has been seen by subscribers.
  absl::StatusOr<void *> buffer3 = pub->GetMessageBuffer(4000);
  ASSERT_OK(buffer3);
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer3, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status3 = pub->PublishMessage(12);
  ASSERT_OK(pub_status3);

  // Check that we've unmapped the unused buffer in the publisher now.
  {
    auto &pub_buffers = pub->GetBuffers();
    ASSERT_EQ(2, pub_buffers.size());
    ASSERT_EQ(nullptr, pub_buffers[0]->buffer);
    ASSERT_NE(nullptr, pub_buffers[1]->buffer);
  }
}

TEST_F(ClientTest, PublishAndResizeSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  // First create subscriber.
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_OK(sub);
  ASSERT_EQ(0, sub->SlotSize()); // No buffers yet.

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_OK(buffer2);
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_OK(pub_status2);

  // Now read both messages.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_OK(msg2);
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());
}

TEST_F(ClientTest, PublishVirtualAndResizeSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  // First create subscriber.
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", {.max_active_messages = 2, .mux = "mainmux"});
  ASSERT_OK(sub);
  ASSERT_EQ(0, sub->SlotSize()); // No buffers yet.

  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", 256, 10, {.mux = "mainmux"});
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_OK(buffer2);
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_OK(pub_status2);

  // Now read both messages.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_OK(msg2);
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());
}

TEST_F(ClientTest, PublishAndResizeSubscriberConcurrently) {
  std::string channel_name = "growing_channel";
  subspace::Client client1;
  subspace::Client client2;
  ASSERT_OK(client1.Init(Socket()));
  ASSERT_OK(client2.Init(Socket()));

  std::atomic<bool> publisher_finished{false};

  auto t1 = std::thread([&]() {
    auto client1_pub = *client1.CreatePublisher(
        channel_name, {.slot_size = 1, .num_slots = 4});
    for (int i = 1; i < 24; i++) {
      std::size_t size = std::pow(2, i);
      auto buffer = client1_pub.GetMessageBuffer(size);
      std::memset(*buffer, i, size);
      ASSERT_OK(client1_pub.PublishMessage(size));
    }
    publisher_finished = true;
    std::cerr << "publisher finished\n";
  });
  auto t2 = std::thread([&]() {
    auto client2_sub = *client2.CreateSubscriber(channel_name);
    while (publisher_finished == false) {
      struct pollfd fd = client2_sub.GetPollFd();
      int e = ::poll(&fd, 1, -1);
      ASSERT_EQ(1, e);
      while (true) {
        auto message = *client2_sub.ReadMessage();
        size_t size = message.length;
        if (size == 0) {
          break;
        } else {
          std::cout << size << std::endl;
        }
      }
    }
    std::cerr << "subscriber done\n";
  });

  t1.join();
  t2.join();
}

TEST_F(ClientTest, PublishConcurrentlyFromOneClientToOneSubscriber) {
  std::string channel_name = "checkin_channel";
  subspace::Client sub_client;
  ASSERT_OK(sub_client.Init(Socket()));
  auto sub = *sub_client.CreateSubscriber(channel_name);

  constexpr int kNumPublishers = 100;
  std::vector<Publisher> pubs;
  pubs.reserve(kNumPublishers);
  subspace::Client pub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  for (int i = 0; i < kNumPublishers; ++i) {
    pubs.emplace_back(*pub_client.CreatePublisher(
        channel_name,
        {.slot_size = 256, .num_slots = 2 * kNumPublishers + 16}));
  }

  std::vector<std::thread> pub_threads;
  pub_threads.reserve(kNumPublishers);
  for (int i = 0; i < kNumPublishers; ++i) {
    pub_threads.emplace_back(std::thread([&pubs, i]() {
      std::array<char, 16> msg = {};
      auto size = std::snprintf(msg.data(), msg.size(), "M%d", i);
      auto buffer = pubs[i].GetMessageBuffer(size);
      std::memcpy(*buffer, msg.data(), size);
      ASSERT_OK(pubs[i].PublishMessage(size));
    }));
  }

  for (auto &t : pub_threads) {
    t.join();
  }

  std::vector<std::string> all_recv_msgs;
  all_recv_msgs.reserve(kNumPublishers);
  while (true) {
    auto message = *sub.ReadMessage();
    size_t size = message.length;
    if (size == 0) {
      break;
    }
    all_recv_msgs.emplace_back(std::string(
        reinterpret_cast<const char *>(message.buffer), message.length));
  }
  EXPECT_EQ(all_recv_msgs.size(), kNumPublishers);
  std::sort(all_recv_msgs.begin(), all_recv_msgs.end());
  auto last_uniq = std::unique(all_recv_msgs.begin(), all_recv_msgs.end());
  EXPECT_EQ(last_uniq - all_recv_msgs.begin(), kNumPublishers);
}

TEST_F(ClientTest, PublishConcurrentlyToOneSubscriber) {
  std::string channel_name = "checkin_channel";
  subspace::Client sub_client;
  ASSERT_OK(sub_client.Init(Socket()));
  auto sub = *sub_client.CreateSubscriber(channel_name);

  std::vector<std::thread> pub_threads;
  constexpr int kNumPublishers = 100;
  pub_threads.reserve(kNumPublishers);
  for (int i = 0; i < kNumPublishers; ++i) {
    pub_threads.emplace_back(std::thread([&channel_name, i]() {
      // We have a backlog of 10 hardcoded for the subscriber's listen socket.
      // We will get a connection refused if we exceed this so use a retry loop
      // with a delay if we get errors on connection.  Happens on MacOS.
      subspace::Client pub_client;
      bool connected = false;
      for (int i = 0; i < 100; i++) {
        if (pub_client.Init(Socket()).ok()) {
          connected = true;
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      ASSERT_TRUE(connected);
      auto pub = *pub_client.CreatePublisher(
          channel_name,
          {.slot_size = 256, .num_slots = 2 * kNumPublishers + 16});
      std::array<char, 16> msg = {};
      auto size = std::snprintf(msg.data(), msg.size(), "M%d", i);
      auto buffer = pub.GetMessageBuffer(size);
      std::memcpy(*buffer, msg.data(), size);
      ASSERT_OK(pub.PublishMessage(size));
    }));
  }

  for (auto &t : pub_threads) {
    t.join();
  }

  std::vector<std::string> all_recv_msgs;
  all_recv_msgs.reserve(kNumPublishers);
  while (true) {
    auto message = *sub.ReadMessage();
    size_t size = message.length;
    if (size == 0) {
      break;
    }
    all_recv_msgs.emplace_back(std::string(
        reinterpret_cast<const char *>(message.buffer), message.length));
  }
  EXPECT_EQ(all_recv_msgs.size(), kNumPublishers);
  std::sort(all_recv_msgs.begin(), all_recv_msgs.end());
  auto last_uniq = std::unique(all_recv_msgs.begin(), all_recv_msgs.end());
  EXPECT_EQ(last_uniq - all_recv_msgs.begin(), kNumPublishers);
}

TEST_F(ClientTest, PublishSingleMessagePollAndReadSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave7");
  ASSERT_OK(sub);

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave7", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  struct pollfd fd = sub->GetPollFd();

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessagePollAndReadPublisherFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  struct pollfd fd = sub->GetPollFd();

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishMultipleMessagePollAndReadPublisherFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_OK(sub);

  for (int i = 0; i < 9; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
    ASSERT_OK(pub_status);
  }
  struct pollfd fd = sub->GetPollFd();

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  for (;;) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    if (msg->length == 0) {
      break;
    }
  }
}

TEST_F(ClientTest, ReliablePublisher1) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "rel_dave", 32, 5, subspace::PublisherOptions().SetReliable(true));
  ASSERT_OK(pub);
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      "rel_dave", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_OK(sub);

  auto &counters = pub->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
  ASSERT_EQ(1, counters.num_reliable_pubs);
  ASSERT_EQ(1, counters.num_reliable_subs);

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read the message from reliable subscriber.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_EQ(nullptr, *buffer);
  }

  msg->Release();

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    struct pollfd fd = pub->GetPollFd();
    c->Wait(fd.fd);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  });

  // Read messages in coroutine.
  co::Coroutine c2(machine, [&sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      std::cerr << msg.status() << std::endl;
      ASSERT_OK(msg);
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(0, msg->length);
  });

  machine.Run();
}

TEST_F(ClientTest, ReliablePublisher2) {
  subspace::Client client;
  InitClient(client);

  // Create subscriber before the publisher.  The subscriber
  // will have to call the server to get the publisher's trigger fd when it
  // calls ReadMessage.
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      "rel_dave", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_OK(sub);

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "rel_dave", 32, 5, subspace::PublisherOptions().SetReliable(true));
  ASSERT_OK(pub);

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read the message from reliable subscriber.  This will make a server
  // call to get the reliable publishers.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_EQ(nullptr, *buffer);
  }

  msg->Release();

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    absl::StatusOr<struct pollfd> fd = pub->GetPollFd();
    ASSERT_OK(fd);
    c->Wait(fd->fd);
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  });

  // Read another message in coroutine.
  co::Coroutine c2(machine, [&sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(0, msg->length);
  });
  machine.Run();
}

TEST_F(ClientTest, ReliablePublisherActivation) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "rel_dave",
      subspace::PublisherOptions().SetSlotSize(32).SetNumSlots(5).SetReliable(
          true));
  ASSERT_OK(pub);
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      "rel_dave", {.reliable = true, .pass_activation = true});
  ASSERT_OK(sub);

  auto &counters = pub->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
  ASSERT_EQ(1, counters.num_reliable_pubs);
  ASSERT_EQ(1, counters.num_reliable_subs);

  // Read the activation.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(1, msg->length);
  ASSERT_TRUE(msg->is_activation);

  msg->Release();

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read the message from reliable subscriber.
  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_EQ(nullptr, *buffer);
  }

  msg->Release();

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    struct pollfd fd = pub->GetPollFd();
    c->Wait(fd.fd);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  });

  // Read messages in coroutine.
  co::Coroutine c2(machine, [&sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      std::cerr << msg.status() << std::endl;
      ASSERT_OK(msg);
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(0, msg->length);
  });

  machine.Run();
}

TEST_F(ClientTest, DroppedMessage) {
  subspace::Client client;
  InitClient(client);

  absl::StatusOr<Subscriber> sub = client.CreateSubscriber("rel_dave");
  ASSERT_OK(sub);

  int num_dropped_messages = 0;
  absl::Status status = sub->RegisterDroppedMessageCallback(
      [&num_dropped_messages, &sub](Subscriber *s, int64_t num_dropped) {
        ASSERT_EQ(*sub, *s);
        num_dropped_messages += num_dropped;
      });
  ASSERT_OK(status);

  absl::StatusOr<Publisher> pub = client.CreatePublisher("rel_dave", 32, 5);
  ASSERT_OK(pub);

  // 5 slots. Fill 4 of them with messages.  Slots 0 to 3 will contain
  // messages and the publisher will have slot 4
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read one message. This will be in slot 0.
  {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(6, msg->length);
  }

  // 4 slots contain messages. The subscriber has slot 0.  The publisher
  // has slot 4.   Publish another 4 messages.
  // The publisher slots and ordinals are:
  // slot: 0: 1    <- subscriber is here.
  // slot: 1: 2
  // slot: 2: 3
  // slot: 3: 4
  // slot: 4: 5   <- publisher will take this slot
  // slot: 1: 6   <- next slot seen by subscriber
  // slot: 2: 7
  // slot: 3: 8
  //
  // The subscriber sees:
  // old slot: 0: 1, new slot: 1: 6
  // old slot: 1: 6, new slot: 2: 7
  // old slot: 2: 7, new slot: 3: 8
  //
  // So on the first read we drop 4 messages (expecting ordinal 2 but get 6).

  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read all messages in channel.
  for (;;) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    if (msg->length == 0) {
      break;
    }
  }
  ASSERT_EQ(4, num_dropped_messages);
}

TEST_F(ClientTest, PublishSingleMessageAndReadSharedPtr) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", subspace::SubscriberOptions().SetMaxActiveMessages(3));
  ASSERT_OK(sub);

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub->ReadMessage<const char>();
  ASSERT_OK(p);
  const auto &ptr = *p;
  ASSERT_TRUE(static_cast<bool>(ptr));
  ASSERT_STREQ("foobar", ptr.get());

  ASSERT_EQ(2, ptr.use_count());

  // Copy the shared ptr using copy constructor.
  subspace::shared_ptr<const char> p2(ptr);
  ASSERT_EQ(3, ptr.use_count());
  ASSERT_EQ(3, p2.use_count());

  // Copy using copy operator.
  subspace::shared_ptr<const char> p3 = ptr;
  ASSERT_EQ(4, ptr.use_count());
  ASSERT_EQ(4, p2.use_count());
  ASSERT_EQ(4, p3.use_count());

  // Move p3 to p4.
  subspace::shared_ptr<const char> p4 = std::move(p3);
  ASSERT_FALSE(static_cast<bool>(p3));
  ASSERT_EQ(4, ptr.use_count());
  ASSERT_EQ(4, p2.use_count());
  ASSERT_EQ(0, p3.use_count());
  ASSERT_EQ(4, p4.use_count());
}

TEST_F(ClientTest, Publish2Message2AndReadSharedPtrs) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_OK(pub);

  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 7);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
    ASSERT_OK(pub_status);
  }

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub->ReadMessage<const char>();
  ASSERT_OK(p);
  ASSERT_TRUE(static_cast<bool>(*p));
  ASSERT_STREQ("foobar", p->get());

  // Create a weak_ptr from p.
  subspace::weak_ptr<const char> w(*p);
  ASSERT_FALSE(w.expired());

  absl::StatusOr<subspace::shared_ptr<const char>> p2 =
      sub->ReadMessage<const char>();

  ASSERT_OK(p2);
  ASSERT_TRUE(static_cast<bool>(*p2));
  ASSERT_STREQ("foobar", p2->get());

  p->reset();

  // weak_ptr is still valid.
  ASSERT_FALSE(w.expired());

  // Publish some more messages to reuse the weak pointer's slot.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // weak_ptr will have expired.
  ASSERT_TRUE(w.expired());

  // Number of active messages: 1
  ASSERT_EQ(1, sub->NumActiveMessages());
  {
    // Another weak ptr from the valid shared ptr.
    subspace::weak_ptr<const char> w2(*p2);
    // Shared ptr from weak ptr and destruct it.
    subspace::shared_ptr<const char> p2(w2);
    // Number of active messages: 2
    ASSERT_EQ(1, sub->NumActiveMessages());

    ASSERT_EQ(3, p2.use_count());
  }
  // Number of active messages: 1
}

TEST_F(ClientTest, FindMessage) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_OK(sub);

  std::vector<subspace::Message> msgs;
  for (int i = 0; i < 9; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
    ASSERT_OK(pub_status);
    msgs.push_back(std::move(*pub_status));
  }

  // Find an unknown message lower than all others.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(12345678);
    ASSERT_OK(m);
    ASSERT_EQ(nullptr, m->buffer);
  }

  // Find an unknown message higher than all others.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(-1);
    ASSERT_OK(m);
    ASSERT_EQ(nullptr, m->buffer);
  }

  // Find a known message.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[4].timestamp);
    ASSERT_OK(m);
    ASSERT_EQ(msgs[4].timestamp, m->timestamp);
    ASSERT_EQ(msgs[4].length, m->length);
    ASSERT_EQ(msgs[4].ordinal, m->ordinal);
  }

  // Find another known message.  This will change ownership of the subscriber's
  // slot.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[7].timestamp);
    ASSERT_OK(m);
    ASSERT_EQ(msgs[7].timestamp, m->timestamp);
    ASSERT_EQ(msgs[7].length, m->length);
    ASSERT_EQ(msgs[7].ordinal, m->ordinal);
  }

  // Find first message.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[0].timestamp);
    ASSERT_OK(m);
    ASSERT_EQ(msgs[0].timestamp, m->timestamp);
    ASSERT_EQ(msgs[0].length, m->length);
    ASSERT_EQ(msgs[0].ordinal, m->ordinal);
  }

  // Find last message.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[8].timestamp);
    ASSERT_OK(m);
    ASSERT_EQ(msgs[8].timestamp, m->timestamp);
    ASSERT_EQ(msgs[8].length, m->length);
    ASSERT_EQ(msgs[8].ordinal, m->ordinal);
  }
}

TEST_F(ClientTest, Mikael) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("mik", 1024, 32);
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("mik");
  ASSERT_OK(sub);

  std::vector<std::string> sent_msgs;
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
    ASSERT_OK(pub_status);
    sent_msgs.push_back(std::string(buf, len + 1));
  }

  std::vector<std::string> received_msgs;

  for (;;) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    if (msg->length == 0) {
      break;
    }
    received_msgs.push_back(
        std::string(reinterpret_cast<const char *>(msg->buffer), msg->length));
  }

  ASSERT_EQ(sent_msgs.size(), received_msgs.size());
  for (int i = 0; i < sent_msgs.size(); i++) {
    EXPECT_EQ(sent_msgs[i], received_msgs[i]) << "i = " << i;
  }
}

TEST_F(ClientTest, RaceBetweenPubAndUnsub) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr int NUM_CHANNELS = 32;

  std::vector<Publisher> pubs;
  for (int i = 0; i < NUM_CHANNELS; ++i) {
    std::array<char, 64> buf = {};
    (void)snprintf(buf.data(), buf.size(), "ch_%d", i);
    absl::StatusOr<Publisher> pub =
        pub_client.CreatePublisher(buf.data(), 1024, 32);
    ASSERT_OK(pub);
    pubs.emplace_back(std::move(*pub));
  }

  std::vector<Subscriber> subs;
  for (int i = 0; i < NUM_CHANNELS; ++i) {
    std::array<char, 64> buf = {};
    (void)snprintf(buf.data(), buf.size(), "ch_%d", i);
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(buf.data());
    ASSERT_OK(sub);
    subs.emplace_back(std::move(*sub));
  }

  std::atomic<bool> pub_stopped = false;
  std::thread pub_thread([&pubs, &pub_stopped]() {
    while (!pub_stopped) {
      for (auto &pub : pubs) {
        absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
        ASSERT_OK(buffer);
        char *buf = reinterpret_cast<char *>(*buffer);
        int len = snprintf(buf, 256, "foobar");

        absl::StatusOr<const Message> pub_status = pub.PublishMessage(len + 1);
        ASSERT_OK(pub_status);
      }
    }
  });

  std::thread sub_thread([&subs]() {
    for (auto &sub : subs) {
      absl::StatusOr<Message> msg = sub.ReadMessage();
      ASSERT_OK(msg);
    }
    // Unsubscribe all channels.
    // Test fails ~20% of the time (by timeout) if we unsubscribe channels.
    // Commenting out this line makes it work.
    subs.clear();
  });

  sub_thread.join();

  pub_stopped = true;
  pub_thread.join();
}

// One publisher with a retirement trigger and two subscribers.  Subscribers
// trigger the retirement.
TEST_F(ClientTest, RetirementTrigger1) {
  auto pub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> p = pub_client->CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .notify_retirement = true});
  ASSERT_OK(p);
  auto pub = std::move(*p);

  {
    // You can't create a virtual publisher with notify_retirement.
    absl::StatusOr<Publisher> p2 =
        pub_client->CreatePublisher("dave6v", {
                                                  .slot_size = 256,
                                                  .num_slots = 10,
                                                  .mux = "/foobar",
                                                  .notify_retirement = true,
                                              });
    ASSERT_FALSE(p2.ok());
  }
  const toolbelt::FileDescriptor &retirement_fd = pub.GetRetirementFd();
  ASSERT_TRUE(retirement_fd.Valid());

  absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> s1 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s2);
  auto sub2 = std::move(*s2);

  // Read the message in sub1.
  absl::StatusOr<subspace::Message> p1 = sub1.ReadMessage();
  ASSERT_OK(p1);
  auto ptr1 = std::move(*p1);
  ASSERT_STREQ("foobar", reinterpret_cast<const char *>(ptr1.buffer));

  // Keep the message alive in sub1 and read it in sub2.
  absl::StatusOr<subspace::Message> p2 = sub2.ReadMessage();
  ASSERT_OK(p2);
  auto ptr2 = std::move(*p2);
  ASSERT_STREQ("foobar", reinterpret_cast<const char *>(ptr2.buffer));

  // Reset the first message.
  ptr1.Release();
  // Reset the second message. This will trigger a retirement.
  ptr2.Release();

  // Read the retirement fd and expect it to contain slot 0.
  struct pollfd fd = {.fd = retirement_fd.Fd(), .events = POLLIN};
  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);
  ASSERT_TRUE(fd.revents & POLLIN);
  int retired_slot;
  ssize_t n = ::read(retirement_fd.Fd(), &retired_slot, sizeof(retired_slot));
  ASSERT_EQ(sizeof(retired_slot), n);
  ASSERT_EQ(0, retired_slot);

  // Pipe should be empty now.
  e = ::poll(&fd, 1, 0);
  ASSERT_EQ(0, e);
  ASSERT_FALSE(fd.revents & POLLIN);
}

// This tests retirement from the the publisher side using dropped messages.  We
// have two subscribers, one reads two messages and the other doesn't read any.
// Since the second subscriber will never see the messages, the publisher will
// recycle the slots and trigger a retirement.
TEST_F(ClientTest, RetirementTrigger2) {
  auto pub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> p = pub_client->CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .notify_retirement = true});
  ASSERT_OK(p);
  auto pub = std::move(*p);

  const toolbelt::FileDescriptor &retirement_fd = pub.GetRetirementFd();

  absl::StatusOr<Subscriber> s1 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s2);
  auto sub2 = std::move(*s2);

  // Fill the slots with messages with enough room for two subscribers.
  for (int i = 0; i < 7; i++) {
    // Publish a message.
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // There should be no retired messages at this point.

  // Read 2 messages in sub1.  These will be retired.
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<subspace::Message> p1 = sub1.ReadMessage();
    ASSERT_OK(p1);
    auto ptr1 = std::move(*p1);
    ASSERT_STREQ("foobar", reinterpret_cast<const char *>(ptr1.buffer));
  }

  // Publish 2 messages, these will take the retired slots.
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // There should be nothing in the retirement fd.
  struct pollfd fd = {.fd = retirement_fd.Fd(), .events = POLLIN};
  int e = ::poll(&fd, 1, 0);
  ASSERT_EQ(0, e);
  ASSERT_FALSE(fd.revents & POLLIN);

  // Publish another message.  This will trigger the recycling of a slot and
  // trigger a publisher-side retirement.
  {
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read the retirement fd and expect it to contain slot 0.
  e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);
  ASSERT_TRUE(fd.revents & POLLIN);
  int retired_slot;
  ssize_t n = ::read(retirement_fd.Fd(), &retired_slot, sizeof(retired_slot));
  ASSERT_EQ(sizeof(retired_slot), n);
  ASSERT_EQ(0, retired_slot);

  // Send another two messages.  This will both trigger a recycled slot and
  // a publisher-side retirement.
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read the retirement pipe.  There should be 2 slot ids in the pipe.
  e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);
  ASSERT_TRUE(fd.revents & POLLIN);
  int retired_slots[2];
  for (int i = 0; i < 2; i++) {
    // Read the retirement fd and expect it to contain slot 1 and 2.
    n = ::read(retirement_fd.Fd(), &retired_slots[i], sizeof(retired_slots[i]));
    ASSERT_EQ(sizeof(retired_slots[i]), n);
  }
  ASSERT_EQ(2, retired_slots[0]);
  ASSERT_EQ(3, retired_slots[1]);

  // Pipe will be empty.
  e = ::poll(&fd, 1, 0);
  ASSERT_EQ(0, e);
  ASSERT_FALSE(fd.revents & POLLIN);
}

// Checks that retirement notification works with two publishers on different
// clients.
TEST_F(ClientTest, RetirementTrigger3) {
  auto pub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
  auto pub2Client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> p = pub_client->CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .notify_retirement = true});
  ASSERT_OK(p);
  auto pub = std::move(*p);

  // Another publisher on a different client.
  absl::StatusOr<Publisher> pp2 = pub2Client->CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .notify_retirement = true});
  ASSERT_OK(pp2);
  auto pub2 = std::move(*pp2);

  // We check for retirement trigger on the second publisher, not the one that
  // retires the slots.
  const toolbelt::FileDescriptor &retirement_fd = pub2.GetRetirementFd();

  absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> s1 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s2);
  auto sub2 = std::move(*s2);

  // Read the message in sub1.
  absl::StatusOr<subspace::Message> p1 = sub1.ReadMessage();
  ASSERT_OK(p1);
  auto ptr1 = *p1;
  ASSERT_STREQ("foobar", reinterpret_cast<const char *>(ptr1.buffer));

  // Keep the message alive in sub1 and read it in sub2.
  absl::StatusOr<subspace::Message> p2 = sub2.ReadMessage();
  ASSERT_OK(p2);
  auto ptr2 = std::move(*p2);
  ASSERT_STREQ("foobar", reinterpret_cast<const char *>(ptr2.buffer));

  // Reset the first message.
  ptr1.Release();
  // Reset the second message. This will trigger a retirement.
  ptr2.Release();

  // Read the retirement fd and expect it to contain slot 0.
  struct pollfd fd = {.fd = retirement_fd.Fd(), .events = POLLIN};
  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);
  ASSERT_TRUE(fd.revents & POLLIN);
  int retired_slot;
  ssize_t n = ::read(retirement_fd.Fd(), &retired_slot, sizeof(retired_slot));
  ASSERT_EQ(sizeof(retired_slot), n);
  ASSERT_EQ(0, retired_slot);

  // Pipe should be empty now.
  e = ::poll(&fd, 1, 0);
  ASSERT_EQ(0, e);
  ASSERT_FALSE(fd.revents & POLLIN);
}

TEST_F(ClientTest, RetirementTrigger4) {
  auto pub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
  auto pub2Client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));
  auto sub_client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> p = pub_client->CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .notify_retirement = true});
  ASSERT_OK(p);
  auto pub = std::move(*p);

  // Another publisher on a different client.
  absl::StatusOr<Publisher> pp2 = pub2Client->CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .notify_retirement = true});
  ASSERT_OK(pp2);
  auto pub2 = std::move(*pp2);

  // We check for retirement trigger on the second publisher, not the one that
  // retires the slots.
  const toolbelt::FileDescriptor &retirement_fd = pub2.GetRetirementFd();

  absl::StatusOr<Subscriber> s1 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", {.max_active_messages = 1});
  ASSERT_OK(s2);
  auto sub2 = std::move(*s2);

  // Fill the slots with messages with enough room for two subscribers.
  for (int i = 0; i < 6; i++) {
    // Publish a message.
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read 2 messages in sub1.  These will be retired.
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<subspace::Message> p1 = sub1.ReadMessage();
    ASSERT_OK(p1);
    auto ptr1 = std::move(*p1);
    ASSERT_STREQ("foobar", reinterpret_cast<const char *>(ptr1.buffer));
  }

  // Publish 2 messages, these will take the retired slots.
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // There should be nothing in the retirement fd.
  struct pollfd fd = {.fd = retirement_fd.Fd(), .events = POLLIN};
  int e = ::poll(&fd, 1, 0);
  ASSERT_EQ(0, e);
  ASSERT_FALSE(fd.revents & POLLIN);

  // Publish another message.  This will trigger the recycling of a slot and
  // trigger a publisher-side retirement.
  {
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read the retirement fd and expect it to contain slot 0.
  e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);
  ASSERT_TRUE(fd.revents & POLLIN);
  int retired_slot;
  ssize_t n = ::read(retirement_fd.Fd(), &retired_slot, sizeof(retired_slot));
  ASSERT_EQ(sizeof(retired_slot), n);
  ASSERT_EQ(0, retired_slot);

  // Send another two messages.  This will both trigger a recycled slot and
  // a publisher-side retirement.
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub.PublishMessage(6);
    ASSERT_OK(pub_status);
  }

  // Read the retirement pipe.  There should be 2 slot ids in the pipe.
  e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);
  ASSERT_TRUE(fd.revents & POLLIN);
  int retired_slots[2];
  for (int i = 0; i < 2; i++) {
    // Read the retirement fd and expect it to contain slot 2 and 3.
    n = ::read(retirement_fd.Fd(), &retired_slots[i], sizeof(retired_slots[i]));
    ASSERT_EQ(sizeof(retired_slots[i]), n);
  }
  ASSERT_EQ(3, retired_slots[0]);
  ASSERT_EQ(4, retired_slots[1]);

  // Pipe will be empty.
  e = ::poll(&fd, 1, 0);
  ASSERT_EQ(0, e);
  ASSERT_FALSE(fd.revents & POLLIN);
}

TEST_F(ClientTest, ChannelDirectory) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> p1 =
      client->CreatePublisher("chan1", {.slot_size = 256, .num_slots = 10});
  ASSERT_OK(p1);

  absl::StatusOr<Publisher> p2 =
      client->CreatePublisher("chan2", {.slot_size = 256, .num_slots = 10});
  ASSERT_OK(p2);

  absl::StatusOr<Subscriber> s1 = client->CreateSubscriber("chan1");
  ASSERT_OK(s1);

  absl::StatusOr<Subscriber> s2 = client->CreateSubscriber("chan2");
  ASSERT_OK(s2);

  // Subscribe to channel directory.
  absl::StatusOr<Subscriber> dir_sub =
      client->CreateSubscriber("/subspace/ChannelDirectory");
  ASSERT_OK(dir_sub);

  sleep(1); // Give some time for directory to be updated.

  // Read the latest channel directory message.
  absl::StatusOr<subspace::Message> msg =
      dir_sub->ReadMessage(subspace::ReadMode::kReadNewest);
  ASSERT_OK(msg);
  ASSERT_NE(0, msg->length);

  subspace::ChannelDirectory dir;
  ASSERT_TRUE(dir.ParseFromArray(msg->buffer, msg->length));
  ASSERT_GE(dir.channels_size(), 2);

  // Check that we have both chan1 and chan2 in the directory.
  bool found_chan1 = false;
  bool found_chan2 = false;
  for (int i = 0; i < dir.channels_size(); i++) {
    const subspace::ChannelInfoProto &info = dir.channels(i);
    if (info.name() == "chan1") {
      found_chan1 = true;
      ASSERT_EQ(256, info.slot_size());
      ASSERT_EQ(10, info.num_slots());
      ASSERT_EQ(1, info.num_pubs());
      ASSERT_EQ(1, info.num_subs());
    } else if (info.name() == "chan2") {
      found_chan2 = true;
      ASSERT_EQ(256, info.slot_size());
      ASSERT_EQ(10, info.num_slots());
      ASSERT_EQ(1, info.num_pubs());
      ASSERT_EQ(1, info.num_subs());
    }
  }
  ASSERT_TRUE(found_chan1);
  ASSERT_TRUE(found_chan2);
}

TEST_F(ClientTest, MessageGetters) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub =
      client->CreatePublisher("chan1", {.slot_size = 256, .num_slots = 10, .type = "test-type"});
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = client->CreateSubscriber("chan1");
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_NE(0, msg->length);
  // test message getters for publisher data
  ASSERT_EQ("test-type", msg->ChannelType());
  ASSERT_EQ(256, msg->SlotSize());
  ASSERT_EQ(10, msg->NumSlots());
}

// This tests checksums.  We have two publishers, one that calculates a checksum and one that doesn't.
// We have 2 subscribers, one that checks the checksum and expects an error if there's an error
// and one that checks the checksum and passes the message intact but with the checksum_error flag set.
TEST_F(ClientTest, ChecksumVerification) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan1", {.slot_size = 256, .num_slots = 10, .checksum = true});
  ASSERT_OK(pub);

  // Create a second publisher that doesn't calculate a checksum.
  absl::StatusOr<Publisher> pub2 =
      client->CreatePublisher("chan1", {.slot_size = 256, .num_slots = 10});
  ASSERT_OK(pub2);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan1", {.checksum = true});
  ASSERT_OK(sub);

  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(6, msg->length);
    ASSERT_STREQ("foobar", reinterpret_cast<const char *>(msg->buffer));
  }

  // Build a message but overwrite the message buffer with a different string.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer();
  ASSERT_OK(buffer2);
  memcpy(*buffer2, "foobar", 6);
  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(6);
  ASSERT_OK(pub_status2);

  char *buf = reinterpret_cast<char *>(*buffer2);
  buf[0] = 'x';

  // Read the message with a bad checksum.
  {
    absl::StatusOr<subspace::Message> msg2 = sub->ReadMessage();
    ASSERT_FALSE(msg2.ok());
    ASSERT_EQ(absl::StatusCode::kInternal, msg2.status().code());
    ASSERT_EQ("Checksum verification failed", msg2.status().message());
  }

  // Send another message with a valid checksum.
  absl::StatusOr<void *> buffer3 = pub->GetMessageBuffer();
  ASSERT_OK(buffer3);
  memcpy(*buffer3, "foobar", 6);
  absl::StatusOr<const Message> pub_status3 = pub->PublishMessage(6);
  ASSERT_OK(pub_status3);

  // Read the message with the valid checksum
  {
    absl::StatusOr<subspace::Message> msg3 = sub->ReadMessage();
    ASSERT_OK(msg3);
    ASSERT_EQ(6, msg3->length);
    ASSERT_STREQ("foobar", reinterpret_cast<const char *>(msg3->buffer));
  }

  // Send a message on pub2 with no checksum and corrupt it.
  absl::StatusOr<void *> buffer4 = pub2->GetMessageBuffer();
  ASSERT_OK(buffer4);
  memcpy(*buffer4, "foobar", 6);
  absl::StatusOr<const Message> pub_status4 = pub2->PublishMessage(6);
  ASSERT_OK(pub_status4);
  char *buf4 = reinterpret_cast<char *>(*buffer4);
  buf4[0] = 'X';

  // Read the corrupted message with no checksum.  Although the subscriber is
  // checking for a checksum, it will not be set since the publisher didn't
  // calculate one.
  {
    absl::StatusOr<subspace::Message> msg5 = sub->ReadMessage();
    ASSERT_OK(msg5);
    ASSERT_EQ(6, msg5->length);
    ASSERT_STREQ("Xoobar", reinterpret_cast<const char *>(msg5->buffer));
    ASSERT_FALSE(msg5->checksum_error);
  }

  // Create another subscriber with pass checksum errors.
  absl::StatusOr<Subscriber> sub2 = client->CreateSubscriber(
      "chan1", {.checksum = true, .pass_checksum_errors = true});
  ASSERT_OK(sub2);

  // First message will be fine since the checksum is good.
  {
    absl::StatusOr<subspace::Message> msg4 = sub2->ReadMessage();
    ASSERT_OK(msg4);
    ASSERT_EQ(6, msg4->length);
    ASSERT_STREQ("foobar", reinterpret_cast<const char *>(msg4->buffer));
  }

  // Second message wil have a checksum error flag set.
  {
    absl::StatusOr<subspace::Message> msg5 = sub2->ReadMessage();
    ASSERT_OK(msg5);
    ASSERT_EQ(6, msg5->length);
    ASSERT_TRUE(msg5->checksum_error);
  }

  // Third message will be fine since the checksum is good.
  {
    absl::StatusOr<subspace::Message> msg6 = sub2->ReadMessage();
    ASSERT_OK(msg6);
    ASSERT_EQ(6, msg6->length);
    ASSERT_STREQ("foobar", reinterpret_cast<const char *>(msg6->buffer));
  }

  // Fourth message doesn't have a checksum.
  {
    absl::StatusOr<subspace::Message> msg7 = sub2->ReadMessage();
    ASSERT_OK(msg7);
    ASSERT_EQ(6, msg7->length);
    ASSERT_STREQ("Xoobar", reinterpret_cast<const char *>(msg7->buffer));
    ASSERT_FALSE(msg7->checksum_error);
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  signal(SIGSEGV, SignalHandler);
  signal(SIGBUS, SignalHandler);
  signal(SIGQUIT, SigQuitHandler);

  absl::InitializeSymbolizer(argv[0]);

  absl::InstallFailureSignalHandler({
      .use_alternate_stack = false,
  });

  return RUN_ALL_TESTS();
}
