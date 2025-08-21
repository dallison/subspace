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

class ClientTest : public ::testing::Test {
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

co::CoroutineScheduler ClientTest::scheduler_;
std::string ClientTest::socket_ = "/tmp/subspace";
int ClientTest::server_pipe_[2];
std::unique_ptr<subspace::Server> ClientTest::server_;
std::thread ClientTest::server_thread_;

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

#define ASSERT_OK(e) ASSERT_TRUE(e.ok())

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
      toolbelt::InetAddress(), toolbelt::InetAddress("1.2.3.4", 2),
      toolbelt::InetAddress("localhost", 3), toolbelt::InetAddress(addr),
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
  ASSERT_TRUE(pub.ok());
}

TEST_F(ClientTest, Resize1) {
  subspace::Client client;
  InitClient(client);
  // Initial slot size is 256.
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_TRUE(pub.ok());

  // No resize.
  absl::StatusOr<void *> buffer1 = pub->GetMessageBuffer(256);
  ASSERT_TRUE(buffer1.ok());
  ASSERT_EQ(256, pub->SlotSize());

  // Resize to new slot size is 512.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(300);
  ASSERT_TRUE(buffer2.ok());
  ASSERT_EQ(512, pub->SlotSize());

  // Won't resize.
  absl::StatusOr<void *> buffer3 = pub->GetMessageBuffer(512);
  ASSERT_TRUE(buffer3.ok());
  ASSERT_EQ(512, pub->SlotSize());
}

TEST_F(ClientTest, ResizeCallback) {
  subspace::Client client;
  InitClient(client);
  // Initial slot size is 256.
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_TRUE(pub.ok());

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
  ASSERT_TRUE(status.ok());

  // No resize.
  absl::StatusOr<void *> buffer1 = pub->GetMessageBuffer(256);
  ASSERT_TRUE(buffer1.ok());
  ASSERT_EQ(256, pub->SlotSize());

  // Resize to new slot size is 512.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(300);
  ASSERT_TRUE(buffer2.ok());
  ASSERT_EQ(512, pub->SlotSize());

  // Won't resize.
  absl::StatusOr<void *> buffer3 = pub->GetMessageBuffer(512);
  ASSERT_TRUE(buffer3.ok());
  ASSERT_EQ(512, pub->SlotSize());

  ASSERT_EQ(1, num_resizes);

  // The resize callback will return an error because it only
  // allows one resize.
  absl::StatusOr<void *> buffer4 = pub->GetMessageBuffer(1000);
  ASSERT_FALSE(buffer4.ok());
}

TEST_F(ClientTest, CreatePublisherWithType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_TRUE(pub.ok());
  ASSERT_EQ("foobar", pub->Type());
}

TEST_F(ClientTest, CreateVirtualPublisherWithType) {
  subspace::Client client;
  InitClient(client);
  {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 100, {.type = "foobar", .mux = "mainmux"});
    std::cerr << pub.status() << std::endl;
    ASSERT_TRUE(pub.ok());
    ASSERT_EQ("foobar", pub->Type());
  }
  // Mux will be destructed here since there are no virtual
  // channels on it.

  // Create again with different type.
  {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 10, {.type = "foobar1", .mux = "mainmux"});
    ASSERT_TRUE(pub.ok());
    ASSERT_EQ("foobar1", pub->Type());
  }
}

TEST_F(ClientTest, TooManyVirtualPublishers) {
  subspace::Client client;
  InitClient(client);
  constexpr int kMuxCapacity = 10;

  std::vector<Publisher> pubs;
  for (int i = 0; i < kMuxCapacity - 2; i++) {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 10, {.type = "foobar", .mux = "mainmux"});
    ASSERT_TRUE(pub.ok());
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
  ASSERT_TRUE(pub.ok());
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
    ASSERT_TRUE(pub.ok());
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
    ASSERT_TRUE(pub.ok());
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

  ASSERT_TRUE(pub1.ok());
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
    ASSERT_TRUE(pub.ok());
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
  ASSERT_TRUE(pub.ok());

  std::vector<Subscriber> subs;
  for (int i = 0; i < 8; i++) {
    absl::StatusOr<Subscriber> sub = client.CreateSubscriber("dave0");
    ASSERT_TRUE(sub.ok());
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
  ASSERT_TRUE(pub.ok());

  // 6 subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumSlots - 4; i++) {
    absl::StatusOr<Subscriber> sub =
        client.CreateSubscriber("dave0", {.mux = "foobar"});
    ASSERT_TRUE(sub.ok());
    subs.push_back(std::move(*sub));
  }

  // 1 Multiplexer subscriber.
  absl::StatusOr<Subscriber> mux_sub = client.CreateSubscriber("foobar");
  ASSERT_TRUE(mux_sub.ok());

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
    ASSERT_TRUE(pub.ok());
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
    ASSERT_TRUE(sub.ok());
  }
  // One more will fail.
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber("dave0");
  ASSERT_FALSE(sub.ok());
}

TEST_F(ClientTest, BadPublisherParameters) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub1 = client.CreatePublisher("dave0", 256, 10);
  ASSERT_TRUE(pub1.ok());

  // Different slot size - this is fine due to channel resizing and we are not
  // fixed size.
  absl::StatusOr<Publisher> pub2 = client.CreatePublisher("dave0", 255, 10);
  ASSERT_TRUE(pub2.ok());

  // Different num slots
  absl::StatusOr<Publisher> pub3 = client.CreatePublisher("dave0", 256, 9);
  ASSERT_FALSE(pub3.ok());

  // Fixed size.
  absl::StatusOr<Publisher> pub4 =
      client.CreatePublisher("dave1", 256, 10, {.fixed_size = true});
  ASSERT_TRUE(pub4.ok());

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
  ASSERT_TRUE(p.ok());

  auto s = client.CreateSubscriber("dave1");
  ASSERT_TRUE(s.ok());
}

TEST_F(ClientTest, CreateVirtualPublisherThenSubscriber) {
  subspace::Client client;
  InitClient(client);
  auto p = client.CreatePublisher("dave1", 256, 10, {.mux = "foobar"});
  ASSERT_TRUE(p.ok());

  auto s = client.CreateSubscriber("dave1", {.mux = "foobar"});
  ASSERT_TRUE(s.ok());
}

TEST_F(ClientTest, CreateVirtualSubscriberThenPublisher) {
  subspace::Client client;
  InitClient(client);

  auto s = client.CreateSubscriber("dave1", {.mux = "foobar"});
  ASSERT_TRUE(s.ok());
  auto p = client.CreatePublisher("dave1", 256, 10, {.mux = "foobar"});
  ASSERT_TRUE(p.ok());
}

TEST_F(ClientTest, CreateVirtualPublisherThenSubscriberMuxMismatch) {
  subspace::Client client;
  InitClient(client);
  auto p1 = client.CreatePublisher("dave1", 256, 10, {.mux = "foobar"});
  ASSERT_TRUE(p1.ok());

  auto s1 = client.CreateSubscriber("dave1", {.mux = "foobar"});
  ASSERT_TRUE(s1.ok());

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
  ASSERT_TRUE(p.ok());
  auto s = client.CreateSubscriber(
      "dave1", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s.ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriberWrongType) {
  subspace::Client client;
  InitClient(client);
  auto p = client.CreatePublisher(
      "dave1", 256, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_TRUE(p.ok());
  auto s = client.CreateSubscriber(
      "dave1", subspace::SubscriberOptions().SetType("barfoo"));
  ASSERT_FALSE(s.ok());
}

TEST_F(ClientTest, CreateSubscriber) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber("dave2");
  ASSERT_TRUE(s.ok());
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
  ASSERT_TRUE(s.ok());
  ASSERT_EQ("foobar", s->Type());
}

TEST_F(ClientTest, MismatchedSubscriberType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s1 = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s1.ok());
  ASSERT_EQ("foobar", s1->Type());

  absl::StatusOr<Subscriber> s2 = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("barfoo"));
  ASSERT_FALSE(s2.ok());
}

TEST_F(ClientTest, CreateSubscriberThenPublisher) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber("dave3");
  ASSERT_TRUE(s.ok());

  absl::StatusOr<Publisher> p = client.CreatePublisher("dave3", 300, 10);
  ASSERT_TRUE(p.ok());

  auto &counters = s->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
}

TEST_F(ClientTest, CreateSubscriberThenPublisherSameType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber(
      "dave3", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s.ok());

  absl::StatusOr<Publisher> p = client.CreatePublisher(
      "dave3", 300, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_TRUE(p.ok());

  auto &counters = s->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
}

TEST_F(ClientTest, CreateSubscriberThenPublisherWrongType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber(
      "dave3", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s.ok());

  absl::StatusOr<Publisher> p = client.CreatePublisher(
      "dave3", 300, 10, subspace::PublisherOptions().SetType("bar"));
  ASSERT_FALSE(p.ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriberDifferentClient) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  ASSERT_TRUE(pub_client.CreatePublisher("dave4", 256, 10).ok());
  ASSERT_TRUE(sub_client.CreateSubscriber("dave4").ok());
}

TEST_F(ClientTest, PublishSingleMessage) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher("dave5", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());
}

TEST_F(ClientTest, PublishSingleMessageAndRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessageAndReadNewest) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());

  // Publish a message.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_TRUE(sub.ok());

  // Another message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    memcpy(*buffer, "foobar2", 7);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read the newest message.
  absl::StatusOr<Message> msg =
      sub->ReadMessage(subspace::ReadMode::kReadNewest);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(7, msg->length);

  // There are no more messages since we read the newest one.
  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessageAndReadWithActivation) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "dave6", {.slot_size = 256, .num_slots = 10, .activate = true});
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", {.max_active_messages = 2, .pass_activation = true});
  ASSERT_TRUE(sub.ok());

  // Read activation message.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(1, msg->length);
  ASSERT_TRUE(msg->is_activation);

  // Read the actual message.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessageAndReadWithCallback) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_TRUE(sub.ok());

  auto status = sub->RegisterMessageCallback(
      [](Subscriber *s, Message msg) { ASSERT_EQ(6, msg.length); });
  ASSERT_TRUE(status.ok());

  status = sub->ProcessAllMessages();
  ASSERT_TRUE(status.ok());

  status = sub->UnregisterMessageCallback();
  ASSERT_TRUE(status.ok());
}

TEST_F(ClientTest, VirtualPublishSingleMessageAndRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", 256, 10, {.mux = "mainmux"});
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.mux = "mainmux"});
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);

  // Read again to make sure we get another 0.
  // Regression test.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, VirtualPublishMultiple) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  std::vector<Publisher> pubs;
  std::vector<Subscriber> subs;
  // Create 10 publishers on mux mainmux.
  for (int i = 0; i < 10; i++) {
    std::string name = "dave" + std::to_string(i);
    absl::StatusOr<Publisher> pub =
        pub_client.CreatePublisher(name, 256, 100, {.mux = "mainmux"});
    ASSERT_TRUE(pub.ok());
    pubs.push_back(std::move(*pub));
  }

  // Create 10 subscribers for the same mux and channel names.
  for (int i = 0; i < 10; i++) {
    std::string name = "dave" + std::to_string(i);
    absl::StatusOr<Subscriber> sub =
        sub_client.CreateSubscriber(name, {.mux = "mainmux"});
    ASSERT_TRUE(sub.ok());
    subs.push_back(std::move(*sub));
  }

  // Create a subscriber to the multiplexer.
  absl::StatusOr<Subscriber> mux_sub = sub_client.CreateSubscriber("mainmux");
  ASSERT_TRUE(mux_sub.ok());

  // Publish a message on all 10 publishers
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<void *> buffer = pubs[i].GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pubs[i].PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read all messages using the virtual subscribers.
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(subs[i].Wait().ok());
    absl::StatusOr<Message> msg = subs[i].ReadMessage();
    ASSERT_TRUE(msg.ok());
    ASSERT_EQ(6, msg->length);
    ASSERT_EQ(i, msg->vchan_id);
  }

  // Make sure mux is triggered.
  ASSERT_TRUE(mux_sub->Wait().ok());

  // Read the all messages using the multiplexer subscriber.
  // This hasn't seen any messages yet so will see them all.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Message> msg = mux_sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
    ASSERT_EQ(6, msg->length);
    ASSERT_EQ(i, msg->vchan_id);
  }

  // Read another and get 0 length.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Message> msg = subs[i].ReadMessage();
    ASSERT_TRUE(msg.ok());
    ASSERT_EQ(0, msg->length);
  }
}

TEST_F(ClientTest, PublishAndResize) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_TRUE(buffer2.ok());
  ASSERT_EQ(4096, pub->SlotSize());

  auto &pub_buffers = pub->GetBuffers();
  ASSERT_EQ(2, pub_buffers.size());

  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_TRUE(pub_status2.ok());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_TRUE(msg2.ok());
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());

  auto &sub_buffers = sub->GetBuffers();
  ASSERT_EQ(2, sub_buffers.size());
}

TEST_F(ClientTest, PublishVirtualAndResize) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", 256, 10, {.mux = "mainmux"});
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", {.max_active_messages = 2, .mux = "mainmux"});
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_TRUE(buffer2.ok());
  ASSERT_EQ(4096, pub->SlotSize());

  auto &pub_buffers = pub->GetBuffers();
  ASSERT_EQ(2, pub_buffers.size());

  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_TRUE(pub_status2.ok());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_TRUE(msg2.ok());
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());

  auto &sub_buffers = sub->GetBuffers();
  ASSERT_EQ(2, sub_buffers.size());
}

TEST_F(ClientTest, PublishAndResize2) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_TRUE(buffer2.ok());
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_TRUE(pub_status2.ok());

  // Now create subscriber and read both messages.
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_TRUE(msg2.ok());
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());
}

TEST_F(ClientTest, PublishAndResizeUnmapBuffers) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  // Publish bigger messages.  This will cause a resize.  We take
  // all the slots to free up buffer index 0 and it will be
  // unmapped
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
    ASSERT_TRUE(buffer2.ok());
    ASSERT_EQ(4096, pub->SlotSize());
    memcpy(*buffer2, "barfoofoobar", 12);

    absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
    ASSERT_TRUE(pub_status2.ok());
  }

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_TRUE(sub.ok()) << sub.status();

  // Create another publisher after resize.
  absl::StatusOr<Publisher> pub2 = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub2.ok());

  // Read all messages.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
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
  ASSERT_TRUE(buffer3.ok());
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer3, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status3 = pub->PublishMessage(12);
  ASSERT_TRUE(pub_status3.ok());

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
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  // First create subscriber.
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", {.max_active_messages = 2});
  ASSERT_TRUE(sub.ok());
  ASSERT_EQ(0, sub->SlotSize()); // No buffers yet.

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_TRUE(buffer2.ok());
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_TRUE(pub_status2.ok());

  // Now read both messages.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_TRUE(msg2.ok());
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());
}

TEST_F(ClientTest, PublishVirtualAndResizeSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  // First create subscriber.
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", {.max_active_messages = 2, .mux = "mainmux"});
  ASSERT_TRUE(sub.ok());
  ASSERT_EQ(0, sub->SlotSize()); // No buffers yet.

  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", 256, 10, {.mux = "mainmux"});
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  // Publish a bigger message.  This will cause a resize.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer(4000);
  ASSERT_TRUE(buffer2.ok());
  ASSERT_EQ(4096, pub->SlotSize());
  memcpy(*buffer2, "barfoofoobar", 12);

  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(12);
  ASSERT_TRUE(pub_status2.ok());

  // Now read both messages.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(256, sub->SlotSize());

  absl::StatusOr<Message> msg2 = sub->ReadMessage();
  ASSERT_TRUE(msg2.ok());
  ASSERT_EQ(12, msg2->length);
  ASSERT_EQ(4096, sub->SlotSize());
}

TEST_F(ClientTest, PublishAndResizeSubscriberConcurrently) {
  std::string channel_name = "growing_channel";
  subspace::Client client1;
  subspace::Client client2;
  ASSERT_TRUE(client1.Init(Socket()).ok());
  ASSERT_TRUE(client2.Init(Socket()).ok());

  std::atomic<bool> publisher_finished{false};

  auto t1 = std::thread([&]() {
    auto client1_pub = *client1.CreatePublisher(channel_name, 1, 4);
    for (int i = 1; i < 24; i++) {
      std::size_t size = std::pow(2, i);
      auto buffer = client1_pub.GetMessageBuffer(size);
      std::memset(*buffer, i, size);
      ASSERT_TRUE(client1_pub.PublishMessage(size).ok());
    }
    publisher_finished = true;
  });
  auto t2 = std::thread([&]() {
    auto client2_sub = *client2.CreateSubscriber(channel_name);
    while (publisher_finished == false) {
      auto message = *client2_sub.ReadMessage();
      size_t size = message.length;
      if (size == 0) {
        continue;
      } else {
        std::cout << size << std::endl;
      }
    }
  });

  t1.join();
  t2.join();
}

TEST_F(ClientTest, PublishSingleMessagePollAndReadSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave7");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave7", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  struct pollfd fd = sub->GetPollFd();

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessagePollAndReadPublisherFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  struct pollfd fd = sub->GetPollFd();

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishMultipleMessagePollAndReadPublisherFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_TRUE(sub.ok());

  for (int i = 0; i < 9; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
    ASSERT_TRUE(pub_status.ok());
  }
  struct pollfd fd = sub->GetPollFd();

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  for (;;) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
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
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      "rel_dave", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_TRUE(sub.ok());

  auto &counters = pub->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
  ASSERT_EQ(1, counters.num_reliable_pubs);
  ASSERT_EQ(1, counters.num_reliable_subs);

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read the message from reliable subscriber.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_EQ(nullptr, *buffer);
  }

  msg->Release();

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    struct pollfd fd = pub->GetPollFd();
    c->Wait(fd.fd);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  });

  // Read messages in coroutine.
  co::Coroutine c2(machine, [&sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      std::cerr << msg.status() << std::endl;
      ASSERT_TRUE(msg.ok());
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
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
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "rel_dave", 32, 5, subspace::PublisherOptions().SetReliable(true));
  ASSERT_TRUE(pub.ok());

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read the message from reliable subscriber.  This will make a server
  // call to get the reliable publishers.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_EQ(nullptr, *buffer);
  }

  msg->Release();

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    absl::StatusOr<struct pollfd> fd = pub->GetPollFd();
    ASSERT_TRUE(fd.ok());
    c->Wait(fd->fd);
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  });

  // Read another message in coroutine.
  co::Coroutine c2(machine, [&sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_TRUE(msg.ok());
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
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
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      "rel_dave", {.reliable = true, .pass_activation = true});
  ASSERT_TRUE(sub.ok());

  auto &counters = pub->GetChannelCounters();
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
  ASSERT_EQ(1, counters.num_reliable_pubs);
  ASSERT_EQ(1, counters.num_reliable_subs);

  // Read the activation.
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(1, msg->length);
  ASSERT_TRUE(msg->is_activation);

  msg->Release();

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read the message from reliable subscriber.
  msg = sub->ReadMessage();
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_EQ(nullptr, *buffer);
  }

  msg->Release();

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    struct pollfd fd = pub->GetPollFd();
    c->Wait(fd.fd);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  });

  // Read messages in coroutine.
  co::Coroutine c2(machine, [&sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      std::cerr << msg.status() << std::endl;
      ASSERT_TRUE(msg.ok());
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
    ASSERT_EQ(0, msg->length);

  });

  machine.Run();
}

TEST_F(ClientTest, DroppedMessage) {
  subspace::Client client;
  InitClient(client);

  absl::StatusOr<Subscriber> sub = client.CreateSubscriber("rel_dave");
  ASSERT_TRUE(sub.ok());

  int num_dropped_messages = 0;
  absl::Status status = sub->RegisterDroppedMessageCallback(
      [&num_dropped_messages, &sub](Subscriber *s, int64_t num_dropped) {
        ASSERT_EQ(*sub, *s);
        num_dropped_messages += num_dropped;
      });
  ASSERT_TRUE(status.ok());

  absl::StatusOr<Publisher> pub = client.CreatePublisher("rel_dave", 32, 5);
  ASSERT_TRUE(pub.ok());

  // 5 slots. Fill 4 of them with messages.  Slots 0 to 3 will contain
  // messages and the publisher will have slot 4
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read one message. This will be in slot 0.
  {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
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
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read all messages in channel.
  for (;;) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
    if (msg->length == 0) {
      break;
    }
  }
  ASSERT_EQ(4, num_dropped_messages);
}

TEST_F(ClientTest, PublishSingleMessageAndReadSharedPtr) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", subspace::SubscriberOptions().SetMaxActiveMessages(3));
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub->ReadMessage<const char>();
  ASSERT_TRUE(p.ok());
  const auto &ptr = *p;
  ASSERT_TRUE(static_cast<bool>(ptr));
  ASSERT_STREQ("foobar", ptr.get());

  ASSERT_EQ(1, ptr.use_count());

  // Copy the shared ptr using copy constructor.
  subspace::shared_ptr<const char> p2(ptr);
  ASSERT_EQ(2, ptr.use_count());
  ASSERT_EQ(2, p2.use_count());

  // Copy using copy operator.
  subspace::shared_ptr<const char> p3 = ptr;
  ASSERT_EQ(3, ptr.use_count());
  ASSERT_EQ(3, p2.use_count());
  ASSERT_EQ(3, p3.use_count());

  // Move p3 to p4.
  subspace::shared_ptr<const char> p4 = std::move(p3);
  ASSERT_FALSE(static_cast<bool>(p3));
  ASSERT_EQ(3, ptr.use_count());
  ASSERT_EQ(3, p2.use_count());
  ASSERT_EQ(0, p3.use_count());
  ASSERT_EQ(3, p4.use_count());
}

TEST_F(ClientTest, Publish2Message2AndReadSharedPtrs) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());

  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
  }

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", subspace::SubscriberOptions().SetMaxActiveMessages(2));
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub->ReadMessage<const char>();
  ASSERT_TRUE(p.ok());
  ASSERT_TRUE(static_cast<bool>(*p));
  ASSERT_STREQ("foobar", p->get());

  // Create a weak_ptr from p.
  subspace::weak_ptr<const char> w(*p);
  ASSERT_FALSE(w.expired());

  absl::StatusOr<subspace::shared_ptr<const char>> p2 =
      sub->ReadMessage<const char>();

  ASSERT_TRUE(p2.ok());
  ASSERT_TRUE(static_cast<bool>(*p2));
  ASSERT_STREQ("foobar", p2->get());

  p->reset();

  // weak_ptr is still valid.
  ASSERT_FALSE(w.expired());

  // Publish some more messages to reuse the weak pointer's slot.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_TRUE(pub_status.ok());
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

    ASSERT_EQ(2, p2.use_count());
  }
  // Number of active messages: 1
}

TEST_F(ClientTest, FindMessage) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_TRUE(sub.ok());

  std::vector<subspace::Message> msgs;
  for (int i = 0; i < 9; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
    ASSERT_TRUE(pub_status.ok());
    msgs.push_back(std::move(*pub_status));
  }

  // Find an unknown message lower than all others.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(12345678);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(nullptr, m->buffer);
  }

  // Find an unknown message higher than all others.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(-1);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(nullptr, m->buffer);
  }

  // Find a known message.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[4].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[4].timestamp, m->timestamp);
    ASSERT_EQ(msgs[4].length, m->length);
    ASSERT_EQ(msgs[4].ordinal, m->ordinal);
  }

  // Find another known message.  This will change ownership of the subscriber's
  // slot.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[7].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[7].timestamp, m->timestamp);
    ASSERT_EQ(msgs[7].length, m->length);
    ASSERT_EQ(msgs[7].ordinal, m->ordinal);
  }

  // Find first message.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[0].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[0].timestamp, m->timestamp);
    ASSERT_EQ(msgs[0].length, m->length);
    ASSERT_EQ(msgs[0].ordinal, m->ordinal);
  }

  // Find last message.
  {
    absl::StatusOr<const Message> m = sub->FindMessage(msgs[8].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[8].timestamp, m->timestamp);
    ASSERT_EQ(msgs[8].length, m->length);
    ASSERT_EQ(msgs[8].ordinal, m->ordinal);
  }
}

TEST_F(ClientTest, Mikael) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("mik", 1024, 32);
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("mik");
  ASSERT_TRUE(sub.ok());

  std::vector<std::string> sent_msgs;
  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_TRUE(buffer.ok());
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(len + 1);
    ASSERT_TRUE(pub_status.ok());
    sent_msgs.push_back(std::string(buf, len + 1));
  }

  std::vector<std::string> received_msgs;

  for (;;) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
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
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  constexpr int NUM_CHANNELS = 32;

  std::vector<Publisher> pubs;
  for (int i = 0; i < NUM_CHANNELS; ++i) {
    std::array<char, 64> buf = {};
    (void)snprintf(buf.data(), buf.size(), "ch_%d", i);
    absl::StatusOr<Publisher> pub =
        pub_client.CreatePublisher(buf.data(), 1024, 32);
    ASSERT_TRUE(pub.ok());
    pubs.emplace_back(std::move(*pub));
  }

  std::vector<Subscriber> subs;
  for (int i = 0; i < NUM_CHANNELS; ++i) {
    std::array<char, 64> buf = {};
    (void)snprintf(buf.data(), buf.size(), "ch_%d", i);
    absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(buf.data());
    ASSERT_TRUE(sub.ok());
    subs.emplace_back(std::move(*sub));
  }

  std::atomic<bool> pub_stopped = false;
  std::thread pub_thread([&pubs, &pub_stopped]() {
    while (!pub_stopped) {
      for (auto &pub : pubs) {
        absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
        ASSERT_TRUE(buffer.ok());
        char *buf = reinterpret_cast<char *>(*buffer);
        int len = snprintf(buf, 256, "foobar");

        absl::StatusOr<const Message> pub_status = pub.PublishMessage(len + 1);
        ASSERT_TRUE(pub_status.ok());
      }
    }
  });

  std::thread sub_thread([&subs]() {
    for (auto &sub : subs) {
      absl::StatusOr<Message> msg = sub.ReadMessage();
      ASSERT_TRUE(msg.ok());
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
  struct pollfd fd = {.events = POLLIN, .fd = retirement_fd.Fd()};
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
  struct pollfd fd = {.events = POLLIN, .fd = retirement_fd.Fd()};
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
  struct pollfd fd = {.events = POLLIN, .fd = retirement_fd.Fd()};
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
  struct pollfd fd = {.events = POLLIN, .fd = retirement_fd.Fd()};
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

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
