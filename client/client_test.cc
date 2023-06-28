// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/hash/hash_testing.h"
#include "client/client.h"
#include "coroutine.h"
#include "server/server.h"
#include "toolbelt/hexdump.h"
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
    char tmp[] = "/tmp/subspaceXXXXXX";
    int fd = mkstemp(tmp);
    ASSERT_NE(-1, fd);
    socket_ = tmp;
    close(fd);

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

TEST_F(ClientTest, CreatePublisherWithType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_TRUE(pub.ok());
  ASSERT_EQ("foobar", pub->Type());
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

  // Different slot size.
  absl::StatusOr<Publisher> pub2 = client.CreatePublisher("dave0", 255, 10);
  ASSERT_FALSE(pub2.ok());

  // Different num slots
  absl::StatusOr<Publisher> pub3 = client.CreatePublisher("dave0", 256, 9);
  ASSERT_FALSE(pub3.ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriber) {
  subspace::Client client;
  InitClient(client);
  ASSERT_TRUE(client.CreatePublisher("dave1", 256, 10).ok());
  ASSERT_TRUE(client.CreateSubscriber("dave1").ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriberSameType) {
  subspace::Client client;
  InitClient(client);
  ASSERT_TRUE(
      client
          .CreatePublisher("dave1", 256, 10,
                           subspace::PublisherOptions().SetType("foobar"))
          .ok());
  ASSERT_TRUE(client
                  .CreateSubscriber(
                      "dave1", subspace::SubscriberOptions().SetType("foobar"))
                  .ok());
}

TEST_F(ClientTest, CreatePublisherThenSubscriberWrongType) {
  subspace::Client client;
  InitClient(client);
  ASSERT_TRUE(
      client
          .CreatePublisher("dave1", 256, 10,
                           subspace::PublisherOptions().SetType("foobar"))
          .ok());
  ASSERT_FALSE(client
                   .CreateSubscriber(
                       "dave1", subspace::SubscriberOptions().SetType("barfoo"))
                   .ok());
}

TEST_F(ClientTest, CreateSubscriber) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber> s = client.CreateSubscriber("dave2");
  ASSERT_TRUE(s.ok());
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

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
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
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
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
  ASSERT_TRUE(sub.ok());

  // Read all messages.
  for (int i = 0; i < 10; i++) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_TRUE(msg.ok());
  }

  // We won't have freed the buffers yet for the publisher, but we
  // will have for the subscriber.  We do that when it runs out
  // of messages to read.
  {
    auto &pub_buffers = pub->GetBuffers();
    ASSERT_EQ(2, pub_buffers.size());
    ASSERT_NE(nullptr, pub_buffers[0].buffer);

    auto &sub_buffers = sub->GetBuffers();
    ASSERT_EQ(2, sub_buffers.size());
    ASSERT_EQ(nullptr, sub_buffers[0].buffer);
    ASSERT_NE(nullptr, sub_buffers[1].buffer);
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
    ASSERT_EQ(nullptr, pub_buffers[0].buffer);
    ASSERT_NE(nullptr, pub_buffers[1].buffer);
  }
}

TEST_F(ClientTest, PublishAndResizeSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  // First create subscriber.
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
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

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    struct pollfd fd = pub->GetPollFd();
    c->Wait(fd);

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

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&pub](co::Coroutine *c) {
    absl::StatusOr<struct pollfd> fd = pub->GetPollFd();
    ASSERT_TRUE(fd.ok());
    c->Wait(*fd);

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

TEST_F(ClientTest, DroppedMessage) {
  subspace::Client client;
  InitClient(client);

  absl::StatusOr<Subscriber> sub = client.CreateSubscriber("rel_dave");
  ASSERT_TRUE(sub.ok());

  int num_dropped_messages = 0;
  sub->RegisterDroppedMessageCallback(
      [&num_dropped_messages, &sub](Subscriber *s, int64_t num_dropped) {
        ASSERT_EQ(*sub, *s);
        num_dropped_messages += num_dropped;
      });

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
  // So on the first read we drop 5 messages (expecting ordinal 2 but get 6).
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
  ASSERT_EQ(5, num_dropped_messages);
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
      "dave6", subspace::SubscriberOptions().SetMaxSharedPtrs(3));
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub->ReadMessage<const char>();
  ASSERT_TRUE(p.ok());
  const auto &ptr = *p;
  ASSERT_TRUE(static_cast<bool>(ptr));
  ASSERT_STREQ("foobar", ptr.get());

  // For use count, the subscriber still has a reference to the slot, so the
  // shared_ptr use counts are one greater than you would expect.
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
      "dave6", subspace::SubscriberOptions().SetMaxSharedPtrs(2));
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub->ReadMessage<const char>();
  ASSERT_TRUE(p.ok());
  const auto &ptr = *p;
  ASSERT_TRUE(static_cast<bool>(ptr));
  ASSERT_STREQ("foobar", ptr.get());

  // Create a weak_ptr from p.
  subspace::weak_ptr<const char> w(ptr);
  ASSERT_FALSE(w.expired());

  // Read second message and overwrite shared_ptr.
  p = sub->ReadMessage<const char>();
  ASSERT_TRUE(p.ok());
  ASSERT_TRUE(static_cast<bool>(ptr));
  ASSERT_STREQ("foobar", ptr.get());

  // weak_ptr will have expired.
  ASSERT_TRUE(w.expired());

  // Another weak ptr.
  subspace::weak_ptr<const char> w2(ptr);
  subspace::shared_ptr<const char> p2(w2);
  ASSERT_EQ(3, p2.use_count());
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
    msgs.push_back(*pub_status);
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

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
