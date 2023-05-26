// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/hash/hash_testing.h"
#include "client/client.h"
#include "toolbelt/hexdump.h"
#include "coroutine.h"
#include "server/server.h"
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/resource.h>

void SignalHandler(int sig) { printf("Signal %d", sig); }

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;
using InetAddress = toolbelt::InetAddress;

class ClientTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    char tmp[] = "/tmp/subspaceXXXXXX";
    int fd = mkstemp(tmp);
    ASSERT_NE(-1, fd);
    socket_ = tmp;
    close(fd);

    // Add --socket arg to allow running on shared testing infrastructure where
    // more than one can run at once.
    char socket_arg[32];
    snprintf(socket_arg, sizeof(socket_arg), "--socket=%s", socket_.c_str());
    server_pid_ = fork();
    if (server_pid_ == 0) {
      // Child.  Run the server from this directory.
      execl("bazel-bin/server/subspace_server", "subspace_server", "--local",
            socket_arg, nullptr);
      abort();
    }

    // Wait until we can connect to the server.
    subspace::Client client;
    while (!client.Init(Socket()).ok()) {
      usleep(10);
    }
  }

  static void TearDownTestSuite() {
    // Kill server and wait for it to stop.
    kill(server_pid_, SIGTERM);
    int status;
    waitpid(server_pid_, &status, 0);
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
  void TearDown() override {}

  void InitClient(subspace::Client &client) {
    ASSERT_TRUE(client.Init(Socket()).ok());
  }

  static const std::string &Socket() { return socket_; }

private:
  static int server_pid_;
  static std::string socket_;
};

int ClientTest::server_pid_;
std::string ClientTest::socket_;

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
      toolbelt::InetAddress(), toolbelt::InetAddress("1.2.3.4", 2), toolbelt::InetAddress("localhost", 3),
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
  absl::StatusOr<Publisher *> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_TRUE(pub.ok());
}

TEST_F(ClientTest, CreatePublisherWithType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher *> pub = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_TRUE(pub.ok());
  ASSERT_EQ("foobar", (*pub)->Type());
}

TEST_F(ClientTest, PublisherTypeMismatch) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher *> pub1 = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("foobar"));

  ASSERT_TRUE(pub1.ok());
  ASSERT_EQ("foobar", (*pub1)->Type());

  absl::StatusOr<Publisher *> pub2 = client.CreatePublisher(
      "dave0", 256, 10, subspace::PublisherOptions().SetType("barfoo"));
  ASSERT_FALSE(pub2.ok());
}

TEST_F(ClientTest, TooManyPublishers) {
  subspace::Client client;
  InitClient(client);
  for (int i = 0; i < 9; i++) {
    absl::StatusOr<Publisher *> pub = client.CreatePublisher("dave0", 256, 10);
    ASSERT_TRUE(pub.ok());
  }
  // One more will fail.
  absl::StatusOr<Publisher *> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, TooManySubscribers) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher *> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_TRUE(pub.ok());

  for (int i = 0; i < 8; i++) {
    absl::StatusOr<Subscriber *> sub = client.CreateSubscriber("dave0");
    ASSERT_TRUE(sub.ok());
  }
  // One more will fail.
  absl::StatusOr<Subscriber *> sub = client.CreateSubscriber("dave0");
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
    absl::StatusOr<Publisher *> pub = client.CreatePublisher(name, 256, 10);
    if (!pub.ok()) {
      printf("%s: %s\n", name, pub.status().ToString().c_str());
    }
    ASSERT_TRUE(pub.ok());
  }
  // One more will fail.
  absl::StatusOr<Publisher *> pub = client.CreatePublisher("mint", 256, 10);
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, DISABLED_PushSubscriberLimit) {
  subspace::Client client;
  InitClient(client);
  for (int i = 0; i < subspace::kMaxSlotOwners; i++) {
    absl::StatusOr<Subscriber *> sub = client.CreateSubscriber("dave0");
    if (!sub.ok()) {
      printf("%s\n", sub.status().ToString().c_str());
    }
    ASSERT_TRUE(sub.ok());
  }
  // One more will fail.
  absl::StatusOr<Subscriber *> sub = client.CreateSubscriber("dave0");
  ASSERT_FALSE(sub.ok());
}

TEST_F(ClientTest, BadPublisherParameters) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher *> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_TRUE(pub.ok());

  // Different slot size.
  pub = client.CreatePublisher("dave0", 255, 10);
  ASSERT_FALSE(pub.ok());

  // Different num slots
  pub = client.CreatePublisher("dave0", 256, 9);
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, RemovePublisher) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher *> pub = client.CreatePublisher("dave0", 256, 10);
  ASSERT_TRUE(pub.ok());
  ASSERT_TRUE(client.RemovePublisher(*pub).ok());
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
  absl::StatusOr<Subscriber *> s = client.CreateSubscriber("dave2");
  ASSERT_TRUE(s.ok());
}

TEST_F(ClientTest, CreateSubscriberWithType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber *> s = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s.ok());
  ASSERT_EQ("foobar", (*s)->Type());
}

TEST_F(ClientTest, MismatchedSubscriberType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber *> s1 = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s1.ok());
  ASSERT_EQ("foobar", (*s1)->Type());

  absl::StatusOr<Subscriber *> s2 = client.CreateSubscriber(
      "dave2", subspace::SubscriberOptions().SetType("barfoo"));
  ASSERT_FALSE(s2.ok());
}

TEST_F(ClientTest, RemoveSubscriber) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber *> s = client.CreateSubscriber("dave0");
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(client.RemoveSubscriber(*s).ok());
}

TEST_F(ClientTest, CreateSubscriberThenPublisher) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber *> s = client.CreateSubscriber("dave3");
  ASSERT_TRUE(s.ok());

  absl::StatusOr<Publisher *> p = client.CreatePublisher("dave3", 300, 10);
  ASSERT_TRUE(p.ok());

  auto &counters = client.GetChannelCounters(*s);
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
}

TEST_F(ClientTest, CreateSubscriberThenPublisherSameType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber *> s = client.CreateSubscriber(
      "dave3", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s.ok());

  absl::StatusOr<Publisher *> p = client.CreatePublisher(
      "dave3", 300, 10, subspace::PublisherOptions().SetType("foobar"));
  ASSERT_TRUE(p.ok());

  auto &counters = client.GetChannelCounters(*s);
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
}

TEST_F(ClientTest, CreateSubscriberThenPublisherWrongType) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Subscriber *> s = client.CreateSubscriber(
      "dave3", subspace::SubscriberOptions().SetType("foobar"));
  ASSERT_TRUE(s.ok());

  absl::StatusOr<Publisher *> p = client.CreatePublisher(
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
  absl::StatusOr<Publisher *> pub = client.CreatePublisher("dave5", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
  ASSERT_TRUE(pub_status.ok());
}

TEST_F(ClientTest, PublishSingleMessageAndRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());
  absl::StatusOr<Publisher *> pub =
      pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub_client.GetMessageBuffer(*pub);
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub_client.PublishMessage(*pub, 6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber *> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Message> msg = sub_client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub_client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessagePollAndReadSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  absl::StatusOr<Subscriber *> sub = sub_client.CreateSubscriber("dave7");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Publisher *> pub =
      pub_client.CreatePublisher("dave7", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub_client.GetMessageBuffer(*pub);
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub_client.PublishMessage(*pub, 6);
  ASSERT_TRUE(pub_status.ok());

  struct pollfd fd = sub_client.GetPollFd(*sub);

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  absl::StatusOr<Message> msg = sub_client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub_client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishSingleMessagePollAndReadPublisherFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  absl::StatusOr<Publisher *> pub =
      pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber *> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<void *> buffer = pub_client.GetMessageBuffer(*pub);
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub_client.PublishMessage(*pub, 6);
  ASSERT_TRUE(pub_status.ok());

  struct pollfd fd = sub_client.GetPollFd(*sub);

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  absl::StatusOr<Message> msg = sub_client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Another read will get 0.
  msg = sub_client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(0, msg->length);
}

TEST_F(ClientTest, PublishMultipleMessagePollAndReadPublisherFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_TRUE(pub_client.Init(Socket()).ok());
  ASSERT_TRUE(sub_client.Init(Socket()).ok());

  absl::StatusOr<Publisher *> pub =
      pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber *> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_TRUE(sub.ok());

  for (int i = 0; i < 9; i++) {
    absl::StatusOr<void *> buffer = pub_client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status =
        pub_client.PublishMessage(*pub, len + 1);
    ASSERT_TRUE(pub_status.ok());
  }
  struct pollfd fd = sub_client.GetPollFd(*sub);

  int e = ::poll(&fd, 1, -1);
  ASSERT_EQ(1, e);

  for (;;) {
    absl::StatusOr<Message> msg = sub_client.ReadMessage(*sub);
    ASSERT_TRUE(msg.ok());
    if (msg->length == 0) {
      break;
    }
  }
}

TEST_F(ClientTest, ReliablePublisher1) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher *> pub = client.CreatePublisher(
      "rel_dave", 32, 5, subspace::PublisherOptions().SetReliable(true));
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<Subscriber *> sub = client.CreateSubscriber(
      "rel_dave", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_TRUE(sub.ok());

  auto &counters = client.GetChannelCounters(*pub);
  ASSERT_EQ(1, counters.num_pubs);
  ASSERT_EQ(1, counters.num_subs);
  ASSERT_EQ(1, counters.num_reliable_pubs);
  ASSERT_EQ(1, counters.num_reliable_subs);

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read the message from reliable subscriber.
  absl::StatusOr<Message> msg = client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_EQ(nullptr, *buffer);
  }

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&client, &pub](co::Coroutine *c) {
    struct pollfd fd = client.GetPollFd(*pub);
    c->Wait(fd);

    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  });

  // Read messages in coroutine.
  co::Coroutine c2(machine, [&client, &sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = client.ReadMessage(*sub);
      ASSERT_TRUE(msg.ok());
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = client.ReadMessage(*sub);
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
  absl::StatusOr<Subscriber *> sub = client.CreateSubscriber(
      "rel_dave", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<Publisher *> pub = client.CreatePublisher(
      "rel_dave", 32, 5, subspace::PublisherOptions().SetReliable(true));
  ASSERT_TRUE(pub.ok());

  // Publish a reliable message.
  {
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read the message from reliable subscriber.  This will make a server
  // call to get the reliable publishers.
  absl::StatusOr<Message> msg = client.ReadMessage(*sub);
  ASSERT_TRUE(msg.ok());
  ASSERT_EQ(6, msg->length);

  // Publish another set of messages.  We have 5 slots.  The subscriber
  // has one.  We can publish another 4 and then will get a nullptr
  // from GetMessageBuffer.
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  }

  // 5th message will get a nullptr because we don't have any
  // slots left.
  {
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_EQ(nullptr, *buffer);
  }

  co::CoroutineScheduler machine;

  // Wait for trigger event in coroutine.
  co::Coroutine c1(machine, [&client, &pub](co::Coroutine *c) {
    absl::StatusOr<struct pollfd> fd = client.GetPollFd(*pub);
    ASSERT_TRUE(fd.ok());
    c->Wait(*fd);

    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  });

  // Read another message in coroutine.
  co::Coroutine c2(machine, [&client, &sub](co::Coroutine *c) {
    for (int i = 0; i < 4; i++) {
      absl::StatusOr<Message> msg = client.ReadMessage(*sub);
      ASSERT_TRUE(msg.ok());
      ASSERT_EQ(6, msg->length);
    }
    // No messages left, will get 0 length and trigger publisher.
    absl::StatusOr<Message> msg = client.ReadMessage(*sub);
    ASSERT_TRUE(msg.ok());
    ASSERT_EQ(0, msg->length);
  });
  machine.Run();
}

TEST_F(ClientTest, DroppedMessage) {
  subspace::Client client;
  InitClient(client);

  absl::StatusOr<Subscriber *> sub = client.CreateSubscriber("rel_dave");
  ASSERT_TRUE(sub.ok());

  int num_dropped_messages = 0;
  client.RegisterDroppedMessageCallback(
      *sub, [&num_dropped_messages, &sub](Subscriber *s, int64_t num_dropped) {
        ASSERT_EQ(*sub, s);
        num_dropped_messages += num_dropped;
      });

  absl::StatusOr<Publisher *> pub = client.CreatePublisher("rel_dave", 32, 5);
  ASSERT_TRUE(pub.ok());

  // 5 slots. Fill 4 of them with messages.  Slots 0 to 3 will contain
  // messages and the publisher will have slot 4
  for (int i = 0; i < 4; i++) {
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read one message. This will be in slot 0.
  {
    absl::StatusOr<Message> msg = client.ReadMessage(*sub);
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
    absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  }

  // Read all messages in channel.
  for (;;) {
    absl::StatusOr<Message> msg = client.ReadMessage(*sub);
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
  absl::StatusOr<Publisher *> pub =
      pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());
  absl::StatusOr<void *> buffer = pub_client.GetMessageBuffer(*pub);
  ASSERT_TRUE(buffer.ok());
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub_client.PublishMessage(*pub, 6);
  ASSERT_TRUE(pub_status.ok());

  absl::StatusOr<Subscriber *> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub_client.ReadMessage<const char>(*sub);
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
  absl::StatusOr<Publisher *> pub =
      pub_client.CreatePublisher("dave6", 256, 10);
  ASSERT_TRUE(pub.ok());

  for (int i = 0; i < 2; i++) {
    absl::StatusOr<void *> buffer = pub_client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status =
        pub_client.PublishMessage(*pub, 6);
    ASSERT_TRUE(pub_status.ok());
  }

  absl::StatusOr<Subscriber *> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_TRUE(sub.ok());

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub_client.ReadMessage<const char>(*sub);
  ASSERT_TRUE(p.ok());
  const auto &ptr = *p;
  ASSERT_TRUE(static_cast<bool>(ptr));
  ASSERT_STREQ("foobar", ptr.get());

  // Create a weak_ptr from p.
  subspace::weak_ptr<const char> w(ptr);
  ASSERT_FALSE(w.expired());

  // Read second message and overwrite shared_ptr.
  p = sub_client.ReadMessage<const char>(*sub);
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

  absl::StatusOr<Publisher *> pub =
      pub_client.CreatePublisher("dave8", 256, 10);
  ASSERT_TRUE(pub.ok());

  absl::StatusOr<Subscriber *> sub = sub_client.CreateSubscriber("dave8");
  ASSERT_TRUE(sub.ok());

  std::vector<subspace::Message> msgs;
  for (int i = 0; i < 9; i++) {
    absl::StatusOr<void *> buffer = pub_client.GetMessageBuffer(*pub);
    ASSERT_TRUE(buffer.ok());
    char *buf = reinterpret_cast<char *>(*buffer);
    int len = snprintf(buf, 256, "foobar %d", i);

    absl::StatusOr<const Message> pub_status =
        pub_client.PublishMessage(*pub, len + 1);
    ASSERT_TRUE(pub_status.ok());
    msgs.push_back(*pub_status);
  }

  // Find an unknown message lower than all others.
  {
    absl::StatusOr<const Message> m = sub_client.FindMessage(*sub, 12345678);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(nullptr, m->buffer);
  }

  // Find an unknown message higher than all others.
  {
    absl::StatusOr<const Message> m = sub_client.FindMessage(*sub, -1);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(nullptr, m->buffer);
  }

  // Find a known message.
  {
    absl::StatusOr<const Message> m =
        sub_client.FindMessage(*sub, msgs[4].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[4].timestamp, m->timestamp);
    ASSERT_EQ(msgs[4].length, m->length);
    ASSERT_EQ(msgs[4].ordinal, m->ordinal);
  }

  // Find another known message.  This will change ownership of the subscriber's
  // slot.
  {
    absl::StatusOr<const Message> m =
        sub_client.FindMessage(*sub, msgs[7].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[7].timestamp, m->timestamp);
    ASSERT_EQ(msgs[7].length, m->length);
    ASSERT_EQ(msgs[7].ordinal, m->ordinal);
  }

  // Find first message.
  {
    absl::StatusOr<const Message> m =
        sub_client.FindMessage(*sub, msgs[0].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[0].timestamp, m->timestamp);
    ASSERT_EQ(msgs[0].length, m->length);
    ASSERT_EQ(msgs[0].ordinal, m->ordinal);
  }

  // Find last message.
  {
    absl::StatusOr<const Message> m =
        sub_client.FindMessage(*sub, msgs[8].timestamp);
    ASSERT_TRUE(m.ok());
    ASSERT_EQ(msgs[8].timestamp, m->timestamp);
    ASSERT_EQ(msgs[8].length, m->length);
    ASSERT_EQ(msgs[8].ordinal, m->ordinal);
  }
}
