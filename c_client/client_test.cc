// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "c_client/subspace.h"
#include "co/coroutine.h"
#include "server/server.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <string.h>
#include <sys/resource.h>
#include <thread>

void SignalHandler(int sig) { printf("Signal %d", sig); }

class ClientTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
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
    printf("Stopping Subspace server\n");
    server_->Stop();

    // Wait for server to tell us that it's stopped.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
  void TearDown() override {}

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

TEST_F(ClientTest, CreatePublisherThenSubscriber) {
  SubspaceClient client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, client.client);

  SubspacePublisherOptions pub_opts =
      subspace_publisher_options_default(256, 10);
  pub_opts.type.type = "foo";
  pub_opts.type.type_length = strlen(pub_opts.type.type);
  SubspacePublisher pub = subspace_create_publisher(client, "dave1", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub2 = subspace_create_publisher(
      client, "dave1", subspace_publisher_options_default(256, 100));
  ASSERT_EQ(nullptr, pub2.publisher);
  ASSERT_TRUE(subspace_has_error());
  char *error_message = subspace_get_last_error();
  std::cerr << error_message << std::endl;
  ASSERT_NE(nullptr, strstr(error_message, "Inconsistent publisher parameter"));

  SubspaceSubscriber sub = subspace_create_subscriber(
      client, "dave1", subspace_subscriber_options_default());
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_FALSE(subspace_remove_publisher(&pub2));
  ASSERT_TRUE(subspace_remove_client(&client));
}

TEST_F(ClientTest, PublishSingleMessageAndRead) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "dave2", subspace_publisher_options_default(256, 10));

  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 256);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_EQ(256, buffer.buffer_size);
  memcpy(buffer.buffer, "foobar", 6);

  const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(uint64_t(-1), pub_status.ordinal);
  ASSERT_NE(0, pub_status.timestamp);

  SubspaceSubscriber sub = subspace_create_subscriber(
      sub_client, "dave2", subspace_subscriber_options_default());
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(6, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "foobar", 6));
  subspace_free_message(&msg);
  subspace_free_message(&msg); // NOP.

  ASSERT_EQ(256, subspace_get_subscriber_slot_size(sub));
  ASSERT_EQ(10, subspace_get_subscriber_num_slots(sub));

  // Read another message and get length 0.
  msg = subspace_read_message(sub);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, msg.message);
  ASSERT_EQ(0, msg.length);
  ASSERT_TRUE(subspace_free_message(&msg));

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_FALSE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_FALSE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_FALSE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

// Callback to read the messages.  Doesn't free them but instead stores
// them.
std::vector<SubspaceMessage> messages_read;
void MessageCallback(SubspaceSubscriber subscriber, SubspaceMessage msg) {
  ASSERT_EQ(6, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "foobar", 6));
  messages_read.push_back(msg);
}

TEST_F(ClientTest, SubscriberCallbacks) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "dave2", subspace_publisher_options_default(256, 10));

  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  // Create a subscriber and set a callback.  We have 3 active messages at once.
  SubspaceSubscriberOptions sub_opts = subspace_subscriber_options_default();
  sub_opts.max_active_messages = 3;

  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "dave2", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  auto status = subspace_register_subscriber_callback(sub, MessageCallback);
  ASSERT_TRUE(status);
  ASSERT_FALSE(subspace_has_error());

  // Publish 6 messages.
  for (int i = 0; i < 6; i++) {
    SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 256);
    ASSERT_FALSE(subspace_has_error());
    ASSERT_NE(nullptr, buffer.buffer);
    ASSERT_EQ(256, buffer.buffer_size);
    snprintf(reinterpret_cast<char *>(buffer.buffer), 256, "foobar%d", i);

    const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
    ASSERT_NE(0, pub_status.length);
    ASSERT_FALSE(subspace_has_error());
    ASSERT_NE(uint64_t(-1), pub_status.ordinal);
    ASSERT_NE(0, pub_status.timestamp);
  }

  // Read all available messages.  Since we have a max number of 3 active
  // messages, we will get 3 messages at a time.
  ASSERT_TRUE(subspace_process_all_messages(sub));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(3, messages_read.size());

  // Remove one message from the vector.
  SubspaceMessage m = messages_read.back();
  subspace_free_message(&m);
  messages_read.pop_back();

  // This will read one message since we have 2 currently active messages in the
  // vector.
  ASSERT_TRUE(subspace_process_all_messages(sub));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(3, messages_read.size());

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

int num_resizes = 0;

bool ResizeCallback(SubspacePublisher publisher, int32_t old_size,
                    int32_t new_size) {
  EXPECT_EQ(256, old_size);
  EXPECT_EQ(1024, new_size);
  num_resizes++;
  return true;
}

TEST_F(ClientTest, Resize) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "dave3", subspace_publisher_options_default(256, 10));

  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());
  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 256);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_EQ(256, buffer.buffer_size);
  memcpy(buffer.buffer, "foobar", 6);
  const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  // Register a resize callback.  This will be called when the
  // publisher resizes the buffer.
  ASSERT_TRUE(subspace_register_resize_callback(pub, ResizeCallback));
  ASSERT_FALSE(subspace_has_error());

  // Publish a message that is bigger than the current slot size.
  SubspaceMessageBuffer buffer2 = subspace_get_message_buffer(pub, 1024);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer2.buffer);
  ASSERT_EQ(1024, buffer2.buffer_size);
  memcpy(buffer2.buffer, "foobar2", 7);
  const SubspaceMessage pub_status2 = subspace_publish_message(pub, 7);
  ASSERT_NE(0, pub_status2.length);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(1, num_resizes);

  ASSERT_TRUE(subspace_unregister_resize_callback(pub));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
}

int num_dropped_messages = 0;
void DroppedMessageCallback(SubspaceSubscriber subscriber, int64_t num_dropped) {
  num_dropped_messages += num_dropped;
}

TEST_F(ClientTest, DroppedMessage) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "dave3", subspace_publisher_options_default(256, 10));

  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  // Create a subscriber and set a callback for dropped messages.
  SubspaceSubscriberOptions sub_opts = subspace_subscriber_options_default();
  sub_opts.max_active_messages = 3;
  sub_opts.log_dropped_messages = true;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "dave3", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());
  auto status =
      subspace_register_dropped_message_callback(sub, DroppedMessageCallback);
  ASSERT_TRUE(status);
  ASSERT_FALSE(subspace_has_error());

  // Publish 1 message and read it.  This will prime the subscriber so that it
  // knows what message to expect.
  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 256);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_EQ(256, buffer.buffer_size);
  memcpy(buffer.buffer, "foobar", 6);
  const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(uint64_t(-1), pub_status.ordinal);
  ASSERT_NE(0, pub_status.timestamp);
  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, msg.message);
  ASSERT_EQ(6, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "foobar", 6));
  subspace_free_message(&msg);


  // Publish 20 messages.
  for (int i = 0; i < 20; i++) {
    SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 256);
    ASSERT_FALSE(subspace_has_error());
    ASSERT_NE(nullptr, buffer.buffer);
    ASSERT_EQ(256, buffer.buffer_size);
    snprintf(reinterpret_cast<char *>(buffer.buffer), 256, "foobar%d", i);

    const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
    ASSERT_NE(0, pub_status.length);
    ASSERT_FALSE(subspace_has_error());
    ASSERT_NE(uint64_t(-1), pub_status.ordinal);
    ASSERT_NE(0, pub_status.timestamp);
  }
  
  // Read all available messages.
  for (;;) {
    SubspaceMessage msg = subspace_read_message(sub);
    ASSERT_FALSE(subspace_has_error());
    if (msg.length == 0) {
      break;
    }
    subspace_free_message(&msg);
  }

  // Why 12 messages.  We have 10 slots with 1 publisher and 1 subscriber.  This leaves us with 7
  // slots that can hold messages (we need one spare slot).  
  ASSERT_EQ(12, num_dropped_messages);
}
