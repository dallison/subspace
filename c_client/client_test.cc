// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "c_client/subspace.h"
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

    SubspacePublisherOptions pub_opts = subspace_publisher_options_default(256, 10);
    pub_opts.type.type = "foo";
    pub_opts.type.type_length = strlen(pub_opts.type.type);
    SubspacePublisher pub = subspace_create_publisher(client, "dave1", pub_opts);
    ASSERT_NE(nullptr, pub.publisher);

    SubspacePublisher pub2 = subspace_create_publisher(
            client, "dave1", subspace_publisher_options_default(256, 100));
    ASSERT_EQ(nullptr, pub2.publisher);
    std::cerr << pub2.error_message << std::endl;
    ASSERT_NE(nullptr, strstr(pub2.error_message, "Inconsistent publisher parameter"));

    SubspaceSubscriber sub =
            subspace_create_subscriber(client, "dave1", subspace_subscriber_options_default());
    ASSERT_NE(nullptr, sub.subscriber);

    subspace_remove_subscriber(&sub);
    subspace_remove_publisher(&pub);
    subspace_remove_publisher(&pub2);
    subspace_remove_client(&client);
}

TEST_F(ClientTest, PublishSingleMessageAndRead) {
    auto pub_client = subspace_create_client_with_socket(Socket().c_str());
    ASSERT_NE(nullptr, pub_client.client);
    auto sub_client = subspace_create_client_with_socket(Socket().c_str());
    ASSERT_NE(nullptr, sub_client.client);

    SubspacePublisher pub = subspace_create_publisher(
            pub_client, "dave2", subspace_publisher_options_default(256, 10));

    ASSERT_NE(nullptr, pub.publisher);
    SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 256);
    ASSERT_NE(nullptr, buffer.buffer);
    ASSERT_EQ(256, buffer.buffer_size);
    memcpy(buffer.buffer, "foobar", 6);
    const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
    ASSERT_NE(0, pub_status.length);
    ASSERT_NE(uint64_t(-1), pub_status.ordinal);
    ASSERT_NE(0, pub_status.timestamp);

    SubspaceSubscriber sub =
            subspace_create_subscriber(sub_client, "dave2", subspace_subscriber_options_default());
    ASSERT_NE(nullptr, sub.subscriber);
    SubspaceMessage msg = subspace_read_message(sub);
    ASSERT_NE(nullptr, msg.message);
    ASSERT_EQ(6, msg.length);
    ASSERT_EQ(0, memcmp(msg.buffer, "foobar", 6));
    subspace_free_message(&msg);
    subspace_free_message(&msg);  // NOP.

    ASSERT_EQ(256, subspace_get_subscriber_slot_size(sub));
    ASSERT_EQ(10, subspace_get_subscriber_num_slots(sub));

    // Read another message and get length 0.
    msg = subspace_read_message(sub);
    ASSERT_NE(nullptr, msg.message);
    ASSERT_EQ(0, msg.length);
    subspace_free_message(&msg);

    subspace_remove_subscriber(&sub);
    subspace_remove_subscriber(&sub);    // NOP
    subspace_remove_publisher(&pub);
    subspace_remove_publisher(&pub);     // NOP
    subspace_remove_client(&pub_client);
    subspace_remove_client(&pub_client);  // NOP
    subspace_remove_client(&sub_client);
}
