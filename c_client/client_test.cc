// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "c_client/subspace.h"
#include "co/coroutine.h"
#include "server/server.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <algorithm>
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <string.h>
#include <sys/resource.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

ABSL_FLAG(bool, use_split_buffers, false,
          "Run C client publishers with split-buffer payload storage");

void SignalHandler(int sig) { printf("Signal %d", sig); }

class ClientTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    printf("Starting Subspace server\n");
#if defined(__ANDROID__)
    char socket_name_template[] = "/data/local/tmp/subspaceXXXXXX"; // NOLINT
#else
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
#endif
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    // The server will write to this pipe to notify us when it
    // has started and stopped.  This end of the pipe is blocking.
    (void)pipe(server_pipe_);

    server_ = std::make_unique<subspace::Server>(
        scheduler_, socket_, "", 0, 0,
        /*local=*/true, server_pipe_[1], /*initial_ordinal=*/1,
        /*wait_for_clients=*/true);

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
    server_->CleanupAfterSession();
    (void)remove(socket_.c_str());
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
#if defined(__ANDROID__)
std::string ClientTest::socket_ = "/data/local/tmp/subspace";
#else
std::string ClientTest::socket_ = "/tmp/subspace";
#endif
int ClientTest::server_pipe_[2];
std::unique_ptr<subspace::Server> ClientTest::server_;
std::thread ClientTest::server_thread_;

bool AddressListContains(void **addresses, size_t count, const void *address) {
  return std::find(addresses, addresses + count, const_cast<void *>(address)) !=
         addresses + count;
}

bool SubspaceStringEquals(SubspaceString value, const char *expected) {
  return value.length == strlen(expected) &&
         memcmp(value.data, expected, value.length) == 0;
}

void SumChecksumCallback(const SubspaceChecksumSpan *spans, size_t span_count,
                         uint8_t *checksum, size_t checksum_size,
                         void *user_data) {
  uint32_t sum = 0;
  for (size_t i = 0; i < span_count; i++) {
    for (size_t j = 0; j < spans[i].size; j++) {
      sum += spans[i].data[j];
    }
  }
  if (checksum_size == 0) {
    return;
  }
  memset(checksum, 0, checksum_size);
  memcpy(checksum, &sum, std::min(sizeof(sum), checksum_size));
  if (user_data != nullptr && *static_cast<bool *>(user_data)) {
    checksum[0] ^= 0xFF;
  }
}

bool ResizeTransformCallback(void *buffer, int64_t size, int64_t *out_size,
                             void *user_data) {
  auto *called = static_cast<bool *>(user_data);
  if (called != nullptr) {
    *called = true;
  }
  if (buffer == nullptr || out_size == nullptr || size <= 0) {
    return false;
  }
  *out_size = size - 1;
  return true;
}

bool FailingTransformCallback(void * /*buffer*/, int64_t /*size*/,
                              int64_t * /*out_size*/, void * /*user_data*/) {
  return false;
}

struct TestCSplitAllocation {
  std::unique_ptr<char[]> memory;
  size_t size = 0;
};

struct TestCSplitBufferState {
  uintptr_t next_handle = 5000;
  std::unordered_map<uintptr_t, TestCSplitAllocation> allocations;
  int allocate_count = 0;
  int map_count = 0;
  int unmap_count = 0;
  int free_count = 0;
};

bool TestCSplitAllocate(const SubspaceSplitBufferInfo *info,
                        SubspaceSplitBufferMapping *mapping, void *user_data) {
  auto *state = static_cast<TestCSplitBufferState *>(user_data);
  if (info == nullptr || mapping == nullptr || state == nullptr ||
      info->allocation_size == 0) {
    return false;
  }

  auto memory = std::make_unique<char[]>(info->allocation_size);
  char *address = memory.get();
  uintptr_t handle = ++state->next_handle;
  state->allocations.emplace(
      handle, TestCSplitAllocation{std::move(memory),
                                   static_cast<size_t>(info->allocation_size)});
  state->allocate_count++;

  mapping->handle = handle;
  mapping->address = address;
  mapping->size = static_cast<size_t>(info->allocation_size);
  mapping->private_data = address;
  return true;
}

bool TestCSplitMap(const SubspaceSplitBufferInfo *info,
                   SubspaceSplitBufferMapping *mapping, void *user_data) {
  auto *state = static_cast<TestCSplitBufferState *>(user_data);
  if (info == nullptr || mapping == nullptr || state == nullptr) {
    return false;
  }

  auto it = state->allocations.find(info->handle);
  if (it == state->allocations.end()) {
    return false;
  }

  state->map_count++;
  mapping->handle = info->handle;
  mapping->address = it->second.memory.get();
  mapping->size = it->second.size;
  mapping->private_data = mapping->address;
  return true;
}

bool TestCSplitUnmap(const SubspaceSplitBufferInfo *info,
                     const SubspaceSplitBufferMapping *mapping,
                     void *user_data) {
  auto *state = static_cast<TestCSplitBufferState *>(user_data);
  if (info == nullptr || mapping == nullptr || state == nullptr ||
      mapping->address == nullptr || mapping->handle == 0) {
    return false;
  }

  state->unmap_count++;
  return true;
}

bool TestCSplitFree(const SubspaceSplitBufferInfo *info,
                    const SubspaceSplitBufferMapping *mapping,
                    void *user_data) {
  auto *state = static_cast<TestCSplitBufferState *>(user_data);
  if (info == nullptr || mapping == nullptr || state == nullptr ||
      mapping->handle == 0) {
    return false;
  }

  state->free_count++;
  state->allocations.erase(mapping->handle);
  return true;
}

bool TestCSplitFailAllocate(const SubspaceSplitBufferInfo * /*info*/,
                            SubspaceSplitBufferMapping * /*mapping*/,
                            void * /*user_data*/) {
  return false;
}

bool TestCSplitFailMap(const SubspaceSplitBufferInfo * /*info*/,
                       SubspaceSplitBufferMapping * /*mapping*/,
                       void * /*user_data*/) {
  return false;
}

TestCSplitBufferState default_split_buffer_state;

SubspacePublisherOptions CPublisherOptionsDefault(int32_t slot_size,
                                                  int num_slots) {
  SubspacePublisherOptions options =
      subspace_publisher_options_default(slot_size, num_slots);
  if (absl::GetFlag(FLAGS_use_split_buffers)) {
    options.use_split_buffers = true;
    options.split_callbacks = {
        .allocate = TestCSplitAllocate,
        .map = TestCSplitMap,
        .unmap = TestCSplitUnmap,
        .free = TestCSplitFree,
        .user_data = &default_split_buffer_state,
    };
  }
  return options;
}

SubspaceSubscriberOptions CSubscriberOptionsDefault() {
  SubspaceSubscriberOptions options = subspace_subscriber_options_default();
  if (absl::GetFlag(FLAGS_use_split_buffers)) {
    options.split_callbacks = {
        .allocate = TestCSplitAllocate,
        .map = TestCSplitMap,
        .unmap = TestCSplitUnmap,
        .free = TestCSplitFree,
        .user_data = &default_split_buffer_state,
    };
  }
  return options;
}

TEST_F(ClientTest, CreatePublisherThenSubscriber) {
  SubspaceClient client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, client.client);

  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(256, 10);
  pub_opts.type.type = "foo";
  pub_opts.type.type_length = strlen(pub_opts.type.type);
  SubspacePublisher pub = subspace_create_publisher(client, "dave1", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub2 = subspace_create_publisher(
      client, "dave1", CPublisherOptionsDefault(256, 100));
  ASSERT_EQ(nullptr, pub2.publisher);
  ASSERT_TRUE(subspace_has_error());
  char *error_message = subspace_get_last_error();
  std::cerr << error_message << std::endl;
  ASSERT_NE(nullptr, strstr(error_message, "Failed to add publisher to dave1"));

  SubspaceSubscriber sub =
      subspace_create_subscriber(client, "dave1", CSubscriberOptionsDefault());
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
      pub_client, "dave2", CPublisherOptionsDefault(256, 10));

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
      sub_client, "dave2", CSubscriberOptionsDefault());
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
  // Empty reads no longer allocate a wrapper, so msg.message is null and
  // subspace_free_message is a no-op (returns false: nothing to free).
  msg = subspace_read_message(sub);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(nullptr, msg.message);
  ASSERT_EQ(0, msg.length);
  ASSERT_FALSE(subspace_free_message(&msg));

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_FALSE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_FALSE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_FALSE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, MetadataAddressesAndDescriptorHelpers) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket_and_name(
      Socket().c_str(), "c-helper-sub");
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(128, 4);
  pub_opts.type.type = "helper_type";
  pub_opts.type.type_length = strlen(pub_opts.type.type);
  pub_opts.metadata_size = 8;
  SubspacePublisher pub =
      subspace_create_publisher(pub_client, "c_helpers", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  ASSERT_GE(subspace_get_publisher_fd(pub), -1);
  struct pollfd pub_poll_fd = subspace_get_publisher_poll_fd(pub);
  ASSERT_GE(pub_poll_fd.fd, -1);
  ASSERT_EQ(8, subspace_get_publisher_metadata_size(pub));

  void **publisher_addresses = nullptr;
  size_t publisher_address_count = 0;
  ASSERT_TRUE(subspace_get_publisher_addresses(pub, &publisher_addresses,
                                               &publisher_address_count));
  ASSERT_EQ(4U, publisher_address_count);
  ASSERT_NE(nullptr, publisher_addresses);

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_TRUE(AddressListContains(publisher_addresses, publisher_address_count,
                                  buffer.buffer));
  memcpy(buffer.buffer, "helpers", 7);

  size_t metadata_size = 0;
  void *publisher_metadata =
      subspace_get_publisher_metadata(pub, &metadata_size);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, publisher_metadata);
  ASSERT_EQ(8U, metadata_size);
  memcpy(publisher_metadata, "metadata", 8);

  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
  sub_opts.type.type = pub_opts.type.type;
  sub_opts.type.type_length = pub_opts.type.type_length;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "c_helpers", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  SubspaceTypeInfo type_info = subspace_get_subscriber_type(sub);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(pub_opts.type.type_length, type_info.type_length);
  ASSERT_EQ(0,
            memcmp(pub_opts.type.type, type_info.type, type_info.type_length));
  ASSERT_GT(subspace_get_subscriber_fd(sub), 0);
  struct pollfd sub_poll_fd = subspace_get_subscriber_poll_fd(sub);
  ASSERT_GT(sub_poll_fd.fd, 0);
  ASSERT_NE(0, sub_poll_fd.events);
  ASSERT_EQ(8, subspace_get_subscriber_metadata_size(sub));

  void **subscriber_addresses = nullptr;
  size_t subscriber_address_count = 0;
  ASSERT_TRUE(subspace_get_subscriber_addresses(sub, &subscriber_addresses,
                                                &subscriber_address_count));
  ASSERT_EQ(4U, subscriber_address_count);
  ASSERT_NE(nullptr, subscriber_addresses);

  const SubspaceMessage pub_status = subspace_publish_message(pub, 7);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  ASSERT_TRUE(subspace_wait_for_subscriber_with_timeout(sub, 1000));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_TRUE(subspace_wait_for_subscriber(sub));
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessage msg =
      subspace_read_message_with_mode(sub, kSubspaceReadNewest);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(7, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "helpers", 7));
  ASSERT_TRUE(AddressListContains(subscriber_addresses,
                                  subscriber_address_count, msg.buffer));

  const void *subscriber_metadata =
      subspace_get_subscriber_metadata(sub, &metadata_size);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, subscriber_metadata);
  ASSERT_EQ(8U, metadata_size);
  ASSERT_EQ(0, memcmp(subscriber_metadata, "metadata", 8));

  subspace_free_message(&msg);
  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, MuxAndChecksumOptions) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  const char mux[] = "c_mux_options";
  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(128, 4);
  pub_opts.mux = mux;
  pub_opts.mux_length = strlen(mux);
  pub_opts.checksum = true;
  pub_opts.checksum_size = 8;
  SubspacePublisher pub =
      subspace_create_publisher(pub_client, "c_mux_channel", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
  sub_opts.mux = mux;
  sub_opts.mux_length = strlen(mux);
  sub_opts.checksum = true;
  sub_opts.pass_checksum_errors = true;
  sub_opts.keep_active_message = true;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "c_mux_channel", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer.buffer);
  memcpy(buffer.buffer, "muxsum", 6);

  const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(6, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "muxsum", 6));

  subspace_free_message(&msg);
  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, ChecksumCallbacks) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(128, 4);
  pub_opts.checksum = true;
  SubspacePublisher pub =
      subspace_create_publisher(pub_client, "c_checksum_callbacks", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
  sub_opts.checksum = true;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "c_checksum_callbacks", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  ASSERT_FALSE(
      subspace_register_publisher_checksum_callback(pub, nullptr, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_register_subscriber_checksum_callback(sub, nullptr, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_TRUE(subspace_register_publisher_checksum_callback(
      pub, SumChecksumCallback, nullptr));
  ASSERT_TRUE(subspace_register_subscriber_checksum_callback(
      sub, SumChecksumCallback, nullptr));

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_FALSE(subspace_has_error());
  memcpy(buffer.buffer, "abc", 3);
  SubspaceMessage pub_status = subspace_publish_message(pub, 3);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(3, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "abc", 3));
  subspace_free_message(&msg);

  buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_FALSE(subspace_has_error());
  memcpy(buffer.buffer, "def", 3);
  pub_status = subspace_publish_message(pub, 3);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());
  static_cast<char *>(buffer.buffer)[1] = 'X';

  msg = subspace_read_message(sub);
  ASSERT_EQ(nullptr, msg.message);
  ASSERT_TRUE(subspace_has_error());
  ASSERT_NE(nullptr,
            strstr(subspace_get_last_error(), "Checksum verification failed"));

  ASSERT_TRUE(subspace_unregister_publisher_checksum_callback(pub));
  ASSERT_TRUE(subspace_unregister_subscriber_checksum_callback(sub));

  buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_FALSE(subspace_has_error());
  memcpy(buffer.buffer, "reset", 5);
  pub_status = subspace_publish_message(pub, 5);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  msg = subspace_read_message(sub);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(5, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "reset", 5));
  subspace_free_message(&msg);

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, ChecksumCallbackMismatchedSubscriber) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(128, 4);
  pub_opts.checksum = true;
  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "c_checksum_mismatch_callbacks", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
  sub_opts.checksum = true;
  SubspaceSubscriber sub = subspace_create_subscriber(
      sub_client, "c_checksum_mismatch_callbacks", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  bool mismatch = true;
  ASSERT_TRUE(subspace_register_publisher_checksum_callback(
      pub, SumChecksumCallback, nullptr));
  ASSERT_TRUE(subspace_register_subscriber_checksum_callback(
      sub, SumChecksumCallback, &mismatch));

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_FALSE(subspace_has_error());
  memcpy(buffer.buffer, "mismatch", 8);
  SubspaceMessage pub_status = subspace_publish_message(pub, 8);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_EQ(nullptr, msg.message);
  ASSERT_TRUE(subspace_has_error());
  ASSERT_NE(nullptr,
            strstr(subspace_get_last_error(), "Checksum verification failed"));

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, MessageFieldsFindAndBulkRead) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);

  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(128, 6);
  pub_opts.metadata_size = 4;
  SubspacePublisher pub =
      subspace_create_publisher(pub_client, "c_message_fields", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);

  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
  sub_opts.max_active_messages = 3;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "c_message_fields", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);

  SubspaceMessage published[3];
  for (int i = 0; i < 3; i++) {
    SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
    ASSERT_NE(nullptr, buffer.buffer);
    int len =
        snprintf(reinterpret_cast<char *>(buffer.buffer), 64, "bulk%d", i);
    published[i] = subspace_publish_message(pub, len + 1);
    ASSERT_NE(uint64_t(-1), published[i].ordinal);
    ASSERT_NE(0U, published[i].timestamp);
    ASSERT_GE(published[i].slot_id, 0);
    ASSERT_FALSE(published[i].checksum_error);
  }

  SubspaceMessage found = subspace_find_message(sub, published[1].timestamp);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, found.message);
  ASSERT_EQ(published[1].timestamp, found.timestamp);
  ASSERT_EQ(published[1].ordinal, found.ordinal);
  ASSERT_EQ(published[1].slot_id, found.slot_id);
  ASSERT_EQ(0, memcmp(found.buffer, "bulk1", 6));
  ASSERT_TRUE(subspace_free_message(&found));

  SubspaceMessage *messages = nullptr;
  size_t count = 0;
  ASSERT_TRUE(
      subspace_get_all_messages(sub, kSubspaceReadNext, &messages, &count));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(3U, count);
  ASSERT_NE(nullptr, messages);
  for (size_t i = 0; i < count; i++) {
    ASSERT_NE(nullptr, messages[i].message);
    ASSERT_GE(messages[i].slot_id, 0);
    ASSERT_EQ(-1, messages[i].vchan_id);
    ASSERT_FALSE(messages[i].is_activation);
    ASSERT_FALSE(messages[i].checksum_error);
  }
  ASSERT_TRUE(subspace_free_messages(messages, count));

  ASSERT_TRUE(subspace_clear_active_message(sub));
  ASSERT_TRUE(subspace_trigger_reliable_publishers(sub));
  ASSERT_TRUE(subspace_trigger_subscriber(sub));
  ASSERT_TRUE(subspace_untrigger_subscriber(sub));

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, ChecksumPassErrorsMessageField) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);

  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(128, 4);
  pub_opts.checksum = true;
  SubspacePublisher pub =
      subspace_create_publisher(pub_client, "c_checksum_pass_field", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);

  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
  sub_opts.checksum = true;
  sub_opts.pass_checksum_errors = true;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "c_checksum_pass_field", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);

  ASSERT_TRUE(subspace_register_publisher_checksum_callback(
      pub, SumChecksumCallback, nullptr));
  ASSERT_TRUE(subspace_register_subscriber_checksum_callback(
      sub, SumChecksumCallback, nullptr));

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  memcpy(buffer.buffer, "field", 5);
  SubspaceMessage pub_status = subspace_publish_message(pub, 5);
  ASSERT_NE(0U, pub_status.length);
  static_cast<char *>(buffer.buffer)[0] = 'F';

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_TRUE(msg.checksum_error);
  ASSERT_EQ(5U, msg.length);
  ASSERT_TRUE(subspace_free_message(&msg));

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, ClientPublisherSubscriberIntrospection) {
  auto client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, client.client);
  ASSERT_TRUE(subspace_set_client_debug(client, false));
  ASSERT_TRUE(subspace_set_client_thread_safe(client, false));

  const char type[] = "c_introspection_type";
  const char mux[] = "c_introspection_mux";
  SubspacePublisherOptions pub_opts = CPublisherOptionsDefault(192, 6);
  pub_opts.type.type = type;
  pub_opts.type.type_length = strlen(type);
  pub_opts.mux = mux;
  pub_opts.mux_length = strlen(mux);
  pub_opts.metadata_size = 8;
  pub_opts.subscriber_queue_size = 12;
  SubspacePublisher pub =
      subspace_create_publisher(client, "c_introspection", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);

  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
  sub_opts.subscriber_queue_size = 4;
  sub_opts.type.type = type;
  sub_opts.type.type_length = strlen(type);
  sub_opts.mux = mux;
  sub_opts.mux_length = strlen(mux);
  SubspaceSubscriber sub =
      subspace_create_subscriber(client, "c_introspection", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);

  bool exists = false;
  ASSERT_TRUE(subspace_channel_exists(client, "c_introspection", &exists));
  ASSERT_TRUE(exists);

  SubspaceChannelInfo info = {};
  ASSERT_TRUE(subspace_get_channel_info(client, "c_introspection", &info));
  ASSERT_TRUE(SubspaceStringEquals(info.channel_name, "c_introspection"));
  ASSERT_TRUE(SubspaceStringEquals(info.type, type));
  ASSERT_EQ(1, info.num_publishers);
  ASSERT_EQ(1, info.num_subscribers);

  SubspaceChannelInfo *infos = nullptr;
  size_t info_count = 0;
  ASSERT_TRUE(subspace_get_all_channel_info(client, &infos, &info_count));
  ASSERT_NE(nullptr, infos);
  ASSERT_GT(info_count, 0U);

  SubspaceChannelStats stats = {};
  ASSERT_TRUE(subspace_get_channel_stats(client, "c_introspection", &stats));
  ASSERT_TRUE(SubspaceStringEquals(stats.channel_name, "c_introspection"));
  SubspaceChannelStats *all_stats = nullptr;
  size_t stats_count = 0;
  ASSERT_TRUE(subspace_get_all_channel_stats(client, &all_stats, &stats_count));
  ASSERT_NE(nullptr, all_stats);
  ASSERT_GT(stats_count, 0U);

  SubspaceChannelCounters counters = {};
  ASSERT_TRUE(
      subspace_get_channel_counters(client, "c_introspection", &counters));
  ASSERT_GE(counters.num_pubs, 1);
  ASSERT_GE(counters.num_subs, 1);
  ASSERT_TRUE(subspace_get_publisher_counters(pub, &counters));
  ASSERT_TRUE(subspace_get_publisher_channel_counters(pub, &counters));
  ASSERT_TRUE(subspace_get_subscriber_counters(sub, &counters));
  ASSERT_TRUE(subspace_get_subscriber_channel_counters(sub, &counters));

  ASSERT_FALSE(subspace_is_publisher_reliable(pub));
  ASSERT_FALSE(subspace_is_publisher_local(pub));
  ASSERT_FALSE(subspace_is_publisher_fixed_size(pub));
  ASSERT_FALSE(subspace_is_publisher_for_tunnel(pub));
  ASSERT_EQ(192, subspace_get_publisher_slot_size(pub));
  ASSERT_EQ(6, subspace_get_publisher_num_slots(pub));
  ASSERT_EQ(12, subspace_get_publisher_queue_size(pub));
  ASSERT_TRUE(SubspaceStringEquals(subspace_get_publisher_name(pub),
                                   "c_introspection"));
  ASSERT_TRUE(SubspaceStringEquals(subspace_get_publisher_type(pub), type));
  ASSERT_TRUE(SubspaceStringEquals(subspace_get_publisher_mux(pub), mux));
  ASSERT_EQ(8, subspace_get_publisher_metadata_size(pub));
  ASSERT_EQ(4, subspace_get_publisher_checksum_size(pub));
  ASSERT_GE(subspace_get_publisher_prefix_size(pub), 64);
  ASSERT_GE(subspace_get_publisher_current_slot_id(pub), 0);
  ASSERT_GE(subspace_get_publisher_virtual_memory_usage(pub), 0U);
  ASSERT_GT(subspace_get_publisher_buffer_shared_memory_name(pub, 0).length,
            0U);

  ASSERT_FALSE(subspace_is_subscriber_reliable(sub));
  ASSERT_FALSE(subspace_is_subscriber_for_tunnel(sub));
  ASSERT_FALSE(subspace_is_subscriber_placeholder(sub));
  ASSERT_TRUE(SubspaceStringEquals(subspace_get_subscriber_name(sub),
                                   "c_introspection"));
  ASSERT_TRUE(SubspaceStringEquals(subspace_get_subscriber_mux(sub), mux));
  ASSERT_EQ(-1, subspace_get_subscriber_configured_vchan_id(sub));
  ASSERT_GE(subspace_get_subscriber_virtual_channel_id(sub), -1);
  ASSERT_GE(subspace_get_subscriber_num_subscribers(sub, -1), 0);
  ASSERT_EQ(0, subspace_get_subscriber_num_active_messages(sub));
  ASSERT_EQ(8, subspace_get_subscriber_metadata_size(sub));
  ASSERT_EQ(4, subspace_get_subscriber_checksum_size(sub));
  ASSERT_EQ(4, subspace_get_subscriber_queue_size(sub));
  ASSERT_GE(subspace_get_subscriber_prefix_size(sub), 64);
  ASSERT_GE(subspace_get_subscriber_virtual_memory_usage(sub), 0U);

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&client));
}

TEST_F(ClientTest, TransformCallbacksAndDiagnostics) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);

  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "c_transform_callbacks", CPublisherOptionsDefault(128, 4));
  ASSERT_NE(nullptr, pub.publisher);
  SubspaceSubscriber sub = subspace_create_subscriber(
      sub_client, "c_transform_callbacks", CSubscriberOptionsDefault());
  ASSERT_NE(nullptr, sub.subscriber);

  bool send_called = false;
  bool receive_called = false;
  ASSERT_TRUE(subspace_register_publisher_on_send_callback(
      pub, ResizeTransformCallback, &send_called));
  ASSERT_TRUE(subspace_register_subscriber_on_receive_callback(
      sub, ResizeTransformCallback, &receive_called));

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  memcpy(buffer.buffer, "callback", 8);
  SubspaceMessage pub_status = subspace_publish_message(pub, 8);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(7U, pub_status.length);
  ASSERT_TRUE(send_called);

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, msg.message);
  ASSERT_EQ(6U, msg.length);
  ASSERT_TRUE(receive_called);

  SubspaceMessageSlot slot = subspace_get_subscriber_slot(sub, msg.slot_id);
  SubspaceMessageSlotSnapshot slot_snapshot = {};
  ASSERT_TRUE(subspace_snapshot_message_slot(slot, &slot_snapshot));
  ASSERT_EQ(msg.slot_id, slot_snapshot.id);
  SubspaceMessagePrefix prefix = subspace_get_subscriber_prefix(sub, slot);
  SubspaceMessagePrefixSnapshot prefix_snapshot = {};
  ASSERT_TRUE(subspace_snapshot_message_prefix(prefix, &prefix_snapshot));
  ASSERT_EQ(msg.slot_id, prefix_snapshot.slot_id);

  SubspaceMessageSlot pub_slot = subspace_get_publisher_current_slot(pub);
  SubspaceMessageSlotSnapshot pub_slot_snapshot = {};
  ASSERT_TRUE(subspace_snapshot_message_slot(pub_slot, &pub_slot_snapshot));
  SubspaceMessagePrefix pub_prefix =
      subspace_get_publisher_prefix(pub, pub_slot);
  SubspaceMessagePrefixSnapshot pub_prefix_snapshot = {};
  ASSERT_TRUE(
      subspace_snapshot_message_prefix(pub_prefix, &pub_prefix_snapshot));

  const char *dump = nullptr;
  size_t dump_length = 0;
  ASSERT_TRUE(subspace_dump_publisher_slots(pub, &dump, &dump_length));
  ASSERT_NE(nullptr, dump);
  ASSERT_GT(dump_length, 0U);
  ASSERT_TRUE(subspace_dump_subscriber_slots(sub, &dump, &dump_length));
  ASSERT_NE(nullptr, dump);
  ASSERT_GT(dump_length, 0U);

  ASSERT_TRUE(subspace_free_message(&msg));
  ASSERT_TRUE(subspace_unregister_publisher_on_send_callback(pub));
  ASSERT_TRUE(subspace_unregister_subscriber_on_receive_callback(sub));

  ASSERT_TRUE(subspace_register_publisher_on_send_callback(
      pub, FailingTransformCallback, nullptr));
  buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  memcpy(buffer.buffer, "fail", 4);
  pub_status = subspace_publish_message(pub, 4);
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(0U, pub_status.length);
  ASSERT_TRUE(subspace_unregister_publisher_on_send_callback(pub));

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, PublisherCancelAndWaitErrors) {
  auto client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, client.client);
  ASSERT_TRUE(subspace_set_client_thread_safe(client, true));

  SubspacePublisher pub = subspace_create_publisher(
      client, "c_cancel_wait", CPublisherOptionsDefault(128, 4));
  ASSERT_NE(nullptr, pub.publisher);

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  memcpy(buffer.buffer, "cancel", 6);
  ASSERT_TRUE(subspace_cancel_publish(pub));
  buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_NE(nullptr, buffer.buffer);
  memcpy(buffer.buffer, "sent", 4);
  SubspaceMessage pub_status = subspace_publish_message(pub, 4);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(4U, pub_status.length);

  ASSERT_FALSE(subspace_wait_for_publisher_with_timeout(pub, 1));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_wait_for_publisher_with_fd_and_timeout(pub, 0, 1));
  ASSERT_TRUE(subspace_has_error());

  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&client));
}

TEST_F(ClientTest, SplitBufferCallbacksPublishAndRead) {
  TestCSplitBufferState state;

  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspaceSplitBufferCallbacks callbacks = {
      .allocate = TestCSplitAllocate,
      .map = TestCSplitMap,
      .unmap = TestCSplitUnmap,
      .free = TestCSplitFree,
      .user_data = &state,
  };

  SubspacePublisherOptions pub_opts = subspace_publisher_options_default(96, 3);
  pub_opts.use_split_buffers = true;
  pub_opts.split_callbacks = callbacks;
  SubspacePublisher pub =
      subspace_create_publisher(pub_client, "c_split_buffers", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(3, state.allocate_count);

  uintptr_t *publisher_handles = nullptr;
  size_t publisher_handle_count = 0;
  ASSERT_TRUE(subspace_get_publisher_split_buffer_handles(
      pub, &publisher_handles, &publisher_handle_count));
  ASSERT_EQ(3U, publisher_handle_count);
  ASSERT_NE(nullptr, publisher_handles);

  SubspaceSubscriberOptions sub_opts = subspace_subscriber_options_default();
  sub_opts.split_callbacks = callbacks;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "c_split_buffers", sub_opts);
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(3, state.map_count);

  uintptr_t *subscriber_handles = nullptr;
  size_t subscriber_handle_count = 0;
  ASSERT_TRUE(subspace_get_subscriber_split_buffer_handles(
      sub, &subscriber_handles, &subscriber_handle_count));
  ASSERT_EQ(3U, subscriber_handle_count);
  ASSERT_NE(nullptr, subscriber_handles);
  for (size_t i = 0; i < subscriber_handle_count; i++) {
    ASSERT_NE(state.allocations.end(),
              state.allocations.find(subscriber_handles[i]));
  }

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer.buffer);
  ASSERT_GE(buffer.buffer_size, 64U);
  memcpy(buffer.buffer, "c-split", 7);

  uintptr_t publisher_handle = 0;
  ASSERT_TRUE(subspace_get_publisher_split_buffer_handle_from_address(
      pub, buffer.buffer, &publisher_handle));
  ASSERT_NE(state.allocations.end(), state.allocations.find(publisher_handle));

  const SubspaceMessage pub_status = subspace_publish_message(pub, 7);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(7, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "c-split", 7));

  uintptr_t subscriber_handle = 0;
  ASSERT_TRUE(subspace_get_subscriber_split_buffer_handle_from_address(
      sub, msg.buffer, &subscriber_handle));
  ASSERT_EQ(publisher_handle, subscriber_handle);

  subspace_free_message(&msg);
  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));

  ASSERT_EQ(3, state.unmap_count);
  ASSERT_EQ(3, state.free_count);
  ASSERT_TRUE(state.allocations.empty());
}

TEST_F(ClientTest, SplitBufferCallbackFailures) {
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisherOptions fail_allocate_opts =
      subspace_publisher_options_default(96, 3);
  fail_allocate_opts.use_split_buffers = true;
  fail_allocate_opts.split_callbacks.allocate = TestCSplitFailAllocate;
  SubspacePublisher failed_pub = subspace_create_publisher(
      pub_client, "c_split_fail_allocate", fail_allocate_opts);
  ASSERT_EQ(nullptr, failed_pub.publisher);
  ASSERT_TRUE(subspace_has_error());

  TestCSplitBufferState state;
  SubspacePublisherOptions pub_opts = subspace_publisher_options_default(96, 3);
  pub_opts.use_split_buffers = true;
  pub_opts.split_callbacks.allocate = TestCSplitAllocate;
  pub_opts.split_callbacks.free = TestCSplitFree;
  pub_opts.split_callbacks.user_data = &state;
  SubspacePublisher pub =
      subspace_create_publisher(pub_client, "c_split_fail_map", pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  SubspaceSubscriberOptions sub_opts = subspace_subscriber_options_default();
  sub_opts.split_callbacks.map = TestCSplitFailMap;
  sub_opts.split_callbacks.user_data = &state;
  SubspaceSubscriber sub =
      subspace_create_subscriber(sub_client, "c_split_fail_map", sub_opts);
  ASSERT_EQ(nullptr, sub.subscriber);
  ASSERT_TRUE(subspace_has_error());

  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

// Callback to read the messages.  Doesn't free them but instead stores
// them.
std::vector<SubspaceMessage> messages_read;
void MessageCallback(SubspaceSubscriber /*subscriber*/, SubspaceMessage msg) {
  ASSERT_EQ(6, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "foobar", 6));
  messages_read.push_back(msg);
}

int manually_invoked_messages = 0;
void ManualMessageCallback(SubspaceSubscriber /*subscriber*/,
                           SubspaceMessage msg) {
  ASSERT_EQ(6, msg.length);
  ASSERT_EQ(0, memcmp(msg.buffer, "manual", 6));
  manually_invoked_messages++;
  ASSERT_TRUE(subspace_free_message(&msg));
}

void InvalidDroppedCallback(SubspaceSubscriber /*subscriber*/,
                            int64_t /*num_dropped*/) {}

bool InvalidResizeCallback(SubspacePublisher /*publisher*/,
                           int32_t /*old_size*/, int32_t /*new_size*/) {
  return true;
}

TEST_F(ClientTest, SubscriberCallbacks) {
  messages_read.clear();
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "dave2", CPublisherOptionsDefault(256, 10));

  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  // Create a subscriber and set a callback.  We have 3 active messages at once.
  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
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

  for (SubspaceMessage &msg : messages_read) {
    ASSERT_TRUE(subspace_free_message(&msg));
  }
  messages_read.clear();

  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, InvokeAndRemoveSubscriberCallback) {
  manually_invoked_messages = 0;
  auto pub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, pub_client.client);
  ASSERT_FALSE(subspace_has_error());
  auto sub_client = subspace_create_client_with_socket(Socket().c_str());
  ASSERT_NE(nullptr, sub_client.client);
  ASSERT_FALSE(subspace_has_error());

  SubspacePublisher pub = subspace_create_publisher(
      pub_client, "c_manual_callback", CPublisherOptionsDefault(128, 4));
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());
  SubspaceSubscriber sub = subspace_create_subscriber(
      sub_client, "c_manual_callback", CSubscriberOptionsDefault());
  ASSERT_NE(nullptr, sub.subscriber);
  ASSERT_FALSE(subspace_has_error());

  ASSERT_FALSE(subspace_register_subscriber_callback(sub, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_TRUE(
      subspace_register_subscriber_callback(sub, ManualMessageCallback));
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 64);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_NE(nullptr, buffer.buffer);
  memcpy(buffer.buffer, "manual", 6);
  const SubspaceMessage pub_status = subspace_publish_message(pub, 6);
  ASSERT_NE(0, pub_status.length);
  ASSERT_FALSE(subspace_has_error());

  SubspaceMessage msg = subspace_read_message(sub);
  ASSERT_NE(nullptr, msg.message);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_FALSE(subspace_invoke_subscriber_callback(sub, SubspaceMessage{}));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_TRUE(subspace_invoke_subscriber_callback(sub, msg));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(1, manually_invoked_messages);

  ASSERT_TRUE(subspace_remove_subscriber_callback(sub));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_TRUE(subspace_invoke_subscriber_callback(sub, msg));
  ASSERT_FALSE(subspace_has_error());
  ASSERT_EQ(1, manually_invoked_messages);

  subspace_free_message(&msg);
  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

TEST_F(ClientTest, InvalidArgumentsReportErrors) {
  SubspaceClient default_client = subspace_create_client();
  if (default_client.client != nullptr) {
    ASSERT_TRUE(subspace_remove_client(&default_client));
  } else {
    ASSERT_TRUE(subspace_has_error());
  }

  SubspaceClient invalid_client = {};
  SubspacePublisher invalid_publisher = {};
  SubspaceSubscriber invalid_subscriber = {};
  SubspaceMessage invalid_message = {};

  ASSERT_FALSE(subspace_remove_client(&invalid_client));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_remove_publisher(&invalid_publisher));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_remove_subscriber(&invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());

  ASSERT_FALSE(subspace_free_message(&invalid_message));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_subscriber_callback(invalid_subscriber,
                                                     MessageCallback));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_register_subscriber_callback(invalid_subscriber, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_remove_subscriber_callback(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_dropped_message_callback(
      invalid_subscriber, InvalidDroppedCallback));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_register_dropped_message_callback(invalid_subscriber, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_remove_dropped_message_callback(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_process_all_messages(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_invoke_subscriber_callback(invalid_subscriber, invalid_message));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_wait_for_subscriber(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_wait_for_subscriber_with_timeout(invalid_subscriber, 1));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_wait_for_subscriber_with_fd(invalid_subscriber, 0));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_wait_for_publisher(invalid_publisher));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_wait_for_publisher_with_fd(invalid_publisher, 0));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_wait_for_publisher_with_timeout(invalid_publisher, 1));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_wait_for_publisher_with_fd_and_timeout(
                    invalid_publisher, 0, 1));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_wait_for_subscriber_with_fd_and_timeout(
                    invalid_subscriber, 0, 1));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_cancel_publish(invalid_publisher));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_resize_callback(invalid_publisher,
                                                 InvalidResizeCallback));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_resize_callback(invalid_publisher, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_unregister_resize_callback(invalid_publisher));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_publisher_checksum_callback(
      invalid_publisher, SumChecksumCallback, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_publisher_checksum_callback(invalid_publisher,
                                                             nullptr, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_unregister_publisher_checksum_callback(invalid_publisher));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_subscriber_checksum_callback(
      invalid_subscriber, SumChecksumCallback, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_subscriber_checksum_callback(
      invalid_subscriber, nullptr, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_unregister_subscriber_checksum_callback(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_publisher_on_send_callback(
      invalid_publisher, ResizeTransformCallback, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_publisher_on_send_callback(invalid_publisher,
                                                            nullptr, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_unregister_publisher_on_send_callback(invalid_publisher));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_subscriber_on_receive_callback(
      invalid_subscriber, ResizeTransformCallback, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_register_subscriber_on_receive_callback(
      invalid_subscriber, nullptr, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(
      subspace_unregister_subscriber_on_receive_callback(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_clear_active_message(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_trigger_reliable_publishers(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_trigger_subscriber(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_FALSE(subspace_untrigger_subscriber(invalid_subscriber));
  ASSERT_TRUE(subspace_has_error());

  SubspaceMessageBuffer message_buffer =
      subspace_get_message_buffer(invalid_publisher, 1);
  ASSERT_EQ(nullptr, message_buffer.buffer);
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(nullptr,
            subspace_get_publisher_metadata(invalid_publisher, nullptr));
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(nullptr,
            subspace_get_subscriber_metadata(invalid_subscriber, nullptr));
  ASSERT_TRUE(subspace_has_error());

  ASSERT_EQ(nullptr, subspace_get_subscriber_type(invalid_subscriber).type);
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_get_subscriber_poll_fd(invalid_subscriber).fd);
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_get_publisher_poll_fd(invalid_publisher).fd);
  ASSERT_TRUE(subspace_has_error());
  ASSERT_EQ(-1, subspace_get_publisher_fd(invalid_publisher));
  ASSERT_EQ(-1, subspace_get_subscriber_fd(invalid_subscriber));
  ASSERT_EQ(-1, subspace_get_publisher_retirement_fd(invalid_publisher));
  ASSERT_EQ(0, subspace_get_subscriber_slot_size(invalid_subscriber));
  ASSERT_EQ(0, subspace_get_subscriber_num_slots(invalid_subscriber));
  ASSERT_EQ(0, subspace_get_subscriber_queue_size(invalid_subscriber));
  ASSERT_EQ(0, subspace_get_publisher_slot_size(invalid_publisher));
  ASSERT_EQ(0, subspace_get_publisher_num_slots(invalid_publisher));
  ASSERT_EQ(0, subspace_get_publisher_queue_size(invalid_publisher));
  ASSERT_EQ(0, subspace_get_publisher_metadata_size(invalid_publisher));
  ASSERT_EQ(0, subspace_get_subscriber_metadata_size(invalid_subscriber));
  ASSERT_EQ(0, subspace_get_publisher_prefix_size(invalid_publisher));
  ASSERT_EQ(0, subspace_get_subscriber_prefix_size(invalid_subscriber));
  ASSERT_EQ(0, subspace_get_publisher_checksum_size(invalid_publisher));
  ASSERT_EQ(0, subspace_get_subscriber_checksum_size(invalid_subscriber));
  ASSERT_EQ(-1, subspace_get_subscriber_current_ordinal(invalid_subscriber));
  ASSERT_EQ(0, subspace_get_subscriber_timestamp(invalid_subscriber));
  ASSERT_EQ(0, subspace_get_subscriber_num_active_messages(invalid_subscriber));
  ASSERT_TRUE(subspace_get_publisher_name(invalid_publisher).data == nullptr);
  ASSERT_TRUE(subspace_get_subscriber_name(invalid_subscriber).data == nullptr);

  void **addresses = reinterpret_cast<void **>(0x1);
  size_t count = 42;
  ASSERT_FALSE(
      subspace_get_publisher_addresses(invalid_publisher, &addresses, &count));
  ASSERT_EQ(nullptr, addresses);
  ASSERT_EQ(0U, count);
  ASSERT_TRUE(subspace_has_error());
  addresses = reinterpret_cast<void **>(0x1);
  count = 42;
  ASSERT_FALSE(subspace_get_subscriber_addresses(invalid_subscriber, &addresses,
                                                 &count));
  ASSERT_EQ(nullptr, addresses);
  ASSERT_EQ(0U, count);
  ASSERT_TRUE(subspace_has_error());

  uintptr_t handle = 123;
  ASSERT_FALSE(subspace_get_publisher_split_buffer_handle_from_address(
      invalid_publisher, nullptr, &handle));
  ASSERT_EQ(0U, handle);
  uintptr_t *handles = reinterpret_cast<uintptr_t *>(0x1);
  count = 42;
  ASSERT_FALSE(subspace_get_publisher_split_buffer_handles(invalid_publisher,
                                                           &handles, &count));
  ASSERT_EQ(nullptr, handles);
  ASSERT_EQ(0U, count);
  handle = 123;
  ASSERT_FALSE(subspace_get_subscriber_split_buffer_handle_from_address(
      invalid_subscriber, nullptr, &handle));
  ASSERT_EQ(0U, handle);
  handles = reinterpret_cast<uintptr_t *>(0x1);
  count = 42;
  ASSERT_FALSE(subspace_get_subscriber_split_buffer_handles(invalid_subscriber,
                                                            &handles, &count));
  ASSERT_EQ(nullptr, handles);
  ASSERT_EQ(0U, count);

  bool exists = true;
  ASSERT_FALSE(subspace_channel_exists(invalid_client, "x", &exists));
  ASSERT_FALSE(exists);
  SubspaceChannelCounters counters = {};
  ASSERT_FALSE(subspace_get_channel_counters(invalid_client, "x", &counters));
  SubspaceChannelInfo info = {};
  ASSERT_FALSE(subspace_get_channel_info(invalid_client, "x", &info));
  SubspaceChannelInfo *infos = reinterpret_cast<SubspaceChannelInfo *>(0x1);
  ASSERT_FALSE(subspace_get_all_channel_info(invalid_client, &infos, &count));
  ASSERT_EQ(nullptr, infos);
  SubspaceChannelStats stats = {};
  ASSERT_FALSE(subspace_get_channel_stats(invalid_client, "x", &stats));
  SubspaceChannelStats *stats_array =
      reinterpret_cast<SubspaceChannelStats *>(0x1);
  ASSERT_FALSE(
      subspace_get_all_channel_stats(invalid_client, &stats_array, &count));
  ASSERT_EQ(nullptr, stats_array);

  SubspaceMessage *messages = reinterpret_cast<SubspaceMessage *>(0x1);
  ASSERT_FALSE(subspace_get_all_messages(invalid_subscriber, kSubspaceReadNext,
                                         &messages, &count));
  ASSERT_EQ(nullptr, messages);
  ASSERT_EQ(nullptr, subspace_find_message(invalid_subscriber, 0).message);
  ASSERT_FALSE(subspace_snapshot_message_slot({}, nullptr));
  ASSERT_FALSE(subspace_snapshot_message_prefix({}, nullptr));
  const char *dump = reinterpret_cast<const char *>(0x1);
  size_t dump_length = 42;
  ASSERT_FALSE(
      subspace_dump_publisher_slots(invalid_publisher, &dump, &dump_length));
  ASSERT_EQ(nullptr, dump);
  ASSERT_EQ(0U, dump_length);
  dump = reinterpret_cast<const char *>(0x1);
  dump_length = 42;
  ASSERT_FALSE(
      subspace_dump_subscriber_slots(invalid_subscriber, &dump, &dump_length));
  ASSERT_EQ(nullptr, dump);
  ASSERT_EQ(0U, dump_length);
}

int num_resizes = 0;

bool ResizeCallback(SubspacePublisher /*publisher*/, int32_t old_size,
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
      pub_client, "dave3", CPublisherOptionsDefault(256, 10));

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
void DroppedMessageCallback(SubspaceSubscriber /*subscriber*/,
                            int64_t num_dropped) {
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
      pub_client, "dave3", CPublisherOptionsDefault(256, 10));

  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());

  // Create a subscriber and set a callback for dropped messages.
  SubspaceSubscriberOptions sub_opts = CSubscriberOptionsDefault();
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

  // Why 11 messages.  We have 10 slots with 1 publisher and 1 subscriber.  This
  // leaves us with 7 slots that can hold messages (we need one spare slot).
  ASSERT_EQ(11, num_dropped_messages);

  ASSERT_TRUE(subspace_remove_dropped_message_callback(sub));
  ASSERT_TRUE(subspace_remove_subscriber(&sub));
  ASSERT_TRUE(subspace_remove_publisher(&pub));
  ASSERT_TRUE(subspace_remove_client(&pub_client));
  ASSERT_TRUE(subspace_remove_client(&sub_client));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  return RUN_ALL_TESTS();
}
