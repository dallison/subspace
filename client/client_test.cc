// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/test_fixture.h"

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "common/system_info.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <inttypes.h>
#include <memory>
#include <sys/resource.h>
#include <sys/wait.h>
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
#include <sys/syscall.h>
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
#endif
#include <unordered_map>

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");
ABSL_FLAG(bool, use_split_buffers, false,
          "Run publishers with split-buffer payload storage");

#ifndef ADDRESS_SANITIZER
static void SignalHandler(int sig) {
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
#endif

using InetAddress = toolbelt::InetAddress;

#ifdef __APPLE__
extern "C" subspace::PluginInterface *NOP_Create();
#endif

class ClientTest : public SubspaceTestBase {};

struct TestSplitAllocation {
  std::unique_ptr<char[]> memory;
  size_t size = 0;
};

struct TestSplitBufferState {
  uintptr_t next_handle = 1000;
  std::unordered_map<uintptr_t, TestSplitAllocation> allocations;
  int allocate_count = 0;
  int map_count = 0;
  int unmap_count = 0;
  int free_count = 0;
};

uint64_t AlignPage(uint64_t size) {
  return subspace::PageAlignedSize(size);
}

uint64_t ExpectedSplitBufferVirtualMemoryUsage(int num_slots,
                                               uint64_t slot_size,
                                               uint64_t prefix_size) {
  return sizeof(subspace::SystemControlBlock) + subspace::CcbSize(num_slots) +
         sizeof(subspace::BufferControlBlock) +
         AlignPage(prefix_size * static_cast<uint64_t>(num_slots)) +
         AlignPage(slot_size) * static_cast<uint64_t>(num_slots);
}

subspace::PublisherOptions PubOpts(int32_t slot_size = 0,
                                   int32_t num_slots = 0) {
  return subspace::PublisherOptions()
      .SetSlotSize(slot_size)
      .SetNumSlots(num_slots);
}

subspace::SubscriberOptions SubOpts() { return subspace::SubscriberOptions(); }

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
absl::StatusOr<toolbelt::FileDescriptor> CreateTestMemfd(const char *name,
                                                         size_t size) {
#ifdef __NR_memfd_create
  int fd = static_cast<int>(
      syscall(__NR_memfd_create, name, static_cast<unsigned int>(MFD_CLOEXEC)));
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create test memfd %s: %s", name, strerror(errno)));
  }
  toolbelt::FileDescriptor result(fd);
  if (ftruncate(result.Fd(), static_cast<off_t>(size)) == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to size test memfd %s: %s", name, strerror(errno)));
  }
  return result;
#else
  return absl::UnimplementedError("memfd_create is not available");
#endif
}
#endif

subspace::SplitBufferCallbacks MakeTestSplitBufferCallbacks(
    std::shared_ptr<TestSplitBufferState> state) {
  subspace::SplitBufferCallbacks callbacks;
  callbacks.allocate =
      [state](const subspace::SplitBufferMetadata &metadata)
      -> absl::StatusOr<subspace::SplitBufferMapping> {
    auto memory = std::make_unique<char[]>(metadata.allocation_size);
    char *address = memory.get();
    uintptr_t handle = ++state->next_handle;
    state->allocations.emplace(
        handle, TestSplitAllocation{std::move(memory),
                                    static_cast<size_t>(
                                        metadata.allocation_size)});
    state->allocate_count++;
    subspace::SplitBufferMapping mapping;
    mapping.handle = handle;
    mapping.address = address;
    mapping.size = static_cast<size_t>(metadata.allocation_size);
    mapping.private_data = address;
    return mapping;
  };
  callbacks.map =
      [state](const subspace::SplitBufferMetadata &metadata)
      -> absl::StatusOr<subspace::SplitBufferMapping> {
    auto it = state->allocations.find(metadata.handle);
    if (it == state->allocations.end()) {
      return absl::NotFoundError("split buffer handle not found");
    }
    state->map_count++;
    subspace::SplitBufferMapping mapping;
    mapping.handle = metadata.handle;
    mapping.address = it->second.memory.get();
    mapping.size = it->second.size;
    return mapping;
  };
  callbacks.unmap = [state](const subspace::SplitBufferMetadata &,
                            const subspace::SplitBufferMapping &mapping)
      -> absl::Status {
    if (mapping.address == nullptr || mapping.handle == 0) {
      return absl::InvalidArgumentError("invalid split buffer mapping");
    }
    state->unmap_count++;
    return absl::OkStatus();
  };
  callbacks.free = [state](const subspace::SplitBufferMetadata &,
                           const subspace::SplitBufferMapping &mapping)
      -> absl::Status {
    if (mapping.handle == 0) {
      return absl::InvalidArgumentError("invalid split buffer handle");
    }
    state->free_count++;
    state->allocations.erase(mapping.handle);
    return absl::OkStatus();
  };
  return callbacks;
}

static void SigQuitHandler(int signum) {
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
  ClientTest::Engine().Show();
#endif
  signal(signum, SIG_DFL);
  raise(signum);
}

TEST_F(ClientTest, InetAddressSupportsAbslHash) {
  struct sockaddr_in addr = {};
#if defined(__APPLE__)
  addr.sin_len = sizeof(int);
#endif
  addr.sin_family = AF_INET;
  addr.sin_port = htons(1234);
  addr.sin_addr.s_addr = htonl(0x12345678);

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

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
TEST(AndroidBufferRegistrationTest, FailedRegistrationRollsBackNumBuffers) {
  constexpr int kNumSlots = 2;
  absl::StatusOr<toolbelt::FileDescriptor> scb_fd =
      CreateTestMemfd("subspace_test_scb", sizeof(subspace::SystemControlBlock));
  if (absl::IsUnimplemented(scb_fd.status())) {
    GTEST_SKIP() << "memfd_create is not available on this platform";
  }
  ASSERT_OK(scb_fd);
  absl::StatusOr<toolbelt::FileDescriptor> ccb_fd =
      CreateTestMemfd("subspace_test_ccb", subspace::CcbSize(kNumSlots));
  ASSERT_OK(ccb_fd);
  absl::StatusOr<toolbelt::FileDescriptor> bcb_fd = CreateTestMemfd(
      "subspace_test_bcb", sizeof(subspace::BufferControlBlock));
  ASSERT_OK(bcb_fd);

  // This test exercises the non-split single-buffer registration rollback path
  // (publisher.cc, gated on !UseSplitBuffers()).  Force the non-split layout so
  // it is deterministic even when the suite runs with --use_split_buffers, and
  // because the harness only wires a registration callback, not the lookup
  // callback that split buffers require.
  subspace::PublisherOptions options;
  options.SetUseSplitBuffers(false);
  subspace::details::PublisherImpl publisher(
      "android_registration_rollback", kNumSlots, /*channel_id=*/0,
      /*publisher_id=*/0, /*vchan_id=*/-1, /*session_id=*/123, "",
      options, [](subspace::Channel *) { return false; },
      /*user_id=*/0, /*group_id=*/0);
  ASSERT_OK(publisher.Map(
      subspace::SharedMemoryFds(std::move(*ccb_fd), std::move(*bcb_fd)),
      *scb_fd));

  int failed_registration_attempts = 0;
  publisher.SetClientBufferRegistrationCallback(
      [&](const subspace::ClientBufferHandleMetadata &metadata,
          const toolbelt::FileDescriptor *fd) {
        failed_registration_attempts++;
        EXPECT_EQ(0u, metadata.buffer_index);
        EXPECT_NE(nullptr, fd);
        EXPECT_TRUE(fd->Valid());
        return absl::InternalError("injected registration failure");
      });

  absl::Status status = publisher.CreateOrAttachBuffers(/*slot_size=*/128);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(1, failed_registration_attempts);
  EXPECT_EQ(0, publisher.GetCcb()->num_buffers.load(std::memory_order_relaxed));
  EXPECT_TRUE(publisher.GetBuffers().empty());

  std::vector<uint32_t> registered_indices;
  publisher.SetClientBufferRegistrationCallback(
      [&](const subspace::ClientBufferHandleMetadata &metadata,
          const toolbelt::FileDescriptor *fd) {
        EXPECT_NE(nullptr, fd);
        EXPECT_TRUE(fd->Valid());
        registered_indices.push_back(metadata.buffer_index);
        return absl::OkStatus();
      });

  ASSERT_OK(publisher.CreateOrAttachBuffers(/*slot_size=*/128));
  ASSERT_EQ(1u, registered_indices.size());
  EXPECT_EQ(0u, registered_indices[0]);
  EXPECT_EQ(1, publisher.GetCcb()->num_buffers.load(std::memory_order_relaxed));
  EXPECT_EQ(1u, publisher.GetBuffers().size());
}
#endif

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
        "dave0", 256, 100, PubOpts().SetType("foobar").SetMux("mainmux"));
    std::cerr << pub.status() << std::endl;
    ASSERT_OK(pub);
    ASSERT_EQ("foobar", pub->Type());
  }
  // Mux will be destructed here since there are no virtual
  // channels on it.

  // Create again with different type.
  {
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        "dave0", 256, 10, PubOpts().SetType("foobar1").SetMux("mainmux"));
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
        "dave0", 256, 10, PubOpts().SetType("foobar").SetMux("mainmux"));
    ASSERT_OK(pub);
    ASSERT_EQ("foobar", pub->Type());
    pubs.push_back(std::move(*pub));
  }
  // Publisher on the mux will fail
  absl::StatusOr<Publisher> mux_pub =
      client.CreatePublisher("mainmux", 256, 10,
                             PubOpts().SetType("foobar"));
  ASSERT_FALSE(mux_pub.ok());

  // One more virtual publisher will fail.
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "dave0", 256, 10, PubOpts().SetType("foobar").SetMux("mainmux"));
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, CreateVirtualPublisherMuxMismatch) {
  subspace::Client client;
  InitClient(client);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "dave0", 256, 100, PubOpts().SetType("foobar").SetMux("mainmux"));
  ASSERT_OK(pub);
  ASSERT_EQ("foobar", pub->Type());

  // Different mux.
  absl::StatusOr<Publisher> pub2 = client.CreatePublisher(
      "dave0", 256, 100, PubOpts().SetType("foobar").SetMux("diffmux"));
  ASSERT_FALSE(pub2.ok());

  // No mux.
  absl::StatusOr<Publisher> pub3 =
      client.CreatePublisher("dave0", 256, 100,
                             PubOpts().SetType("foobar"));
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
        "dave0", 256, 100, PubOpts().SetType("foobar").SetMux("mainmux"));
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
        name, 256, 100, PubOpts().SetType("foobar").SetMux("mainmux"));
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
  absl::StatusOr<const subspace::ChannelInfo> info =
      client.GetChannelInfo("dave0");
  ASSERT_OK(info);
  EXPECT_EQ(9, info->num_publishers);
}

TEST_F(ClientTest, MaxPublishersOptionLimitsPublisherCount) {
  subspace::Client client;
  InitClient(client);
  subspace::PublisherOptions opts;
  opts.SetMaxPublishers(2);

  absl::StatusOr<Publisher> pub1 =
      client.CreatePublisher("max_publishers", 256, 10, opts);
  ASSERT_OK(pub1);
  absl::StatusOr<Publisher> pub2 =
      client.CreatePublisher("max_publishers", 256, 10, opts);
  ASSERT_OK(pub2);

  absl::StatusOr<Publisher> pub3 =
      client.CreatePublisher("max_publishers", 256, 10, opts);
  ASSERT_FALSE(pub3.ok());
  EXPECT_THAT(pub3.status().message(),
              ::testing::HasSubstr("maximum number of publishers"));
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
      client.CreatePublisher("dave0", 256, kNumSlots,
                             PubOpts().SetMux("foobar"));
  ASSERT_OK(pub);

  // 6 subscribers.
  std::vector<Subscriber> subs;
  for (int i = 0; i < kNumSlots - 3; i++) {
    absl::StatusOr<Subscriber> sub =
        client.CreateSubscriber("dave0", SubOpts().SetMux("foobar"));
    ASSERT_OK(sub);
    subs.push_back(std::move(*sub));
  }

  // 1 Multiplexer subscriber.
  absl::StatusOr<Subscriber> mux_sub = client.CreateSubscriber("foobar");
  ASSERT_OK(mux_sub);

  // One more will fail.
  absl::StatusOr<Subscriber> sub =
      client.CreateSubscriber("dave0", SubOpts().SetMux("foobar"));
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
      client.CreatePublisher("dave1", 256, 10, PubOpts().SetFixedSize(true));
  ASSERT_OK(pub4);

  // Not fixed size - mismatch fixed size option, same slot size.
  absl::StatusOr<Publisher> pub5 =
      client.CreatePublisher("dave1", 256, 10, PubOpts().SetFixedSize(false));
  ASSERT_FALSE(pub5.ok());

  // Different slot size - we are fixed size, this will fail.
  absl::StatusOr<Publisher> pub6 =
      client.CreatePublisher("dave1", 512, 10, PubOpts().SetFixedSize(true));
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
  auto p = client.CreatePublisher("dave1", 256, 10,
                                  PubOpts().SetMux("foobar"));
  ASSERT_OK(p);

  auto s = client.CreateSubscriber("dave1", SubOpts().SetMux("foobar"));
  ASSERT_OK(s);
}

TEST_F(ClientTest, CreateVirtualSubscriberThenPublisher) {
  subspace::Client client;
  InitClient(client);

  auto s = client.CreateSubscriber("dave1", SubOpts().SetMux("foobar"));
  ASSERT_OK(s);
  auto p = client.CreatePublisher("dave1", 256, 10,
                                  PubOpts().SetMux("foobar"));
  ASSERT_OK(p);
}

TEST_F(ClientTest, CreateVirtualPublisherThenSubscriberMuxMismatch) {
  subspace::Client client;
  InitClient(client);
  auto p1 = client.CreatePublisher("dave1", 256, 10,
                                   PubOpts().SetMux("foobar"));
  ASSERT_OK(p1);

  auto s1 = client.CreateSubscriber("dave1", SubOpts().SetMux("foobar"));
  ASSERT_OK(s1);

  // Different mux.
  auto s2 = client.CreateSubscriber("dave1", SubOpts().SetMux("diffmux"));
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

TEST_F(ClientTest, SplitBuffersPublishWithHandlesAndSeparatePrefix) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  subspace::PublisherOptions pub_options;
  pub_options.SetSlotSize(128)
      .SetNumSlots(4)
      .SetMetadataSize(8)
      .SetUseSplitBuffers(true);
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("split_buffers", pub_options);
  ASSERT_OK(pub);
  EXPECT_TRUE(pub->UsesSplitBuffers());
  const uint64_t expected_vm = ExpectedSplitBufferVirtualMemoryUsage(
      pub->NumSlots(), pub->SlotSize(), pub->PrefixSize());
  EXPECT_EQ(expected_vm, pub->GetVirtualMemoryUsage());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer(64);
  ASSERT_OK(buffer);
  ASSERT_NE(nullptr, *buffer);

  uintptr_t publisher_handle = 0;
  ASSERT_TRUE(pub->GetSplitBufferHandleFromAddress(*buffer, &publisher_handle));
  EXPECT_NE(0U, publisher_handle);

  auto metadata = pub->GetMetadata();
  ASSERT_EQ(8U, metadata.size());
  std::memcpy(metadata.data(), "metadata", metadata.size());
  std::memcpy(*buffer, "split-buf", 9);

  subspace::MessagePrefix *prefix = pub->Prefix();
  ASSERT_NE(nullptr, prefix);
  EXPECT_NE(reinterpret_cast<void *>(prefix), *buffer);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(9);
  ASSERT_OK(pub_status);
  ASSERT_GE(pub_status->slot_id, 0);

  subspace::SubscriberOptions sub_options;
  sub_options.SetMaxActiveMessages(2);
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("split_buffers", sub_options);
  ASSERT_OK(sub);
  EXPECT_TRUE(sub->UsesSplitBuffers());
  EXPECT_EQ(expected_vm, sub->GetVirtualMemoryUsage());

  uintptr_t *subscriber_handles = nullptr;
  size_t subscriber_handle_count = 0;
  ASSERT_TRUE(
      sub->GetSplitBufferHandles(&subscriber_handles, &subscriber_handle_count));
  ASSERT_EQ(4U, subscriber_handle_count);
  ASSERT_NE(nullptr, subscriber_handles);
  for (size_t i = 0; i < subscriber_handle_count; i++) {
    EXPECT_NE(0U, subscriber_handles[i]);
  }

  subspace::MessagePrefix *sub_prefix =
      sub->Prefix(sub->GetSlot(pub_status->slot_id));
  ASSERT_NE(nullptr, sub_prefix);
  EXPECT_EQ(9, sub_prefix->message_size);
  auto sub_metadata = subspace::GetMetadataSpan(
      sub_prefix, sub->ChecksumSize(), sub->MetadataSize());
  ASSERT_EQ(8U, sub_metadata.size());
  EXPECT_EQ(
      0, std::memcmp(sub_metadata.data(), "metadata", sub_metadata.size()));
}

TEST_F(ClientTest, PlaceholderSubscriberLearnsSplitBuffersOnReload) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("split_buffers_placeholder");
  ASSERT_OK(sub);
  EXPECT_FALSE(sub->UsesSplitBuffers());
  EXPECT_TRUE(sub->IsPlaceholder());

  subspace::PublisherOptions pub_options;
  pub_options.SetSlotSize(128).SetNumSlots(4).SetUseSplitBuffers(true);
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("split_buffers_placeholder", pub_options);
  ASSERT_OK(pub);
  EXPECT_TRUE(pub->UsesSplitBuffers());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer(64);
  ASSERT_OK(buffer);
  std::memcpy(*buffer, "split", 5);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(5, msg->length);
  EXPECT_TRUE(sub->UsesSplitBuffers());
  EXPECT_FALSE(sub->IsPlaceholder());

  subspace::MessagePrefix *sub_prefix =
      sub->Prefix(sub->GetSlot(pub_status->slot_id));
  ASSERT_NE(nullptr, sub_prefix);
  EXPECT_NE(reinterpret_cast<const void *>(sub_prefix), msg->buffer);
}

TEST_F(ClientTest, PlaceholderSubscriberReloadRebuildsCcbSubscriberCount) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr char kChannel[] = "placeholder_subscriber_count";
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(kChannel);
  ASSERT_OK(sub);
  ASSERT_TRUE(sub->IsPlaceholder());

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(kChannel, 128, 4);
  ASSERT_OK(pub);

  subspace::ServerChannel *server_channel = Server()->FindChannel(kChannel);
  ASSERT_NE(nullptr, server_channel);
  server_channel->GetCcb()->subscribers.ClearAll();
  server_channel->GetCcb()->num_subs = subspace::SubscriberCounter();
  ASSERT_EQ(0, pub->NumSubscribers());

  absl::StatusOr<Message> empty = sub->ReadMessage();
  ASSERT_OK(empty);
  ASSERT_EQ(0, empty->length);
  EXPECT_FALSE(sub->IsPlaceholder());
  EXPECT_EQ(1, sub->NumSubscribers());
  EXPECT_EQ(1, pub->NumSubscribers());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  std::memcpy(*buffer, "count", 5);
  ASSERT_OK(pub->PublishMessage(5));

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(5, msg->length);
}

TEST_F(ClientTest, RejectsMixedSplitBufferPublisherOptions) {
  subspace::Client client;
  InitClient(client);

  subspace::PublisherOptions split_options;
  split_options.SetSlotSize(128).SetNumSlots(4).SetUseSplitBuffers(true);
  absl::StatusOr<Publisher> split_pub =
      client.CreatePublisher("mixed_split_publishers", split_options);
  ASSERT_OK(split_pub);

  subspace::PublisherOptions combined_options;
  combined_options.SetSlotSize(128).SetNumSlots(4).SetUseSplitBuffers(false);
  absl::StatusOr<Publisher> combined_pub =
      client.CreatePublisher("mixed_split_publishers", combined_options);
  ASSERT_FALSE(combined_pub.ok());
  EXPECT_THAT(combined_pub.status().message(),
              ::testing::HasSubstr("Inconsistent split-buffer mode"));

  subspace::PublisherOptions bridge_split_options;
  bridge_split_options.SetSlotSize(128)
      .SetNumSlots(4)
      .SetUseSplitBuffers(false)
      .SetSplitBuffersOverBridge(true);
  absl::StatusOr<Publisher> bridge_split_pub = client.CreatePublisher(
      "mixed_bridge_split_publishers", bridge_split_options);
  ASSERT_OK(bridge_split_pub);

  subspace::PublisherOptions no_bridge_split_options;
  no_bridge_split_options.SetSlotSize(128)
      .SetNumSlots(4)
      .SetUseSplitBuffers(false)
      .SetSplitBuffersOverBridge(false);
  absl::StatusOr<Publisher> no_bridge_split_pub = client.CreatePublisher(
      "mixed_bridge_split_publishers", no_bridge_split_options);
  ASSERT_FALSE(no_bridge_split_pub.ok());
  EXPECT_THAT(no_bridge_split_pub.status().message(),
              ::testing::HasSubstr("Inconsistent bridge split-buffer mode"));
}

TEST_F(ClientTest, SplitBufferCallbacksAllocateMapUnmapAndFreePayloadSlots) {
  auto state = std::make_shared<TestSplitBufferState>();

  {
    subspace::Client pub_client;
    subspace::Client sub_client;
    ASSERT_OK(pub_client.Init(Socket()));
    ASSERT_OK(sub_client.Init(Socket()));

    subspace::PublisherOptions pub_options;
    pub_options.SetSlotSize(96)
        .SetNumSlots(3)
        .SetUseSplitBuffers(true)
        .SetSplitBufferCallbacks(MakeTestSplitBufferCallbacks(state));
    absl::StatusOr<Publisher> pub =
        pub_client.CreatePublisher("split_buffers_callbacks", pub_options);
    ASSERT_OK(pub);
    EXPECT_TRUE(pub->UsesSplitBuffers());
    EXPECT_EQ(3, state->allocate_count);

    subspace::SubscriberOptions sub_options;
    sub_options.SetSplitBufferCallbacks(MakeTestSplitBufferCallbacks(state));
    absl::StatusOr<Subscriber> sub =
        sub_client.CreateSubscriber("split_buffers_callbacks", sub_options);
    ASSERT_OK(sub);
    EXPECT_TRUE(sub->UsesSplitBuffers());
    EXPECT_EQ(3, state->map_count);

    uintptr_t *subscriber_handles = nullptr;
    size_t subscriber_handle_count = 0;
    ASSERT_TRUE(sub->GetSplitBufferHandles(&subscriber_handles,
                                           &subscriber_handle_count));
    ASSERT_EQ(3U, subscriber_handle_count);
    ASSERT_NE(nullptr, subscriber_handles);
    for (size_t i = 0; i < subscriber_handle_count; i++) {
      EXPECT_NE(state->allocations.end(),
                state->allocations.find(subscriber_handles[i]));
    }

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer(64);
    ASSERT_OK(buffer);
    std::memcpy(*buffer, "callback-split", 14);
    uintptr_t publisher_handle = 0;
    ASSERT_TRUE(
        pub->GetSplitBufferHandleFromAddress(*buffer, &publisher_handle));
    EXPECT_NE(state->allocations.end(),
              state->allocations.find(publisher_handle));

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(14);
    ASSERT_OK(pub_status);
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(14, msg->length);
    EXPECT_EQ(0, std::memcmp(msg->buffer, "callback-split", 14));
  }

  EXPECT_EQ(3, state->unmap_count);
  EXPECT_EQ(3, state->free_count);
  EXPECT_TRUE(state->allocations.empty());
}

TEST_F(ClientTest, PublishSingleMessageWithPrefixAndRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "dave6", PubOpts(256, 10).SetType("foobar"));
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

  auto prefix = pub->Prefix();
  ASSERT_NE(nullptr, prefix);
  prefix->SetIsBridged();
  prefix->timestamp = 1234;

  absl::StatusOr<const Message> pub_status = pub->PublishMessageWithPrefix(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("dave6");
  ASSERT_OK(sub);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);

  auto prefix2 = sub->Prefix();
  ASSERT_NE(nullptr, prefix2);
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
      sub_client.CreateSubscriber("dave6",
                                  SubOpts().SetMaxActiveMessages(2));
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
      "dave6", PubOpts(256, 10).SetActivate(true));
  ASSERT_OK(pub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", SubOpts().SetMaxActiveMessages(2).SetPassActivation(true));
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
#ifdef __APPLE__
  ASSERT_OK(Server()->LoadBuiltinPlugin(
      "NOP", std::unique_ptr<subspace::PluginInterface>(NOP_Create())));
#else
  ASSERT_OK(Server()->LoadPlugin("NOP", "plugins/nop_plugin.so"));
#endif
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
      pub_client.CreatePublisher("dave6", 256, 10,
                                 PubOpts().SetMux("mainmux"));
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", SubOpts().SetMux("mainmux"));
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
        pub_client.CreatePublisher(name, 256, 100,
                                   PubOpts().SetMux("mainmux"));
    ASSERT_OK(pub);
    pubs.push_back(std::move(*pub));
  }

  // Create 10 subscribers for the same mux and channel names.
  for (int i = 0; i < 10; i++) {
    std::string name = "dave" + std::to_string(i);
    absl::StatusOr<Subscriber> sub =
        sub_client.CreateSubscriber(name, SubOpts().SetMux("mainmux"));
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

  std::cerr << "reading mux subscriber" << std::endl;
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
      sub_client.CreateSubscriber("dave6",
                                  SubOpts().SetMaxActiveMessages(2));
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
      pub_client.CreatePublisher("dave6", 256, 10,
                                 PubOpts().SetMux("mainmux"));
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "dave6", SubOpts().SetMaxActiveMessages(2).SetMux("mainmux"));
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
      sub_client.CreateSubscriber("dave6",
                                  SubOpts().SetMaxActiveMessages(2));
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
    if (sub_buffers[1]->IsSplitBuffers()) {
      ASSERT_FALSE(sub_buffers[1]->split_slot_buffers.empty());
      ASSERT_NE(nullptr, sub_buffers[1]->split_slot_buffers[0]);
    } else {
      ASSERT_NE(nullptr, sub_buffers[1]->buffer);
    }
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
    if (pub_buffers[1]->IsSplitBuffers()) {
      ASSERT_FALSE(pub_buffers[1]->split_slot_buffers.empty());
      ASSERT_NE(nullptr, pub_buffers[1]->split_slot_buffers[0]);
    } else {
      ASSERT_NE(nullptr, pub_buffers[1]->buffer);
    }
  }
}

TEST_F(ClientTest, PublishAndResizeSubscriberFirst) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  // First create subscriber.
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6",
                                  SubOpts().SetMaxActiveMessages(2));
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
      "dave6", SubOpts().SetMaxActiveMessages(2).SetMux("mainmux"));
  ASSERT_OK(sub);
  ASSERT_EQ(0, sub->SlotSize()); // No buffers yet.

  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", 256, 10,
                                 PubOpts().SetMux("mainmux"));
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
        channel_name, PubOpts(1, 4));
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

  const int kNumPublishers =
      absl::GetFlag(FLAGS_use_split_buffers) ? 16 : 100;
  std::vector<Publisher> pubs;
  pubs.reserve(kNumPublishers);
  subspace::Client pub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  for (int i = 0; i < kNumPublishers; ++i) {
    absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
        channel_name, PubOpts(256, 2 * kNumPublishers + 16));
    ASSERT_OK(pub) << pub.status();
    pubs.emplace_back(std::move(*pub));
  }

  std::vector<std::thread> pub_threads;
  pub_threads.reserve(kNumPublishers);
  for (int i = 0; i < kNumPublishers; ++i) {
    pub_threads.emplace_back(std::thread([&pubs, i]() {
      std::array<char, 16> msg = {};
      auto size = std::snprintf(msg.data(), msg.size(), "M%d", i);
      auto buffer = pubs[i].GetMessageBuffer(size);
      ASSERT_OK(buffer) << buffer.status();
      ASSERT_NE(nullptr, *buffer);
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
  std::string channel_name = "checkin_channel_multi_client";
  subspace::Client sub_client;
  ASSERT_OK(sub_client.Init(Socket()));
  auto sub = *sub_client.CreateSubscriber(channel_name);

  std::vector<std::thread> pub_threads;
#ifdef __APPLE__
  constexpr int kNumPublishers = 16;
#else
  const int kNumPublishers =
      absl::GetFlag(FLAGS_use_split_buffers) ? 16 : 100;
#endif
  pub_threads.reserve(kNumPublishers);
  std::atomic<int> published{0};
  for (int i = 0; i < kNumPublishers; ++i) {
    pub_threads.emplace_back(std::thread(
        [&channel_name, &published, kNumPublishers, i]() {
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
      absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
          channel_name, PubOpts(256, 2 * kNumPublishers + 16));
      ASSERT_OK(pub) << pub.status();
      std::array<char, 16> msg = {};
      auto size = std::snprintf(msg.data(), msg.size(), "M%d", i);
      auto buffer = pub->GetMessageBuffer(size);
      ASSERT_OK(buffer) << buffer.status();
      ASSERT_NE(nullptr, *buffer);
      std::memcpy(*buffer, msg.data(), size);
      ASSERT_OK(pub->PublishMessage(size));
      published.fetch_add(1, std::memory_order_release);
      while (published.load(std::memory_order_acquire) < kNumPublishers) {
        std::this_thread::yield();
      }
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

TEST_F(ClientTest, PublishSingleMessagePollAndReadAfterPlaceholderRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("placeholder_read");
  ASSERT_OK(sub);

  struct pollfd fd = sub->GetPollFd();
  ASSERT_EQ(1, ::poll(&fd, 1, 1000));

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher("placeholder_read", 256, 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  fd = sub->GetPollFd();
  ASSERT_EQ(1, ::poll(&fd, 1, 1000));

  msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
}

TEST_F(ClientTest, SlowReliableSubscriberDrainReachesEmptyRead) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "slow_reliable_drain",
      subspace::SubscriberOptions().SetReliable(true).SetMaxActiveMessages(2));
  ASSERT_OK(sub);

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "slow_reliable_drain",
      subspace::PublisherOptions().SetReliable(true).SetSlotSize(64).SetNumSlots(4));
  ASSERT_OK(pub);

  std::atomic<bool> stop_publisher{false};
  std::thread publisher([&] {
    uint64_t seq = 0;
    while (!stop_publisher.load()) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer(sizeof(seq));
      ASSERT_OK(buffer);
      if (*buffer == nullptr) {
        (void)pub->Wait(std::chrono::milliseconds(100));
        continue;
      }
      std::memcpy(*buffer, &seq, sizeof(seq));
      ASSERT_OK(pub->PublishMessage(sizeof(seq)));
      ++seq;
    }
  });

  struct pollfd fd = sub->GetPollFd();
  ASSERT_EQ(1, ::poll(&fd, 1, 1000));

  int reads_before_empty = 0;
  constexpr int kMaxReadsBeforeEmpty = 20;
  for (; reads_before_empty < kMaxReadsBeforeEmpty; ++reads_before_empty) {
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    if (msg->length == 0) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  stop_publisher.store(true);
  publisher.join();

  EXPECT_LT(reads_before_empty, kMaxReadsBeforeEmpty)
      << "a slow reliable subscriber should finish draining the poll-triggered "
         "batch instead of continuously chasing messages published during the drain";
}

// Reproduces the external-epoll pattern: fetch the poll fd ONCE before the
// loop, then poll on that fd and read, WITHOUT ever calling GetPollFd() again.
// Verifies that messages published across many separate poll cycles are all
// eventually observed (i.e. the stable poll-drain snapshot does not strand a
// caller that never re-fetches the fd).
TEST_F(ClientTest, ExternalPollLoopGetPollFdOnceReadOnce) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr int kNum = 20;
  constexpr int kNumSlots = kNum + 1;
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("ext_poll1");
  ASSERT_OK(sub);
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("ext_poll1", 256, kNumSlots);
  ASSERT_OK(pub);

  std::atomic<int> published{0};
  std::thread publisher([&] {
    for (int i = 0; i < kNum; i++) {
      // Pace publishes so they land in distinct poll cycles.
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      absl::StatusOr<void *> buf = pub->GetMessageBuffer();
      ASSERT_OK(buf);
      int len = snprintf(static_cast<char *>(*buf), 256, "m%d", i);
      ASSERT_OK(pub->PublishMessage(len + 1));
      published.fetch_add(1);
    }
  });

  // Fetch the poll fd exactly once, before the loop.
  struct pollfd fd = sub->GetPollFd();

  std::set<std::string> received;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (static_cast<int>(received.size()) < kNum &&
         std::chrono::steady_clock::now() < deadline) {
    int e = ::poll(&fd, 1, 500);
    if (e <= 0) {
      continue;
    }
    // Read a SINGLE message per wake; never re-fetch the poll fd.
    absl::StatusOr<Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    if (msg->length > 0) {
      received.insert(
          std::string(reinterpret_cast<const char *>(msg->buffer)));
    }
  }
  publisher.join();
  EXPECT_EQ(static_cast<int>(received.size()), kNum)
      << "external-poll reader that never re-fetches the fd missed messages";
}

// As above, but drains until empty on each poll wake (still GetPollFd once).
TEST_F(ClientTest, ExternalPollLoopGetPollFdOnceDrainUntilEmpty) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  constexpr int kNum = 20;
  constexpr int kNumSlots = kNum + 1;
  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("ext_poll2");
  ASSERT_OK(sub);
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("ext_poll2", 256, kNumSlots);
  ASSERT_OK(pub);

  std::thread publisher([&] {
    for (int i = 0; i < kNum; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      absl::StatusOr<void *> buf = pub->GetMessageBuffer();
      ASSERT_OK(buf);
      int len = snprintf(static_cast<char *>(*buf), 256, "m%d", i);
      ASSERT_OK(pub->PublishMessage(len + 1));
    }
  });

  struct pollfd fd = sub->GetPollFd();

  std::set<std::string> received;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (static_cast<int>(received.size()) < kNum &&
         std::chrono::steady_clock::now() < deadline) {
    int e = ::poll(&fd, 1, 500);
    if (e <= 0) {
      continue;
    }
    for (;;) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length == 0) {
        break;
      }
      received.insert(
          std::string(reinterpret_cast<const char *>(msg->buffer)));
    }
  }
  publisher.join();
  EXPECT_EQ(static_cast<int>(received.size()), kNum)
      << "external-poll drain reader that never re-fetches the fd missed "
         "messages";
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

  msg->Reset();

  TestCoroMachine machine;

  // Wait for trigger event in coroutine.
  machine.Spawn([&pub](TestCoroContext &c) {
    struct pollfd fd = pub->GetPollFd();
    c.Wait(fd.fd);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  });

  // Read messages in coroutine.
  machine.Spawn([&sub](TestCoroContext &c) {
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

  msg->Reset();

  TestCoroMachine machine;

  // Wait for trigger event in coroutine.
  machine.Spawn([&pub](TestCoroContext &c) {
    absl::StatusOr<struct pollfd> fd = pub->GetPollFd();
    ASSERT_OK(fd);
    c.Wait(fd->fd);
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  });

  // Read another message in coroutine.
  machine.Spawn([&sub](TestCoroContext &c) {
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
      "rel_dave", SubOpts().SetReliable(true).SetPassActivation(true));
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

  msg->Reset();

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

  msg->Reset();

  TestCoroMachine machine;

  // Wait for trigger event in coroutine.
  machine.Spawn([&pub](TestCoroContext &c) {
    struct pollfd fd = pub->GetPollFd();
    c.Wait(fd.fd);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "foobar", 6);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
    ASSERT_OK(pub_status);
  });

  // Read messages in coroutine.
  machine.Spawn([&sub](TestCoroContext &c) {
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

// Two coroutines share a reliable publisher.  One coroutine ("publisher") is
// in the middle of producing a message; the other ("waiter") wants the next
// buffer.  A "buffer_busy" flag models the in-progress publish: the waiter must
// not take a buffer until the buffer is both available *and* not busy, even
// though a freed slot (and the publisher's fd trigger) becomes available while
// the publish is still in flight.  After the publisher finishes it clears the
// flag and calls TriggerReliableWait() to wake the waiter.
TEST_F(ClientTest, ReliablePublisherBusyFlagTriggerWakeup) {
  subspace::Client client;
  InitClient(client);

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "rel_busy", 32, 5, subspace::PublisherOptions().SetReliable(true));
  ASSERT_OK(pub);
  // A reliable subscriber that does not read keeps every slot busy, so the
  // publisher cannot obtain a buffer until the subscriber drains them.
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      "rel_busy", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_OK(sub);

  // Fill the channel until the publisher can no longer get a buffer.  At this
  // point the buffer is unavailable.
  int published = 0;
  for (;;) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    if (*buffer == nullptr) {
      break;
    }
    memcpy(*buffer, "fill", 4);
    ASSERT_OK(pub->PublishMessage(4));
    published++;
  }
  ASSERT_GE(published, 4);

  bool buffer_busy = true;
  bool waiter_saw_busy = false;
  bool waiter_published = false;

  TestCoroMachine machine;

  // Waiter: wait until the buffer is available AND not busy before taking it.
  machine.Spawn([&](TestCoroContext &c) {
    for (;;) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer != nullptr) {
        if (!buffer_busy) {
          memcpy(*buffer, "late", 4);
          ASSERT_OK(pub->PublishMessage(4));
          waiter_published = true;
          return;
        }
        // Available but another coroutine still owns the in-progress publish.
        // Release the buffer/lock and wait for the busy flag to clear.
        waiter_saw_busy = true;
        pub->CancelPublish();
      }
      struct pollfd fd = pub->GetPollFd();
      c.Wait(fd.fd);
    }
  });

  // Publisher: drain the subscriber to free slots (which triggers the
  // publisher's fd), then publish while the waiter is still gated by the busy
  // flag, and finally clear the flag and wake the waiter.
  machine.Spawn([&](TestCoroContext &c) {
    for (int i = 0; i < published; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
    }
    // Give the waiter a chance to observe (available && busy) and go back to
    // sleep.
    c.Millisleep(50);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "main", 4);
    ASSERT_OK(pub->PublishMessage(4));

    buffer_busy = false;
    pub->TriggerReliableWait();
  });

  machine.Run();

  EXPECT_TRUE(waiter_saw_busy);
  EXPECT_TRUE(waiter_published);
}

// Same scenario as ReliablePublisherBusyFlagTriggerWakeup but with a client
// that is not in thread-safe mode (the typical single-threaded coroutine
// setup).  Without thread-safe mode there is no client lock to commit, so the
// waiter does not need to CancelPublish() the available-but-busy buffer; it
// simply goes back to waiting.
TEST_F(ClientTest, ReliablePublisherBusyFlagTriggerWakeupNotThreadSafe) {
  subspace::Client client;
  // Note: intentionally not calling InitClient(), which would enable
  // thread-safe mode.  Initialize the client directly to leave it
  // non-thread-safe.
  ASSERT_OK(client.Init(Socket()));

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "rel_busy_nts", 32, 5, subspace::PublisherOptions().SetReliable(true));
  ASSERT_OK(pub);
  // A reliable subscriber that does not read keeps every slot busy, so the
  // publisher cannot obtain a buffer until the subscriber drains them.
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      "rel_busy_nts", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_OK(sub);

  // Fill the channel until the publisher can no longer get a buffer.  At this
  // point the buffer is unavailable.
  int published = 0;
  for (;;) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    if (*buffer == nullptr) {
      break;
    }
    memcpy(*buffer, "fill", 4);
    ASSERT_OK(pub->PublishMessage(4));
    published++;
  }
  ASSERT_GE(published, 4);

  bool buffer_busy = true;
  bool waiter_saw_busy = false;
  bool waiter_published = false;

  TestCoroMachine machine;

  // Waiter: wait until the buffer is available AND not busy before taking it.
  machine.Spawn([&](TestCoroContext &c) {
    for (;;) {
      absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
      ASSERT_OK(buffer);
      if (*buffer != nullptr) {
        if (!buffer_busy) {
          memcpy(*buffer, "late", 4);
          ASSERT_OK(pub->PublishMessage(4));
          waiter_published = true;
          return;
        }
        // Available but another coroutine still owns the in-progress publish.
        // In non-thread-safe mode there is no lock to release, so just wait for
        // the busy flag to clear.
        waiter_saw_busy = true;
      }
      struct pollfd fd = pub->GetPollFd();
      c.Wait(fd.fd);
    }
  });

  // Publisher: drain the subscriber to free slots (which triggers the
  // publisher's fd), then publish while the waiter is still gated by the busy
  // flag, and finally clear the flag and wake the waiter.
  machine.Spawn([&](TestCoroContext &c) {
    for (int i = 0; i < published; i++) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
    }
    // Give the waiter a chance to observe (available && busy) and go back to
    // sleep.
    c.Millisleep(50);

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    memcpy(*buffer, "main", 4);
    ASSERT_OK(pub->PublishMessage(4));

    buffer_busy = false;
    pub->TriggerReliableWait();
  });

  machine.Run();

  EXPECT_TRUE(waiter_saw_busy);
  EXPECT_TRUE(waiter_published);
}

TEST_F(ClientTest, DroppedMessage) {
  subspace::Client client;
  InitClient(client);

  absl::StatusOr<Subscriber> sub =
      client.CreateSubscriber("rel_dave", SubOpts().SetKeepActiveMessage(true));
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
      "dave6", subspace::SubscriberOptions().SetMaxActiveMessages(3).SetKeepActiveMessage(false));
  ASSERT_OK(sub);

  absl::StatusOr<subspace::shared_ptr<const char>> p =
      sub->ReadMessage<const char>();
  ASSERT_OK(p);
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
      "dave6", subspace::SubscriberOptions().SetMaxActiveMessages(2).SetKeepActiveMessage(false));
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
    ASSERT_EQ(2, sub->NumActiveMessages());

    ASSERT_EQ(2, p2.use_count());
  }
  // Number of active messages: 1
}

class TestAliasedMessage {
public:
  TestAliasedMessage(const void *buffer, size_t length)
      : base_ptr_(const_cast<void *>(buffer)) {
    assert(length >= GetSize());
  }
  TestAliasedMessage() = default;

  static size_t GetSize() { return 20; }

  void SetName(std::string_view s) {
    assert(s.size() <= 12);
    std::memcpy(MutableBase(), s.data(), s.size());
    std::memset(MutableBase() + s.size(), 0, 12 - s.size());
  };
  std::string_view Name() const {
    std::string_view result{ConstBase(), 12};
    return result.substr(0, result.find('\0'));
  }

  void SetId(std::int32_t i) { std::memcpy(MutableBase() + 12, &i, sizeof(i)); }
  std::int32_t Id() const {
    std::int32_t result = 0;
    std::memcpy(&result, ConstBase() + 12, sizeof(result));
    return result;
  }

  void SetScore(float f) { std::memcpy(MutableBase() + 16, &f, sizeof(f)); }
  float Score() const {
    float result = 0;
    std::memcpy(&result, ConstBase() + 16, sizeof(result));
    return result;
  }

private:
  void *base_ptr_ = nullptr;
  char *MutableBase() { return reinterpret_cast<char *>(base_ptr_); }
  const char *ConstBase() const {
    return reinterpret_cast<const char *>(base_ptr_);
  }
};

struct TestAliasedMessageAliaser {
  void Set(const void *buffer, size_t length) {
    msg = TestAliasedMessage(buffer, length);
  }
  void Reset() { msg = TestAliasedMessage(); }
  void AliasTo(const void *buffer, size_t length,
               const TestAliasedMessage *&dest) const {
    dest = &msg;
  }
  TestAliasedMessage msg;
};

TEST_F(ClientTest, PublishSingleMessageAndReadAliasedSharedPtr) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher("dave6", TestAliasedMessage::GetSize(), 10);
  ASSERT_OK(pub);
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  {
    TestAliasedMessage pub_msg(*buffer, TestAliasedMessage::GetSize());
    pub_msg.SetName("foobar");
    pub_msg.SetId(42);
    pub_msg.SetScore(3.14F);
  }
  absl::StatusOr<const Message> pub_status =
      pub->PublishMessage(TestAliasedMessage::GetSize());
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("dave6", subspace::SubscriberOptions()
                                               .SetMaxActiveMessages(3)
                                               .SetKeepActiveMessage(false));
  ASSERT_OK(sub);

  using SharedPtrAliasedMsg =
      subspace::shared_ptr<const TestAliasedMessage, TestAliasedMessageAliaser>;
  absl::StatusOr<SharedPtrAliasedMsg> p =
      sub->ReadMessage<const TestAliasedMessage, TestAliasedMessageAliaser>();
  ASSERT_OK(p);
  const auto &ptr = *p;
  ASSERT_TRUE(static_cast<bool>(ptr));
  ASSERT_EQ("foobar", ptr->Name());
  ASSERT_EQ(42, ptr->Id());
  ASSERT_EQ(3.14F, ptr->Score());

  ASSERT_EQ(1, ptr.use_count());

  // Copy the shared ptr using copy constructor.
  SharedPtrAliasedMsg p2(ptr);
  ASSERT_EQ(2, ptr.use_count());
  ASSERT_EQ(2, p2.use_count());
  ASSERT_EQ("foobar", p2->Name());
  ASSERT_EQ(42, p2->Id());
  ASSERT_EQ(3.14F, p2->Score());

  // Copy using copy operator.
  SharedPtrAliasedMsg p3 = ptr;
  ASSERT_EQ(3, ptr.use_count());
  ASSERT_EQ(3, p2.use_count());
  ASSERT_EQ(3, p3.use_count());
  ASSERT_EQ("foobar", p3->Name());
  ASSERT_EQ(42, p3->Id());
  ASSERT_EQ(3.14F, p3->Score());

  // Move p3 to p4.
  SharedPtrAliasedMsg p4 = std::move(p3);
  ASSERT_FALSE(static_cast<bool>(p3));
  ASSERT_EQ(3, ptr.use_count());
  ASSERT_EQ(3, p2.use_count());
  ASSERT_EQ(0, p3.use_count());
  ASSERT_EQ(3, p4.use_count());
  ASSERT_EQ("foobar", p4->Name());
  ASSERT_EQ(42, p4->Id());
  ASSERT_EQ(3.14F, p4->Score());
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
    subspace::MessagePrefix *prefix = pub->Prefix();
    ASSERT_NE(nullptr, prefix);
    prefix->ordinal = static_cast<uint64_t>(i + 1);
    prefix->timestamp = static_cast<uint64_t>(123456789 + i);

    absl::StatusOr<const Message> pub_status = pub->PublishMessageWithPrefix(
        len + 1, /*use_slot_id_from_prefix=*/false);
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
  for (size_t i = 0; i < sent_msgs.size(); i++) {
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
      "dave6", PubOpts(256, 10).SetNotifyRetirement(true));
  ASSERT_OK(p);
  auto pub = std::move(*p);

  {
    // You can't create a virtual publisher with notify_retirement.
    absl::StatusOr<Publisher> p2 =
        pub_client->CreatePublisher("dave6v", PubOpts(256, 10).SetMux("/foobar").SetNotifyRetirement(true));
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
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1));
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1));
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

  sub1.ClearActiveMessage();
  sub2.ClearActiveMessage();
  std::cerr << "resetting first message" << std::endl;
  // Reset the first message.
  ptr1.Reset();
  // Reset the second message. This will trigger a retirement.
  std::cerr << "resetting second message" << std::endl;
  ptr2.Reset();

  // Read the retirement fd and expect it to contain slot 0.
  struct pollfd fd = {};
  fd.fd = retirement_fd.Fd();
  fd.events = POLLIN;
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
      "dave6", PubOpts(256, 10).SetNotifyRetirement(true));
  ASSERT_OK(p);
  auto pub = std::move(*p);

  const toolbelt::FileDescriptor &retirement_fd = pub.GetRetirementFd();

  absl::StatusOr<Subscriber> s1 =
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1).SetKeepActiveMessage(true));
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1).SetKeepActiveMessage(true));
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
  struct pollfd fd = {};
  fd.fd = retirement_fd.Fd();
  fd.events = POLLIN;
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
      "dave6", PubOpts(256, 10).SetNotifyRetirement(true));
  ASSERT_OK(p);
  auto pub = std::move(*p);

  // Another publisher on a different client.
  absl::StatusOr<Publisher> pp2 = pub2Client->CreatePublisher(
      "dave6", PubOpts(256, 10).SetNotifyRetirement(true));
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
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1));
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1));
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

  sub1.ClearActiveMessage();
  sub2.ClearActiveMessage();
  // Reset the first message.
  ptr1.Reset();
  // Reset the second message. This will trigger a retirement.
  ptr2.Reset();

  // Read the retirement fd and expect it to contain slot 0.
  struct pollfd fd = {};
  fd.fd = retirement_fd.Fd();
  fd.events = POLLIN;
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
      "dave6", PubOpts(256, 10).SetNotifyRetirement(true));
  ASSERT_OK(p);
  auto pub = std::move(*p);

  // Another publisher on a different client.
  absl::StatusOr<Publisher> pp2 = pub2Client->CreatePublisher(
      "dave6", PubOpts(256, 10).SetNotifyRetirement(true));
  ASSERT_OK(pp2);
  auto pub2 = std::move(*pp2);

  // We check for retirement trigger on the second publisher, not the one that
  // retires the slots.
  const toolbelt::FileDescriptor &retirement_fd = pub2.GetRetirementFd();

  absl::StatusOr<Subscriber> s1 =
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1).SetKeepActiveMessage(true));
  ASSERT_OK(s1);
  auto sub1 = std::move(*s1);

  absl::StatusOr<Subscriber> s2 =
      sub_client->CreateSubscriber("dave6", SubOpts().SetMaxActiveMessages(1).SetKeepActiveMessage(true));
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
  struct pollfd fd = {};
  fd.fd = retirement_fd.Fd();
  fd.events = POLLIN;
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
  constexpr char kClientName[] = "channel-directory-test";
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket(), kClientName));

  absl::StatusOr<Publisher> p1 =
      client->CreatePublisher("chan1", PubOpts(256, 10));
  ASSERT_OK(p1);

  absl::StatusOr<Publisher> p2 =
      client->CreatePublisher("chan2", PubOpts(256, 10));
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
      ASSERT_GE(info.channel_id(), 0);
      ASSERT_EQ(2, info.participants_size());
      bool found_publisher = false;
      bool found_subscriber = false;
      for (const subspace::ChannelParticipantInfoProto &participant :
           info.participants()) {
        EXPECT_EQ(kClientName, participant.program_name());
        EXPECT_EQ(static_cast<uint64_t>(getpid()), participant.pid());
        EXPECT_GE(participant.id(), 0);
        EXPECT_NE(participant.is_publisher(), participant.is_subscriber());
        found_publisher |= participant.is_publisher();
        found_subscriber |= participant.is_subscriber();
      }
      EXPECT_TRUE(found_publisher);
      EXPECT_TRUE(found_subscriber);
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

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan1", PubOpts(256, 10).SetType("test-type"));
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

// This tests checksums.  We have two publishers, one that calculates a checksum
// and one that doesn't. We have 2 subscribers, one that checks the checksum and
// expects an error if there's an error and one that checks the checksum and
// passes the message intact but with the checksum_error flag set.
TEST_F(ClientTest, ChecksumVerification) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan1", PubOpts(256, 10).SetChecksum(true));
  ASSERT_OK(pub);

  // Create a second publisher that doesn't calculate a checksum.
  absl::StatusOr<Publisher> pub2 =
      client->CreatePublisher("chan1", PubOpts(256, 10));
  ASSERT_OK(pub2);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan1", SubOpts().SetChecksum(true));
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
      "chan1", SubOpts().SetChecksum(true).SetPassChecksumErrors(true));
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

TEST_F(ClientTest, ChecksumCallback) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cb", PubOpts(256, 10).SetChecksum(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cb", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  auto fake_crc =
      [](const std::array<absl::Span<const uint8_t>, 3> &data,
         absl::Span<std::byte> checksum) {
    uint32_t sum = 0;
    for (const auto &span : data) {
      for (uint8_t byte : span) {
        sum += byte;
      }
    }
    *reinterpret_cast<uint32_t *>(checksum.data()) = sum;
  };

  pub->SetChecksumCallback(fake_crc);
  sub->SetChecksumCallback(fake_crc);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "abc", 3);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(3);
  ASSERT_OK(pub_status);

  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(3, msg->length);
    ASSERT_STREQ("abc", reinterpret_cast<const char *>(msg->buffer));
  }

  // Publish and corrupt the message buffer to force a checksum failure.
  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer();
  ASSERT_OK(buffer2);
  memcpy(*buffer2, "def", 3);
  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(3);
  ASSERT_OK(pub_status2);
  char *buf = reinterpret_cast<char *>(*buffer2);
  buf[1] = 'X';

  {
    absl::StatusOr<subspace::Message> msg2 = sub->ReadMessage();
    ASSERT_FALSE(msg2.ok());
    ASSERT_EQ(absl::StatusCode::kInternal, msg2.status().code());
    ASSERT_EQ("Checksum verification failed", msg2.status().message());
  }
}

TEST_F(ClientTest, ChecksumCallbackPassErrors) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cb_pass", PubOpts(256, 10).SetChecksum(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = client->CreateSubscriber(
      "chan_cb_pass", SubOpts().SetChecksum(true).SetPassChecksumErrors(true));
  ASSERT_OK(sub);

  auto fake_crc =
      [](const std::array<absl::Span<const uint8_t>, 3> &data,
         absl::Span<std::byte> checksum) {
    uint32_t sum = 0;
    for (const auto &span : data) {
      for (uint8_t byte : span) {
        sum += byte;
      }
    }
    *reinterpret_cast<uint32_t *>(checksum.data()) = sum;
  };

  pub->SetChecksumCallback(fake_crc);
  sub->SetChecksumCallback(fake_crc);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "xyz", 3);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(3);
  ASSERT_OK(pub_status);
  char *buf = reinterpret_cast<char *>(*buffer);
  buf[2] = 'Z';

  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(3, msg->length);
    ASSERT_TRUE(msg->checksum_error);
  }
}

TEST_F(ClientTest, ChecksumCallbackReset) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cb_reset", PubOpts(256, 10).SetChecksum(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cb_reset", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  auto fake_crc =
      [](const std::array<absl::Span<const uint8_t>, 3> &data,
         absl::Span<std::byte> checksum) {
    uint32_t sum = 0;
    for (const auto &span : data) {
      for (uint8_t byte : span) {
        sum += byte;
      }
    }
    *reinterpret_cast<uint32_t *>(checksum.data()) = sum;
  };

  auto fake_crc_mismatch =
      [&fake_crc](const std::array<absl::Span<const uint8_t>, 3> &data,
                  absl::Span<std::byte> checksum) {
    fake_crc(data, checksum);
    checksum[0] ^= std::byte{0xFF};
  };

  pub->SetChecksumCallback(fake_crc);
  sub->SetChecksumCallback(fake_crc_mismatch);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "reset", 5);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_FALSE(msg.ok());
    ASSERT_EQ(absl::StatusCode::kInternal, msg.status().code());
    ASSERT_EQ("Checksum verification failed", msg.status().message());
  }

  pub->ResetChecksumCallback();
  sub->ResetChecksumCallback();

  absl::StatusOr<void *> buffer2 = pub->GetMessageBuffer();
  ASSERT_OK(buffer2);
  memcpy(*buffer2, "reset", 5);
  absl::StatusOr<const Message> pub_status2 = pub->PublishMessage(5);
  ASSERT_OK(pub_status2);

  {
    absl::StatusOr<subspace::Message> msg2 = sub->ReadMessage();
    ASSERT_OK(msg2);
    ASSERT_EQ(5, msg2->length);
    ASSERT_STREQ("reset", reinterpret_cast<const char *>(msg2->buffer));
  }
}

TEST_F(ClientTest, ChecksumCallbackPublisherOnly) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cb_pub_only",
      PubOpts(256, 10).SetChecksum(true));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cb_pub_only", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  auto fake_crc =
      [](const std::array<absl::Span<const uint8_t>, 3> &data,
         absl::Span<std::byte> checksum) {
    uint32_t sum = 0;
    for (const auto &span : data) {
      for (uint8_t byte : span) {
        sum += byte;
      }
    }
    *reinterpret_cast<uint32_t *>(checksum.data()) = sum;
  };

  pub->SetChecksumCallback(fake_crc);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "onlypub", 7);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
  ASSERT_OK(pub_status);

  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_FALSE(msg.ok());
    ASSERT_EQ(absl::StatusCode::kInternal, msg.status().code());
    ASSERT_EQ("Checksum verification failed", msg.status().message());
  }
}

TEST_F(ClientTest, Checksum20Byte) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_ck20",
      PubOpts(256, 10).SetChecksum(true).SetChecksumSize(20));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_ck20", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  ASSERT_EQ(128, pub->PrefixSize());
  ASSERT_EQ(128, sub->PrefixSize());
  ASSERT_EQ(20, pub->ChecksumSize());
  ASSERT_EQ(20, sub->ChecksumSize());

  // 20-byte checksum: 5 CRC32 values computed with different seeds.
  auto checksum_20 =
      [](const std::array<absl::Span<const uint8_t>, 3> &data,
         absl::Span<std::byte> checksum) {
    ASSERT_GE(checksum.size(), 20u);
    uint32_t *out = reinterpret_cast<uint32_t *>(checksum.data());
    for (int k = 0; k < 5; k++) {
      uint32_t crc = 0xFFFFFFFF ^ static_cast<uint32_t>(k * 0x11111111);
      for (const auto &span : data) {
        crc = subspace::SubspaceCRC32(crc, span.data(), span.size());
      }
      out[k] = ~crc;
    }
  };

  pub->SetChecksumCallback(checksum_20);
  sub->SetChecksumCallback(checksum_20);

  // Successful publish and read.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "hello20", 7);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
    ASSERT_OK(pub_status);

    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(7, msg->length);
    ASSERT_EQ(0, memcmp("hello20", msg->buffer, 7));
  }

  // Corrupt message data after publish to trigger checksum failure.
  {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    memcpy(*buffer, "corrupt", 7);
    absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
    ASSERT_OK(pub_status);
    char *buf = reinterpret_cast<char *>(*buffer);
    buf[0] = 'X';

    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_FALSE(msg.ok());
    ASSERT_EQ(absl::StatusCode::kInternal, msg.status().code());
    ASSERT_EQ("Checksum verification failed", msg.status().message());
  }
}

TEST_F(ClientTest, ChecksumSizeDefault) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_def", PubOpts(256, 10));
  ASSERT_OK(pub);
  ASSERT_EQ(64, pub->PrefixSize());
  ASSERT_EQ(4, pub->ChecksumSize());
  ASSERT_EQ(0, pub->MetadataSize());
}

TEST_F(ClientTest, LargeChecksumSize) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  // checksum_size=32 → Aligned<64>(48 + 32) = 128 bytes prefix.
  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_big", PubOpts(256, 10).SetChecksumSize(32));
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());
  ASSERT_EQ(32, pub->ChecksumSize());
}

TEST_F(ClientTest, MetadataSize) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  // metadata_size=100 → Aligned<64>(48 + 4 + 100) = Aligned<64>(152) = 192.
  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_meta", PubOpts(256, 10).SetMetadataSize(100));
  ASSERT_OK(pub);
  ASSERT_EQ(192, pub->PrefixSize());
  ASSERT_EQ(4, pub->ChecksumSize());
  ASSERT_EQ(100, pub->MetadataSize());
}

TEST_F(ClientTest, PrefixSizeInconsistent) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub1 = client->CreatePublisher(
      "chan_ps_incon",
      PubOpts(256, 10).SetChecksumSize(20));
  ASSERT_OK(pub1);

  // A second publisher with different sizes should fail.
  absl::StatusOr<Publisher> pub2 = client->CreatePublisher(
      "chan_ps_incon",
      PubOpts(256, 10).SetChecksumSize(32));
  ASSERT_FALSE(pub2.ok());
}

// Virtual channels share storage on a multiplexer, so they must share a
// single prefix layout. The mux owns the layout: the first publisher to any
// vchan on a given mux fixes cs/ms, subsequent publishers (on the same vchan
// or any sibling vchan) must agree, and subscribers on any vchan see the
// authoritative sizes from the mux.
TEST_F(ClientTest, VirtualChannelMuxPrefixIsShared) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  // First publisher on vchan_a with checksum_size=20, metadata_size=50:
  //   48 + 20 + 50 = 118 → Aligned<64> = 128.
  absl::StatusOr<Publisher> pub_a = client->CreatePublisher(
      "vchan_a", PubOpts(256, 10).SetMux("shared_mux").SetChecksumSize(20).SetMetadataSize(50));
  ASSERT_OK(pub_a);
  ASSERT_EQ(128, pub_a->PrefixSize());
  ASSERT_EQ(20, pub_a->ChecksumSize());
  ASSERT_EQ(50, pub_a->MetadataSize());

  // A subscriber on a different vchan on the same mux must see the mux's
  // sizes, not the per-vchan defaults (4/0/64).
  absl::StatusOr<Subscriber> sub_b =
      client->CreateSubscriber("vchan_b", SubOpts().SetMux("shared_mux"));
  ASSERT_OK(sub_b);
  ASSERT_EQ(128, sub_b->PrefixSize());
  ASSERT_EQ(20, sub_b->ChecksumSize());
  ASSERT_EQ(50, sub_b->MetadataSize());

  // A second publisher on a different vchan on the same mux with matching
  // sizes must succeed and report the same prefix layout.
  absl::StatusOr<Publisher> pub_b = client->CreatePublisher(
      "vchan_b", PubOpts(256, 10).SetMux("shared_mux").SetChecksumSize(20).SetMetadataSize(50));
  ASSERT_OK(pub_b);
  ASSERT_EQ(128, pub_b->PrefixSize());

  // A second publisher on yet another vchan with mismatching cs/ms must be
  // rejected, because virtual channels on a mux cannot disagree on layout.
  absl::StatusOr<Publisher> pub_c = client->CreatePublisher(
      "vchan_c", PubOpts(256, 10).SetMux("shared_mux").SetChecksumSize(32));
  ASSERT_FALSE(pub_c.ok());

  absl::StatusOr<Publisher> pub_d = client->CreatePublisher(
      "vchan_d", PubOpts(256, 10).SetMux("shared_mux").SetMetadataSize(100));
  ASSERT_FALSE(pub_d.ok());
}

// Symmetric scenario: a subscriber creates the placeholder mux + virtual
// channel first, then a publisher arrives with non-default sizes. Subscribers
// on sibling virtual channels created after that must see the publisher's
// sizes via the mux.
TEST_F(ClientTest, VirtualChannelMuxPrefixSubscriberFirst) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Subscriber> sub_a =
      client->CreateSubscriber("vchan_a", SubOpts().SetMux("sub_first_mux"));
  ASSERT_OK(sub_a);

  absl::StatusOr<Publisher> pub_a = client->CreatePublisher(
      "vchan_a", PubOpts(256, 10).SetMux("sub_first_mux").SetChecksumSize(20).SetMetadataSize(50));
  ASSERT_OK(pub_a);
  ASSERT_EQ(128, pub_a->PrefixSize());

  // Subscriber on a sibling vchan should now see the mux's sizes.
  absl::StatusOr<Subscriber> sub_b =
      client->CreateSubscriber("vchan_b", SubOpts().SetMux("sub_first_mux"));
  ASSERT_OK(sub_b);
  ASSERT_EQ(128, sub_b->PrefixSize());
  ASSERT_EQ(20, sub_b->ChecksumSize());
  ASSERT_EQ(50, sub_b->MetadataSize());
}

TEST_F(ClientTest, SubscriberGetsSizes) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_sub_sizes",
      PubOpts(256, 10).SetChecksumSize(20).SetMetadataSize(50));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_sub_sizes");
  ASSERT_OK(sub);

  ASSERT_EQ(pub->PrefixSize(), sub->PrefixSize());
  ASSERT_EQ(20, sub->ChecksumSize());
  ASSERT_EQ(50, sub->MetadataSize());
}

// metadata_size=8, checksum_size=4: 48+4+8=60 → prefix=64
TEST_F(ClientTest, MetadataSmall) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_meta_s", PubOpts(256, 10).SetMetadataSize(8));
  ASSERT_OK(pub);
  ASSERT_EQ(64, pub->PrefixSize());
  ASSERT_EQ(8, pub->MetadataSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_meta_s");
  ASSERT_OK(sub);
  ASSERT_EQ(8, sub->MetadataSize());

  // Write metadata, publish, then read and verify.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "hello", 5);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(8u, meta.size());
  memcpy(meta.data(), "METADAT!", 8);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(5, msg->length);
  ASSERT_EQ(0, memcmp("hello", msg->buffer, 5));

  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(8u, sub_meta.size());
  ASSERT_EQ(0, memcmp("METADAT!", sub_meta.data(), 8));
}

// metadata_size=12, checksum_size=4: 48+4+12=64 → prefix=64 (exact fit)
TEST_F(ClientTest, MetadataExactFit) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_meta_ex",
      PubOpts(256, 10).SetMetadataSize(12));
  ASSERT_OK(pub);
  ASSERT_EQ(64, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_meta_ex");
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "exact", 5);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(12u, meta.size());
  memcpy(meta.data(), "EXACTLY12!!!", 12);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(12u, sub_meta.size());
  ASSERT_EQ(0, memcmp("EXACTLY12!!!", sub_meta.data(), 12));
}

// metadata_size=13, checksum_size=4: 48+4+13=65 → prefix=128 (spills to 2nd chunk)
TEST_F(ClientTest, MetadataSpillsToSecondChunk) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_meta_sp",
      PubOpts(256, 10).SetMetadataSize(13));
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_meta_sp");
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "spill", 5);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(13u, meta.size());
  memcpy(meta.data(), "SPILL_13BYTES", 13);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(13u, sub_meta.size());
  ASSERT_EQ(0, memcmp("SPILL_13BYTES", sub_meta.data(), 13));
}

// metadata_size=200, checksum_size=32: 48+32+200=280 → prefix=320 (5 chunks)
TEST_F(ClientTest, MetadataLargeWithLargeChecksum) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_meta_lg",
      PubOpts(512, 10).SetChecksumSize(32).SetMetadataSize(200));
  ASSERT_OK(pub);
  ASSERT_EQ(320, pub->PrefixSize());
  ASSERT_EQ(32, pub->ChecksumSize());
  ASSERT_EQ(200, pub->MetadataSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_meta_lg");
  ASSERT_OK(sub);
  ASSERT_EQ(320, sub->PrefixSize());
  ASSERT_EQ(200, sub->MetadataSize());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "largemetadata", 13);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(200u, meta.size());
  // Fill metadata with a recognizable pattern.
  for (int i = 0; i < 200; i++) {
    meta[i] = static_cast<std::byte>(i & 0xFF);
  }

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(13);
  ASSERT_OK(pub_status);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(13, msg->length);

  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(200u, sub_meta.size());
  for (int i = 0; i < 200; i++) {
    ASSERT_EQ(static_cast<std::byte>(i & 0xFF), sub_meta[i]) << "at index " << i;
  }
}

TEST_F(ClientTest, ChecksumSizeTooLarge) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_big_fail",
      PubOpts(256, 10).SetChecksumSize(0x10000));
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, MetadataSizeTooLarge) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_ms_big_fail",
      PubOpts(256, 10).SetMetadataSize(0x10000));
  ASSERT_FALSE(pub.ok());
}

TEST_F(ClientTest, ChecksumSizeAtMax) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_max",
      PubOpts(0x20000, 2).SetChecksumSize(0xFFFF));
  ASSERT_OK(pub);
  ASSERT_EQ(0xFFFF, pub->ChecksumSize());
}

TEST_F(ClientTest, MetadataSizeAtMax) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_ms_max",
      PubOpts(0x20000, 2).SetMetadataSize(0xFFFF));
  ASSERT_OK(pub);
  ASSERT_EQ(0xFFFF, pub->MetadataSize());
}

// metadata_size=0: GetMetadata returns empty span.
TEST_F(ClientTest, MetadataZero) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_meta_z", PubOpts(256, 10));
  ASSERT_OK(pub);
  ASSERT_EQ(64, pub->PrefixSize());
  ASSERT_EQ(0, pub->MetadataSize());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);

  auto meta = pub->GetMetadata();
  ASSERT_TRUE(meta.empty());
}

// Multiple publishes with different metadata each time.
TEST_F(ClientTest, MetadataMultipleMessages) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_meta_mm",
      PubOpts(256, 10).SetMetadataSize(16));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_meta_mm");
  ASSERT_OK(sub);

  for (int i = 0; i < 5; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    char data[4];
    snprintf(data, sizeof(data), "m%02d", i);
    memcpy(*buffer, data, 3);

    auto meta = pub->GetMetadata();
    ASSERT_EQ(16u, meta.size());
    memset(meta.data(), 0, 16);
    uint32_t tag = 0xDEAD0000 | i;
    memcpy(meta.data(), &tag, sizeof(tag));

    absl::StatusOr<const Message> pub_status = pub->PublishMessage(3);
    ASSERT_OK(pub_status);

    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    ASSERT_EQ(3, msg->length);

    auto sub_meta = sub->GetMetadata();
    ASSERT_EQ(16u, sub_meta.size());
    uint32_t read_tag;
    memcpy(&read_tag, sub_meta.data(), sizeof(read_tag));
    ASSERT_EQ(tag, read_tag);
  }
}

// Checksum + metadata: successful round-trip.
TEST_F(ClientTest, ChecksumWithMetadata) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_meta",
      PubOpts(256, 10).SetChecksum(true).SetMetadataSize(16));
  ASSERT_OK(pub);
  ASSERT_EQ(4, pub->ChecksumSize());
  ASSERT_EQ(16, pub->MetadataSize());
  // 48 + 4 + 16 = 68 → Aligned<64> = 128
  ASSERT_EQ(128, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cs_meta", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "hello", 5);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(16u, meta.size());
  memcpy(meta.data(), "META_CHECKSUM!!\0", 16);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(5, msg->length);
  ASSERT_STREQ("hello", reinterpret_cast<const char *>(msg->buffer));

  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(16u, sub_meta.size());
  ASSERT_EQ(0, memcmp("META_CHECKSUM!!\0", sub_meta.data(), 16));
}

// Checksum + metadata: corrupt the message payload after publish → checksum error.
TEST_F(ClientTest, ChecksumWithMetadataCorruptPayload) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_meta_cp",
      PubOpts(256, 10).SetChecksum(true).SetMetadataSize(16));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cs_meta_cp", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "foobar", 6);

  auto meta = pub->GetMetadata();
  memcpy(meta.data(), "ABCDEFGHIJKLMNOP", 16);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Corrupt the payload after publishing.
  reinterpret_cast<char *>(*buffer)[0] = 'X';

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_FALSE(msg.ok());
  ASSERT_EQ(absl::StatusCode::kInternal, msg.status().code());
}

// Checksum + metadata: corrupt the metadata after publish → checksum error.
TEST_F(ClientTest, ChecksumWithMetadataCorruptMetadata) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_meta_cm",
      PubOpts(256, 10).SetChecksum(true).SetMetadataSize(16));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cs_meta_cm", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "intact", 6);

  auto meta = pub->GetMetadata();
  memcpy(meta.data(), "ABCDEFGHIJKLMNOP", 16);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Corrupt the metadata after publishing.
  meta[0] = std::byte{0xFF};

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_FALSE(msg.ok());
  ASSERT_EQ(absl::StatusCode::kInternal, msg.status().code());
}

// Checksum + metadata: corrupt metadata, pass_checksum_errors → message
// delivered with checksum_error flag set.
TEST_F(ClientTest, ChecksumWithMetadataCorruptMetadataPassError) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_meta_pe",
      PubOpts(256, 10).SetChecksum(true).SetMetadataSize(16));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = client->CreateSubscriber(
      "chan_cs_meta_pe", SubOpts().SetChecksum(true).SetPassChecksumErrors(true));
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "intact", 6);

  auto meta = pub->GetMetadata();
  memcpy(meta.data(), "ABCDEFGHIJKLMNOP", 16);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Corrupt metadata.
  meta[15] ^= std::byte{0x01};

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_TRUE(msg->checksum_error);
}

// Verify that padding bytes between (checksum + metadata) and the next 64-byte
// boundary are NOT covered by the checksum.  Corrupting them must not
// invalidate the checksum.
TEST_F(ClientTest, ChecksumIgnoresPrefixPadding) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  // checksum_size=4, metadata_size=16 → used=48+4+16=68, prefix=128.
  // Padding region is bytes [68..128) relative to prefix start.
  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_pad",
      PubOpts(256, 10).SetChecksum(true).SetMetadataSize(16));
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cs_pad", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "padtest", 7);

  auto meta = pub->GetMetadata();
  memcpy(meta.data(), "0123456789abcdef", 16);

  subspace::MessagePrefix *prefix = pub->Prefix();
  ASSERT_NE(nullptr, prefix);
  char *prefix_base = reinterpret_cast<char *>(prefix);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(7);
  ASSERT_OK(pub_status);

  // Padding starts after checksum + metadata: offset 48 + 4 + 16 = 68.
  int32_t used = offsetof(subspace::MessagePrefix, checksum) +
                 pub->ChecksumSize() + pub->MetadataSize();
  int32_t pad_len = pub->PrefixSize() - used;
  ASSERT_GT(pad_len, 0);

  // Scribble over the entire padding region.
  memset(prefix_base + used, 0xAA, pad_len);

  // Checksum should still be valid because padding is not checksummed.
  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(7, msg->length);
  ASSERT_STREQ("padtest", reinterpret_cast<const char *>(msg->buffer));
}

// Same test with a larger checksum (20 bytes) to ensure the padding boundary
// is computed correctly for non-default checksum sizes.
TEST_F(ClientTest, ChecksumIgnoresPrefixPaddingLargeChecksum) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  // checksum_size=20, metadata_size=32 → used=48+20+32=100, prefix=128.
  // Padding region is bytes [100..128).
  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cs_pad_lg",
      PubOpts(256, 10).SetChecksum(true).SetChecksumSize(20).SetMetadataSize(32));
  ASSERT_OK(pub);
  ASSERT_EQ(128, pub->PrefixSize());
  ASSERT_EQ(20, pub->ChecksumSize());
  ASSERT_EQ(32, pub->MetadataSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cs_pad_lg", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "bigpad", 6);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(32u, meta.size());
  for (int i = 0; i < 32; i++) {
    meta[i] = static_cast<std::byte>(i);
  }

  subspace::MessagePrefix *prefix = pub->Prefix();
  ASSERT_NE(nullptr, prefix);
  char *prefix_base = reinterpret_cast<char *>(prefix);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  // Scribble over the padding region.
  int32_t used = offsetof(subspace::MessagePrefix, checksum) +
                 pub->ChecksumSize() + pub->MetadataSize();
  int32_t pad_len = pub->PrefixSize() - used;
  ASSERT_GT(pad_len, 0);
  memset(prefix_base + used, 0xBB, pad_len);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_STREQ("bigpad", reinterpret_cast<const char *>(msg->buffer));

  // Verify metadata survived.
  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(32u, sub_meta.size());
  for (int i = 0; i < 32; i++) {
    ASSERT_EQ(static_cast<std::byte>(i), sub_meta[i]) << "at index " << i;
  }
}

// ---------------------------------------------------------------------------
// Self-contained AES-128-CMAC (RFC 4493) for checksum callback tests.
// Only forward encryption is needed.
//
// NOTE: this is AI generated and has not be validated for use in anything
// but a test.  I have no way of knowing whether it is correct or not.
// ---------------------------------------------------------------------------
namespace {

// FIPS 197 S-box.
static const uint8_t kAesSbox[256] = {
    0x63,0x7c,0x77,0x7b,0xf2,0x6b,0x6f,0xc5,0x30,0x01,0x67,0x2b,0xfe,0xd7,0xab,0x76,
    0xca,0x82,0xc9,0x7d,0xfa,0x59,0x47,0xf0,0xad,0xd4,0xa2,0xaf,0x9c,0xa4,0x72,0xc0,
    0xb7,0xfd,0x93,0x26,0x36,0x3f,0xf7,0xcc,0x34,0xa5,0xe5,0xf1,0x71,0xd8,0x31,0x15,
    0x04,0xc7,0x23,0xc3,0x18,0x96,0x05,0x9a,0x07,0x12,0x80,0xe2,0xeb,0x27,0xb2,0x75,
    0x09,0x83,0x2c,0x1a,0x1b,0x6e,0x5a,0xa0,0x52,0x3b,0xd6,0xb3,0x29,0xe3,0x2f,0x84,
    0x53,0xd1,0x00,0xed,0x20,0xfc,0xb1,0x5b,0x6a,0xcb,0xbe,0x39,0x4a,0x4c,0x58,0xcf,
    0xd0,0xef,0xaa,0xfb,0x43,0x4d,0x33,0x85,0x45,0xf9,0x02,0x7f,0x50,0x3c,0x9f,0xa8,
    0x51,0xa3,0x40,0x8f,0x92,0x9d,0x38,0xf5,0xbc,0xb6,0xda,0x21,0x10,0xff,0xf3,0xd2,
    0xcd,0x0c,0x13,0xec,0x5f,0x97,0x44,0x17,0xc4,0xa7,0x7e,0x3d,0x64,0x5d,0x19,0x73,
    0x60,0x81,0x4f,0xdc,0x22,0x2a,0x90,0x88,0x46,0xee,0xb8,0x14,0xde,0x5e,0x0b,0xdb,
    0xe0,0x32,0x3a,0x0a,0x49,0x06,0x24,0x5c,0xc2,0xd3,0xac,0x62,0x91,0x95,0xe4,0x79,
    0xe7,0xc8,0x37,0x6d,0x8d,0xd5,0x4e,0xa9,0x6c,0x56,0xf4,0xea,0x65,0x7a,0xae,0x08,
    0xba,0x78,0x25,0x2e,0x1c,0xa6,0xb4,0xc6,0xe8,0xdd,0x74,0x1f,0x4b,0xbd,0x8b,0x8a,
    0x70,0x3e,0xb5,0x66,0x48,0x03,0xf6,0x0e,0x61,0x35,0x57,0xb9,0x86,0xc1,0x1d,0x9e,
    0xe1,0xf8,0x98,0x11,0x69,0xd9,0x8e,0x94,0x9b,0x1e,0x87,0xe9,0xce,0x55,0x28,0xdf,
    0x8c,0xa1,0x89,0x0d,0xbf,0xe6,0x42,0x68,0x41,0x99,0x2d,0x0f,0xb0,0x54,0xbb,0x16,
};

static const uint8_t kRcon[10] = {
    0x01,0x02,0x04,0x08,0x10,0x20,0x40,0x80,0x1b,0x36};

inline void Xor128(uint8_t *dst, const uint8_t *src) {
  for (int i = 0; i < 16; i++) dst[i] ^= src[i];
}

inline uint8_t Gmul2(uint8_t a) {
  return static_cast<uint8_t>((a << 1) ^ ((a >> 7) * 0x1b));
}

void Aes128KeyExpand(const uint8_t key[16], uint8_t rk[176]) {
  memcpy(rk, key, 16);
  for (int i = 0; i < 10; i++) {
    const uint8_t *prev = rk + 16 * i;
    uint8_t *next = rk + 16 * (i + 1);
    uint8_t t[4] = {
        static_cast<uint8_t>(kAesSbox[prev[13]] ^ kRcon[i]),
        kAesSbox[prev[14]],
        kAesSbox[prev[15]],
        kAesSbox[prev[12]]};
    for (int j = 0; j < 4; j++) next[j] = prev[j] ^ t[j];
    for (int w = 1; w < 4; w++)
      for (int j = 0; j < 4; j++)
        next[4 * w + j] = prev[4 * w + j] ^ next[4 * (w - 1) + j];
  }
}

void MixColumns(uint8_t s[16]) {
  for (int c = 0; c < 4; c++) {
    uint8_t *col = s + 4 * c;
    uint8_t a0 = col[0], a1 = col[1], a2 = col[2], a3 = col[3];
    col[0] = Gmul2(a0) ^ Gmul2(a1) ^ a1 ^ a2 ^ a3;
    col[1] = a0 ^ Gmul2(a1) ^ Gmul2(a2) ^ a2 ^ a3;
    col[2] = a0 ^ a1 ^ Gmul2(a2) ^ Gmul2(a3) ^ a3;
    col[3] = Gmul2(a0) ^ a0 ^ a1 ^ a2 ^ Gmul2(a3);
  }
}

void Aes128Encrypt(const uint8_t rk[176], uint8_t block[16]) {
  Xor128(block, rk);
  for (int r = 1; r <= 10; r++) {
    for (int i = 0; i < 16; i++) block[i] = kAesSbox[block[i]];
    // ShiftRows (column-major state).
    // Row 1: left by 1.
    uint8_t t = block[1];
    block[1] = block[5]; block[5] = block[9];
    block[9] = block[13]; block[13] = t;
    // Row 2: left by 2.
    std::swap(block[2], block[10]);
    std::swap(block[6], block[14]);
    // Row 3: left by 3 (= right by 1).
    t = block[15];
    block[15] = block[11]; block[11] = block[7];
    block[7] = block[3]; block[3] = t;
    if (r < 10) MixColumns(block);
    Xor128(block, rk + 16 * r);
  }
}

void ShiftLeft128(const uint8_t in[16], uint8_t out[16]) {
  for (int i = 0; i < 15; i++)
    out[i] = static_cast<uint8_t>((in[i] << 1) | (in[i + 1] >> 7));
  out[15] = static_cast<uint8_t>(in[15] << 1);
}

// AES-128-CMAC (RFC 4493) over a concatenation of spans.
void Aes128Cmac(const uint8_t key[16],
                const std::array<absl::Span<const uint8_t>, 3> &data,
                uint8_t mac[16]) {
  uint8_t rk[176];
  Aes128KeyExpand(key, rk);

  // Generate subkeys K1, K2.
  uint8_t L[16] = {};
  Aes128Encrypt(rk, L);
  uint8_t K1[16], K2[16];
  ShiftLeft128(L, K1);
  if (L[0] & 0x80) K1[15] ^= 0x87;
  ShiftLeft128(K1, K2);
  if (K1[0] & 0x80) K2[15] ^= 0x87;

  size_t total = 0;
  for (const auto &s : data) total += s.size();

  uint8_t X[16] = {};
  uint8_t blk[16];
  size_t pos = 0;
  size_t processed = 0;

  for (const auto &span : data) {
    for (size_t i = 0; i < span.size(); i++) {
      blk[pos++] = span[i];
      processed++;
      if (pos == 16 && processed < total) {
        Xor128(X, blk);
        Aes128Encrypt(rk, X);
        pos = 0;
      }
    }
  }

  if (pos == 16 && total > 0) {
    Xor128(blk, K1);
  } else {
    blk[pos++] = 0x80;
    while (pos < 16) blk[pos++] = 0x00;
    Xor128(blk, K2);
  }
  Xor128(X, blk);
  Aes128Encrypt(rk, X);
  memcpy(mac, X, 16);
}

// Fixed test key (RFC 4493 test vector key).
static const uint8_t kCmacTestKey[16] = {
    0x2b,0x7e,0x15,0x16,0x28,0xae,0xd2,0xa6,
    0xab,0xf7,0x15,0x88,0x09,0xcf,0x4f,0x3c};

// ChecksumCallback wrapper that computes AES-128-CMAC.
void CmacChecksumCallback(
    const std::array<absl::Span<const uint8_t>, 3> &data,
    absl::Span<std::byte> checksum) {
  uint8_t mac[16];
  Aes128Cmac(kCmacTestKey, data, mac);
  memcpy(checksum.data(), mac, std::min<size_t>(checksum.size(), 16));
}

} // anonymous namespace

// Validate the AES-128-CMAC implementation against RFC 4493 test vectors.
TEST(Aes128CmacTest, Rfc4493Vectors) {
  // Example 2: 16-byte message (one complete block).
  {
    const uint8_t msg[16] = {
        0x6b,0xc1,0xbe,0xe2,0x2e,0x40,0x9f,0x96,
        0xe9,0x3d,0x7e,0x11,0x73,0x93,0x17,0x2a};
    const uint8_t expected[16] = {
        0x07,0x0a,0x16,0xb4,0x6b,0x4d,0x41,0x44,
        0xf7,0x9b,0xdd,0x9d,0xd0,0x4a,0x28,0x7c};
    std::array<absl::Span<const uint8_t>, 3> data = {
        absl::Span<const uint8_t>(msg, 16),
        absl::Span<const uint8_t>(),
        absl::Span<const uint8_t>()};
    uint8_t mac[16];
    Aes128Cmac(kCmacTestKey, data, mac);
    ASSERT_EQ(0, memcmp(mac, expected, 16));
  }
  // Example 3: 40-byte message (not a multiple of 16).
  {
    const uint8_t msg[40] = {
        0x6b,0xc1,0xbe,0xe2,0x2e,0x40,0x9f,0x96,
        0xe9,0x3d,0x7e,0x11,0x73,0x93,0x17,0x2a,
        0xae,0x2d,0x8a,0x57,0x1e,0x03,0xac,0x9c,
        0x9e,0xb7,0x6f,0xac,0x45,0xaf,0x8e,0x51,
        0x30,0xc8,0x1c,0x46,0xa3,0x5c,0xe4,0x11};
    const uint8_t expected[16] = {
        0xdf,0xa6,0x67,0x47,0xde,0x9a,0xe6,0x30,
        0x30,0xca,0x32,0x61,0x14,0x97,0xc8,0x27};
    std::array<absl::Span<const uint8_t>, 3> data = {
        absl::Span<const uint8_t>(msg, 40),
        absl::Span<const uint8_t>(),
        absl::Span<const uint8_t>()};
    uint8_t mac[16];
    Aes128Cmac(kCmacTestKey, data, mac);
    ASSERT_EQ(0, memcmp(mac, expected, 16));
  }
  // Example 4: 64-byte message (four complete blocks).
  {
    const uint8_t msg[64] = {
        0x6b,0xc1,0xbe,0xe2,0x2e,0x40,0x9f,0x96,
        0xe9,0x3d,0x7e,0x11,0x73,0x93,0x17,0x2a,
        0xae,0x2d,0x8a,0x57,0x1e,0x03,0xac,0x9c,
        0x9e,0xb7,0x6f,0xac,0x45,0xaf,0x8e,0x51,
        0x30,0xc8,0x1c,0x46,0xa3,0x5c,0xe4,0x11,
        0xe5,0xfb,0xc1,0x19,0x1a,0x0a,0x52,0xef,
        0xf6,0x9f,0x24,0x45,0xdf,0x4f,0x9b,0x17,
        0xad,0x2b,0x41,0x7b,0xe6,0x6c,0x37,0x10};
    const uint8_t expected[16] = {
        0x51,0xf0,0xbe,0xbf,0x7e,0x3b,0x9d,0x92,
        0xfc,0x49,0x74,0x17,0x79,0x36,0x3c,0xfe};
    std::array<absl::Span<const uint8_t>, 3> data = {
        absl::Span<const uint8_t>(msg, 64),
        absl::Span<const uint8_t>(),
        absl::Span<const uint8_t>()};
    uint8_t mac[16];
    Aes128Cmac(kCmacTestKey, data, mac);
    ASSERT_EQ(0, memcmp(mac, expected, 16));
  }
  // Same 40-byte message split across all three spans produces identical MAC.
  {
    const uint8_t msg[40] = {
        0x6b,0xc1,0xbe,0xe2,0x2e,0x40,0x9f,0x96,
        0xe9,0x3d,0x7e,0x11,0x73,0x93,0x17,0x2a,
        0xae,0x2d,0x8a,0x57,0x1e,0x03,0xac,0x9c,
        0x9e,0xb7,0x6f,0xac,0x45,0xaf,0x8e,0x51,
        0x30,0xc8,0x1c,0x46,0xa3,0x5c,0xe4,0x11};
    const uint8_t expected[16] = {
        0xdf,0xa6,0x67,0x47,0xde,0x9a,0xe6,0x30,
        0x30,0xca,0x32,0x61,0x14,0x97,0xc8,0x27};
    std::array<absl::Span<const uint8_t>, 3> data = {
        absl::Span<const uint8_t>(msg, 10),
        absl::Span<const uint8_t>(msg + 10, 17),
        absl::Span<const uint8_t>(msg + 27, 13)};
    uint8_t mac[16];
    Aes128Cmac(kCmacTestKey, data, mac);
    ASSERT_EQ(0, memcmp(mac, expected, 16));
  }
}

// AES-128-CMAC checksum with metadata: successful round-trip.
TEST_F(ClientTest, ChecksumAes128CmacWithMetadata) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  // checksum_size=16 for the 128-bit CMAC output.
  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cmac",
      PubOpts(256, 10).SetChecksum(true).SetChecksumSize(16).SetMetadataSize(24));
  ASSERT_OK(pub);
  ASSERT_EQ(16, pub->ChecksumSize());
  ASSERT_EQ(24, pub->MetadataSize());

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cmac", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  pub->SetChecksumCallback(CmacChecksumCallback);
  sub->SetChecksumCallback(CmacChecksumCallback);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "cmac-test-payload", 17);

  auto meta = pub->GetMetadata();
  ASSERT_EQ(24u, meta.size());
  memcpy(meta.data(), "metadata-for-cmac-test!!", 24);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(17);
  ASSERT_OK(pub_status);

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(17, msg->length);
  ASSERT_EQ(0, memcmp("cmac-test-payload", msg->buffer, 17));

  auto sub_meta = sub->GetMetadata();
  ASSERT_EQ(24u, sub_meta.size());
  ASSERT_EQ(0, memcmp("metadata-for-cmac-test!!", sub_meta.data(), 24));
}

// AES-128-CMAC: corrupt payload after publish → checksum error.
TEST_F(ClientTest, ChecksumAes128CmacCorruptPayload) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cmac_cp",
      PubOpts(256, 10).SetChecksum(true).SetChecksumSize(16).SetMetadataSize(24));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cmac_cp", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  pub->SetChecksumCallback(CmacChecksumCallback);
  sub->SetChecksumCallback(CmacChecksumCallback);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "cmac-corrupt", 12);

  auto meta = pub->GetMetadata();
  memcpy(meta.data(), "metadata_________________xx", 24);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(12);
  ASSERT_OK(pub_status);

  reinterpret_cast<char *>(*buffer)[0] = 'X';

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_FALSE(msg.ok());
  ASSERT_EQ(absl::StatusCode::kInternal, msg.status().code());
}

// AES-128-CMAC: corrupt metadata after publish → checksum error.
TEST_F(ClientTest, ChecksumAes128CmacCorruptMetadata) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cmac_cm",
      PubOpts(256, 10).SetChecksum(true).SetChecksumSize(16).SetMetadataSize(24));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub =
      client->CreateSubscriber("chan_cmac_cm", SubOpts().SetChecksum(true));
  ASSERT_OK(sub);

  pub->SetChecksumCallback(CmacChecksumCallback);
  sub->SetChecksumCallback(CmacChecksumCallback);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "intact-payload", 14);

  auto meta = pub->GetMetadata();
  memcpy(meta.data(), "metadata_________________xx", 24);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(14);
  ASSERT_OK(pub_status);

  meta[0] ^= std::byte{0xFF};

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_FALSE(msg.ok());
  ASSERT_EQ(absl::StatusCode::kInternal, msg.status().code());
}

// AES-128-CMAC: corrupt metadata, pass_checksum_errors → delivered with flag.
TEST_F(ClientTest, ChecksumAes128CmacCorruptMetadataPassError) {
  auto client = EVAL_AND_ASSERT_OK(subspace::Client::Create(Socket()));

  absl::StatusOr<Publisher> pub = client->CreatePublisher(
      "chan_cmac_pe",
      PubOpts(256, 10).SetChecksum(true).SetChecksumSize(16).SetMetadataSize(24));
  ASSERT_OK(pub);

  absl::StatusOr<Subscriber> sub = client->CreateSubscriber(
      "chan_cmac_pe", SubOpts().SetChecksum(true).SetPassChecksumErrors(true));
  ASSERT_OK(sub);

  pub->SetChecksumCallback(CmacChecksumCallback);
  sub->SetChecksumCallback(CmacChecksumCallback);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "intact-payload", 14);

  auto meta = pub->GetMetadata();
  memcpy(meta.data(), "metadata_________________xx", 24);

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(14);
  ASSERT_OK(pub_status);

  meta[23] ^= std::byte{0x01};

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(14, msg->length);
  ASSERT_TRUE(msg->checksum_error);
}

TEST_F(ClientTest, TunnelPublisherSetsCrossMachineFlag) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "tunnel_test1",
      PubOpts(256, 10).SetForTunnel(true));
  ASSERT_OK(pub);
  ASSERT_TRUE(pub->ForTunnel());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber(
      "tunnel_test1", SubOpts().SetForTunnel(true));
  ASSERT_OK(sub);
  ASSERT_TRUE(sub->ForTunnel());

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "tunnel", 6);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(6);
  ASSERT_OK(pub_status);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(6, msg->length);
  ASSERT_EQ(0, memcmp(msg->buffer, "tunnel", 6));

  const subspace::MessagePrefix *prefix = sub->Prefix();
  ASSERT_NE(nullptr, prefix);
  ASSERT_TRUE(prefix->IsCrossMachine());

  // Verify tunnel counts via GetChannelInfo.
  absl::StatusOr<const subspace::ChannelInfo> info =
      pub_client.GetChannelInfo("tunnel_test1");
  ASSERT_OK(info);
  ASSERT_EQ(1, info->num_tunnel_pubs);
  ASSERT_EQ(1, info->num_tunnel_subs);
}

TEST_F(ClientTest, NonTunnelPublisherDoesNotSetCrossMachineFlag) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  absl::StatusOr<Publisher> pub = pub_client.CreatePublisher(
      "tunnel_test2", PubOpts(256, 10));
  ASSERT_OK(pub);
  ASSERT_FALSE(pub->ForTunnel());

  absl::StatusOr<Subscriber> sub = sub_client.CreateSubscriber("tunnel_test2");
  ASSERT_OK(sub);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "local", 5);
  absl::StatusOr<const Message> pub_status = pub->PublishMessage(5);
  ASSERT_OK(pub_status);

  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(5, msg->length);

  const subspace::MessagePrefix *prefix = sub->Prefix();
  ASSERT_NE(nullptr, prefix);
  ASSERT_FALSE(prefix->IsCrossMachine());

  // Verify tunnel counts are zero.
  absl::StatusOr<const subspace::ChannelInfo> info =
      pub_client.GetChannelInfo("tunnel_test2");
  ASSERT_OK(info);
  ASSERT_EQ(0, info->num_tunnel_pubs);
  ASSERT_EQ(0, info->num_tunnel_subs);
}

// ---------------------------------------------------------------------------
// Coverage: Client API paths
// ---------------------------------------------------------------------------

TEST_F(ClientTest, InitTwiceFails) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  absl::Status s = client.Init(Socket());
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("already connected"));
}

TEST_F(ClientTest, CheckConnectedBeforeInit) {
  subspace::Client client;
  auto pub = client.CreatePublisher("no_init_chan", PubOpts(64, 4));
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(), ::testing::HasSubstr("not connected"));
}

TEST_F(ClientTest, ChannelExistsTrue) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = client.CreatePublisher("exists_test", PubOpts(64, 4));
  ASSERT_OK(pub);
  auto exists = client.ChannelExists("exists_test");
  ASSERT_OK(exists);
  ASSERT_TRUE(*exists);
}

TEST_F(ClientTest, ChannelExistsFalse) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto exists = client.ChannelExists("no_such_channel_42");
  ASSERT_OK(exists);
  ASSERT_FALSE(*exists);
}

TEST_F(ClientTest, GetChannelStatsByName) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("stats_test", PubOpts(256, 4)));
  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'x', 100);
  auto msg = pub.PublishMessage(100);
  ASSERT_OK(msg);

  auto stats = client.GetChannelStats("stats_test");
  ASSERT_OK(stats);
  ASSERT_EQ("stats_test", stats->channel_name);
  ASSERT_EQ(100u, stats->total_bytes);
  ASSERT_EQ(1u, stats->total_messages);
  ASSERT_EQ(100u, stats->max_message_size);
}

TEST_F(ClientTest, GetChannelStatsAll) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub1 = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("allstats1", PubOpts(64, 4)));
  auto pub2 = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("allstats2", PubOpts(64, 4)));

  auto buf1 = EVAL_AND_ASSERT_OK(pub1.GetMessageBuffer());
  memset(buf1, 'a', 10);
  ASSERT_OK(pub1.PublishMessage(10));

  auto buf2 = EVAL_AND_ASSERT_OK(pub2.GetMessageBuffer());
  memset(buf2, 'b', 20);
  ASSERT_OK(pub2.PublishMessage(20));

  auto all_stats = client.GetChannelStats();
  ASSERT_OK(all_stats);
  ASSERT_GE(all_stats->size(), 2u);

  bool found1 = false, found2 = false;
  for (auto &s : *all_stats) {
    if (s.channel_name == "allstats1") {
      found1 = true;
      ASSERT_EQ(10u, s.total_bytes);
    }
    if (s.channel_name == "allstats2") {
      found2 = true;
      ASSERT_EQ(20u, s.total_bytes);
    }
  }
  ASSERT_TRUE(found1);
  ASSERT_TRUE(found2);
}

TEST_F(ClientTest, GetChannelCountersByName) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("counters_name", PubOpts(64, 4)));
  auto counters = client.GetChannelCounters("counters_name");
  ASSERT_OK(counters);
  ASSERT_EQ(1, counters->num_pubs);
}

TEST_F(ClientTest, GetChannelCountersByNameNotFound) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto counters = client.GetChannelCounters("nonexistent_channel_xyz");
  ASSERT_FALSE(counters.ok());
  EXPECT_THAT(counters.status().message(), ::testing::HasSubstr("doesn't exist"));
}

TEST_F(ClientTest, GetChannelInfoAll) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("infoall_test", PubOpts(64, 4)));

  auto all_info = client.GetChannelInfo();
  ASSERT_OK(all_info);
  bool found = false;
  for (auto &info : *all_info) {
    if (info.channel_name == "infoall_test") {
      found = true;
      ASSERT_EQ(1, info.num_publishers);
    }
  }
  ASSERT_TRUE(found);
}

TEST_F(ClientTest, GetCurrentOrdinal) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto pub = EVAL_AND_ASSERT_OK(
      pub_client.CreatePublisher("ordinal_test", PubOpts(64, 4)));
  auto sub = EVAL_AND_ASSERT_OK(
      sub_client.CreateSubscriber("ordinal_test"));

  // Before any reads, ordinal should be -1 (no current slot).
  ASSERT_EQ(-1, sub.GetCurrentOrdinal());

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'x', 10);
  ASSERT_OK(pub.PublishMessage(10));

  auto msg = sub.ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(10, msg->length);

  int64_t ord = sub.GetCurrentOrdinal();
  ASSERT_GT(ord, 0);
}

TEST_F(ClientTest, SetDebugDoesNotCrash) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  client.SetDebug(true);
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("debug_test", PubOpts(64, 4)));
  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'y', 5);
  ASSERT_OK(pub.PublishMessage(5));
  client.SetDebug(false);
}

// ---------------------------------------------------------------------------
// Coverage: Publisher edge cases
// ---------------------------------------------------------------------------

TEST_F(ClientTest, UnreliablePublisherFileDescriptorAndPollFd) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("unreliable_fd", PubOpts(64, 4)));
  ASSERT_FALSE(pub.IsReliable());
  auto fd = pub.GetFileDescriptor();
  ASSERT_FALSE(fd.Valid());

  // GetPollFd for unreliable publisher also returns an fd.  Verify it
  // doesn't crash and returns the expected event mask.
  struct pollfd pfd = pub.GetPollFd();
  ASSERT_EQ(POLLIN, pfd.events);
}

TEST_F(ClientTest, UnreliablePublisherGetFileDescriptorInvalid) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("unreliable_fd2", PubOpts(64, 4)));
  ASSERT_FALSE(pub.IsReliable());
  auto fd = pub.GetFileDescriptor();
  ASSERT_FALSE(fd.Valid());
}

TEST_F(ClientTest, PublishZeroSizeFails) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("zero_pub", PubOpts(64, 4)));
  [[maybe_unused]] auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  auto msg = pub.PublishMessage(0);
  ASSERT_FALSE(msg.ok());
  EXPECT_THAT(msg.status().message(), ::testing::HasSubstr("greater than 0"));
}

TEST_F(ClientTest, ReliablePublisherNoSubscribersEmptyBuffer) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(client.CreatePublisher(
      "reliable_nosub",
      subspace::PublisherOptions().SetSlotSize(64).SetNumSlots(4).SetReliable(
          true)));
  auto buf = pub.GetMessageBuffer();
  ASSERT_OK(buf);
  ASSERT_EQ(nullptr, *buf);
}

TEST_F(ClientTest, OnSendCallbackSuccess) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("onsend_test", PubOpts(256, 4)));

  bool callback_called = false;
  pub.SetOnSendCallback(
      [&](void *buffer, int64_t size) -> absl::StatusOr<int64_t> {
        callback_called = true;
        return size;
      });

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'z', 50);
  ASSERT_OK(pub.PublishMessage(50));
  ASSERT_TRUE(callback_called);
  pub.ClearOnSendCallback();
}

TEST_F(ClientTest, OnSendCallbackError) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("onsend_err", PubOpts(256, 4)));

  pub.SetOnSendCallback(
      [](void *, int64_t) -> absl::StatusOr<int64_t> {
        return absl::InternalError("send callback failed");
      });

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'a', 10);
  auto msg = pub.PublishMessage(10);
  ASSERT_FALSE(msg.ok());
  EXPECT_THAT(msg.status().message(), ::testing::HasSubstr("send callback failed"));
  pub.ClearOnSendCallback();
}

TEST_F(ClientTest, WaitForUnreliablePublisherFails) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("unreliable_wait", PubOpts(64, 4)));
  ASSERT_FALSE(pub.IsReliable());
  absl::Status s = pub.Wait();
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("Unreliable publishers can't wait"));
}

TEST_F(ClientTest, WaitForReliablePublisherTimeout) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber(
      "reliable_timeout",
      subspace::SubscriberOptions().SetReliable(true)));
  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "reliable_timeout",
      subspace::PublisherOptions().SetSlotSize(64).SetNumSlots(2).SetReliable(
          true)));

  // Fill all slots to force backpressure.
  auto buf1 = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf1, 'a', 10);
  ASSERT_OK(pub.PublishMessage(10));

  // Wait with a short timeout — should time out since subscriber hasn't read.
  absl::Status s = pub.Wait(std::chrono::milliseconds(10));
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("Timeout"));
}

TEST_F(ClientTest, WaitForSubscriberTimeout) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  // Create pub first so channel exists, then subscriber.
  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "sub_timeout", PubOpts(64, 4)));
  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber("sub_timeout"));

  // Read any initial trigger to drain the subscriber fd.
  auto msg = sub.ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(0, msg->length);

  // Now wait with timeout — no new message, should time out.
  absl::Status s = sub.Wait(std::chrono::milliseconds(10));
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("Timeout"));
}

// ---------------------------------------------------------------------------
// Coverage: Subscriber edge cases
// ---------------------------------------------------------------------------

TEST_F(ClientTest, MaxActiveMessagesTooSmall) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto sub = client.CreateSubscriber(
      "max_active_test",
      subspace::SubscriberOptions().SetMaxActiveMessages(0));
  ASSERT_FALSE(sub.ok());
  EXPECT_THAT(sub.status().message(),
              ::testing::HasSubstr("MaxActiveMessages"));
}

TEST_F(ClientTest, OnReceiveCallbackSuccess) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "onrecv_test", PubOpts(256, 4)));
  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber("onrecv_test"));

  bool callback_called = false;
  sub.SetOnReceiveCallback(
      [&](void *buffer, int64_t size) -> absl::StatusOr<int64_t> {
        callback_called = true;
        return size;
      });

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'q', 50);
  ASSERT_OK(pub.PublishMessage(50));

  auto msg = sub.ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(50, msg->length);
  ASSERT_TRUE(callback_called);
  sub.ClearOnReceiveCallback();
}

TEST_F(ClientTest, OnReceiveCallbackError) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "onrecv_err", PubOpts(256, 4)));
  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber("onrecv_err"));

  sub.SetOnReceiveCallback(
      [](void *, int64_t) -> absl::StatusOr<int64_t> {
        return absl::InternalError("receive callback failed");
      });

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'q', 10);
  ASSERT_OK(pub.PublishMessage(10));

  auto msg = sub.ReadMessage();
  ASSERT_FALSE(msg.ok());
  EXPECT_THAT(msg.status().message(),
              ::testing::HasSubstr("receive callback failed"));
  sub.ClearOnReceiveCallback();
}

TEST_F(ClientTest, ProcessAllMessagesWithoutCallback) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto sub = EVAL_AND_ASSERT_OK(client.CreateSubscriber("process_nocb"));
  absl::Status s = sub.ProcessAllMessages();
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("No message callback"));
}

TEST_F(ClientTest, WaitForSubscriberWithExtraFd) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "twofd_test", PubOpts(64, 4)));
  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber("twofd_test"));

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'w', 10);
  ASSERT_OK(pub.PublishMessage(10));

  int extra_pipe[2];
  ASSERT_EQ(0, pipe(extra_pipe));
  toolbelt::FileDescriptor extra_fd(extra_pipe[0]);

  auto result = sub.Wait(extra_fd, std::chrono::milliseconds(100));
  ASSERT_OK(result);
  ASSERT_EQ(sub.GetPollFd().fd, *result);

  ::close(extra_pipe[1]);
}

TEST_F(ClientTest, WaitForSubscriberExtraFdFires) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "twofd_extra", PubOpts(64, 4)));
  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber("twofd_extra"));

  // Drain any initial trigger.
  auto msg = sub.ReadMessage();
  ASSERT_OK(msg);

  int extra_pipe[2];
  ASSERT_EQ(0, pipe(extra_pipe));
  toolbelt::FileDescriptor extra_fd(extra_pipe[0]);

  // Write to the extra pipe so it triggers (not the subscriber fd).
  ASSERT_EQ(1, ::write(extra_pipe[1], "x", 1));

  auto result = sub.Wait(extra_fd, std::chrono::milliseconds(100));
  ASSERT_OK(result);
  // The result should be the extra fd, not the subscriber fd.
  ASSERT_NE(sub.GetPollFd().fd, *result);

  ::close(extra_pipe[1]);
}

TEST_F(ClientTest, WaitForReliablePublisherWithExtraFd) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber(
      "reliable_twofd",
      subspace::SubscriberOptions().SetReliable(true)));
  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "reliable_twofd",
      subspace::PublisherOptions().SetSlotSize(64).SetNumSlots(4).SetReliable(
          true)));

  int extra_pipe[2];
  ASSERT_EQ(0, pipe(extra_pipe));
  toolbelt::FileDescriptor extra_fd(extra_pipe[0]);

  // Write to extra pipe so it fires.
  ASSERT_EQ(1, ::write(extra_pipe[1], "y", 1));

  auto result = pub.Wait(extra_fd, std::chrono::milliseconds(100));
  ASSERT_OK(result);
  ASSERT_EQ(extra_pipe[0], *result);

  ::close(extra_pipe[1]);
}

// ---------------------------------------------------------------------------
// Coverage: Callback registration errors
// ---------------------------------------------------------------------------

TEST_F(ClientTest, DoubleRegisterDroppedMessageCallback) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto sub = EVAL_AND_ASSERT_OK(client.CreateSubscriber("double_dropped"));

  ASSERT_OK(sub.RegisterDroppedMessageCallback(
      [](Subscriber *, int64_t) {}));
  absl::Status s = sub.RegisterDroppedMessageCallback(
      [](Subscriber *, int64_t) {});
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("already been registered"));
  ASSERT_OK(sub.UnregisterDroppedMessageCallback());
}

TEST_F(ClientTest, UnregisterDroppedMessageCallbackNotRegistered) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto sub = EVAL_AND_ASSERT_OK(client.CreateSubscriber("unreg_dropped"));
  absl::Status s = sub.UnregisterDroppedMessageCallback();
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(),
              ::testing::HasSubstr("No dropped message callback"));
}

TEST_F(ClientTest, DoubleRegisterMessageCallback) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto sub = EVAL_AND_ASSERT_OK(client.CreateSubscriber("double_msg_cb"));

  ASSERT_OK(sub.RegisterMessageCallback(
      [](Subscriber *, Message) {}));
  absl::Status s = sub.RegisterMessageCallback(
      [](Subscriber *, Message) {});
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("already been registered"));
  ASSERT_OK(sub.UnregisterMessageCallback());
}

TEST_F(ClientTest, UnregisterMessageCallbackNotRegistered) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto sub = EVAL_AND_ASSERT_OK(client.CreateSubscriber("unreg_msg_cb"));
  absl::Status s = sub.UnregisterMessageCallback();
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("No message callback"));
}

TEST_F(ClientTest, DoubleRegisterResizeCallback) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("double_resize", PubOpts(64, 4)));

  ASSERT_OK(pub.RegisterResizeCallback(
      [](Publisher *, int, int) -> absl::Status { return absl::OkStatus(); }));
  absl::Status s = pub.RegisterResizeCallback(
      [](Publisher *, int, int) -> absl::Status { return absl::OkStatus(); });
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("already been registered"));
  ASSERT_OK(pub.UnregisterResizeCallback());
}

TEST_F(ClientTest, UnregisterResizeCallbackNotRegistered) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("unreg_resize", PubOpts(64, 4)));
  absl::Status s = pub.UnregisterResizeCallback();
  ASSERT_FALSE(s.ok());
  EXPECT_THAT(s.message(), ::testing::HasSubstr("No resize callback"));
}

// ---------------------------------------------------------------------------
// Coverage: Message edge cases
// ---------------------------------------------------------------------------

TEST_F(ClientTest, DefaultMessageGetters) {
  subspace::Message msg;
  ASSERT_EQ(0, msg.length);
  ASSERT_EQ(nullptr, msg.buffer);
  ASSERT_EQ("", msg.ChannelType());
  ASSERT_EQ(0u, msg.NumSlots());
  ASSERT_EQ(0u, msg.SlotSize());
}

TEST_F(ClientTest, MessageCopyAndMove) {
  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  auto pub = EVAL_AND_ASSERT_OK(pub_client.CreatePublisher(
      "msg_copy", PubOpts(64, 4)));
  auto sub = EVAL_AND_ASSERT_OK(sub_client.CreateSubscriber("msg_copy"));

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memset(buf, 'c', 20);
  ASSERT_OK(pub.PublishMessage(20));

  auto msg1 = EVAL_AND_ASSERT_OK(sub.ReadMessage());
  ASSERT_EQ(20, msg1.length);

  // Copy constructor.
  subspace::Message msg2 = msg1;
  ASSERT_EQ(20, msg2.length);
  ASSERT_EQ(msg1.ordinal, msg2.ordinal);

  // Move constructor.
  subspace::Message msg3 = std::move(msg2);
  ASSERT_EQ(20, msg3.length);

  // Copy assignment.
  subspace::Message msg4;
  msg4 = msg1;
  ASSERT_EQ(20, msg4.length);

  // Move assignment.
  subspace::Message msg5;
  msg5 = std::move(msg3);
  ASSERT_EQ(20, msg5.length);

  // Reset.
  msg5.Reset();
  ASSERT_EQ(0, msg5.length);
}

// ---------------------------------------------------------------------------
// Coverage: Options builder chains
// ---------------------------------------------------------------------------

TEST_F(ClientTest, PublisherOptionsChain) {
  subspace::PublisherOptions opts;
  opts.SetSlotSize(128)
      .SetNumSlots(8)
      .SetReliable(true)
      .SetLocal(true)
      .SetFixedSize(true)
      .SetType("my_type")
      .SetForTunnel(true)
      .SetMux("/mymux")
      .SetVchanId(7)
      .SetActivate(true)
      .SetNotifyRetirement(true)
      .SetChecksum(true)
      .SetChecksumSize(8)
      .SetMetadataSize(16)
      .SetMaxPublishers(3);

  ASSERT_EQ(128, opts.SlotSize());
  ASSERT_EQ(8, opts.NumSlots());
  ASSERT_TRUE(opts.IsReliable());
  ASSERT_TRUE(opts.IsLocal());
  ASSERT_TRUE(opts.IsFixedSize());
  ASSERT_EQ("my_type", opts.Type());
  ASSERT_TRUE(opts.ForTunnel());
  ASSERT_EQ("/mymux", opts.Mux());
  ASSERT_EQ(7, opts.VchanId());
  ASSERT_TRUE(opts.Activate());
  ASSERT_TRUE(opts.NotifyRetirement());
  ASSERT_TRUE(opts.Checksum());
  ASSERT_EQ(8, opts.ChecksumSize());
  ASSERT_EQ(16, opts.MetadataSize());
  ASSERT_EQ(3, opts.MaxPublishers());
}

TEST_F(ClientTest, SubscriberOptionsChain) {
  subspace::SubscriberOptions opts;
  opts.SetReliable(true)
      .SetType("sub_type")
      .SetMaxActiveMessages(20)
      .SetBridge(true)
      .SetForTunnel(true)
      .SetMux("/submux")
      .SetVchanId(3)
      .SetPassActivation(true)
      .SetReadWrite(true)
      .SetChecksum(true)
      .SetPassChecksumErrors(true)
      .SetKeepActiveMessage(true);
  opts.SetLogDroppedMessages(true);

  ASSERT_TRUE(opts.IsReliable());
  ASSERT_EQ("sub_type", opts.Type());
  ASSERT_EQ(19, opts.MaxSharedPtrs());
  ASSERT_EQ(20, opts.MaxActiveMessages());
  ASSERT_TRUE(opts.LogDroppedMessages());
  ASSERT_TRUE(opts.IsBridge());
  ASSERT_TRUE(opts.ForTunnel());
  ASSERT_EQ("/submux", opts.Mux());
  ASSERT_EQ(3, opts.VchanId());
  ASSERT_TRUE(opts.PassActivation());
  ASSERT_TRUE(opts.ReadWrite());
  ASSERT_TRUE(opts.Checksum());
  ASSERT_TRUE(opts.PassChecksumErrors());
  ASSERT_TRUE(opts.KeepActiveMessage());

  // SetMaxSharedPtrs sets max_active_messages = shared_ptrs + 1.
  subspace::SubscriberOptions opts2;
  opts2.SetMaxSharedPtrs(10);
  ASSERT_EQ(10, opts2.MaxSharedPtrs());
  ASSERT_EQ(11, opts2.MaxActiveMessages());
}

// ---------------------------------------------------------------------------
// Coverage: ResizeChannel with FixedSize publisher
// ---------------------------------------------------------------------------

TEST_F(ClientTest, ResizeFixedSizePublisherFails) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(client.CreatePublisher(
      "fixed_resize",
      subspace::PublisherOptions().SetSlotSize(128).SetNumSlots(4).SetFixedSize(
          true)));

  [[maybe_unused]] auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  auto bigger = pub.GetMessageBuffer(256);
  ASSERT_FALSE(bigger.ok());
  EXPECT_THAT(bigger.status().message(), ::testing::HasSubstr("fixed size"));
}

// ---------------------------------------------------------------------------
// Coverage: Resize callback returning error
// ---------------------------------------------------------------------------

TEST_F(ClientTest, ResizeCallbackReturnsError) {
  subspace::Client client;
  ASSERT_OK(client.Init(Socket()));
  auto pub = EVAL_AND_ASSERT_OK(
      client.CreatePublisher("resize_err", PubOpts(64, 4)));

  ASSERT_OK(pub.RegisterResizeCallback(
      [](Publisher *, int, int) -> absl::Status {
        return absl::InternalError("resize denied");
      }));

  [[maybe_unused]] auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  auto bigger = pub.GetMessageBuffer(256);
  ASSERT_FALSE(bigger.ok());
  EXPECT_THAT(bigger.status().message(), ::testing::HasSubstr("resize denied"));
  ASSERT_OK(pub.UnregisterResizeCallback());
}

// ---------------------------------------------------------------------------
// Free function CreatePublisher / CreateSubscriber convenience helpers.
// ---------------------------------------------------------------------------

TEST_F(ClientTest, FreeCreatePublisher) {
  auto pub_or = subspace::CreatePublisher(
      "free_pub", PubOpts(256, 10), Socket());
  ASSERT_OK(pub_or);
  auto pub = std::move(*pub_or);
  ASSERT_EQ(256, pub.SlotSize());
  ASSERT_EQ(10, pub.NumSlots());
}

TEST_F(ClientTest, FreeCreateSubscriber) {
  // Need a publisher first so the channel exists with concrete slots.
  subspace::Client client;
  InitClient(client);
  auto pub =
      EVAL_AND_ASSERT_OK(client.CreatePublisher("free_sub", 256, 10));

  auto sub_or = subspace::CreateSubscriber("free_sub", {}, Socket());
  ASSERT_OK(sub_or);
  auto sub = std::move(*sub_or);

  // Verify the subscriber works by publishing and reading a message.
  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer(256));
  memcpy(buf, "test", 4);
  ASSERT_OK(pub.PublishMessage(4));

  auto msg = sub.ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(4, msg->length);
  ASSERT_EQ(0, memcmp(msg->buffer, "test", 4));
}

TEST_F(ClientTest, FreeCreatePublisherAndSubscriberRoundTrip) {
  auto pub_or = subspace::CreatePublisher(
      "free_rt", PubOpts(256, 10), Socket());
  ASSERT_OK(pub_or);
  auto pub = std::move(*pub_or);

  auto sub_or = subspace::CreateSubscriber("free_rt", {}, Socket());
  ASSERT_OK(sub_or);
  auto sub = std::move(*sub_or);

  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer(256));
  memcpy(buf, "hello", 5);
  auto pub_msg = pub.PublishMessage(5);
  ASSERT_OK(pub_msg);

  auto read_msg = sub.ReadMessage();
  ASSERT_OK(read_msg);
  ASSERT_EQ(5, read_msg->length);
  ASSERT_EQ(0, memcmp(read_msg->buffer, "hello", 5));
}

TEST_F(ClientTest, FreeCreatePublisherBadSocket) {
  auto pub_or = subspace::CreatePublisher(
      "bad_pub", PubOpts(256, 10),
      "/tmp/no_such_subspace_socket");
  ASSERT_FALSE(pub_or.ok());
}

TEST_F(ClientTest, FreeCreateSubscriberBadSocket) {
  auto sub_or = subspace::CreateSubscriber(
      "bad_sub", {}, "/tmp/no_such_subspace_socket");
  ASSERT_FALSE(sub_or.ok());
}

// Separate fixture that loads the NOP plugin before the server starts,
// so that OnReady is called during Run() on the scheduler thread.
class PluginTest : public ::testing::Test {
public:
  static void SetUpTestSuite() {
    printf("Starting Subspace server with NOP plugin\n");
#if defined(__ANDROID__)
    char socket_name_template[] = "/data/local/tmp/subspaceXXXXXX"; // NOLINT
#else
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
#endif
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    (void)pipe(server_pipe_);

    server_ = std::make_unique<subspace::Server>(
        engine_, socket_, "", 0, 0,
        /*local=*/true, server_pipe_[1], /*initial_ordinal=*/1,
        /*wait_for_clients=*/true);

#ifdef __APPLE__
    auto status = server_->LoadBuiltinPlugin(
        "NOP", std::unique_ptr<subspace::PluginInterface>(NOP_Create()));
#else
    auto status = server_->LoadPlugin("NOP", "plugins/nop_plugin.so");
#endif
    if (!status.ok()) {
      fprintf(stderr, "Failed to load NOP plugin: %s\n",
              status.ToString().c_str());
      exit(1);
    }

    server_thread_ = std::thread([]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  static void TearDownTestSuite() {
    printf("Stopping Subspace server with NOP plugin\n");
    server_->Stop();

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
    server_->CleanupAfterSession();
    (void)remove(socket_.c_str());
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }

  static const std::string &Socket() { return socket_; }
  static subspace::Server *Server() { return server_.get(); }

private:
  inline static subspace::async::RuntimeEngine engine_;
  inline static std::string socket_;
  inline static int server_pipe_[2];
  inline static std::unique_ptr<subspace::Server> server_;
  inline static std::thread server_thread_;
};

TEST_F(PluginTest, HeartbeatPublishes) {
  subspace::Client sub_client;
  ASSERT_OK(sub_client.Init(Socket()));
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber("/nop/Heartbeat");
  ASSERT_OK(sub);

  constexpr int kExpectedMessages = 2;
  int received = 0;
  uint64_t prev_seq = 0;
  while (received < kExpectedMessages) {
    absl::Status wait_status = sub->Wait(std::chrono::seconds(5));
    ASSERT_OK(wait_status) << "Timed out waiting for heartbeat message "
                           << received + 1;
    for (;;) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      ASSERT_OK(msg);
      if (msg->length == 0) {
        break;
      }
      ASSERT_EQ(sizeof(uint64_t), msg->length);
      uint64_t seq;
      memcpy(&seq, msg->buffer, sizeof(seq));
      if (received > 0) {
        EXPECT_GT(seq, prev_seq);
      }
      prev_seq = seq;
      received++;
    }
  }
  ASSERT_GE(received, kExpectedMessages);
}

class SplitBufferPluginTest : public ::testing::Test {
public:
  static void SetUpTestSuite() {
    printf("Starting Subspace server with split-buffer test plugin\n");
#if defined(__ANDROID__)
    char socket_name_template[] = "/data/local/tmp/subspaceXXXXXX"; // NOLINT
#else
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
#endif
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    (void)pipe(server_pipe_);

    server_ = std::make_unique<subspace::Server>(
        engine_, socket_, "", 0, 0,
        /*local=*/true, server_pipe_[1], /*initial_ordinal=*/1,
        /*wait_for_clients=*/true);

#ifndef __APPLE__
    auto status = server_->LoadPlugin("SPLIT_BUFFER_FREE_TEST",
                                      "plugins/split_buffer_free_test_plugin.so");
    if (!status.ok()) {
      fprintf(stderr, "Failed to load split-buffer test plugin: %s\n",
              status.ToString().c_str());
      exit(1);
    }
#endif

    server_thread_ = std::thread([]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  static void TearDownTestSuite() {
    printf("Stopping Subspace server with split-buffer test plugin\n");
    server_->Stop();

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
    server_->CleanupAfterSession();
    (void)remove(socket_.c_str());
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }

  static const std::string &Socket() { return socket_; }
  static subspace::Server *Server() { return server_.get(); }

private:
  inline static subspace::async::RuntimeEngine engine_;
  inline static std::string socket_;
  inline static int server_pipe_[2];
  inline static std::unique_ptr<subspace::Server> server_;
  inline static std::thread server_thread_;
};

TEST_F(SplitBufferPluginTest, ServerCleanupUsesPluginEndToEnd) {
#ifdef __APPLE__
  GTEST_SKIP() << "split-buffer cleanup test plugin is dlopen-only";
#else
  std::string log_path = Socket() + ".split_buffer_free.log";
  (void)remove(log_path.c_str());
  ASSERT_EQ(0, setenv("SUBSPACE_SPLIT_BUFFER_FREE_TEST_LOG", log_path.c_str(),
                      /*overwrite=*/1));

  auto state = std::make_shared<TestSplitBufferState>();
  const std::string channel = "split_buffers_plugin_cleanup";

  subspace::Client pub_client;
  subspace::Client sub_client;
  ASSERT_OK(pub_client.Init(Socket()));
  ASSERT_OK(sub_client.Init(Socket()));

  subspace::PublisherOptions pub_options;
  pub_options.SetSlotSize(96)
      .SetNumSlots(3)
      .SetUseSplitBuffers(true)
      .SetSplitBufferCallbacks(MakeTestSplitBufferCallbacks(state));
  absl::StatusOr<Publisher> pub =
      pub_client.CreatePublisher(channel, pub_options);
  ASSERT_OK(pub);
  EXPECT_TRUE(pub->UsesSplitBuffers());
  EXPECT_EQ(3, state->allocate_count);

  // This round trip on the publisher client ensures the preceding one-way
  // client-buffer registration messages have reached the server.
  ASSERT_OK(pub_client.GetChannelInfo(channel));

  subspace::SubscriberOptions sub_options;
  sub_options.SetSplitBufferCallbacks(MakeTestSplitBufferCallbacks(state));
  absl::StatusOr<Subscriber> sub =
      sub_client.CreateSubscriber(channel, sub_options);
  ASSERT_OK(sub);
  EXPECT_TRUE(sub->UsesSplitBuffers());
  EXPECT_EQ(3, state->map_count);

  absl::StatusOr<void *> buffer = pub->GetMessageBuffer(64);
  ASSERT_OK(buffer);
  std::memcpy(*buffer, "plugin-split", 12);

  uintptr_t publisher_handle = 0;
  ASSERT_TRUE(
      pub->GetSplitBufferHandleFromAddress(*buffer, &publisher_handle));
  EXPECT_NE(state->allocations.end(), state->allocations.find(publisher_handle));

  absl::StatusOr<const Message> pub_status = pub->PublishMessage(12);
  ASSERT_OK(pub_status);
  absl::StatusOr<Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(12, msg->length);
  EXPECT_EQ(0, std::memcmp(msg->buffer, "plugin-split", 12));

  subspace::ServerChannel *server_channel = Server()->FindChannel(channel);
  ASSERT_NE(nullptr, server_channel);
  server_channel->RemoveBuffer(Server()->GetSessionId(), Server());

  std::ifstream log(log_path);
  ASSERT_TRUE(log.is_open());
  std::string line;
  int freed_slots = 0;
  while (std::getline(log, line)) {
    if (line.find(channel) != std::string::npos &&
        line.find("split_callback") != std::string::npos) {
      freed_slots++;
    }
  }
  EXPECT_EQ(3, freed_slots);
#endif
}

// ---------------------------------------------------------------------------
// Cross-process split-buffer test.
//
// Split buffers keep message prefixes in the channel's regular shared memory
// while placing each payload slot in a separately-mapped payload buffer.  All
// existing split-buffer tests run the publisher and subscriber in the *same*
// process, so they never exercise a subscriber mapping payload buffers that
// were created by a publisher living in a different process.  This test does
// exactly that: the publisher runs in the parent process and the subscriber
// runs in a forked child process, both talking to a server hosted by the
// parent.  The child verifies the full payload of every message so any
// cross-process mapping/aliasing bug in the split-buffer path shows up as a
// payload mismatch (or crash) rather than silently passing.
//
// A control variant runs the same flow with split buffers disabled so we can
// tell whether a failure is specific to split buffers or a more general
// cross-process problem.
namespace {

constexpr int kXProcNumMessages = 8;
constexpr int32_t kXProcSlotSize = 4096;
constexpr int32_t kXProcNumSlots = 32;
constexpr int32_t kXProcPayloadSize = 4000;

// Deterministic payload byte for message `msg_index` at offset `offset`.
inline uint8_t XProcPayloadByte(int msg_index, int offset) {
  return static_cast<uint8_t>((msg_index * 131 + offset * 7 + 17) & 0xff);
}

struct ScopeGuard {
  std::function<void()> fn;
  ~ScopeGuard() {
    if (fn) {
      fn();
    }
  }
};

// Body of the subscriber child process.  Uses plain checks + stderr logging
// (not gtest macros) and returns a process exit code: 0 on success.
int RunXProcSubscriberChild(const std::string &socket,
                            const std::string &channel, int ready_fd,
                            int ack_fd) {
  // Wait until the parent has created the server and the channel.
  char token = 0;
  if (::read(ready_fd, &token, 1) != 1) {
    fprintf(stderr, "[sub] failed to read ready token\n");
    return 10;
  }

  subspace::Client client;
  if (absl::Status s = client.Init(socket); !s.ok()) {
    fprintf(stderr, "[sub] client Init failed: %s\n", s.ToString().c_str());
    return 11;
  }

  absl::StatusOr<Subscriber> sub =
      client.CreateSubscriber(channel, subspace::SubscriberOptions());
  if (!sub.ok()) {
    fprintf(stderr, "[sub] CreateSubscriber failed: %s\n",
            sub.status().ToString().c_str());
    return 12;
  }

  // Tell the parent the subscriber is attached so it can start publishing.
  token = 'S';
  if (::write(ack_fd, &token, 1) != 1) {
    fprintf(stderr, "[sub] failed to write ack token\n");
    return 14;
  }

  std::vector<bool> seen(kXProcNumMessages, false);
  int received = 0;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (received < kXProcNumMessages) {
    // Drain everything currently available.
    for (;;) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      if (!msg.ok()) {
        fprintf(stderr, "[sub] ReadMessage failed: %s\n",
                msg.status().ToString().c_str());
        return 15;
      }
      if (msg->length == 0) {
        break; // Nothing more right now.
      }
      if (msg->length != kXProcPayloadSize) {
        fprintf(stderr, "[sub] bad message length %lld (want %d)\n",
                static_cast<long long>(msg->length), kXProcPayloadSize);
        return 16;
      }
      const uint8_t *data = static_cast<const uint8_t *>(msg->buffer);
      int idx = static_cast<int>(data[0]) | (static_cast<int>(data[1]) << 8) |
                (static_cast<int>(data[2]) << 16) |
                (static_cast<int>(data[3]) << 24);
      if (idx < 0 || idx >= kXProcNumMessages) {
        fprintf(stderr, "[sub] bad message index %d\n", idx);
        return 17;
      }
      for (int j = 4; j < kXProcPayloadSize; j++) {
        uint8_t want = XProcPayloadByte(idx, j);
        if (data[j] != want) {
          fprintf(stderr,
                  "[sub] payload mismatch msg %d offset %d: got %u want %u\n",
                  idx, j, data[j], want);
          return 18;
        }
      }
      if (!seen[idx]) {
        seen[idx] = true;
        received++;
      }
    }
    if (received >= kXProcNumMessages) {
      break;
    }
    auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      fprintf(stderr, "[sub] timed out after receiving %d/%d messages\n",
              received, kXProcNumMessages);
      return 19;
    }
    (void)sub->Wait(std::chrono::duration_cast<std::chrono::nanoseconds>(
        deadline - now));
  }
  return 0;
}

void RunCrossProcessSplitBufferTest(bool use_split_buffers) {
  signal(SIGPIPE, SIG_IGN);

  char socket_template[] = "/tmp/subspace_xprocXXXXXX"; // NOLINT
  int tmpfd = mkstemp(socket_template);
  ASSERT_GE(tmpfd, 0);
  ::close(tmpfd);
  ::remove(socket_template); // We want the unique name, not the file.
  std::string socket = socket_template;
  std::string channel = use_split_buffers ? "xproc_split" : "xproc_single";

  int ready_pipe[2]; // parent -> child
  int ack_pipe[2];   // child -> parent
  ASSERT_EQ(0, pipe(ready_pipe));
  ASSERT_EQ(0, pipe(ack_pipe));

  // Fork the subscriber while the process is still single-threaded (before the
  // server thread starts), so the child gets a clean address space with no
  // locks held by background threads.
  pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::close(ready_pipe[1]);
    ::close(ack_pipe[0]);
    int code =
        RunXProcSubscriberChild(socket, channel, ready_pipe[0], ack_pipe[1]);
    ::close(ready_pipe[0]);
    ::close(ack_pipe[1]);
    _exit(code);
  }

  // Parent (server host + publisher).
  ::close(ready_pipe[0]);
  ::close(ack_pipe[1]);

  subspace::async::RuntimeEngine engine;
  std::unique_ptr<subspace::Server> server;
  std::thread server_thread;
  int server_pipe[2] = {-1, -1};
  bool child_reaped = false;

  // Guard destructs *after* the client/publisher declared below it (reverse
  // declaration order), so on any early ASSERT return the publisher is torn
  // down against a live server before the server itself is stopped.
  ScopeGuard guard{[&]() {
    if (!child_reaped) {
      ::kill(child, SIGKILL);
      int st = 0;
      (void)::waitpid(child, &st, 0);
    }
    if (server) {
      server->Stop();
      if (server_pipe[0] >= 0) {
        char b[8];
        (void)::read(server_pipe[0], b, 8);
      }
      if (server_thread.joinable()) {
        server_thread.join();
      }
      server->CleanupAfterSession();
    }
    if (server_pipe[0] >= 0) {
      ::close(server_pipe[0]);
    }
    if (server_pipe[1] >= 0) {
      ::close(server_pipe[1]);
    }
    ::close(ready_pipe[1]);
    ::close(ack_pipe[0]);
    ::remove(socket.c_str());
  }};

  ASSERT_EQ(0, pipe(server_pipe));
  server = std::make_unique<subspace::Server>(
      engine, socket, "", 0, 0, /*local=*/true, server_pipe[1],
      /*initial_ordinal=*/1, /*wait_for_clients=*/true);
  server_thread = std::thread([&]() {
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
    absl::Status s = server->Run(/*num_asio_threads=*/4);
#else
    absl::Status s = server->Run();
#endif
    if (!s.ok()) {
      fprintf(stderr, "Error running Subspace server: %s\n",
              s.ToString().c_str());
    }
  });
  char buf[8];
  (void)::read(server_pipe[0], buf, 8); // Wait until the server is ready.

  // Declared after the guard so they are destroyed before the server stops.
  subspace::Client client;
  ASSERT_OK(client.Init(socket));

  subspace::PublisherOptions pub_options;
  pub_options.SetSlotSize(kXProcSlotSize)
      .SetNumSlots(kXProcNumSlots)
      .SetUseSplitBuffers(use_split_buffers);
  absl::StatusOr<Publisher> pub = client.CreatePublisher(channel, pub_options);
  ASSERT_OK(pub);
  ASSERT_EQ(use_split_buffers, pub->UsesSplitBuffers());

  // Tell the child the server + channel exist.
  char token = 'R';
  ASSERT_EQ(1, ::write(ready_pipe[1], &token, 1));

  // Wait for the subscriber to attach before publishing so no messages are
  // dropped (the channel is unreliable, but the slot pool is deep enough to
  // hold every message the subscriber will read).
  token = 0;
  ASSERT_EQ(1, ::read(ack_pipe[0], &token, 1));
  ASSERT_EQ('S', token);

  for (int i = 0; i < kXProcNumMessages; i++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer(kXProcPayloadSize);
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    uint8_t *data = static_cast<uint8_t *>(*buffer);
    data[0] = static_cast<uint8_t>(i & 0xff);
    data[1] = static_cast<uint8_t>((i >> 8) & 0xff);
    data[2] = static_cast<uint8_t>((i >> 16) & 0xff);
    data[3] = static_cast<uint8_t>((i >> 24) & 0xff);
    for (int j = 4; j < kXProcPayloadSize; j++) {
      data[j] = XProcPayloadByte(i, j);
    }
    ASSERT_OK(pub->PublishMessage(kXProcPayloadSize));
  }

  // Wait for the subscriber process to finish (while the server is still up)
  // and report its result.
  int status = 0;
  ASSERT_EQ(child, waitpid(child, &status, 0));
  child_reaped = true;
  ASSERT_TRUE(WIFEXITED(status))
      << "subscriber process did not exit normally (status " << status << ")";
  EXPECT_EQ(0, WEXITSTATUS(status))
      << "subscriber process reported failure code " << WEXITSTATUS(status);
}

} // namespace

TEST(CrossProcessSplitBufferTest, PublisherAndSubscriberInSeparateProcesses) {
  RunCrossProcessSplitBufferTest(/*use_split_buffers=*/true);
}

TEST(CrossProcessSplitBufferTest, ControlSingleBufferInSeparateProcesses) {
  RunCrossProcessSplitBufferTest(/*use_split_buffers=*/false);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  subspace::SetDefaultUseSplitBuffers(absl::GetFlag(FLAGS_use_split_buffers));

  // Let sanitizers install their own crash handlers so they can report the
  // original memory error instead of routing through Abseil's stack unwinder.
#ifndef ADDRESS_SANITIZER
  signal(SIGSEGV, SignalHandler);
  signal(SIGBUS, SignalHandler);
#endif
  signal(SIGQUIT, SigQuitHandler);

  absl::InitializeSymbolizer(argv[0]);

#ifndef ADDRESS_SANITIZER
  absl::InstallFailureSignalHandler({
      .use_alternate_stack = false,
  });
#endif

  return RUN_ALL_TESTS();
}
