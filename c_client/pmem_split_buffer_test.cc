// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.
//
// Cross-process pub/sub over the C client, using Subspace *split buffers* whose
// payload storage is supplied by the application through the split-buffer
// callbacks.  The callbacks back each payload slot with real, cross-process
// shared memory:
//
//   * QNX   : pmem (PMEM_FLAGS_SHMEM) allocation + MAP_PHYS remap in the peer.
//   * POSIX : shm_open + ftruncate + mmap (so this test also runs on
//             Linux/macOS and in CI).
//
// Unlike the ccu-stack pmem_smoke example -- which allocates its own memory and
// hands the fd to the peer out-of-band via SCM_RIGHTS, using Subspace only as a
// control channel -- here the payload flows through Subspace's normal
// get_message_buffer / publish / read_message path.  The memory backing those
// message buffers is the pmem/shm region returned by our allocate/map
// callbacks.
//
// The crucial constraint that shapes the allocator: Subspace invokes the
// subscriber's `map` callback in the *subscriber process*, passing only the
// metadata in SubspaceSplitBufferInfo (channel_name, session_id, buffer_index,
// slot_id, ...).  There is no fd handed to the subscriber.  session_id,
// buffer_index and slot_id are identical on the publisher (allocate) and
// subscriber (map) side for a given slot, so the allocator derives a
// deterministic shared-memory *name* from them and both sides attach to the
// same region by that name.
//
// The publisher runs in the parent process (which also hosts the server); the
// subscriber runs in a forked child.  The child verifies every payload byte, so
// any bug in cross-process mapping surfaces as a mismatch or crash rather than a
// silent pass.

#include "absl/flags/parse.h"
#include "c_client/subspace.h"
#include "co/coroutine.h"
#include "server/server.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <signal.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#if defined(__QNXNTO__)
#include <pmem.h>
#include <pmem_id.h>
// The pmem physical memory pool to draw from.  Adjust for your platform if the
// camera pool is not the right choice for a smoke test.
#ifndef SUBSPACE_PMEM_TEST_ID
#define SUBSPACE_PMEM_TEST_ID PMEM_CAMERA_ID
#endif
#endif

namespace {

constexpr int kNumMessages = 8;
constexpr int32_t kSlotSize = 4096;
constexpr int kNumSlots = 16;
constexpr int32_t kPayloadSize = 4000;

inline uint8_t PayloadByte(int msg_index, int offset) {
  return static_cast<uint8_t>((msg_index * 131 + offset * 7 + 17) & 0xff);
}

// Deterministic shared-memory object name for a given payload slot.  Both the
// publisher (allocate) and the subscriber (map) compute the same name from the
// metadata Subspace supplies, which is how the subscriber finds the publisher's
// region without any fd being passed between processes.
std::string DeriveRegionName(const SubspaceSplitBufferInfo *info) {
  char channel[128];
  const char *src = info->channel_name != nullptr ? info->channel_name : "";
  size_t j = 0;
  for (size_t i = 0; src[i] != '\0' && j + 1 < sizeof(channel); i++) {
    char c = src[i];
    channel[j++] =
        (std::isalnum(static_cast<unsigned char>(c)) != 0) ? c : '_';
  }
  channel[j] = '\0';

  char name[256];
  snprintf(name, sizeof(name), "/subspace_pmem_%llu_%s_%u_%u",
           static_cast<unsigned long long>(info->session_id), channel,
           info->buffer_index, info->slot_id);
  return std::string(name);
}

// A single mapped shared-memory region.
struct ShmRegion {
  int fd = -1;
  void *addr = nullptr;
  size_t size = 0;
#if defined(__QNXNTO__)
  void *pmem_cookie = nullptr; // non-null on the owning (publisher) side.
#endif
};

#if defined(__QNXNTO__)

// The peer maps the same physical pages via MAP_PHYS, so the publisher records
// the physical base address and size in a small companion file keyed by the
// deterministic region name.
std::string CoordPath(const std::string &name) { return "/tmp" + name + ".coord"; }

struct PmemCoord {
  int64_t phys = 0;
  uint64_t size = 0;
};

bool WriteCoord(const std::string &name, const PmemCoord &coord) {
  std::string path = CoordPath(name);
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0600);
  if (fd < 0) {
    return false;
  }
  bool ok = ::write(fd, &coord, sizeof(coord)) == static_cast<ssize_t>(sizeof(coord));
  ::close(fd);
  return ok;
}

bool ReadCoord(const std::string &name, PmemCoord *coord) {
  std::string path = CoordPath(name);
  for (int attempt = 0; attempt < 200; attempt++) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd >= 0) {
      bool ok = ::read(fd, coord, sizeof(*coord)) ==
                static_cast<ssize_t>(sizeof(*coord));
      ::close(fd);
      if (ok) {
        return true;
      }
    }
    usleep(10000);
  }
  return false;
}

bool ShmRegionCreate(const std::string &name, size_t size, ShmRegion *out) {
  if (pmem_init() != 0) {
    fprintf(stderr, "[pmem] pmem_init failed\n");
    return false;
  }
  // PHYS_CONTIG so a single physical base address describes the whole region,
  // which lets the peer remap it with a single MAP_PHYS mmap.
  const uint32_t flags =
      PMEM_FLAGS_SHMEM | PMEM_FLAGS_PHYS_CONTIG | PMEM_FLAGS_CACHE_NONE;
  pmem_handle_t handle = nullptr;
  void *addr = pmem_malloc_ext_v2(size, SUBSPACE_PMEM_TEST_ID, flags,
                                  PMEM_ALIGNMENT_4K, /*vmid_mask=*/0, &handle,
                                  nullptr);
  if (addr == nullptr || handle == nullptr) {
    fprintf(stderr, "[pmem] pmem_malloc_ext_v2 failed (size=%zu)\n", size);
    return false;
  }
  int fd = -1;
  off64_t offset = 0;
  if (pmem_get_fd(addr, SUBSPACE_PMEM_TEST_ID, &fd, &offset) != 0) {
    fprintf(stderr, "[pmem] pmem_get_fd failed\n");
    (void)pmem_free(addr);
    return false;
  }
  PmemCoord coord{static_cast<int64_t>(offset), size};
  if (!WriteCoord(name, coord)) {
    fprintf(stderr, "[pmem] failed to publish coordinates for %s\n",
            name.c_str());
    if (fd >= 0) {
      ::close(fd);
    }
    (void)pmem_free(addr);
    return false;
  }
  out->fd = fd;
  out->addr = addr;
  out->size = size;
  out->pmem_cookie = addr;
  return true;
}

bool ShmRegionOpen(const std::string &name, size_t /*size*/, ShmRegion *out) {
  PmemCoord coord;
  if (!ReadCoord(name, &coord)) {
    fprintf(stderr, "[pmem] failed to read coordinates for %s\n", name.c_str());
    return false;
  }
  void *addr =
      mmap64(nullptr, static_cast<size_t>(coord.size), PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_PHYS, NOFD, static_cast<off64_t>(coord.phys));
  if (addr == MAP_FAILED) {
    fprintf(stderr, "[pmem] MAP_PHYS mmap failed errno=%d\n", errno);
    return false;
  }
  out->fd = -1;
  out->addr = addr;
  out->size = static_cast<size_t>(coord.size);
  out->pmem_cookie = nullptr;
  return true;
}

void ShmRegionClose(ShmRegion *r) {
  if (r->pmem_cookie != nullptr) {
    (void)pmem_free(r->pmem_cookie); // owner: releases the pmem allocation.
  } else if (r->addr != nullptr && r->addr != MAP_FAILED) {
    ::munmap(r->addr, r->size); // peer: drop the MAP_PHYS mapping.
  }
  if (r->fd >= 0) {
    ::close(r->fd);
  }
  *r = ShmRegion{};
}

void ShmRegionUnlink(const std::string &name) {
  ::unlink(CoordPath(name).c_str());
}

#else // POSIX / Linux / macOS

bool ShmRegionCreate(const std::string &name, size_t size, ShmRegion *out) {
  int fd = ::shm_open(name.c_str(), O_CREAT | O_RDWR, 0600);
  if (fd < 0) {
    fprintf(stderr, "[shm] shm_open(create) %s failed errno=%d\n", name.c_str(),
            errno);
    return false;
  }
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    fprintf(stderr, "[shm] ftruncate %s failed errno=%d\n", name.c_str(),
            errno);
    ::close(fd);
    ::shm_unlink(name.c_str());
    return false;
  }
  void *addr =
      ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    fprintf(stderr, "[shm] mmap(create) %s failed errno=%d\n", name.c_str(),
            errno);
    ::close(fd);
    ::shm_unlink(name.c_str());
    return false;
  }
  out->fd = fd;
  out->addr = addr;
  out->size = size;
  return true;
}

bool ShmRegionOpen(const std::string &name, size_t size, ShmRegion *out) {
  int fd = -1;
  for (int attempt = 0; attempt < 200; attempt++) {
    fd = ::shm_open(name.c_str(), O_RDWR, 0600);
    if (fd >= 0) {
      break;
    }
    usleep(10000);
  }
  if (fd < 0) {
    fprintf(stderr, "[shm] shm_open(open) %s failed errno=%d\n", name.c_str(),
            errno);
    return false;
  }
  void *addr =
      ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    fprintf(stderr, "[shm] mmap(open) %s failed errno=%d\n", name.c_str(),
            errno);
    ::close(fd);
    return false;
  }
  out->fd = fd;
  out->addr = addr;
  out->size = size;
  return true;
}

void ShmRegionClose(ShmRegion *r) {
  if (r->addr != nullptr && r->addr != MAP_FAILED) {
    ::munmap(r->addr, r->size);
  }
  if (r->fd >= 0) {
    ::close(r->fd);
  }
  *r = ShmRegion{};
}

void ShmRegionUnlink(const std::string &name) { ::shm_unlink(name.c_str()); }

#endif

// Per-process allocator state shared by the C callbacks via user_data.  The
// publisher and subscriber are separate processes, each with their own state.
struct PmemSplitState {
  std::mutex mu;
  std::unordered_map<uintptr_t, ShmRegion> regions;
  uintptr_t next_handle = 1;
};

// Publisher side: create a fresh pmem/shm region for a payload slot.
bool PmemSplitAllocate(const SubspaceSplitBufferInfo *info,
                       SubspaceSplitBufferMapping *mapping, void *user_data) {
  auto *state = static_cast<PmemSplitState *>(user_data);
  if (info == nullptr || mapping == nullptr || state == nullptr ||
      info->allocation_size == 0) {
    return false;
  }
  std::string name = DeriveRegionName(info);
  ShmRegion region;
  if (!ShmRegionCreate(name, static_cast<size_t>(info->allocation_size),
                       &region)) {
    return false;
  }
  std::lock_guard<std::mutex> lock(state->mu);
  uintptr_t handle = state->next_handle++;
  state->regions[handle] = region;
  mapping->handle = handle;
  mapping->address = region.addr;
  mapping->size = region.size;
  mapping->private_data = nullptr;
  return true;
}

// Subscriber side: attach to the region the publisher created for this slot.
bool PmemSplitMap(const SubspaceSplitBufferInfo *info,
                  SubspaceSplitBufferMapping *mapping, void *user_data) {
  auto *state = static_cast<PmemSplitState *>(user_data);
  if (info == nullptr || mapping == nullptr || state == nullptr) {
    return false;
  }
  std::string name = DeriveRegionName(info);
  size_t size = info->allocation_size != 0
                    ? static_cast<size_t>(info->allocation_size)
                    : static_cast<size_t>(info->full_size);
  ShmRegion region;
  if (!ShmRegionOpen(name, size, &region)) {
    return false;
  }
  std::lock_guard<std::mutex> lock(state->mu);
  uintptr_t handle = state->next_handle++;
  state->regions[handle] = region;
  mapping->handle = handle;
  mapping->address = region.addr;
  mapping->size = region.size;
  mapping->private_data = nullptr;
  return true;
}

// Subscriber side: drop a mapping without destroying the backing region.
bool PmemSplitUnmap(const SubspaceSplitBufferInfo * /*info*/,
                    const SubspaceSplitBufferMapping *mapping,
                    void *user_data) {
  auto *state = static_cast<PmemSplitState *>(user_data);
  if (mapping == nullptr || state == nullptr || mapping->handle == 0) {
    return false;
  }
  std::lock_guard<std::mutex> lock(state->mu);
  auto it = state->regions.find(mapping->handle);
  if (it == state->regions.end()) {
    return false;
  }
  ShmRegionClose(&it->second);
  state->regions.erase(it);
  return true;
}

// Publisher side: destroy the backing region and remove its name.
bool PmemSplitFree(const SubspaceSplitBufferInfo *info,
                   const SubspaceSplitBufferMapping *mapping, void *user_data) {
  auto *state = static_cast<PmemSplitState *>(user_data);
  if (info == nullptr || mapping == nullptr || state == nullptr ||
      mapping->handle == 0) {
    return false;
  }
  std::lock_guard<std::mutex> lock(state->mu);
  auto it = state->regions.find(mapping->handle);
  if (it == state->regions.end()) {
    return false;
  }
  ShmRegionClose(&it->second);
  state->regions.erase(it);
  ShmRegionUnlink(DeriveRegionName(info));
  return true;
}

SubspaceSplitBufferCallbacks MakeCallbacks(PmemSplitState *state) {
  SubspaceSplitBufferCallbacks callbacks = {};
  callbacks.allocate = PmemSplitAllocate;
  callbacks.map = PmemSplitMap;
  callbacks.unmap = PmemSplitUnmap;
  callbacks.free = PmemSplitFree;
  callbacks.user_data = state;
  return callbacks;
}

struct ScopeGuard {
  std::function<void()> fn;
  ~ScopeGuard() {
    if (fn) {
      fn();
    }
  }
};

// Body of the forked subscriber process.  Returns a process exit code.
int RunSubscriberChild(const std::string &socket, const std::string &channel,
                       int ready_fd, int ack_fd) {
  char token = 0;
  if (::read(ready_fd, &token, 1) != 1) {
    fprintf(stderr, "[sub] failed to read ready token\n");
    return 10;
  }

  SubspaceClient client = subspace_create_client_with_socket(socket.c_str());
  if (client.client == nullptr) {
    fprintf(stderr, "[sub] create client failed: %s\n",
            subspace_get_last_error());
    return 11;
  }

  PmemSplitState state;
  SubspaceSubscriberOptions opts = subspace_subscriber_options_default();
  opts.split_callbacks = MakeCallbacks(&state);
  SubspaceSubscriber sub =
      subspace_create_subscriber(client, channel.c_str(), opts);
  if (sub.subscriber == nullptr) {
    fprintf(stderr, "[sub] create subscriber failed: %s\n",
            subspace_get_last_error());
    return 12;
  }
  if (!subspace_subscriber_uses_split_buffers(sub)) {
    fprintf(stderr, "[sub] subscriber is not using split buffers\n");
    return 13;
  }

  token = 'S';
  if (::write(ack_fd, &token, 1) != 1) {
    fprintf(stderr, "[sub] failed to write ack token\n");
    return 14;
  }

  std::vector<bool> seen(kNumMessages, false);
  int received = 0;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (received < kNumMessages) {
    for (;;) {
      SubspaceMessage msg = subspace_read_message(sub);
      if (subspace_has_error()) {
        fprintf(stderr, "[sub] read_message failed: %s\n",
                subspace_get_last_error());
        return 15;
      }
      if (msg.length == 0) {
        break;
      }
      if (static_cast<int32_t>(msg.length) != kPayloadSize) {
        fprintf(stderr, "[sub] bad message length %zu (want %d)\n", msg.length,
                kPayloadSize);
        return 16;
      }
      const uint8_t *data = static_cast<const uint8_t *>(msg.buffer);
      int idx = static_cast<int>(data[0]) | (static_cast<int>(data[1]) << 8) |
                (static_cast<int>(data[2]) << 16) |
                (static_cast<int>(data[3]) << 24);
      if (idx < 0 || idx >= kNumMessages) {
        fprintf(stderr, "[sub] bad message index %d\n", idx);
        return 17;
      }
      for (int j = 4; j < kPayloadSize; j++) {
        uint8_t want = PayloadByte(idx, j);
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
      subspace_free_message(&msg);
    }
    if (received >= kNumMessages) {
      break;
    }
    auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      fprintf(stderr, "[sub] timed out after receiving %d/%d messages\n",
              received, kNumMessages);
      return 19;
    }
    uint64_t ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
            .count());
    subspace_wait_for_subscriber_with_timeout(sub, ms);
  }

  subspace_remove_subscriber(&sub);
  subspace_remove_client(&client);
  return 0;
}

TEST(PmemSplitBufferTest, CrossProcessPublishAndRead) {
  signal(SIGPIPE, SIG_IGN);

  char socket_template[] = "/tmp/subspace_pmem_splitXXXXXX"; // NOLINT
  int tmpfd = mkstemp(socket_template);
  ASSERT_GE(tmpfd, 0);
  ::close(tmpfd);
  ::remove(socket_template);
  std::string socket = socket_template;
  std::string channel = "pmem_split_channel";

  int ready_pipe[2]; // parent -> child
  int ack_pipe[2];   // child -> parent
  ASSERT_EQ(0, pipe(ready_pipe));
  ASSERT_EQ(0, pipe(ack_pipe));

  // Fork the subscriber while single-threaded (before the server thread
  // starts) so the child inherits a clean address space.
  pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::close(ready_pipe[1]);
    ::close(ack_pipe[0]);
    int code = RunSubscriberChild(socket, channel, ready_pipe[0], ack_pipe[1]);
    ::close(ready_pipe[0]);
    ::close(ack_pipe[1]);
    _exit(code);
  }

  ::close(ready_pipe[0]);
  ::close(ack_pipe[1]);

  co::CoroutineScheduler scheduler;
  std::unique_ptr<subspace::Server> server;
  std::thread server_thread;
  int server_pipe[2] = {-1, -1};
  bool child_reaped = false;

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
      scheduler, socket, "", 0, 0, /*local=*/true, server_pipe[1],
      /*initial_ordinal=*/1, /*wait_for_clients=*/true);
  server_thread = std::thread([&]() {
    absl::Status s = server->Run();
    if (!s.ok()) {
      fprintf(stderr, "Error running Subspace server: %s\n",
              s.ToString().c_str());
    }
  });
  char buf[8];
  (void)::read(server_pipe[0], buf, 8);

  SubspaceClient client = subspace_create_client_with_socket(socket.c_str());
  ASSERT_NE(nullptr, client.client);
  ASSERT_FALSE(subspace_has_error());

  PmemSplitState state;
  SubspacePublisherOptions pub_opts =
      subspace_publisher_options_default(kSlotSize, kNumSlots);
  pub_opts.use_split_buffers = true;
  pub_opts.split_callbacks = MakeCallbacks(&state);
  SubspacePublisher pub =
      subspace_create_publisher(client, channel.c_str(), pub_opts);
  ASSERT_NE(nullptr, pub.publisher);
  ASSERT_FALSE(subspace_has_error());
  ASSERT_TRUE(subspace_publisher_uses_split_buffers(pub));

  char token = 'R';
  ASSERT_EQ(1, ::write(ready_pipe[1], &token, 1));

  token = 0;
  ASSERT_EQ(1, ::read(ack_pipe[0], &token, 1));
  ASSERT_EQ('S', token);

  for (int i = 0; i < kNumMessages; i++) {
    SubspaceMessageBuffer buffer =
        subspace_get_message_buffer(pub, kPayloadSize);
    ASSERT_FALSE(subspace_has_error());
    ASSERT_NE(nullptr, buffer.buffer);
    ASSERT_GE(buffer.buffer_size, static_cast<size_t>(kPayloadSize));
    uint8_t *data = static_cast<uint8_t *>(buffer.buffer);
    data[0] = static_cast<uint8_t>(i & 0xff);
    data[1] = static_cast<uint8_t>((i >> 8) & 0xff);
    data[2] = static_cast<uint8_t>((i >> 16) & 0xff);
    data[3] = static_cast<uint8_t>((i >> 24) & 0xff);
    for (int j = 4; j < kPayloadSize; j++) {
      data[j] = PayloadByte(i, j);
    }
    SubspaceMessage status = subspace_publish_message(pub, kPayloadSize);
    ASSERT_NE(0, status.length);
    ASSERT_FALSE(subspace_has_error());
  }

  int status = 0;
  ASSERT_EQ(child, waitpid(child, &status, 0));
  child_reaped = true;
  ASSERT_TRUE(WIFEXITED(status))
      << "subscriber process did not exit normally (status " << status << ")";
  EXPECT_EQ(0, WEXITSTATUS(status))
      << "subscriber process reported failure code " << WEXITSTATUS(status);

  subspace_remove_publisher(&pub);
  subspace_remove_client(&client);
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  return RUN_ALL_TESTS();
}
