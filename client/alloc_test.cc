// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Checks that the per-message publisher and subscriber paths make no heap
// allocations once a channel is set up.
//
// Allocations are counted only on the thread that is being measured, because
// the server runs in this process. On macOS the malloc logger sees every
// malloc, calloc and realloc as well as operator new. Elsewhere operator new
// is replaced. Each counted allocation records its call stack so that a
// failure names the allocation site.
//
// Work that happens once per change in a channel's publishers or subscribers
// (fetching new trigger fds, resizing buffers) and logging of dropped
// messages are reported but not asserted.

#include "client/test_fixture.h"

#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <poll.h>
#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

constexpr int kMaxEvents = 64;
constexpr int kDepth = 18;

// Allocations recorded for one measurement. Only the counted thread writes
// them.
struct Tally {
  size_t count = 0;
  size_t bytes = 0;
  void *frames[kMaxEvents][kDepth];
  int depth[kMaxEvents];
  size_t size[kMaxEvents];
};

Tally *active_tally = nullptr;
std::atomic<bool> in_record{false};

void Record(size_t size) {
  Tally *t = active_tally;
  if (t == nullptr || in_record.exchange(true, std::memory_order_relaxed)) {
    return;
  }
  const size_t i = t->count++;
  t->bytes += size;
  if (i < static_cast<size_t>(kMaxEvents)) {
    t->depth[i] = backtrace(t->frames[i], kDepth);
    t->size[i] = size;
  }
  in_record.store(false, std::memory_order_relaxed);
}

#if defined(__APPLE__)
// The malloc logger can run while a thread's local storage is being set up, so
// it identifies the counted thread without using any.
extern "C" {
extern void (*malloc_logger)(uint32_t type, uintptr_t arg1, uintptr_t arg2,
                             uintptr_t arg3, uintptr_t result,
                             uint32_t num_hot_frames_to_skip);
}

// MALLOC_LOG_TYPE_ALLOCATE.
constexpr uint32_t kMallocLoggerAlloc = 2;
std::atomic<uintptr_t> counted_thread{0};

uintptr_t CurrentThread() {
  return reinterpret_cast<uintptr_t>(pthread_self());
}

void CountMalloc(uint32_t type, uintptr_t, uintptr_t size, uintptr_t,
                 uintptr_t, uint32_t) {
  if ((type & kMallocLoggerAlloc) != 0 &&
      counted_thread.load(std::memory_order_relaxed) == CurrentThread()) {
    Record(static_cast<size_t>(size));
  }
}

struct MallocLoggerInstaller {
  MallocLoggerInstaller() { malloc_logger = CountMalloc; }
} malloc_logger_installer;

void StartCounting() {
  counted_thread.store(CurrentThread(), std::memory_order_relaxed);
}
void StopCounting() { counted_thread.store(0, std::memory_order_relaxed); }
#else
thread_local bool counting = false;

void StartCounting() { counting = true; }
void StopCounting() { counting = false; }
#endif

}  // namespace

#if !defined(__APPLE__)
void *operator new(std::size_t size) {
  if (counting) {
    Record(size);
  }
  void *p = std::malloc(size == 0 ? 1 : size);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}

void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }
#endif

namespace {

std::string Describe(void *addr) {
  Dl_info info;
  if (dladdr(addr, &info) == 0 || info.dli_sname == nullptr) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%p", addr);
    return buf;
  }
  int status = 0;
  char *demangled =
      abi::__cxa_demangle(info.dli_sname, nullptr, nullptr, &status);
  std::string s =
      status == 0 && demangled != nullptr ? demangled : info.dli_sname;
  std::free(demangled);
  return s;
}

bool IsHookFrame(const std::string &frame) {
  for (const char *hook : {"Record", "CountMalloc", "backtrace", "malloc",
                           "operator new", "__libcpp_operator_new",
                           "__libcpp_allocate", "allocator<"}) {
    if (frame.find(hook) != std::string::npos) {
      return true;
    }
  }
  return false;
}

// Run f with counting on, adding its allocations to t.
template <typename F> void Counted(Tally &t, F &&f) {
  active_tally = &t;
  StartCounting();
  f();
  StopCounting();
  active_tally = nullptr;
}

// Print each distinct allocation site in t.
void Report(const std::string &name, const Tally &t) {
  printf("\n== %s: %zu allocations, %zu bytes\n", name.c_str(), t.count,
         t.bytes);
  std::map<std::string, std::pair<int, size_t>> sites;
  const size_t n = std::min(t.count, static_cast<size_t>(kMaxEvents));
  for (size_t i = 0; i < n; i++) {
    std::string text;
    int shown = 0;
    for (int f = 0; f < t.depth[i] && shown < 8; f++) {
      std::string frame = Describe(t.frames[i][f]);
      if (IsHookFrame(frame)) {
        continue;
      }
      if (frame.size() > 200) {
        frame.resize(200);
      }
      text += "      " + frame + "\n";
      shown++;
    }
    auto &site = sites[text];
    site.first++;
    site.second = t.size[i];
  }
  for (const auto &[text, site] : sites) {
    printf("  %d x %zu bytes\n%s", site.first, site.second, text.c_str());
  }
  fflush(stdout);
}

constexpr int kWarmup = 100;
constexpr int kIterations = 300;

class AllocTest : public SubspaceTestBase {
public:
  // Assert that no allocations were counted, naming the sites if there were.
  void ExpectNone(const std::string &name, const Tally &t) {
    printf("%-52s %zu\n", name.c_str(), t.count);
    if (t.count != 0) {
      Report(name, t);
    }
    EXPECT_EQ(0u, t.count) << name;
  }

  // Work allowed to allocate. Reported only.
  void Note(const std::string &name, const Tally &t, int events) {
    printf("%-52s %zu per event (not asserted)\n", name.c_str(),
           t.count / events);
  }

  std::shared_ptr<subspace::Client> NewClient() {
    absl::StatusOr<std::shared_ptr<subspace::Client>> client =
        subspace::Client::Create(Socket());
    EXPECT_TRUE(client.ok()) << client.status();
    return client.ok() ? *client : nullptr;
  }
};

subspace::PublisherOptions Pub(int slot_size, int num_slots) {
  return subspace::PublisherOptions().SetSlotSize(slot_size).SetNumSlots(
      num_slots);
}

subspace::SubscriberOptions Sub() {
  subspace::SubscriberOptions options;
  options.SetLogDroppedMessages(false);
  return options;
}

// Publish one message. Returns false if no slot was available.
bool Publish(Publisher &pub, const char *text = "message") {
  absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
  if (!buffer.ok() || *buffer == nullptr) {
    return false;
  }
  const size_t len = strlen(text);
  memcpy(*buffer, text, len);
  return pub.PublishMessage(static_cast<int64_t>(len)).ok();
}

// Read one message and drop it. Returns its length, or 0 if there was none.
size_t Read(Subscriber &sub,
            subspace::ReadMode mode = subspace::ReadMode::kReadNext) {
  absl::StatusOr<Message> msg = sub.ReadMessage(mode);
  return msg.ok() ? msg->length : 0;
}

// Publish then read, one message at a time.
void PublishAndReadCycle(AllocTest &test, const std::string &name,
                         Publisher &pub, Subscriber &sub) {
  for (int i = 0; i < kWarmup; i++) {
    ASSERT_TRUE(Publish(pub));
    ASSERT_GT(Read(sub), 0u);
  }
  Tally publish;
  Tally read;
  Tally empty;
  bool ok = true;
  for (int i = 0; i < kIterations; i++) {
    Counted(publish, [&] { ok &= Publish(pub); });
    Counted(read, [&] { ok &= Read(sub) > 0; });
    Counted(empty, [&] { ok &= Read(sub) == 0; });
  }
  EXPECT_TRUE(ok) << name;
  test.ExpectNone(name + ": publish", publish);
  test.ExpectNone(name + ": read", read);
  test.ExpectNone(name + ": read with nothing waiting", empty);
}

TEST_F(AllocTest, CounterSeesAnAllocation) {
  // An indirect call keeps an optimized build from removing the new/delete
  // pair.
  using OpNew = void *(*)(std::size_t);
  volatile OpNew allocate = &::operator new;
  void *p = nullptr;
  Tally t;
  Counted(t, [&] { p = allocate(sizeof(int)); });
  ::operator delete(p);
  ASSERT_EQ(1u, t.count);
}

TEST_F(AllocTest, Unreliable) {
  auto client = NewClient();
  auto pub = EVAL_AND_ASSERT_OK(
      client->CreatePublisher("alloc_unreliable", Pub(256, 16)));
  auto sub =
      EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_unreliable", Sub()));
  PublishAndReadCycle(*this, "unreliable", pub, sub);
}

TEST_F(AllocTest, Reliable) {
  auto client = NewClient();
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "alloc_reliable", Pub(256, 16).SetReliable(true)));
  auto sub = EVAL_AND_ASSERT_OK(
      client->CreateSubscriber("alloc_reliable", Sub().SetReliable(true)));
  PublishAndReadCycle(*this, "reliable", pub, sub);
}

TEST_F(AllocTest, VirtualAndMultiplexerSubscribers) {
  auto client = NewClient();
  constexpr char kMux[] = "alloc_mux";
  auto pub = EVAL_AND_ASSERT_OK(
      client->CreatePublisher("alloc_vchan", Pub(256, 32).SetMux(kMux)));
  auto vsub = EVAL_AND_ASSERT_OK(
      client->CreateSubscriber("alloc_vchan", Sub().SetMux(kMux)));
  auto msub = EVAL_AND_ASSERT_OK(client->CreateSubscriber(kMux, Sub()));
  for (int i = 0; i < kWarmup; i++) {
    ASSERT_TRUE(Publish(pub));
    ASSERT_GT(Read(vsub), 0u);
    ASSERT_GT(Read(msub), 0u);
  }
  Tally publish;
  Tally vread;
  Tally mread;
  bool ok = true;
  for (int i = 0; i < kIterations; i++) {
    Counted(publish, [&] { ok &= Publish(pub); });
    Counted(vread, [&] { ok &= Read(vsub) > 0; });
    Counted(mread, [&] { ok &= Read(msub) > 0; });
  }
  EXPECT_TRUE(ok);
  ExpectNone("virtual publisher: publish", publish);
  ExpectNone("virtual subscriber: read", vread);
  ExpectNone("multiplexer subscriber: read", mread);
}

TEST_F(AllocTest, BacklogAndTwoSubscribers) {
  auto client = NewClient();
  auto client2 = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_backlog", Pub(256, 32)));
  auto sub =
      EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_backlog", Sub()));
  auto other =
      EVAL_AND_ASSERT_OK(client2->CreateSubscriber("alloc_backlog", Sub()));
  constexpr int kBurst = 8;
  for (int i = 0; i < kWarmup; i++) {
    for (int b = 0; b < kBurst; b++) {
      ASSERT_TRUE(Publish(pub));
    }
    for (int b = 0; b < kBurst; b++) {
      ASSERT_GT(Read(sub), 0u);
      ASSERT_GT(Read(other), 0u);
    }
  }
  Tally publish;
  Tally read;
  bool ok = true;
  for (int i = 0; i < kIterations / 4; i++) {
    Counted(publish, [&] {
      for (int b = 0; b < kBurst; b++) {
        ok &= Publish(pub);
      }
    });
    Counted(read, [&] {
      for (int b = 0; b < kBurst; b++) {
        ok &= Read(sub) > 0;
        ok &= Read(other) > 0;
      }
    });
  }
  EXPECT_TRUE(ok);
  ExpectNone("backlog of 8, two subscribers: publish", publish);
  ExpectNone("backlog of 8, two subscribers: read", read);
}

TEST_F(AllocTest, ReadNewest) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_newest", Pub(256, 16)));
  auto sub =
      EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_newest", Sub()));
  for (int i = 0; i < kWarmup; i++) {
    ASSERT_TRUE(Publish(pub));
    ASSERT_TRUE(Publish(pub));
    ASSERT_GT(Read(sub, subspace::ReadMode::kReadNewest), 0u);
  }
  Tally read;
  bool ok = true;
  for (int i = 0; i < kIterations; i++) {
    ok &= Publish(pub);
    ok &= Publish(pub);
    ok &= Publish(pub);
    Counted(read, 
        [&] { ok &= Read(sub, subspace::ReadMode::kReadNewest) > 0; });
  }
  EXPECT_TRUE(ok);
  ExpectNone("read newest", read);
}

TEST_F(AllocTest, Checksum) {
  auto client = NewClient();
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "alloc_checksum", Pub(256, 16).SetChecksum(true)));
  auto sub = EVAL_AND_ASSERT_OK(
      client->CreateSubscriber("alloc_checksum", Sub().SetChecksum(true)));
  PublishAndReadCycle(*this, "checksum", pub, sub);
}

TEST_F(AllocTest, KeepActiveMessage) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_keep", Pub(256, 16)));
  auto sub = EVAL_AND_ASSERT_OK(client->CreateSubscriber(
      "alloc_keep", Sub().SetKeepActiveMessage(true)));
  PublishAndReadCycle(*this, "keep active message", pub, sub);
}

TEST_F(AllocTest, SplitBuffers) {
  auto client = NewClient();
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "alloc_split", Pub(256, 16).SetUseSplitBuffers(true)));
  auto sub = EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_split", Sub()));
  PublishAndReadCycle(*this, "split buffers", pub, sub);
}

// Holding max_active_messages, then failing to read another, then releasing.
TEST_F(AllocTest, MaxActiveMessages) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_active", Pub(256, 16)));
  auto sub = EVAL_AND_ASSERT_OK(
      client->CreateSubscriber("alloc_active", Sub().SetMaxActiveMessages(4)));
  std::vector<Message> held;
  held.reserve(8);
  auto cycle = [&](bool &ok) {
    for (int b = 0; b < 5; b++) {
      ok &= Publish(pub);
    }
    for (int b = 0; b < 4; b++) {
      absl::StatusOr<Message> msg = sub.ReadMessage();
      ok &= msg.ok() && msg->length > 0;
      if (msg.ok()) {
        held.push_back(std::move(*msg));
      }
    }
    // The fifth is waiting but the subscriber is at its limit.
    absl::StatusOr<Message> blocked = sub.ReadMessage();
    ok &= blocked.ok() && blocked->length == 0;
    held.clear();
    ok &= Read(sub) > 0;
  };
  bool ok = true;
  for (int i = 0; i < kWarmup / 4; i++) {
    cycle(ok);
  }
  ASSERT_TRUE(ok);
  Tally allocations;
  for (int i = 0; i < kIterations / 4; i++) {
    Counted(allocations, [&] { cycle(ok); });
  }
  EXPECT_TRUE(ok);
  ExpectNone("max active messages: hold, blocked read, release", allocations);
}

// A subscriber that falls behind the ring and detects dropped messages.
TEST_F(AllocTest, DroppedMessages) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_drops", Pub(256, 8)));
  auto quiet =
      EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_drops", Sub()));
  int64_t dropped = 0;
  ASSERT_OK(quiet.RegisterDroppedMessageCallback(
      [&dropped](Subscriber *, int64_t n) { dropped += n; }));
  subspace::SubscriberOptions logging_options;
  logging_options.SetLogDroppedMessages(true);
  auto logging = EVAL_AND_ASSERT_OK(
      client->CreateSubscriber("alloc_drops", logging_options));

  auto lap = [&](bool &ok) {
    for (int b = 0; b < 20; b++) {
      ok &= Publish(pub);
    }
  };
  bool ok = true;
  for (int i = 0; i < 10; i++) {
    lap(ok);
    while (Read(quiet) > 0) {
    }
    while (Read(logging) > 0) {
    }
  }
  ASSERT_TRUE(ok);
  ASSERT_GT(dropped, 0);
  const int64_t warm_drops = dropped;

  Tally callback_reads;
  Tally logged_reads;
  for (int i = 0; i < 50; i++) {
    lap(ok);
    Counted(callback_reads, [&] {
      while (Read(quiet) > 0) {
      }
    });
    Counted(logged_reads, [&] {
      while (Read(logging) > 0) {
      }
    });
  }
  EXPECT_TRUE(ok);
  EXPECT_GT(dropped, warm_drops);
  ExpectNone("dropped messages, callback", callback_reads);
  // toolbelt::Logger formats through std::string, and localtime_r copies TZ
  // on macOS, so a logged drop allocates.
  Note("dropped messages, logged", logged_reads, 50);
}

TEST_F(AllocTest, MessageCallbackAndFind) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_callback", Pub(256, 16)));
  auto sub =
      EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_callback", Sub()));
  int received = 0;
  ASSERT_OK(sub.RegisterMessageCallback(
      [&received](Subscriber *, Message m) { received += m.length > 0; }));
  bool ok = true;
  for (int i = 0; i < kWarmup; i++) {
    ok &= Publish(pub);
    ok &= sub.ProcessAllMessages().ok();
  }
  ASSERT_TRUE(ok);
  Tally process;
  for (int i = 0; i < kIterations; i++) {
    ok &= Publish(pub);
    Counted(process, [&] { ok &= sub.ProcessAllMessages().ok(); });
  }
  EXPECT_TRUE(ok);
  EXPECT_EQ(kWarmup + kIterations, received);
  ExpectNone("message callback via ProcessAllMessages", process);
  ASSERT_OK(sub.UnregisterMessageCallback());

  Tally find;
  int publish_failures = 0;
  int read_failures = 0;
  int find_failures = 0;
  for (int i = 0; i < kIterations; i++) {
    publish_failures += !Publish(pub);
    absl::StatusOr<Message> latest = sub.ReadMessage();
    read_failures += !(latest.ok() && latest->length > 0);
    const uint64_t timestamp = latest.ok() ? latest->timestamp : 0;
    latest = Message();
    Counted(find, [&] {
      absl::StatusOr<Message> found = sub.FindMessage(timestamp);
      find_failures += !(found.ok() && found->length > 0);
    });
  }
  EXPECT_EQ(0, publish_failures);
  EXPECT_EQ(0, read_failures);
  EXPECT_EQ(0, find_failures);
  ExpectNone("find message by timestamp", find);
}

TEST_F(AllocTest, Waits) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_wait", Pub(256, 16)));
  auto sub = EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_wait", Sub()));
  bool ok = true;
  for (int i = 0; i < kWarmup; i++) {
    ok &= Publish(pub);
    ok &= sub.Wait(std::chrono::seconds(1)).ok();
    ok &= Read(sub) > 0;
  }
  ASSERT_TRUE(ok);
  Tally wait;
  for (int i = 0; i < kIterations; i++) {
    ok &= Publish(pub);
    Counted(wait, [&] { ok &= sub.Wait(std::chrono::seconds(1)).ok(); });
    ok &= Read(sub) > 0;
  }
  EXPECT_TRUE(ok);
  ExpectNone("subscriber wait on a published message", wait);
}

// A reliable publisher whose subscriber holds every slot it can, so publishing
// runs out of slots and the publisher waits for the subscriber.
TEST_F(AllocTest, ReliablePublisherOutOfSlots) {
  auto client = NewClient();
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "alloc_rel_full", Pub(256, 8).SetReliable(true)));
  auto sub = EVAL_AND_ASSERT_OK(
      client->CreateSubscriber("alloc_rel_full", Sub().SetReliable(true)));
  auto fill = [&](int &published) {
    published = 0;
    while (Publish(pub)) {
      published++;
    }
  };
  auto drain = [&](bool &ok) {
    while (Read(sub) > 0) {
    }
    ok &= pub.Wait(std::chrono::seconds(1)).ok();
  };
  bool ok = true;
  int published = 0;
  for (int i = 0; i < 20; i++) {
    fill(published);
    ok &= published > 0;
    drain(ok);
  }
  ASSERT_TRUE(ok);
  Tally full;
  Tally waits;
  for (int i = 0; i < 50; i++) {
    Counted(full, [&] { fill(published); });
    ok &= published > 0;
    Counted(waits, [&] { drain(ok); });
  }
  EXPECT_TRUE(ok);
  ExpectNone("reliable publisher fills the ring", full);
  ExpectNone("reliable subscriber drains, publisher waits", waits);
}

// Retirement notification: a subscriber releases, the publisher reads the
// retirement fd, then reclaims and republishes the retired slot.
TEST_F(AllocTest, RetirementAndLeases) {
  auto client = NewClient();
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "alloc_lease",
      Pub(256, 16).SetNotifyRetirement(true).SetMaxOutstandingSlotLeases(2)));
  auto sub = EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_lease", Sub()));
  const int retirement_fd = pub.GetRetirementFd().Fd();

  auto drain_retirements = [&] {
    for (;;) {
      struct pollfd pfd = {.fd = retirement_fd, .events = POLLIN};
      if (::poll(&pfd, 1, 0) != 1) {
        return;
      }
      int32_t slot_id;
      if (::read(retirement_fd, &slot_id, sizeof(slot_id)) !=
          sizeof(slot_id)) {
        return;
      }
    }
  };

  Tally acquire;
  Tally publish;
  Tally read;
  Tally retire_wait;
  Tally reclaim;
  Tally release;
  bool ok = true;
  auto cycle = [&](bool count) {
    auto measure = [&](Tally &total, auto &&f) {
      if (count) {
        Counted(total, f);
      } else {
        f();
      }
    };
    subspace::PublisherBufferLease lease;
    measure(acquire, [&] {
      absl::StatusOr<subspace::PublisherBufferLease> l =
          pub.AcquireBufferLease();
      ok &= l.ok() && static_cast<bool>(*l);
      if (l.ok()) {
        lease = *l;
      }
    });
    if (!lease) {
      return;
    }
    memcpy(lease.buffer, "lease", 5);
    const int32_t slot_id = lease.slot_id;
    measure(publish, [&] { ok &= pub.PublishBufferLease(lease, 5).ok(); });
    measure(read, [&] { ok &= Read(sub) > 0; });
    measure(retire_wait, [&] {
      struct pollfd pfd = {.fd = retirement_fd, .events = POLLIN};
      ok &= ::poll(&pfd, 1, 1000) == 1;
      int32_t retired = -1;
      ok &= ::read(retirement_fd, &retired, sizeof(retired)) ==
            sizeof(retired);
    });
    drain_retirements();
    subspace::PublisherBufferLease again;
    measure(reclaim, [&] {
      absl::StatusOr<subspace::PublisherBufferLease> l =
          pub.ReclaimBufferLease(slot_id);
      ok &= l.ok();
      if (l.ok()) {
        again = *l;
      }
    });
    if (again) {
      measure(release, [&] { ok &= pub.ReleaseBufferLease(again).ok(); });
    }
    drain_retirements();
  };
  for (int i = 0; i < kWarmup; i++) {
    cycle(false);
  }
  ASSERT_TRUE(ok);
  for (int i = 0; i < kIterations; i++) {
    cycle(true);
  }
  EXPECT_TRUE(ok);
  ExpectNone("lease: acquire", acquire);
  ExpectNone("lease: publish", publish);
  ExpectNone("lease: subscriber read and release", read);
  ExpectNone("lease: publisher reads retirement fd", retire_wait);
  ExpectNone("lease: reclaim retired slot", reclaim);
  ExpectNone("lease: release reclaimed lease", release);
}

// A subscriber's first reads on a channel, with and without an activation
// message from the publisher. A publisher sends its activation once, when it
// is created, and a new publisher makes existing subscribers reload, so each
// round uses a new publisher and new subscribers.
TEST_F(AllocTest, Activation) {
  auto client = NewClient();
  auto client2 = NewClient();
  auto first_reads = [&](const char *channel, bool activate, Tally &skipped,
                         Tally &passed, bool &ok) {
    auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
        channel, Pub(256, 16).SetActivate(activate)));
    auto sub = EVAL_AND_ASSERT_OK(client2->CreateSubscriber(channel, Sub()));
    subspace::SubscriberOptions pass = Sub();
    pass.SetPassActivation(true);
    auto passing =
        EVAL_AND_ASSERT_OK(client2->CreateSubscriber(channel, pass));
    ok &= Publish(pub);
    Counted(skipped, [&] {
      while (Read(sub) > 0) {
      }
    });
    Counted(passed, [&] {
      while (Read(passing) > 0) {
      }
    });
  };
  constexpr int kRounds = 20;
  Tally plain_skipped;
  Tally plain_passed;
  Tally skipped;
  Tally passed;
  bool ok = true;
  for (int i = 0; i < kRounds; i++) {
    first_reads("alloc_no_activate", false, plain_skipped, plain_passed, ok);
    first_reads("alloc_activate", true, skipped, passed, ok);
  }
  EXPECT_TRUE(ok);
  ExpectNone("first read, no activation", plain_skipped);
  ExpectNone("first read, no activation, passing activations", plain_passed);
  ExpectNone("first read, activation skipped", skipped);
  ExpectNone("first read, activation passed", passed);
}

// A multiplexer subscriber seeing a virtual channel for the first time.
TEST_F(AllocTest, NewVirtualChannel) {
  auto client = NewClient();
  constexpr char kMux[] = "alloc_new_vchan_mux";
  auto first = EVAL_AND_ASSERT_OK(
      client->CreatePublisher("alloc_new_vchan_0", Pub(256, 32).SetMux(kMux)));
  auto msub = EVAL_AND_ASSERT_OK(client->CreateSubscriber(kMux, Sub()));
  bool ok = true;
  for (int i = 0; i < kWarmup; i++) {
    ok &= Publish(first);
    ok &= Read(msub) > 0;
  }
  ASSERT_TRUE(ok);
  constexpr int kChannels = 10;
  Tally new_vchan;
  Tally steady;
  for (int i = 1; i <= kChannels; i++) {
    auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
        "alloc_new_vchan_" + std::to_string(i), Pub(256, 32).SetMux(kMux)));
    // Absorb the reload for the new publisher on a message from a known
    // virtual channel.
    ok &= Publish(first);
    ok &= Read(msub) > 0;
    ok &= Publish(pub);
    Counted(new_vchan, [&] { ok &= Read(msub) > 0; });
    ok &= Publish(pub);
    Counted(steady, [&] { ok &= Read(msub) > 0; });
  }
  EXPECT_TRUE(ok);
  Note("first message from a new virtual channel", new_vchan, kChannels);
  ExpectNone("later messages from that virtual channel", steady);
}

// Subscribers and publishers joining and leaving. The first operation after a
// change fetches trigger fds from the server; everything after must not
// allocate.
TEST_F(AllocTest, UserChanges) {
  auto client = NewClient();
  auto client2 = NewClient();
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "alloc_users", Pub(256, 16).SetReliable(true)));
  auto stable = EVAL_AND_ASSERT_OK(
      client->CreateSubscriber("alloc_users", Sub().SetReliable(true)));
  bool ok = true;
  for (int i = 0; i < kWarmup; i++) {
    ok &= Publish(pub);
    ok &= Read(stable) > 0;
  }
  ASSERT_TRUE(ok);

  Tally after_join;
  Tally after_leave;
  Tally steady_publish;
  Tally steady_read;
  constexpr int kChanges = 20;
  for (int i = 0; i < kChanges; i++) {
    std::optional<Subscriber> joined =
        EVAL_AND_ASSERT_OK(client2->CreateSubscriber("alloc_users", Sub()));
    Counted(after_join, [&] { ok &= Publish(pub); });
    ok &= Read(stable) > 0;
    ok &= Read(*joined) > 0;
    Counted(steady_publish, [&] { ok &= Publish(pub); });
    Counted(steady_read, [&] {
      ok &= Read(stable) > 0;
      ok &= Read(*joined) > 0;
    });
    joined.reset();
    Counted(after_leave, [&] { ok &= Publish(pub); });
    ok &= Read(stable) > 0;
  }
  EXPECT_TRUE(ok);
  Note("first publish after a subscriber joins", after_join, kChanges);
  Note("first publish after a subscriber leaves", after_leave, kChanges);
  ExpectNone("publish after the join was absorbed", steady_publish);
  ExpectNone("read after the join was absorbed", steady_read);

  Tally after_pub_join;
  Tally steady_after_pub;
  for (int i = 0; i < kChanges; i++) {
    auto other = EVAL_AND_ASSERT_OK(client2->CreatePublisher(
        "alloc_users", Pub(256, 16).SetReliable(true)));
    ok &= Publish(pub);
    Counted(after_pub_join, [&] { ok &= Read(stable) > 0; });
    ok &= Publish(pub);
    Counted(steady_after_pub, [&] { ok &= Read(stable) > 0; });
  }
  EXPECT_TRUE(ok);
  Note("first read after a reliable publisher joins", after_pub_join,
       kChanges);
  ExpectNone("read after the publisher join was absorbed", steady_after_pub);
}

// A message larger than the slot makes the publisher allocate bigger buffers.
TEST_F(AllocTest, Resize) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_resize", Pub(64, 8)));
  auto sub =
      EVAL_AND_ASSERT_OK(client->CreateSubscriber("alloc_resize", Sub()));
  bool ok = true;
  ok &= Publish(pub);
  ok &= Read(sub) > 0;
  Tally grow;
  Counted(grow, [&] {
    absl::StatusOr<void *> buffer = pub.GetMessageBuffer(4096);
    ok &= buffer.ok() && *buffer != nullptr;
    if (buffer.ok() && *buffer != nullptr) {
      memset(*buffer, 'x', 4096);
      ok &= pub.PublishMessage(4096).ok();
    }
  });
  Tally first_read;
  Counted(first_read, [&] { ok &= Read(sub) > 0; });
  EXPECT_TRUE(ok);
  Note("publisher resizes the channel", grow, 1);
  Note("subscriber maps the resized buffer", first_read, 1);
  PublishAndReadCycle(*this, "after resize", pub, sub);
}

// Called on the publish commit path. Drive it with an obligation owned by a
// subscriber that no longer exists.
TEST_F(AllocTest, ReleaseDepartedObligations) {
  auto client = NewClient();
  auto pub =
      EVAL_AND_ASSERT_OK(client->CreatePublisher("alloc_departed", Pub(256, 8)));
  subspace::ServerChannel *channel = Server()->FindChannel("alloc_departed");
  ASSERT_NE(nullptr, channel);
  subspace::MessageSlot *slot = &channel->GetCcb()->slots[0];
  const uint64_t old_refs = slot->refs.load(std::memory_order_acquire);
  const uint64_t old_ordinal = slot->ordinal.load(std::memory_order_relaxed);
  slot->ordinal.store(1, std::memory_order_relaxed);
  slot->vchan_id.store(-1, std::memory_order_relaxed);
  constexpr int kDepartedId = subspace::kMaxSlotOwners - 1;
  Tally allocations;
  bool ok = true;
  for (int i = 0; i < kIterations; i++) {
    slot->refs.store(subspace::BuildRefsBitField(1, -1, 1),
                     std::memory_order_release);
    slot->pending_subs.Set(kDepartedId);
    Counted(allocations, [&] { channel->ReleaseDepartedObligations(slot); });
    ok &= subspace::PendingCount(slot->refs.load(std::memory_order_acquire)) ==
          0;
  }
  slot->ordinal.store(old_ordinal, std::memory_order_relaxed);
  slot->refs.store(old_refs, std::memory_order_release);
  EXPECT_TRUE(ok);
  ExpectNone("ReleaseDepartedObligations", allocations);
}

}  // namespace
