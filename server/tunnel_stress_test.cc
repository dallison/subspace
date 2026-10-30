// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

// Load and restart tests for the tunnel, each sized to take a few seconds.
// The restart test runs the subspace_server binary named by SUBSPACE_SERVER.

#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "google/protobuf/text_format.h"
#include "proto/tunnel_config.pb.h"
#include "server/tunnel.h"
#include "server/tunnel_test_util.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <poll.h>
#include <signal.h>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#if defined(__linux__)
#include <sys/prctl.h>
#endif

namespace subspace {
namespace {

using ::absl_testing::IsOk;
using namespace std::chrono_literals;
using namespace tunnel_test;

// A message carrying its sequence number, padded to `size` bytes with a
// pattern that depends on the number.
std::string Numbered(uint32_t seq, size_t size) {
  std::string data =
      Pattern(std::max(size, sizeof(seq)), static_cast<int>(seq % 251));
  memcpy(data.data(), &seq, sizeof(seq));
  return data;
}

// The number of a message Numbered made, or -1 if the message is damaged.
int64_t SequenceOf(const std::string &data) {
  uint32_t seq;
  if (data.size() < sizeof(seq)) {
    return -1;
  }
  memcpy(&seq, data.data(), sizeof(seq));
  return data == Numbered(seq, data.size()) ? int64_t{seq} : -1;
}

// Waits for counters that other threads update to agree.
void Settle(const std::function<bool()> &agree) {
  auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!agree() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
}

::testing::AssertionResult Consecutive(const std::vector<int64_t> &received,
                                       int64_t first, int64_t last) {
  if (received.size() != static_cast<size_t>(last - first + 1)) {
    return ::testing::AssertionFailure()
           << received.size() << " messages arrived, not "
           << last - first + 1;
  }
  for (size_t i = 0; i < received.size(); i++) {
    if (received[i] != first + static_cast<int64_t>(i)) {
      return ::testing::AssertionFailure()
             << "message " << i << " is number " << received[i];
    }
  }
  return ::testing::AssertionSuccess();
}

// Messages before number `last` may be missing, but what arrives is intact
// and in order, and ends with number `last`: a subscriber that falls behind
// on an unreliable channel loses older messages, never the newest.
::testing::AssertionResult InOrderToTheEnd(const std::vector<int64_t> &received,
                                           int64_t last) {
  int64_t previous = -1;
  for (size_t i = 0; i < received.size(); i++) {
    if (received[i] < 0 || received[i] > last) {
      return ::testing::AssertionFailure() << "message " << i << " is damaged";
    }
    if (received[i] <= previous) {
      return ::testing::AssertionFailure()
             << "message " << i << " is number " << received[i]
             << " after number " << previous;
    }
    previous = received[i];
  }
  if (previous != last) {
    return ::testing::AssertionFailure()
           << "the last message is number " << previous << ", not " << last;
  }
  return ::testing::AssertionSuccess();
}

// Reads numbered messages on its own thread until number `last` arrives or
// nothing has arrived for `idle`.
class Collector {
public:
  Collector(Subscriber &sub, int64_t last, std::chrono::milliseconds idle = 10s)
      : thread_([this, &sub, last, idle]() { Run(sub, last, idle); }) {}
  ~Collector() { Join(); }

  // The numbers of the messages read, with -1 for damaged ones.
  const std::vector<int64_t> &Join() {
    if (thread_.joinable()) {
      thread_.join();
    }
    return received_;
  }

private:
  void Run(Subscriber &sub, int64_t last, std::chrono::milliseconds idle) {
    for (;;) {
      absl::StatusOr<std::string> data = Read(sub, idle);
      if (!data.ok()) {
        return;
      }
      received_.push_back(SequenceOf(*data));
      if (received_.back() == last) {
        return;
      }
    }
  }

  std::vector<int64_t> received_;
  std::thread thread_;
};

// Each publishing and subscribing client is used by one thread.
class TunnelStressTest : public ::testing::Test {
protected:
  void SetUp() override {
    signal(SIGPIPE, SIG_IGN);
    ASSERT_NO_FATAL_FAILURE(servers_.Start());
    for (int i = 0; i < 2; i++) {
      ASSERT_THAT(publishers_[i].Init(servers_.Socket(i)), IsOk());
      ASSERT_THAT(subscribers_[i].Init(servers_.Socket(i)), IsOk());
    }
  }

  TunnelServers servers_;
  Client publishers_[2];
  Client subscribers_[2];
};

// Mostly small, with a larger message now and then that makes the channel
// grow.
size_t VariedSize(uint32_t i) {
  constexpr size_t kLarge[] = {1000, 4000, 16000};
  return i % 50 == 49 ? kLarge[(i / 50) % 3] : 16 + (i * 37) % 200;
}

// The subscriber holds back the tunnel and the publisher behind it, and the
// sizes make both servers resize while they wait, but nothing is lost.
TEST_F(TunnelStressTest, ReliableChannelDeliversEveryMessageInOrder) {
  constexpr uint32_t kMessages = 5000;
  absl::StatusOr<Publisher> pub = publishers_[0].CreatePublisher(
      "/stress/reliable",
      PublisherOptions().SetSlotSize(256).SetNumSlots(8).SetReliable(true));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = subscribers_[1].CreateSubscriber(
      "/stress/reliable", SubscriberOptions().SetReliable(true));
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());
  const TunnelManager::Stats &alpha = servers_.Stats(0);
  const TunnelManager::Stats &beta = servers_.Stats(1);
  const uint64_t sent_before = alpha.messages_sent;
  const uint64_t received_before = beta.messages_received;

  absl::Status published;
  std::vector<int64_t> received;
  {
    Collector collector(*sub, kMessages - 1);
    for (uint32_t i = 0; i < kMessages && published.ok(); i++) {
      published = Publish(*pub, Numbered(i, VariedSize(i)));
    }
    received = collector.Join();
  }
  ASSERT_THAT(published, IsOk());
  EXPECT_TRUE(Consecutive(received, 0, kMessages - 1));
  EXPECT_FALSE(Read(*sub, 200ms).ok()) << "an extra message arrived";
  Settle([&]() {
    return alpha.messages_sent - sent_before == kMessages &&
           beta.messages_received - received_before == kMessages;
  });
  EXPECT_EQ(alpha.messages_sent - sent_before, kMessages);
  EXPECT_EQ(beta.messages_received - received_before, kMessages);
}

// Publishing as fast as possible overwrites messages before they're read,
// on either server, but what does arrive is intact and in order, the stream
// reaches its end, and the connection between the servers loses nothing.
TEST_F(TunnelStressTest, UnreliableChannelDeliversIntactMessagesWhenItDrops) {
  constexpr uint32_t kMessages = 20000;
  absl::StatusOr<Publisher> pub = publishers_[0].CreatePublisher(
      "/stress/unreliable",
      PublisherOptions().SetSlotSize(256).SetNumSlots(16));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub =
      subscribers_[1].CreateSubscriber("/stress/unreliable");
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());
  const TunnelManager::Stats &alpha = servers_.Stats(0);
  const TunnelManager::Stats &beta = servers_.Stats(1);
  const uint64_t sent_before = alpha.messages_sent;
  const uint64_t received_before = beta.messages_received;

  absl::Status published;
  std::vector<int64_t> received;
  {
    Collector collector(*sub, kMessages - 1);
    for (uint32_t i = 0; i < kMessages && published.ok(); i++) {
      published = Publish(*pub, Numbered(i, VariedSize(i)));
    }
    received = collector.Join();
  }
  ASSERT_THAT(published, IsOk());
  EXPECT_TRUE(InOrderToTheEnd(received, kMessages - 1));
  Settle([&]() {
    return alpha.messages_sent - sent_before ==
           beta.messages_received - received_before;
  });
  const uint64_t sent = alpha.messages_sent - sent_before;
  EXPECT_EQ(beta.messages_received - received_before, sent);
  EXPECT_LE(sent, kMessages);
  EXPECT_GE(sent, received.size());
  RecordProperty("sent", static_cast<int>(sent));
  RecordProperty("delivered", static_cast<int>(received.size()));
}

// A channel in the many-channels test.
struct Stream {
  int from;
  bool reliable;
  std::optional<Publisher> pub;
  std::optional<Subscriber> sub;
  std::vector<int64_t> received;
};

// Like WaitForRoute, for all the streams at once.
absl::Status WaitForRoutes(std::vector<Stream> &streams) {
  auto deadline = std::chrono::steady_clock::now() + 15s;
  std::vector<bool> routed(streams.size(), false);
  size_t unrouted = streams.size();
  while (unrouted > 0 && std::chrono::steady_clock::now() < deadline) {
    for (size_t i = 0; i < streams.size(); i++) {
      if (!routed[i]) {
        if (absl::Status s = Publish(*streams[i].pub, kProbe); !s.ok()) {
          return s;
        }
      }
    }
    std::this_thread::sleep_for(50ms);
    for (size_t i = 0; i < streams.size(); i++) {
      while (Read(*streams[i].sub, 0ms).ok()) {
        if (!routed[i]) {
          routed[i] = true;
          unrouted--;
        }
      }
    }
  }
  if (unrouted > 0) {
    return absl::DeadlineExceededError(
        absl::StrFormat("%d channels have no route", unrouted));
  }
  std::this_thread::sleep_for(200ms);
  for (Stream &stream : streams) {
    while (Read(*stream.sub, 0ms).ok()) {
    }
  }
  return absl::OkStatus();
}

// Reads `side`'s subscribers in turn until each has its last message, or
// nothing has arrived for 10 seconds.
void ReadStreams(std::vector<Stream> &streams, int side, int64_t last) {
  auto idle_since = std::chrono::steady_clock::now();
  for (;;) {
    bool done = true;
    bool progress = false;
    for (Stream &stream : streams) {
      if (stream.from == side ||
          (!stream.received.empty() && stream.received.back() == last)) {
        continue;
      }
      done = false;
      for (;;) {
        absl::StatusOr<Message> msg = stream.sub->ReadMessage();
        if (!msg.ok() || msg->length == 0) {
          break;
        }
        progress = true;
        stream.received.push_back(SequenceOf(std::string(
            static_cast<const char *>(msg->buffer), msg->length)));
        if (stream.received.back() == last) {
          break;
        }
      }
    }
    if (done) {
      return;
    }
    auto now = std::chrono::steady_clock::now();
    if (progress) {
      idle_since = now;
    } else if (now - idle_since > 10s) {
      return;
    }
    std::this_thread::sleep_for(1ms);
  }
}

// Channels in both directions at once, half of them reliable, with message
// sizes that make most of them grow several times.
TEST_F(TunnelStressTest, ManyChannelsFlowBothWaysAtOnce) {
  constexpr int kChannelsPerSide = 8;
  constexpr uint32_t kMessages = 50;
  std::vector<Stream> streams;
  for (int from = 0; from < 2; from++) {
    for (int n = 0; n < kChannelsPerSide; n++) {
      Stream &stream = streams.emplace_back();
      stream.from = from;
      stream.reliable = n % 2 == 0;
      std::string name =
          absl::StrFormat("/stress/many/%s/%d", kDeviceNames[from], n);
      absl::StatusOr<Publisher> pub = publishers_[from].CreatePublisher(
          name, PublisherOptions()
                    .SetSlotSize(64 << (n % 4))
                    .SetNumSlots(8)
                    .SetReliable(stream.reliable));
      ASSERT_THAT(pub, IsOk()) << name;
      stream.pub.emplace(std::move(*pub));
      absl::StatusOr<Subscriber> sub = subscribers_[1 - from].CreateSubscriber(
          name, SubscriberOptions().SetReliable(stream.reliable));
      ASSERT_THAT(sub, IsOk()) << name;
      stream.sub.emplace(std::move(*sub));
    }
  }
  ASSERT_THAT(WaitForRoutes(streams), IsOk());
  const TunnelManager::Stats *stats[2] = {&servers_.Stats(0),
                                          &servers_.Stats(1)};
  const uint64_t sent_before[2] = {stats[0]->messages_sent,
                                   stats[1]->messages_sent};
  const uint64_t received_before[2] = {stats[0]->messages_received,
                                       stats[1]->messages_received};

  absl::Status published[2];
  {
    std::vector<std::thread> threads;
    for (int side = 0; side < 2; side++) {
      threads.emplace_back([&streams, &published, side]() {
        for (uint32_t i = 0; i < kMessages; i++) {
          for (size_t n = 0; n < streams.size(); n++) {
            Stream &stream = streams[n];
            if (stream.from != side) {
              continue;
            }
            size_t size = 16 + (i * 2654435761u + n * 40503u) % (32 * 1024);
            published[side] = Publish(*stream.pub, Numbered(i, size));
            if (!published[side].ok()) {
              return;
            }
          }
        }
      });
      threads.emplace_back(
          [&streams, side]() { ReadStreams(streams, side, kMessages - 1); });
    }
    for (std::thread &thread : threads) {
      thread.join();
    }
  }
  for (int side = 0; side < 2; side++) {
    ASSERT_THAT(published[side], IsOk()) << kDeviceNames[side];
  }
  for (const Stream &stream : streams) {
    if (stream.reliable) {
      EXPECT_TRUE(Consecutive(stream.received, 0, kMessages - 1))
          << stream.sub->Name();
    } else {
      EXPECT_TRUE(InOrderToTheEnd(stream.received, kMessages - 1))
          << stream.sub->Name();
    }
  }
  auto sent = [&](int side) {
    return stats[side]->messages_sent - sent_before[side];
  };
  auto received = [&](int side) {
    return stats[side]->messages_received - received_before[side];
  };
  Settle([&]() { return sent(0) == received(1) && sent(1) == received(0); });
  EXPECT_EQ(sent(0), received(1));
  EXPECT_EQ(sent(1), received(0));
}

// Publishes numbered messages on its own thread, about one a millisecond,
// until stopped.
class Streamer {
public:
  Streamer(Publisher &pub, uint32_t first)
      : next_(first), thread_([this, &pub]() { Run(pub); }) {}
  ~Streamer() { (void)Stop(); }

  // Returns the first failure, if there was one.
  absl::Status Stop() {
    stop_ = true;
    if (thread_.joinable()) {
      thread_.join();
    }
    return status_;
  }

  // The number the next message would have had.  Call after Stop.
  uint32_t Next() const { return next_; }

private:
  void Run(Publisher &pub) {
    while (!stop_) {
      const std::string data = Numbered(next_, 64 + next_ % 1024);
      absl::StatusOr<void *> buffer =
          pub.GetMessageBuffer(static_cast<SlotSizeType>(data.size()));
      if (!buffer.ok()) {
        status_ = buffer.status();
        return;
      }
      if (*buffer == nullptr) {
        // While the far server restarts, a reliable publisher can go
        // without a free slot for longer than Publish waits.
        (void)pub.Wait(100ms);
        continue;
      }
      memcpy(*buffer, data.data(), data.size());
      absl::StatusOr<const Message> published =
          pub.PublishMessage(static_cast<int64_t>(data.size()));
      if (!published.ok()) {
        status_ = published.status();
        return;
      }
      next_++;
      std::this_thread::sleep_for(1ms);
    }
  }

  std::atomic<bool> stop_{false};
  uint32_t next_;
  absl::Status status_;
  std::thread thread_;
};

// Reads `count` numbered messages, the first numbered after `after` and the
// rest consecutive.  Returns the last one's number.
//
// With `skip_history`, a new subscriber's history may come first, in order:
// probes and messages from the slots it found, up to `max_history` of them.
// A reliable publisher keeps reusing the lowest-numbered slot it can, so the
// others can hold messages from any time, numbered after `after` or not.
absl::StatusOr<int64_t> ReadConsecutive(Subscriber &sub, int64_t count,
                                        int64_t after,
                                        std::chrono::milliseconds first_wait,
                                        bool skip_history = false,
                                        int64_t max_history = 0) {
  int64_t last = after;
  // How many consecutive messages end with `last`.
  int64_t run = 0;
  int64_t history = 0;
  // The newest number read, with -1 for probes and -2 before any.
  int64_t newest = -2;
  while (run < count) {
    absl::StatusOr<std::string> data =
        Read(sub, run == 0 && history == 0 ? first_wait : 5s);
    if (!data.ok()) {
      return absl::Status(data.status().code(),
                          absl::StrFormat("message %d of %d after %d: %s", run,
                                          count, last,
                                          data.status().message()));
    }
    const bool probe = skip_history && *data == kProbe;
    const int64_t seq = probe ? -1 : SequenceOf(*data);
    if (seq < 0 && !probe) {
      return absl::DataLossError(
          absl::StrFormat("message %d, of %d bytes after number %d, is damaged",
                          run, data->size(), last));
    }
    if (run == 0 ? seq > after : seq == last + 1) {
      last = seq;
      run++;
    } else if (skip_history && (seq > newest || (probe && newest == -1))) {
      // Everything before this was history.
      history += run;
      run = 0;
      if (seq > after) {
        last = seq;
        run = 1;
      } else {
        history++;
      }
      if (history > max_history) {
        return absl::InternalError(absl::StrFormat(
            "more than %d messages of history came before number %d",
            max_history, seq));
      }
    } else {
      return absl::InternalError(absl::StrFormat(
          "message %d is number %d after number %d", run, seq, last));
    }
    newest = seq;
  }
  return last;
}

// The prefix of the names of the shared memory files process `pid` maps, or
// an empty string if it maps none.  Only Linux keeps them in /dev/shm.
std::string SharedMemoryPrefix(pid_t pid) {
#if defined(__linux__)
  constexpr std::string_view kShm = "/dev/shm/";
  constexpr std::string_view kSubspace = "subspace_";
  std::ifstream maps(absl::StrFormat("/proc/%d/maps", pid));
  std::string line;
  while (std::getline(maps, line)) {
    size_t name = line.find(absl::StrCat(kShm, kSubspace));
    if (name == std::string::npos) {
      continue;
    }
    name += kShm.size();
    // Names start with "subspace_SESSION_".
    size_t end = line.find('_', name + kSubspace.size());
    if (end != std::string::npos) {
      return line.substr(name, end + 1 - name);
    }
  }
#else
  (void)pid;
#endif
  return "";
}

// A subspace_server process running the tunnel, as a deployment would.  A
// server in this process couldn't stand in for one: when it stops, the
// connections its coroutines hold may stay open.
class ServerProcess {
public:
  ~ServerProcess() { Kill(); }

  void Start(const TunnelConfig &config) {
    config_ = config;
    const char *binary = getenv("SUBSPACE_SERVER");
    ASSERT_NE(binary, nullptr)
        << "set SUBSPACE_SERVER to the subspace_server binary";
    char socket_template[] = TUNNEL_TEST_TMP "/subspace_tunnelXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_template[0]));
    socket_ = &socket_template[0];
    char config_template[] = TUNNEL_TEST_TMP "/subspace_tunnel_configXXXXXX";
    int config_fd = mkstemp(&config_template[0]);
    ASSERT_NE(config_fd, -1);
    config_path_ = &config_template[0];
    std::string text;
    ASSERT_TRUE(google::protobuf::TextFormat::PrintToString(config, &text));
    ASSERT_EQ(::write(config_fd, text.data(), text.size()),
              static_cast<ssize_t>(text.size()));
    ::close(config_fd);

    std::vector<std::string> args = {binary,
                                     "--socket=" + socket_,
                                     "--local",
                                     "--tunnel",
                                     "--tunnel_config=" + config_path_,
                                     // Other tests' servers use it too.
                                     "--cleanup_filesystem=false",
                                     "--notify_fd=3"};
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
    args.push_back("--num_asio_threads=4");
#endif
    std::vector<char *> argv;
    for (std::string &arg : args) {
      argv.push_back(arg.data());
    }
    argv.push_back(nullptr);
    int ready[2];
    ASSERT_EQ(0, pipe(ready));
    long max_fd = sysconf(_SC_OPEN_MAX);
    if (max_fd < 0 || max_fd > 65536) {
      max_fd = 65536;
    }

    pid_ = fork();
    ASSERT_NE(pid_, -1);
    if (pid_ == 0) {
      // Only async-signal-safe calls until exec.  The server keeps nothing
      // of this process's but stdio and the ready pipe, so it holds none of
      // this process's connections open.
#if defined(__linux__)
      (void)prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
      if (ready[1] != 3) {
        (void)dup2(ready[1], 3);
      }
      for (int fd = 4; fd < max_fd; fd++) {
        (void)::close(fd);
      }
      execv(argv[0], argv.data());
      _exit(127);
    }
    ::close(ready[1]);
    // The server writes 8 bytes to the pipe once it's listening.
    struct pollfd pfd = {.fd = ready[0], .events = POLLIN, .revents = 0};
    int64_t value = 0;
    const bool started =
        ::poll(&pfd, 1, 10000) == 1 &&
        ::read(ready[0], &value, sizeof(value)) == sizeof(value);
    ::close(ready[0]);
    ASSERT_TRUE(started) << binary << " didn't start";
  }

  // Ends the server at once, as a crash would, and starts another with the
  // same configuration.  The new server has a new socket.
  void Restart() {
    Kill();
    Start(config_);
  }

  // Ends the server at once and removes its socket and configuration files.
  // On Linux this also removes the /dev/shm files it leaves behind; on
  // macOS and QNX the next server started with --cleanup_filesystem does.
  void Kill() {
    if (pid_ <= 0) {
      return;
    }
    std::string shm_prefix = SharedMemoryPrefix(pid_);
    ::kill(pid_, SIGKILL);
    int status;
    (void)waitpid(pid_, &status, 0);
    pid_ = -1;
#if defined(__linux__)
    std::error_code error;
    if (!shm_prefix.empty()) {
      for (const auto &entry :
           std::filesystem::directory_iterator("/dev/shm", error)) {
        if (entry.path().filename().string().rfind(shm_prefix, 0) == 0) {
          std::filesystem::remove(entry.path(), error);
        }
      }
    }
#endif
    (void)remove(socket_.c_str());
    (void)remove(config_path_.c_str());
  }

  const std::string &Socket() const { return socket_; }

private:
  TunnelConfig config_;
  pid_t pid_ = -1;
  std::string socket_;
  std::string config_path_;
};

// Server `i` of two that are each other's tunnel peers.
TunnelConfig PeerConfig(int i, const std::array<int, 2> &ports) {
  TunnelConfig config;
  config.set_device_name(kDeviceNames[i]);
  config.set_listen_address("127.0.0.1");
  config.set_listen_port(ports[i]);
  TunnelPeer *peer = config.add_peers();
  peer->set_name(kDeviceNames[1 - i]);
  peer->set_tcp_addr("127.0.0.1");
  peer->set_tcp_port(ports[1 - i]);
  return config;
}

// One server and then the other dies in the middle of a reliable stream and
// a new one takes its place.  Messages on their way to a server that dies are
// lost with it, but once messages flow again there are no gaps or duplicates.
TEST(TunnelRestartTest, ReliableStreamResumesAfterEitherServerRestarts) {
  signal(SIGPIPE, SIG_IGN);
  constexpr char kChannel[] = "/stress/restart";
  const PublisherOptions pub_options =
      PublisherOptions().SetSlotSize(256).SetNumSlots(16).SetReliable(true);
  const SubscriberOptions sub_options = SubscriberOptions().SetReliable(true);
  const std::array<int, 2> ports = FreePorts<2>();
  ASSERT_GT(ports[0], 0);
  ASSERT_GT(ports[1], 0);
  ServerProcess servers[2];
  for (int i = 0; i < 2; i++) {
    ASSERT_NO_FATAL_FAILURE(servers[i].Start(PeerConfig(i, ports)));
  }

  auto alpha = std::make_unique<Client>();
  ASSERT_THAT(alpha->Init(servers[0].Socket()), IsOk());
  absl::StatusOr<Publisher> created_pub =
      alpha->CreatePublisher(kChannel, pub_options);
  ASSERT_THAT(created_pub, IsOk());
  std::optional<Publisher> pub(std::move(*created_pub));
  auto beta = std::make_unique<Client>();
  ASSERT_THAT(beta->Init(servers[1].Socket()), IsOk());
  absl::StatusOr<Subscriber> created_sub =
      beta->CreateSubscriber(kChannel, sub_options);
  ASSERT_THAT(created_sub, IsOk());
  std::optional<Subscriber> sub(std::move(*created_sub));
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  auto streamer = std::make_unique<Streamer>(*pub, 0);
  absl::StatusOr<int64_t> last = ReadConsecutive(*sub, 200, -1, 5s);
  ASSERT_THAT(last, IsOk());

  // The receiving server dies while alpha keeps publishing.  Alpha's new
  // tunnel subscriber starts with whatever is still in alpha's slots, which
  // can be far older than the rest (see subscriber.cc), so the new beta gets
  // up to a channel's worth of old messages before the stream picks up.
  sub.reset();
  beta.reset();
  ASSERT_NO_FATAL_FAILURE(servers[1].Restart());
  beta = std::make_unique<Client>();
  ASSERT_THAT(beta->Init(servers[1].Socket()), IsOk());
  created_sub = beta->CreateSubscriber(kChannel, sub_options);
  ASSERT_THAT(created_sub, IsOk());
  sub.emplace(std::move(*created_sub));
  last = ReadConsecutive(*sub, 200, *last, 15s, /*skip_history=*/true,
                         /*max_history=*/pub_options.NumSlots());
  ASSERT_THAT(last, IsOk()) << "after beta restarted";

  // Then the sending server does, once everything it published has
  // arrived.  The new publisher continues the numbering.
  ASSERT_THAT(streamer->Stop(), IsOk());
  const uint32_t next = streamer->Next();
  streamer.reset();
  last = ReadConsecutive(*sub, next - 1 - *last, *last, 5s);
  ASSERT_THAT(last, IsOk()) << "before alpha restarted";
  pub.reset();
  alpha.reset();
  ASSERT_NO_FATAL_FAILURE(servers[0].Restart());
  alpha = std::make_unique<Client>();
  ASSERT_THAT(alpha->Init(servers[0].Socket()), IsOk());
  created_pub = alpha->CreatePublisher(kChannel, pub_options);
  ASSERT_THAT(created_pub, IsOk());
  pub.emplace(std::move(*created_pub));
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());
  streamer = std::make_unique<Streamer>(*pub, next);
  last = ReadConsecutive(*sub, 200, *last, 5s);
  ASSERT_THAT(last, IsOk()) << "after alpha restarted";
  EXPECT_EQ(*last, next + 199);
  EXPECT_THAT(streamer->Stop(), IsOk());
}

} // namespace
} // namespace subspace
