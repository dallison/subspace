// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "common/async/runtime.h"
#include "proto/tunnel_config.pb.h"
#include "server/server.h"
#include "server/tunnel.h"
#include <arpa/inet.h>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <netinet/in.h>
#include <signal.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace subspace {
namespace {

using ::absl_testing::IsOk;
using namespace std::chrono_literals;

constexpr const char *kDeviceNames[2] = {"alpha", "beta"};

// Finds a free loopback TCP port.
int FreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t len = sizeof(addr);
  int port = -1;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0 &&
      ::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) == 0) {
    port = ntohs(addr.sin_port);
  }
  ::close(fd);
  return port;
}

// Two servers that know each other only through their tunnels.
class TunnelServers {
public:
  ~TunnelServers() { Stop(); }

  void Start() {
    int ports[2] = {FreePort(), FreePort()};
    ASSERT_GT(ports[0], 0);
    ASSERT_GT(ports[1], 0);
    for (int i = 0; i < 2; i++) {
      char socket_template[] = "/tmp/subspace_tunnelXXXXXX"; // NOLINT
      ::close(mkstemp(&socket_template[0]));
      socket_[i] = &socket_template[0];
      ASSERT_EQ(0, pipe(server_pipe_[i]));

      TunnelConfig config;
      config.set_device_name(kDeviceNames[i]);
      config.set_listen_address("127.0.0.1");
      config.set_listen_port(ports[i]);
      TunnelPeer *peer = config.add_peers();
      peer->set_name(kDeviceNames[1 - i]);
      peer->set_tcp_addr("127.0.0.1");
      peer->set_tcp_port(ports[1 - i]);

      // Local servers run no native discovery, so only the tunnel connects
      // them.
      server_[i] = std::make_unique<Server>(engine_[i], socket_[i], "", 0, 0,
                                            /*local=*/true, server_pipe_[i][1]);
      server_[i]->SetTunnelConfig(config);
      server_thread_[i] = std::thread([this, i]() {
        // The ingress and egress connections run on their own strands, so
        // several Asio threads exercise them concurrently.  The co backend
        // ignores the thread count.
        absl::Status s = server_[i]->Run(/*num_asio_threads=*/4);
        if (!s.ok()) {
          fprintf(stderr, "Error running Subspace server: %s\n",
                  s.ToString().c_str());
          exit(1);
        }
      });
      char buf[8];
      (void)::read(server_pipe_[i][0], buf, sizeof(buf));
    }
    running_ = true;
  }

  void Stop() {
    if (!running_) {
      return;
    }
    char buf[8];
    for (int i = 0; i < 2; i++) {
      server_[i]->Stop();
      (void)::read(server_pipe_[i][0], buf, sizeof(buf));
      server_thread_[i].join();
      server_[i]->CleanupAfterSession();
      (void)remove(socket_[i].c_str());
      ::close(server_pipe_[i][0]);
      ::close(server_pipe_[i][1]);
    }
    running_ = false;
  }

  const std::string &Socket(int i) const { return socket_[i]; }
  const TunnelManager::Stats &Stats(int i) const {
    return server_[i]->GetTunnelManager()->GetStats();
  }

private:
  async::RuntimeEngine engine_[2];
  std::string socket_[2];
  int server_pipe_[2][2];
  std::unique_ptr<Server> server_[2];
  std::thread server_thread_[2];
  bool running_ = false;
};

std::string Pattern(size_t size, int seed) {
  std::string data(size, '\0');
  for (size_t i = 0; i < size; i++) {
    data[i] = static_cast<char>((i * 31 + seed) % 251);
  }
  return data;
}

absl::Status Publish(Publisher &pub, const std::string &data) {
  absl::StatusOr<void *> buffer =
      pub.GetMessageBuffer(static_cast<SlotSizeType>(data.size()));
  if (!buffer.ok()) {
    return buffer.status();
  }
  memcpy(*buffer, data.data(), data.size());
  return pub.PublishMessage(static_cast<int64_t>(data.size())).status();
}

// Reads the next message's payload, waiting up to `timeout` for it.
absl::StatusOr<std::string> Read(Subscriber &sub,
                                 std::chrono::milliseconds timeout = 5s) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    absl::StatusOr<Message> msg = sub.ReadMessage();
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length > 0) {
      return std::string(static_cast<const char *>(msg->buffer), msg->length);
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return absl::DeadlineExceededError("no message arrived");
    }
    std::this_thread::sleep_for(2ms);
  }
}

// Publishes probes until one arrives, which shows the tunnel has a route for
// the channel, then discards any probes still on their way.
absl::Status WaitForRoute(Publisher &pub, Subscriber &sub) {
  auto deadline = std::chrono::steady_clock::now() + 15s;
  bool routed = false;
  while (!routed && std::chrono::steady_clock::now() < deadline) {
    if (absl::Status s = Publish(pub, "probe"); !s.ok()) {
      return s;
    }
    routed = Read(sub, 50ms).ok();
  }
  if (!routed) {
    return absl::DeadlineExceededError("the tunnel never carried a message");
  }
  while (Read(sub, 200ms).ok()) {
  }
  return absl::OkStatus();
}

class TunnelTest : public ::testing::Test {
protected:
  void SetUp() override {
    signal(SIGPIPE, SIG_IGN);
    servers_.Start();
    ASSERT_THAT(clients_[0].Init(servers_.Socket(0)), IsOk());
    ASSERT_THAT(clients_[1].Init(servers_.Socket(1)), IsOk());
  }

  TunnelServers servers_;
  Client clients_[2];
};

TEST_F(TunnelTest, MessagesCrossTheTunnel) {
  absl::StatusOr<Publisher> pub = clients_[0].CreatePublisher(
      "/tunnel_test/basic", PublisherOptions().SetSlotSize(256).SetNumSlots(16));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub =
      clients_[1].CreateSubscriber("/tunnel_test/basic");
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  uint64_t sent_before = servers_.Stats(0).messages_sent;
  for (int i = 0; i < 10; i++) {
    std::string data = absl::StrFormat("message %d", i);
    ASSERT_THAT(Publish(*pub, data), IsOk());
    absl::StatusOr<std::string> received = Read(*sub);
    ASSERT_THAT(received, IsOk());
    EXPECT_EQ(*received, data);
  }
  EXPECT_EQ(servers_.Stats(0).messages_sent - sent_before, 10u);
  EXPECT_GE(servers_.Stats(1).messages_received, 10u);
}

TEST_F(TunnelTest, ChannelsFlowBothWays) {
  absl::StatusOr<Publisher> pub_a =
      clients_[0].CreatePublisher("/tunnel_test/a", 256, 16);
  ASSERT_THAT(pub_a, IsOk());
  absl::StatusOr<Publisher> pub_b =
      clients_[1].CreatePublisher("/tunnel_test/b", 256, 16);
  ASSERT_THAT(pub_b, IsOk());
  absl::StatusOr<Subscriber> sub_a =
      clients_[1].CreateSubscriber("/tunnel_test/a");
  ASSERT_THAT(sub_a, IsOk());
  absl::StatusOr<Subscriber> sub_b =
      clients_[0].CreateSubscriber("/tunnel_test/b");
  ASSERT_THAT(sub_b, IsOk());
  ASSERT_THAT(WaitForRoute(*pub_a, *sub_a), IsOk());
  ASSERT_THAT(WaitForRoute(*pub_b, *sub_b), IsOk());

  ASSERT_THAT(Publish(*pub_a, "from alpha"), IsOk());
  ASSERT_THAT(Publish(*pub_b, "from beta"), IsOk());
  absl::StatusOr<std::string> a = Read(*sub_a);
  ASSERT_THAT(a, IsOk());
  EXPECT_EQ(*a, "from alpha");
  absl::StatusOr<std::string> b = Read(*sub_b);
  ASSERT_THAT(b, IsOk());
  EXPECT_EQ(*b, "from beta");
}

class TunnelResizeTest : public TunnelTest,
                         public ::testing::WithParamInterface<int> {};

// The receiving side starts with the sender's slot size and grows with the
// messages; nothing limits their size.
TEST_P(TunnelResizeTest, MessagesGrowPastTheSlotSize) {
  const int from = GetParam();
  const int to = 1 - from;
  absl::StatusOr<Publisher> pub = clients_[from].CreatePublisher(
      "/tunnel_test/resize", PublisherOptions().SetSlotSize(256).SetNumSlots(4));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub =
      clients_[to].CreateSubscriber("/tunnel_test/resize");
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  int seed = 0;
  for (size_t size : {size_t{1000}, size_t{64 * 1024}, size_t{4 << 20},
                      size_t{100}}) {
    std::string data = Pattern(size, ++seed);
    ASSERT_THAT(Publish(*pub, data), IsOk());
    absl::StatusOr<std::string> received = Read(*sub, 10s);
    ASSERT_THAT(received, IsOk()) << size << " bytes";
    ASSERT_EQ(received->size(), size);
    EXPECT_TRUE(*received == data) << size << " bytes";
  }
  EXPECT_GE(sub->SlotSize(), 4 << 20);
}

INSTANTIATE_TEST_SUITE_P(Directions, TunnelResizeTest, ::testing::Values(0, 1),
                         [](const ::testing::TestParamInfo<int> &info) {
                           return info.param == 0 ? "AlphaToBeta"
                                                  : "BetaToAlpha";
                         });

} // namespace
} // namespace subspace
