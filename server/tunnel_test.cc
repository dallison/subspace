// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "client/checksum.h"
#include "client/client.h"
#include "common/async/runtime.h"
#include "proto/tunnel_config.pb.h"
#include "server/server.h"
#include "server/tunnel.h"
#include "server/tunnel_test_util.h"
#include "toolbelt/sockets.h"
#include <array>
#include <chrono>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <optional>
#include <signal.h>
#include <string>
#include <sys/file.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace subspace {
namespace {

using ::absl_testing::IsOk;
using namespace std::chrono_literals;
using namespace tunnel_test;

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

// The receiving publisher runs out of slots while the subscriber holds them,
// so it waits; the messages after the wait need bigger slots.
TEST_F(TunnelTest, ReliableChannelResizesWhileAMessageWaits) {
  absl::StatusOr<Publisher> pub = clients_[0].CreatePublisher(
      "/tunnel_test/reliable",
      PublisherOptions().SetSlotSize(256).SetNumSlots(4).SetReliable(true));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = clients_[1].CreateSubscriber(
      "/tunnel_test/reliable", SubscriberOptions().SetReliable(true));
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  std::vector<std::string> sent;
  for (int i = 0; i < 10; i++) {
    sent.push_back(Pattern(size_t{100} << i, i));
    ASSERT_THAT(Publish(*pub, sent.back()), IsOk()) << i;
  }
  // Give the tunnel time to fill the receiving channel and start waiting.
  std::this_thread::sleep_for(300ms);
  for (size_t i = 0; i < sent.size(); i++) {
    absl::StatusOr<std::string> received = Read(*sub);
    ASSERT_THAT(received, IsOk()) << i;
    ASSERT_EQ(received->size(), sent[i].size()) << i;
    EXPECT_TRUE(*received == sent[i]) << i;
  }
}

TEST_F(TunnelTest, ChecksumsAndMetadataSurvive) {
  absl::StatusOr<Publisher> pub = clients_[0].CreatePublisher(
      "/tunnel_test/checksum", PublisherOptions()
                                   .SetSlotSize(256)
                                   .SetNumSlots(8)
                                   .SetChecksum(true)
                                   .SetMetadataSize(24));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = clients_[1].CreateSubscriber(
      "/tunnel_test/checksum", SubscriberOptions().SetChecksum(true));
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  for (int i = 0; i < 3; i++) {
    std::string data = Pattern(300 * (i + 1), i);
    std::string metadata = absl::StrFormat("metadata %d", i);
    ASSERT_THAT(Publish(*pub, data, metadata), IsOk());
    // ReadMessage fails if the checksum doesn't verify.
    absl::StatusOr<std::string> received = Read(*sub);
    ASSERT_THAT(received, IsOk()) << i;
    EXPECT_TRUE(*received == data) << i;
    absl::Span<const std::byte> area = sub->GetMetadata();
    ASSERT_EQ(area.size(), 24u);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(area.data()),
                          metadata.size()),
              metadata);
  }
}

// The receiving server numbers its virtual channels independently.  The
// checksum is one the tunnel can't compute, so it must arrive as it was sent.
TEST_F(TunnelTest, VirtualChannelsCross) {
  auto custom_checksum = [](const std::array<absl::Span<const uint8_t>, 3> &data,
                            absl::Span<std::byte> checksum) {
    uint32_t crc = 0x5eed;
    for (const absl::Span<const uint8_t> &span : data) {
      crc = SubspaceCRC32(crc, span.data(), span.size());
    }
    memcpy(checksum.data(), &crc, sizeof(crc));
  };
  absl::StatusOr<Publisher> other = clients_[1].CreatePublisher(
      "/tunnel_test/vother", PublisherOptions()
                                 .SetSlotSize(256)
                                 .SetNumSlots(8)
                                 .SetMux("/tunnel_test/mux"));
  ASSERT_THAT(other, IsOk());
  absl::StatusOr<Publisher> pub = clients_[0].CreatePublisher(
      "/tunnel_test/virtual", PublisherOptions()
                                  .SetSlotSize(256)
                                  .SetNumSlots(8)
                                  .SetMux("/tunnel_test/mux")
                                  .SetVchanId(7)
                                  .SetChecksum(true));
  ASSERT_THAT(pub, IsOk());
  pub->SetChecksumCallback(custom_checksum);
  absl::StatusOr<Subscriber> sub = clients_[1].CreateSubscriber(
      "/tunnel_test/virtual",
      SubscriberOptions().SetMux("/tunnel_test/mux").SetChecksum(true));
  ASSERT_THAT(sub, IsOk());
  sub->SetChecksumCallback(custom_checksum);
  ASSERT_NE(sub->VirtualChannelId(), 7);
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  ASSERT_THAT(Publish(*pub, "virtual"), IsOk());
  absl::StatusOr<Message> msg;
  for (int i = 0; i < 1000; i++) {
    msg = sub->ReadMessage();
    if (!msg.ok() || msg->length > 0) {
      break;
    }
    std::this_thread::sleep_for(5ms);
  }
  ASSERT_THAT(msg, IsOk());
  ASSERT_EQ(msg->length, 7u);
  EXPECT_EQ(std::string(static_cast<const char *>(msg->buffer), msg->length),
            "virtual");
  EXPECT_EQ(msg->vchan_id, sub->VirtualChannelId());
}

// Messages that arrived over the tunnel aren't sent back.
TEST_F(TunnelTest, MessagesAreNotEchoed) {
  const char *kChannel = "/tunnel_test/echo";
  absl::StatusOr<Publisher> pubs[2] = {
      clients_[0].CreatePublisher(kChannel, 256, 16),
      clients_[1].CreatePublisher(kChannel, 256, 16)};
  ASSERT_THAT(pubs[0], IsOk());
  ASSERT_THAT(pubs[1], IsOk());
  absl::StatusOr<Subscriber> subs[2] = {clients_[0].CreateSubscriber(kChannel),
                                        clients_[1].CreateSubscriber(kChannel)};
  ASSERT_THAT(subs[0], IsOk());
  ASSERT_THAT(subs[1], IsOk());
  ASSERT_THAT(WaitForRoute(*pubs[0], *subs[1]), IsOk());
  // Discard alpha's probes, which its own subscriber also saw.
  while (Read(*subs[0], 200ms).ok()) {
  }
  ASSERT_THAT(WaitForRoute(*pubs[1], *subs[0]), IsOk());
  while (Read(*subs[1], 200ms).ok()) {
  }

  for (int from = 0; from < 2; from++) {
    std::string data = absl::StrFormat("from %s", kDeviceNames[from]);
    ASSERT_THAT(Publish(*pubs[from], data), IsOk());
    for (int i = 0; i < 2; i++) {
      absl::StatusOr<std::string> received = Read(*subs[i]);
      ASSERT_THAT(received, IsOk()) << from << " to " << i;
      EXPECT_EQ(*received, data);
    }
    for (int i = 0; i < 2; i++) {
      EXPECT_FALSE(Read(*subs[i], 500ms).ok()) << from << " echoed to " << i;
    }
  }
}

// With discovery off, only the static routes carry channels.
TEST(TunnelStaticRouteTest, RoutesWorkWithoutDiscovery) {
  signal(SIGPIPE, SIG_IGN);
  TunnelServers servers;
  servers.Start([](int i, TunnelConfig &config) {
    config.set_disable_discovery(true);
    if (i == 0) {
      TunnelSendRoute *route = config.add_send_routes();
      route->set_topic("/static/out");
      route->add_peers(kDeviceNames[1]);
    } else {
      TunnelReceiveRoute *route = config.add_receive_routes();
      route->set_topic("/static/out");
      route->add_peers(kDeviceNames[0]);
      route->set_mux("/static/mux");
      route->set_vchan_id(6);
    }
  });
  Client clients[2];
  ASSERT_THAT(clients[0].Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(clients[1].Init(servers.Socket(1)), IsOk());

  absl::StatusOr<Publisher> pub =
      clients[0].CreatePublisher("/static/out", 256, 16);
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Publisher> unrouted =
      clients[0].CreatePublisher("/static/unrouted", 256, 16);
  ASSERT_THAT(unrouted, IsOk());
  absl::StatusOr<Subscriber> unrouted_sub =
      clients[1].CreateSubscriber("/static/unrouted");
  ASSERT_THAT(unrouted_sub, IsOk());

  // Subscribing before the tunnel creates the channel on beta would choose
  // the virtual channel there.
  auto deadline = std::chrono::steady_clock::now() + 15s;
  while (servers.Stats(1).messages_received == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    ASSERT_THAT(Publish(*pub, "probe"), IsOk());
    std::this_thread::sleep_for(50ms);
  }
  ASSERT_GT(servers.Stats(1).messages_received, 0u);
  absl::StatusOr<Subscriber> sub = clients[1].CreateSubscriber(
      "/static/out", SubscriberOptions().SetMux("/static/mux"));
  ASSERT_THAT(sub, IsOk());
  EXPECT_EQ(sub->VirtualChannelId(), 6);
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  ASSERT_THAT(Publish(*pub, "routed"), IsOk());
  absl::StatusOr<std::string> received = Read(*sub);
  ASSERT_THAT(received, IsOk());
  EXPECT_EQ(*received, "routed");
  ASSERT_THAT(Publish(*unrouted, "unrouted"), IsOk());
  EXPECT_FALSE(Read(*unrouted_sub, 1s).ok());
  for (int i = 0; i < 2; i++) {
    EXPECT_EQ(servers.Stats(i).discovery_sent, 0u) << i;
    EXPECT_EQ(servers.Stats(i).discovery_received, 0u) << i;
  }
}

// A server that can run native discovery and bridging, the tunnel, or both.
class MixedServer {
public:
  ~MixedServer() { Stop(); }

  // Native discovery and bridging run unless `discovery_port` is zero, and
  // talk to the server on `peer_discovery_port`.
  void Start(int discovery_port, int peer_discovery_port,
             const std::optional<TunnelConfig> &tunnel) {
    char socket_template[] = TUNNEL_TEST_TMP "/subspace_tunnelXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_template[0]));
    socket_ = &socket_template[0];
    ASSERT_EQ(0, pipe(pipe_));
    server_ = std::make_unique<Server>(
        engine_, socket_, "", discovery_port, peer_discovery_port,
        /*local=*/discovery_port == 0, pipe_[1]);
    if (tunnel.has_value()) {
      server_->SetTunnelConfig(*tunnel);
    }
    Server *server = server_.get();
    thread_ = std::thread([server]() {
      absl::Status s = server->Run(/*num_asio_threads=*/4);
      if (!s.ok()) {
        // exit() can hang while other threads are running.
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        _exit(1);
      }
    });
    char buf[8];
    (void)::read(pipe_[0], buf, sizeof(buf));
  }

  void Stop() {
    if (server_ == nullptr) {
      return;
    }
    server_->Stop();
    char buf[8];
    (void)::read(pipe_[0], buf, sizeof(buf));
    thread_.join();
    server_->CleanupAfterSession();
    server_.reset();
    (void)remove(socket_.c_str());
    ::close(pipe_[0]);
    ::close(pipe_[1]);
  }

  const std::string &Socket() const { return socket_; }
  const TunnelManager::Stats &Stats() const {
    return server_->GetTunnelManager()->GetStats();
  }

private:
  async::RuntimeEngine engine_;
  std::string socket_;
  int pipe_[2] = {-1, -1};
  std::unique_ptr<Server> server_;
  std::thread thread_;
};

// Native discovery broadcasts, so servers that run it start while holding
// the bridge tests' port lock.
class NativeDiscoveryPorts {
public:
  ~NativeDiscoveryPorts() { Unlock(); }

  // Takes the lock and reserves two free UDP ports.
  void Lock() {
    lock_fd_ = ::open(TUNNEL_TEST_TMP "/subspace_bridge_test_port.lock",
                      O_CREAT | O_RDWR, 0666);
    ASSERT_NE(-1, lock_fd_);
    ASSERT_EQ(0, ::flock(lock_fd_, LOCK_EX));
    for (int i = 0; i < 2; i++) {
      ASSERT_THAT(reserved_[i].Bind(toolbelt::InetAddress::AnyAddress(0)),
                  IsOk());
      ports_[i] = reserved_[i].BoundAddress().Port();
    }
  }

  // Frees port `i` for the server about to bind it.
  int Take(int i) {
    reserved_[i].Close();
    return ports_[i];
  }
  int Port(int i) const { return ports_[i]; }

  void Unlock() {
    if (lock_fd_ < 0) {
      return;
    }
    (void)::flock(lock_fd_, LOCK_UN);
    ::close(lock_fd_);
    lock_fd_ = -1;
  }

private:
  int lock_fd_ = -1;
  std::array<toolbelt::UDPSocket, 2> reserved_;
  int ports_[2] = {0, 0};
};

TunnelConfig TunnelTo(const char *name, int port, const char *peer_name,
                      int peer_port) {
  TunnelConfig config;
  config.set_device_name(name);
  config.set_listen_address("127.0.0.1");
  config.set_listen_port(port);
  TunnelPeer *peer = config.add_peers();
  peer->set_name(peer_name);
  peer->set_tcp_addr("127.0.0.1");
  peer->set_tcp_port(peer_port);
  return config;
}

// Native discovery and bridging connect alpha and beta while the tunnel
// connects beta and gamma.  Neither carries the other's traffic.
TEST(TunnelCoexistenceTest, NativeAndTunnelBridgingStaySeparate) {
  signal(SIGPIPE, SIG_IGN);
  const std::array<int, 2> tunnel_ports = FreePorts<2>();
  MixedServer servers[3];
  {
    NativeDiscoveryPorts native;
    ASSERT_NO_FATAL_FAILURE(native.Lock());
    ASSERT_NO_FATAL_FAILURE(
        servers[0].Start(native.Take(0), native.Port(1), std::nullopt));
    ASSERT_NO_FATAL_FAILURE(servers[1].Start(
        native.Take(1), native.Port(0),
        TunnelTo("beta", tunnel_ports[0], "gamma", tunnel_ports[1])));
  }
  ASSERT_NO_FATAL_FAILURE(servers[2].Start(
      0, 0, TunnelTo("gamma", tunnel_ports[1], "beta", tunnel_ports[0])));

  Client clients[3];
  for (int i = 0; i < 3; i++) {
    ASSERT_THAT(clients[i].Init(servers[i].Socket()), IsOk());
  }
  // Published natively on alpha, wanted on beta and gamma.
  absl::StatusOr<Publisher> native =
      clients[0].CreatePublisher("/coexist/native", 256, 16);
  ASSERT_THAT(native, IsOk());
  absl::StatusOr<Subscriber> native_subs[2] = {
      clients[1].CreateSubscriber("/coexist/native"),
      clients[2].CreateSubscriber("/coexist/native")};
  ASSERT_THAT(native_subs[0], IsOk());
  ASSERT_THAT(native_subs[1], IsOk());

  // Published on gamma, wanted on beta and alpha.
  absl::StatusOr<Publisher> tunneled =
      clients[2].CreatePublisher("/coexist/tunnel", 256, 16);
  ASSERT_THAT(tunneled, IsOk());
  absl::StatusOr<Subscriber> tunnel_subs[2] = {
      clients[1].CreateSubscriber("/coexist/tunnel"),
      clients[0].CreateSubscriber("/coexist/tunnel")};
  ASSERT_THAT(tunnel_subs[0], IsOk());
  ASSERT_THAT(tunnel_subs[1], IsOk());

  // Published on beta, wanted on both sides.
  absl::StatusOr<Publisher> both =
      clients[1].CreatePublisher("/coexist/both", 256, 16);
  ASSERT_THAT(both, IsOk());
  absl::StatusOr<Subscriber> both_subs[2] = {
      clients[0].CreateSubscriber("/coexist/both"),
      clients[2].CreateSubscriber("/coexist/both")};
  ASSERT_THAT(both_subs[0], IsOk());
  ASSERT_THAT(both_subs[1], IsOk());

  ASSERT_THAT(WaitForRoute(*native, *native_subs[0]), IsOk());
  ASSERT_THAT(WaitForRoute(*tunneled, *tunnel_subs[0]), IsOk());
  ASSERT_THAT(WaitForRoute(*both, *both_subs[0]), IsOk());
  ASSERT_THAT(WaitForRoute(*both, *both_subs[1]), IsOk());
  // Probes may have crossed where they shouldn't; those are checked below.
  for (auto *subs : {native_subs, tunnel_subs, both_subs}) {
    for (int i = 0; i < 2; i++) {
      while (Read(*subs[i], 200ms).ok()) {
      }
    }
  }

  ASSERT_THAT(Publish(*native, "native"), IsOk());
  ASSERT_THAT(Publish(*tunneled, "tunnel"), IsOk());
  ASSERT_THAT(Publish(*both, "both"), IsOk());
  absl::StatusOr<std::string> received = Read(*native_subs[0]);
  ASSERT_THAT(received, IsOk());
  EXPECT_EQ(*received, "native");
  received = Read(*tunnel_subs[0]);
  ASSERT_THAT(received, IsOk());
  EXPECT_EQ(*received, "tunnel");
  for (int i = 0; i < 2; i++) {
    received = Read(*both_subs[i]);
    ASSERT_THAT(received, IsOk()) << i;
    EXPECT_EQ(*received, "both");
  }
  EXPECT_FALSE(Read(*native_subs[1], 1s).ok())
      << "the tunnel carried a natively bridged channel";
  EXPECT_FALSE(Read(*tunnel_subs[1], 1s).ok())
      << "native bridging carried a tunneled channel";
  for (int i = 0; i < 2; i++) {
    EXPECT_FALSE(Read(*both_subs[i], 200ms).ok()) << "duplicate on " << i;
  }
}

// Publishes probes until one arrives twice, which shows that both native
// bridging and the tunnel carry the channel.
absl::Status WaitForBothPaths(Publisher &pub, Subscriber &sub) {
  auto deadline = std::chrono::steady_clock::now() + 20s;
  std::map<std::string, int> copies;
  for (int n = 0; std::chrono::steady_clock::now() < deadline; n++) {
    if (absl::Status s = Publish(pub, absl::StrFormat("probe %d", n));
        !s.ok()) {
      return s;
    }
    for (;;) {
      absl::StatusOr<std::string> probe = Read(sub, 50ms);
      if (!probe.ok()) {
        break;
      }
      if (++copies[*probe] == 2) {
        return absl::OkStatus();
      }
    }
  }
  return absl::DeadlineExceededError("no probe arrived over both paths");
}

// Native bridging and the tunnel both connect alpha and beta.  Each carries
// every channel, so subscribers get every message twice; docs/tunnel.md says
// not to connect servers this way.  Neither forwards what the other
// delivered, so nothing loops.
TEST(TunnelCoexistenceTest, BothPathsBetweenTwoServersDeliverTwiceButDontLoop) {
  signal(SIGPIPE, SIG_IGN);
  const std::array<int, 2> tunnel_ports = FreePorts<2>();
  MixedServer servers[2];
  {
    NativeDiscoveryPorts native;
    ASSERT_NO_FATAL_FAILURE(native.Lock());
    for (int i = 0; i < 2; i++) {
      ASSERT_NO_FATAL_FAILURE(servers[i].Start(
          native.Take(i), native.Port(1 - i),
          TunnelTo(kDeviceNames[i], tunnel_ports[i], kDeviceNames[1 - i],
                   tunnel_ports[1 - i])));
    }
  }

  Client clients[2];
  for (int i = 0; i < 2; i++) {
    ASSERT_THAT(clients[i].Init(servers[i].Socket()), IsOk());
  }
  const char *kChannel = "/coexist/twice";
  absl::StatusOr<Publisher> pubs[2] = {
      clients[0].CreatePublisher(kChannel, 256, 16),
      clients[1].CreatePublisher(kChannel, 256, 16)};
  ASSERT_THAT(pubs[0], IsOk());
  ASSERT_THAT(pubs[1], IsOk());
  absl::StatusOr<Subscriber> subs[2] = {clients[0].CreateSubscriber(kChannel),
                                        clients[1].CreateSubscriber(kChannel)};
  ASSERT_THAT(subs[0], IsOk());
  ASSERT_THAT(subs[1], IsOk());
  ASSERT_THAT(WaitForBothPaths(*pubs[0], *subs[1]), IsOk());
  ASSERT_THAT(WaitForBothPaths(*pubs[1], *subs[0]), IsOk());
  // Each subscriber also saw its own server's probes.
  for (int i = 0; i < 2; i++) {
    while (Read(*subs[i], 200ms).ok()) {
    }
  }

  for (int from = 0; from < 2; from++) {
    const int to = 1 - from;
    const uint64_t sent_before[2] = {servers[0].Stats().messages_sent,
                                     servers[1].Stats().messages_sent};
    std::string data = absl::StrFormat("from %s", kDeviceNames[from]);
    ASSERT_THAT(Publish(*pubs[from], data), IsOk());
    for (int copy = 0; copy < 2; copy++) {
      absl::StatusOr<std::string> received = Read(*subs[to]);
      ASSERT_THAT(received, IsOk()) << "copy " << copy << " on " << to;
      EXPECT_EQ(*received, data);
    }
    absl::StatusOr<std::string> local = Read(*subs[from]);
    ASSERT_THAT(local, IsOk());
    EXPECT_EQ(*local, data);
    for (int i = 0; i < 2; i++) {
      EXPECT_FALSE(Read(*subs[i], 1s).ok())
          << data << " arrived again on " << kDeviceNames[i];
    }
    EXPECT_EQ(servers[from].Stats().messages_sent - sent_before[from], 1u);
    EXPECT_EQ(servers[to].Stats().messages_sent - sent_before[to], 0u);
  }
}

} // namespace
} // namespace subspace
