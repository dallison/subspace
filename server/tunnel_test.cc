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
#include "toolbelt/sockets.h"
#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <fcntl.h>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <netinet/in.h>
#include <signal.h>
#include <string>
#include <sys/file.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#if defined(__ANDROID__)
#define TUNNEL_TEST_TMP "/data/local/tmp"
#else
#define TUNNEL_TEST_TMP "/tmp"
#endif

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

  // `configure` can change server `i`'s configuration before it starts.
  void Start(const std::function<void(int i, TunnelConfig &)> &configure =
                 nullptr) {
    int ports[2] = {FreePort(), FreePort()};
    ASSERT_GT(ports[0], 0);
    ASSERT_GT(ports[1], 0);
    for (int i = 0; i < 2; i++) {
      char socket_template[] = TUNNEL_TEST_TMP "/subspace_tunnelXXXXXX"; // NOLINT
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
      if (configure) {
        configure(i, config);
      }

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

absl::Status Publish(Publisher &pub, const std::string &data,
                     const std::string &metadata = "") {
  absl::StatusOr<void *> buffer;
  for (;;) {
    buffer = pub.GetMessageBuffer(static_cast<SlotSizeType>(data.size()));
    if (!buffer.ok() || *buffer != nullptr) {
      break;
    }
    // A reliable publisher waits for a free slot.
    if (absl::Status s = pub.Wait(5s); !s.ok()) {
      return s;
    }
  }
  if (!buffer.ok()) {
    return buffer.status();
  }
  memcpy(*buffer, data.data(), data.size());
  if (!metadata.empty()) {
    absl::Span<std::byte> area = pub.GetMetadata();
    if (area.size() < metadata.size()) {
      pub.CancelPublish();
      return absl::InvalidArgumentError("metadata doesn't fit");
    }
    memcpy(area.data(), metadata.data(), metadata.size());
  }
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

// Native discovery and bridging connect alpha and beta while the tunnel
// connects beta and gamma.  Neither carries the other's traffic.
TEST(TunnelCoexistenceTest, NativeAndTunnelBridgingStaySeparate) {
  signal(SIGPIPE, SIG_IGN);
  // Native discovery broadcasts, so share the bridge tests' port lock.
  int lock_fd = ::open(TUNNEL_TEST_TMP "/subspace_bridge_test_port.lock",
                       O_CREAT | O_RDWR, 0666);
  ASSERT_NE(-1, lock_fd);
  ASSERT_EQ(0, ::flock(lock_fd, LOCK_EX));
  std::array<toolbelt::UDPSocket, 2> reserved;
  int disc_ports[2];
  for (int i = 0; i < 2; i++) {
    ASSERT_THAT(reserved[i].Bind(toolbelt::InetAddress::AnyAddress(0)), IsOk());
    disc_ports[i] = reserved[i].BoundAddress().Port();
  }
  int tunnel_ports[2] = {FreePort(), FreePort()};

  struct Instance {
    async::RuntimeEngine engine;
    std::string socket;
    int pipe[2];
    std::unique_ptr<Server> server;
    std::thread thread;
  };
  Instance instances[3];
  const char *kNames[3] = {"alpha", "beta", "gamma"};
  for (int i = 0; i < 3; i++) {
    Instance &instance = instances[i];
    char socket_template[] = TUNNEL_TEST_TMP "/subspace_tunnelXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_template[0]));
    instance.socket = &socket_template[0];
    ASSERT_EQ(0, pipe(instance.pipe));
    if (i < 2) {
      reserved[i].Close();
      instance.server = std::make_unique<Server>(
          instance.engine, instance.socket, "", disc_ports[i],
          disc_ports[1 - i], /*local=*/false, instance.pipe[1]);
    } else {
      instance.server = std::make_unique<Server>(
          instance.engine, instance.socket, "", 0, 0, /*local=*/true,
          instance.pipe[1]);
    }
    if (i > 0) {
      int self = i - 1;
      TunnelConfig config;
      config.set_device_name(kNames[i]);
      config.set_listen_address("127.0.0.1");
      config.set_listen_port(tunnel_ports[self]);
      TunnelPeer *peer = config.add_peers();
      peer->set_name(kNames[3 - i]);
      peer->set_tcp_addr("127.0.0.1");
      peer->set_tcp_port(tunnel_ports[1 - self]);
      instance.server->SetTunnelConfig(config);
    }
    instance.thread = std::thread([&instance]() {
      absl::Status s = instance.server->Run(/*num_asio_threads=*/4);
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });
    char buf[8];
    (void)::read(instance.pipe[0], buf, sizeof(buf));
  }
  ASSERT_EQ(0, ::flock(lock_fd, LOCK_UN));
  ::close(lock_fd);

  {
    Client clients[3];
    for (int i = 0; i < 3; i++) {
      ASSERT_THAT(clients[i].Init(instances[i].socket), IsOk());
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

  for (Instance &instance : instances) {
    instance.server->Stop();
    char buf[8];
    (void)::read(instance.pipe[0], buf, sizeof(buf));
    instance.thread.join();
    instance.server->CleanupAfterSession();
    (void)remove(instance.socket.c_str());
    ::close(instance.pipe[0]);
    ::close(instance.pipe[1]);
  }
}

} // namespace
} // namespace subspace
