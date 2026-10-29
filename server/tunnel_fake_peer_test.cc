// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

// Tests the tunnel against a peer that speaks the wire format directly, the
// way a cruise device does.

#include "absl/status/status_matchers.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "client/checksum.h"
#include "client/client.h"
#include "common/async/runtime.h"
#include "common/channel.h"
#include "proto/tunnel_config.pb.h"
#include "proto/tunnel_discovery.pb.h"
#include "proto/tunnel_handshake.pb.h"
#include "server/server.h"
#include "server/tunnel.h"
#include "server/tunnel_protocol.h"
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#if defined(__ANDROID__)
#define TUNNEL_TEST_TMP "/data/local/tmp"
#else
#define TUNNEL_TEST_TMP "/tmp"
#endif

namespace subspace {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using namespace std::chrono_literals;
using tunnel::Discovery;
using tunnel::DiscoveryTopic;
using tunnel::Handshake;
using tunnel::PacketHeader;
using tunnel::PacketType;
using Clock = std::chrono::steady_clock;

constexpr const char *kPeerName = "cruise";

sockaddr_in Loopback(int port) {
  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  return addr;
}

int FreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr = Loopback(0);
  socklen_t len = sizeof(addr);
  int port = -1;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0 &&
      ::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) == 0) {
    port = ntohs(addr.sin_port);
  }
  ::close(fd);
  return port;
}

bool WaitReadable(int fd, Clock::time_point deadline) {
  for (;;) {
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - Clock::now());
    if (remaining.count() < 0) {
      return false;
    }
    pollfd p = {.fd = fd, .events = POLLIN, .revents = 0};
    int n = ::poll(&p, 1, static_cast<int>(remaining.count()));
    if (n > 0) {
      return true;
    }
    if (n == 0 || errno != EINTR) {
      return false;
    }
  }
}

struct Packet {
  PacketHeader header;
  std::string body;
};

// One end of a tunnel connection.
class WireConnection {
public:
  explicit WireConnection(int fd) : fd_(fd) {}
  ~WireConnection() { Close(); }
  WireConnection(const WireConnection &) = delete;
  WireConnection &operator=(const WireConnection &) = delete;

  static absl::StatusOr<std::unique_ptr<WireConnection>> Connect(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr = Loopback(port);
    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) !=
        0) {
      ::close(fd);
      return absl::UnavailableError(
          absl::StrFormat("connect to port %d: %s", port, strerror(errno)));
    }
    return std::make_unique<WireConnection>(fd);
  }

  absl::Status SendRaw(std::string_view bytes) {
    while (!bytes.empty()) {
      ssize_t n = ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
      if (n < 0 && errno == EINTR) {
        continue;
      }
      if (n <= 0) {
        return absl::InternalError(strerror(errno));
      }
      bytes.remove_prefix(static_cast<size_t>(n));
    }
    return absl::OkStatus();
  }

  absl::Status Send(PacketType type, uint64_t seq, std::string_view body) {
    absl::StatusOr<PacketHeader> header = tunnel::MakePacketHeader(
        type, body.size(), seq, tunnel::RealtimeNowNs());
    if (!header.ok()) {
      return header.status();
    }
    std::string packet(tunnel::kPacketHeaderSize, '\0');
    tunnel::EncodePacketHeader(*header,
                               reinterpret_cast<std::byte *>(packet.data()));
    packet.append(body);
    return SendRaw(packet);
  }

  absl::Status SendHandshake(const Handshake &handshake) {
    return Send(PacketType::kHandshake, 0, handshake.SerializeAsString());
  }

  absl::StatusOr<Packet> Read(std::chrono::milliseconds timeout = 5s) {
    Clock::time_point deadline = Clock::now() + timeout;
    std::array<std::byte, tunnel::kPacketHeaderSize> header_bytes;
    if (absl::Status s = ReadExactly(header_bytes.data(), header_bytes.size(),
                                     deadline);
        !s.ok()) {
      return s;
    }
    absl::StatusOr<PacketHeader> header =
        tunnel::DecodePacketHeader(header_bytes);
    if (!header.ok()) {
      return header.status();
    }
    Packet packet{.header = *header,
                  .body = std::string(header->BodySize(), '\0')};
    if (absl::Status s =
            ReadExactly(packet.body.data(), packet.body.size(), deadline);
        !s.ok()) {
      return s;
    }
    return packet;
  }

  // True if the other end closes the connection within `timeout`.
  bool ClosedWithin(std::chrono::milliseconds timeout) {
    Clock::time_point deadline = Clock::now() + timeout;
    char buffer[4096];
    while (WaitReadable(fd_, deadline)) {
      ssize_t n = ::recv(fd_, buffer, sizeof(buffer), 0);
      if (n == 0 || (n < 0 && errno != EINTR)) {
        return true;
      }
    }
    return false;
  }

  void Close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

private:
  absl::Status ReadExactly(void *buffer, size_t length,
                           Clock::time_point deadline) {
    char *p = static_cast<char *>(buffer);
    while (length > 0) {
      if (!WaitReadable(fd_, deadline)) {
        return absl::DeadlineExceededError("timed out reading");
      }
      ssize_t n = ::recv(fd_, p, length, 0);
      if (n < 0 && errno == EINTR) {
        continue;
      }
      if (n == 0) {
        return absl::AbortedError("connection closed");
      }
      if (n < 0) {
        return absl::InternalError(strerror(errno));
      }
      p += n;
      length -= static_cast<size_t>(n);
    }
    return absl::OkStatus();
  }

  int fd_;
};

struct Accepted {
  Handshake handshake;
  std::unique_ptr<WireConnection> connection;
};

// Listens where the server expects its peer.
class FakePeer {
public:
  ~FakePeer() {
    if (listen_fd_ >= 0) {
      ::close(listen_fd_);
    }
  }

  absl::Status Listen() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    (void)::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr = Loopback(0);
    socklen_t len = sizeof(addr);
    if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr),
               sizeof(addr)) != 0 ||
        ::listen(listen_fd_, 64) != 0 ||
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len) !=
            0) {
      return absl::InternalError(strerror(errno));
    }
    port_ = ntohs(addr.sin_port);
    return absl::OkStatus();
  }

  int Port() const { return port_; }

  // Accepts the server's connections until one is for `topic`.  Connections
  // for other topics are kept for later calls.
  absl::StatusOr<Accepted> Accept(std::string_view topic,
                                  std::chrono::milliseconds timeout = 10s) {
    if (auto it = pending_.find(std::string(topic)); it != pending_.end()) {
      Accepted accepted = std::move(it->second);
      pending_.erase(it);
      return accepted;
    }
    Clock::time_point deadline = Clock::now() + timeout;
    for (;;) {
      if (!WaitReadable(listen_fd_, deadline)) {
        return absl::DeadlineExceededError(
            absl::StrFormat("the server never connected for %s", topic));
      }
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        continue;
      }
      auto connection = std::make_unique<WireConnection>(fd);
      absl::StatusOr<Packet> packet = connection->Read();
      if (!packet.ok()) {
        return packet.status();
      }
      if (packet->header.type != PacketType::kHandshake ||
          packet->header.seq != 0) {
        return absl::InvalidArgumentError(
            "the first packet isn't a handshake");
      }
      Accepted accepted{.connection = std::move(connection)};
      if (!accepted.handshake.ParseFromString(packet->body)) {
        return absl::InvalidArgumentError("malformed handshake");
      }
      if (accepted.handshake.topic_name() == topic) {
        return accepted;
      }
      pending_.insert_or_assign(accepted.handshake.topic_name(),
                                std::move(accepted));
    }
  }

private:
  int listen_fd_ = -1;
  int port_ = 0;
  std::map<std::string, Accepted> pending_;
};

Handshake DiscoveryHandshake() {
  Handshake handshake;
  handshake.set_topic_name(tunnel::kDiscoveryChannelName);
  tunnel::SubspaceTransport *transport = handshake.mutable_subspace();
  transport->set_slot_size(256);
  transport->set_num_slots(24);
  transport->set_vchan_id(-1);
  return handshake;
}

// A discovery message listing (topic, datatype) pairs.
Discovery MakeDiscovery(
    const std::string &device_name,
    const std::vector<std::pair<std::string, std::string>> &published,
    const std::vector<std::pair<std::string, std::string>> &subscribed) {
  Discovery msg;
  msg.set_device_name(device_name);
  for (const auto &[topic, datatype] : published) {
    DiscoveryTopic *info = msg.add_published_topics();
    info->set_topic(topic);
    info->set_datatype(datatype);
    info->set_vchan_id(-1);
  }
  for (const auto &[topic, datatype] : subscribed) {
    DiscoveryTopic *info = msg.add_subscribed_topics();
    info->set_topic(topic);
    info->set_datatype(datatype);
  }
  return msg;
}

std::string DiscoveryBody(const std::string &payload, uint64_t ordinal) {
  MessagePrefix prefix = {};
  prefix.message_size = payload.size();
  prefix.ordinal = ordinal;
  prefix.vchan_id = -1;
  prefix.checksum_size = 4;
  std::string body(reinterpret_cast<const char *>(&prefix), sizeof(prefix));
  body.append(payload);
  return body;
}

absl::StatusOr<Discovery> ParseDiscovery(const Packet &packet) {
  if (packet.header.type != PacketType::kData) {
    return absl::InvalidArgumentError("not a data packet");
  }
  if (packet.body.size() < sizeof(MessagePrefix)) {
    return absl::InvalidArgumentError("no prefix");
  }
  MessagePrefix prefix;
  memcpy(&prefix, packet.body.data(), sizeof(prefix));
  if (prefix.message_size != packet.body.size() - sizeof(prefix)) {
    return absl::InvalidArgumentError("message size doesn't match the packet");
  }
  Discovery msg;
  if (!msg.ParseFromArray(packet.body.data() + sizeof(prefix),
                          static_cast<int>(packet.body.size() - sizeof(prefix)))) {
    return absl::InvalidArgumentError("discovery message doesn't decode");
  }
  return msg;
}

void Checksum(MessagePrefix *prefix, std::string_view payload) {
  auto data =
      GetMessageChecksumData(prefix, payload.data(), payload.size(), 4, 0);
  CalculateCRC32Checksum(data.Spans(), GetChecksumSpan(prefix, 4));
}

// A checksum the tunnel can't compute: CRC32 with a different seed.
void CustomChecksum(const std::array<absl::Span<const uint8_t>, 3> &data,
                    absl::Span<std::byte> checksum) {
  uint32_t crc = 0x5eed;
  for (const absl::Span<const uint8_t> &span : data) {
    crc = SubspaceCRC32(crc, span.data(), span.size());
  }
  memcpy(checksum.data(), &crc, sizeof(crc));
}

// A data packet body the way a cruise publisher sends it.
std::string CruiseDataBody(std::string_view payload, uint64_t ordinal,
                           bool checksum, int64_t flags = 0) {
  MessagePrefix prefix = {};
  prefix.slot_id = 5;
  prefix.message_size = payload.size();
  prefix.ordinal = ordinal;
  prefix.timestamp = 1;
  prefix.flags = flags;
  prefix.vchan_id = -1;
  prefix.checksum_size = 4;
  if (checksum) {
    prefix.flags |= kMessageHasChecksum;
    Checksum(&prefix, payload);
  }
  std::string body(reinterpret_cast<const char *>(&prefix), sizeof(prefix));
  body.append(payload);
  return body;
}

std::string Pattern(size_t size, int seed) {
  std::string data(size, '\0');
  for (size_t i = 0; i < size; i++) {
    data[i] = static_cast<char>((i * 31 + seed) % 251);
  }
  return data;
}

absl::StatusOr<std::string> Read(Subscriber &sub,
                                 std::chrono::milliseconds timeout = 5s) {
  Clock::time_point deadline = Clock::now() + timeout;
  for (;;) {
    absl::StatusOr<Message> msg = sub.ReadMessage();
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length > 0) {
      return std::string(static_cast<const char *>(msg->buffer), msg->length);
    }
    if (Clock::now() >= deadline) {
      return absl::DeadlineExceededError("no message arrived");
    }
    std::this_thread::sleep_for(2ms);
  }
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

const DiscoveryTopic *
FindTopic(const google::protobuf::RepeatedPtrField<DiscoveryTopic> &list,
          const std::string &topic) {
  for (const DiscoveryTopic &info : list) {
    if (info.topic() == topic) {
      return &info;
    }
  }
  return nullptr;
}

class TunnelFakePeerTest : public ::testing::Test {
protected:
  void SetUp() override {
    signal(SIGPIPE, SIG_IGN);
    ASSERT_THAT(peer_.Listen(), IsOk());
  }

  void TearDown() override {
    if (!server_) {
      return;
    }
    client_.reset();
    server_->Stop();
    char buf[8];
    (void)::read(server_pipe_[0], buf, sizeof(buf));
    server_thread_.join();
    server_->CleanupAfterSession();
    (void)remove(socket_.c_str());
    ::close(server_pipe_[0]);
    ::close(server_pipe_[1]);
  }

  void StartServer(int refresh_secs = 0) {
    char socket_template[] = TUNNEL_TEST_TMP "/subspace_tunnelXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_template[0]));
    socket_ = &socket_template[0];
    ASSERT_EQ(0, pipe(server_pipe_));

    TunnelConfig config;
    config.set_device_name("alpha");
    config.set_listen_address("127.0.0.1");
    config.set_listen_port(FreePort());
    TunnelPeer *peer = config.add_peers();
    peer->set_name(kPeerName);
    peer->set_tcp_addr("127.0.0.1");
    peer->set_tcp_port(peer_.Port());
    config.add_discovery_denylist("/alpha/secret");
    if (refresh_secs > 0) {
      config.set_discovery_refresh_secs(refresh_secs);
    }

    server_ = std::make_unique<Server>(engine_, socket_, "", 0, 0,
                                       /*local=*/true, server_pipe_[1]);
    server_->SetTunnelConfig(config);
    server_thread_ = std::thread([this]() {
      absl::Status s = server_->Run(/*num_asio_threads=*/4);
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });
    char buf[8];
    (void)::read(server_pipe_[0], buf, sizeof(buf));
    client_ = std::make_unique<Client>();
    ASSERT_THAT(client_->Init(socket_), IsOk());
  }

  int ServerPort() const { return server_->GetTunnelManager()->ListenPort(); }
  const TunnelManager::Stats &Stats() const {
    return server_->GetTunnelManager()->GetStats();
  }

  // Opens a connection to the server whose handshake names `topic`, shaped
  // like a discovery channel.
  std::unique_ptr<WireConnection> OpenDiscovery(std::string_view topic) {
    absl::StatusOr<std::unique_ptr<WireConnection>> connection =
        WireConnection::Connect(ServerPort());
    EXPECT_THAT(connection, IsOk());
    if (!connection.ok()) {
      return nullptr;
    }
    Handshake handshake = DiscoveryHandshake();
    handshake.set_topic_name(std::string(topic));
    EXPECT_THAT((*connection)->SendHandshake(handshake), IsOk());
    return std::move(*connection);
  }

  // Opens a discovery connection to the server and sends it one message.
  std::unique_ptr<WireConnection> Announce(const Discovery &msg) {
    std::unique_ptr<WireConnection> connection =
        OpenDiscovery(tunnel::kDiscoveryChannelName);
    if (connection != nullptr) {
      EXPECT_THAT(connection->Send(PacketType::kData, 1,
                                   DiscoveryBody(msg.SerializeAsString(), 1)),
                  IsOk());
    }
    return connection;
  }

  // Opens a data connection to the server for `topic`.
  std::unique_ptr<WireConnection>
  OpenData(const std::string &topic, uint64_t slot_size, uint32_t num_slots) {
    absl::StatusOr<std::unique_ptr<WireConnection>> connection =
        WireConnection::Connect(ServerPort());
    EXPECT_THAT(connection, IsOk());
    if (!connection.ok()) {
      return nullptr;
    }
    Handshake handshake;
    handshake.set_topic_name(topic);
    handshake.set_channel_type("cruise.Type");
    tunnel::SubspaceTransport *transport = handshake.mutable_subspace();
    transport->set_slot_size(slot_size);
    transport->set_num_slots(num_slots);
    transport->set_vchan_id(-1);
    EXPECT_THAT((*connection)->SendHandshake(handshake), IsOk());
    return std::move(*connection);
  }

  // Reads the server's discovery messages until one satisfies `done`,
  // checking that their sequence numbers count up from `*seq`.
  absl::StatusOr<Discovery>
  ReadDiscoveryUntil(WireConnection &connection, uint64_t *seq,
                     const std::function<bool(const Discovery &)> &done) {
    Clock::time_point deadline = Clock::now() + 10s;
    while (Clock::now() < deadline) {
      absl::StatusOr<Packet> packet = connection.Read();
      if (!packet.ok()) {
        return packet.status();
      }
      if (packet->header.seq != *seq + 1) {
        return absl::InternalError(
            absl::StrFormat("sequence %d follows %d", packet->header.seq, *seq));
      }
      *seq = packet->header.seq;
      absl::StatusOr<Discovery> msg = ParseDiscovery(*packet);
      if (!msg.ok() || done(*msg)) {
        return msg;
      }
    }
    return absl::DeadlineExceededError("no matching discovery message");
  }

  FakePeer peer_;
  async::RuntimeEngine engine_;
  std::string socket_;
  int server_pipe_[2];
  std::unique_ptr<Server> server_;
  std::thread server_thread_;
  std::unique_ptr<Client> client_;
};

TEST_F(TunnelFakePeerTest, AnnouncesChannelsInCruiseFormat) {
  StartServer();
  absl::StatusOr<Publisher> out = client_->CreatePublisher(
      "/alpha/out", PublisherOptions().SetSlotSize(256).SetNumSlots(8).SetType(
                        "alpha.Out"));
  ASSERT_THAT(out, IsOk());
  absl::StatusOr<Publisher> reliable = client_->CreatePublisher(
      "/alpha/reliable",
      PublisherOptions().SetSlotSize(256).SetNumSlots(8).SetReliable(true));
  ASSERT_THAT(reliable, IsOk());
  absl::StatusOr<Publisher> virtual_pub = client_->CreatePublisher(
      "/alpha/virtual", PublisherOptions()
                            .SetSlotSize(256)
                            .SetNumSlots(8)
                            .SetMux("/alpha/mux")
                            .SetVchanId(5));
  ASSERT_THAT(virtual_pub, IsOk());
  absl::StatusOr<Publisher> secret =
      client_->CreatePublisher("/alpha/secret", 256, 8);
  ASSERT_THAT(secret, IsOk());
  absl::StatusOr<Publisher> local = client_->CreatePublisher(
      "/alpha/local",
      PublisherOptions().SetSlotSize(256).SetNumSlots(8).SetLocal(true));
  ASSERT_THAT(local, IsOk());
  absl::StatusOr<Subscriber> in = client_->CreateSubscriber(
      "/alpha/in", SubscriberOptions().SetType("alpha.In"));
  ASSERT_THAT(in, IsOk());

  absl::StatusOr<Accepted> discovery =
      peer_.Accept(tunnel::kDiscoveryChannelName);
  ASSERT_THAT(discovery, IsOk());
  const Handshake &handshake = discovery->handshake;
  EXPECT_EQ(handshake.channel_type(), "");
  ASSERT_TRUE(handshake.has_subspace());
  EXPECT_EQ(handshake.subspace().slot_size(), 256u);
  EXPECT_EQ(handshake.subspace().num_slots(), 24u);
  EXPECT_FALSE(handshake.subspace().is_reliable());
  EXPECT_EQ(handshake.subspace().mux(), "");
  EXPECT_EQ(handshake.subspace().vchan_id(), -1);
  EXPECT_TRUE(handshake.subspace().has_checksum_size());
  EXPECT_EQ(handshake.subspace().checksum_size(), 4);
  EXPECT_EQ(handshake.subspace().metadata_size(), 0);

  uint64_t seq = 0;
  absl::StatusOr<Discovery> msg = ReadDiscoveryUntil(
      *discovery->connection, &seq, [](const Discovery &m) {
        return FindTopic(m.published_topics(), "/alpha/out") != nullptr &&
               FindTopic(m.published_topics(), "/alpha/reliable") != nullptr &&
               FindTopic(m.published_topics(), "/alpha/virtual") != nullptr &&
               FindTopic(m.subscribed_topics(), "/alpha/in") != nullptr;
      });
  ASSERT_THAT(msg, IsOk());
  EXPECT_EQ(msg->device_name(), "alpha");

  const DiscoveryTopic *info =
      FindTopic(msg->published_topics(), "/alpha/out");
  EXPECT_EQ(info->datatype(), "alpha.Out");
  EXPECT_EQ(info->mux(), "");
  EXPECT_EQ(info->vchan_id(), -1);
  EXPECT_FALSE(info->is_reliable());
  EXPECT_TRUE(
      FindTopic(msg->published_topics(), "/alpha/reliable")->is_reliable());
  info = FindTopic(msg->published_topics(), "/alpha/virtual");
  EXPECT_EQ(info->mux(), "/alpha/mux");
  EXPECT_EQ(info->vchan_id(), 5);
  info = FindTopic(msg->subscribed_topics(), "/alpha/in");
  EXPECT_EQ(info->datatype(), "alpha.In");
  EXPECT_EQ(info->vchan_id(), 0);

  for (const auto *list : {&msg->published_topics(), &msg->subscribed_topics()}) {
    for (const DiscoveryTopic &topic : *list) {
      EXPECT_NE(topic.topic(), "/alpha/secret");
      EXPECT_NE(topic.topic(), "/alpha/local");
      EXPECT_NE(topic.topic(), "/alpha/mux");
      EXPECT_FALSE(absl::StartsWith(topic.topic(), "/subspace/"))
          << topic.topic();
    }
  }
}

TEST_F(TunnelFakePeerTest, RepliesWhenThePeerPublishesWhatItWants) {
  StartServer();
  absl::StatusOr<Subscriber> sub = client_->CreateSubscriber("/cruise/reply");
  ASSERT_THAT(sub, IsOk());
  absl::StatusOr<Accepted> discovery =
      peer_.Accept(tunnel::kDiscoveryChannelName);
  ASSERT_THAT(discovery, IsOk());
  uint64_t seq = 0;
  ASSERT_THAT(ReadDiscoveryUntil(*discovery->connection, &seq,
                                 [](const Discovery &m) {
                                   return FindTopic(m.subscribed_topics(),
                                                    "/cruise/reply") != nullptr;
                                 }),
              IsOk());
  // Let any other pending announcements go by.
  while (discovery->connection->Read(500ms).ok()) {
    seq++;
  }

  std::unique_ptr<WireConnection> announcement = Announce(
      MakeDiscovery(kPeerName, {{"/cruise/reply", "cruise.Reply"}}, {}));
  // Well before the 5 second refresh.
  absl::StatusOr<Packet> reply = discovery->connection->Read(2s);
  ASSERT_THAT(reply, IsOk());
  EXPECT_EQ(reply->header.seq, seq + 1);
  absl::StatusOr<Discovery> msg = ParseDiscovery(*reply);
  ASSERT_THAT(msg, IsOk());
  EXPECT_NE(FindTopic(msg->subscribed_topics(), "/cruise/reply"), nullptr);
}

TEST_F(TunnelFakePeerTest, SendsChannelsThePeerSubscribesTo) {
  StartServer();
  absl::StatusOr<Publisher> pub = client_->CreatePublisher(
      "/alpha/stream", PublisherOptions()
                           .SetSlotSize(128)
                           .SetNumSlots(8)
                           .SetChecksum(true)
                           .SetType("alpha.Stream"));
  ASSERT_THAT(pub, IsOk());
  std::unique_ptr<WireConnection> announcement = Announce(
      MakeDiscovery(kPeerName, {}, {{"/alpha/stream", "alpha.Stream"}}));

  absl::StatusOr<Accepted> stream = peer_.Accept("/alpha/stream");
  ASSERT_THAT(stream, IsOk());
  const tunnel::SubspaceTransport &transport = stream->handshake.subspace();
  EXPECT_EQ(stream->handshake.channel_type(), "alpha.Stream");
  EXPECT_EQ(transport.slot_size(), 128u);
  EXPECT_GE(transport.num_slots(), 8u);
  EXPECT_FALSE(transport.is_reliable());
  EXPECT_EQ(transport.mux(), "");
  EXPECT_EQ(transport.vchan_id(), -1);
  EXPECT_EQ(transport.checksum_size(), 4);
  EXPECT_EQ(transport.metadata_size(), 0);

  // The last message is bigger than the slots.
  std::vector<std::string> messages = {"one", "two", Pattern(5000, 3)};
  for (size_t i = 0; i < messages.size(); i++) {
    ASSERT_THAT(Publish(*pub, messages[i]), IsOk());
    absl::StatusOr<Packet> packet = stream->connection->Read();
    ASSERT_THAT(packet, IsOk()) << i;
    EXPECT_EQ(packet->header.type, PacketType::kData);
    EXPECT_EQ(packet->header.seq, i + 1);
    EXPECT_EQ(packet->header.packet_length,
              tunnel::kPacketHeaderSize + sizeof(MessagePrefix) +
                  messages[i].size());
    ASSERT_EQ(packet->body.size(), sizeof(MessagePrefix) + messages[i].size());
    MessagePrefix prefix;
    memcpy(&prefix, packet->body.data(), sizeof(prefix));
    std::string payload = packet->body.substr(sizeof(prefix));
    EXPECT_EQ(prefix.flags, kMessageHasChecksum);
    EXPECT_EQ(prefix.message_size, messages[i].size());
    EXPECT_TRUE(payload == messages[i]) << i;
    MessagePrefix expected = prefix;
    Checksum(&expected, payload);
    EXPECT_EQ(expected.checksum, prefix.checksum) << i;
  }
}

TEST_F(TunnelFakePeerTest, ReceivesCruiseMessages) {
  StartServer();
  absl::StatusOr<Subscriber> sub = client_->CreateSubscriber(
      "/cruise/data", SubscriberOptions().SetChecksum(true));
  ASSERT_THAT(sub, IsOk());
  std::unique_ptr<WireConnection> data = OpenData("/cruise/data", 64, 8);
  ASSERT_NE(data, nullptr);

  std::string large = Pattern(1000, 1);
  std::string huge = Pattern(2 << 20, 2);
  ASSERT_THAT(data->Send(PacketType::kData, 1,
                         CruiseDataBody("hello", 1, /*checksum=*/true)),
              IsOk());
  ASSERT_THAT(data->Send(PacketType::kOperation, 0, "operation"), IsOk());
  ASSERT_THAT(data->Send(PacketType::kData, 2,
                         CruiseDataBody("activation", 2, /*checksum=*/true,
                                        kMessageActivate)),
              IsOk());
  ASSERT_THAT(data->Send(PacketType::kData, 3,
                         CruiseDataBody(large, 3, /*checksum=*/true)),
              IsOk());
  ASSERT_THAT(data->Send(PacketType::kData, 4,
                         CruiseDataBody(huge, 4, /*checksum=*/true)),
              IsOk());
  // With multihop on, the peer forwards what it received from another machine.
  ASSERT_THAT(data->Send(PacketType::kData, 5,
                         CruiseDataBody("forwarded", 5, /*checksum=*/true,
                                        kMessageCrossMachine)),
              IsOk());

  // ReadMessage fails if a checksum doesn't verify.
  for (const std::string &expected :
       {std::string("hello"), large, huge, std::string("forwarded")}) {
    absl::StatusOr<std::string> received = Read(*sub);
    ASSERT_THAT(received, IsOk()) << expected.size();
    ASSERT_EQ(received->size(), expected.size());
    EXPECT_TRUE(*received == expected) << expected.size();
  }
  EXPECT_FALSE(Read(*sub, 300ms).ok());
  EXPECT_EQ(Stats().messages_received, 4u);
}

// The peer numbers the virtual channel 9, and here it's 3.  The prefix keeps
// the peer's number, so a checksum the tunnel can't compute still verifies.
TEST_F(TunnelFakePeerTest, KeepsThePeersPrefix) {
  StartServer();
  absl::StatusOr<Subscriber> sub = client_->CreateSubscriber(
      "/cruise/virtual", SubscriberOptions()
                             .SetMux("/cruise/mux")
                             .SetVchanId(3)
                             .SetChecksum(true));
  ASSERT_THAT(sub, IsOk());
  sub->SetChecksumCallback(CustomChecksum);
  absl::StatusOr<Subscriber> nine = client_->CreateSubscriber(
      "/cruise/nine", SubscriberOptions().SetMux("/cruise/mux").SetVchanId(9));
  ASSERT_THAT(nine, IsOk());
  std::unique_ptr<WireConnection> data = OpenData("/cruise/virtual", 64, 8);
  ASSERT_NE(data, nullptr);

  const std::string payload = "virtual";
  MessagePrefix prefix = {};
  prefix.slot_id = 5;
  prefix.message_size = payload.size();
  prefix.ordinal = 1;
  prefix.timestamp = 1;
  prefix.vchan_id = 9;
  prefix.checksum_size = 4;
  prefix.SetHasChecksum();
  auto spans = GetMessageChecksumData(&prefix, payload.data(), payload.size(),
                                      4, 0);
  CustomChecksum(spans.Spans(), GetChecksumSpan(&prefix, 4));
  std::string body(reinterpret_cast<const char *>(&prefix), sizeof(prefix));
  body.append(payload);
  ASSERT_THAT(data->Send(PacketType::kData, 1, body), IsOk());

  absl::StatusOr<Message> msg;
  for (int i = 0; i < 1000; i++) {
    msg = sub->ReadMessage();
    if (!msg.ok() || msg->length > 0) {
      break;
    }
    std::this_thread::sleep_for(5ms);
  }
  // ReadMessage fails if the checksum doesn't verify.
  ASSERT_THAT(msg, IsOk());
  ASSERT_EQ(std::string(static_cast<const char *>(msg->buffer), msg->length),
            payload);
  EXPECT_EQ(msg->vchan_id, 3);
  const auto *received = reinterpret_cast<const MessagePrefix *>(
      static_cast<const char *>(msg->buffer) - sub->PrefixSize());
  EXPECT_EQ(received->vchan_id, 9);
  EXPECT_EQ(received->checksum, prefix.checksum);
  EXPECT_FALSE(Read(*nine, 300ms).ok());
}

TEST_F(TunnelFakePeerTest, DropsMessagesTooLargeForACappedChannel) {
  StartServer();
  absl::StatusOr<Publisher> capper = client_->CreatePublisher(
      "/cruise/capped",
      PublisherOptions().SetSlotSize(256).SetNumSlots(8).SetMaxSlotSize(1024));
  ASSERT_THAT(capper, IsOk());
  absl::StatusOr<Subscriber> sub = client_->CreateSubscriber("/cruise/capped");
  ASSERT_THAT(sub, IsOk());
  std::unique_ptr<WireConnection> data = OpenData("/cruise/capped", 256, 8);
  ASSERT_NE(data, nullptr);

  std::string fits = Pattern(512, 1);
  std::string too_big = Pattern(4096, 2);
  std::string after = Pattern(100, 3);
  ASSERT_THAT(
      data->Send(PacketType::kData, 1, CruiseDataBody(fits, 1, false)),
      IsOk());
  ASSERT_THAT(
      data->Send(PacketType::kData, 2, CruiseDataBody(too_big, 2, false)),
      IsOk());
  ASSERT_THAT(
      data->Send(PacketType::kData, 3, CruiseDataBody(after, 3, false)),
      IsOk());

  // The connection survives the dropped message.
  for (const std::string &expected : {fits, after}) {
    absl::StatusOr<std::string> received = Read(*sub);
    ASSERT_THAT(received, IsOk()) << expected.size();
    EXPECT_TRUE(*received == expected) << expected.size();
  }
  EXPECT_EQ(Stats().oversized_received, 1u);
  EXPECT_FALSE(data->ClosedWithin(100ms));
}

TEST_F(TunnelFakePeerTest, IgnoresUnknownDevices) {
  StartServer();
  absl::StatusOr<Publisher> pub =
      client_->CreatePublisher("/alpha/private", 256, 8);
  ASSERT_THAT(pub, IsOk());
  Discovery msg = MakeDiscovery("stranger", {}, {{"/alpha/private", ""}});
  std::unique_ptr<WireConnection> stranger = Announce(msg);
  EXPECT_THAT(peer_.Accept("/alpha/private", 1s),
              StatusIs(absl::StatusCode::kDeadlineExceeded));

  msg.set_device_name(kPeerName);
  std::unique_ptr<WireConnection> known = Announce(msg);
  EXPECT_THAT(peer_.Accept("/alpha/private"), IsOk());
}

TEST_F(TunnelFakePeerTest, SkipsDiscoveryThatDoesNotDecode) {
  StartServer();
  absl::StatusOr<Publisher> pub =
      client_->CreatePublisher("/alpha/wanted", 256, 8);
  ASSERT_THAT(pub, IsOk());
  std::unique_ptr<WireConnection> discovery =
      OpenDiscovery(tunnel::kDiscoveryChannelName);
  ASSERT_NE(discovery, nullptr);
  // An invalid wire type, then a valid message with no device name.
  ASSERT_THAT(discovery->Send(PacketType::kData, 1,
                              DiscoveryBody(std::string(3, '\xff'), 1)),
              IsOk());
  ASSERT_THAT(discovery->Send(
                  PacketType::kData, 2,
                  DiscoveryBody(MakeDiscovery("", {}, {{"/alpha/wanted", ""}})
                                    .SerializeAsString(),
                                2)),
              IsOk());
  ASSERT_THAT(discovery->Send(
                  PacketType::kData, 3,
                  DiscoveryBody(MakeDiscovery(kPeerName, {},
                                              {{"/alpha/wanted", ""}})
                                    .SerializeAsString(),
                                3)),
              IsOk());

  EXPECT_THAT(peer_.Accept("/alpha/wanted"), IsOk());
  EXPECT_EQ(Stats().malformed_received, 2u);
  EXPECT_EQ(Stats().discovery_received, 1u);
}

// Peers also announce themselves in an older encoding on
// /ipc/discovery/receive, which the tunnel doesn't read.
TEST_F(TunnelFakePeerTest, DiscardsInternalChannels) {
  StartServer();
  absl::StatusOr<Publisher> pub =
      client_->CreatePublisher("/alpha/wanted", 256, 8);
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> legacy =
      client_->CreateSubscriber("/ipc/discovery/receive");
  ASSERT_THAT(legacy, IsOk());

  std::unique_ptr<WireConnection> ros = OpenDiscovery("/ipc/discovery/receive");
  ASSERT_NE(ros, nullptr);
  std::unique_ptr<WireConnection> directory =
      OpenDiscovery("/subspace/ChannelDirectory");
  ASSERT_NE(directory, nullptr);
  // Would ask for /alpha/wanted if the tunnel decoded it.
  std::string announcement =
      MakeDiscovery(kPeerName, {}, {{"/alpha/wanted", ""}})
          .SerializeAsString();
  for (WireConnection *connection : {ros.get(), directory.get()}) {
    for (uint64_t seq = 1; seq <= 3; seq++) {
      ASSERT_THAT(connection->Send(PacketType::kData, seq,
                                   DiscoveryBody(announcement, seq)),
                  IsOk());
    }
  }

  EXPECT_THAT(peer_.Accept("/alpha/wanted", 1s),
              StatusIs(absl::StatusCode::kDeadlineExceeded));
  EXPECT_FALSE(Read(*legacy, 100ms).ok());
  EXPECT_FALSE(ros->ClosedWithin(100ms));
  EXPECT_FALSE(directory->ClosedWithin(100ms));
  EXPECT_EQ(Stats().messages_received, 0u);
  EXPECT_EQ(Stats().discovery_received, 0u);
}

TEST_F(TunnelFakePeerTest, RepeatsDiscoveryOnRefresh) {
  StartServer(/*refresh_secs=*/1);
  absl::StatusOr<Accepted> discovery =
      peer_.Accept(tunnel::kDiscoveryChannelName);
  ASSERT_THAT(discovery, IsOk());
  uint64_t seq = 0;
  for (int i = 0; i < 3; i++) {
    absl::StatusOr<Packet> packet = discovery->connection->Read(3s);
    ASSERT_THAT(packet, IsOk()) << i;
    EXPECT_EQ(packet->header.seq, ++seq);
    EXPECT_THAT(ParseDiscovery(*packet), IsOk());
  }
}

TEST_F(TunnelFakePeerTest, ResendsDiscoveryAfterReconnecting) {
  StartServer();
  absl::StatusOr<Accepted> discovery =
      peer_.Accept(tunnel::kDiscoveryChannelName);
  ASSERT_THAT(discovery, IsOk());
  ASSERT_THAT(discovery->connection->Read(), IsOk());
  discovery->connection->Close();

  absl::StatusOr<Accepted> again = peer_.Accept(tunnel::kDiscoveryChannelName);
  ASSERT_THAT(again, IsOk());
  absl::StatusOr<Packet> packet = again->connection->Read();
  ASSERT_THAT(packet, IsOk());
  EXPECT_EQ(packet->header.seq, 1u);
  absl::StatusOr<Discovery> msg = ParseDiscovery(*packet);
  ASSERT_THAT(msg, IsOk());
  EXPECT_EQ(msg->device_name(), "alpha");
}

TEST_F(TunnelFakePeerTest, ClosesConnectionsWithBadHandshakes) {
  StartServer();
  absl::StatusOr<std::unique_ptr<WireConnection>> no_transport =
      WireConnection::Connect(ServerPort());
  ASSERT_THAT(no_transport, IsOk());
  Handshake handshake;
  handshake.set_topic_name("/cruise/nothing");
  ASSERT_THAT((*no_transport)->SendHandshake(handshake), IsOk());
  EXPECT_TRUE((*no_transport)->ClosedWithin(2s));

  absl::StatusOr<std::unique_ptr<WireConnection>> bad_magic =
      WireConnection::Connect(ServerPort());
  ASSERT_THAT(bad_magic, IsOk());
  ASSERT_THAT((*bad_magic)->SendRaw(std::string(tunnel::kPacketHeaderSize, 'x')),
              IsOk());
  EXPECT_TRUE((*bad_magic)->ClosedWithin(2s));
}

} // namespace
} // namespace subspace
