// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/tunnel.h"

#include "absl/strings/str_format.h"
#include "client/client.h"
#include "common/async/runtime.h"
#include "common/async/wait.h"
#include "common/channel.h"
#include "server/server.h"
#include "server/server_channel.h"
#include "server/tunnel_config.h"
#include "server/tunnel_protocol.h"
#include "toolbelt/clock.h"
#include <algorithm>
#include <arpa/inet.h>
#include <cstring>
#include <limits>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace subspace {

using tunnel::Handshake;
using tunnel::PacketHeader;
using tunnel::PacketType;

namespace {

constexpr std::chrono::seconds
    kReconnectInterval(tunnel::kReconnectIntervalSecs);
constexpr std::chrono::seconds kConnectTimeout(tunnel::kReconnectIntervalSecs);
// Payloads up to this size are copied in behind the prefix so the packet goes
// out in one send.
constexpr size_t kCoalesceLimit = 16 * 1024;
// toolbelt tracks receive progress in an int, so large reads are split.
constexpr size_t kMaxReceiveChunk = 16 * 1024 * 1024;

absl::Status PeerClosed() {
  return absl::AbortedError("the peer closed the connection");
}

bool IsPeerClosed(const absl::Status &status) {
  return absl::IsAborted(status);
}

absl::StatusOr<toolbelt::InetAddress> ResolveIPv4(const std::string &host,
                                                  int port) {
  struct addrinfo hints = {};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo *result = nullptr;
  int err = getaddrinfo(host.c_str(), nullptr, &hints, &result);
  if (err != 0) {
    return absl::UnavailableError(
        absl::StrFormat("can't resolve %s: %s", host, gai_strerror(err)));
  }
  if (result == nullptr) {
    return absl::UnavailableError(
        absl::StrFormat("%s has no IPv4 address", host));
  }
  sockaddr_in addr;
  memcpy(&addr, result->ai_addr, sizeof(addr));
  freeaddrinfo(result);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  return toolbelt::InetAddress(addr);
}

std::string PeerAddress(const async::StreamSocket &socket) {
  sockaddr_in addr = {};
  socklen_t len = sizeof(addr);
  if (getpeername(async::StreamSocketFd(socket),
                  reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
    return "unknown peer";
  }
  return toolbelt::InetAddress(addr).ToString();
}

void SetNoDelay(const async::StreamSocket &socket) {
  int one = 1;
  (void)setsockopt(async::StreamSocketFd(socket), IPPROTO_TCP, TCP_NODELAY,
                   &one, sizeof(one));
}

// Reads exactly `length` bytes.
absl::Status ReadFully(async::StreamSocket &socket, void *buffer,
                       size_t length, async::Context ctx) {
  char *p = static_cast<char *>(buffer);
  while (length > 0) {
    size_t chunk = std::min(length, kMaxReceiveChunk);
    absl::StatusOr<ssize_t> n = socket.Receive(p, chunk, ctx);
    if (!n.ok()) {
      return n.status();
    }
    if (*n <= 0) {
      return PeerClosed();
    }
    p += *n;
    length -= static_cast<size_t>(*n);
  }
  return absl::OkStatus();
}

absl::Status Skip(async::StreamSocket &socket, size_t length,
                  async::Context ctx) {
  std::vector<char> scratch(std::min<size_t>(length, 64 * 1024));
  while (length > 0) {
    size_t chunk = std::min(length, scratch.size());
    if (absl::Status s = ReadFully(socket, scratch.data(), chunk, ctx);
        !s.ok()) {
      return s;
    }
    length -= chunk;
  }
  return absl::OkStatus();
}

absl::StatusOr<PacketHeader> ReadHeader(async::StreamSocket &socket,
                                        async::Context ctx) {
  std::array<std::byte, tunnel::kPacketHeaderSize> buffer{};
  if (absl::Status s = ReadFully(socket, buffer.data(), buffer.size(), ctx);
      !s.ok()) {
    return s;
  }
  return tunnel::DecodePacketHeader(buffer);
}

absl::Status SendAll(async::StreamSocket &socket, const void *data,
                     size_t length, async::Context ctx) {
  const char *p = static_cast<const char *>(data);
  while (length > 0) {
    absl::StatusOr<ssize_t> n = socket.Send(p, length, ctx);
    if (!n.ok()) {
      return n.status();
    }
    if (*n <= 0) {
      return absl::UnavailableError("connection closed while sending");
    }
    p += *n;
    length -= static_cast<size_t>(*n);
  }
  return absl::OkStatus();
}

absl::Status SendPacket(async::StreamSocket &socket, PacketType type,
                        uint64_t seq, const void *body, size_t body_size,
                        async::Context ctx) {
  absl::StatusOr<PacketHeader> header =
      tunnel::MakePacketHeader(type, body_size, seq, tunnel::RealtimeNowNs());
  if (!header.ok()) {
    return header.status();
  }
  std::vector<std::byte> packet(tunnel::kPacketHeaderSize + body_size);
  tunnel::EncodePacketHeader(*header, packet.data());
  if (body_size > 0) {
    memcpy(packet.data() + tunnel::kPacketHeaderSize, body, body_size);
  }
  return SendAll(socket, packet.data(), packet.size(), ctx);
}

absl::Status SendHandshake(async::StreamSocket &socket,
                           const Handshake &handshake, async::Context ctx) {
  std::string body = handshake.SerializeAsString();
  return SendPacket(socket, PacketType::kHandshake, 0, body.data(),
                    body.size(), ctx);
}

Handshake DiscoveryHandshake() {
  Handshake handshake;
  handshake.set_topic_name(tunnel::kDiscoveryChannelName);
  tunnel::SubspaceTransport *transport = handshake.mutable_subspace();
  transport->set_slot_size(tunnel::kDiscoverySlotSize);
  transport->set_num_slots(tunnel::kDiscoveryNumSlots);
  transport->set_vchan_id(-1);
  transport->set_checksum_size(4);
  return handshake;
}

} // namespace

TunnelManager::TunnelManager(Server *server, TunnelConfig config)
    : server_(server), config_(std::move(config)) {}

TunnelManager::~TunnelManager() = default;

absl::Status TunnelManager::Init() {
  std::string hostname;
  char buffer[256];
  if (gethostname(buffer, sizeof(buffer)) == 0) {
    buffer[sizeof(buffer) - 1] = '\0';
    hostname = buffer;
  }
  device_name_ = TunnelDeviceName(config_, server_->machine_name_, hostname);
  if (absl::Status s = ValidateTunnelConfig(config_, device_name_); !s.ok()) {
    return s;
  }
  denylist_.insert(config_.discovery_denylist().begin(),
                   config_.discovery_denylist().end());
  refresh_interval_ =
      std::chrono::seconds(TunnelDiscoveryRefreshSecs(config_));

  absl::StatusOr<toolbelt::TriggerFd> rescan = toolbelt::TriggerFd::Create();
  if (!rescan.ok()) {
    return rescan.status();
  }
  rescan_trigger_ = std::move(*rescan);

  for (const TunnelPeer &peer_config : config_.peers()) {
    auto peer = std::make_unique<Peer>();
    peer->config = peer_config;
    absl::StatusOr<toolbelt::TriggerFd> trigger =
        toolbelt::TriggerFd::Create();
    if (!trigger.ok()) {
      return trigger.status();
    }
    peer->discovery_trigger = std::move(*trigger);
    absl::StatusOr<toolbelt::InetAddress> address =
        ResolveIPv4(peer_config.tcp_addr(), peer_config.tcp_port());
    if (address.ok()) {
      peer->address = *address;
    } else {
      server_->logger_.Log(toolbelt::LogLevel::kWarning,
                           "Tunnel peer %s: %s; will keep trying",
                           peer_config.name().c_str(),
                           address.status().ToString().c_str());
    }
    peers_by_name_[peer_config.name()] = peer.get();
    peers_.push_back(std::move(peer));
  }

  toolbelt::InetAddress listen_address =
      toolbelt::InetAddress::AnyAddress(config_.listen_port());
  if (!config_.listen_address().empty()) {
    absl::StatusOr<toolbelt::InetAddress> address =
        ResolveIPv4(config_.listen_address(), config_.listen_port());
    if (!address.ok()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("tunnel listen_address: %s",
                          address.status().message()));
    }
    listen_address = *address;
  }
  absl::StatusOr<async::StreamSocket> listener =
      async::ListenStream(listen_address);
  if (!listener.ok()) {
    return absl::Status(listener.status().code(),
                        absl::StrFormat("tunnel listener on %s: %s",
                                        listen_address.ToString(),
                                        listener.status().message()));
  }
  listener_ = std::move(*listener);
  server_->logger_.Log(toolbelt::LogLevel::kInfo,
                       "Tunnel device %s listening on %s with %d peers",
                       device_name_.c_str(), listen_address.ToString().c_str(),
                       static_cast<int>(peers_.size()));
  return absl::OkStatus();
}

int TunnelManager::ListenPort() const {
  sockaddr_in addr = {};
  socklen_t len = sizeof(addr);
  if (getsockname(async::StreamSocketFd(listener_),
                  reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
    return -1;
  }
  return ntohs(addr.sin_port);
}

void TunnelManager::Spawn() {
  SpawnOnStrand([this](async::Context ctx) { ListenerCoroutine(ctx); },
                "Tunnel listener");
  SpawnOnStrand([this](async::Context ctx) { ScannerCoroutine(ctx); },
                "Tunnel scanner");
  for (const std::unique_ptr<Peer> &peer : peers_) {
    Peer *p = peer.get();
    SpawnOnStrand(
        [this, p](async::Context ctx) { DiscoverySenderCoroutine(ctx, p); },
        absl::StrFormat("Tunnel discovery to %s", p->config.name()));
  }
}

void TunnelManager::ChannelsChanged() { rescan_trigger_.Trigger(); }

void TunnelManager::SpawnOnStrand(std::function<void(async::Context)> fn,
                                  std::string name) {
  server_->runtime_.SpawnOnStrand(
      std::move(fn),
      {.name = std::move(name),
       .interrupt_fd = server_->shutdown_trigger_fd_.GetPollFd().Fd()});
}

void TunnelManager::SpawnOnNewStrand(std::function<void(async::Context)> fn,
                                     std::string name) {
  server_->runtime_.SpawnOnNewStrand(
      std::move(fn),
      {.name = std::move(name),
       .interrupt_fd = server_->shutdown_trigger_fd_.GetPollFd().Fd()});
}

void TunnelManager::SleepUnlessShutdown(async::Context ctx,
                                        std::chrono::nanoseconds duration) {
  (void)async::WaitReadable(
      ctx, server_->shutdown_trigger_fd_.GetPollFd().Fd(), duration);
}

void TunnelManager::ListenerCoroutine(async::Context ctx) {
  while (!server_->shutting_down_) {
    absl::StatusOr<async::StreamSocket> socket = listener_.Accept(ctx);
    if (server_->shutting_down_) {
      break;
    }
    if (!socket.ok()) {
      server_->logger_.Log(toolbelt::LogLevel::kError,
                           "Tunnel can't accept a connection: %s",
                           socket.status().ToString().c_str());
      SleepUnlessShutdown(ctx, std::chrono::seconds(1));
      continue;
    }
    auto connection = std::make_shared<async::StreamSocket>(std::move(*socket));
    if (absl::Status s = connection->SetNonBlocking(); !s.ok()) {
      server_->logger_.Log(toolbelt::LogLevel::kError,
                           "Tunnel can't configure a connection: %s",
                           s.ToString().c_str());
      continue;
    }
    SetNoDelay(*connection);
    std::string peer = PeerAddress(*connection);
    SpawnOnStrand(
        [this, connection, peer](async::Context handshake_ctx) {
          HandshakeCoroutine(handshake_ctx, connection, peer);
        },
        "Tunnel handshake from " + peer);
  }
  listener_.Close();
}

void TunnelManager::HandshakeCoroutine(
    async::Context ctx, std::shared_ptr<async::StreamSocket> socket,
    std::string peer) {
  if (server_->shutting_down_) {
    return;
  }
  absl::Status status;
  for (;;) {
    absl::StatusOr<PacketHeader> header = ReadHeader(*socket, ctx);
    if (!header.ok()) {
      status = header.status();
      break;
    }
    if (header->type != PacketType::kHandshake) {
      // Nothing can be published before the handshake says where.
      status = Skip(*socket, header->BodySize(), ctx);
      if (!status.ok()) {
        break;
      }
      continue;
    }
    if (header->BodySize() > tunnel::kMaxControlBodySize) {
      status = absl::InvalidArgumentError(
          absl::StrFormat("handshake of %d bytes is too large",
                          header->BodySize()));
      break;
    }
    std::string body(header->BodySize(), '\0');
    status = ReadFully(*socket, body.data(), body.size(), ctx);
    if (!status.ok()) {
      break;
    }
    Handshake handshake;
    if (!handshake.ParseFromString(body)) {
      status = absl::InvalidArgumentError("malformed handshake");
      break;
    }
    status = tunnel::ValidateHandshake(handshake);
    if (!status.ok()) {
      break;
    }
    if (server_->shutting_down_) {
      return;
    }
    if (handshake.topic_name() == tunnel::kDiscoveryChannelName) {
      DiscoveryReceiverLoop(ctx, *socket, handshake, peer);
      return;
    }
    if (tunnel::IsInternalChannel(handshake.topic_name())) {
      DiscardConnection(ctx, *socket, handshake.topic_name(), peer);
      return;
    }
    IngressPlan plan = PlanIngress(handshake, peer);
    std::string name = "Tunnel ingress " + plan.topic + " from " + peer;
    SpawnOnNewStrand(
        [this, socket, plan = std::move(plan)](async::Context ingress_ctx) {
          IngressCoroutine(ingress_ctx, socket, plan);
        },
        std::move(name));
    return;
  }
  if (!server_->shutting_down_ && !IsPeerClosed(status)) {
    server_->logger_.Log(toolbelt::LogLevel::kWarning,
                         "Tunnel connection from %s failed before its "
                         "handshake: %s",
                         peer.c_str(), status.ToString().c_str());
  }
}

TunnelManager::IngressPlan
TunnelManager::PlanIngress(const Handshake &handshake,
                           const std::string &peer) {
  const tunnel::SubspaceTransport &transport = handshake.subspace();
  IngressPlan plan;
  plan.topic = handshake.topic_name();
  plan.peer = peer;
  plan.checksum_size = tunnel::HandshakeChecksumSize(transport);
  plan.metadata_size = transport.metadata_size();
  plan.wire_prefix_size = tunnel::WirePrefixSize(transport);

  uint64_t slot_size = transport.slot_size() != 0 ? transport.slot_size()
                                                  : tunnel::kDefaultSlotSize;
  int32_t num_slots = transport.num_slots() != 0
                          ? static_cast<int32_t>(transport.num_slots())
                          : tunnel::kDefaultNumSlots;
  PublisherOptions &options = plan.options;
  options.SetBridge(true)
      .SetForTunnel(true)
      .SetReliable(transport.is_reliable())
      .SetType(handshake.channel_type())
      .SetChecksumSize(plan.checksum_size)
      .SetMetadataSize(plan.metadata_size);

  // The publisher must agree with the channel as it exists here or the server
  // refuses it.  Message sizes don't depend on the handshake: the publisher
  // resizes to fit each message.
  ServerChannel *channel = server_->FindChannel(plan.topic);
  if (!server_->IsPublicChannel(channel)) {
    channel = nullptr;
  }
  if (channel == nullptr) {
    if (!transport.mux().empty()) {
      options.SetMux(transport.mux()).SetVchanId(transport.vchan_id());
    }
    options.SetSlotSize(static_cast<SlotSizeType>(slot_size))
        .SetNumSlots(num_slots);
    return plan;
  }

  // The peer's multiplexer only matters when this publisher creates the
  // channel.  A virtual channel's size cap is its multiplexer's.
  ServerChannel *capped = channel;
  if (channel->IsVirtual()) {
    VirtualChannel *vchan = static_cast<VirtualChannel *>(channel);
    options.SetMux(vchan->GetMux()->Name())
        .SetVchanId(vchan->GetVirtualChannelId());
    capped = vchan->GetMux();
  }
  if (!channel->Type().empty() &&
      channel->Type() != handshake.channel_type()) {
    // Peers and local programs may name types differently.
    options.SetType("");
  }
  if (capped->MaxSlotSize() > 0) {
    options.SetMaxSlotSize(capped->MaxSlotSize());
    slot_size = std::min<uint64_t>(
        slot_size, static_cast<uint64_t>(capped->MaxSlotSize()));
  }
  if (!channel->IsPlaceholder()) {
    options.SetSubscriberQueueArenaSize(channel->SubscriberQueueArenaSize());
    int num_pubs, num_subs, num_bridge_pubs, num_bridge_subs, num_tunnel_pubs,
        num_tunnel_subs;
    channel->CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs,
                        num_tunnel_pubs, num_tunnel_subs);
    if (num_pubs > 0) {
      options.SetFixedSize(channel->IsFixedSize());
      num_slots = std::min(num_slots, channel->NumSlots());
      if (channel->IsFixedSize()) {
        slot_size = std::min<uint64_t>(slot_size, channel->SlotSize());
      }
    }
  }
  options.SetSlotSize(static_cast<SlotSizeType>(slot_size))
      .SetNumSlots(num_slots);
  return plan;
}

void TunnelManager::IngressCoroutine(
    async::Context ctx, std::shared_ptr<async::StreamSocket> socket,
    IngressPlan plan) {
  if (server_->shutting_down_) {
    return;
  }
  const std::string &topic = plan.topic;
  Client client(ctx);
  if (absl::Status s =
          client.Init(server_->socket_name_, "tunnel ingress " + topic);
      !s.ok()) {
    server_->logger_.Log(toolbelt::LogLevel::kError,
                         "Tunnel can't connect to the server for %s: %s",
                         topic.c_str(), s.ToString().c_str());
    return;
  }
  absl::StatusOr<Publisher> pub = client.CreatePublisher(topic, plan.options);
  if (!pub.ok()) {
    server_->logger_.Log(toolbelt::LogLevel::kError,
                         "Tunnel can't publish %s for %s: %s", topic.c_str(),
                         plan.peer.c_str(), pub.status().ToString().c_str());
    return;
  }
  if (pub->PrefixSize() != plan.wire_prefix_size) {
    server_->logger_.Log(toolbelt::LogLevel::kError,
                         "Tunnel %s from %s: the channel's prefix is %d bytes "
                         "but the peer's is %d",
                         topic.c_str(), plan.peer.c_str(), pub->PrefixSize(),
                         plan.wire_prefix_size);
    return;
  }
  server_->logger_.Log(toolbelt::LogLevel::kInfo,
                       "Tunnel receiving %s from %s", topic.c_str(),
                       plan.peer.c_str());

  const size_t prefix_size = static_cast<size_t>(plan.wire_prefix_size);
  absl::Status status;
  while (!server_->shutting_down_) {
    absl::StatusOr<PacketHeader> header = ReadHeader(*socket, ctx);
    if (!header.ok()) {
      status = header.status();
      break;
    }
    if (header->type == PacketType::kHandshake) {
      status = absl::InvalidArgumentError("unexpected second handshake");
      break;
    }
    const size_t body_size = header->BodySize();
    if (header->type != PacketType::kData || body_size < prefix_size) {
      if (header->type == PacketType::kData) {
        stats_.malformed_received++;
      }
      status = Skip(*socket, body_size, ctx);
      if (!status.ok()) {
        break;
      }
      continue;
    }
    const size_t payload_size = body_size - prefix_size;
    const bool too_large =
        payload_size >
            static_cast<size_t>(std::numeric_limits<SlotSizeType>::max()) ||
        (pub->MaxSlotSize() > 0 &&
         payload_size > static_cast<size_t>(pub->MaxSlotSize())) ||
        (pub->IsFixedSize() &&
         payload_size > static_cast<size_t>(pub->SlotSize()));
    if (payload_size == 0 || too_large) {
      if (too_large) {
        stats_.oversized_received++;
        server_->logger_.Log(toolbelt::LogLevel::kWarning,
                             "Tunnel dropped a %zu byte message on %s from %s: "
                             "too large for the channel",
                             payload_size, topic.c_str(), plan.peer.c_str());
      }
      status = Skip(*socket, body_size, ctx);
      if (!status.ok()) {
        break;
      }
      continue;
    }

    absl::StatusOr<void *> buffer;
    for (;;) {
      buffer = pub->GetMessageBuffer(static_cast<SlotSizeType>(payload_size));
      if (!buffer.ok() || *buffer != nullptr || server_->shutting_down_) {
        break;
      }
      // A reliable publisher has to wait for its subscribers to free a slot.
      if (absl::Status s = pub->Wait(ctx); !s.ok()) {
        buffer = s;
        break;
      }
    }
    if (!buffer.ok()) {
      status = buffer.status();
      break;
    }
    if (server_->shutting_down_) {
      if (*buffer != nullptr) {
        pub->CancelPublish();
      }
      break;
    }
    std::byte *prefix_bytes = reinterpret_cast<std::byte *>(pub->Prefix());
    status = ReadFully(*socket, prefix_bytes, prefix_size, ctx);
    if (status.ok()) {
      status = ReadFully(*socket, *buffer, payload_size, ctx);
    }
    if (!status.ok()) {
      pub->CancelPublish();
      break;
    }
    MessagePrefix *prefix = pub->Prefix();
    if (prefix->message_size != payload_size) {
      stats_.malformed_received++;
      pub->CancelPublish();
      continue;
    }
    if (prefix->IsActivation()) {
      pub->CancelPublish();
      continue;
    }
    // The checksum doesn't cover the bridged flag.  The sender's virtual
    // channel ID stays in the prefix, where the checksum does cover it, and
    // the message goes to this publisher's virtual channel.
    prefix->SetIsBridged();
    absl::StatusOr<const Message> published =
        pub->PublishMessageWithPrefix(static_cast<int64_t>(payload_size),
                                      /*use_slot_id_from_prefix=*/false,
                                      /*use_vchan_id_from_prefix=*/false);
    if (!published.ok()) {
      status = published.status();
      break;
    }
    stats_.messages_received++;
  }
  if (!server_->shutting_down_ && !IsPeerClosed(status)) {
    server_->logger_.Log(toolbelt::LogLevel::kWarning,
                         "Tunnel stopped receiving %s from %s: %s",
                         topic.c_str(), plan.peer.c_str(),
                         status.ToString().c_str());
  } else if (!server_->shutting_down_) {
    server_->logger_.Log(toolbelt::LogLevel::kInfo,
                         "Tunnel connection for %s from %s closed",
                         topic.c_str(), plan.peer.c_str());
  }
}

void TunnelManager::EgressCoroutine(async::Context ctx, EgressRoute route) {
  if (server_->shutting_down_) {
    return;
  }
  const std::string &topic = route.topic;
  const std::string &peer_name = route.peer.name();
  Client client(ctx);
  if (absl::Status s =
          client.Init(server_->socket_name_, "tunnel egress " + topic);
      !s.ok()) {
    server_->logger_.Log(toolbelt::LogLevel::kError,
                         "Tunnel can't connect to the server for %s: %s",
                         topic.c_str(), s.ToString().c_str());
    return;
  }
  std::string last_failure;
  while (!server_->shutting_down_) {
    if (!route.address.Valid()) {
      absl::StatusOr<toolbelt::InetAddress> address =
          ResolveIPv4(route.peer.tcp_addr(), route.peer.tcp_port());
      if (!address.ok()) {
        LogFailure(last_failure,
                   absl::StrFormat("Tunnel can't send %s to %s: %s", topic,
                                   peer_name, address.status().ToString()));
        SleepUnlessShutdown(ctx, kReconnectInterval);
        continue;
      }
      route.address = *address;
    }
    absl::StatusOr<async::StreamSocket> socket =
        async::ConnectStream(route.address, kConnectTimeout, ctx);
    if (server_->shutting_down_) {
      break;
    }
    if (!socket.ok()) {
      LogFailure(last_failure,
                 absl::StrFormat("Tunnel can't connect to %s for %s: %s",
                                 peer_name, topic, socket.status().ToString()));
      SleepUnlessShutdown(ctx, kReconnectInterval);
      continue;
    }
    SetNoDelay(*socket);
    absl::Status status = ForwardChannel(ctx, client, *socket, route);
    socket->Close();
    if (server_->shutting_down_) {
      break;
    }
    LogFailure(last_failure,
               absl::StrFormat("Tunnel stopped sending %s to %s: %s", topic,
                               peer_name, status.ToString()));
    SleepUnlessShutdown(ctx, kReconnectInterval);
  }
}

void TunnelManager::LogFailure(std::string &last_failure,
                               std::string failure) {
  server_->logger_.Log(failure == last_failure ? toolbelt::LogLevel::kDebug
                                               : toolbelt::LogLevel::kInfo,
                       "%s", failure.c_str());
  last_failure = std::move(failure);
}

absl::Status TunnelManager::ForwardChannel(async::Context ctx, Client &client,
                                           async::StreamSocket &socket,
                                           const EgressRoute &route) {
  const LocalPublication &publication = route.publication;
  SubscriberOptions options;
  options.SetBridge(true).SetForTunnel(true).SetReliable(publication.reliable);
  if (!publication.mux.empty()) {
    options.SetMux(publication.mux).SetVchanId(publication.vchan_id);
  }
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(route.topic, options);
  if (!sub.ok()) {
    return sub.status();
  }

  Handshake handshake;
  handshake.set_topic_name(route.topic);
  handshake.set_channel_type(sub->Type());
  tunnel::SubspaceTransport *transport = handshake.mutable_subspace();
  // Subscriber::SlotSize() is the current message's slot size; buffers grow,
  // so the last is the channel's.
  const auto &buffers = sub->GetBuffers();
  transport->set_slot_size(buffers.empty() ? 0 : buffers.back()->slot_size);
  transport->set_num_slots(static_cast<uint32_t>(sub->NumSlots()));
  transport->set_is_reliable(publication.reliable);
  transport->set_mux(publication.mux);
  transport->set_vchan_id(publication.mux.empty() ? -1 : publication.vchan_id);
  transport->set_checksum_size(sub->ChecksumSize());
  transport->set_metadata_size(sub->MetadataSize());
  if (absl::Status s = SendHandshake(socket, handshake, ctx); !s.ok()) {
    return s;
  }
  server_->logger_.Log(toolbelt::LogLevel::kInfo, "Tunnel sending %s to %s",
                       route.topic.c_str(), route.peer.name().c_str());

  const int32_t prefix_size = sub->PrefixSize();
  const toolbelt::FileDescriptor socket_fd(async::StreamSocketFd(socket),
                                           /*owned=*/false);
  uint64_t seq = 0;
  std::vector<std::byte> packet;
  for (;;) {
    absl::StatusOr<int> fd = sub->Wait(socket_fd, ctx);
    if (server_->shutting_down_) {
      return absl::OkStatus();
    }
    if (!fd.ok()) {
      return fd.status();
    }
    if (*fd == socket_fd.Fd()) {
      // Peers never send on this connection, so this is a close.
      char byte;
      absl::StatusOr<ssize_t> n = socket.Receive(&byte, 1, ctx);
      if (!n.ok()) {
        return n.status();
      }
      if (*n <= 0) {
        return PeerClosed();
      }
      continue;
    }
    for (;;) {
      absl::StatusOr<Message> msg = sub->ReadMessage();
      if (!msg.ok()) {
        return msg.status();
      }
      if (msg->length == 0) {
        break;
      }
      MessageSlot *slot = sub->GetSlot(msg->slot_id);
      const MessagePrefix *prefix =
          slot == nullptr ? nullptr : sub->Prefix(slot);
      if (prefix == nullptr ||
          (prefix->flags & (kMessageBridged | kMessageCrossMachine)) != 0) {
        continue;
      }
      if (sub->PrefixSize() != prefix_size) {
        return absl::FailedPreconditionError("the channel's prefix changed");
      }
      const size_t body_size = static_cast<size_t>(prefix_size) + msg->length;
      absl::StatusOr<PacketHeader> header = tunnel::MakePacketHeader(
          PacketType::kData, body_size, seq + 1, tunnel::RealtimeNowNs());
      if (!header.ok()) {
        stats_.oversized_not_sent++;
        server_->logger_.Log(toolbelt::LogLevel::kWarning,
                             "Tunnel can't send a %zu byte message on %s to "
                             "%s: %s",
                             static_cast<size_t>(msg->length),
                             route.topic.c_str(),
                             route.peer.name().c_str(),
                             header.status().ToString().c_str());
        continue;
      }
      const bool coalesce = msg->length <= kCoalesceLimit;
      packet.resize(tunnel::kPacketHeaderSize + prefix_size +
                    (coalesce ? msg->length : 0));
      tunnel::EncodePacketHeader(*header, packet.data());
      std::byte *wire_prefix = packet.data() + tunnel::kPacketHeaderSize;
      memcpy(wire_prefix, prefix, prefix_size);
      if (coalesce) {
        memcpy(wire_prefix + prefix_size, msg->buffer, msg->length);
      }
      if (absl::Status s = SendAll(socket, packet.data(), packet.size(), ctx);
          !s.ok()) {
        return s;
      }
      if (!coalesce) {
        if (absl::Status s = SendAll(socket, msg->buffer, msg->length, ctx);
            !s.ok()) {
          return s;
        }
      }
      seq++;
      stats_.messages_sent++;
    }
  }
}

void TunnelManager::ScannerCoroutine(async::Context ctx) {
  Rescan();
  while (!server_->shutting_down_) {
    absl::Status status =
        async::WaitReadable(ctx, rescan_trigger_.GetPollFd().Fd());
    if (server_->shutting_down_) {
      break;
    }
    if (!status.ok()) {
      server_->logger_.Log(toolbelt::LogLevel::kError,
                           "Tunnel channel scanner: %s",
                           status.ToString().c_str());
      SleepUnlessShutdown(ctx, std::chrono::seconds(1));
      continue;
    }
    rescan_trigger_.Clear();
    Rescan();
  }
}

bool TunnelManager::Excluded(const std::string &topic) const {
  return tunnel::IsInternalChannel(topic) || denylist_.contains(topic);
}

void TunnelManager::Rescan() {
  bool changed = false;
  for (const auto &[name, channel_ptr] : server_->channels_) {
    ServerChannel *channel = channel_ptr.get();
    if (!server_->IsPublicChannel(channel) || channel->IsMux() ||
        channel->IsLocal() || Excluded(name)) {
      continue;
    }
    bool has_publisher = false;
    bool reliable = false;
    bool has_subscriber = false;
    for (const auto &[id, user] : channel->GetUsers()) {
      if (user == nullptr || user->IsBridge()) {
        continue;
      }
      if (user->IsPublisher()) {
        has_publisher = true;
        reliable |= user->IsReliable();
      } else if (user->IsSubscriber()) {
        has_subscriber = true;
      }
    }
    if (has_publisher) {
      LocalPublication publication{.datatype = channel->Type(),
                                   .reliable = reliable};
      if (channel->IsVirtual()) {
        publication.mux = static_cast<VirtualChannel *>(channel)->GetMux()->Name();
        publication.vchan_id = channel->GetVirtualChannelId();
      }
      auto [it, inserted] = local_publications_.try_emplace(name, publication);
      if (inserted) {
        changed = true;
      } else if (reliable && !it->second.reliable) {
        // Routes already running keep their mode until they reconnect.
        it->second.reliable = true;
        changed = true;
      }
    }
    if (has_subscriber &&
        local_subscriptions_.try_emplace(name, channel->Type()).second) {
      changed = true;
    }
  }
  if (!changed) {
    return;
  }
  RequestDiscoverySend();
  for (const std::unique_ptr<Peer> &peer : peers_) {
    for (const std::string &topic : peer->remote_subscriptions) {
      if (local_publications_.count(topic) > 0) {
        EnsureRoute(topic, *peer);
      }
    }
  }
}

void TunnelManager::RequestDiscoverySend() {
  discovery_generation_++;
  for (const std::unique_ptr<Peer> &peer : peers_) {
    peer->discovery_trigger.Trigger();
  }
}

void TunnelManager::EnsureRoute(const std::string &topic, Peer &peer) {
  if (!routes_.emplace(topic, peer.config.name()).second) {
    return;
  }
  EgressRoute route{.topic = topic,
                    .publication = local_publications_.at(topic),
                    .peer = peer.config,
                    .address = peer.address};
  std::string name = "Tunnel egress " + topic + " to " + peer.config.name();
  SpawnOnNewStrand(
      [this, route = std::move(route)](async::Context ctx) {
        EgressCoroutine(ctx, route);
      },
      std::move(name));
}

tunnel::Discovery TunnelManager::BuildDiscoveryMessage() const {
  tunnel::Discovery msg;
  msg.set_device_name(device_name_);
  for (const auto &[topic, publication] : local_publications_) {
    tunnel::DiscoveryTopic *info = msg.add_published_topics();
    info->set_topic(topic);
    info->set_datatype(publication.datatype);
    info->set_mux(publication.mux);
    info->set_vchan_id(publication.vchan_id);
    info->set_is_reliable(publication.reliable);
  }
  for (const auto &[topic, datatype] : local_subscriptions_) {
    tunnel::DiscoveryTopic *info = msg.add_subscribed_topics();
    info->set_topic(topic);
    info->set_datatype(datatype);
  }
  return msg;
}

void TunnelManager::WaitToReconnect(async::Context ctx, Peer &peer) {
  const int trigger_fd = peer.discovery_trigger.GetPollFd().Fd();
  absl::StatusOr<int> fd =
      async::WaitEither(ctx, trigger_fd,
                        server_->shutdown_trigger_fd_.GetPollFd().Fd(),
                        kReconnectInterval);
  if (fd.ok() && *fd == trigger_fd) {
    peer.discovery_trigger.Clear();
  }
}

void TunnelManager::DiscoverySenderCoroutine(async::Context ctx, Peer *peer) {
  const std::string &name = peer->config.name();
  std::string last_failure;
  while (!server_->shutting_down_) {
    if (!peer->address.Valid()) {
      absl::StatusOr<toolbelt::InetAddress> address =
          ResolveIPv4(peer->config.tcp_addr(), peer->config.tcp_port());
      if (!address.ok()) {
        LogFailure(last_failure,
                   absl::StrFormat("Tunnel can't reach peer %s: %s", name,
                                   address.status().ToString()));
        WaitToReconnect(ctx, *peer);
        continue;
      }
      peer->address = *address;
    }
    absl::StatusOr<async::StreamSocket> socket =
        async::ConnectStream(peer->address, kConnectTimeout, ctx);
    if (server_->shutting_down_) {
      break;
    }
    if (!socket.ok()) {
      LogFailure(last_failure,
                 absl::StrFormat("Tunnel can't reach peer %s: %s", name,
                                 socket.status().ToString()));
      WaitToReconnect(ctx, *peer);
      continue;
    }
    SetNoDelay(*socket);
    absl::Status status = RunDiscoveryConnection(ctx, *socket, *peer);
    socket->Close();
    if (server_->shutting_down_) {
      break;
    }
    LogFailure(last_failure,
               absl::StrFormat("Tunnel discovery connection to %s closed: %s",
                               name, status.ToString()));
    WaitToReconnect(ctx, *peer);
  }
}

absl::Status TunnelManager::RunDiscoveryConnection(async::Context ctx,
                                                   async::StreamSocket &socket,
                                                   Peer &peer) {
  if (absl::Status s = SendHandshake(socket, DiscoveryHandshake(), ctx);
      !s.ok()) {
    return s;
  }
  server_->logger_.Log(toolbelt::LogLevel::kInfo,
                       "Tunnel discovery connected to %s",
                       peer.config.name().c_str());
  const int trigger_fd = peer.discovery_trigger.GetPollFd().Fd();
  const int socket_fd = async::StreamSocketFd(socket);
  uint64_t seq = 0;
  bool send = true;
  auto next_refresh = std::chrono::steady_clock::now();
  for (;;) {
    if (send) {
      // Clear first: a change made while sending triggers another send.
      peer.discovery_trigger.Clear();
      peer.sent_generation = discovery_generation_;
      if (absl::Status s = SendDiscovery(ctx, socket, ++seq); !s.ok()) {
        return s;
      }
      next_refresh = std::chrono::steady_clock::now() + refresh_interval_;
      send = false;
    }
    std::chrono::nanoseconds timeout(0);
    if (refresh_interval_.count() > 0) {
      timeout = std::max<std::chrono::nanoseconds>(
          next_refresh - std::chrono::steady_clock::now(),
          std::chrono::milliseconds(1));
    }
    absl::StatusOr<int> fd = async::WaitEither(ctx, trigger_fd, socket_fd,
                                               timeout);
    if (server_->shutting_down_) {
      return absl::OkStatus();
    }
    if (absl::IsDeadlineExceeded(fd.status())) {
      send = true;
      continue;
    }
    if (!fd.ok()) {
      return fd.status();
    }
    if (*fd == trigger_fd) {
      peer.discovery_trigger.Clear();
      send = peer.sent_generation != discovery_generation_;
      continue;
    }
    // Peers never send on this connection, so this is a close.
    char byte;
    absl::StatusOr<ssize_t> n = socket.Receive(&byte, 1, ctx);
    if (!n.ok()) {
      return n.status();
    }
    if (*n <= 0) {
      return PeerClosed();
    }
  }
}

absl::Status TunnelManager::SendDiscovery(async::Context ctx,
                                          async::StreamSocket &socket,
                                          uint64_t seq) {
  // Never empty, because the device name is required.
  std::string payload = BuildDiscoveryMessage().SerializeAsString();
  const size_t prefix_size =
      static_cast<size_t>(Channel::ComputePrefixSize(4, 0));
  MessagePrefix prefix = {};
  prefix.message_size = payload.size();
  prefix.ordinal = seq;
  prefix.timestamp = toolbelt::Now();
  prefix.vchan_id = -1;
  prefix.checksum_size = 4;
  std::vector<std::byte> body(prefix_size + payload.size());
  memcpy(body.data(), &prefix, sizeof(prefix));
  memcpy(body.data() + prefix_size, payload.data(), payload.size());
  if (absl::Status s = SendPacket(socket, PacketType::kData, seq, body.data(),
                                  body.size(), ctx);
      !s.ok()) {
    return s;
  }
  stats_.discovery_sent++;
  return absl::OkStatus();
}

void TunnelManager::DiscoveryReceiverLoop(async::Context ctx,
                                          async::StreamSocket &socket,
                                          const Handshake &handshake,
                                          const std::string &peer) {
  const size_t prefix_size =
      static_cast<size_t>(tunnel::WirePrefixSize(handshake.subspace()));
  absl::Status status;
  while (!server_->shutting_down_) {
    absl::StatusOr<PacketHeader> header = ReadHeader(socket, ctx);
    if (!header.ok()) {
      status = header.status();
      break;
    }
    const size_t body_size = header->BodySize();
    if (header->type != PacketType::kData || body_size <= prefix_size) {
      if (status = Skip(socket, body_size, ctx); !status.ok()) {
        break;
      }
      continue;
    }
    if (body_size > tunnel::kMaxControlBodySize) {
      status = absl::InvalidArgumentError(absl::StrFormat(
          "discovery message of %d bytes is too large", body_size));
      break;
    }
    std::vector<std::byte> body(body_size);
    if (status = ReadFully(socket, body.data(), body.size(), ctx);
        !status.ok()) {
      break;
    }
    if (server_->shutting_down_) {
      break;
    }
    tunnel::Discovery msg;
    // Protobuf accepts many byte strings that aren't discovery messages, but
    // a real one always names its device.
    if (!msg.ParseFromArray(body.data() + prefix_size,
                            static_cast<int>(body.size() - prefix_size)) ||
        msg.device_name().empty()) {
      stats_.malformed_received++;
      server_->logger_.Log(toolbelt::LogLevel::kWarning,
                           "Tunnel discovery from %s doesn't decode",
                           peer.c_str());
      continue;
    }
    HandleDiscovery(msg);
  }
  if (!server_->shutting_down_ && !IsPeerClosed(status)) {
    server_->logger_.Log(toolbelt::LogLevel::kWarning,
                         "Tunnel discovery from %s stopped: %s", peer.c_str(),
                         status.ToString().c_str());
  }
}

void TunnelManager::DiscardConnection(async::Context ctx,
                                      async::StreamSocket &socket,
                                      const std::string &topic,
                                      const std::string &peer) {
  server_->logger_.Log(toolbelt::LogLevel::kDebug,
                       "Tunnel ignoring internal channel %s from %s",
                       topic.c_str(), peer.c_str());
  std::vector<char> scratch(64 * 1024);
  while (!server_->shutting_down_) {
    absl::StatusOr<ssize_t> n =
        socket.Receive(scratch.data(), scratch.size(), ctx);
    if (!n.ok() || *n <= 0) {
      return;
    }
  }
}

void TunnelManager::HandleDiscovery(const tunnel::Discovery &msg) {
  stats_.discovery_received++;
  auto it = peers_by_name_.find(msg.device_name());
  if (it == peers_by_name_.end()) {
    if (unknown_peers_logged_.insert(msg.device_name()).second) {
      server_->logger_.Log(toolbelt::LogLevel::kWarning,
                           "Tunnel discovery from unknown device \"%s\"; add "
                           "it to the tunnel configuration's peers",
                           msg.device_name().c_str());
    }
    return;
  }
  Peer &peer = *it->second;
  bool reply = false;
  for (const tunnel::DiscoveryTopic &info : msg.published_topics()) {
    if (!Excluded(info.topic()) &&
        peer.remote_publications.insert(info.topic()).second &&
        local_subscriptions_.count(info.topic()) > 0) {
      reply = true;
    }
  }
  for (const tunnel::DiscoveryTopic &info : msg.subscribed_topics()) {
    if (!Excluded(info.topic()) &&
        peer.remote_subscriptions.insert(info.topic()).second &&
        local_publications_.count(info.topic()) > 0) {
      EnsureRoute(info.topic(), peer);
    }
  }
  if (reply) {
    // Peers connect for a topic once they know we subscribe to it.
    RequestDiscoverySend();
  }
}

} // namespace subspace
