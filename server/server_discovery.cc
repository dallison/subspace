// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "proto/subspace.pb.h"
#include <vector>

namespace subspace {

static const ServerChannel *SplitBufferOptionsChannel(
    const ServerChannel *channel) {
  if (channel->IsVirtual()) {
    return static_cast<const VirtualChannel *>(channel)->GetMux();
  }
  return channel;
}

static bool ChannelUsesSplitBuffers(const ServerChannel *channel) {
  const ServerChannel *split_channel = SplitBufferOptionsChannel(channel);
  return split_channel->HasSplitBufferOptions() &&
         split_channel->GetSplitBufferOptions().use_split_buffers;
}

static bool ChannelUsesSplitBuffersOverBridge(const ServerChannel *channel) {
  const ServerChannel *split_channel = SplitBufferOptionsChannel(channel);
  return split_channel->HasSplitBufferOptions() &&
         split_channel->GetSplitBufferOptions().split_buffers_over_bridge;
}

// Send a query discovery message for the given channel.  This
// is sent on the IPv4 broadcast address over UDP.
void Server::SendQuery(const std::string &channel_name) {
  if (local_) {
    return;
  }
  // Spawn a coroutine to send the Query message.  Runs on the client strand:
  // it shares the discovery sockets/connections with the discovery coroutines
  // and is itself invoked from client handlers (also on the strand).
  runtime_.SpawnOnStrand(
      [this, channel_name](async::Context ctx) {
        logger_.Log(toolbelt::LogLevel::kDebug,
                    "Sending Query %s with discovery port %d",
                    channel_name.c_str(), discovery_port_);
        Discovery disc;
        disc.set_server_id(server_id_);
        disc.set_port(discovery_port_);
        auto *query = disc.mutable_query();
        query->set_channel_name(channel_name);

        if (absl::Status s = TransmitDiscovery(disc, discovery_addr_, ctx);
            !s.ok()) {
          logger_.Log(toolbelt::LogLevel::kError, "Failed to send Query: %s",
                      s.ToString().c_str());
          return;
        }
      },
      {.name = "Send discovery query",
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
}

// Send an advertise discovery message over UDP.
void Server::SendAdvertise(const std::string &channel_name, bool reliable) {
  if (local_ || !IsPublicChannel(FindChannel(channel_name))) {
    return;
  }
  // Spawn a coroutine to send the Publish message.  Runs on the client strand
  // (shares the discovery sockets/connections with the discovery coroutines and
  // is invoked from client handlers).
  runtime_.SpawnOnStrand(
      [this, channel_name, reliable](async::Context ctx) {
        logger_.Log(toolbelt::LogLevel::kDebug,
                    "Sending Advertise %s with discovery port %d",
                    channel_name.c_str(), discovery_port_);
        Discovery disc;
        disc.set_server_id(server_id_);
        disc.set_port(discovery_port_);
        auto *advertise = disc.mutable_advertise();
        advertise->set_channel_name(channel_name);
        advertise->set_reliable(reliable);
        if (auto it = channels_.find(channel_name); it != channels_.end()) {
          advertise->set_split_buffers(
              ChannelUsesSplitBuffersOverBridge(it->second.get()));
        }
        if (absl::Status s = TransmitDiscovery(disc, discovery_addr_, ctx);
            !s.ok()) {
          logger_.Log(toolbelt::LogLevel::kError,
                      "Failed to send Advertise: %s", s.ToString().c_str());
          return;
        }
      },
      {.name = "Send discovery advertise",
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
}

absl::Status Server::TransmitDiscovery(const Discovery &disc,
                                       const toolbelt::InetAddress &udp_dest,
                                       [[maybe_unused]] async::Context ctx) {
  if (tcp_discovery_) {
    int64_t length = disc.ByteSizeLong();
    // SendMessage writes a 4-byte length prefix immediately before the
    // payload, so reserve room for it at the front of the buffer.
    std::vector<char> buffer(sizeof(int32_t) + length);
    if (!disc.SerializeToArray(buffer.data() + sizeof(int32_t),
                               static_cast<int>(length))) {
      return absl::InternalError("Failed to serialize discovery message");
    }
    // Copy the connection list so a send that drops a connection can't
    // invalidate the iterator.  Writes are blocking (no coroutine yield) so
    // messages from different coroutines can't interleave on a connection.
    auto connections = discovery_connections_;
    for (const auto &conn : connections) {
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
      absl::StatusOr<ssize_t> n = conn->socket->SendMessage(
          buffer.data() + sizeof(int32_t), length, ctx);
#else
      absl::StatusOr<ssize_t> n =
          conn->socket->SendMessage(buffer.data() + sizeof(int32_t), length);
#endif
      if (!n.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to send discovery message to %s: %s",
                    conn->remote.ToString().c_str(), n.status().ToString().c_str());
      }
    }
    return absl::OkStatus();
  }

  char buffer[kDiscoveryBufferSize];
  if (!disc.SerializeToArray(buffer, sizeof(buffer))) {
    return absl::InternalError("Failed to serialize discovery message");
  }
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  return discovery_transmitter_.SendTo(udp_dest, buffer, disc.ByteSizeLong(),
                                       ctx);
#else
  return discovery_transmitter_.SendTo(udp_dest, buffer, disc.ByteSizeLong());
#endif
}

void Server::AddDiscoveryConnection(std::shared_ptr<DiscoveryConnection> conn) {
  discovery_connections_.insert(std::move(conn));
}

void Server::RemoveDiscoveryConnection(
    const std::shared_ptr<DiscoveryConnection> &conn) {
  discovery_connections_.erase(conn);
}

void Server::AdvertiseAllChannels() {
  for (auto &[name, ch] : channels_) {
    if (IsPublicChannel(ch.get()) && !ch->IsLocal() && !ch->IsBridgePublisher()) {
      SendAdvertise(name, ch->IsReliable());
    }
  }
}

// Reads length-delimited Discovery protobufs from a single TCP discovery
// connection and dispatches them to the same handlers used by UDP discovery.
// Returns when the connection is closed or fails.
void Server::DiscoveryConnectionReaderLoop(
    async::Context ctx, std::shared_ptr<DiscoveryConnection> conn) {
  char buffer[kDiscoveryBufferSize];
  for (;;) {
    absl::StatusOr<ssize_t> n =
        conn->socket->ReceiveMessage(buffer, sizeof(buffer), ctx);
    if (!n.ok()) {
      logger_.Log(toolbelt::LogLevel::kDebug,
                  "Discovery connection to %s closed: %s",
                  conn->remote.ToString().c_str(), n.status().ToString().c_str());
      return;
    }
    if (*n == 0) {
      logger_.Log(toolbelt::LogLevel::kDebug,
                  "Discovery connection to %s closed by peer",
                  conn->remote.ToString().c_str());
      return;
    }
    Discovery disc;
    if (!disc.ParseFromArray(buffer, static_cast<int>(*n))) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to parse discovery message");
      continue;
    }
    if (disc.server_id() == server_id_) {
      continue;
    }
    // Build the peer address used as a bridge dedup key, mirroring the UDP
    // path where the sender port is replaced by the advertised discovery port.
    toolbelt::InetAddress sender = conn->remote;
    sender.SetPort(disc.port());
    logger_.Log(toolbelt::LogLevel::kDebug, "Discovery message from %s\n%s",
                sender.ToString().c_str(), disc.DebugString().c_str());
    switch (disc.data_case()) {
    case Discovery::kQuery:
      IncomingQuery(disc.query(), sender);
      break;
    case Discovery::kAdvertise:
      IncomingAdvertise(disc.advertise(), sender, disc.server_id());
      break;
    case Discovery::kSubscribe:
      IncomingSubscribe(disc.subscribe(), sender, disc.server_id());
      break;
    default:
      break;
    }
  }
}

// Listens for an incoming TCP discovery connection and serves each one with a
// reader coroutine.  Used by the server that does not have a peer address.
void Server::DiscoveryListenerCoroutine(async::Context ctx) {
  async::StreamSocket listener;
  if (absl::Status s = listener.Bind(
          toolbelt::InetAddress::AnyAddress(discovery_port_), true);
      !s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to bind TCP discovery listener on port %d: %s",
                discovery_port_, s.ToString().c_str());
    return;
  }
  logger_.Log(toolbelt::LogLevel::kInfo,
              "Listening for TCP discovery connections on port %d",
              discovery_port_);
  for (;;) {
    absl::StatusOr<async::StreamSocket> incoming = listener.Accept(ctx);
    if (!incoming.ok()) {
      if (shutting_down_) {
        return;
      }
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to accept discovery connection: %s",
                  incoming.status().ToString().c_str());
      continue;
    }
    auto conn = std::make_shared<DiscoveryConnection>();
    conn->socket =
        std::make_shared<async::StreamSocket>(std::move(*incoming));
    if (absl::StatusOr<toolbelt::SocketAddress> peer =
            conn->socket->GetPeerName();
        peer.ok() && peer->Type() == toolbelt::SocketAddress::kAddressInet) {
      conn->remote = peer->GetInetAddress();
    }
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Accepted TCP discovery connection from %s",
                conn->remote.ToString().c_str());
    AddDiscoveryConnection(conn);
    // Advertise our channels right away so the peer can bridge without waiting
    // for the next gratuitous advertise cycle.
    AdvertiseAllChannels();
    // Runs on the client strand: the reader handles incoming
    // Advertise/Subscribe messages which mutate channels_ and channel_ids_.
    runtime_.SpawnOnStrand(
        [this, conn](async::Context reader_ctx) {
          DiscoveryConnectionReaderLoop(reader_ctx, conn);
          RemoveDiscoveryConnection(conn);
        },
        {.name = "Discovery reader",
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }
}

// Dials the configured peer for TCP discovery, retrying on failure, and serves
// the connection inline.  Used by the server that has a peer address.
void Server::DiscoveryConnectorCoroutine(async::Context ctx) {
  constexpr int kRetrySecs = 1;
  for (;;) {
    if (shutting_down_) {
      return;
    }
    auto socket = std::make_shared<async::StreamSocket>();
    if (absl::Status s = socket->Connect(peer_address_); !s.ok()) {
      logger_.Log(toolbelt::LogLevel::kDebug,
                  "TCP discovery connect to %s failed: %s; retrying",
                  peer_address_.ToString().c_str(), s.ToString().c_str());
      async::Sleep(ctx, kRetrySecs);
      continue;
    }
    auto conn = std::make_shared<DiscoveryConnection>();
    conn->socket = socket;
    conn->remote = peer_address_;
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Connected TCP discovery to %s",
                peer_address_.ToString().c_str());
    AddDiscoveryConnection(conn);
    AdvertiseAllChannels();
    DiscoveryConnectionReaderLoop(ctx, conn);
    RemoveDiscoveryConnection(conn);
    if (shutting_down_) {
      return;
    }
    async::Sleep(ctx, kRetrySecs);
  }
}

// This coroutine receives discovery messages over UDP.
void Server::DiscoveryReceiverCoroutine(async::Context ctx) {
  char buffer[kDiscoveryBufferSize];

  for (;;) {
    toolbelt::InetAddress sender;
    absl::StatusOr<ssize_t> n = discovery_receiver_.ReceiveFrom(
        sender, buffer, sizeof(buffer), ctx);
    if (!n.ok()) {
      if (shutting_down_) {
        return;
      }
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to read discovery message: %s",
                  n.status().ToString().c_str());
      continue;
    }
    Discovery disc;
    if (!disc.ParseFromArray(buffer, static_cast<int>(*n))) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to parse discovery message");
      continue;
    }
    if (disc.server_id() == server_id_) {
      return;
    }
    sender.SetPort(disc.port());
    logger_.Log(toolbelt::LogLevel::kDebug, "Discovery message from %s\n%s",
                sender.ToString().c_str(), disc.DebugString().c_str());
    switch (disc.data_case()) {
    case Discovery::kQuery:
      IncomingQuery(disc.query(), sender);
      break;
    case Discovery::kAdvertise:
      IncomingAdvertise(disc.advertise(), sender, disc.server_id());
      break;
    case Discovery::kSubscribe:
      IncomingSubscribe(disc.subscribe(), sender, disc.server_id());
      break;
    default:
      break;
    }
  }
}

void Server::SubscribeOverBridge(ServerChannel *channel, bool reliable,
                                 bool split_buffers_over_bridge,
                                 toolbelt::InetAddress publisher) {
  // Read the local channel's split-buffer preference here, on the client
  // strand, and pass it to the receiver coroutine (which runs on the parallel
  // io_context and must not touch channels_).
  const ServerChannel *split_channel = SplitBufferOptionsChannel(channel);
  const bool local_has_split_options = split_channel->HasSplitBufferOptions();
  const bool local_use_split_buffers =
      local_has_split_options &&
      split_channel->GetSplitBufferOptions().use_split_buffers;
  std::string channel_name = channel->Name();
  runtime_.SpawnOnNewStrand(
      [this, channel_name, reliable, split_buffers_over_bridge, publisher,
       local_has_split_options, local_use_split_buffers](async::Context ctx) {
        BridgeReceiverCoroutine(ctx, channel_name, reliable,
                                split_buffers_over_bridge, publisher,
                                local_has_split_options,
                                local_use_split_buffers);
      },
      {.name = absl::StrFormat("Bridge receiver for %s", channel_name),
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
}

void Server::IncomingQuery(const Discovery::Query &query,
                           const toolbelt::InetAddress & /*sender*/) {
  // Someone is asking who publishes a channel.  Do I publish it?  If so,
  // send an Advertise out.
  auto channel = channels_.find(query.channel_name());
  if (channel != channels_.end() && IsPublicChannel(channel->second.get())) {
    if (channel->second->IsLocal() || channel->second->IsBridgePublisher()) {
      return;
    }
    SendAdvertise(query.channel_name(), channel->second->IsReliable());
  }
}

void Server::IncomingAdvertise(const Discovery::Advertise &advertise,
                               const toolbelt::InetAddress &sender,
                               const std::string &server_id) {
  // Do I want to subscribe to this channel?
  auto channel = channels_.find(advertise.channel_name());
  if (channel != channels_.end() && IsPublicChannel(channel->second.get())) {
    if (channel->second->IsBridged(sender, advertise.reliable(), server_id)) {
      // Already bridged to this sender with the same server instance.
      logger_.Log(toolbelt::LogLevel::kDebug,
                  "Channel %s is already bridged to this address",
                  advertise.channel_name().c_str());
      return;
    }
    if (channel->second->IsBridgeSubscriber()) {
      return;
    }
    // Remove any stale bridge entry from a previous server instance
    // before adding the new one.
    channel->second->RemoveBridgedAddress(sender, advertise.reliable());
    channel->second->AddBridgedAddress(sender, advertise.reliable(), server_id);

    int num_pubs, num_subs, num_bridge_pubs, num_bridge_subs;
    int num_tunnel_pubs, num_tunnel_subs;
    channel->second->CountUsers(num_pubs, num_subs, num_bridge_pubs,
                                num_bridge_subs, num_tunnel_pubs,
                                num_tunnel_subs);
    if (num_subs > 0) {
      SubscribeOverBridge(channel->second.get(), advertise.reliable(),
                          advertise.split_buffers(), sender);
    }
  }
}

void Server::IncomingSubscribe(const Discovery::Subscribe &subscribe,
                               const toolbelt::InetAddress &sender,
                               const std::string &server_id) {
  bool sub_reliable = subscribe.reliable();

  auto channel = channels_.find(subscribe.channel_name());
  if (channel != channels_.end() && IsPublicChannel(channel->second.get())) {
    if (channel->second->IsLocal()) {
      return;
    }
    if (channel->second->IsBridged(sender, sub_reliable, server_id)) {
      // Already bridged to this sender with the same server instance.
      logger_.Log(toolbelt::LogLevel::kDebug,
                  "Channel %s is already bridged to this address",
                  subscribe.channel_name().c_str());
      return;
    }
    // We've been asked who publishes a public channel and we do.
    ServerChannel *ch = channel->second.get();
    bool pub_reliable = ch->IsReliable();

    channel->second->RemoveBridgedAddress(sender, sub_reliable);
    channel->second->AddBridgedAddress(sender, sub_reliable, server_id);

    // The subscribe message contains the address and port of the socket
    // listening for our connection for the channel.  The address family is
    // carried in the message itself (FAMILY_INET for TCP, FAMILY_VSOCK for
    // vsock) so the two servers do not have to share the same local address
    // type.  Older peers leave family unset, which defaults to FAMILY_INET.
    absl::StatusOr<toolbelt::SocketAddress> subscriber_addr =
        ParseChannelAddress(subscribe.receiver(), "subscribe receiver");
    if (!subscriber_addr.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Invalid subscribe receiver for %s: %s",
                  subscribe.channel_name().c_str(),
                  subscriber_addr.status().ToString().c_str());
      return;
    }

    bool notify_retirement = !channel->second->GetRetirementFds().empty();
    // Snapshot the channel metadata the transmitter needs while we are still on
    // the client strand.  The transmitter runs on the parallel io_context and
    // must not touch the strand-confined ServerChannel for this state.
    BridgeChannelInfo info{
        .channel_name = ch->Name(),
        .slot_size = ch->SlotSize(),
        .num_slots = ch->NumSlots(),
        .subscriber_queue_arena_size = ch->SubscriberQueueArenaSize(),
        .checksum_size = ch->ChecksumSize(),
        .metadata_size = ch->MetadataSize(),
        .wire_split_buffers = ChannelUsesSplitBuffers(ch),
        .split_buffers_over_bridge = ChannelUsesSplitBuffersOverBridge(ch),
    };
    runtime_.SpawnOnNewStrand(
        [this, info = std::move(info), pub_reliable, sub_reliable,
         subscriber_addr = std::move(*subscriber_addr), sender,
         notify_retirement](async::Context ctx) mutable {
          BridgeTransmitterCoroutine(ctx, std::move(info), pub_reliable,
                                     sub_reliable, std::move(subscriber_addr),
                                     sender, notify_retirement);
        },
        {.name = absl::StrFormat("Bridge transmitter for %s", channel->first)
                     .c_str(),
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

  } else {
    logger_.Log(toolbelt::LogLevel::kDebug, "I don't publish channel %s",
                subscribe.channel_name().c_str());
  }
}

void Server::GratuitousAdvertiseCoroutine(async::Context ctx) {
  constexpr int kPeriodSecs = 5;
  while (!shutting_down_) {
    async::Sleep(ctx, kPeriodSecs);
    if (shutting_down_) {
      break;
    }
    for (auto &channel : channels_) {
      if (IsPublicChannel(channel.second.get()) && !channel.second->IsLocal() && !channel.second->IsBridgePublisher()) {
        SendAdvertise(channel.first, channel.second->IsReliable());
      }
    }
  }
}

} // namespace subspace
