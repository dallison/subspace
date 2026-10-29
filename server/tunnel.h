// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client/options.h"
#include "common/async/context.h"
#include "common/async/socket.h"
#include "proto/tunnel_config.pb.h"
#include "proto/tunnel_discovery.pb.h"
#include "proto/tunnel_handshake.pb.h"
#include "toolbelt/sockets.h"
#include "toolbelt/triggerfd.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace subspace {

class Client;
class Server;

// Tunnel-compatible discovery and bridging.  Each configured peer is sent a
// discovery message listing the channels this server publishes and
// subscribes to.  For every channel a peer subscribes to that this server
// publishes, a TCP connection to the peer carries the channel's messages.
//
// The tunnel runs alongside native discovery and bridging but never carries
// their traffic or offers its own to them: its publishers and subscribers are
// bridge users, which native discovery ignores, and it forwards only messages
// published on this server.
//
// Everything except the data connections runs on the server's client strand
// and may use the server's channel state directly.
class TunnelManager {
public:
  struct Stats {
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> messages_sent{0};
    // Messages too large for the local channel, or for the peer.
    std::atomic<uint64_t> oversized_received{0};
    std::atomic<uint64_t> oversized_not_sent{0};
    // Messages dropped for a malformed prefix, and discovery messages that
    // didn't decode.
    std::atomic<uint64_t> malformed_received{0};
    std::atomic<uint64_t> discovery_received{0};
    std::atomic<uint64_t> discovery_sent{0};
  };

  TunnelManager(Server *server, TunnelConfig config);
  ~TunnelManager();

  TunnelManager(const TunnelManager &) = delete;
  TunnelManager &operator=(const TunnelManager &) = delete;

  // Validates the configuration and binds the listener.
  absl::Status Init();

  // Starts the tunnel's coroutines.  Call after Init succeeds.
  void Spawn();

  // Tells the tunnel that local publishers or subscribers have changed.
  void ChannelsChanged();

  const std::string &DeviceName() const { return device_name_; }
  int ListenPort() const;
  const Stats &GetStats() const { return stats_; }

private:
  // A channel this server announces as published.
  struct LocalPublication {
    std::string datatype;
    std::string mux;
    int32_t vchan_id = -1;
    bool reliable = false;
  };

  struct Peer {
    TunnelPeer config;
    // Invalid until the peer's address resolves.
    toolbelt::InetAddress address;
    // Topics the peer has told us it publishes and subscribes to.  Peers only
    // ever add topics.
    absl::flat_hash_set<std::string> remote_publications;
    absl::flat_hash_set<std::string> remote_subscriptions;
    // Wakes the peer's discovery sender.
    toolbelt::TriggerFd discovery_trigger;
    uint64_t sent_generation = 0;
  };

  // What an egress connection needs, copied so it can run off the strand.
  struct EgressRoute {
    std::string topic;
    LocalPublication publication;
    TunnelPeer peer;
    toolbelt::InetAddress address;
  };

  // How to publish what an ingress connection carries.
  struct IngressPlan {
    std::string topic;
    std::string peer;
    PublisherOptions options;
    int32_t checksum_size = 4;
    int32_t metadata_size = 0;
    int32_t wire_prefix_size = 0;
  };

  void ListenerCoroutine(async::Context ctx);
  void HandshakeCoroutine(async::Context ctx,
                          std::shared_ptr<async::StreamSocket> socket,
                          std::string peer);
  void IngressCoroutine(async::Context ctx,
                        std::shared_ptr<async::StreamSocket> socket,
                        IngressPlan plan);
  void EgressCoroutine(async::Context ctx, EgressRoute route);
  absl::Status ForwardChannel(async::Context ctx, Client &client,
                              async::StreamSocket &socket,
                              const EgressRoute &route);
  void ScannerCoroutine(async::Context ctx);
  void DiscoverySenderCoroutine(async::Context ctx, Peer *peer);
  // Waits out the reconnect interval, ending early when there is something
  // new to tell the peer.
  void WaitToReconnect(async::Context ctx, Peer &peer);
  absl::Status RunDiscoveryConnection(async::Context ctx,
                                      async::StreamSocket &socket, Peer &peer);
  absl::Status SendDiscovery(async::Context ctx, async::StreamSocket &socket,
                             uint64_t seq);
  void DiscoveryReceiverLoop(async::Context ctx, async::StreamSocket &socket,
                             const tunnel::Handshake &handshake,
                             const std::string &peer);
  // Reads and discards a connection for an internal channel until the peer
  // closes it.  Closing it instead would make the peer reconnect forever.
  void DiscardConnection(async::Context ctx, async::StreamSocket &socket,
                         const std::string &topic, const std::string &peer);

  IngressPlan PlanIngress(const tunnel::Handshake &handshake,
                          const std::string &peer);
  void Rescan();
  void HandleDiscovery(const tunnel::Discovery &msg);
  void EnsureRoute(const std::string &topic, Peer &peer);
  // Makes every peer's discovery sender send a fresh message.
  void RequestDiscoverySend();
  tunnel::Discovery BuildDiscoveryMessage() const;
  bool Excluded(const std::string &topic) const;
  // Logs a connection failure at info level, or at debug level when it
  // repeats the previous one.
  void LogFailure(std::string &last_failure, std::string failure);
  // Waits on the shutdown trigger so a retry delay ends early at shutdown.
  void SleepUnlessShutdown(async::Context ctx,
                           std::chrono::nanoseconds duration);
  void SpawnOnStrand(std::function<void(async::Context)> fn, std::string name);
  void SpawnOnNewStrand(std::function<void(async::Context)> fn,
                        std::string name);

  Server *server_;
  TunnelConfig config_;
  std::string device_name_;
  absl::flat_hash_set<std::string> denylist_;
  std::chrono::seconds refresh_interval_{0};
  async::StreamSocket listener_;
  toolbelt::TriggerFd rescan_trigger_;

  // Strand-confined.
  std::vector<std::unique_ptr<Peer>> peers_;
  absl::flat_hash_map<std::string, Peer *> peers_by_name_;
  // Ordered so discovery messages list topics in a stable order.  Topics are
  // never removed, matching the peers' additive discovery.
  std::map<std::string, LocalPublication> local_publications_;
  std::map<std::string, std::string> local_subscriptions_;
  absl::flat_hash_set<std::pair<std::string, std::string>> routes_;
  // Bumped whenever the discovery message would change, or a peer needs it
  // again.
  uint64_t discovery_generation_ = 1;
  absl::flat_hash_set<std::string> unknown_peers_logged_;

  Stats stats_;
};

} // namespace subspace
