// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSERVERSERVER_H
#define _xSERVERSERVER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client/message.h"
#include "client_handler.h"
#include "co/coroutine.h"
#include "common/async/context.h"
#include "common/async/runtime.h"
#include "common/async/socket.h"
#include "common/async/wait.h"
#include "plugin.h"
#include "proto/subspace.pb.h"
#include "server/plugin.h"
#include "server/server_channel.h"
#include "server/shadow_replicator.h"
#include "toolbelt/bitset.h"
#include "toolbelt/clock.h"
#include "toolbelt/fd.h"
#include "toolbelt/logging.h"
#include "toolbelt/sockets.h"
#include "toolbelt/triggerfd.h"
#include <memory>
#include <mutex>
#include <vector>

namespace subspace {

// Values written to the notify_fd when the server is ready and
// is stopped.
constexpr int64_t kServerReady = 1;
constexpr int64_t kServerStopped = 2;
constexpr int64_t kServerWaiting = 3;

// In multithreaded tests we can't dlclose the plugins because the dynamic linker doesn't
// play well with threads.
void ClosePluginsOnShutdown();
bool ShouldClosePluginsOnShutdown();

absl::StatusOr<toolbelt::SocketAddress>
ParseChannelAddress(const ChannelAddress &address, const char *field_name);

// The Subspace server.
// This is a single-threaded, coroutine-based server that maintains shared
// memory IPC channels and communicates with other servers to allow for
// cross-computer IPC.
class Server {
public:
  // The notify_fd is a file descriptor that the server will write to
  // when it is ready to run (after the socket has been created) and when
  // it is shutting down.  It will write 8 bytes to it so it can be
  // either a pipe or an eventfd.  If notify_fd is -1 then it won't be used.
  // The values are written in host byte order.
  Server(co::CoroutineScheduler &scheduler, const std::string &socket_name,
         const std::string &interface, int disc_port, int peer_port, bool local,
         int notify_fd = -1, int initial_ordinal = 1,
         bool wait_for_clients = false, bool publish_server_channels = true);
  // This constructor can be used when you have a single peer server to talk to.
  Server(co::CoroutineScheduler &scheduler, const std::string &socket_name,
         const std::string &interface, const toolbelt::InetAddress &peer,
         int disc_port, int peer_port, bool local, int notify_fd = -1,
         int initial_ordinal = 1, bool wait_for_clients = false, bool publish_server_channels = true);

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // Asio-backed server.  These mirror the co::CoroutineScheduler constructors
  // above but take an externally-owned boost::asio::io_context that the
  // embedder runs (e.g. from main.cc or a test).  They are additive: the co
  // constructors are unchanged so existing code that builds a Server with a
  // CoroutineScheduler continues to compile and work on the co backend.
  Server(boost::asio::io_context &ioc, const std::string &socket_name,
         const std::string &interface, int disc_port, int peer_port, bool local,
         int notify_fd = -1, int initial_ordinal = 1,
         bool wait_for_clients = false, bool publish_server_channels = true);
  Server(boost::asio::io_context &ioc, const std::string &socket_name,
         const std::string &interface, const toolbelt::InetAddress &peer,
         int disc_port, int peer_port, bool local, int notify_fd = -1,
         int initial_ordinal = 1, bool wait_for_clients = false,
         bool publish_server_channels = true);
#endif

  virtual ~Server();
  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }
  toolbelt::LogLevel GetLogLevel() const { return logger_.GetLogLevel(); }

  // Run the server.  On the Asio backend `num_asio_threads` controls how many
  // threads run the io_context (default 1, i.e. inline on the calling thread).
  // Client handlers and the listener are confined to a strand, so they remain
  // serialized regardless of the thread count; other coroutines (discovery,
  // bridging) can run concurrently across the threads.  The argument is ignored
  // on the co backend, which is always single-threaded.
  absl::Status Run(int num_asio_threads = 1);
  void Stop(bool force = false);

  // The machine name can be used to distinguish between multiple servers
  // running on the same computer.
  void SetMachineName(std::string name) { machine_name_ = std::move(name); }
  const std::string& MachineName() const { return machine_name_; }

  void SetShadowSocket(const std::string &socket_name);
  void SetShadowSockets(const std::string &primary, const std::string &secondary);
  void SimulateCrash() {
    simulate_crash_ = true;
    for (auto &[name, ch] : channels_) {
      ch->SetSkipCleanup(true);
    }
  }
  const std::unique_ptr<ShadowReplicator> &GetPrimaryShadowReplicator() {
    return primary_shadow_replicator_;
  }
  const std::unique_ptr<ShadowReplicator> &GetSecondaryShadowReplicator() {
    return secondary_shadow_replicator_;
  }

  template <typename Fn> void ForEachShadow(Fn fn) {
    if (primary_shadow_replicator_)
      fn(primary_shadow_replicator_);
    if (secondary_shadow_replicator_)
      fn(secondary_shadow_replicator_);
  }

  uint64_t GetVirtualMemoryUsage() const;
  const std::string& GetSocketName() const { return socket_name_; }

  uint64_t GetSessionId() const { return session_id_; }

  absl::StatusOr<toolbelt::FileDescriptor> CreateBridgeNotificationPipe();

  struct BridgePortRange {
    int first_port = 0;
    int last_port = 0;

    bool Enabled() const { return first_port != 0 || last_port != 0; }
  };

  absl::Status SetBridgePortRange(int first_port, int last_port,
                                  bool fallback_to_ephemeral = false);

  void SetCleanupFilesystem(bool v) { cleanup_filesystem_ = v; }

  // Use a TCP connection (instead of UDP broadcast/unicast) for discovery.
  // This is useful when the two servers cannot exchange UDP datagrams
  // directly, for example when one of them runs inside a NAT'd virtual
  // machine or Android emulator.  When enabled, a server with a peer address
  // configured dials the peer; a server without one listens for an incoming
  // discovery connection.
  void SetTcpDiscovery(bool v) { tcp_discovery_ = v; }

  // Address to advertise to peers for this server's bridge (and retirement)
  // listeners, overriding the local interface address.  This lets a NAT'd
  // server advertise a loopback/forwarded endpoint (e.g. 127.0.0.1 reached
  // via `adb forward`/`adb reverse`).  When set, bridge listeners bind to the
  // any-address so the forwarded loopback port reaches them.
  void SetBridgeAdvertiseAddress(const toolbelt::InetAddress &addr) {
    bridge_advertise_address_ = addr;
  }

  // Enable vsock (AF_VSOCK) for bridge and retirement connections instead of
  // TCP.  Discovery still runs over IP (UDP/TCP); only the per-channel bridge
  // data connections switch to vsock.  `advertise_cid` is the context id this
  // server advertises to peers as the address they should connect to - i.e.
  // this server's own local CID (e.g. the guest CID assigned by the
  // hypervisor, VMADDR_CID_HOST on the host, or VMADDR_CID_LOCAL for loopback).
  void SetVsockBridging(bool enable, uint32_t advertise_cid) {
    use_vsock_ = enable;
    vsock_cid_ = advertise_cid;
  }

  void CleanupFilesystem();
  void CleanupAfterSession();

  absl::Status LoadPlugin(const std::string &name, const std::string &path);
  absl::Status LoadBuiltinPlugin(const std::string &name,
                                 std::unique_ptr<PluginInterface> interface);
  absl::Status UnloadPlugin(const std::string &name);

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
  virtual co::CoroutineScheduler &GetScheduler() { return runtime_.scheduler(); }
#endif

  // Backend-agnostic way for plugins (and internal code) to spawn a coroutine
  // on the server's runtime.  The spawned function receives the active
  // async::Context (a co::Coroutine* under the co backend, a yield_context
  // under Asio) which it threads into client/socket/wait calls.
  void SpawnCoroutine(std::function<void(async::Context)> fn,
                      async::SpawnOptions opts = {}) {
    runtime_.Spawn(std::move(fn), std::move(opts));
  }

  absl::flat_hash_map<std::string, std::unique_ptr<ServerChannel>> &
  GetChannels() {
    return channels_;
  }

  int GetShutdownTriggerFd() {
    return shutdown_trigger_fd_.GetPollFd().Fd();
  }

  bool ShuttingDown() const { return shutting_down_; }

  size_t GetNumChannels() const { return channels_.size(); }

  absl::Status HandleIncomingConnection(async::Context ctx,
                                        async::UnixSocket &listen_socket);

  // Create a channel in both process and shared memory.  For a placeholder
  // subscriber, the channel parameters are not known, so slot_size and
  // num_slots will be zero.
  absl::StatusOr<ServerChannel *> CreateChannel(const std::string &channel_name,
                                                int slot_size, int num_slots,
                                                const std::string &mux,
                                                int vchan_id, std::string type);
  absl::StatusOr<ServerChannel *>
  CreateMultiplexer(const std::string &channel_name, int slot_size,
                    int num_slots, std::string type);
  absl::Status RemapChannel(ServerChannel *channel, int slot_size,
                            int num_slots);
  ServerChannel *FindChannel(const std::string &channel_name);
  void RemoveChannel(ServerChannel *channel);

private:
  friend class ClientHandler;
  friend class ServerChannel;
  friend class VirtualChannel;
  static constexpr size_t kDiscoveryBufferSize = 1024;

  // A single TCP discovery connection to a peer server.  Used only when
  // tcp_discovery_ is enabled.
  struct DiscoveryConnection {
    std::shared_ptr<async::StreamSocket> socket;
    // Remote address of the peer (without the discovery port, which is taken
    // from each received message), used as a stable key for bridge dedup.
    toolbelt::InetAddress remote;
  };

  struct Plugin {
    Plugin(const std::string &n, void *h, std::unique_ptr<PluginInterface> i)
        : name(n), handle(h), interface(std::move(i)) {}
    ~Plugin() {
      if (handle) {
        if (ShouldClosePluginsOnShutdown()) {
          dlclose(handle);
        }
      }
    }
    std::string name;
    void *handle = nullptr;
    std::unique_ptr<PluginInterface> interface;
  };

  absl::Status RecoverFromShadow(RecoveredState &state);

  void ForeachChannel(std::function<void(ServerChannel*)> func);

  void RemoveAllUsersFor(ClientHandler *handler);
  void CloseHandler(ClientHandler *handler);
  void NotifyViaFd(int64_t val);
  void CreateShutdownTrigger();
  void ListenerCoroutine(async::Context ctx, async::UnixSocket &listen_socket);
  void ChannelDirectoryCoroutine(async::Context ctx);
  void SendChannelDirectory();
  void StatisticsCoroutine(async::Context ctx);
  void DiscoveryReceiverCoroutine(async::Context ctx);
  void DiscoveryListenerCoroutine(async::Context ctx);
  void DiscoveryConnectorCoroutine(async::Context ctx);
  void DiscoveryConnectionReaderLoop(async::Context ctx,
                                     std::shared_ptr<DiscoveryConnection> conn);
  void AddDiscoveryConnection(std::shared_ptr<DiscoveryConnection> conn);
  void
  RemoveDiscoveryConnection(const std::shared_ptr<DiscoveryConnection> &conn);
  void AdvertiseAllChannels();
  // Send a discovery message to peers.  In TCP mode it is written to every
  // active discovery connection; in UDP mode it is sent to udp_dest.  On the co
  // backend sends are blocking (no coroutine yield) so messages from different
  // coroutines can't interleave on a connection (the Context is ignored).  On
  // the Asio backend the single-threaded io_context can't block, so the Context
  // drives cooperative non-blocking sends.
  absl::Status TransmitDiscovery(const Discovery &disc,
                                 const toolbelt::InetAddress &udp_dest,
                                 async::Context ctx);
  // The address bridge listeners bind to: the any-address when a bridge
  // advertise address is configured, otherwise the local interface address.
  toolbelt::SocketAddress BridgeBindBase() const;
  void PublisherCoroutine(async::Context ctx);
  void SendQuery(const std::string &channel_name);
  void SendAdvertise(const std::string &channel_name, bool reliable);
  // Immutable snapshot of the channel state a bridge transmitter needs.  It is
  // gathered on the client strand (in IncomingSubscribe) before the transmitter
  // is spawned onto the parallel io_context, so the transmitter never has to
  // touch the strand-confined ServerChannel for this metadata.
  struct BridgeChannelInfo {
    std::string channel_name;
    int slot_size = 0;
    int num_slots = 0;
    int32_t checksum_size = 0;
    int32_t metadata_size = 0;
    bool wire_split_buffers = false;
    bool split_buffers_over_bridge = false;
  };
  struct BridgeRetirementState;
  void BridgeTransmitterCoroutine(async::Context ctx, BridgeChannelInfo info,
                                  bool pub_reliable, bool sub_reliable,
                                  toolbelt::SocketAddress subscriber,
                                  toolbelt::InetAddress sender,
                                  bool notify_retirement);
  void BridgeReceiverCoroutine(async::Context ctx, std::string channel_name,
                               bool sub_reliable,
                               bool split_buffers_over_bridge,
                               toolbelt::InetAddress publisher,
                               bool local_has_split_options,
                               bool local_use_split_buffers);
  void RetirementCoroutine(
      async::Context ctx, const std::string &channel_name,
      toolbelt::FileDescriptor &&retirement_fd,
      std::shared_ptr<async::StreamSocket> retirement_transmitter);

  void RetirementReceiverCoroutine(
      async::Context ctx,
      std::shared_ptr<async::StreamSocket> retirement_listener,
      std::shared_ptr<BridgeRetirementState> retirement_state);

  void SubscribeOverBridge(ServerChannel *channel, bool reliable,
                           bool split_buffers_over_bridge,
                           toolbelt::InetAddress publisher);
  void IncomingQuery(const Discovery::Query &query,
                     const toolbelt::InetAddress &sender);
  void IncomingAdvertise(const Discovery::Advertise &advertise,
                         const toolbelt::InetAddress &sender,
                         const std::string &server_id);
  void IncomingSubscribe(const Discovery::Subscribe &subscribe,
                         const toolbelt::InetAddress &sender,
                         const std::string &server_id);
  void GratuitousAdvertiseCoroutine(async::Context ctx);
  absl::Status BindBridgeListener(async::StreamSocket &listener);
  absl::Status RegisterPlugin(const std::string &name, void *handle,
                              std::unique_ptr<PluginInterface> interface);
  absl::Status SendSubscribeMessage(const std::string &channel_name,
                                    bool reliable,
                                    toolbelt::InetAddress publisher,
                                    async::StreamSocket &receiver_listener,
                                    char *buffer, size_t buffer_size,
                                    async::Context ctx);

  static uint64_t AllocateSessionId() { return toolbelt::Now(); }

  // Plugin callers.
  void OnReady();
  void OnNewChannel(const std::string &channel_name);
  void OnRemoveChannel(const std::string &channel_name);
  void OnNewPublisher(const std::string &channel_name, int publisher_id);
  void OnRemovePublisher(const std::string &channel_name, int publisher_id);
  void OnNewSubscriber(const std::string &channel_name, int subscriber_id);
  void OnRemoveSubscriber(const std::string &channel_name, int subscriber_id);
  absl::StatusOr<bool> FreeClientBufferWithPlugins(
      const ClientBufferHandleMetadata &metadata);

  std::string socket_name_;
  uint64_t session_id_;
  std::vector<std::unique_ptr<ClientHandler>> client_handlers_;
  std::string server_id_;
  std::string hostname_;
  std::string interface_;
  toolbelt::SocketAddress my_address_;
  toolbelt::InetAddress peer_address_;
  int discovery_port_;
  int discovery_peer_port_;
  bool local_;
  bool tcp_discovery_ = false;
  toolbelt::InetAddress bridge_advertise_address_;
  // When true, bridge (and retirement) connections use vsock instead of TCP.
  // Discovery still runs over IP; only the bridge data connections change.
  // vsock_cid_ is the context id this server advertises to peers as the one
  // they should connect to (its own local CID).
  bool use_vsock_ = false;
  uint32_t vsock_cid_ = 0;
  absl::flat_hash_set<std::shared_ptr<DiscoveryConnection>>
      discovery_connections_;
  toolbelt::FileDescriptor notify_fd_;

  // Atomic only because of testing.
  std::atomic<bool> shutting_down_ = false;

  absl::flat_hash_map<std::string, std::unique_ptr<ServerChannel>> channels_;

  SystemControlBlock *scb_;
  toolbelt::FileDescriptor scb_fd_;
  toolbelt::BitSet<kMaxChannels> channel_ids_;
  async::AsyncRuntime runtime_;

  toolbelt::TriggerFd channel_directory_trigger_fd_;
  toolbelt::InetAddress discovery_addr_;
  async::UDPSocket discovery_transmitter_;
  async::UDPSocket discovery_receiver_;
  toolbelt::Logger logger_;

  // Optional pipe to allow test to be notified when discovery sets up a
  // new connection.  The server will send an encoded protobuf Subscribed
  // message through this pipe if it is set up.
  toolbelt::Pipe bridge_notification_pipe_;

  int initial_ordinal_ = 1;

  bool wait_for_clients_ = false;
  bool simulate_crash_ = false;
  bool cleanup_filesystem_ = false;

  // In tests we will load a plugin while the server is running.  This needs a
  // lock.
  std::mutex plugin_lock_;

  std::vector<std::unique_ptr<Plugin>> plugins_;
  toolbelt::TriggerFd shutdown_trigger_fd_;
  std::string machine_name_;
  bool publish_server_channels_ = true;
  BridgePortRange bridge_port_range_;
  bool bridge_ports_fallback_to_ephemeral_ = false;

  std::unique_ptr<ShadowReplicator> primary_shadow_replicator_;
  std::unique_ptr<ShadowReplicator> secondary_shadow_replicator_;
};

} // namespace subspace

#endif // _xSERVERSERVER_H
