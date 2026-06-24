// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "common/split_buffer.h"
#include "client_handler.h"
#include "proto/subspace.pb.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/sockets.h"
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <ifaddrs.h>
#include <net/if.h>
#include <setjmp.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace subspace {

// In multithreaded tests we can't dlclose the plugins because the dynamic
// linker doesn't play well with threads.
static std::atomic<bool> close_plugins_on_shutdown = false;
void ClosePluginsOnShutdown() { close_plugins_on_shutdown = true; }

bool ShouldClosePluginsOnShutdown() { return close_plugins_on_shutdown; }

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

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
static bool CleanupSplitBufferMetadataFile(const std::filesystem::path &path) {
  absl::StatusOr<SplitBufferMetadata> metadata =
      ReadSplitBufferMetadataFile(path.string());
  if (!metadata.ok() || metadata->object_name.empty()) {
    return false;
  }

  (void)DestroySplitSharedMemoryBuffer(*metadata);
  return true;
}

static void CleanupPosixShadowFile(const std::filesystem::path &path) {
  auto status = Channel::PosixSharedMemoryName(path.string());
  if (status.ok()) {
    (void)shm_unlink(status->c_str());
  }
  (void)std::filesystem::remove(path);
}

static void CleanupSubspaceTmpFile(const std::filesystem::path &path) {
  if (CleanupSplitBufferMetadataFile(path)) {
    return;
  }
  CleanupPosixShadowFile(path);
}

static void CleanupSplitBufferObjectFile(const std::filesystem::path &path) {
  std::string object_name = path.filename().string();
  (void)shm_unlink(object_name.c_str());
  (void)std::filesystem::remove(path);
}
#endif

static bool ChannelUsesSplitBuffersOverBridge(const ServerChannel *channel) {
  const ServerChannel *split_channel = SplitBufferOptionsChannel(channel);
  return split_channel->HasSplitBufferOptions() &&
         split_channel->GetSplitBufferOptions().split_buffers_over_bridge;
}

struct BridgeReceiveTarget {
  MessagePrefix *prefix = nullptr;
  char *prefix_without_padding = nullptr;
  size_t prefix_length = 0;
  void *payload = nullptr;
};

struct BridgeReceivedMessage {
  MessagePrefix *prefix = nullptr;
  size_t payload_length = 0;
};

static absl::Status SendSplitBridgeMessage(async::Context ctx,
                                           async::StreamSocket &bridge,
                                           const Message &msg,
                                           const MessagePrefix *prefix,
                                           size_t prefix_area) {
  const char *prefix_addr = reinterpret_cast<const char *>(prefix);
  char *prefix_without_padding =
      const_cast<char *>(prefix_addr) + sizeof(int32_t);
  size_t prefix_length = prefix_area - sizeof(int32_t);
  if (absl::StatusOr<ssize_t> n =
          bridge.SendMessage(prefix_without_padding, prefix_length, ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge prefix: %s",
                        n.status().ToString()));
  }

  int32_t payload_length = htonl(static_cast<int32_t>(msg.length));
  if (absl::StatusOr<ssize_t> n =
          bridge.Send(reinterpret_cast<const char *>(&payload_length),
                      sizeof(payload_length), ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge payload length: %s",
                        n.status().ToString()));
  }
  if (absl::StatusOr<ssize_t> n =
          bridge.Send(static_cast<const char *>(msg.buffer), msg.length,
                      ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge payload: %s",
                        n.status().ToString()));
  }
  return absl::OkStatus();
}

static absl::Status SendCombinedBridgeMessage(async::Context ctx,
                                              async::StreamSocket &bridge,
                                              const Message &msg,
                                              const MessagePrefix *prefix,
                                              size_t prefix_area) {
  const char *prefix_addr = reinterpret_cast<const char *>(prefix);
  // SendMessage uses the 4 bytes immediately below the buffer for the frame
  // length. The MessagePrefix padding exists for this purpose, so the legacy
  // bridge path can write prefix and payload in one chunk.
  char *data_addr = const_cast<char *>(prefix_addr) + sizeof(int32_t);
  size_t message_length = msg.length + prefix_area - sizeof(int32_t);
  if (absl::StatusOr<ssize_t> n =
          bridge.SendMessage(data_addr, message_length, ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge message: %s",
                        n.status().ToString()));
  }
  return absl::OkStatus();
}

static absl::Status SendBridgeMessage(async::Context ctx,
                                      async::StreamSocket &bridge,
                                      const Message &msg,
                                      const MessagePrefix *prefix,
                                      size_t prefix_area,
                                      bool split_buffers_on_wire) {
  if (split_buffers_on_wire) {
    return SendSplitBridgeMessage(ctx, bridge, msg, prefix, prefix_area);
  }
  return SendCombinedBridgeMessage(ctx, bridge, msg, prefix, prefix_area);
}

static absl::StatusOr<size_t>
ReceiveSplitBridgeMessage(async::Context ctx, async::StreamSocket &bridge,
                          const Subscribed &subscribed,
                          const BridgeReceiveTarget &target) {
  absl::StatusOr<ssize_t> prefix = bridge.ReceiveMessage(
      target.prefix_without_padding, target.prefix_length, ctx);
  if (!prefix.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge prefix: %s", prefix.status().ToString()));
  }
  if (static_cast<size_t>(*prefix) != target.prefix_length) {
    return absl::InternalError(
        absl::StrFormat("bridge prefix has invalid length %zd, expected %zu",
                        *prefix, target.prefix_length));
  }
  if (target.prefix->message_size >
      static_cast<uint64_t>(subscribed.slot_size())) {
    return absl::InternalError(absl::StrFormat(
        "bridge payload is too large: %llu > %d",
        static_cast<unsigned long long>(target.prefix->message_size),
        subscribed.slot_size()));
  }

  absl::StatusOr<ssize_t> payload =
      bridge.ReceiveMessage(static_cast<char *>(target.payload),
                            static_cast<size_t>(target.prefix->message_size),
                            ctx);
  if (!payload.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge payload: %s", payload.status().ToString()));
  }
  size_t payload_length = static_cast<size_t>(*payload);
  if (payload_length != static_cast<size_t>(target.prefix->message_size)) {
    return absl::InternalError(absl::StrFormat(
        "bridge payload has invalid length %zu, expected %llu", payload_length,
        static_cast<unsigned long long>(target.prefix->message_size)));
  }
  return payload_length;
}

static absl::StatusOr<size_t>
ReceiveCombinedBridgeMessageForSplitPublisher(async::Context ctx,
                                              async::StreamSocket &bridge,
                                              const Subscribed &subscribed,
                                              const BridgeReceiveTarget &target) {
  std::vector<char> combined(subscribed.slot_size() + target.prefix_length);
  absl::StatusOr<ssize_t> n =
      bridge.ReceiveMessage(combined.data(), combined.size(), ctx);
  if (!n.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge message: %s", n.status().ToString()));
  }
  if (static_cast<size_t>(*n) < target.prefix_length) {
    return absl::InternalError(
        absl::StrFormat("bridge message is too short: %zd < %zu", *n,
                        target.prefix_length));
  }
  memcpy(target.prefix_without_padding, combined.data(), target.prefix_length);
  size_t payload_length = static_cast<size_t>(*n) - target.prefix_length;
  memcpy(target.payload, combined.data() + target.prefix_length,
         payload_length);
  return payload_length;
}

static absl::StatusOr<size_t>
ReceiveCombinedBridgeMessage(async::Context ctx, async::StreamSocket &bridge,
                             const Subscribed &subscribed,
                             const BridgeReceiveTarget &target) {
  absl::StatusOr<ssize_t> n = bridge.ReceiveMessage(
      target.prefix_without_padding,
      subscribed.slot_size() + target.prefix_length, ctx);
  if (!n.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge message: %s", n.status().ToString()));
  }
  if (static_cast<size_t>(*n) < target.prefix_length) {
    return absl::InternalError(absl::StrFormat(
        "bridge message is too short: %zd < %zu", *n, target.prefix_length));
  }
  return static_cast<size_t>(*n) - target.prefix_length;
}

static absl::StatusOr<BridgeReceivedMessage>
ReceiveBridgeMessage(async::Context ctx, async::StreamSocket &bridge,
                     Publisher &pub, const Subscribed &subscribed,
                     void *payload_buffer) {
  // The MessagePrefix struct contains 4 bytes of padding at offset 0. This is
  // to allow SendMessage to use it for the length in the combined wire format.
  size_t prefix_area = pub.PrefixSize();
  BridgeReceiveTarget target = {
      .prefix = pub.Prefix(),
      .prefix_without_padding = nullptr,
      .prefix_length = prefix_area - sizeof(int32_t),
      .payload = payload_buffer,
  };
  if (target.prefix == nullptr) {
    return absl::InternalError("failed to find message prefix");
  }
  char *prefix_addr = reinterpret_cast<char *>(target.prefix);
  target.prefix_without_padding = prefix_addr + sizeof(int32_t);

  if (subscribed.split_buffers()) {
    absl::StatusOr<size_t> payload_length =
        ReceiveSplitBridgeMessage(ctx, bridge, subscribed, target);
    if (!payload_length.ok()) {
      return payload_length.status();
    }
    return BridgeReceivedMessage{.prefix = target.prefix,
                                 .payload_length = *payload_length};
  }
  if (pub.UsesSplitBuffers()) {
    absl::StatusOr<size_t> payload_length =
        ReceiveCombinedBridgeMessageForSplitPublisher(ctx, bridge, subscribed,
                                                      target);
    if (!payload_length.ok()) {
      return payload_length.status();
    }
    return BridgeReceivedMessage{.prefix = target.prefix,
                                 .payload_length = *payload_length};
  }
  absl::StatusOr<size_t> payload_length =
      ReceiveCombinedBridgeMessage(ctx, bridge, subscribed, target);
  if (!payload_length.ok()) {
    return payload_length.status();
  }
  return BridgeReceivedMessage{.prefix = target.prefix,
                               .payload_length = *payload_length};
}

// Look for the IP address and calculate the broadcast address
// for the given interface.  If the interface name is empty
// choose the first interface that supports broadcast and
// has an IP address assigned.
static absl::Status FindIPAddresses(const std::string &interface,
                                    toolbelt::InetAddress &ipaddr,
                                    toolbelt::InetAddress &broadcast_addr,
                                    toolbelt::Logger &logger) {
  if (interface.empty()) {
    logger.Log(
        toolbelt::LogLevel::kInfo,
        "No interface supplied; will choose the first broadcast interface with "
        "an IPv4 address.\nThis may not be what you want.  Please provide a "
        "--interface argument.");
  }
  struct ifaddrs *addrs;

  int e = getifaddrs(&addrs);
  if (e == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to get ifaddrs: %s", strerror(errno)));
  }
  bool found = false;
  for (struct ifaddrs *addr = addrs; addr != nullptr; addr = addr->ifa_next) {
    struct sockaddr *saddr = addr->ifa_addr;
    if (!interface.empty() && interface != addr->ifa_name) {
      continue;
    }
    if (saddr == nullptr) {
      continue;
    }
    if ((addr->ifa_flags & IFF_LOOPBACK) != 0) {
      continue;
    }
    if ((addr->ifa_flags & IFF_BROADCAST) == 0) {
      continue;
    }
    if (saddr->sa_family == AF_INET) {
      // Extract IP address and netmask.
      struct sockaddr_in *ipp = reinterpret_cast<struct sockaddr_in *>(saddr);
      ipaddr = *ipp;
      struct sockaddr_in *nm =
          reinterpret_cast<struct sockaddr_in *>(addr->ifa_netmask);
      in_addr_t netmask = ntohl(nm->sin_addr.s_addr);

      // Construct the broadcast address
      in_addr_t ip = ntohl(ipp->sin_addr.s_addr);
      in_addr_t bcast = ip | ~netmask;
      struct sockaddr_in bcast_sa = *ipp; // Copy most of IP address.
      bcast_sa.sin_addr.s_addr = htonl(bcast);
      broadcast_addr = bcast_sa;
      found = true;
      break;
    }
  }
  freeifaddrs(addrs);
  if (!found) {
    if (interface.empty()) {
      return absl::InternalError("No IPv4 network interfaces can be found");
    }
    return absl::InternalError(
        absl::StrFormat("Interface %s either doesn't exists or can't be used",
                        interface.c_str()));
  }
  return absl::OkStatus();
}

// VMADDR_CID_ANY (== (uint32_t)-1) without requiring <linux/vm_sockets.h>,
// which is Linux-only.  Used to bind a vsock listener that accepts connections
// to any of the host's local context ids.
static constexpr uint32_t kVsockCidAny = 0xFFFFFFFFu;

static absl::StatusOr<toolbelt::SocketAddress>
BridgeAddressWithPort(const toolbelt::SocketAddress &addr, int port) {
  switch (addr.Type()) {
  case toolbelt::SocketAddress::kAddressInet:
    return toolbelt::SocketAddress(
        addr.GetInetAddress().IpAddressInNetworkOrder(), port);
  case toolbelt::SocketAddress::kAddressVirtual:
    return toolbelt::SocketAddress(addr.GetVirtualAddress().Cid(), port);
  default:
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported bridge listener address: %s", addr.ToString()));
  }
}

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
Server::Server(co::CoroutineScheduler &scheduler,
               const std::string &socket_name, const std::string &interface,
               int disc_port, int peer_port, bool local, int notify_fd,
               int initial_ordinal, bool wait_for_clients,
               bool publish_server_channels)
    : socket_name_(socket_name), interface_(interface),
      discovery_port_(disc_port), discovery_peer_port_(peer_port),
      local_(local), notify_fd_(notify_fd), runtime_(scheduler),
      logger_("Subspace server"), initial_ordinal_(initial_ordinal),
      wait_for_clients_(wait_for_clients),
      publish_server_channels_(publish_server_channels) {
  CreateShutdownTrigger();
}

Server::Server(co::CoroutineScheduler &scheduler,
               const std::string &socket_name, const std::string &interface,
               const toolbelt::InetAddress &peer, int disc_port, int peer_port,
               bool local, int notify_fd, int initial_ordinal,
               bool wait_for_clients, bool publish_server_channels)
    : socket_name_(socket_name), interface_(interface), peer_address_(peer),
      discovery_port_(disc_port), discovery_peer_port_(peer_port),
      local_(local), notify_fd_(notify_fd), runtime_(scheduler),
      logger_("Subspace server"), initial_ordinal_(initial_ordinal),
      wait_for_clients_(wait_for_clients),
      publish_server_channels_(publish_server_channels) {
  CreateShutdownTrigger();
}
#else  // SUBSPACE_CORO_BACKEND_ASIO
Server::Server(boost::asio::io_context &ioc, const std::string &socket_name,
               const std::string &interface, int disc_port, int peer_port,
               bool local, int notify_fd, int initial_ordinal,
               bool wait_for_clients, bool publish_server_channels)
    : socket_name_(socket_name), interface_(interface),
      discovery_port_(disc_port), discovery_peer_port_(peer_port),
      local_(local), notify_fd_(notify_fd), runtime_(ioc),
      logger_("Subspace server"), initial_ordinal_(initial_ordinal),
      wait_for_clients_(wait_for_clients),
      publish_server_channels_(publish_server_channels) {
  CreateShutdownTrigger();
}

Server::Server(boost::asio::io_context &ioc, const std::string &socket_name,
               const std::string &interface, const toolbelt::InetAddress &peer,
               int disc_port, int peer_port, bool local, int notify_fd,
               int initial_ordinal, bool wait_for_clients,
               bool publish_server_channels)
    : socket_name_(socket_name), interface_(interface), peer_address_(peer),
      discovery_port_(disc_port), discovery_peer_port_(peer_port),
      local_(local), notify_fd_(notify_fd), runtime_(ioc),
      logger_("Subspace server"), initial_ordinal_(initial_ordinal),
      wait_for_clients_(wait_for_clients),
      publish_server_channels_(publish_server_channels) {
  CreateShutdownTrigger();
}
#endif

absl::Status Server::SetBridgePortRange(int first_port, int last_port,
                                        bool fallback_to_ephemeral) {
  if (first_port == 0 && last_port == 0) {
    bridge_port_range_ = {};
    bridge_ports_fallback_to_ephemeral_ = fallback_to_ephemeral;
    return absl::OkStatus();
  }
  if (first_port <= 0 || last_port <= 0 || first_port > 65535 ||
      last_port > 65535 || first_port > last_port) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "invalid bridge port range %d-%d", first_port, last_port));
  }

  bridge_port_range_ = {.first_port = first_port, .last_port = last_port};
  bridge_ports_fallback_to_ephemeral_ = fallback_to_ephemeral;
  return absl::OkStatus();
}

Server::~Server() {
  // Clear this before other data members get destroyed.
  client_handlers_.clear();
  if (!simulate_crash_) {
    for (auto &[name, channel] : channels_) {
      if (channel != nullptr && !channel->IsVirtual()) {
        channel->RemoveBuffer(session_id_, this);
      }
    }
  }
}

void Server::Stop(bool force) {
  if (shutting_down_) {
    return;
  }
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // The Asio backend replaces the co scheduler's per-coroutine interrupt fd
  // with Asio cancellation.  Mark shutdown so the coroutines' `while
  // (!shutting_down_)` loops exit, let plugins react, trigger the shutdown fd
  // (for anything that also watches it), then ask the runtime to cancel every
  // live coroutine.  Each coroutine's pending wait completes with
  // operation_aborted and it returns while the server is still alive, so
  // nothing touches destroyed state.  Once they have all returned the
  // io_context drains and Run() returns on its own.
  (void)force;
  shutting_down_ = true;
  for (auto &plugin : plugins_) {
    plugin->interface->OnShutdown();
  }
  shutdown_trigger_fd_.Trigger();
  runtime_.StopGracefully();
#else
  if (force || !wait_for_clients_) {
    runtime_.Stop();
    return;
  }
  shutting_down_ = true;
  for (auto &plugin : plugins_) {
    plugin->interface->OnShutdown();
  }
  shutdown_trigger_fd_.Trigger();
  NotifyViaFd(kServerWaiting);
#endif
}

void Server::CreateShutdownTrigger() {
  auto fd = toolbelt::TriggerFd::Create();
  if (!fd.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create shutdown trigger fd: %s",
                fd.status().ToString().c_str());
    abort();
  }
  shutdown_trigger_fd_ = std::move(*fd);
}

toolbelt::SocketAddress Server::BridgeBindBase() const {
  // vsock bridging: bind the listener to (VMADDR_CID_ANY, 0) so it accepts
  // connections addressed to any of this host's local CIDs and the kernel
  // assigns an ephemeral vsock port.  The CID we advertise to peers (vsock_cid_)
  // is handled separately at advertise time.
  if (use_vsock_) {
    return toolbelt::SocketAddress(kVsockCidAny, /*port=*/0);
  }
  // When advertising a separate (e.g. forwarded loopback) address, bind to the
  // any-address so the listener is reachable both on the local interface and
  // via a forwarded loopback port.  Otherwise bind to the local interface.
  if (bridge_advertise_address_.Valid()) {
    return toolbelt::InetAddress::AnyAddress(0);
  }
  return my_address_;
}

absl::Status Server::BindBridgeListener(async::StreamSocket &listener) {
  toolbelt::SocketAddress bind_base = BridgeBindBase();
  if (!bridge_port_range_.Enabled()) {
    return listener.Bind(toolbelt::SocketAddress::AnyPort(bind_base), true);
  }

  absl::Status last_status = absl::UnavailableError("no ports tried");
  for (int port = bridge_port_range_.first_port;
       port <= bridge_port_range_.last_port; ++port) {
    absl::StatusOr<toolbelt::SocketAddress> addr =
        BridgeAddressWithPort(bind_base, port);
    if (!addr.ok()) {
      return addr.status();
    }

    async::StreamSocket candidate;
    absl::Status status = candidate.Bind(*addr, true);
    if (status.ok()) {
      listener = std::move(candidate);
      return absl::OkStatus();
    }
    last_status = status;
  }

  if (bridge_ports_fallback_to_ephemeral_) {
    return listener.Bind(toolbelt::SocketAddress::AnyPort(bind_base), true);
  }

  return absl::UnavailableError(absl::StrFormat(
      "unable to bind bridge listener in configured port range %d-%d: %s",
      bridge_port_range_.first_port, bridge_port_range_.last_port,
      last_status.ToString()));
}

absl::StatusOr<toolbelt::FileDescriptor>
Server::CreateBridgeNotificationPipe() {
  auto status = toolbelt::Pipe::Create();
  if (!status.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to create bridge notification pipe: %s",
                        status.status().ToString().c_str()));
  }
  bridge_notification_pipe_ = *status;
  return bridge_notification_pipe_.ReadFd();
}

void Server::CloseHandler(ClientHandler *handler) {
  for (auto it = client_handlers_.begin(); it != client_handlers_.end(); it++) {
    if (it->get() == handler) {
      client_handlers_.erase(it);
      break;
    }
  }
}

// This coroutine listens for incoming client connections on the given
// UDS and spawns a handler coroutine to handle the communication with
// the client.
void Server::ListenerCoroutine(async::Context ctx,
                               async::UnixSocket &listen_socket) {
  for (;;) {
    if (shutting_down_) {
      // Keep this running until all other coroutines have completed.  This
      // makes sure that other coroutines that own publishers and subscribers
      // are able to connect to the server and delete them on shutdown.  On the
      // co backend the count comes from the scheduler; on Asio it is the number
      // of live (cancellation-tracked) coroutines.  When this listener is the
      // only one left, it is safe to stop.
      auto strings = runtime_.AllCoroutineStrings();
      if (strings.size() == 1) {
        break;
      }
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
      // Shutdown was signalled (our Accept was cancelled by StopGracefully).
      // Stop accepting new connections and instead poll the drain count,
      // yielding the strand via a short sleep so the remaining coroutines
      // (notably the client handlers that service the orderly disconnect of
      // the in-process clients) can run and finish.  Asio cancellation is
      // one-shot, so we cannot simply re-block in Accept and expect to wake
      // again; this poll plays the role the persistently-readable interrupt fd
      // plays on the co backend.
      async::Sleep(ctx, std::chrono::milliseconds(5));
      continue;
#endif
    }
    absl::Status status = HandleIncomingConnection(ctx, listen_socket);
    if (!status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unable to make incoming connection: %s",
                  status.ToString().c_str());
    }
  }
}

void Server::NotifyViaFd(int64_t val) {
  if (!notify_fd_.Valid()) {
    return;
  }
  const char *p = reinterpret_cast<const char *>(&val);
  size_t remaining = sizeof(val);
  while (remaining > 0) {
    ssize_t n = ::write(notify_fd_.Fd(), p, remaining);
    if (n <= 0) {
      return;
    }
    remaining -= n;
    p += n;
  }
}

void Server::ForeachChannel(std::function<void(ServerChannel *)> func) {
  for (auto &channel : channels_) {
    func(channel.second.get());
  }
}

void Server::CleanupAfterSession() {
  std::string session_shm_file_prefix =
      "subspace_" + std::to_string(session_id_);

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  // Android uses anonymous fd-backed shared memory; there are no shm files to
  // remove for a session.

#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  // Remove this session's /tmp shadow files.  Split-buffer metadata shadows are
  // parsed first so their paired subspace_sb_* shm objects can be removed too.
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    if (entry.path().filename().string().rfind(session_shm_file_prefix, 0) ==
        0) {
      CleanupSubspaceTmpFile(entry.path());
    }
  }
  // Remove shadow files for the session's POSIX shm objects.
  std::string session_shm_prefix =
      absl::StrFormat("%08x.", static_cast<uint32_t>(session_id_));
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind(session_shm_prefix, 0) == 0 &&
        (filename.find(".scb.") != std::string::npos ||
         filename.find(".ccb.") != std::string::npos ||
         filename.find(".bcb.") != std::string::npos)) {
      (void)std::filesystem::remove(entry.path());
      // There is also a shm segment with the same name as the file.
      (void)shm_unlink(entry.path().filename().c_str());
    }
  }

#else
  // Remove all files starting with "subspace_SESSION" in /dev/shm.
  for (const auto &entry : std::filesystem::directory_iterator("/dev/shm")) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind(session_shm_file_prefix, 0) == 0) {
      (void)std::filesystem::remove(entry.path());
    }
  }
#endif
}

void Server::CleanupFilesystem() {
  logger_.Log(toolbelt::LogLevel::kInfo, "Cleaning up filesystem...");
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  // Android uses anonymous fd-backed shared memory; there are no shm files to
  // remove.

#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  // Remove Subspace /tmp artifacts.  Split-buffer metadata shadows are parsed
  // before deletion so their paired subspace_sb_* shm objects can be removed.
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind("subspace_sb_", 0) == 0) {
      CleanupSplitBufferObjectFile(entry.path());
    } else if (filename.rfind("subspace_", 0) == 0) {
      CleanupSubspaceTmpFile(entry.path());
    }
  }
  // Remove all files in /tmp that contain .scb., .ccb. or .bcb. in the name.
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    std::string filename = entry.path().filename().string();
    if (filename.find(".scb.") != std::string::npos ||
        filename.find(".ccb.") != std::string::npos ||
        filename.find(".bcb.") != std::string::npos) {
      (void)std::filesystem::remove(entry.path());
      // There is also a shm segment with the same name as the file.
      (void)shm_unlink(entry.path().filename().c_str());
    }
  }

#else
  // Remove all files starting with "subspace_" in /dev/shm.
  for (const auto &entry : std::filesystem::directory_iterator("/dev/shm")) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind("subspace_", 0) == 0) {
      (void)std::filesystem::remove(entry.path());
    }
  }
#endif
}

absl::Status Server::Run(int num_asio_threads) {
  std::vector<struct pollfd> poll_fds;

#ifndef __linux__
  // Remove socket name if it exists.  On Non-Linux systems the socket
  // is a file in the file system.  On Linux it's in the abstract
  // namespace (not a file).
  remove(socket_name_.c_str());
#endif
  session_id_ = AllocateSessionId();
  uint64_t instance_nonce_ = session_id_;

  async::UnixSocket listen_socket;
  absl::Status status = listen_socket.Bind(socket_name_, true);
  if (!status.ok()) {
    return status;
  }

  // Notify listener that the server is ready.  Do this in a coroutine so that
  // it executes when we start running.
  if (notify_fd_.Valid()) {
    runtime_.Spawn([this](async::Context) { NotifyViaFd(kServerReady); });
  }
  OnReady();

  bool recovered = false;
  bool primary_connected = false;
  bool secondary_connected = false;

  // Helper to attempt recovery from a single shadow replicator.
  auto try_recover_from =
      [this](const std::unique_ptr<ShadowReplicator> &replicator,
             const char *label) -> bool {
    absl::Status status = replicator->Connect();
    if (!status.ok()) {
      logger_.Log(toolbelt::LogLevel::kWarning,
                  "Failed to connect to %s shadow: %s", label,
                  status.ToString().c_str());
      return false;
    }
    absl::StatusOr<RecoveredState> state = replicator->ReceiveStateDump();
    if (!state.ok()) {
      logger_.Log(toolbelt::LogLevel::kWarning,
                  "Failed to receive state dump from %s shadow: %s", label,
                  state.status().ToString().c_str());
      return false;
    }
    if (state->session_id == 0 || !state->scb_fd.Valid()) {
      return false;
    }

    // Restore the original session_id so that buffer shared memory
    // file names remain consistent with the files already on disk.
    session_id_ = state->session_id;

    scb_fd_ = std::move(state->scb_fd);
    scb_ = reinterpret_cast<SystemControlBlock *>(
        MapMemory(scb_fd_.Fd(), sizeof(SystemControlBlock),
                  PROT_READ | PROT_WRITE, "SCB"));
    if (scb_ == MAP_FAILED) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to map recovered SCB from %s shadow", label);
      return false;
    }
    absl::Status rs = RecoverFromShadow(*state);
    if (!rs.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to recover from %s shadow: %s", label,
                  rs.ToString().c_str());
      return false;
    }
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Recovered %d channels from %s shadow (session_id=%lu)",
                static_cast<int>(state->channels.size()), label,
                state->session_id);
    return true;
  };

  // Phase 1: Try primary shadow.
  if (primary_shadow_replicator_ != nullptr) {
    if (try_recover_from(primary_shadow_replicator_, "primary")) {
      recovered = true;
    }
    primary_connected = primary_shadow_replicator_->Connected();
  }

  // Phase 2: If not recovered, try secondary shadow.
  if (!recovered && secondary_shadow_replicator_ != nullptr) {
    if (try_recover_from(secondary_shadow_replicator_, "secondary")) {
      recovered = true;
    }
    secondary_connected = secondary_shadow_replicator_->Connected();
  }

  // Phase 3: If still not recovered, create a fresh SCB.
  if (!recovered) {
    if (cleanup_filesystem_) {
      CleanupFilesystem();
    }
    absl::StatusOr<SystemControlBlock *> scb =
        CreateSystemControlBlock(scb_fd_, session_id_);
    if (!scb.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to create SystemControlBlock in shared memory: %s",
          scb.status().ToString().c_str()));
    }
    scb_ = *scb;
  }

  // Connect any shadow that wasn't tried during recovery so it can
  // receive the re-replication.
  if (!primary_connected && primary_shadow_replicator_ != nullptr) {
    if (primary_shadow_replicator_->Connect().ok()) {
      primary_connected = true;
      // Drain the state dump so the shadow is ready for new events.
      if (auto s = primary_shadow_replicator_->ReceiveStateDump(); !s.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to receive state dump from primary shadow: %s",
                    s.status().ToString().c_str());
      }
    }
  }
  if (!secondary_connected && secondary_shadow_replicator_ != nullptr) {
    if (secondary_shadow_replicator_->Connect().ok()) {
      secondary_connected = true;
      if (auto s = secondary_shadow_replicator_->ReceiveStateDump(); !s.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to receive state dump from secondary shadow: %s",
                    s.status().ToString().c_str());
      }
    }
  }

  // Phase 4: Send init and re-replicate recovered state to all connected
  // shadows.
  auto replicate_to_shadow =
      [this, recovered](const std::unique_ptr<ShadowReplicator> &shadow) {
        shadow->SendInit(session_id_, scb_fd_);
        if (recovered) {
          for (auto &[name, ch] : channels_) {
            shadow->SendCreateChannel(ch.get());
            ch->ForEachClientBuffer([&](const RegisteredClientBuffer &buffer) {
              shadow->SendRegisterClientBuffer(buffer.metadata, buffer.fd);
            });
            for (auto &[uid, user] : ch->GetUsers()) {
              if (user == nullptr) {
                continue;
              }
              if (user->IsPublisher()) {
                shadow->SendAddPublisher(
                    name, static_cast<PublisherUser *>(user.get()));
              } else if (user->IsSubscriber()) {
                shadow->SendAddSubscriber(
                    name, static_cast<SubscriberUser *>(user.get()));
              }
            }
          }
        }
      };

  if (primary_connected) {
    replicate_to_shadow(primary_shadow_replicator_);
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Connected to primary shadow process");
  }
  if (secondary_connected) {
    replicate_to_shadow(secondary_shadow_replicator_);
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Connected to secondary shadow process");
  }

  // Create the trigger fd for sending the channel directory.
  if (absl::Status s = channel_directory_trigger_fd_.Open(); !s.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to create channel directory trigger: %s",
                        s.ToString().c_str()));
  }

  char hostname[256] = "local";
  if (!local_) {
    if (gethostname(hostname, sizeof(hostname)) == -1) {
      return absl::InternalError(
          absl::StrFormat("Can't get local hostname: %s", strerror(errno)));
    }

    hostname_ = hostname;
  }

  // Make a unique server id to identify this server.  Include the instance
  // nonce (allocated fresh every Run() call) so that a restarted server gets
  // a different id even within the same process.
  server_id_ = absl::StrFormat("%s.%s.%d.%lu", hostname, socket_name_, getpid(),
                               instance_nonce_);

  if (!local_) {
    // Find the IP and broadcast IPv4 addresses based on the interface supplied
    // as an argument.  If there is no interface, choose the first interface
    // with an IPv4 address that supports broadcast.
    toolbelt::InetAddress ip_addr;
    toolbelt::InetAddress bcast_addr;
    if (absl::Status s =
            FindIPAddresses(interface_, ip_addr, bcast_addr, logger_);
        !s.ok()) {
      // TCP discovery does not need a broadcast-capable interface, and when a
      // bridge advertise address is configured we don't need the local
      // interface address either.  Treat the failure as non-fatal in that
      // case so loopback/NAT setups (e.g. an Android emulator) can run.
      if (tcp_discovery_ && bridge_advertise_address_.Valid()) {
        logger_.Log(toolbelt::LogLevel::kWarning,
                    "Could not determine local interface address (%s); "
                    "continuing with TCP discovery and advertised bridge "
                    "address %s",
                    s.ToString().c_str(),
                    bridge_advertise_address_.ToString().c_str());
      } else {
        return s;
      }
    } else {
      logger_.Log(toolbelt::LogLevel::kInfo, "IPv4: %s, Broadcast: %s",
                  ip_addr.ToString().c_str(), bcast_addr.ToString().c_str());
      my_address_ = ip_addr;
    }

    if (!tcp_discovery_) {
      // Bind the discovery transmitter to the network and any free
      // port on the requested interface.
      if (absl::Status s = discovery_transmitter_.Bind(ip_addr); !s.ok()) {
        return s;
      }

      if (peer_address_.Valid()) {
        // If peer address is supplied, use it.
        discovery_addr_ = peer_address_;
      } else {
        // Otherwise use the broadcast address.
        discovery_addr_ = bcast_addr;
        discovery_addr_.SetPort(discovery_peer_port_);
        if (absl::Status s = discovery_transmitter_.SetBroadcast(); !s.ok()) {
          return s;
        }
      }

      // Open the discovery receiver socket.
      if (absl::Status s = discovery_receiver_.Bind(
              toolbelt::InetAddress::AnyAddress(discovery_port_));
          !s.ok()) {
        return s;
      }
    }
  }

  // TODO: why does this not work?  The BridgeSenderCoroutine causes a terminate
  // because the co::AbortException is not caught in the coroutine caller.  This
  // appears to be a bug somewhere as I can't find why the catch isn't working.
  // We don't need to abort handling anyway.
  runtime_.EnableAborts(false);

  // Start the listener coroutine.  It runs on the client strand so that it is
  // serialized with the client handlers it spawns (they all share the
  // client_handlers_ vector and the server's channel state).
  runtime_.SpawnOnStrand(
      [this, &listen_socket](async::Context ctx) {
        ListenerCoroutine(ctx, listen_socket);
      },
      {.name = "Listener UDS",
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

  // All of the coroutines below run on the client strand because they read or
  // mutate server-wide state (the channels_ map, channel_ids_, per-channel
  // bridge bookkeeping and the shared discovery sockets) that the client
  // handlers also touch.  Confining them to the same strand keeps that state
  // mutex-free even when the io_context runs on multiple threads.  Only the
  // bridge data-plane coroutines (transmitter/receiver and their retirement
  // helpers) run directly on the io_context so their I/O can spread across
  // threads.
  if (publish_server_channels_) {
    // Start the channel directory coroutine.
    runtime_.SpawnOnStrand(
        [this](async::Context ctx) { ChannelDirectoryCoroutine(ctx); },
        {.name = "Channel directory",
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

    // Start the channel stats coroutine.
    runtime_.SpawnOnStrand(
        [this](async::Context ctx) { StatisticsCoroutine(ctx); },
        {.name = "Channel stats",
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }

  if (!local_) {
    if (tcp_discovery_) {
      // TCP discovery: a server with a peer address dials it; otherwise we
      // listen for an incoming discovery connection.
      if (peer_address_.Valid()) {
        runtime_.SpawnOnStrand(
            [this](async::Context ctx) { DiscoveryConnectorCoroutine(ctx); },
            {.name = "Discovery connector",
             .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
      } else {
        runtime_.SpawnOnStrand(
            [this](async::Context ctx) { DiscoveryListenerCoroutine(ctx); },
            {.name = "Discovery listener",
             .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
      }
    } else {
      // Start the discovery receiver coroutine.
      runtime_.SpawnOnStrand(
          [this](async::Context ctx) { DiscoveryReceiverCoroutine(ctx); },
          {.name = "Discovery receiver",
           .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
    }

    // Start the gratuitous Advertiser coroutine.  This sends Advertise messages
    // every 5 seconds.
    runtime_.SpawnOnStrand(
        [this](async::Context ctx) { GratuitousAdvertiseCoroutine(ctx); },
        {.name = "Gratuitous advertiser",
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

    // If we recovered channels, immediately advertise them so that
    // bridges can be re-established without waiting for the first
    // gratuitous advertise cycle.
    if (recovered) {
      for (auto &[name, ch] : channels_) {
        if (!ch->IsLocal() && !ch->IsBridgePublisher()) {
          SendAdvertise(name, ch->IsReliable());
        }
      }
    }
  }

  // Run the coroutine main loop.
  runtime_.Run(num_asio_threads);

  // Notify listener that we're stopped.
  NotifyViaFd(kServerStopped);
  return absl::OkStatus();
}

absl::Status
Server::HandleIncomingConnection(async::Context ctx,
                                 async::UnixSocket &listen_socket) {
  absl::StatusOr<async::UnixSocket> s = listen_socket.Accept(ctx);
  if (!s.ok()) {
    return s.status();
  }
  client_handlers_.push_back(
      std::make_unique<ClientHandler>(this, std::move(*s)));
  ClientHandler *handler_ptr = client_handlers_.back().get();

  // Client handlers run on the client strand so they are serialized with one
  // another and with the listener.  This preserves the single-threaded
  // invariant for all client-handler logic and the shared server state they
  // touch, without any mutexes, even when the io_context runs on many threads.
  runtime_.SpawnOnStrand(
      [this, handler_ptr](async::Context handler_ctx) {
        handler_ptr->Run(handler_ctx);
        CloseHandler(handler_ptr);
      },
      {.name = "Client handler",
       .interrupt_fd =
           wait_for_clients_ ? shutdown_trigger_fd_.GetPollFd().Fd() : -1,
       // On Asio, do not force-cancel client handlers at shutdown: they must
       // stay alive to service the orderly disconnect of the server's
       // in-process clients (statistics / channel directory).  They exit on
       // their own when the peer closes the connection, and the listener waits
       // for them via the drain count before stopping.
       .cancellable = false});

  return absl::OkStatus();
}

absl::StatusOr<ServerChannel *>
Server::CreateMultiplexer(const std::string &channel_name, int slot_size,
                          int num_slots, std::string type) {
  absl::StatusOr<int> channel_id = channel_ids_.Allocate("mux");
  if (!channel_id.ok()) {
    return channel_id.status();
  }
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Creating multiplexer %s with %d slots", channel_name.c_str(),
              num_slots);
  ServerChannel *channel = new ChannelMultiplexer(
      *channel_id, channel_name, num_slots, std::move(type), session_id_);
  channel->SetDebug(logger_.GetLogLevel() <= toolbelt::LogLevel::kVerboseDebug);

  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  channels_.emplace(std::make_pair(channel_name, channel));
  return channel;
}

absl::StatusOr<ServerChannel *>
Server::CreateChannel(const std::string &channel_name, int slot_size,
                      int num_slots, const std::string &mux, int vchan_id,
                      std::string type) {
  if (!mux.empty()) {
    ServerChannel *mux_channel = FindChannel(mux);
    if (mux_channel == nullptr) {
      // No mux found, create one.
      absl::StatusOr<ServerChannel *> m =
          CreateMultiplexer(mux, slot_size, num_slots, type);
      if (!m.ok()) {
        return m.status();
      }
      mux_channel = *m;
    }
    if (!mux_channel->IsMux()) {
      return absl::InternalError(
          absl::StrFormat("Channel %s is not a multiplexer", mux));
    }
    if (mux_channel->IsPlaceholder()) {
      // Remap the memory now that we know the slots.
      absl::Status status = RemapChannel(mux_channel, slot_size, num_slots);
      if (!status.ok()) {
        return status;
      }
    }
    // Create a virtual channel associated with the multiplexer.
    ChannelMultiplexer *m = static_cast<ChannelMultiplexer *>(mux_channel);
    absl::StatusOr<std::unique_ptr<VirtualChannel>> vchan =
        m->CreateVirtualChannel(*this, channel_name, vchan_id);
    if (!vchan.ok()) {
      return vchan.status();
    }
    ServerChannel *channel = vchan->get();

    logger_.Log(toolbelt::LogLevel::kDebug,
                "Creating virtual channel %s with %d slots using mux %s",
                channel_name.c_str(), num_slots, mux.c_str());
    // The channels_ map owns all the server channels.
    channels_.emplace(std::make_pair(channel_name, std::move(*vchan)));
    OnNewChannel(channel_name);
    ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
      s->SendCreateChannel(channel);
    });
    return channel;
  }

  absl::StatusOr<int> channel_id = channel_ids_.Allocate("channel");
  if (!channel_id.ok()) {
    return channel_id.status();
  }
  ServerChannel *channel =
      new ServerChannel(*channel_id, channel_name, num_slots, std::move(type),
                        false, session_id_);
  channel->SetDebug(logger_.GetLogLevel() <= toolbelt::LogLevel::kVerboseDebug);
  channel->SetLastKnownSlotSize(slot_size);

  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  channels_.emplace(std::make_pair(channel_name, channel));
  OnNewChannel(channel_name);
  ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
    s->SendCreateChannel(channel);
  });

  return channel;
}

uint64_t Server::GetVirtualMemoryUsage() const {
  uint64_t size = 0;
  for (const auto &channel : channels_) {
    size += channel.second->GetVirtualMemoryUsage();
  }
  return size;
}

absl::Status Server::RemapChannel(ServerChannel *channel, int slot_size,
                                  int num_slots) {
  if (channel->IsVirtual()) {
    ChannelMultiplexer *mux = static_cast<VirtualChannel *>(channel)->GetMux();
    logger_.Log(toolbelt::LogLevel::kDebug,
                "Remapping multiplexer %s with %d slots",
                channel->Name().c_str(), num_slots);
    return RemapChannel(mux, slot_size, num_slots);
  }
  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetLastKnownSlotSize(slot_size);
  channel->SetSharedMemoryFds(std::move(*fds));
  // Remapping replaces the CCB/BCB FDs; shadow recovery must receive the
  // refreshed descriptors instead of retaining the placeholder mappings.
  ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
    s->SendCreateChannel(channel);
  });
  return absl::OkStatus();
}

ServerChannel *Server::FindChannel(const std::string &channel_name) {
  auto it = channels_.find(channel_name);
  if (it == channels_.end()) {
    return nullptr;
  }
  return it->second.get();
}

absl::Status Server::RecoverFromShadow(RecoveredState &state) {
  for (auto &rch : state.channels) {
    channel_ids_.Set(rch.channel_id);

    auto *channel = new ServerChannel(rch.channel_id, rch.name, rch.num_slots,
                                      rch.type, false, session_id_);
    channel->SetDebug(logger_.GetLogLevel() <=
                      toolbelt::LogLevel::kVerboseDebug);
    channel->SetLastKnownSlotSize(rch.slot_size);
    channel->SetChecksumSize(rch.checksum_size);
    channel->SetMetadataSize(rch.metadata_size);
    if (rch.has_split_buffer_options) {
      if (absl::Status s = channel->ValidateOrSetSplitBufferOptions(
              {.use_split_buffers = rch.use_split_buffers,
               .split_buffers_over_bridge = rch.split_buffers_over_bridge},
              /*set_if_missing=*/true, "shadow recovery");
          !s.ok()) {
        return s;
      }
    }
    if (rch.has_max_publishers) {
      if (absl::Status s = channel->ValidateOrSetMaxPublishers(
              rch.max_publishers, /*set_if_missing=*/true, "shadow recovery");
          !s.ok()) {
        return s;
      }
    }

    if (absl::Status s = channel->MapExisting(scb_fd_, std::move(rch.ccb_fd),
                                              std::move(rch.bcb_fd));
        !s.ok()) {
      return s;
    }
    for (RegisteredClientBuffer &buffer : rch.client_buffers) {
      channel->RegisterClientBuffer(std::move(buffer.metadata),
                                    std::move(buffer.fd));
    }

    for (auto &rpub : rch.publishers) {
      auto pub = std::make_unique<PublisherUser>(
          nullptr, rpub.id, rpub.is_reliable, rpub.is_local, rpub.is_bridge,
          rpub.for_tunnel, rpub.is_fixed_size);

      toolbelt::TriggerFd tfd(rpub.poll_fd, rpub.trigger_fd);
      pub->SetTriggerFd(std::move(tfd));

      if (rpub.notify_retirement) {
        toolbelt::Pipe pipe;
        pipe.ReadFd() = std::move(rpub.retirement_read_fd);
        pipe.WriteFd() = std::move(rpub.retirement_write_fd);
        pub->SetRetirementPipe(std::move(pipe));
      }

      channel->AddUser(rpub.id, std::move(pub));
    }

    for (auto &rsub : rch.subscribers) {
      auto sub = std::make_unique<SubscriberUser>(
          nullptr, rsub.id, rsub.is_reliable, rsub.is_bridge, rsub.for_tunnel,
          rsub.max_active_messages);

      toolbelt::TriggerFd tfd(rsub.trigger_fd, rsub.poll_fd);
      sub->SetTriggerFd(std::move(tfd));

      channel->AddUser(rsub.id, std::move(sub));
    }

    channels_.emplace(rch.name, channel);
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Recovered channel '%s' (id=%d, %d pubs, %d subs)",
                rch.name.c_str(), rch.channel_id,
                static_cast<int>(rch.publishers.size()),
                static_cast<int>(rch.subscribers.size()));
  }
  return absl::OkStatus();
}

void Server::RemoveChannel(ServerChannel *channel) {
  OnRemoveChannel(channel->Name());
  ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
    s->SendRemoveChannel(channel->Name(), channel->GetChannelId());
  });
  if (!simulate_crash_) {
    channel->RemoveBuffer(session_id_, this);
  }
  channel_ids_.Clear(channel->GetChannelId());
  auto it = channels_.find(channel->Name());
  if (it == channels_.end()) {
    return;
  }
  if (it->second->IsVirtual()) {
    auto vchan = static_cast<VirtualChannel *>(it->second.get());
    ChannelMultiplexer *mux = vchan->GetMux();
    mux->RemoveVirtualChannel(vchan);
    if (mux->IsEmpty()) {
      RemoveChannel(mux);
    }
  }
  channels_.erase(it);
  SendChannelDirectory();
}

void Server::RemoveAllUsersFor(ClientHandler *handler) {
  std::vector<ServerChannel *> empty_channels;
  for (auto &channel : channels_) {
    channel.second->RemoveAllUsersFor(handler);
    if (channel.second->IsEmpty()) {
      empty_channels.push_back(channel.second.get());
    }
  }
  // toolbelt::Now remove all empty channels.
  for (auto *channel : empty_channels) {
    RemoveChannel(channel);
  }
}

void Server::ChannelDirectoryCoroutine(async::Context ctx) {
  // Coroutine aware client.
  Client client(ctx);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(
        toolbelt::LogLevel::kError,
        "Failed to initialize Subspace client for channel directory: %s",
        status.ToString().c_str());
    return;
  }
  constexpr int kDirectorySlotSize = 1024;
  constexpr int kDirectoryNumSlots = 32;

  absl::StatusOr<Publisher> channel_directory = client.CreatePublisher(
      "/subspace/ChannelDirectory", kDirectorySlotSize, kDirectoryNumSlots,
      PublisherOptions()
          .SetType("subspace.ChannelDirectory")
          .SetUseSplitBuffers(false));
  if (!channel_directory.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create channel directory channel: %s",
                channel_directory.status().ToString().c_str());
    return;
  }
  while (!shutting_down_) {
    (void)async::WaitReadable(ctx,
                              channel_directory_trigger_fd_.GetPollFd().Fd());
    channel_directory_trigger_fd_.Clear();

    ChannelDirectory directory;
    directory.set_server_id(server_id_);
    for (auto &channel : channels_) {
      auto info = directory.add_channels();
      channel.second->GetChannelInfo(info);
    }
    int64_t length = directory.ByteSizeLong();
    absl::StatusOr<void *> buffer =
        channel_directory->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get channel directory buffer: %s",
                  buffer.status().ToString().c_str());
      return;
    }
    bool ok = directory.SerializeToArray(*buffer, length);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel directory");
      continue;
    }
    absl::StatusOr<const Message> s = channel_directory->PublishMessage(length);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish channel directory: %s",
                  s.status().ToString().c_str());
    }
  }
}

void Server::SendChannelDirectory() { channel_directory_trigger_fd_.Trigger(); }

void Server::StatisticsCoroutine(async::Context ctx) {
  Client client(ctx);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to initialize Subspace client for statistics: %s",
                status.ToString().c_str());
    return;
  }
  constexpr int kStatsSlotSize = 1024;
  constexpr int kStatsNumSlots = 32;

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "/subspace/Statistics", kStatsSlotSize, kStatsNumSlots,
      PublisherOptions()
          .SetType("subspace.Statistics")
          .SetUseSplitBuffers(false));
  if (!pub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create statistics channel: %s",
                pub.status().ToString().c_str());
    return;
  }

  constexpr int kPeriodSecs = 2;
  while (!shutting_down_) {
    async::Sleep(ctx, kPeriodSecs);
    Statistics stats;
    stats.set_timestamp(toolbelt::Now());
    stats.set_server_id(server_id_);
    for (auto &channel : channels_) {
      auto s = stats.add_channels();
      channel.second->GetChannelStats(s);
    }
    int64_t length = stats.ByteSizeLong();

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get channel stats buffer: %s",
                  buffer.status().ToString().c_str());
      return;
    }
    bool ok = stats.SerializeToArray(*buffer, length);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel stats");
      continue;
    }
    absl::StatusOr<const Message> s = pub->PublishMessage(length);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish statistics: %s",
                  s.status().ToString().c_str());
    }
  }
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
  if (local_) {
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
    if (!ch->IsLocal() && !ch->IsBridgePublisher()) {
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
    // Runs on the client strand: the reader handles incoming Advertise/Subscribe
    // messages which mutate channels_ and channel_ids_.
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

// This coroutine creates a subscriber to the given channel and sends
// every message received over the bridge to another server.  It first sends
// a 'Subscribed' message detailing the information about the channel then
// enters a loop reading messages sent to the channel on this side of the
// bridge and sending them over.
void Server::BridgeTransmitterCoroutine(async::Context ctx,
                                        BridgeChannelInfo info,
                                        bool pub_reliable, bool sub_reliable,
                                        toolbelt::SocketAddress subscriber,
                                        toolbelt::InetAddress sender,
                                        bool notify_retirement) {
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeTransmitterCoroutine running");

  // IncomingSubscribe added the bridged-publisher entry (keyed by the discovery
  // `sender` address) before spawning us.  Remove it on every exit path - not
  // just the normal end-of-stream teardown but also the early returns below
  // (failed connect, bind, handshake, ...) - otherwise a failed bridge would
  // leave the channel permanently marked as bridged and block re-establishment.
  // bridged_publishers_ is owned by the client strand, so the erase is posted
  // there; we look the channel up by name in case it was removed meanwhile.
  struct BridgeGuard {
    Server *server;
    std::string channel_name;
    toolbelt::InetAddress sender;
    bool sub_reliable;
    ~BridgeGuard() {
      server->runtime_.RunOnStrand([server = server, channel_name = channel_name,
                                    sender = sender, sub_reliable = sub_reliable]() {
        auto it = server->channels_.find(channel_name);
        if (it != server->channels_.end()) {
          it->second->RemoveBridgedAddress(sender, sub_reliable);
        }
      });
    }
  } bridge_guard{this, info.channel_name, sender, sub_reliable};

  async::StreamSocket bridge;
  if (absl::Status status = bridge.Connect(subscriber); !status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to bridge subscriber: %s",
                status.ToString().c_str());
    return;
  }
  if (absl::Status status = bridge.SetNonBlocking(); !status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError, "%s", status.ToString().c_str());
    return;
  }

  const std::string &channel_name = info.channel_name;

  // Send a Subscribed message to the transmitter.
  logger_.Log(toolbelt::LogLevel::kDebug, "Sending subscribed to %s",
              subscriber.ToString().c_str());
  char buffer[kDiscoveryBufferSize];

  // SendMessage needs 4 bytes below the buffer.
  char *databuf = buffer + sizeof(int32_t);
  size_t buflen = sizeof(buffer) - sizeof(int32_t);

  Subscribed subscribed;
  subscribed.set_channel_name(channel_name);
  subscribed.set_slot_size(info.slot_size);
  subscribed.set_num_slots(info.num_slots);
  subscribed.set_reliable(pub_reliable);
  subscribed.set_checksum_size(info.checksum_size);
  subscribed.set_metadata_size(info.metadata_size);
  const bool wire_split_buffers = info.wire_split_buffers;
  subscribed.set_split_buffers(wire_split_buffers);
  subscribed.set_split_buffers_over_bridge(info.split_buffers_over_bridge);

  async::StreamSocket retirement_listener;
  toolbelt::SocketAddress retirement_addr;

  if (notify_retirement) {
    // We want to be notified when the slot has been retired.
    subscribed.set_notify_retirement(true);
    // Allocate a listen socket to wait for an incoming connection from the
    // other server.

    absl::Status s = BindBridgeListener(retirement_listener);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unable to bind socket for retirement receiver for %s: %s",
                  channel_name.c_str(), s.ToString().c_str());
      return;
    }
    retirement_addr = retirement_listener.BoundAddress();
    logger_.Log(toolbelt::LogLevel::kDebug, "Retirement listener: %s",
                retirement_addr.ToString().c_str());

    // Tell the other side where to connect to notify us of slot retirement
    auto *ret_addr = subscribed.mutable_retirement_socket();
    switch (retirement_addr.Type()) {
    case toolbelt::SocketAddress::kAddressInet: {
      in_addr ip_addr = bridge_advertise_address_.Valid()
                            ? bridge_advertise_address_.IpAddress()
                            : retirement_addr.GetInetAddress().IpAddress();
      ret_addr->set_address(&ip_addr, sizeof(ip_addr));
      ret_addr->set_family(ChannelAddress::FAMILY_INET);
      break;
    }
    case toolbelt::SocketAddress::kAddressVirtual: {
      // vsock: advertise our configured local CID (see SendSubscribeMessage).
      uint32_t cid = vsock_cid_;
      ret_addr->set_address(&cid, sizeof(cid));
      ret_addr->set_family(ChannelAddress::FAMILY_VSOCK);
      break;
    }
    default:
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unsupported address type for retirement notification: %s",
                  retirement_addr.ToString().c_str());
      return;
    }

    ret_addr->set_port(retirement_addr.Port());
  }

  bool ok = subscribed.SerializeToArray(databuf, static_cast<int>(buflen));
  if (!ok) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to serialize subscribed message");
    return;
  }
  int64_t length = subscribed.ByteSizeLong();
  absl::StatusOr<ssize_t> n_sent_1 =
      bridge.SendMessage(databuf, length, ctx);
  if (!n_sent_1.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send subscribed for %s: %s", channel_name.c_str(),
                n_sent_1.status().ToString().c_str());
    return;
  }

  Client client(ctx);
  if (absl::Status s = client.Init(socket_name_); !s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to Subspace server: %s",
                s.ToString().c_str());
    return;
  }
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      channel_name,
      SubscriberOptions().SetReliable(sub_reliable).SetBridge(true));
  if (!sub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create bridge subscriber for %s: %s",
                channel_name.c_str(), sub.status().ToString().c_str());
    return;
  }

  // For retirement notifications, we keep track of the Messages we
  // send over the bridge.  This extends the lifetime of the AciveMessage until
  // the other side of the bridge sends us a retirement notification for the
  // message's slot.
  std::shared_ptr<std::vector<std::shared_ptr<ActiveMessage>>>
      active_retirement_msgs =
          std::make_shared<std::vector<std::shared_ptr<ActiveMessage>>>();
  bool notifying_of_retirement = notify_retirement;

  // Spawn a coroutine to read from the retirement connection.
  if (notifying_of_retirement) {
    active_retirement_msgs->resize(info.num_slots);
    runtime_.SpawnOnNewStrand(
        [this, &retirement_listener,
         active_retirement_msgs](async::Context ret_ctx) mutable {
          return RetirementReceiverCoroutine(ret_ctx, retirement_listener,
                                             active_retirement_msgs);
        },
        {.name = absl::StrFormat("Retirement listener for %s", channel_name),
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }

  if (bridge_notification_pipe_.WriteFd().Valid()) {
    // Also send the Subscribed message to the bridge notification pipe.  This
    // is sent without a coroutine.
    // TODO: use a coroutine to avoid blocking?
    // We send the whole buffer including the 4-byte length.
    if (absl::StatusOr<ssize_t> s = bridge_notification_pipe_.WriteFd().Write(
            buffer, length + sizeof(int32_t));
        !s.ok()) {
      logger_.Log(
          toolbelt::LogLevel::kError,
          "Failed to send subscribed message to bridge notification pipe: %s",
          s.status().ToString().c_str());
      // Ignore the error.
    }
  }

  // Read messages from subscriber and send to bridge socket.
  bool done = false;
  while (!done) {
    if (absl::Status status = sub->Wait(); !status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to wait for subscriber: %s",
                  status.ToString().c_str());
      break;
    }
    // Read all available messages and send to transmitter.
    for (;;) {
      absl::StatusOr<Message> msg = sub->ReadMessageInternal(
          ReadMode::kReadNext, /*pass_activation=*/true,
          /*clear_trigger=*/true);
      if (!msg.ok()) {
        done = true;
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to read message from bridge subscriber for %s: %s",
                    channel_name.c_str(), msg.status().ToString().c_str());
        break;
      }
      if (msg->length == 0) {
        // End of messages, wait for more.
        break;
      }
      // We want to send the MessagePrefix along with the message.
      // The prefix area may be larger than sizeof(MessagePrefix) when
      // checksum_size or metadata_size requires additional 64-byte chunks.
      size_t prefix_area = sub->PrefixSize();
      const MessageSlot *slot = sub->GetSlot(msg->slot_id);
      const MessagePrefix *prefix =
          slot == nullptr ? nullptr
                          : sub->Prefix(const_cast<MessageSlot *>(slot));
      if (prefix == nullptr) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to find message prefix for bridge message on %s",
                    channel_name.c_str());
        continue;
      }
      // NOTE: there's a question here about whether we want to send an
      // activation message across the bridge.  Currently we do send
      // it but the receiver will disregard it.  I don't think we need
      // to actually send it but there's no harm.
      if ((prefix->flags & kMessageBridged) != 0) {
        // This message came from bridge.  We don't forward them again.
        continue;
      }
      // Note that for a reliable publisher where the subscriber is slower
      // we will get backpressure from the receiver because it won't
      // read from the socket.  We will keep transmitting until the TCP
      // buffers fill up, at which point we will stop sending until we
      // get a POLLOUT event.  We are using a nonblocking socket to write
      // to network and the check for EAGAIN is in SendMessage in the
      // socket.  Since we are using coroutines, the EAGAIN will yield
      // this coroutine until POLLOUT says we can try again.
      //
      // The backpressure received here will be applied upwards because
      // we will stop reading the messages from the channel and thus
      // backpressure any publishers writing to that channel.
      if (absl::Status status =
              SendBridgeMessage(ctx, bridge, *msg, prefix, prefix_area,
                                wire_split_buffers);
          !status.ok()) {
        done = true;
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to send bridge message for %s: %s",
                    channel_name.c_str(), status.ToString().c_str());
        break;
      }
      if (notifying_of_retirement) {
        // We need to keep track of the message so that we can retire it
        // when the other side retires the slot.
        (*active_retirement_msgs)[msg->slot_id] =
            std::move(msg->active_message);
      }
    }

    const ChannelCounters &counters = sub->GetCounters();
    if (counters.num_pubs == 0) {
      // No publisher left to send anything.  We're done.
      break;
    }
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Bridge transmitter for %s terminating", channel_name.c_str());
  // bridge_guard removes the bridged-publisher entry (by `sender`) as we unwind.
}

// This coroutine reads retirement messages from the bridge and removes
// the reference to the slot that has been retired on the other side.
// References to the active messages are kept in a vector, indexed by slot_id.
// If these references are the last reference to the slot, it will be retired
// on this side and the publisher's retirement FD is sent the slot.
void Server::RetirementReceiverCoroutine(
    async::Context ctx, async::StreamSocket &retirement_listener,
    std::shared_ptr<std::vector<std::shared_ptr<ActiveMessage>>>
        active_retirement_msgs) {
  // Accept connection on the retirement listener socket.
  absl::StatusOr<async::StreamSocket> retirement_socket =
      retirement_listener.Accept(ctx);
  if (!retirement_socket.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to accept retirement connection: %s",
                retirement_socket.status().ToString().c_str());
    return;
  }

  // Read retirement messages from the socket.
  char buffer[kDiscoveryBufferSize];
  for (;;) {
    absl::StatusOr<ssize_t> n_recv =
        retirement_socket->ReceiveMessage(buffer, sizeof(buffer), ctx);
    if (!n_recv.ok()) {
      return;
    }
    if (*n_recv == 0) {
      // No more messages, we're done.
      break;
    }
    RetirementNotification retirement;
    if (!retirement.ParseFromArray(buffer, static_cast<int>(*n_recv))) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to parse Retirement message");
      continue;
    }
    int slot_id = retirement.slot_id();
    // We always send 1 greater than the slot id to prevent a slot id of 0
    // being sent (which is serialized as 0 bytes.  Remove the adustment.
    slot_id -= 1;

    if (slot_id < 0 ||
        static_cast<size_t>(slot_id) >= active_retirement_msgs->size()) {
      continue;
    }
    (*active_retirement_msgs)[slot_id]->DecRef();
  }
}

// Send a Subscribe message over UDP.
absl::Status
Server::SendSubscribeMessage(const std::string &channel_name, bool reliable,
                             toolbelt::InetAddress publisher,
                             async::StreamSocket &receiver_listener,
                             char *buffer, size_t buffer_size,
                             async::Context ctx) {
  const toolbelt::SocketAddress &receiver_addr =
      receiver_listener.BoundAddress();
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Sending subscribe with bridge receiver socket: %s",
              receiver_addr.ToString().c_str());
  // Send a subscribe request to the publisher.
  Discovery disc;
  disc.set_server_id(server_id_);
  auto *sub = disc.mutable_subscribe();
  sub->set_channel_name(channel_name);
  sub->set_reliable(reliable);
  auto *sub_addr = sub->mutable_receiver();
  switch (receiver_addr.Type()) {
  case toolbelt::SocketAddress::kAddressInet: {
    // IPv4 address.  Advertise the configured bridge address if set, so a
    // NAT'd peer can reach us via a forwarded loopback endpoint.
    in_addr ip_addr = bridge_advertise_address_.Valid()
                          ? bridge_advertise_address_.IpAddress()
                          : receiver_addr.GetInetAddress().IpAddress();
    sub_addr->set_address(&ip_addr, sizeof(ip_addr));
    sub_addr->set_family(ChannelAddress::FAMILY_INET);
    break;
  }
  case toolbelt::SocketAddress::kAddressVirtual: {
    // vsock: advertise our configured local CID (the listener is bound to
    // VMADDR_CID_ANY, so its own CID is not the one peers should dial).
    uint32_t cid = vsock_cid_;
    sub_addr->set_address(&cid, sizeof(cid));
    sub_addr->set_family(ChannelAddress::FAMILY_VSOCK);
    break;
  }
  default:
    return absl::InternalError(
        absl::StrFormat("Unsupported address type for subscribe: %s",
                        receiver_addr.ToString().c_str()));
  }
  sub_addr->set_port(receiver_addr.Port());

  (void)buffer;
  (void)buffer_size;
  logger_.Log(toolbelt::LogLevel::kDebug, "Sending subscribe to %s: %s",
              publisher.ToString().c_str(), disc.DebugString().c_str());
  absl::Status s = TransmitDiscovery(disc, publisher, ctx);
  if (!s.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to send subscribe: %s", s.ToString()));
  }
  return absl::OkStatus();
}

// This coroutine receives messages on a TCP socket and publishes
// them to a local channel.  The messages contain the prefix which
// is sent intact to the channel.  It first receives a 'Subscribed' message
// with the channel details, then enters a loop reading message from the
// bridge and publishing them to the local channel.
void Server::BridgeReceiverCoroutine(async::Context ctx,
                                     std::string channel_name,
                                     bool sub_reliable,
                                     bool split_buffers_over_bridge,
                                     toolbelt::InetAddress publisher,
                                     bool local_has_split_options,
                                     bool local_use_split_buffers) {
  // bridged_publishers_ and channels_ are owned by the client strand, but this
  // coroutine runs on the parallel io_context.  Post the teardown bookkeeping
  // onto the strand (capturing everything by value, since the guard's members
  // are gone by the time the posted work runs) so it is serialized with
  // discovery and the client handlers.
  struct BridgeGuard {
    Server *server;
    std::string channel_name;
    toolbelt::InetAddress publisher;
    bool sub_reliable;
    ~BridgeGuard() {
      server->runtime_.RunOnStrand(
          [server = server, channel_name = channel_name, publisher = publisher,
           sub_reliable = sub_reliable]() {
            auto it = server->channels_.find(channel_name);
            if (it != server->channels_.end()) {
              it->second->RemoveBridgedAddress(publisher, sub_reliable);
            }
          });
    }
  } bridge_guard{this, channel_name, publisher, sub_reliable};

  // Open a listening TCP socket for the incoming bridge connection.
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeReceiverCoroutine running");
  char buffer[kDiscoveryBufferSize];

  async::StreamSocket receiver_listener;
  absl::Status s = BindBridgeListener(receiver_listener);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Unable to bind socket for bridge receiver for %s: %s",
                channel_name.c_str(), s.ToString().c_str());
    return;
  }
  const toolbelt::SocketAddress &receiver_addr =
      receiver_listener.BoundAddress();
  logger_.Log(toolbelt::LogLevel::kDebug, "Bridge receiver socket: %s",
              receiver_addr.ToString().c_str());

  s = SendSubscribeMessage(channel_name, sub_reliable, publisher,
                           receiver_listener, buffer, sizeof(buffer), ctx);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send Subscribe message for channel %s: %s",
                channel_name.c_str(), s.ToString().c_str());
    return;
  }

  // Accept connection on listen socket.
  absl::StatusOr<async::StreamSocket> bridge =
      receiver_listener.Accept(ctx);
  if (!bridge.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to accept incoming bridge connection: %s",
                bridge.status().ToString().c_str());
    return;
  }

  // Wait for the Subscribed message from the server.
  Subscribed subscribed;
  // Recieve it into offset 3 in buffer so that we can write the length for
  // sending it out again to the bridge notification pipe.
  absl::StatusOr<ssize_t> n_recv = bridge->ReceiveMessage(
      buffer + sizeof(int32_t), sizeof(buffer) - sizeof(int32_t), ctx);
  if (!n_recv.ok()) {
    logger_.Log(toolbelt::LogLevel::kError, "Failed to receive Subscribed: %s",
                n_recv.status().ToString().c_str());
    return;
  }
  if (!subscribed.ParseFromArray(buffer + sizeof(int32_t),
                                 static_cast<int>(*n_recv))) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to parse Subscribed message");
    return;
  }
  bool bridge_publisher_split_buffers =
      split_buffers_over_bridge || subscribed.split_buffers_over_bridge();
  // The local channel's split-buffer preference was read on the client strand
  // (in SubscribeOverBridge) and passed in; it overrides the wire value.  We
  // must not look channels_ up here because this coroutine runs on the parallel
  // io_context.
  if (local_has_split_options) {
    bridge_publisher_split_buffers = local_use_split_buffers;
  }

  // Build a publisher to publish incoming bridge messages to the channel.
  Client client(ctx);
  s = client.Init(socket_name_);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to Subspace server: %s",
                s.ToString().c_str());
    return;
  }

  std::unique_ptr<async::StreamSocket> retirement_transmitter;

  if (subscribed.notify_retirement()) {
    retirement_transmitter = std::make_unique<async::StreamSocket>();
    toolbelt::SocketAddress retirement_addr;
    // The address family travels in the message (see SendSubscribeMessage);
    // unset defaults to FAMILY_INET for compatibility with older peers.
    switch (subscribed.retirement_socket().family()) {
    case ChannelAddress::FAMILY_INET: {
      // IPv4 address.
      struct sockaddr_in tmp_addr;
      memset(&tmp_addr, 0, sizeof(tmp_addr));
      tmp_addr.sin_family = AF_INET;
      tmp_addr.sin_port = htons(subscribed.retirement_socket().port());
      tmp_addr.sin_addr.s_addr = ntohl(*reinterpret_cast<const uint32_t *>(
          subscribed.retirement_socket().address().data()));
      retirement_addr = toolbelt::SocketAddress(tmp_addr);
      break;
    }
    case ChannelAddress::FAMILY_VSOCK: {
      // Virtual address.
      struct sockaddr_vm tmp_addr;
      memset(&tmp_addr, 0, sizeof(tmp_addr));
      tmp_addr.svm_family = AF_VSOCK;
      tmp_addr.svm_port = subscribed.retirement_socket().port();
      tmp_addr.svm_cid = *reinterpret_cast<const uint32_t *>(
          subscribed.retirement_socket().address().data());
      retirement_addr = toolbelt::SocketAddress(tmp_addr);
      break;
    }
    default:
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unsupported address family for retirement notification");
      return;
    }

    s = retirement_transmitter->Connect(retirement_addr);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to connect to retirement socket for %s: %s",
                  channel_name.c_str(), s.ToString().c_str());
      return;
    }
  }
  // For a reliable publisher, this will send an activation message
  // to the local channel.
  int32_t bridge_cs = subscribed.checksum_size();
  int32_t bridge_ms = subscribed.metadata_size();
  if (bridge_cs <= 0) {
    bridge_cs = 4;
  }
  if (bridge_ms < 0) {
    bridge_ms = 0;
  }
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      channel_name, subscribed.slot_size(), subscribed.num_slots(),
      PublisherOptions()
          .SetReliable(subscribed.reliable())
          .SetBridge(true)
          .SetNotifyRetirement(subscribed.notify_retirement())
          .SetChecksumSize(bridge_cs)
          .SetMetadataSize(bridge_ms)
          .SetUseSplitBuffers(bridge_publisher_split_buffers));
  if (!pub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create bridge publisher for %s: %s",
                channel_name.c_str(), pub.status().ToString().c_str());
    return;
  }

  toolbelt::FileDescriptor retirement_fd;
  if (subscribed.notify_retirement()) {
    // We need to send the retirement fd to the publisher.
    // This is used to notify us when a slot is retired.
    retirement_fd = pub->GetRetirementFd();
    if (!retirement_fd.Valid()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get retirement fd for %s", channel_name.c_str());
      return;
    }

    // Add a coroutine to listen for retirement notifications and send them to
    // the socket connected to the other server.
    runtime_.SpawnOnNewStrand(
        [this, retirement_fd = std::move(retirement_fd),
         &retirement_transmitter, channel_name](async::Context ret_ctx) mutable {
          RetirementCoroutine(ret_ctx, channel_name, std::move(retirement_fd),
                              std::move(retirement_transmitter));
        },
        {.name = absl::StrFormat("Retirement notifier for %s", channel_name),
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }

  if (bridge_notification_pipe_.WriteFd().Valid()) {
    // Also send the Subscribed message to the bridge notification pipe.  This
    // is sent without a coroutine.
    // TODO: use a coroutine to avoid blocking?
    // We send the whole buffer including the 4-byte length.
    // Write the length in big endian.
    buffer[0] = (*n_recv >> 24) & 0xff;
    buffer[1] = (*n_recv >> 16) & 0xff;
    buffer[2] = (*n_recv >> 8) & 0xff;
    buffer[3] = *n_recv & 0xff;
    if (absl::StatusOr<ssize_t> sz_or =
            bridge_notification_pipe_.WriteFd().Write(
                buffer, *n_recv + sizeof(int32_t));
        !sz_or.ok()) {
      logger_.Log(
          toolbelt::LogLevel::kError,
          "Failed to send subscribed message to bridge notification pipe: %s",
          sz_or.status().ToString().c_str());
      // Ignore the error.
    }
  }

  // Now we receive messages from the bridge and send to the
  // publisher.
  for (;;) {
    absl::StatusOr<void *> buf = pub->GetMessageBuffer();
    if (!buf.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get buffer for bridge subscriber %s: %s",
                  channel_name.c_str(), buf.status().ToString().c_str());
      break;
    }
    if (*buf == nullptr) {
      // Can only happen if publisher is reliable.
      if (!pub->IsReliable()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Got null buffer for unreliable publisher");
        break;
      }
      // Wait for a buffer to become available and try again.
      //
      // Since we don't read the network until we have a place to
      // put the message, we will apply backpressure to the transmitter
      // through buffering in the TCP stack.  When the transmitter's
      // buffers fill up, it will stop sending until we can read the
      // message.  The backpressure is thus propagated up the
      // chain.
      if (absl::Status status = pub->Wait(); !status.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to wait for reliable publisher: %s",
                    status.ToString().c_str());
        break;
      }
      continue;
    }

    absl::StatusOr<BridgeReceivedMessage> bridge_msg =
        ReceiveBridgeMessage(ctx, *bridge, *pub, subscribed, *buf);
    if (!bridge_msg.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to read bridge message for %s: %s",
                  channel_name.c_str(), bridge_msg.status().ToString().c_str());
      break;
    }

    // Set the kMessageBridged flag in the prefix so that this message isn't
    // forwarded again over a bridge.
    MessagePrefix *prefix = bridge_msg->prefix;
    if ((prefix->flags & kMessageActivate) != 0) {
      // Since we have created a reliable publisher and it has sent an
      // activation message through, we don't send another one.
      continue;
    }
    prefix->flags |= kMessageBridged;

    absl::StatusOr<const Message> pub_msg = pub->PublishMessageInternal(
        bridge_msg->payload_length, /*omit_prefix=*/true,
        /*omit_prefix_slot_id=*/true);
    if (!pub_msg.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish bridge message for %s: %s",
                  channel_name.c_str(), pub_msg.status().ToString().c_str());
    }
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Bridge receiver coroutine terminating");
}

// This coroutine receives retirement notifications from the bridge publisher
// and sends those over the bridge to the server that originally published the
// message.  The slot ID sent back to the original server is the slot of the
// original message, not this side's slot.  That is held in the MessagePrefix
// which is kept intact.
void Server::RetirementCoroutine(
    async::Context ctx, const std::string &channel_name,
    toolbelt::FileDescriptor &&retirement_fd,
    std::unique_ptr<async::StreamSocket> retirement_transmitter) {
  logger_.Log(toolbelt::LogLevel::kDebug, "Retirement coroutine for %s running",
              channel_name.c_str());
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // The retirement fd is read on the io_context thread, so the read must never
  // block it.  WaitReadable can report readiness that this reader cannot then
  // satisfy with a blocking read - e.g. a spurious epoll edge, or a second
  // retirement coroutine spawned for the same channel after the bridge
  // re-establishes consuming the byte first.  A blocking read in that case
  // would freeze the entire single-threaded server (every client handler,
  // discovery and bridge coroutine stalls behind it), which manifested as an
  // intermittent hang during client teardown.  Make the fd non-blocking and
  // treat EAGAIN as "wait again", exactly like the socket facade does.
  if (int fl = ::fcntl(retirement_fd.Fd(), F_GETFL, 0); fl >= 0) {
    (void)::fcntl(retirement_fd.Fd(), F_SETFL, fl | O_NONBLOCK);
  }
#endif
  for (;;) {
    int32_t slot_id;
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
    absl::StatusOr<ssize_t> n;
    for (;;) {
      ssize_t r = ::read(retirement_fd.Fd(), &slot_id, sizeof(slot_id));
      if (r >= 0) {
        n = r;
        break;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        if (absl::Status w = async::WaitReadable(ctx, retirement_fd.Fd());
            !w.ok()) {
          return; // Cancelled (graceful shutdown) or wait failed.
        }
        continue;
      }
      return; // Read error, we're done.
    }
#else
    absl::StatusOr<ssize_t> n =
        retirement_fd.Read(&slot_id, sizeof(slot_id), ctx);
    if (!n.ok()) {
      // Failed to read the slot ID, we're done.
      return;
    }
#endif
    if (*n == 0) {
      return; // EOF, we're done.
    }

    // We received a retirement notification, send the fd.
    logger_.Log(toolbelt::LogLevel::kVerboseDebug,
                "Received retirement notification for %s, slot %d",
                channel_name.c_str(), slot_id);

    // Send the retirement fd to the other side.
    RetirementNotification msg;
    // If the slot id is zero the protobuf message will be serialized to 0
    // bytes. Sending a message of zero length is very confusing, so we adjust
    // it by adding 1.  We remove the adjustment when it is received.
    msg.set_slot_id(slot_id + 1);

    char buffer[32];
    bool ok = msg.SerializeToArray(buffer + sizeof(int32_t),
                                   sizeof(buffer) - sizeof(int32_t));
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize retirement notification for %s",
                  channel_name.c_str());
      return;
    }
    absl::StatusOr<ssize_t> nsent = retirement_transmitter->SendMessage(
        buffer + sizeof(int32_t), msg.ByteSizeLong(), ctx);
    if (!nsent.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to send retirement fd for %s: %s",
                  channel_name.c_str(), nsent.status().ToString().c_str());
      return;
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
  if (channel != channels_.end()) {
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
  if (channel != channels_.end()) {
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
  if (channel != channels_.end()) {
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
    toolbelt::SocketAddress subscriber_addr;
    switch (subscribe.receiver().family()) {
    case ChannelAddress::FAMILY_INET: {
      in_addr subscriber_ip;
      memcpy(&subscriber_ip, subscribe.receiver().address().data(),
             sizeof(subscriber_ip));
      // Need this in host byte order.
      subscriber_ip.s_addr = ntohl(subscriber_ip.s_addr);
      subscriber_addr =
          toolbelt::SocketAddress(subscriber_ip, subscribe.receiver().port());
      break;
    }
    case ChannelAddress::FAMILY_VSOCK: {
      uint32_t cid = *reinterpret_cast<const uint32_t *>(
          subscribe.receiver().address().data());
      subscriber_addr =
          toolbelt::SocketAddress(cid, subscribe.receiver().port());
      break;
    }
    default:
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unknown address family for subscribe receiver");
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
        .checksum_size = ch->ChecksumSize(),
        .metadata_size = ch->MetadataSize(),
        .wire_split_buffers = ChannelUsesSplitBuffers(ch),
        .split_buffers_over_bridge = ChannelUsesSplitBuffersOverBridge(ch),
    };
    runtime_.SpawnOnNewStrand(
        [this, info = std::move(info), pub_reliable, sub_reliable,
         subscriber_addr = std::move(subscriber_addr), sender,
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
      if (!channel.second->IsLocal() && !channel.second->IsBridgePublisher()) {
        SendAdvertise(channel.first, channel.second->IsReliable());
      }
    }
  }
}

absl::Status Server::LoadPlugin(const std::string &name,
                                const std::string &path) {
  std::lock_guard<std::mutex> lock(plugin_lock_);

  void *handle = nullptr;
  bool builtin = (path == "BUILTIN");
  if (!builtin) {
    handle = dlopen(path.c_str(), RTLD_LAZY);
    if (handle == nullptr) {
      return absl::InternalError(
          absl::StrFormat("Can't open plugin file %s: %s", path, dlerror()));
    }
  }
  // Form the name of the init function and find it in the shared object.
  std::string interfaceFunc = absl::StrFormat("%s_Create", name);
  void *func = dlsym(builtin ? RTLD_DEFAULT : handle, interfaceFunc.c_str());
  if (func == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Can't find plugin initialization symbol %s: %s",
                        interfaceFunc, dlerror()));
  }
  // Call the init function to get the interface.
  using InitFunc = PluginInterface *(*)();
  InitFunc init = reinterpret_cast<InitFunc>(func);
  std::unique_ptr<PluginInterface> interface(init());

  return RegisterPlugin(name, handle, std::move(interface));
}

absl::Status
Server::LoadBuiltinPlugin(const std::string &name,
                          std::unique_ptr<PluginInterface> interface) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  return RegisterPlugin(name, nullptr, std::move(interface));
}

absl::Status
Server::RegisterPlugin(const std::string &name, void *handle,
                       std::unique_ptr<PluginInterface> interface) {
  // Call the OnStartup function in the loaded plugin.
  absl::Status status = interface->OnStartup(*this, name);
  if (!status.ok()) {
    return status;
  }
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
  interface->SetScheduler(runtime_.scheduler());
#endif
  plugins_.push_back(
      std::make_unique<Plugin>(name, handle, std::move(interface)));
  return absl::OkStatus();
}

absl::Status Server::UnloadPlugin(const std::string &name) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  auto it = std::find_if(
      plugins_.begin(), plugins_.end(),
      [&name](const std::unique_ptr<Plugin> &p) { return p->name == name; });
  if (it == plugins_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("No such plugin %s loaded", name.c_str()));
  }
  plugins_.erase(it);
  return absl::OkStatus();
}

void Server::OnReady() {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnReady(*this);
  }
}

void Server::OnNewChannel(const std::string &channel_name) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewChannel(*this, channel_name);
  }
}

void Server::OnRemoveChannel(const std::string &channel_name) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemoveChannel(*this, channel_name);
  }
}

void Server::OnNewPublisher(const std::string &channel_name, int publisher_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewPublisher(*this, channel_name, publisher_id);
  }
}

void Server::OnRemovePublisher(const std::string &channel_name,
                               int publisher_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemovePublisher(*this, channel_name, publisher_id);
  }
}

void Server::OnNewSubscriber(const std::string &channel_name,
                             int subscriber_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewSubscriber(*this, channel_name, subscriber_id);
  }
}

void Server::OnRemoveSubscriber(const std::string &channel_name,
                                int subscriber_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemoveSubscriber(*this, channel_name, subscriber_id);
  }
}

absl::StatusOr<bool> Server::FreeClientBufferWithPlugins(
    const ClientBufferHandleMetadata &metadata) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    absl::StatusOr<bool> freed =
        plugin->interface->OnFreeClientBuffer(*this, metadata);
    if (!freed.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Plugin %s failed to free client buffer for channel %s "
                  "buffer %u slot %u handle 0x%zx: %s",
                  plugin->name.c_str(), metadata.channel_name.c_str(),
                  metadata.buffer_index, metadata.slot_id,
                  static_cast<size_t>(metadata.handle),
                  freed.status().ToString().c_str());
      continue;
    }
    if (*freed) {
      return true;
    }
  }
  return false;
}

void Server::SetShadowSocket(const std::string &socket_name) {
  SetShadowSockets(socket_name, "");
}

void Server::SetShadowSockets(const std::string &primary,
                              const std::string &secondary) {
  if (!primary.empty()) {
    primary_shadow_replicator_ =
        std::make_unique<ShadowReplicator>(primary, logger_);
  }
  if (!secondary.empty()) {
    secondary_shadow_replicator_ =
        std::make_unique<ShadowReplicator>(secondary, logger_);
  }
}

} // namespace subspace
