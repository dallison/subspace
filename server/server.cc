// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "client_handler.h"
#include "common/split_buffer.h"
#include "proto/subspace.pb.h"
#include "proto/tunnel_config.pb.h"
#include "server/tunnel.h"
#include "toolbelt/sockets.h"
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <ifaddrs.h>
#include <net/if.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace subspace {

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
  // Nothing accepts once this loop has exited.  Drop the socket now rather
  // than leaving it to the end of Run(), so a replacement server can bind
  // without waiting for the runtime to wind down.  On Linux the name lives
  // in the abstract namespace and is released as soon as the fd closes.
  listen_socket.Close();
  logger_.Log(toolbelt::LogLevel::kInfo, "Released listen socket %s",
              socket_name_.c_str());
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

  if (tunnel_config_ != nullptr) {
    auto tunnel = std::make_unique<TunnelManager>(this, *tunnel_config_);
    if (absl::Status s = tunnel->Init(); !s.ok()) {
      return s;
    }
    tunnel_ = std::move(tunnel);
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
              if (user->IsPublisher() && !ch->IsTelemetryChannel()) {
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
      // Bind the receiver before the transmitter.  The transmitter asks for
      // an ephemeral port, and the kernel can assign this server's discovery
      // port.  Binding 0.0.0.0 on that port afterwards fails with EADDRINUSE.
      if (absl::Status s = discovery_receiver_.Bind(
              toolbelt::InetAddress::AnyAddress(discovery_port_));
          !s.ok()) {
        return s;
      }

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

  StartRecoveredTelemetryCoroutines();

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
        if (IsPublicChannel(ch.get()) && !ch->IsLocal() && !ch->IsBridgePublisher()) {
          SendAdvertise(name, ch->IsReliable());
        }
      }
    }
  }

  // The tunnel has its own peers, so it runs even for a --local server.
  if (tunnel_ != nullptr) {
    tunnel_->Spawn();
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

void Server::SetTunnelConfig(const TunnelConfig &config) {
  tunnel_config_ = std::make_unique<TunnelConfig>(config);
}

} // namespace subspace
