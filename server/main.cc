// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "server.h"
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>
#if __has_include(<linux/vm_sockets.h>)
#include <linux/vm_sockets.h>
#endif
#endif

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
#include <boost/asio/io_context.hpp>
#else
static co::CoroutineScheduler *g_scheduler;
void Signal(int sig) {
  printf("\nAll coroutines:\n");
  g_scheduler->Show();
  signal(sig, SIG_DFL);
  raise(sig);
}
#endif

#if defined(__ANDROID__)
ABSL_FLAG(std::string, socket, "/data/local/tmp/subspace",
          "Name of Unix socket to listen on");
#else
ABSL_FLAG(std::string, socket, "/tmp/subspace",
          "Name of Unix socket to listen on");
#endif
ABSL_FLAG(int, disc_port, 6502, "Discovery UDP port");
ABSL_FLAG(int, peer_port, 6502, "Discovery peer UDP port");
ABSL_FLAG(std::string, peer_address, "", "Bridge peer hostname or IP address");
ABSL_FLAG(std::string, bridge_ports, "",
          "TCP bridge port or inclusive port range START-END. Empty uses an "
          "ephemeral port for each bridge listener.");
ABSL_FLAG(bool, bridge_ports_fallback_ephemeral, false,
          "If true, use an ephemeral TCP bridge port when --bridge_ports is "
          "configured but unavailable.");
ABSL_FLAG(std::string, log_level, "info", "Log level");
ABSL_FLAG(std::string, profile_file, "",
          "Optional channel profile textproto file. Empty disables profile "
          "loading, writing, and sizing overrides.");
ABSL_FLAG(std::string, interface, "", "Discovery network interface");
ABSL_FLAG(bool, local, false, "Use local computer only");
ABSL_FLAG(bool, tcp_discovery, false,
          "Use a TCP connection for discovery instead of UDP broadcast/unicast. "
          "A server with --peer_address dials it; a server without one listens "
          "on --disc_port. Useful across NAT (e.g. an Android emulator/VM).");
ABSL_FLAG(std::string, bridge_advertise_address, "",
          "IP address to advertise to peers for this server's bridge "
          "listeners, overriding the local interface address (e.g. 127.0.0.1 "
          "reached via adb forward/reverse). Bridge listeners bind to the "
          "any-address when this is set.");
ABSL_FLAG(int, notify_fd, -1, "File descriptor to notify of startup");
ABSL_FLAG(std::string, machine, "", "Machine name");
ABSL_FLAG(bool, vsock, false,
          "Use vsock (AF_VSOCK) instead of TCP for the per-channel bridge (and "
          "retirement) connections. Discovery still runs over IP. Linux only.");
ABSL_FLAG(int, vsock_cid, -1,
          "Local vsock context id (CID) to advertise to peers as the address "
          "they should connect to for bridges. -1 (default) auto-detects this "
          "server's local CID via /dev/vsock. Well-known CIDs: 1 = local "
          "(loopback), 2 = host. Only used when --vsock is set.");
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
ABSL_FLAG(int, num_asio_threads, 1,
          "Number of threads that run the io_context. Client handlers and the "
          "UDS listener are confined to a strand so they stay serialized; "
          "discovery/bridging coroutines spread across the threads.");
#endif

// macOS, Android, and QNX all benefit from cleaning up the filesystem on
// startup.  On Linux the server uses /dev/shm directly and tests routinely
// run multiple instances, so we keep it off by default.
#if defined(__APPLE__) || defined(__ANDROID__) || defined(__QNX__) || defined(__QNXNTO__)
ABSL_FLAG(bool, cleanup_filesystem, true, "Cleanup the filesystem on server startup");
#else
ABSL_FLAG(bool, cleanup_filesystem, false, "Cleanup the filesystem on server startup");
#endif
ABSL_FLAG(std::vector<std::string>, plugins, {},
          "List of plugins to load");
ABSL_FLAG(std::string, shadow_socket, "",
          "Primary shadow process Unix socket (empty = disabled)");
ABSL_FLAG(std::string, secondary_shadow_socket, "",
          "Secondary shadow process Unix socket (empty = disabled)");

// Look up this host's local vsock context id (CID) so we can advertise it to
// bridge peers.  Only meaningful on Linux with vsock support.
static bool ResolveLocalVsockCid(uint32_t *cid_out, std::string *err) {
#if defined(__linux__) && defined(IOCTL_VM_SOCKETS_GET_LOCAL_CID)
  int fd = ::open("/dev/vsock", O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    *err = std::string("open(/dev/vsock): ") + strerror(errno);
    return false;
  }
  unsigned int cid = 0;
  int r = ::ioctl(fd, IOCTL_VM_SOCKETS_GET_LOCAL_CID, &cid);
  int e = errno;
  ::close(fd);
  if (r < 0) {
    *err = std::string("ioctl(IOCTL_VM_SOCKETS_GET_LOCAL_CID): ") + strerror(e);
    return false;
  }
  *cid_out = static_cast<uint32_t>(cid);
  return true;
#else
  (void)cid_out;
  *err = "vsock local CID auto-detection is only supported on Linux; pass "
         "--vsock_cid explicitly";
  return false;
#endif
}

static bool ParsePort(const std::string &value, int *port) {
  if (value.empty()) {
    return false;
  }

  char *end = nullptr;
  errno = 0;
  long parsed = std::strtol(value.c_str(), &end, 10);
  if (errno != 0 || end == value.c_str() || *end != '\0' || parsed <= 0 ||
      parsed > 65535) {
    return false;
  }

  *port = static_cast<int>(parsed);
  return true;
}

static bool ParseBridgePorts(const std::string &value, int *first_port,
                             int *last_port) {
  if (value.empty()) {
    *first_port = 0;
    *last_port = 0;
    return true;
  }

  size_t dash = value.find('-');
  if (dash == std::string::npos) {
    if (!ParsePort(value, first_port)) {
      return false;
    }
    *last_port = *first_port;
    return true;
  }
  if (value.find('-', dash + 1) != std::string::npos) {
    return false;
  }

  return ParsePort(value.substr(0, dash), first_port) &&
         ParsePort(value.substr(dash + 1), last_port) &&
         *first_port <= *last_port;
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // The execution engine for the asio backend is an io_context that Server::Run
  // drives.
  boost::asio::io_context engine;
  signal(SIGPIPE, SIG_IGN);
#else
  co::CoroutineScheduler engine;
  g_scheduler = &engine; // For signal handler.
  signal(SIGPIPE, SIG_IGN);
  signal(SIGQUIT, Signal);
#endif

  // Close the plugins when the server is shutting down.
  subspace::ClosePluginsOnShutdown();

  std::unique_ptr<subspace::Server> server;

  if (absl::GetFlag(FLAGS_peer_address).empty()) {
    server = std::make_unique<subspace::Server>(
        engine, absl::GetFlag(FLAGS_socket), absl::GetFlag(FLAGS_interface),
        absl::GetFlag(FLAGS_disc_port), absl::GetFlag(FLAGS_peer_port),
        absl::GetFlag(FLAGS_local), absl::GetFlag(FLAGS_notify_fd));
  } else {
    toolbelt::InetAddress peer_address(absl::GetFlag(FLAGS_peer_address), absl::GetFlag(FLAGS_peer_port));
    server = std::make_unique<subspace::Server>(
        engine, absl::GetFlag(FLAGS_socket), absl::GetFlag(FLAGS_interface),
        peer_address, absl::GetFlag(FLAGS_disc_port),
        absl::GetFlag(FLAGS_peer_port), absl::GetFlag(FLAGS_local),
        absl::GetFlag(FLAGS_notify_fd));
  }

  server->SetLogLevel(absl::GetFlag(FLAGS_log_level));
  server->SetProfileFile(absl::GetFlag(FLAGS_profile_file));
  int bridge_first_port = 0;
  int bridge_last_port = 0;
  if (!ParseBridgePorts(absl::GetFlag(FLAGS_bridge_ports), &bridge_first_port,
                        &bridge_last_port)) {
    fprintf(stderr,
            "Invalid --bridge_ports value '%s'; expected PORT or START-END\n",
            absl::GetFlag(FLAGS_bridge_ports).c_str());
    exit(1);
  }
  if (bridge_first_port != 0) {
    absl::Status status = server->SetBridgePortRange(
        bridge_first_port, bridge_last_port,
        absl::GetFlag(FLAGS_bridge_ports_fallback_ephemeral));
    if (!status.ok()) {
      fprintf(stderr, "Invalid bridge port range: %s\n",
              status.ToString().c_str());
      exit(1);
    }
  }
  if (absl::GetFlag(FLAGS_cleanup_filesystem)) {
    server->SetCleanupFilesystem(true);
  }
  if (absl::GetFlag(FLAGS_tcp_discovery)) {
    server->SetTcpDiscovery(true);
  }
  if (const std::string &bridge_advertise =
          absl::GetFlag(FLAGS_bridge_advertise_address);
      !bridge_advertise.empty()) {
    toolbelt::InetAddress advertise_address(bridge_advertise, 0);
    if (!advertise_address.Valid()) {
      fprintf(stderr, "Invalid --bridge_advertise_address value '%s'\n",
              bridge_advertise.c_str());
      exit(1);
    }
    server->SetBridgeAdvertiseAddress(advertise_address);
  }

  // Load the plugins.  Each plugin is a name:path pair.
  for (const auto &p : absl::GetFlag(FLAGS_plugins)) {
    auto pos = p.find(':');
    if (pos == std::string::npos) {
      fprintf(stderr, "Plugin '%s' is not in name:path format\n", p.c_str());
      exit(1);
    }
    std::string name = p.substr(0, pos);
    std::string path = p.substr(pos + 1);
    absl::Status status = server->LoadPlugin(name, path);
    if (!status.ok()) {
      fprintf(stderr, "Failed to load plugin %s from %s: %s\n",
              name.c_str(), path.c_str(), status.ToString().c_str());
      exit(1);
    }
  }

  if (absl::GetFlag(FLAGS_vsock)) {
    int cid_flag = absl::GetFlag(FLAGS_vsock_cid);
    uint32_t cid = 0;
    if (cid_flag >= 0) {
      cid = static_cast<uint32_t>(cid_flag);
    } else {
      std::string err;
      if (!ResolveLocalVsockCid(&cid, &err)) {
        fprintf(stderr, "Failed to determine local vsock CID: %s\n",
                err.c_str());
        exit(1);
      }
    }
    server->SetVsockBridging(true, cid);
  }

  server->SetMachineName(absl::GetFlag(FLAGS_machine));
  server->SetShadowSockets(absl::GetFlag(FLAGS_shadow_socket),
                            absl::GetFlag(FLAGS_secondary_shadow_socket));

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  int num_asio_threads = absl::GetFlag(FLAGS_num_asio_threads);
  if (num_asio_threads < 1) {
    num_asio_threads = 1;
  }
  absl::Status s = server->Run(num_asio_threads);
#else
  absl::Status s = server->Run();
#endif
  if (!s.ok()) {
    fprintf(stderr, "Error running Subspace server: %s\n",
            s.ToString().c_str());
    exit(1);
  }
}