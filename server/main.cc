// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "server.h"
#include <csignal>
#include <string>

static co::CoroutineScheduler *g_scheduler;
void Signal(int sig) {
  printf("\nAll coroutines:\n");
  g_scheduler->Show();
  signal(sig, SIG_DFL);
  raise(sig);
}

ABSL_FLAG(std::string, socket, "/tmp/subspace",
          "Name of Unix socket to listen on");
ABSL_FLAG(int, disc_port, 6502, "Discovery UDP port");
ABSL_FLAG(int, peer_port, 6502, "Discovery peer UDP port");
ABSL_FLAG(std::string, peer_address, "", "Bridge peer hostname or IP address");
ABSL_FLAG(std::string, log_level, "info", "Log level");
ABSL_FLAG(std::string, interface, "", "Discovery network interface");
ABSL_FLAG(bool, local, false, "Use local computer only");
ABSL_FLAG(int, notify_fd, -1, "File descriptor to notify of startup");
ABSL_FLAG(std::string, machine, "", "Machine name");

// macOS and QNX both use /tmp-shadowed POSIX shared memory, so it's safe and
// useful to clean up the filesystem on startup there.  On Linux the server
// uses /dev/shm directly and tests routinely run multiple instances, so we
// keep it off by default.
#if defined(__APPLE__) || defined(__QNX__) || defined(__QNXNTO__)
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

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);

  co::CoroutineScheduler scheduler;

  g_scheduler = &scheduler; // For signal handler.
  signal(SIGPIPE, SIG_IGN);
  signal(SIGQUIT, Signal);

  // Close the plugins when the server is shutting down.
  subspace::ClosePluginsOnShutdown();

  std::unique_ptr<subspace::Server> server;

  if (absl::GetFlag(FLAGS_peer_address).empty()) {
    server = std::make_unique<subspace::Server>(
        scheduler, absl::GetFlag(FLAGS_socket), absl::GetFlag(FLAGS_interface),
        absl::GetFlag(FLAGS_disc_port), absl::GetFlag(FLAGS_peer_port),
        absl::GetFlag(FLAGS_local), absl::GetFlag(FLAGS_notify_fd));
  } else {
    toolbelt::InetAddress peer_address(absl::GetFlag(FLAGS_peer_address), absl::GetFlag(FLAGS_peer_port));
    server = std::make_unique<subspace::Server>(
        scheduler, absl::GetFlag(FLAGS_socket), absl::GetFlag(FLAGS_interface),
        peer_address, absl::GetFlag(FLAGS_disc_port),
        absl::GetFlag(FLAGS_peer_port), absl::GetFlag(FLAGS_local),
        absl::GetFlag(FLAGS_notify_fd));
  }

  server->SetLogLevel(absl::GetFlag(FLAGS_log_level));
  if (absl::GetFlag(FLAGS_cleanup_filesystem)) {
    server->SetCleanupFilesystem(true);
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

  server->SetMachineName(absl::GetFlag(FLAGS_machine));
  server->SetShadowSockets(absl::GetFlag(FLAGS_shadow_socket),
                            absl::GetFlag(FLAGS_secondary_shadow_socket));

  absl::Status s = server->Run();
  if (!s.ok()) {
    fprintf(stderr, "Error running Subspace server: %s\n",
            s.ToString().c_str());
    exit(1);
  }
}