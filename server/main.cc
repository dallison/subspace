// Copyright 2023 David Allison
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
ABSL_FLAG(std::string, log_level, "info", "Log level");
ABSL_FLAG(std::string, interface, "", "Discovery network interface");
ABSL_FLAG(bool, local, false, "Use local computer only");
ABSL_FLAG(int, notify_fd, -1, "File descriptor to notify of startup");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);

  co::CoroutineScheduler scheduler;

  g_scheduler = &scheduler; // For signal handler.
  signal(SIGPIPE, SIG_IGN);
  signal(SIGQUIT, Signal);

  subspace::Server server(
      scheduler, absl::GetFlag(FLAGS_socket), absl::GetFlag(FLAGS_interface),
      absl::GetFlag(FLAGS_disc_port), absl::GetFlag(FLAGS_peer_port),
      absl::GetFlag(FLAGS_local), absl::GetFlag(FLAGS_notify_fd));

  server.SetLogLevel(absl::GetFlag(FLAGS_log_level));
  absl::Status s = server.Run();
  if (!s.ok()) {
    fprintf(stderr, "Error running Subspace server: %s\n",
            s.ToString().c_str());
    exit(1);
  }
}