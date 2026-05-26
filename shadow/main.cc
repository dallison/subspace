// Copyright 2023-2026 David Allison
// Shadow support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "co/coroutine.h"
#include "shadow/shadow.h"
#include <csignal>
#include <cstdio>
#include <string>

ABSL_FLAG(std::string, socket, "/tmp/subspace_shadow",
          "Name of Unix socket to listen on");
ABSL_FLAG(std::string, log_level, "info", "Log level");

static subspace::Shadow *g_shadow = nullptr;

void HandleSignal(int /*sig*/) {
  if (g_shadow != nullptr) {
    g_shadow->Stop();
  }
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, HandleSignal);
  signal(SIGTERM, HandleSignal);

  co::CoroutineScheduler scheduler;
  subspace::Shadow shadow(scheduler, absl::GetFlag(FLAGS_socket));
  shadow.SetLogLevel(absl::GetFlag(FLAGS_log_level));
  g_shadow = &shadow;

  absl::Status s = shadow.Run();
  if (!s.ok()) {
    fprintf(stderr, "Error running shadow process: %s\n",
            s.ToString().c_str());
    return 1;
  }
  return 0;
}
