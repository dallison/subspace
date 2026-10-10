#include "dashboard/engine.h"
#include "dashboard/ui.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <csignal>
#include <iostream>
#include <string>

ABSL_FLAG(std::string, socket, "/tmp/subspace", "Subspace server Unix socket");
ABSL_FLAG(std::string, channel, "/dashboard",
          "Subspace channel used by the dashboard");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  std::signal(SIGPIPE, SIG_IGN);

  subspace::dashboard::DashboardEngine engine(
      {.socket = absl::GetFlag(FLAGS_socket),
       .channel = absl::GetFlag(FLAGS_channel)});
  if (auto status = engine.Init(); !status.ok()) {
    std::cerr << "Unable to start dashboard: " << status << '\n';
    return 1;
  }

  subspace::dashboard::DashboardApplication app(engine);
  app.Run();

  const auto snapshot = engine.Snapshot();
  if (!snapshot.error.empty()) {
    std::cerr << "Dashboard stopped: " << snapshot.error << '\n';
    return 1;
  }
  return 0;
}
