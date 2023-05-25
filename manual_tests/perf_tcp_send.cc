#include "common/sockets.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "common/clock.h"
#include <csignal>
#include <inttypes.h>

ABSL_FLAG(int, num_msgs, 1000, "Number of messages to send");
ABSL_FLAG(int, msg_size, 1000, "Size of each message");
ABSL_FLAG(std::string, hostname, "localhost", "Hostname");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  signal(SIGPIPE, SIG_IGN);

  subspace::TCPSocket socket;
  std::string hostname = absl::GetFlag(FLAGS_hostname);

  absl::Status status = socket.Connect(subspace::InetAddress(hostname.c_str(), 6522));
  if (!status.ok()) {
    fprintf(stderr, "Failed to connect: %s\n", status.ToString().c_str());
    exit(1);
  }
  int msg_size = absl::GetFlag(FLAGS_msg_size);
  int num_msgs = absl::GetFlag(FLAGS_num_msgs);
  void *memory = malloc(msg_size);

  for (int i = 0; i < num_msgs; i++) {
    absl::StatusOr<ssize_t> n =
        socket.Send(reinterpret_cast<char *>(memory), msg_size);
    if (!n.ok()) {
      fprintf(stderr, "Failed to read: %s\n", n.status().ToString().c_str());
      exit(1);
    }
  }
  free(memory);
}