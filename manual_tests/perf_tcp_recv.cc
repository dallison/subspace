#include "common/sockets.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "common/clock.h"
#include <csignal>
#include <inttypes.h>

ABSL_FLAG(int, num_msgs, 1000, "Number of messages to measure");
ABSL_FLAG(int, msg_size, 1000, "Size of each message");
ABSL_FLAG(std::string, hostname, "localhost", "Hostname");
ABSL_FLAG(bool, csv, false, "CSV output");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  signal(SIGPIPE, SIG_IGN);

  subspace::TCPSocket socket;
  std::string hostname = absl::GetFlag(FLAGS_hostname);

  if (absl::Status s =
          socket.Bind(subspace::InetAddress(hostname.c_str(), 6522), true);
      !s.ok()) {
    fprintf(stderr, "Failed to bind to localhost: %s\n", s.ToString().c_str());
    exit(1);
  }
  if (absl::Status status = socket.SetReuseAddr(); !status.ok()) {
    fprintf(stderr, "%s", status.ToString().c_str());
    exit(1);
  }
  absl::StatusOr<subspace::TCPSocket> s = socket.Accept();
  if (!s.ok()) {
    fprintf(stderr, "Failed to accept: %s\n", s.status().ToString().c_str());
    exit(1);
  }
  int msg_size = absl::GetFlag(FLAGS_msg_size);
  int num_msgs = absl::GetFlag(FLAGS_num_msgs);
  void *memory = malloc(msg_size);

  uint64_t start = 0;
  int64_t total_bytes = 0;

  for (int i = 0; i < num_msgs; i++) {
    absl::StatusOr<ssize_t> n =
        s->Receive(reinterpret_cast<char *>(memory), msg_size);
    if (!n.ok()) {
      fprintf(stderr, "Failed to read: %s\n", n.status().ToString().c_str());
      exit(1);
    }
    if (start == 0) {
      start = subspace::Now();
    }
    total_bytes += *n;
  }
  free(memory);

  uint64_t end = subspace::Now();
  double period = (end - start) / 1e9;
  double msg_rate = num_msgs / period;
  double byte_rate = total_bytes / period;
  double latency = (period * 1e6) / num_msgs;
  if (absl::GetFlag(FLAGS_csv)) {
    printf("%d,%d,%g,%g,%g,%g\n", msg_size, num_msgs, period, msg_rate, latency,
           byte_rate);
  } else {
    printf("TCP: %d bytes, %d messages received in %gs, %g msgs/sec, latency: "
           "%gus. %g bytes/sec\n",
           msg_size, num_msgs, period, msg_rate, latency, byte_rate);
  }
}