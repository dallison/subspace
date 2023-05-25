// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "client/client.h"
#include <time.h>
#include <csignal>

ABSL_FLAG(std::string, socket, "/tmp/subspace",
          "Name of Unix socket to listen on");
ABSL_FLAG(int, num_msgs, 1000, "Number of messages to send");
ABSL_FLAG(bool, reliable, true, "Use reliable transport");
ABSL_FLAG(int, num_slots, 500, "Number of slots in channel");
ABSL_FLAG(int, slot_size, 4096, "Size of a slot");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  signal(SIGPIPE, SIG_IGN);

  subspace::Client client;
  absl::Status s = client.Init(absl::GetFlag(FLAGS_socket));
  if (!s.ok()) {
    fprintf(stderr, "Can't connect to Subspace server: %s\n", s.ToString().c_str());
    exit(1);
  }
  bool reliable = absl::GetFlag(FLAGS_reliable);
  int num_slots = absl::GetFlag(FLAGS_num_slots);
  int slot_size = absl::GetFlag(FLAGS_slot_size);

  absl::StatusOr<subspace::Publisher *> pub = client.CreatePublisher(
      "test", slot_size, num_slots,
      subspace::PublisherOptions().SetPublic(true).SetReliable(reliable));
  if (!pub.ok()) {
    fprintf(stderr, "Can't create publisher: %s\n",
            pub.status().ToString().c_str());
    exit(1);
  }
  int num_msgs = absl::GetFlag(FLAGS_num_msgs);

  for (int i = 0; i < num_msgs; i++) {
    for (;;) {
      absl::StatusOr<void *> buffer = client.GetMessageBuffer(*pub);
      if (!buffer.ok()) {
        fprintf(stderr, "Can't get publisher buffer: %s\n",
                buffer.status().ToString().c_str());
        exit(1);
      }
      if (*buffer == nullptr) {
        // Wait for publisher trigger.
        absl::Status s = client.WaitForReliablePublisher(*pub);
        if (!s.ok()) {
          fprintf(stderr, "Can't wait for publisher: %s", s.ToString().c_str());
          exit(1);
        }
        continue;
      }
      break;
    }
    //printf("publishing %d\n", i);
    absl::StatusOr<const subspace::Message> status = client.PublishMessage(*pub, slot_size);
    if (!status.ok()) {
      fprintf(stderr, "Can't publish message: %s\n", status.status().ToString().c_str());
      exit(1);
    }
  }
}
