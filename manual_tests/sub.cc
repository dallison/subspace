// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "client/client.h"
#include <csignal>
#include <inttypes.h>

ABSL_FLAG(std::string, socket, "/tmp/subspace",
          "Name of Unix socket to listen on");
ABSL_FLAG(bool, reliable, false, "Use reliable transport");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  signal(SIGPIPE, SIG_IGN);

  subspace::Client client;

  absl::Status init_status = client.Init(absl::GetFlag(FLAGS_socket));
  if (!init_status.ok()) {
    fprintf(stderr, "Can't connect to Subspace server: %s\n", init_status.ToString().c_str());
    exit(1);
  }
  bool reliable = absl::GetFlag(FLAGS_reliable);

  absl::StatusOr<subspace::Subscriber> sub = client.CreateSubscriber(
      "test", subspace::SubscriberOptions().SetReliable(reliable));
  if (!sub.ok()) {
    fprintf(stderr, "Can't create subscriber: %s\n",
            sub.status().ToString().c_str());
    exit(1);
  }

  sub->RegisterDroppedMessageCallback(
      [](subspace::Subscriber *s, int64_t n) {
        printf("Dropped %" PRId64 " messages\n", n);
      });

  for (;;) {
    if (absl::Status wait_status = sub->Wait(); !wait_status.ok()) {
      fprintf(stderr, "Can't wait for subscriber: %s\n", wait_status.ToString().c_str());
      exit(1);
    }
    for (;;) {
      absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
      if (!msg.ok()) {
        fprintf(stderr, "Can't read message: %s\n",
                msg.status().ToString().c_str());
        exit(1);
      }
      if (msg->length == 0) {
        break;
      }
      int64_t ordinal = sub->GetCurrentOrdinal();
      printf("Message %" PRId64 ": %s\n", ordinal,
             reinterpret_cast<const char *>(msg->buffer));
    }
  }
}