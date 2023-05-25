// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "client/client.h"
#include "common/clock.h"
#include <csignal>
#include <inttypes.h>

ABSL_FLAG(std::string, socket, "/tmp/subspace",
          "Name of Unix socket to listen on");
ABSL_FLAG(bool, reliable, true, "Use reliable transport");
ABSL_FLAG(int, num_msgs, 1000, "Number of messages to measure");
ABSL_FLAG(int, slot_size, 4096, "Size of a slot");
ABSL_FLAG(bool, csv, false, "CSV output");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  signal(SIGPIPE, SIG_IGN);

  subspace::Client client;
  absl::Status s = client.Init(absl::GetFlag(FLAGS_socket));
  if (!s.ok()) {
    fprintf(stderr, "Can't connect to Subspace server: %s\n",
            s.ToString().c_str());
    exit(1);
  }
  bool reliable = absl::GetFlag(FLAGS_reliable);
  int slot_size = absl::GetFlag(FLAGS_slot_size);

  absl::StatusOr<subspace::Subscriber *> sub = client.CreateSubscriber(
      "test", subspace::SubscriberOptions().SetReliable(reliable));
  if (!sub.ok()) {
    fprintf(stderr, "Can't create subscriber: %s\n",
            sub.status().ToString().c_str());
    exit(1);
  }

  client.RegisterDroppedMessageCallback(
      *sub, [](subspace::Subscriber *s, int64_t n) {
        printf("Dropped %" PRId64 " messages\n", n);
      });

  int num_msgs = absl::GetFlag(FLAGS_num_msgs);
  uint64_t start = 0;
  int64_t total_bytes = 0;
  uint64_t total_wait = 0;

  int i = 0;
  while (i < num_msgs) {
    uint64_t wait_start = 0;
    if (start != 0) {
      wait_start = subspace::Now();
    }
    if (absl::Status s = client.WaitForSubscriber(*sub); !s.ok()) {
      fprintf(stderr, "Can't wait for subscriber: %s\n", s.ToString().c_str());
      exit(1);
    }
    if (wait_start != 0) {
      total_wait += subspace::Now() - wait_start;
    }
    for (;;) {
      absl::StatusOr<subspace::Message> msg = client.ReadMessage(*sub);
      if (!msg.ok()) {
        fprintf(stderr, "Can't read message: %s\n",
                msg.status().ToString().c_str());
        exit(1);
      }
      if (msg->length == 0) {
        break;
      }
      if (start == 0) {
        start = subspace::Now();
      }
      // printf("%d\n", i);
      total_bytes += msg->length;
      i++;
    }
  }
  uint64_t end = subspace::Now();
  double period = (end - start - total_wait) / 1e9;
  double msg_rate = num_msgs / period;
  double byte_rate = total_bytes / period;
  double latency = (period * 1e6) / num_msgs;
  if (absl::GetFlag(FLAGS_csv)) {
    printf("%d,%d,%g,%g,%g,%g\n", slot_size, num_msgs, period, msg_rate,
           latency, byte_rate);
  } else {
    printf("Subspace: %d bytes, %d messages received in %gs, %g msgs/sec, "
           "latency: %gus. %g bytes/sec\n",
           slot_size, num_msgs, period, msg_rate, latency, byte_rate);
  }
}