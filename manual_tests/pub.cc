// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "client/client.h"
#include <time.h>

ABSL_FLAG(std::string, socket, "/tmp/subspace",
          "Name of Unix socket to listen on");
ABSL_FLAG(int, num_msgs, 1, "Number of messages to send");
ABSL_FLAG(double, frequency, 0, "Freqency to send at (Hz)");
ABSL_FLAG(bool, reliable, false, "Use reliable transport");
ABSL_FLAG(int, num_slots, 5, "Number of slots in channel");
ABSL_FLAG(bool, local, true,
          "Restrict the channel to the local machine. Set to false to allow "
          "the channel to be bridged to other servers.");
ABSL_FLAG(std::string, channel, "test", "Channel name to publish on");

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  // Line-buffer stdout so progress is visible (and not lost on termination)
  // when output is redirected to a file or pipe, e.g. in scripted tests.
  setvbuf(stdout, nullptr, _IOLBF, 0);

  subspace::Client client;
  absl::Status init_status = client.Init(absl::GetFlag(FLAGS_socket));
  if (!init_status.ok()) {
    fprintf(stderr, "Can't connect to Subspace server: %s\n",
            init_status.ToString().c_str());
    exit(1);
  }
  bool reliable = absl::GetFlag(FLAGS_reliable);
  int num_slots = absl::GetFlag(FLAGS_num_slots);
  bool local = absl::GetFlag(FLAGS_local);
  std::string channel = absl::GetFlag(FLAGS_channel);

  absl::StatusOr<subspace::Publisher> pub = client.CreatePublisher(
      channel, 256, num_slots,
      subspace::PublisherOptions().SetLocal(local).SetReliable(reliable));
  if (!pub.ok()) {
    fprintf(stderr, "Can't create publisher: %s\n",
            pub.status().ToString().c_str());
    exit(1);
  }
  int num_msgs = absl::GetFlag(FLAGS_num_msgs);
  double frequency = absl::GetFlag(FLAGS_frequency);

  int delay_ns = 0;

  for (;;) {
    if (frequency <= 0) {
      printf("Sending %d message%s as fast as possible\n", num_msgs,
             num_msgs == 1 ? "s" : "");
    } else {
      delay_ns = 1000000000 / frequency;
      printf("Sending %d message%s at %gHz\n", num_msgs,
             num_msgs == 1 ? "s" : "", frequency);
    }
    struct timespec delay = {.tv_sec = delay_ns / 1000000000,
                             .tv_nsec = delay_ns % 1000000000};

    for (int i = 0; i < num_msgs; i++) {
      void *buf = nullptr;
      for (;;) {
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        if (!buffer.ok()) {
          fprintf(stderr, "Can't get publisher buffer: %s\n",
                  buffer.status().ToString().c_str());
          exit(1);
        }
        if (*buffer == nullptr) {
          // Wait for publisher trigger.
          absl::Status wait_status = pub->Wait();
          if (!wait_status.ok()) {
            fprintf(stderr, "Can't wait for publisher: %s",
                    wait_status.ToString().c_str());
            exit(1);
          }
          continue;
        }
        buf = *buffer;
        break;
      }
      size_t len = snprintf(reinterpret_cast<char *>(buf), 256, "%s", "hello");
      absl::StatusOr<const subspace::Message> status =
          pub->PublishMessage(len);
      if (!status.ok()) {
        fprintf(stderr, "Can't publish message: %s\n",
                status.status().ToString().c_str());
        exit(1);
      }
      if (i < (num_msgs - 1)) {
        nanosleep(&delay, nullptr);
      }
    }
    printf("All messages sent, hit return to do it again, ^C to stop\n");
    getchar();
  }
}
