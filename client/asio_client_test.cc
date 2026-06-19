// Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.
//
// End-to-end test of the Asio backend: an in-process Subspace server driven by
// a boost::asio::io_context on a background thread, with an Asio client
// (driven by a yield_context on its own io_context) performing a pub/sub round
// trip over the Unix-domain socket RPC.  This exercises the asio UnixSocket
// facade (framed messages + SCM_RIGHTS fd passing), the additive
// Client(async::Context) path, and the io_context Server constructor.
//
// Under the co backend this file compiles to a trivial passing test so the
// target is harmless if built there.

#include "common/async/context.h"

#include <gtest/gtest.h>

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include "client/client.h"
#include "server/server.h"

#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <cstdio>
#include <cstring>
#include <signal.h>
#include <string>
#include <thread>
#include <unistd.h>

namespace {

TEST(AsioClientServer, PubSubRoundTrip) {
  signal(SIGPIPE, SIG_IGN);

  char socket_template[] = "/tmp/subspace_asioXXXXXX"; // NOLINT
  ::close(mkstemp(&socket_template[0]));
  std::string socket = &socket_template[0];

  int server_pipe[2];
  ASSERT_EQ(0, ::pipe(server_pipe));

  // The server runs its own io_context on a background thread.
  boost::asio::io_context server_ioc;
  subspace::Server server(server_ioc, socket, /*interface=*/"", /*disc_port=*/0,
                          /*peer_port=*/0, /*local=*/true,
                          /*notify_fd=*/server_pipe[1], /*initial_ordinal=*/1,
                          /*wait_for_clients=*/false);

  std::thread server_thread([&]() {
    absl::Status s = server.Run();
    if (!s.ok()) {
      fprintf(stderr, "asio server error: %s\n", s.ToString().c_str());
    }
  });

  // Wait for the server-ready notification.
  char buf[8];
  (void)::read(server_pipe[0], buf, sizeof(buf));

  // The client runs on its own io_context, cooperatively via a yield_context.
  boost::asio::io_context client_ioc;
  std::string activation_seen;
  int payload_len = -1;
  std::string payload;
  std::string init_error;

  boost::asio::spawn(
      client_ioc,
      [&](subspace::async::Context yield) {
        subspace::Client client(yield);
        if (absl::Status s = client.Init(socket, "asio-client"); !s.ok()) {
          init_error = s.ToString();
          return;
        }
        absl::StatusOr<subspace::Publisher> pub = client.CreatePublisher(
            "asio_chan", {.slot_size = 256, .num_slots = 10, .activate = true});
        if (!pub.ok()) {
          init_error = pub.status().ToString();
          return;
        }
        absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
        if (!buffer.ok()) {
          init_error = buffer.status().ToString();
          return;
        }
        std::memcpy(*buffer, "foobar", 6);
        if (absl::Status s = pub->PublishMessage(6).status(); !s.ok()) {
          init_error = s.ToString();
          return;
        }

        absl::StatusOr<subspace::Subscriber> sub = client.CreateSubscriber(
            "asio_chan", {.max_active_messages = 2, .pass_activation = true});
        if (!sub.ok()) {
          init_error = sub.status().ToString();
          return;
        }
        // First read is the activation message.
        absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
        if (msg.ok() && msg->is_activation) {
          activation_seen = "yes";
        }
        // Second read is the real payload.
        msg = sub->ReadMessage();
        if (msg.ok()) {
          payload_len = static_cast<int>(msg->length);
          payload.assign(static_cast<const char *>(msg->buffer), msg->length);
        }
      },
      boost::asio::detached);

  client_ioc.run();

  server.Stop();
  (void)::read(server_pipe[0], buf, sizeof(buf));
  server_thread.join();
  server.CleanupAfterSession();
  (void)::remove(socket.c_str());
  ::close(server_pipe[0]);
  ::close(server_pipe[1]);

  EXPECT_EQ(init_error, "");
  EXPECT_EQ(activation_seen, "yes");
  EXPECT_EQ(payload_len, 6);
  EXPECT_EQ(payload, "foobar");
}

}  // namespace

#else  // co backend: nothing to exercise here.

TEST(AsioClientServer, SkippedOnCoBackend) { SUCCEED(); }

#endif
