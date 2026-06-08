// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/runtime.h"
#include "common/async/socket.h"
#include "common/async/wait.h"

#include <cstring>
#include <gtest/gtest.h>
#include <unistd.h>

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
#include <boost/asio/io_context.hpp>
#endif

namespace subspace::async {
namespace {

// Helper to construct an AsyncRuntime and run it, abstracting over the two
// backends so the test bodies are backend-agnostic.
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
struct RuntimeFixture {
  boost::asio::io_context ioc;
  AsyncRuntime runtime{ioc};
  void Run() { ioc.run(); }
};
#else
struct RuntimeFixture {
  co::CoroutineScheduler scheduler;
  AsyncRuntime runtime{scheduler};
  void Run() { scheduler.Run(); }
};
#endif

TEST(AsyncTest, WaitReadableWakesOnData) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));

  RuntimeFixture fx;
  bool got_data = false;

  fx.runtime.Spawn([&](Context ctx) {
    ASSERT_TRUE(WaitReadable(ctx, fds[0]).ok());
    char buf[8] = {};
    ssize_t n = ::read(fds[0], buf, sizeof(buf));
    got_data = (n == 3 && buf[0] == 'a' && buf[1] == 'b' && buf[2] == 'c');
  });

  // Writer coroutine: sleeps a touch, then writes.
  fx.runtime.Spawn([&](Context ctx) {
    Sleep(ctx, std::chrono::milliseconds(10));
    ASSERT_EQ(3, ::write(fds[1], "abc", 3));
  });

  fx.Run();
  EXPECT_TRUE(got_data);
  ::close(fds[0]);
  ::close(fds[1]);
}

TEST(AsyncTest, WaitReadableTimeout) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));

  RuntimeFixture fx;
  bool timed_out = false;

  fx.runtime.Spawn([&](Context ctx) {
    absl::Status s = WaitReadable(ctx, fds[0], std::chrono::milliseconds(20));
    timed_out = absl::IsDeadlineExceeded(s);
  });

  fx.Run();
  EXPECT_TRUE(timed_out);
  ::close(fds[0]);
  ::close(fds[1]);
}

TEST(AsyncTest, WaitEitherReturnsReadyFd) {
  int a[2], b[2];
  ASSERT_EQ(0, ::pipe(a));
  ASSERT_EQ(0, ::pipe(b));

  RuntimeFixture fx;
  int ready = -1;

  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<int> r = WaitEither(ctx, a[0], b[0]);
    if (r.ok()) {
      ready = *r;
    }
  });

  fx.runtime.Spawn([&](Context ctx) {
    Sleep(ctx, std::chrono::milliseconds(10));
    ASSERT_EQ(1, ::write(b[1], "x", 1));
  });

  fx.Run();
  EXPECT_EQ(ready, b[0]);
  ::close(a[0]);
  ::close(a[1]);
  ::close(b[0]);
  ::close(b[1]);
}

// A round-trip over the stream socket facade, exercising the length-delimited
// framing on the loopback interface.
TEST(AsyncTest, StreamSocketLoopbackMessage) {
  RuntimeFixture fx;
  std::string received;

  StreamSocket listener;
  ASSERT_TRUE(
      listener.Bind(SocketAddress(InetAddress::AnyAddress(0)), true).ok());
  SocketAddress bound = listener.BoundAddress();
  int port = bound.Port();
  ASSERT_GT(port, 0);

  // Server: accept, receive one framed message.
  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<StreamSocket> conn = listener.Accept(ctx);
    if (!conn.ok()) {
      return;
    }
    char buf[256];
    absl::StatusOr<ssize_t> n = conn->ReceiveMessage(buf, sizeof(buf), ctx);
    if (n.ok() && *n > 0) {
      received.assign(buf, static_cast<size_t>(*n));
    }
  });

  // Client: connect to loopback, send one framed message.
  fx.runtime.Spawn([&, port](Context ctx) {
    StreamSocket client;
    SocketAddress dest(InetAddress("127.0.0.1", port));
    if (!client.Connect(dest).ok()) {
      return;
    }
    char outbuf[64];
    const char *payload = "hello-bridge";
    size_t len = std::strlen(payload);
    // SendMessage requires 4 bytes below the payload pointer.
    std::memcpy(outbuf + sizeof(int32_t), payload, len);
    (void)client.SendMessage(outbuf + sizeof(int32_t), len, ctx);
  });

  fx.Run();
  EXPECT_EQ(received, "hello-bridge");
}

}  // namespace
}  // namespace subspace::async
