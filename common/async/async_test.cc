// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/runtime.h"
#include "common/async/socket.h"
#include "common/async/wait.h"

#include <cstring>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
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

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
TEST(AsyncTest, WaitReadableWorksWithoutCancellationSlot) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));

  boost::asio::io_context ioc;
  bool got_data = false;

  boost::asio::spawn(
      ioc,
      [&](Context ctx) {
        ASSERT_TRUE(
            WaitReadable(ctx, fds[0], std::chrono::milliseconds(100)).ok());
        char buf[8] = {};
        ssize_t n = ::read(fds[0], buf, sizeof(buf));
        got_data = (n == 1 && buf[0] == 'x');
      },
      boost::asio::detached);

  boost::asio::spawn(
      ioc,
      [&](Context ctx) {
        Sleep(ctx, std::chrono::milliseconds(10));
        ASSERT_EQ(1, ::write(fds[1], "x", 1));
      },
      boost::asio::detached);

  ioc.run();
  EXPECT_TRUE(got_data);
  ::close(fds[0]);
  ::close(fds[1]);
}
#endif

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

TEST(AsyncTest, WaitEitherTimeout) {
  int a[2], b[2];
  ASSERT_EQ(0, ::pipe(a));
  ASSERT_EQ(0, ::pipe(b));

  RuntimeFixture fx;
  bool timed_out = false;

  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<int> r =
        WaitEither(ctx, a[0], b[0], std::chrono::milliseconds(20));
    timed_out = absl::IsDeadlineExceeded(r.status());
  });

  fx.Run();
  EXPECT_TRUE(timed_out);
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

// Exercises the Unix-domain socket facade end to end: a framed message plus a
// passed file descriptor (SCM_RIGHTS), mirroring the client<->server RPC.  The
// wire framing matches toolbelt so this validates interop between backends.
TEST(AsyncTest, UnixSocketMessageAndFdPassing) {
  RuntimeFixture fx;
  std::string received_msg;
  std::string received_via_fd;

  const std::string path =
      "subspace_async_uds_test_" + std::to_string(::getpid());

  UnixSocket listener;
  ASSERT_TRUE(listener.Bind(path, /*listen=*/true).ok());

  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<UnixSocket> conn = listener.Accept(ctx);
    if (!conn.ok()) {
      return;
    }
    absl::StatusOr<std::vector<char>> msg =
        conn->ReceiveVariableLengthMessage(ctx);
    if (!msg.ok()) {
      return;
    }
    received_msg.assign(msg->data(), msg->size());

    std::vector<toolbelt::FileDescriptor> fds;
    if (!conn->ReceiveFds(fds, ctx).ok() || fds.empty()) {
      return;
    }
    char buf[16] = {};
    ssize_t n = ::read(fds[0].Fd(), buf, sizeof(buf));
    if (n > 0) {
      received_via_fd.assign(buf, static_cast<size_t>(n));
    }
  });

  fx.runtime.Spawn([&, path](Context ctx) {
    UnixSocket client;
    if (!client.Connect(path).ok()) {
      return;
    }
    // A pipe whose read end we pass to the peer.
    int p[2];
    if (::pipe(p) != 0) {
      return;
    }
    ASSERT_EQ(3, ::write(p[1], "FD!", 3));
    ::close(p[1]);

    char outbuf[64];
    const char *payload = "hello-uds";
    size_t len = std::strlen(payload);
    std::memcpy(outbuf + sizeof(int32_t), payload, len);
    if (!client.SendMessage(outbuf + sizeof(int32_t), len, ctx).ok()) {
      return;
    }
    std::vector<toolbelt::FileDescriptor> fds;
    fds.emplace_back(p[0], /*owned=*/false);
    (void)client.SendFds(fds, ctx);
    ::close(p[0]);
  });

  fx.Run();
  EXPECT_EQ(received_msg, "hello-uds");
  EXPECT_EQ(received_via_fd, "FD!");
}

TEST(AsyncTest, WaitWritableOnPipe) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));
  int flags = ::fcntl(fds[1], F_GETFL, 0);
  ASSERT_EQ(0, ::fcntl(fds[1], F_SETFL, flags | O_NONBLOCK));

  RuntimeFixture fx;
  absl::Status empty_status = absl::UnknownError("not run");
  absl::Status full_status = absl::UnknownError("not run");
  fx.runtime.Spawn([&](Context ctx) {
    empty_status = WaitWritable(ctx, fds[1], std::chrono::milliseconds(100));
    char buf[4096] = {};
    while (::write(fds[1], buf, sizeof(buf)) > 0) {
    }
    full_status = WaitWritable(ctx, fds[1], std::chrono::milliseconds(20));
  });
  fx.Run();
  EXPECT_TRUE(empty_status.ok()) << empty_status;
  EXPECT_TRUE(absl::IsDeadlineExceeded(full_status)) << full_status;
  ::close(fds[0]);
  ::close(fds[1]);
}

TEST(AsyncTest, ConnectStreamLoopback) {
  absl::StatusOr<StreamSocket> listener =
      ListenStream(InetAddress("127.0.0.1", 0));
  ASSERT_TRUE(listener.ok()) << listener.status();
  int port = listener->BoundAddress().Port();
  ASSERT_GT(port, 0);
  EXPECT_GE(StreamSocketFd(*listener), 0);

  RuntimeFixture fx;
  std::string received;
  absl::Status connect_status = absl::UnknownError("not run");
  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<StreamSocket> conn = listener->Accept(ctx);
    if (!conn.ok()) {
      return;
    }
    char buf[4];
    absl::StatusOr<ssize_t> n = conn->Receive(buf, sizeof(buf), ctx);
    if (n.ok() && *n > 0) {
      received.assign(buf, static_cast<size_t>(*n));
    }
  });
  fx.runtime.Spawn([&, port](Context ctx) {
    absl::StatusOr<StreamSocket> client = ConnectStream(
        InetAddress("127.0.0.1", port), std::chrono::seconds(5), ctx);
    connect_status = client.status();
    if (!client.ok()) {
      return;
    }
    EXPECT_GE(StreamSocketFd(*client), 0);
    (void)client->Send("ping", 4, ctx);
  });
  fx.Run();
  EXPECT_TRUE(connect_status.ok()) << connect_status;
  EXPECT_EQ(received, "ping");
}

TEST(AsyncTest, ConnectStreamRefused) {
  int port = 0;
  {
    absl::StatusOr<StreamSocket> listener =
        ListenStream(InetAddress("127.0.0.1", 0));
    ASSERT_TRUE(listener.ok()) << listener.status();
    port = listener->BoundAddress().Port();
  }

  RuntimeFixture fx;
  absl::Status status = absl::UnknownError("not run");
  fx.runtime.Spawn([&, port](Context ctx) {
    status = ConnectStream(InetAddress("127.0.0.1", port),
                           std::chrono::seconds(5), ctx)
                 .status();
  });
  fx.Run();
  EXPECT_TRUE(absl::IsUnavailable(status)) << status;
}

#if defined(__linux__)
// Linux drops a SYN when the listener's accept queue is full, so a connect
// to a listener with a zero backlog and a queued connection stays pending.
TEST(AsyncTest, ConnectStreamTimeout) {
  int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  InetAddress loopback("127.0.0.1", 0);
  ASSERT_EQ(0, ::bind(listen_fd,
                      reinterpret_cast<const sockaddr *>(&loopback.GetAddress()),
                      sizeof(sockaddr_in)));
  ASSERT_EQ(0, ::listen(listen_fd, 0));
  sockaddr_in bound;
  socklen_t len = sizeof(bound);
  ASSERT_EQ(0, ::getsockname(listen_fd, reinterpret_cast<sockaddr *>(&bound),
                             &len));
  std::vector<int> fillers;
  for (int i = 0; i < 3; i++) {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    ASSERT_GE(fd, 0);
    (void)::connect(fd, reinterpret_cast<const sockaddr *>(&bound),
                    sizeof(bound));
    fillers.push_back(fd);
  }

  RuntimeFixture fx;
  absl::Status status = absl::UnknownError("not run");
  fx.runtime.Spawn([&](Context ctx) {
    status = ConnectStream(InetAddress(bound), std::chrono::milliseconds(200),
                           ctx)
                 .status();
  });
  fx.Run();
  EXPECT_TRUE(absl::IsDeadlineExceeded(status)) << status;
  for (int fd : fillers) {
    ::close(fd);
  }
  ::close(listen_fd);
}
#endif

}  // namespace
}  // namespace subspace::async
