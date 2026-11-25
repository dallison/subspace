// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/frame_pool.h"
#include "common/async/runtime.h"
#include "common/async/socket.h"
#include "common/async/task.h"
#include "common/async/wait.h"
#include "common/coroutine_stack.h"

#include <cstring>
#include <stdexcept>
#include <thread>
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
#elif SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
#include "co/coroutine.h"
#endif

// The coroutine bodies below are written once for every backend with
// ASYNC_AWAIT and ASYNC_RETURN (see task.h).  gtest's ASSERT_* macros return,
// which a C++20 coroutine can't do, so the bodies use EXPECT_* instead.

namespace subspace::async {
namespace {

// Helper to construct an AsyncRuntime and run it, abstracting over the
// backends so the test bodies are backend-agnostic.
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
struct RuntimeFixture {
  boost::asio::io_context ioc;
  AsyncRuntime runtime{ioc};
  void Run() { ioc.run(); }
};
#elif SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
struct RuntimeFixture {
  co20::Scheduler scheduler;
  AsyncRuntime runtime{scheduler};
  void Run() { scheduler.Run(); }
};
#else
struct RuntimeFixture {
  co::CoroutineScheduler scheduler;
  AsyncRuntime runtime{scheduler};
  void Run() { scheduler.Run(); }
};
#endif

TEST(AsyncTest, CoroutineStackSizeFollowsBuildSetting) {
  EXPECT_EQ(kConfiguredCoroutineStackSize % 4096, 0u);
#ifdef SUBSPACE_COROUTINE_STACK_ASAN
  EXPECT_GE(CoroutineStackSize(), kAsanCoroutineStackFloor);
  if (kConfiguredCoroutineStackSize > kAsanCoroutineStackFloor) {
    EXPECT_EQ(CoroutineStackSize(), kConfiguredCoroutineStackSize);
  }
#else
  EXPECT_EQ(CoroutineStackSize(), kConfiguredCoroutineStackSize);
#endif
}

TEST(AsyncTest, WaitReadableWakesOnData) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));

  RuntimeFixture fx;
  bool got_data = false;

  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::Status s = ASYNC_AWAIT(WaitReadable(ctx, fds[0]));
    EXPECT_TRUE(s.ok()) << s;
    char buf[8] = {};
    ssize_t n = ::read(fds[0], buf, sizeof(buf));
    got_data = (n == 3 && buf[0] == 'a' && buf[1] == 'b' && buf[2] == 'c');
  });

  // Writer coroutine: sleeps a touch, then writes.
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    ASYNC_AWAIT(Sleep(ctx, std::chrono::milliseconds(10)));
    EXPECT_EQ(3, ::write(fds[1], "abc", 3));
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

  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::Status s = ASYNC_AWAIT(
        WaitReadable(ctx, fds[0], std::chrono::milliseconds(20)));
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

// Under Asio the reactor decides what a second waiter on an fd gets.  epoll
// won't register the fd again, so the second wait fails at once.  kqueue
// moves the fd's events to the second registration, so the first wait is
// never woken.
TEST(AsyncTest, SecondWaiterOnAnFdReturns) {
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO &&                     \
    !defined(BOOST_ASIO_HAS_EPOLL)
  GTEST_SKIP() << "only epoll refuses a second waiter on an fd";
#endif
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));

  RuntimeFixture fx;
  absl::Status first;
  absl::Status second;
  bool second_returned = false;
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    first = ASYNC_AWAIT(WaitReadable(ctx, fds[0], std::chrono::seconds(5)));
  });
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    second = ASYNC_AWAIT(WaitReadable(ctx, fds[0], std::chrono::seconds(5)));
    second_returned = true;
  });
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    ASYNC_AWAIT(Sleep(ctx, std::chrono::milliseconds(10)));
    EXPECT_EQ(1, ::write(fds[1], "x", 1));
  });

  fx.Run();
  EXPECT_TRUE(first.ok()) << first;
  EXPECT_TRUE(second_returned);
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  EXPECT_TRUE(absl::IsInternal(second)) << second;
#else
  EXPECT_TRUE(second.ok()) << second;
#endif
  ::close(fds[0]);
  ::close(fds[1]);
}

TEST(AsyncTest, WaitEitherReturnsReadyFd) {
  int a[2], b[2];
  ASSERT_EQ(0, ::pipe(a));
  ASSERT_EQ(0, ::pipe(b));

  RuntimeFixture fx;
  int ready = -1;

  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::StatusOr<int> r = ASYNC_AWAIT(WaitEither(ctx, a[0], b[0]));
    if (r.ok()) {
      ready = *r;
    }
  });

  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    ASYNC_AWAIT(Sleep(ctx, std::chrono::milliseconds(10)));
    EXPECT_EQ(1, ::write(b[1], "x", 1));
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

  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::StatusOr<int> r = ASYNC_AWAIT(
        WaitEither(ctx, a[0], b[0], std::chrono::milliseconds(20)));
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
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::StatusOr<StreamSocket> conn = ASYNC_AWAIT(listener.Accept(ctx));
    if (!conn.ok()) {
      ASYNC_RETURN;
    }
    char buf[256];
    absl::StatusOr<ssize_t> n =
        ASYNC_AWAIT(conn->ReceiveMessage(buf, sizeof(buf), ctx));
    if (n.ok() && *n > 0) {
      received.assign(buf, static_cast<size_t>(*n));
    }
  });

  // Client: connect to loopback, send one framed message.
  fx.runtime.Spawn([&, port](Context ctx) -> Task<void> {
    StreamSocket client;
    SocketAddress dest(InetAddress("127.0.0.1", port));
    if (!client.Connect(dest).ok()) {
      ASYNC_RETURN;
    }
    char outbuf[64];
    const char *payload = "hello-bridge";
    size_t len = std::strlen(payload);
    // SendMessage requires 4 bytes below the payload pointer.
    std::memcpy(outbuf + sizeof(int32_t), payload, len);
    (void)ASYNC_AWAIT(client.SendMessage(outbuf + sizeof(int32_t), len, ctx));
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

  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::StatusOr<UnixSocket> conn = ASYNC_AWAIT(listener.Accept(ctx));
    if (!conn.ok()) {
      ASYNC_RETURN;
    }
    absl::StatusOr<std::vector<char>> msg =
        ASYNC_AWAIT(conn->ReceiveVariableLengthMessage(ctx));
    if (!msg.ok()) {
      ASYNC_RETURN;
    }
    received_msg.assign(msg->data(), msg->size());

    std::vector<toolbelt::FileDescriptor> fds;
    absl::Status s = ASYNC_AWAIT(conn->ReceiveFds(fds, ctx));
    if (!s.ok() || fds.empty()) {
      ASYNC_RETURN;
    }
    char buf[16] = {};
    ssize_t n = ::read(fds[0].Fd(), buf, sizeof(buf));
    if (n > 0) {
      received_via_fd.assign(buf, static_cast<size_t>(n));
    }
  });

  fx.runtime.Spawn([&, path](Context ctx) -> Task<void> {
    UnixSocket client;
    if (!client.Connect(path).ok()) {
      ASYNC_RETURN;
    }
    // A pipe whose read end we pass to the peer.
    int p[2];
    if (::pipe(p) != 0) {
      ASYNC_RETURN;
    }
    EXPECT_EQ(3, ::write(p[1], "FD!", 3));
    ::close(p[1]);

    char outbuf[64];
    const char *payload = "hello-uds";
    size_t len = std::strlen(payload);
    std::memcpy(outbuf + sizeof(int32_t), payload, len);
    absl::StatusOr<ssize_t> sent =
        ASYNC_AWAIT(client.SendMessage(outbuf + sizeof(int32_t), len, ctx));
    if (!sent.ok()) {
      ::close(p[0]);
      ASYNC_RETURN;
    }
    std::vector<toolbelt::FileDescriptor> fds;
    fds.emplace_back(p[0], /*owned=*/false);
    (void)ASYNC_AWAIT(client.SendFds(fds, ctx));
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
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    empty_status = ASYNC_AWAIT(
        WaitWritable(ctx, fds[1], std::chrono::milliseconds(100)));
    char buf[4096] = {};
    while (::write(fds[1], buf, sizeof(buf)) > 0) {
    }
    full_status = ASYNC_AWAIT(
        WaitWritable(ctx, fds[1], std::chrono::milliseconds(20)));
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
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::StatusOr<StreamSocket> conn = ASYNC_AWAIT(listener->Accept(ctx));
    if (!conn.ok()) {
      ASYNC_RETURN;
    }
    char buf[4];
    absl::StatusOr<ssize_t> n =
        ASYNC_AWAIT(conn->Receive(buf, sizeof(buf), ctx));
    if (n.ok() && *n > 0) {
      received.assign(buf, static_cast<size_t>(*n));
    }
  });
  fx.runtime.Spawn([&, port](Context ctx) -> Task<void> {
    absl::StatusOr<StreamSocket> client = ASYNC_AWAIT(ConnectStream(
        InetAddress("127.0.0.1", port), std::chrono::seconds(5), ctx));
    connect_status = client.status();
    if (!client.ok()) {
      ASYNC_RETURN;
    }
    EXPECT_GE(StreamSocketFd(*client), 0);
    (void)ASYNC_AWAIT(client->Send("ping", 4, ctx));
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
  fx.runtime.Spawn([&, port](Context ctx) -> Task<void> {
    absl::StatusOr<StreamSocket> client = ASYNC_AWAIT(ConnectStream(
        InetAddress("127.0.0.1", port), std::chrono::seconds(5), ctx));
    status = client.status();
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
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    absl::StatusOr<StreamSocket> client = ASYNC_AWAIT(ConnectStream(
        InetAddress(bound), std::chrono::milliseconds(200), ctx));
    status = client.status();
  });
  fx.Run();
  EXPECT_TRUE(absl::IsDeadlineExceeded(status)) << status;
  for (int fd : fillers) {
    ::close(fd);
  }
  ::close(listen_fd);
}
#endif

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20

// A coroutine's interrupt fd ends its wait with CancelledError, which is how
// the server shuts its coroutines down.
TEST(AsyncTest, InterruptFdCancelsWait) {
  int fds[2], trigger[2];
  ASSERT_EQ(0, ::pipe(fds));
  ASSERT_EQ(0, ::pipe(trigger));

  RuntimeFixture fx;
  absl::Status status = absl::UnknownError("not run");
  fx.runtime.Spawn(
      [&](Context ctx) -> Task<void> {
        status = co_await WaitReadable(ctx, fds[0], std::chrono::seconds(5));
      },
      {.name = "waiter", .interrupt_fd = trigger[0]});
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    co_await Sleep(ctx, std::chrono::milliseconds(10));
    EXPECT_EQ(1, ::write(trigger[1], "x", 1));
  });
  fx.Run();
  EXPECT_TRUE(absl::IsCancelled(status)) << status;
  for (int fd : {fds[0], fds[1], trigger[0], trigger[1]}) {
    ::close(fd);
  }
}

TEST(AsyncTest, AllCoroutineStringsTracksLiveCoroutines) {
  RuntimeFixture fx;
  size_t seen_while_running = 0;
  fx.runtime.Spawn(
      [&](Context ctx) -> Task<void> {
        co_await Sleep(ctx, std::chrono::milliseconds(10));
      },
      {.name = "sleeper"});
  fx.runtime.Spawn([&](Context ctx) -> Task<void> {
    seen_while_running = fx.runtime.AllCoroutineStrings().size();
    co_return;
  });
  fx.Run();
  EXPECT_EQ(seen_while_running, 2u);
  EXPECT_TRUE(fx.runtime.AllCoroutineStrings().empty());
}

// A coroutine destroyed while suspended no longer shows as live.
TEST(AsyncTest, AllCoroutineStringsDropsDestroyedCoroutines) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));
  auto scheduler = std::make_unique<co20::Scheduler>();
  AsyncRuntime runtime(*scheduler);
  runtime.Spawn(
      [&](Context ctx) -> Task<void> {
        co_await WaitReadable(ctx, fds[0]);
      },
      {.name = "waiter"});
  runtime.Spawn([&](Context ctx) -> Task<void> {
    co_await Sleep(ctx, std::chrono::milliseconds(1));
    runtime.Stop();
  });
  runtime.Run();
  EXPECT_EQ(runtime.AllCoroutineStrings(),
            std::vector<std::string>{"waiter"});
  scheduler.reset();
  EXPECT_TRUE(runtime.AllCoroutineStrings().empty());
  ::close(fds[0]);
  ::close(fds[1]);
}

Task<int> AddOneTask(int x) { ASYNC_RETURN x + 1; }

Task<int> AddTwoTask(int x) {
  int y = ASYNC_AWAIT(AddOneTask(x));
  ASYNC_RETURN ASYNC_AWAIT(AddOneTask(y));
}

Task<void> NothingTask() { ASYNC_RETURN; }

Task<int> ThrowingTask() {
  throw std::runtime_error("boom");
  ASYNC_RETURN 0;
}

// Tasks made from the pooled promise still produce their results, including
// for void and for an exception.
TEST(AsyncTest, PooledTasksProduceResults) {
  EXPECT_EQ(3, SyncWait(AddTwoTask(1)));
  SyncWait(NothingTask());
  EXPECT_THROW(SyncWait(ThrowingTask()), std::runtime_error);
}

// Once a thread has run a task of each size, running them again takes every
// frame from the pool.
TEST(AsyncTest, FramePoolReusesFrames) {
  if (!kFramePoolEnabled) {
    GTEST_SKIP() << "frame pool is disabled in this build";
  }
  for (int i = 0; i < 4; i++) {
    EXPECT_EQ(i + 2, SyncWait(AddTwoTask(i)));
  }
  const size_t before = FramePoolHeapAllocations();
  for (int i = 0; i < 1000; i++) {
    EXPECT_EQ(i + 2, SyncWait(AddTwoTask(i)));
    SyncWait(NothingTask());
  }
  EXPECT_EQ(before, FramePoolHeapAllocations());
}

// A frame can be released by a thread other than the one that allocated it,
// and a thread that exits with frames in its pool gives them back.
TEST(AsyncTest, FramePoolAcrossThreads) {
  for (int round = 0; round < 20; round++) {
    // The frame is created here and finished and freed on the other thread.
    Task<int> task = AddTwoTask(round);
    int result = -1;
    std::thread t([&] { result = SyncWait(std::move(task)); });
    t.join();
    EXPECT_EQ(round + 2, result);

    // A thread whose pool holds frames when it exits.
    std::thread u([&] { EXPECT_EQ(5, SyncWait(AddTwoTask(3))); });
    u.join();
  }
}

// Outside a coroutine, a task whose Context doesn't suspend runs to the end
// in SyncWait, blocking the thread in poll().
TEST(AsyncTest, SyncWaitBlocksWithNullContext) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));
  absl::Status timeout = SyncWait(
      WaitReadable(NullContext(), fds[0], std::chrono::milliseconds(20)));
  EXPECT_TRUE(absl::IsDeadlineExceeded(timeout)) << timeout;
  ASSERT_EQ(1, ::write(fds[1], "x", 1));
  absl::Status ready = SyncWait(
      WaitReadable(NullContext(), fds[0], std::chrono::milliseconds(20)));
  EXPECT_TRUE(ready.ok()) << ready;
  SyncWait(Sleep(NullContext(), std::chrono::milliseconds(1)));

  // A closed fd is an error rather than a wait that never ends.
  int closed[2];
  ASSERT_EQ(0, ::pipe(closed));
  ::close(closed[0]);
  absl::Status bad = SyncWait(WaitReadable(NullContext(), closed[0]));
  EXPECT_TRUE(absl::IsInternal(bad)) << bad;
  ::close(closed[1]);

  int sv[2];
  ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM, 0, sv));
  UnixSocket a(sv[0], /*connected=*/true);
  UnixSocket b(sv[1], /*connected=*/true);
  char outbuf[16];
  std::memcpy(outbuf + sizeof(int32_t), "sync", 4);
  absl::StatusOr<ssize_t> sent =
      SyncWait(a.SendMessage(outbuf + sizeof(int32_t), 4, NullContext()));
  ASSERT_TRUE(sent.ok()) << sent.status();
  char inbuf[16];
  absl::StatusOr<ssize_t> got =
      SyncWait(b.ReceiveMessage(inbuf, sizeof(inbuf), NullContext()));
  ASSERT_TRUE(got.ok()) << got.status();
  EXPECT_EQ(std::string(inbuf, static_cast<size_t>(*got)), "sync");
  ::close(fds[0]);
  ::close(fds[1]);
}

// With an AsyncWaiter Context the wait goes through the waiter.  A stackful
// co::Coroutine yields there, so the writer gets to run while the reader
// waits on the same thread.
TEST(AsyncTest, AsyncWaiterContextYieldsStackfulCoroutine) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));
  co::CoroutineScheduler scheduler;
  absl::Status status = absl::UnknownError("not run");
  scheduler.Spawn([&](co::Coroutine *c) {
    const toolbelt::AsyncWaiter *waiter = c;
    status = SyncWait(
        WaitReadable(Context(waiter), fds[0], std::chrono::seconds(5)));
  });
  scheduler.Spawn([&](co::Coroutine *c) {
    c->Millisleep(10);
    EXPECT_EQ(1, ::write(fds[1], "x", 1));
  });
  scheduler.Run();
  EXPECT_TRUE(status.ok()) << status;
  ::close(fds[0]);
  ::close(fds[1]);
}

#endif

}  // namespace
}  // namespace subspace::async
