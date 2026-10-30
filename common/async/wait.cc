// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/wait.h"

#include <sys/poll.h>

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include "absl/strings/str_format.h"
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <memory>
#include <unistd.h>

namespace subspace::async {

namespace {
using Clock = std::chrono::steady_clock;

boost::asio::io_context &IocFromYield(boost::asio::yield_context yield) {
  return static_cast<boost::asio::io_context &>(
      yield.get_executor().context());
}

// When a wait of `timeout` that starts now ends, or max if it has no timeout.
Clock::time_point DeadlineAfter(std::chrono::nanoseconds timeout) {
  const Clock::time_point now = Clock::now();
  if (timeout.count() == 0 || timeout >= Clock::time_point::max() - now) {
    return Clock::time_point::max();
  }
  return now + timeout;
}

// The timeout for the rest of a wait that ends at `deadline`: zero if it has
// none, and DeadlineExceededError once it has passed.
absl::StatusOr<std::chrono::nanoseconds> TimeLeft(Clock::time_point deadline) {
  if (deadline == Clock::time_point::max()) {
    return std::chrono::nanoseconds(0);
  }
  auto left = std::chrono::duration_cast<std::chrono::nanoseconds>(
      deadline - Clock::now());
  if (left.count() <= 0) {
    return absl::DeadlineExceededError("Timeout waiting for fd");
  }
  return left;
}

// Asio can report an fd ready when it isn't.  Its epoll reactor returns the
// state it keeps for a descriptor to a pool as soon as the descriptor is
// released, so a readiness event still queued for a descriptor that a wait
// has released can complete the next wait that gets the same state.  The
// waits here register and release a descriptor every time, so they check with
// poll() that an fd is ready before returning, and wait again if it isn't.
// An error or hangup counts as ready, so the caller's next read or write
// reports it.
constexpr short kAlwaysReady = POLLERR | POLLHUP;

// Whether `fd` is ready for `events` now.
bool IsReady(int fd, short events) {
  struct pollfd pfd = {.fd = fd, .events = events, .revents = 0};
  return ::poll(&pfd, 1, 0) > 0 &&
         (pfd.revents & (events | kAlwaysReady)) != 0;
}

// Which of `fd1` and `fd2` is readable now, or -1 if neither is.
int ReadableOf(int fd1, int fd2) {
  struct pollfd fds[2] = {{.fd = fd1, .events = POLLIN, .revents = 0},
                          {.fd = fd2, .events = POLLIN, .revents = 0}};
  if (::poll(fds, 2, 0) > 0) {
    if ((fds[0].revents & (POLLIN | kAlwaysReady)) != 0) {
      return fd1;
    }
    if ((fds[1].revents & (POLLIN | kAlwaysReady)) != 0) {
      return fd2;
    }
  }
  return -1;
}

// Registers `fd` with the reactor.  epoll registers an fd only once in an
// io_context, so this fails while another coroutine is waiting on `fd`.
// Failing here, rather than letting the descriptor's constructor throw, keeps
// the waiting coroutine from ending without a trace.  kqueue accepts the
// second registration and moves the fd's events to it, so there this
// succeeds and the other coroutine's wait is never woken.
absl::StatusOr<std::shared_ptr<boost::asio::posix::stream_descriptor>>
RegisterFd(boost::asio::io_context &ioc, int fd, const char *what) {
  auto sd = std::make_shared<boost::asio::posix::stream_descriptor>(ioc);
  boost::system::error_code ec;
  sd->assign(fd, ec);
  if (ec) {
    return absl::InternalError(absl::StrFormat("%s: can't wait on fd %d: %s",
                                               what, fd, ec.message()));
  }
  return sd;
}

// Wait for the reactor to report `fd` ready for `cond`, which it can do when
// it isn't (see IsReady).
absl::Status ReactorWaitFd(Context ctx, int fd,
                           boost::asio::posix::descriptor_base::wait_type cond,
                           std::chrono::nanoseconds timeout, const char *what) {
  boost::asio::io_context &ioc = IocFromYield(ctx);
  // We register the caller's real fd with the reactor and release() it before
  // the descriptor is destroyed so we never close it.  We intentionally do NOT
  // ::dup(): dup'ing churns ephemeral fd numbers through the shared process fd
  // table, and two io_contexts (e.g. an in-process server + client) recycling
  // those numbers across their separate reactors can resume a coroutine owned
  // by the other io_context's thread.  The stable real fd avoids that.
  //
  // The coroutine always suspends on a sentinel steady_timer (`notifier`),
  // never directly on the stream_descriptor.  This is essential for graceful
  // shutdown: a cancellation emitted onto the coroutine can complete a
  // *descriptor* wait synchronously, re-entering and unwinding this coroutine
  // (destroying this frame's descriptor) while asio is still walking the
  // cancellation chain - a use-after-free.  Cancelling a steady_timer always
  // posts its completion, so the coroutine only ever resumes on a clean stack.
  // The descriptor (and the optional timeout timer) use plain handlers that
  // merely wake the notifier, so a cancellation never reaches them directly.
  absl::StatusOr<std::shared_ptr<boost::asio::posix::stream_descriptor>>
      registered = RegisterFd(ioc, fd, what);
  if (!registered.ok()) {
    return registered.status();
  }
  std::shared_ptr<boost::asio::posix::stream_descriptor> sd =
      std::move(*registered);
  auto fd_ready = std::make_shared<bool>(false);
  auto timed_out = std::make_shared<bool>(false);
  auto cancelled = std::make_shared<bool>(false);
  auto notified = std::make_shared<bool>(false);

  // The notifier is shared so the cancellation handler installed below can hold
  // it alive and safely poke it even if it fires after this frame unwinds.
  auto notifier = std::make_shared<boost::asio::steady_timer>(ioc);
  notifier->expires_at(boost::asio::steady_timer::time_point::max());

  // Single, idempotent wake of the notifier.  Every path that wants to resume
  // the coroutine (fd ready, timeout, graceful-shutdown cancellation) goes
  // through here, so notifier->cancel() is called exactly once - never twice,
  // and never racing asio's per-operation timer cancellation.
  auto wake = [notifier, notified]() {
    if (!*notified) {
      *notified = true;
      notifier->cancel();
    }
  };

  std::shared_ptr<boost::asio::steady_timer> timer;
  if (timeout.count() != 0) {
    timer = std::make_shared<boost::asio::steady_timer>(ioc, timeout);
  }

  // Bind the descriptor (and timeout-timer) completions to the coroutine's own
  // executor (its strand).  Under a multi-threaded io_context the reactor would
  // otherwise run these handlers on an arbitrary thread the instant the fd is
  // ready - possibly before the coroutine below registers notifier->async_wait()
  // - so the wake() would cancel nothing and the coroutine would hang forever on
  // the never-expiring notifier (a missed wakeup).  Serializing them with the
  // async_wait registration on the strand makes the wake safe.
  sd->async_wait(
      cond,
      boost::asio::bind_executor(
          ctx.get_executor(),
          [sd, timer, fd_ready, timed_out, cancelled,
           wake](const boost::system::error_code &ec) {
            if (!*timed_out && !*cancelled && !ec) {
              *fd_ready = true;
            }
            wake();
          }));

  if (timer != nullptr) {
    timer->async_wait(boost::asio::bind_executor(
        ctx.get_executor(), [sd, timed_out, fd_ready, cancelled,
                             wake](const boost::system::error_code &ec) {
          if (!*fd_ready && !*cancelled && !ec) {
            *timed_out = true;
            wake();
          }
        }));
  }

  // Graceful shutdown: instead of letting the coroutine's cancellation_signal
  // cancel the notifier's timer operation directly (which races the notifier's
  // own natural completion and corrupts asio's timer queue - a use-after-free),
  // route cancellation through the same idempotent wake().  We assign our own
  // handler to the coroutine's cancellation slot and wait on the notifier with
  // an *unconnected* slot so asio installs no per-operation timer cancellation.
  auto exec = ctx.get_executor();
  auto cancel_slot = ctx.get_cancellation_slot();
  if (cancel_slot.is_connected()) {
    cancel_slot.assign([exec, wake, cancelled](boost::asio::cancellation_type) {
      boost::asio::post(exec, [wake, cancelled]() {
        *cancelled = true;
        wake();
      });
    });
  }

  boost::system::error_code ec;
  notifier->async_wait(
      boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot(),
                                          ctx[ec]));

  // The coroutine has resumed (fd ready, timed out, or cancelled).  Clear our
  // cancellation handler so a late re-emit cannot poke a stale wake, then cancel
  // any still-pending descriptor/timer waits and release the real fd so
  // destroying the shared descriptor does not close it.
  if (cancel_slot.is_connected()) {
    cancel_slot.clear();
  }
  boost::system::error_code ignored;
  sd->cancel(ignored);
  sd->release();
  if (timer != nullptr) {
    timer->cancel();
  }

  if (*fd_ready) {
    return absl::OkStatus();
  }
  if (*timed_out) {
    return absl::DeadlineExceededError("Timeout waiting for fd");
  }
  // Resumed without the fd becoming ready and without timing out: the wait
  // was cancelled (e.g. graceful shutdown).
  return absl::CancelledError(absl::StrFormat("%s: operation canceled", what));
}

// Wait for `fd` to become ready for `cond`, which `poll_events` names for
// poll().
absl::Status WaitFd(Context ctx, int fd,
                    boost::asio::posix::descriptor_base::wait_type cond,
                    short poll_events, std::chrono::nanoseconds timeout,
                    const char *what) {
  const Clock::time_point deadline = DeadlineAfter(timeout);
  for (;;) {
    // Checking before suspending also keeps a persistently-readable fd (e.g. a
    // triggered shutdown fd) from racing a graceful-shutdown cancellation that
    // could otherwise repeatedly wake us before the reactor reports the
    // readable fd, livelocking the coroutine so it never observes the fd and
    // never exits.
    if (IsReady(fd, poll_events)) {
      return absl::OkStatus();
    }
    absl::StatusOr<std::chrono::nanoseconds> left = TimeLeft(deadline);
    if (!left.ok()) {
      return left.status();
    }
    if (absl::Status s = ReactorWaitFd(ctx, fd, cond, *left, what); !s.ok()) {
      return s;
    }
  }
}

// Wait for the reactor to report `fd1` or `fd2` readable, which it can do
// when neither is (see IsReady).
absl::Status ReactorWaitEither(Context ctx, int fd1, int fd2,
                               std::chrono::nanoseconds timeout) {
  boost::asio::io_context &ioc = IocFromYield(ctx);
  // Register the caller's real fds (no ::dup(); see ReactorWaitFd) and
  // release() them before the descriptors are destroyed so we never close
  // them.
  absl::StatusOr<std::shared_ptr<boost::asio::posix::stream_descriptor>>
      registered1 = RegisterFd(ioc, fd1, "WaitEither");
  if (!registered1.ok()) {
    return registered1.status();
  }
  std::shared_ptr<boost::asio::posix::stream_descriptor> sd1 =
      std::move(*registered1);
  absl::StatusOr<std::shared_ptr<boost::asio::posix::stream_descriptor>>
      registered2 = RegisterFd(ioc, fd2, "WaitEither");
  if (!registered2.ok()) {
    sd1->release();
    return registered2.status();
  }
  std::shared_ptr<boost::asio::posix::stream_descriptor> sd2 =
      std::move(*registered2);

  auto result_fd = std::make_shared<int>(-1);
  auto done = std::make_shared<bool>(false);
  auto timed_out = std::make_shared<bool>(false);
  auto cancelled = std::make_shared<bool>(false);
  auto notified = std::make_shared<bool>(false);
  // Shared so the cancellation handler can keep it alive past this frame.
  auto notifier = std::make_shared<boost::asio::steady_timer>(ioc);
  notifier->expires_at(boost::asio::steady_timer::time_point::max());

  // Single idempotent wake (see ReactorWaitFd): fd-ready and graceful-shutdown
  // cancellation both resume the coroutine through here, so notifier->cancel()
  // is called exactly once and never races asio's per-operation cancellation.
  auto wake = [notifier, notified]() {
    if (!*notified) {
      *notified = true;
      notifier->cancel();
    }
  };

  std::shared_ptr<boost::asio::steady_timer> timer;
  if (timeout.count() != 0) {
    timer = std::make_shared<boost::asio::steady_timer>(ioc, timeout);
  }

  // Bind both descriptor completions to the coroutine's executor (its strand)
  // so they are serialized with the notifier->async_wait() registration below
  // and with each other; otherwise a multi-threaded io_context could run a
  // completion on another thread before the wait is registered (missed wakeup)
  // or run both concurrently and race on `done`.
  sd1->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                  boost::asio::bind_executor(
                      ctx.get_executor(),
                      [sd1, sd2, timer, result_fd, done, timed_out, cancelled,
                       wake,
                       fd1](const boost::system::error_code &ec) {
                        if (!*done && !*timed_out && !*cancelled && !ec) {
                          *done = true;
                          *result_fd = fd1;
                          boost::system::error_code ignored;
                          sd2->cancel(ignored);
                          if (timer != nullptr) {
                            timer->cancel();
                          }
                        }
                        wake();
                      }));

  sd2->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                  boost::asio::bind_executor(
                      ctx.get_executor(),
                      [sd1, sd2, timer, result_fd, done, timed_out, cancelled,
                       wake,
                       fd2](const boost::system::error_code &ec) {
                        if (!*done && !*timed_out && !*cancelled && !ec) {
                          *done = true;
                          *result_fd = fd2;
                          boost::system::error_code ignored;
                          sd1->cancel(ignored);
                          if (timer != nullptr) {
                            timer->cancel();
                          }
                        }
                        wake();
                      }));

  if (timer != nullptr) {
    timer->async_wait(boost::asio::bind_executor(
        ctx.get_executor(), [sd1, sd2, done, timed_out, cancelled,
                             wake](const boost::system::error_code &ec) {
          if (!*done && !*cancelled && !ec) {
            *timed_out = true;
            boost::system::error_code ignored;
            sd1->cancel(ignored);
            sd2->cancel(ignored);
          }
          wake();
        }));
  }

  // Graceful shutdown routes through the same idempotent wake() rather than
  // cancelling the notifier op directly (see ReactorWaitFd).
  auto exec = ctx.get_executor();
  auto cancel_slot = ctx.get_cancellation_slot();
  if (cancel_slot.is_connected()) {
    cancel_slot.assign([exec, wake, cancelled](boost::asio::cancellation_type) {
      boost::asio::post(exec, [wake, cancelled]() {
        *cancelled = true;
        wake();
      });
    });
  }

  boost::system::error_code ec;
  notifier->async_wait(
      boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot(),
                                          ctx[ec]));

  // Clear our handler so a late re-emit cannot poke a stale wake, then release
  // the real fds so destroying the shared descriptors does not close them.
  if (cancel_slot.is_connected()) {
    cancel_slot.clear();
  }
  boost::system::error_code ignored;
  sd1->cancel(ignored);
  sd2->cancel(ignored);
  sd1->release();
  sd2->release();
  if (timer != nullptr) {
    timer->cancel();
  }

  if (*result_fd < 0) {
    if (*timed_out) {
      return absl::DeadlineExceededError("Timeout waiting for fd");
    }
    return absl::CancelledError("WaitEither: operation canceled");
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status WaitReadable(Context ctx, int fd, std::chrono::nanoseconds timeout) {
  return WaitFd(ctx, fd, boost::asio::posix::stream_descriptor::wait_read,
                POLLIN, timeout, "WaitReadable");
}

absl::Status WaitWritable(Context ctx, int fd,
                          std::chrono::nanoseconds timeout) {
  return WaitFd(ctx, fd, boost::asio::posix::stream_descriptor::wait_write,
                POLLOUT, timeout, "WaitWritable");
}

absl::StatusOr<int> WaitEither(Context ctx, int fd1, int fd2,
                               std::chrono::nanoseconds timeout) {
  const Clock::time_point deadline = DeadlineAfter(timeout);
  for (;;) {
    // As in WaitFd, checking first also avoids a livelock at shutdown.
    if (int fd = ReadableOf(fd1, fd2); fd >= 0) {
      return fd;
    }
    absl::StatusOr<std::chrono::nanoseconds> left = TimeLeft(deadline);
    if (!left.ok()) {
      return left.status();
    }
    if (absl::Status s = ReactorWaitEither(ctx, fd1, fd2, *left); !s.ok()) {
      return s;
    }
  }
}

void Sleep(Context ctx, std::chrono::nanoseconds duration) {
  boost::asio::io_context &ioc = IocFromYield(ctx);
  // Shared so the cancellation handler can keep it alive past this frame.
  auto timer = std::make_shared<boost::asio::steady_timer>(ioc, duration);
  auto notified = std::make_shared<bool>(false);
  // Idempotent wake: natural expiry resumes the coroutine normally; a
  // graceful-shutdown cancellation resumes it early via a single timer->cancel()
  // routed through here, never via asio's per-operation cancellation (which
  // would race the timer's natural expiry and corrupt the timer queue).
  auto wake = [timer, notified]() {
    if (!*notified) {
      *notified = true;
      timer->cancel();
    }
  };
  auto exec = ctx.get_executor();
  auto cancel_slot = ctx.get_cancellation_slot();
  if (cancel_slot.is_connected()) {
    cancel_slot.assign([exec, wake](boost::asio::cancellation_type) {
      boost::asio::post(exec, [wake]() { wake(); });
    });
  }
  boost::system::error_code ec;
  timer->async_wait(
      boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot(),
                                          ctx[ec]));
  if (cancel_slot.is_connected()) {
    cancel_slot.clear();
  }
}

}  // namespace subspace::async

#else  // SUBSPACE_CORO_BACKEND_CO

namespace subspace::async {

absl::Status WaitReadable(Context ctx, int fd, std::chrono::nanoseconds timeout) {
  int r = ctx->Wait(fd, POLLIN, static_cast<uint64_t>(timeout.count()));
  if (timeout.count() != 0 && r == -1) {
    return absl::DeadlineExceededError("Timeout waiting for fd");
  }
  return absl::OkStatus();
}

absl::Status WaitWritable(Context ctx, int fd,
                          std::chrono::nanoseconds timeout) {
  int r = ctx->Wait(fd, POLLOUT, static_cast<uint64_t>(timeout.count()));
  if (r == fd) {
    return absl::OkStatus();
  }
  if (r == -1 && timeout.count() != 0) {
    return absl::DeadlineExceededError("Timeout waiting for fd");
  }
  // Woken by the coroutine's interrupt fd.
  return absl::CancelledError("WaitWritable: interrupted");
}

absl::StatusOr<int> WaitEither(Context ctx, int fd1, int fd2,
                               std::chrono::nanoseconds timeout) {
  int r = ctx->Wait(std::vector<int>{fd1, fd2}, POLLIN,
                    static_cast<uint64_t>(timeout.count()));
  if (r == -1) {
    if (timeout.count() != 0) {
      return absl::DeadlineExceededError("Timeout waiting for fd");
    }
    return absl::InternalError("WaitEither: no fd became ready");
  }
  return r;
}

void Sleep(Context ctx, std::chrono::nanoseconds duration) {
  ctx->Nanosleep(static_cast<uint64_t>(duration.count()));
}

}  // namespace subspace::async

#endif
