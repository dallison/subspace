// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
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
boost::asio::io_context &IocFromYield(boost::asio::yield_context yield) {
  return static_cast<boost::asio::io_context &>(
      yield.get_executor().context());
}
}  // namespace

absl::Status WaitReadable(Context ctx, int fd, std::chrono::nanoseconds timeout) {
  boost::asio::io_context &ioc = IocFromYield(ctx);
  // Fast path: if the fd is already readable, return immediately without
  // suspending.  This keeps a persistently-readable fd (e.g. a triggered
  // shutdown fd) from racing a graceful-shutdown cancellation that could
  // otherwise repeatedly wake us before the reactor reports the readable fd,
  // livelocking the coroutine so it never observes the fd and never exits.
  {
    struct pollfd pfd = {.fd = fd, .events = POLLIN, .revents = 0};
    if (::poll(&pfd, 1, 0) > 0 && (pfd.revents & (POLLIN | POLLHUP)) != 0) {
      return absl::OkStatus();
    }
  }
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
  auto sd = std::make_shared<boost::asio::posix::stream_descriptor>(ioc, fd);
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
      boost::asio::posix::stream_descriptor::wait_read,
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
  ctx.get_cancellation_slot().assign(
      [exec, wake, cancelled](boost::asio::cancellation_type) {
        boost::asio::post(exec, [wake, cancelled]() {
          *cancelled = true;
          wake();
        });
      });

  boost::system::error_code ec;
  notifier->async_wait(
      boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot(),
                                          ctx[ec]));

  // The coroutine has resumed (fd ready, timed out, or cancelled).  Clear our
  // cancellation handler so a late re-emit cannot poke a stale wake, then cancel
  // any still-pending descriptor/timer waits and release the real fd so
  // destroying the shared descriptor does not close it.
  ctx.get_cancellation_slot().clear();
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
  // Resumed without the fd becoming readable and without timing out: the wait
  // was cancelled (e.g. graceful shutdown).
  return absl::InternalError("WaitReadable: operation canceled");
}

absl::StatusOr<int> WaitEither(Context ctx, int fd1, int fd2) {
  boost::asio::io_context &ioc = IocFromYield(ctx);
  // Register the caller's real fds (no ::dup(); see WaitReadable) and release()
  // them before the descriptors are destroyed so we never close them.
  auto sd1 = std::make_shared<boost::asio::posix::stream_descriptor>(ioc, fd1);
  auto sd2 = std::make_shared<boost::asio::posix::stream_descriptor>(ioc, fd2);

  auto result_fd = std::make_shared<int>(-1);
  auto done = std::make_shared<bool>(false);
  auto notified = std::make_shared<bool>(false);
  // Shared so the cancellation handler can keep it alive past this frame.
  auto notifier = std::make_shared<boost::asio::steady_timer>(ioc);
  notifier->expires_at(boost::asio::steady_timer::time_point::max());

  // Single idempotent wake (see WaitReadable): fd-ready and graceful-shutdown
  // cancellation both resume the coroutine through here, so notifier->cancel()
  // is called exactly once and never races asio's per-operation cancellation.
  auto wake = [notifier, notified]() {
    if (!*notified) {
      *notified = true;
      notifier->cancel();
    }
  };

  // Bind both descriptor completions to the coroutine's executor (its strand)
  // so they are serialized with the notifier->async_wait() registration below
  // and with each other; otherwise a multi-threaded io_context could run a
  // completion on another thread before the wait is registered (missed wakeup)
  // or run both concurrently and race on `done`.
  sd1->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                  boost::asio::bind_executor(
                      ctx.get_executor(),
                      [sd1, sd2, result_fd, done, wake,
                       fd1](const boost::system::error_code &ec) {
                        if (!*done && !ec) {
                          *done = true;
                          *result_fd = fd1;
                          boost::system::error_code ignored;
                          sd2->cancel(ignored);
                        }
                        wake();
                      }));

  sd2->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                  boost::asio::bind_executor(
                      ctx.get_executor(),
                      [sd1, sd2, result_fd, done, wake,
                       fd2](const boost::system::error_code &ec) {
                        if (!*done && !ec) {
                          *done = true;
                          *result_fd = fd2;
                          boost::system::error_code ignored;
                          sd1->cancel(ignored);
                        }
                        wake();
                      }));

  // Graceful shutdown routes through the same idempotent wake() rather than
  // cancelling the notifier op directly (see WaitReadable).
  auto exec = ctx.get_executor();
  ctx.get_cancellation_slot().assign(
      [exec, wake](boost::asio::cancellation_type) {
        boost::asio::post(exec, [wake]() { wake(); });
      });

  boost::system::error_code ec;
  notifier->async_wait(
      boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot(),
                                          ctx[ec]));

  // Clear our handler so a late re-emit cannot poke a stale wake, then release
  // the real fds so destroying the shared descriptors does not close them.
  ctx.get_cancellation_slot().clear();
  boost::system::error_code ignored;
  sd1->cancel(ignored);
  sd2->cancel(ignored);
  sd1->release();
  sd2->release();

  if (*result_fd < 0) {
    return absl::InternalError("WaitEither: no fd became ready");
  }
  return *result_fd;
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
  ctx.get_cancellation_slot().assign(
      [exec, wake](boost::asio::cancellation_type) {
        boost::asio::post(exec, [wake]() { wake(); });
      });
  boost::system::error_code ec;
  timer->async_wait(
      boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot(),
                                          ctx[ec]));
  ctx.get_cancellation_slot().clear();
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

absl::StatusOr<int> WaitEither(Context ctx, int fd1, int fd2) {
  int r = ctx->Wait(std::vector<int>{fd1, fd2}, POLLIN);
  if (r == -1) {
    return absl::InternalError("WaitEither: no fd became ready");
  }
  return r;
}

void Sleep(Context ctx, std::chrono::nanoseconds duration) {
  ctx->Nanosleep(static_cast<uint64_t>(duration.count()));
}

}  // namespace subspace::async

#endif
