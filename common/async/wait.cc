// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/wait.h"

#include <sys/poll.h>

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include "absl/strings/str_format.h"
#include <boost/asio/io_context.hpp>
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
  // Dup the fd so the posix::stream_descriptor can own/close its own copy
  // without disturbing the caller's fd.
  int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    return absl::InternalError("Failed to dup fd for WaitReadable");
  }

  if (timeout.count() == 0) {
    boost::asio::posix::stream_descriptor sd(ioc, dup_fd);
    boost::system::error_code ec;
    sd.async_wait(boost::asio::posix::stream_descriptor::wait_read, ctx[ec]);
    if (ec) {
      return absl::InternalError(
          absl::StrFormat("WaitReadable error: %s", ec.message()));
    }
    return absl::OkStatus();
  }

  auto sd = std::make_shared<boost::asio::posix::stream_descriptor>(ioc, dup_fd);
  auto timer = std::make_shared<boost::asio::steady_timer>(ioc, timeout);
  auto timed_out = std::make_shared<bool>(false);
  auto fd_ready = std::make_shared<bool>(false);

  boost::asio::steady_timer notifier(ioc);
  notifier.expires_at(boost::asio::steady_timer::time_point::max());

  sd->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                 [sd, timer, fd_ready, timed_out,
                  &notifier](const boost::system::error_code &ec) {
                   if (!*timed_out && !ec) {
                     *fd_ready = true;
                     timer->cancel();
                     notifier.cancel();
                   }
                 });

  timer->async_wait([sd, timed_out, fd_ready,
                     &notifier](const boost::system::error_code &ec) {
    if (!*fd_ready && !ec) {
      *timed_out = true;
      boost::system::error_code ignored;
      sd->cancel(ignored);
      notifier.cancel();
    }
  });

  boost::system::error_code ec;
  notifier.async_wait(ctx[ec]);

  if (*timed_out) {
    return absl::DeadlineExceededError("Timeout waiting for fd");
  }
  if (!*fd_ready) {
    return absl::InternalError("WaitReadable: unexpected state");
  }
  return absl::OkStatus();
}

absl::StatusOr<int> WaitEither(Context ctx, int fd1, int fd2) {
  boost::asio::io_context &ioc = IocFromYield(ctx);
  int dup1 = ::dup(fd1);
  int dup2 = ::dup(fd2);
  if (dup1 < 0 || dup2 < 0) {
    if (dup1 >= 0)
      ::close(dup1);
    if (dup2 >= 0)
      ::close(dup2);
    return absl::InternalError("Failed to dup fds for WaitEither");
  }

  auto sd1 = std::make_shared<boost::asio::posix::stream_descriptor>(ioc, dup1);
  auto sd2 = std::make_shared<boost::asio::posix::stream_descriptor>(ioc, dup2);

  auto result_fd = std::make_shared<int>(-1);
  auto done = std::make_shared<bool>(false);
  boost::asio::steady_timer notifier(ioc);
  notifier.expires_at(boost::asio::steady_timer::time_point::max());

  sd1->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                  [sd1, sd2, result_fd, done, &notifier,
                   fd1](const boost::system::error_code &ec) {
                    if (!*done && !ec) {
                      *done = true;
                      *result_fd = fd1;
                      boost::system::error_code ignored;
                      sd2->cancel(ignored);
                      notifier.cancel();
                    }
                  });

  sd2->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                  [sd1, sd2, result_fd, done, &notifier,
                   fd2](const boost::system::error_code &ec) {
                    if (!*done && !ec) {
                      *done = true;
                      *result_fd = fd2;
                      boost::system::error_code ignored;
                      sd1->cancel(ignored);
                      notifier.cancel();
                    }
                  });

  boost::system::error_code ec;
  notifier.async_wait(ctx[ec]);

  if (*result_fd < 0) {
    return absl::InternalError("WaitEither: no fd became ready");
  }
  return *result_fd;
}

void Sleep(Context ctx, std::chrono::nanoseconds duration) {
  boost::asio::io_context &ioc = IocFromYield(ctx);
  boost::asio::steady_timer timer(ioc, duration);
  boost::system::error_code ec;
  timer.async_wait(ctx[ec]);
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
