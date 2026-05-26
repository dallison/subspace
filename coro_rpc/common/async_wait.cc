// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "coro_rpc/common/async_wait.h"
#include "absl/strings/str_format.h"
#include <boost/asio/this_coro.hpp>
#include <unistd.h>

namespace subspace::coro_rpc {

boost::asio::awaitable<absl::Status> async_wait_readable(int fd) {
  auto executor = co_await boost::asio::this_coro::executor;

  int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    co_return absl::InternalError("Failed to dup fd for async_wait");
  }
  boost::asio::posix::stream_descriptor sd(executor, dup_fd);
  auto [ec] = co_await sd.async_wait(
      boost::asio::posix::stream_descriptor::wait_read, use_nothrow);
  if (ec) {
    co_return absl::InternalError(
        absl::StrFormat("async_wait_readable error: %s", ec.message()));
  }
  co_return absl::OkStatus();
}

boost::asio::awaitable<absl::StatusOr<int>> async_wait_either(int fd1,
                                                               int fd2) {
  auto executor = co_await boost::asio::this_coro::executor;

  int dup1 = ::dup(fd1);
  int dup2 = ::dup(fd2);
  if (dup1 < 0 || dup2 < 0) {
    if (dup1 >= 0)
      ::close(dup1);
    if (dup2 >= 0)
      ::close(dup2);
    co_return absl::InternalError("Failed to dup fds for async_wait_either");
  }

  auto sd1 =
      std::make_shared<boost::asio::posix::stream_descriptor>(executor, dup1);
  auto sd2 =
      std::make_shared<boost::asio::posix::stream_descriptor>(executor, dup2);

  auto result_fd = std::make_shared<int>(-1);
  auto done = std::make_shared<bool>(false);
  boost::asio::steady_timer notifier(executor);
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

  co_await notifier.async_wait(use_nothrow);

  if (*result_fd < 0) {
    co_return absl::InternalError("async_wait_either: no fd became ready");
  }
  co_return *result_fd;
}

boost::asio::awaitable<absl::Status>
async_wait_readable_timeout(int fd, std::chrono::nanoseconds timeout) {
  if (timeout.count() == 0) {
    co_return co_await async_wait_readable(fd);
  }

  auto executor = co_await boost::asio::this_coro::executor;

  int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    co_return absl::InternalError("Failed to dup fd for async_wait_timeout");
  }

  auto sd =
      std::make_shared<boost::asio::posix::stream_descriptor>(executor, dup_fd);
  auto timer =
      std::make_shared<boost::asio::steady_timer>(executor, timeout);
  auto timed_out = std::make_shared<bool>(false);
  auto fd_ready = std::make_shared<bool>(false);

  boost::asio::steady_timer notifier(executor);
  notifier.expires_at(boost::asio::steady_timer::time_point::max());

  sd->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                 [sd, timer, fd_ready, timed_out, &notifier](
                     const boost::system::error_code &ec) {
                   if (!*timed_out && !ec) {
                     *fd_ready = true;
                     timer->cancel();
                     notifier.cancel();
                   }
                 });

  timer->async_wait([sd, timed_out, fd_ready, &notifier](
                        const boost::system::error_code &ec) {
    if (!*fd_ready && !ec) {
      *timed_out = true;
      boost::system::error_code ignored;
      sd->cancel(ignored);
      notifier.cancel();
    }
  });

  co_await notifier.async_wait(use_nothrow);

  if (*timed_out) {
    co_return absl::DeadlineExceededError("Timeout waiting for fd");
  }
  if (!*fd_ready) {
    co_return absl::InternalError(
        "async_wait_readable_timeout: unexpected state");
  }
  co_return absl::OkStatus();
}

} // namespace subspace::coro_rpc
