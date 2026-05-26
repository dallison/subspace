// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asio_rpc/common/async_wait.h"
#include <unistd.h>

namespace subspace::asio_rpc {

static boost::asio::io_context &
ioc_from_yield(boost::asio::yield_context yield) {
  return static_cast<boost::asio::io_context &>(
      yield.get_executor().context());
}

absl::Status async_wait_readable(int fd, boost::asio::yield_context yield) {
  return async_wait_readable(ioc_from_yield(yield), fd, yield);
}

absl::StatusOr<int> async_wait_either(int fd1, int fd2,
                                      boost::asio::yield_context yield) {
  return async_wait_either(ioc_from_yield(yield), fd1, fd2, yield);
}

absl::Status async_wait_readable_timeout(int fd,
                                         std::chrono::nanoseconds timeout,
                                         boost::asio::yield_context yield) {
  return async_wait_readable_timeout(ioc_from_yield(yield), fd, timeout, yield);
}

absl::Status async_wait_readable(boost::asio::io_context &ioc, int fd,
                                 boost::asio::yield_context yield) {
  int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    return absl::InternalError("Failed to dup fd for async_wait");
  }
  boost::asio::posix::stream_descriptor sd(ioc, dup_fd);
  boost::system::error_code ec;
  sd.async_wait(boost::asio::posix::stream_descriptor::wait_read, yield[ec]);
  if (ec) {
    return absl::InternalError(
        absl::StrFormat("async_wait_readable error: %s", ec.message()));
  }
  return absl::OkStatus();
}

absl::StatusOr<int> async_wait_either(boost::asio::io_context &ioc, int fd1,
                                      int fd2,
                                      boost::asio::yield_context yield) {
  int dup1 = ::dup(fd1);
  int dup2 = ::dup(fd2);
  if (dup1 < 0 || dup2 < 0) {
    if (dup1 >= 0)
      ::close(dup1);
    if (dup2 >= 0)
      ::close(dup2);
    return absl::InternalError("Failed to dup fds for async_wait_either");
  }

  auto sd1 =
      std::make_shared<boost::asio::posix::stream_descriptor>(ioc, dup1);
  auto sd2 =
      std::make_shared<boost::asio::posix::stream_descriptor>(ioc, dup2);

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
  notifier.async_wait(yield[ec]);

  if (*result_fd < 0) {
    return absl::InternalError("async_wait_either: no fd became ready");
  }
  return *result_fd;
}

absl::Status async_wait_readable_timeout(boost::asio::io_context &ioc, int fd,
                                         std::chrono::nanoseconds timeout,
                                         boost::asio::yield_context yield) {
  if (timeout.count() == 0) {
    return async_wait_readable(ioc, fd, yield);
  }

  int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    return absl::InternalError("Failed to dup fd for async_wait_timeout");
  }

  auto sd =
      std::make_shared<boost::asio::posix::stream_descriptor>(ioc, dup_fd);
  auto timer = std::make_shared<boost::asio::steady_timer>(ioc, timeout);
  auto timed_out = std::make_shared<bool>(false);
  auto fd_ready = std::make_shared<bool>(false);

  boost::asio::steady_timer notifier(ioc);
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

  boost::system::error_code ec;
  notifier.async_wait(yield[ec]);

  if (*timed_out) {
    return absl::DeadlineExceededError("Timeout waiting for fd");
  }
  if (!*fd_ready) {
    return absl::InternalError("async_wait_readable_timeout: unexpected state");
  }
  return absl::OkStatus();
}

} // namespace subspace::asio_rpc
