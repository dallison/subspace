// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#include <boost/asio.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>

namespace subspace::asio_rpc {

// Wait for a single FD to become readable, yielding the Asio coroutine.
absl::Status async_wait_readable(boost::asio::io_context &ioc, int fd,
                                 boost::asio::yield_context yield);

// Wait for either fd1 or fd2 to become readable.
// Returns which raw FD triggered (fd1 or fd2).
absl::StatusOr<int> async_wait_either(boost::asio::io_context &ioc, int fd1,
                                      int fd2,
                                      boost::asio::yield_context yield);

// Wait for a single FD with a timeout.
// Returns DeadlineExceeded if the timeout expires.
// A zero timeout means wait forever.
absl::Status async_wait_readable_timeout(boost::asio::io_context &ioc, int fd,
                                         std::chrono::nanoseconds timeout,
                                         boost::asio::yield_context yield);

// Overloads that derive the io_context from the yield_context's executor.
absl::Status async_wait_readable(int fd, boost::asio::yield_context yield);

absl::StatusOr<int> async_wait_either(int fd1, int fd2,
                                      boost::asio::yield_context yield);

absl::Status async_wait_readable_timeout(int fd,
                                         std::chrono::nanoseconds timeout,
                                         boost::asio::yield_context yield);

} // namespace subspace::asio_rpc
