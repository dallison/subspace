// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>

namespace subspace::coro_rpc {

constexpr auto use_nothrow =
    boost::asio::as_tuple(boost::asio::use_awaitable);

// Wait for a single FD to become readable, suspending the C++20 coroutine.
boost::asio::awaitable<absl::Status> async_wait_readable(int fd);

// Wait for either fd1 or fd2 to become readable.
// Returns which raw FD triggered (fd1 or fd2).
boost::asio::awaitable<absl::StatusOr<int>> async_wait_either(int fd1,
                                                               int fd2);

// Wait for a single FD with a timeout.
// Returns DeadlineExceeded if the timeout expires.
// A zero timeout means wait forever.
boost::asio::awaitable<absl::Status>
async_wait_readable_timeout(int fd, std::chrono::nanoseconds timeout);

} // namespace subspace::coro_rpc
