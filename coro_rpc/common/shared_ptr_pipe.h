// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "coro_rpc/common/async_wait.h"
#include "rpc/common/shared_ptr_pipe_base.h"

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <memory>
#include <unistd.h>

namespace subspace::coro_rpc {

// A SharedPtrPipe that carries std::shared_ptr<T> between two C++20 Asio
// coroutines (boost::asio::awaitable) in the same process, built on the
// backend-agnostic rpc_internal::SharedPtrPipeBase.
//
// Read() is awaitable: when the pipe is empty it suspends the calling coroutine
// (via a boost::asio stream_descriptor wait) until data arrives.
//
// Write() is intentionally synchronous and non-blocking.  The producer (the
// request coroutine) cannot co_await from inside a plain reply callback, and a
// shared_ptr is only sizeof(std::shared_ptr<T>) bytes, far below PIPE_BUF, so a
// write to a non-full pipe completes atomically in a single syscall.  In-flight
// replies are bounded by the method channel's slot count, which is far smaller
// than the pipe capacity, so the pipe never fills in practice.
//
// See rpc_internal::SharedPtrPipeBase for the in-transit reference semantics
// and the single-process restriction.
template <typename T>
class SharedPtrPipe : public rpc_internal::SharedPtrPipeBase {
public:
  static absl::StatusOr<SharedPtrPipe<T>> Create() {
    SharedPtrPipe<T> pipe;
    if (absl::Status status = pipe.OpenPipe(); !status.ok()) {
      return status;
    }
    return pipe;
  }

  SharedPtrPipe() = default;
  SharedPtrPipe(SharedPtrPipe &&) = default;
  SharedPtrPipe &operator=(SharedPtrPipe &&) = default;

  // Read the next shared_ptr from the pipe, suspending the coroutine until data
  // is available.
  boost::asio::awaitable<absl::StatusOr<std::shared_ptr<T>>> Read() {
    char buffer[rpc_internal::SharedPtrCodec<T>::kBufferSize];
    const size_t length = sizeof(buffer);
    size_t total = 0;
    while (total < length) {
      size_t n = 0;
      absl::StatusOr<rpc_internal::PipeIo> io =
          RawRead(ReadFd(), buffer + total, length - total, &n);
      if (!io.ok()) {
        co_return io.status();
      }
      if (*io == rpc_internal::PipeIo::kEof) {
        co_return absl::InternalError("EOF on SharedPtrPipe");
      }
      if (*io == rpc_internal::PipeIo::kWouldBlock) {
        if (absl::Status s = co_await Wait(ReadFd(), Direction::kRead);
            !s.ok()) {
          co_return s;
        }
        continue;
      }
      total += n;
    }
    co_return rpc_internal::SharedPtrCodec<T>::Decode(buffer);
  }

  // Write a shared_ptr to the pipe, taking an in-transit reference for the
  // duration of the transfer.  Non-blocking: returns ResourceExhausted if the
  // pipe is momentarily full (see the class comment for why this does not occur
  // in practice).
  absl::Status Write(std::shared_ptr<T> p) {
    char buffer[rpc_internal::SharedPtrCodec<T>::kBufferSize];
    rpc_internal::SharedPtrCodec<T>::Encode(p, buffer);
    bool transferred = false;
    struct Cleanup {
      char *buffer;
      bool *transferred;
      ~Cleanup() {
        if (!*transferred) {
          rpc_internal::SharedPtrCodec<T>::ReleaseInTransit(buffer);
        }
      }
    } cleanup{buffer, &transferred};

    const size_t length = sizeof(buffer);
    size_t total = 0;
    while (total < length) {
      size_t n = 0;
      absl::StatusOr<rpc_internal::PipeIo> io =
          RawWrite(WriteFd(), buffer + total, length - total, &n);
      if (!io.ok()) {
        return io.status();
      }
      if (*io == rpc_internal::PipeIo::kEof) {
        return absl::InternalError("EOF on SharedPtrPipe");
      }
      if (*io == rpc_internal::PipeIo::kWouldBlock) {
        return absl::ResourceExhaustedError("SharedPtrPipe is full");
      }
      total += n;
    }
    transferred = true;
    return absl::OkStatus();
  }

private:
  enum class Direction { kRead, kWrite };

  // Suspend the coroutine until `fd` is ready in the requested direction.  A
  // dup is wrapped in a stream_descriptor so Asio's destructor closes the dup
  // and never the pipe fd this object owns.
  static boost::asio::awaitable<absl::Status> Wait(int fd, Direction direction) {
    auto executor = co_await boost::asio::this_coro::executor;
    int dup_fd = ::dup(fd);
    if (dup_fd < 0) {
      co_return absl::InternalError("Failed to dup fd for SharedPtrPipe wait");
    }
    boost::asio::posix::stream_descriptor sd(executor, dup_fd);
    auto [ec] = co_await sd.async_wait(
        direction == Direction::kRead
            ? boost::asio::posix::stream_descriptor::wait_read
            : boost::asio::posix::stream_descriptor::wait_write,
        use_nothrow);
    if (ec) {
      co_return absl::InternalError("SharedPtrPipe wait failed: " +
                                    ec.message());
    }
    co_return absl::OkStatus();
  }
};

} // namespace subspace::coro_rpc
