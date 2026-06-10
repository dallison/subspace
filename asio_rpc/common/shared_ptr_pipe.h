// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "rpc/common/shared_ptr_pipe_base.h"

#include <boost/asio.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/spawn.hpp>

#include <memory>
#include <unistd.h>

namespace subspace::asio_rpc {

// A SharedPtrPipe that carries std::shared_ptr<T> between two stackful Asio
// coroutines (boost::asio::yield_context) in the same process.  When a
// non-blocking read/write would block, the calling coroutine is suspended via
// boost::asio until the pipe end becomes ready, rather than blocking the
// io_context thread.
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
  absl::StatusOr<std::shared_ptr<T>> Read(boost::asio::yield_context yield) {
    char buffer[rpc_internal::SharedPtrCodec<T>::kBufferSize];
    const size_t length = sizeof(buffer);
    size_t total = 0;
    while (total < length) {
      size_t n = 0;
      absl::StatusOr<rpc_internal::PipeIo> io =
          RawRead(ReadFd(), buffer + total, length - total, &n);
      if (!io.ok()) {
        return io.status();
      }
      if (*io == rpc_internal::PipeIo::kEof) {
        return absl::InternalError("EOF on SharedPtrPipe");
      }
      if (*io == rpc_internal::PipeIo::kWouldBlock) {
        if (absl::Status s = Wait(ReadFd(), Direction::kRead, yield); !s.ok()) {
          return s;
        }
        continue;
      }
      total += n;
    }
    return rpc_internal::SharedPtrCodec<T>::Decode(buffer);
  }

  // Write a shared_ptr to the pipe, taking an in-transit reference for the
  // duration of the transfer.  Suspends the coroutine if the pipe is full.
  absl::Status Write(std::shared_ptr<T> p, boost::asio::yield_context yield) {
    char buffer[rpc_internal::SharedPtrCodec<T>::kBufferSize];
    rpc_internal::SharedPtrCodec<T>::Encode(p, buffer);
    bool transferred = false;
    // Release the in-transit reference unless the bytes made it into the pipe.
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
        if (absl::Status s = Wait(WriteFd(), Direction::kWrite, yield);
            !s.ok()) {
          return s;
        }
        continue;
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
  static absl::Status Wait(int fd, Direction direction,
                           boost::asio::yield_context yield) {
    int dup_fd = ::dup(fd);
    if (dup_fd < 0) {
      return absl::InternalError("Failed to dup fd for SharedPtrPipe wait");
    }
    boost::asio::posix::stream_descriptor sd(yield.get_executor(), dup_fd);
    boost::system::error_code ec;
    sd.async_wait(direction == Direction::kRead
                      ? boost::asio::posix::stream_descriptor::wait_read
                      : boost::asio::posix::stream_descriptor::wait_write,
                  yield[ec]);
    if (ec) {
      return absl::InternalError("SharedPtrPipe wait failed: " + ec.message());
    }
    return absl::OkStatus();
  }
};

} // namespace subspace::asio_rpc
