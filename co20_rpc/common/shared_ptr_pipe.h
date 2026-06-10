// Copyright 2023-2026 David Allison
// co20 RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "co/coroutine_cpp20.h"
#include "rpc/common/shared_ptr_pipe_base.h"

#include <memory>
#include <sys/poll.h>

namespace subspace::co20_rpc {

// A SharedPtrPipe that carries std::shared_ptr<T> between two C++20 stackless
// coroutines (co20) in the same process, built on the backend-agnostic
// rpc_internal::SharedPtrPipeBase.
//
// Read() is a co20::ValueTask: when the pipe is empty it suspends the calling
// coroutine (via co20::Wait) until data arrives or the coroutine's interrupt fd
// fires.  Because co20::ValueTask<T> requires T to be default-constructible
// (and absl::StatusOr is not), Read yields its result through an out parameter
// and returns an absl::Status (CancelledError when interrupted by shutdown).
//
// Write() is intentionally synchronous and non-blocking: the producer (the
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

  // Read the next shared_ptr into `out`, suspending the coroutine until data is
  // available.  Returns CancelledError if the coroutine's interrupt fd fires.
  co20::ValueTask<absl::Status> Read(std::shared_ptr<T> &out) {
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
        int fd = co_await co20::Wait(ReadFd(), POLLIN);
        if (co20::self != nullptr && fd == co20::self->GetInterruptFd()) {
          co_return absl::CancelledError("SharedPtrPipe read interrupted");
        }
        continue;
      }
      total += n;
    }
    out = rpc_internal::SharedPtrCodec<T>::Decode(buffer);
    co_return absl::OkStatus();
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
};

} // namespace subspace::co20_rpc
