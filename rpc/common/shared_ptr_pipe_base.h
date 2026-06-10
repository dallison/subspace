// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <string>
#include <unistd.h>

namespace subspace::rpc_internal {

// Result of a single non-blocking read/write attempt on a pipe fd.
enum class PipeIo {
  kProgress,   // Bytes were transferred (see the out parameter for how many).
  kWouldBlock, // The fd is not ready; the caller must wait and retry.
  kEof,        // The peer closed the pipe.
};

// Backend-agnostic machinery for a pipe that carries std::shared_ptr<T> values
// between two coroutines in the same process.  It owns the pipe file
// descriptors and provides the non-blocking byte transfer primitives plus the
// shared_ptr (de)serialization, but it deliberately knows nothing about any
// particular async runtime: each RPC backend derives a SharedPtrPipe<T> that
// supplies the runtime-specific "wait until readable/writable" step between
// non-blocking attempts (co::Coroutine, boost::asio::yield_context,
// boost::asio::awaitable, the co20 scheduler, ...).
//
// NB: like toolbelt::SharedPtrPipe this only works within a single process; the
// transferred bytes are the raw representation of a std::shared_ptr and are
// meaningless across a process boundary.
class SharedPtrPipeBase {
public:
  SharedPtrPipeBase() = default;
  SharedPtrPipeBase(const SharedPtrPipeBase &) = delete;
  SharedPtrPipeBase &operator=(const SharedPtrPipeBase &) = delete;

  SharedPtrPipeBase(SharedPtrPipeBase &&other) noexcept
      : read_fd_(other.read_fd_), write_fd_(other.write_fd_) {
    other.read_fd_ = -1;
    other.write_fd_ = -1;
  }
  SharedPtrPipeBase &operator=(SharedPtrPipeBase &&other) noexcept {
    if (this != &other) {
      Close();
      read_fd_ = other.read_fd_;
      write_fd_ = other.write_fd_;
      other.read_fd_ = -1;
      other.write_fd_ = -1;
    }
    return *this;
  }

  ~SharedPtrPipeBase() { Close(); }

  int ReadFd() const { return read_fd_; }
  int WriteFd() const { return write_fd_; }

  void Close() {
    if (read_fd_ >= 0) {
      ::close(read_fd_);
      read_fd_ = -1;
    }
    if (write_fd_ >= 0) {
      ::close(write_fd_);
      write_fd_ = -1;
    }
  }

protected:
  // Create the underlying pipe and put both ends into non-blocking mode so the
  // derived class can interleave non-blocking I/O with its runtime's async
  // wait.
  absl::Status OpenPipe() {
    int fds[2];
    if (::pipe(fds) == -1) {
      return absl::InternalError(std::string("Failed to create pipe: ") +
                                 ::strerror(errno));
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
    for (int fd : {read_fd_, write_fd_}) {
      int flags = ::fcntl(fd, F_GETFL, 0);
      if (flags == -1 ||
          ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        absl::Status status = absl::InternalError(
            std::string("Failed to set pipe non-blocking: ") +
            ::strerror(errno));
        Close();
        return status;
      }
    }
    return absl::OkStatus();
  }

  // Attempt a single non-blocking read of up to `length` bytes.  On
  // PipeIo::kProgress, *bytes holds the number of bytes read (always > 0).  An
  // EINTR is reported as kProgress with *bytes == 0 so the caller simply
  // retries.
  static absl::StatusOr<PipeIo> RawRead(int fd, char *buffer, size_t length,
                                        size_t *bytes) {
    *bytes = 0;
    ssize_t n = ::read(fd, buffer, length);
    if (n > 0) {
      *bytes = static_cast<size_t>(n);
      return PipeIo::kProgress;
    }
    if (n == 0) {
      return PipeIo::kEof;
    }
    if (errno == EINTR) {
      return PipeIo::kProgress; // *bytes == 0: retry.
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return PipeIo::kWouldBlock;
    }
    return absl::InternalError(std::string("SharedPtrPipe read failed: ") +
                               ::strerror(errno));
  }

  // Attempt a single non-blocking write of up to `length` bytes.  Semantics
  // mirror RawRead.
  static absl::StatusOr<PipeIo> RawWrite(int fd, const char *buffer,
                                         size_t length, size_t *bytes) {
    *bytes = 0;
    ssize_t n = ::write(fd, buffer, length);
    if (n > 0) {
      *bytes = static_cast<size_t>(n);
      return PipeIo::kProgress;
    }
    if (n == 0) {
      return PipeIo::kEof;
    }
    if (errno == EINTR) {
      return PipeIo::kProgress; // *bytes == 0: retry.
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return PipeIo::kWouldBlock;
    }
    return absl::InternalError(std::string("SharedPtrPipe write failed: ") +
                               ::strerror(errno));
  }

  int read_fd_ = -1;
  int write_fd_ = -1;
};

// Encodes/decodes a std::shared_ptr<T> into the raw byte buffer carried by the
// pipe, maintaining the "in-transit reference" the way toolbelt::SharedPtrPipe
// does: writing places an extra reference into the buffer that stays live while
// the bytes are in the pipe; reading takes ownership of that reference.
template <typename T> struct SharedPtrCodec {
  static constexpr size_t kBufferSize = sizeof(std::shared_ptr<T>);

  // Construct an in-transit (+1 ref) copy of `p` into `buffer`.
  static void Encode(const std::shared_ptr<T> &p, char *buffer) {
    new (buffer) std::shared_ptr<T>(p);
  }

  // Drop the in-transit reference held by `buffer` (failure / cleanup path).
  static void ReleaseInTransit(char *buffer) {
    reinterpret_cast<std::shared_ptr<T> *>(buffer)->reset();
  }

  // Take ownership of the shared_ptr encoded in `buffer`, clearing the buffer's
  // in-transit reference.
  static std::shared_ptr<T> Decode(char *buffer) {
    auto *p = reinterpret_cast<std::shared_ptr<T> *>(buffer);
    std::shared_ptr<T> result = *p;
    p->reset();
    return result;
  }
};

} // namespace subspace::rpc_internal
