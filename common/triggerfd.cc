// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/triggerfd.h"
#include "absl/strings/str_format.h"

#if defined(__linux__)
#include <sys/eventfd.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace subspace {
absl::Status TriggerFd::Open() {
#if defined(__linux__)
  int fd = eventfd(0, EFD_NONBLOCK);
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Unable to create trigger fd eventfd: %s", strerror(errno)));
  }
  poll_fd_.SetFd(fd);
  trigger_fd_ = poll_fd_;
#else
  int pipes[2];
  if (pipe(pipes) == -1) {
    return absl::InternalError(absl::StrFormat(
        "Unable to create trigger fd pipe: %s", strerror(errno)));
  }
  // Both ends of the pipe are nonblocking.
  if (fcntl(pipes[0], F_SETFL, (fcntl(pipes[0], F_GETFL) | O_NONBLOCK)) == -1) {
    return absl::InternalError(absl::StrFormat(
        "Unable to set non-blocking on read pipe: %s", strerror(errno)));
  }
  if (fcntl(pipes[1], F_SETFL, (fcntl(pipes[1], F_GETFL) | O_NONBLOCK)) == -1) {
    return absl::InternalError(absl::StrFormat(
        "Unable to set non-blocking on write pipe: %s", strerror(errno)));
  }
  poll_fd_.SetFd(pipes[0]);     // Read end
  trigger_fd_.SetFd(pipes[1]);  // Write end.
#endif
  return absl::OkStatus();
}

void TriggerFd::Trigger() {
#if defined(__linux__)
  // Linux eventfd.  Write an 8 byte value to trigger it.
  int64_t val = 1;
  (void)::write(trigger_fd_.Fd(), &val, 8);
#else
  // Pipe.  Write a single byte to it.
  char val = 'x';
  (void)::write(trigger_fd_.Fd(), &val, 1);
#endif
}

void TriggerFd::Clear() {
#if defined(__linux__)
  // Linux eventfd, read a single 8 byte value.  This will clear the
  // eventfd.
  int64_t val;
  (void)::read(poll_fd_.Fd(), &val, 8);
#else
  // Pipe.  Read all bytes from the pipe.  The pipe is nonblocking so we will
  // get an EAGAIN when we've cleared it.
  for (;;) {
    char buffer[256];
    ssize_t n = ::read(poll_fd_.Fd(), buffer, sizeof(buffer));
    if (n <= 0) {
      break;
    }
  }
#endif
}

}  // namespace subspace
