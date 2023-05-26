// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_TRIGGERFD_H
#define __COMMON_TRIGGERFD_H

#include "absl/status/status.h"
#include "toolbelt/fd.h"

namespace subspace {

// A TriggerFd is a file descriptor that can be used to trigger
// an event.  If the facility is available, it is implemented
// as an eventfd, otherwise it's a pipe.
class TriggerFd {
 public:
  TriggerFd() = default;

  TriggerFd(const toolbelt::FileDescriptor& poll_fd, const toolbelt::FileDescriptor& trigger_fd)
      : poll_fd_(poll_fd), trigger_fd_(trigger_fd) {}
  TriggerFd(const TriggerFd &f) = delete;
  TriggerFd(TriggerFd &&f)
      : poll_fd_(std::move(f.poll_fd_)), trigger_fd_(std::move(f.trigger_fd_)) {
    f.poll_fd_.Reset();
    f.trigger_fd_.Reset();
  }
  TriggerFd &operator=(const TriggerFd &f) = delete;
  TriggerFd &operator=(TriggerFd &&f) {
    poll_fd_ = std::move(f.poll_fd_);
    trigger_fd_ = std::move(f.trigger_fd_);
    f.poll_fd_.Reset();
    f.trigger_fd_.Reset();
    return *this;
  }

  ~TriggerFd() = default;

  absl::Status Open();

  void Close() {
    poll_fd_.Close();
    trigger_fd_.Close();
  }

  void SetPollFd(toolbelt::FileDescriptor fd) { poll_fd_ = std::move(fd); }
  void SetTriggerFd(toolbelt::FileDescriptor fd) { trigger_fd_ = std::move(fd); }

  void Trigger();
  void Clear();

  toolbelt::FileDescriptor &GetPollFd() { return poll_fd_; }
  toolbelt::FileDescriptor &GetTriggerFd() { return trigger_fd_; }

  void AddPollFd(std::vector<struct pollfd> &fds) {
    fds.push_back({.fd = poll_fd_.Fd(), .events = 0});
  }

 private:
  toolbelt::FileDescriptor poll_fd_;     // File descriptor to poll on.
  toolbelt::FileDescriptor trigger_fd_;  // File descriptor to trigger.
};

}  // namespace subspace

#endif  // __COMMON_TRIGGERFD_H
