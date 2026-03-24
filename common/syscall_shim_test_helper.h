// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef SUBSPACE_COMMON_SYSCALL_SHIM_TEST_HELPER_H
#define SUBSPACE_COMMON_SYSCALL_SHIM_TEST_HELPER_H

#include "common/syscall_shim.h"
#include <cerrno>

namespace subspace {
namespace testing {

// RAII guard: installs a custom SyscallShim on construction, restores
// the default on destruction.
class ScopedSyscallShim {
public:
  explicit ScopedSyscallShim(SyscallShim *shim) { SetSyscallShim(shim); }
  ~ScopedSyscallShim() { SetSyscallShim(nullptr); }
  ScopedSyscallShim(const ScopedSyscallShim &) = delete;
  ScopedSyscallShim &operator=(const ScopedSyscallShim &) = delete;
};

// A SyscallShim where individual calls can be configured to fail after a
// countdown.  A countdown of 0 means "fail on the next call".  A countdown
// of -1 (the default) means "never fail -- forward to real syscall".
//
// When a call fails, the shim sets errno to the configured value and returns
// the appropriate error sentinel (-1 for most calls, MAP_FAILED for mmap).
struct FailingShim : SyscallShim {
  int mmap_fail_countdown = -1;
  int mmap_errno = ENOMEM;

  int munmap_fail_countdown = -1;
  int munmap_errno = EINVAL;

  int open_fail_countdown = -1;
  int open_errno = EACCES;

  int ftruncate_fail_countdown = -1;
  int ftruncate_errno = EIO;

#if !defined(__ANDROID__)
  int shm_open_fail_countdown = -1;
  int shm_open_errno = EACCES;
#endif

  int poll_fail_countdown = -1;
  int poll_errno = EINTR;

  int stat_fail_countdown = -1;
  int stat_errno = ENOENT;

  int fstat_fail_countdown = -1;
  int fstat_errno = EBADF;

  int chmod_fail_countdown = -1;
  int chmod_errno = EPERM;

  int chown_fail_countdown = -1;
  int chown_errno = EPERM;

  int write_fail_countdown = -1;
  int write_errno = EIO;

  int mmap_call_count = 0;
  int munmap_call_count = 0;
  int open_call_count = 0;
  int ftruncate_call_count = 0;
#if !defined(__ANDROID__)
  int shm_open_call_count = 0;
#endif
  int poll_call_count = 0;
  int stat_call_count = 0;
  int fstat_call_count = 0;
  int chmod_call_count = 0;
  int chown_call_count = 0;
  int write_call_count = 0;

  FailingShim() {
    mmap_fn = MmapWrapper;
    munmap_fn = MunmapWrapper;
    open_fn = OpenWrapper;
    ftruncate_fn = FtruncateWrapper;
#if !defined(__ANDROID__)
    shm_open_fn = ShmOpenWrapper;
#endif
    poll_fn = PollWrapper;
    stat_fn = StatWrapper;
    fstat_fn = FstatWrapper;
    chmod_fn = ChmodWrapper;
    chown_fn = ChownWrapper;
    write_fn = WriteWrapper;
  }

private:
  // Each wrapper checks its countdown, decrements, and either fails or
  // delegates to the real syscall.  We store "this" in a thread-local so
  // the static wrappers can find the instance.
  static FailingShim *Self() {
    return static_cast<FailingShim *>(&GetSyscallShim());
  }

  static bool ShouldFail(int &countdown) {
    if (countdown < 0)
      return false;
    if (countdown == 0) {
      countdown = -1;
      return true;
    }
    --countdown;
    return false;
  }

  static void *MmapWrapper(void *addr, size_t len, int prot, int flags, int fd,
                            off_t offset) {
    auto *s = Self();
    s->mmap_call_count++;
    if (ShouldFail(s->mmap_fail_countdown)) {
      errno = s->mmap_errno;
      return MAP_FAILED;
    }
    return ::mmap(addr, len, prot, flags, fd, offset);
  }

  static int MunmapWrapper(void *addr, size_t len) {
    auto *s = Self();
    s->munmap_call_count++;
    if (ShouldFail(s->munmap_fail_countdown)) {
      errno = s->munmap_errno;
      return -1;
    }
    return ::munmap(addr, len);
  }

  static int OpenWrapper(const char *path, int flags, mode_t mode) {
    auto *s = Self();
    s->open_call_count++;
    if (ShouldFail(s->open_fail_countdown)) {
      errno = s->open_errno;
      return -1;
    }
    return ::open(path, flags, mode);
  }

  static int FtruncateWrapper(int fd, off_t length) {
    auto *s = Self();
    s->ftruncate_call_count++;
    if (ShouldFail(s->ftruncate_fail_countdown)) {
      errno = s->ftruncate_errno;
      return -1;
    }
    return ::ftruncate(fd, length);
  }

#if !defined(__ANDROID__)
  static int ShmOpenWrapper(const char *name, int oflag, mode_t mode) {
    auto *s = Self();
    s->shm_open_call_count++;
    if (ShouldFail(s->shm_open_fail_countdown)) {
      errno = s->shm_open_errno;
      return -1;
    }
    return ::shm_open(name, oflag, mode);
  }
#endif

  static int PollWrapper(struct pollfd *fds, nfds_t nfds, int timeout) {
    auto *s = Self();
    s->poll_call_count++;
    if (ShouldFail(s->poll_fail_countdown)) {
      errno = s->poll_errno;
      return -1;
    }
    return ::poll(fds, nfds, timeout);
  }

  static int StatWrapper(const char *path, struct stat *buf) {
    auto *s = Self();
    s->stat_call_count++;
    if (ShouldFail(s->stat_fail_countdown)) {
      errno = s->stat_errno;
      return -1;
    }
    return ::stat(path, buf);
  }

  static int FstatWrapper(int fd, struct stat *buf) {
    auto *s = Self();
    s->fstat_call_count++;
    if (ShouldFail(s->fstat_fail_countdown)) {
      errno = s->fstat_errno;
      return -1;
    }
    return ::fstat(fd, buf);
  }

  static int ChmodWrapper(const char *path, mode_t mode) {
    auto *s = Self();
    s->chmod_call_count++;
    if (ShouldFail(s->chmod_fail_countdown)) {
      errno = s->chmod_errno;
      return -1;
    }
    return ::chmod(path, mode);
  }

  static int ChownWrapper(const char *path, uid_t owner, gid_t group) {
    auto *s = Self();
    s->chown_call_count++;
    if (ShouldFail(s->chown_fail_countdown)) {
      errno = s->chown_errno;
      return -1;
    }
    return ::chown(path, owner, group);
  }

  static ssize_t WriteWrapper(int fd, const void *buf, size_t count) {
    auto *s = Self();
    s->write_call_count++;
    if (ShouldFail(s->write_fail_countdown)) {
      errno = s->write_errno;
      return -1;
    }
    return ::write(fd, buf, count);
  }
};

} // namespace testing
} // namespace subspace

#endif // SUBSPACE_COMMON_SYSCALL_SHIM_TEST_HELPER_H
