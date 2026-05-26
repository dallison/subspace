// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef SUBSPACE_COMMON_SYSCALL_SHIM_H
#define SUBSPACE_COMMON_SYSCALL_SHIM_H

#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace subspace {

// Thin indirection layer over POSIX system calls, enabling tests to inject
// failures into error-handling paths that are otherwise impossible to reach.
//
// Production code calls GetSyscallShim().mmap_fn(...) etc.  The default shim
// forwards every call to the real libc symbol.  Tests install a custom shim
// via SetSyscallShim() to make individual calls return errors on demand.
struct SyscallShim {
  void *(*mmap_fn)(void *, size_t, int, int, int, off_t) = ::mmap;
  int (*munmap_fn)(void *, size_t) = ::munmap;
  int (*open_fn)(const char *, int, mode_t) = nullptr;
  int (*close_fn)(int) = ::close;
  int (*ftruncate_fn)(int, off_t) = ::ftruncate;
#if !defined(__ANDROID__)
  int (*shm_open_fn)(const char *, int, mode_t) = nullptr;
  int (*shm_unlink_fn)(const char *) = ::shm_unlink;
#endif
  int (*poll_fn)(struct pollfd *, nfds_t, int) = ::poll;
  int (*stat_fn)(const char *, struct stat *) = ::stat;
  int (*fstat_fn)(int, struct stat *) = ::fstat;
  int (*chmod_fn)(const char *, mode_t) = ::chmod;
  int (*chown_fn)(const char *, uid_t, gid_t) = ::chown;
  ssize_t (*write_fn)(int, const void *, size_t) = ::write;
  ssize_t (*read_fn)(int, void *, size_t) = ::read;

  SyscallShim();
};

SyscallShim &GetSyscallShim();

// Install a custom shim.  Pass nullptr to restore the default.
void SetSyscallShim(SyscallShim *shim);

} // namespace subspace

#endif // SUBSPACE_COMMON_SYSCALL_SHIM_H
