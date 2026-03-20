// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/syscall_shim.h"

namespace subspace {

// Wrapper for ::open which is variadic and cannot be stored directly as a
// typed function pointer.
static int RealOpen(const char *path, int flags, mode_t mode) {
  return ::open(path, flags, mode);
}

SyscallShim::SyscallShim() : open_fn(RealOpen) {}

static SyscallShim default_shim;
static thread_local SyscallShim *active_shim = &default_shim;

SyscallShim &GetSyscallShim() { return *active_shim; }

void SetSyscallShim(SyscallShim *shim) {
  active_shim = (shim != nullptr) ? shim : &default_shim;
}

} // namespace subspace
