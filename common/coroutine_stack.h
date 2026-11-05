// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_COROUTINE_STACK_H
#define _COMMON_COROUTINE_STACK_H

#include <cstddef>

// Stack size for coroutines the server and the shadow process spawn.  This is
// ordinary process memory, not shared memory, so clients do not need the same
// value.  A downstream Bazel module sets subspace.coroutine_stack_size(bytes =
// N) in its root MODULE.bazel.  Building this repo uses
// --//:coroutine_stack_size=N, and CMake uses
// -DSUBSPACE_COROUTINE_STACK_SIZE=N.  N must be a positive multiple of 4096.
// The default is 64 KiB, the co library's default.
#ifndef SUBSPACE_COROUTINE_STACK_SIZE
#define SUBSPACE_COROUTINE_STACK_SIZE (64 * 1024)
#endif

// On macOS, ASan's strerror interceptor takes about 56 KiB of stack.  A
// coroutine that formats errno then overruns a 64 KiB stack.  Builds with
// AddressSanitizer use at least 256 KiB, even when the configured size is
// smaller.
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define SUBSPACE_COROUTINE_STACK_ASAN 1
#endif
#endif
#if defined(__SANITIZE_ADDRESS__)
#define SUBSPACE_COROUTINE_STACK_ASAN 1
#endif

namespace subspace {

inline constexpr size_t kConfiguredCoroutineStackSize =
    SUBSPACE_COROUTINE_STACK_SIZE;
static_assert(kConfiguredCoroutineStackSize > 0 &&
                  kConfiguredCoroutineStackSize % 4096 == 0,
              "SUBSPACE_COROUTINE_STACK_SIZE must be a positive multiple of 4096");

inline constexpr size_t kAsanCoroutineStackFloor = 256 * 1024;

inline constexpr size_t CoroutineStackSize() {
#ifdef SUBSPACE_COROUTINE_STACK_ASAN
  return kConfiguredCoroutineStackSize < kAsanCoroutineStackFloor
             ? kAsanCoroutineStackFloor
             : kConfiguredCoroutineStackSize;
#else
  return kConfiguredCoroutineStackSize;
#endif
}

}  // namespace subspace

#endif  // _COMMON_COROUTINE_STACK_H
