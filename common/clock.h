// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_CLOCK_H
#define __COMMON_CLOCK_H

#include <cstdint>
#include <time.h>
#if defined(__APPLE__)
#include <mach/mach_time.h>
#endif

namespace subspace {

#if defined(__APPLE__)
namespace {
mach_timebase_info_data_t timebase;
bool timebase_set = false;
} // namespace
#endif

// Current monotonic time in nanoseconds.
inline uint64_t Now() {
#if defined(__APPLE__)
  if (!timebase_set) {
    mach_timebase_info(&timebase);
    timebase_set = true;
  }
  return mach_absolute_time() * timebase.numer / timebase.denom;
#else
  struct timespec tp;
  clock_gettime(CLOCK_MONOTONIC, &tp);
  return static_cast<uint64_t>(tp.tv_sec) * 1000000000LL +
         static_cast<uint64_t>(tp.tv_nsec);
#endif
}
} // namespace subspace

#endif //  __COMMON_CLOCK_H
