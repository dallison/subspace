// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_FRAME_POOL_H
#define _COMMON_ASYNC_FRAME_POOL_H

// A recycling allocator for C++20 coroutine frames.
//
// Under the co20 backend every call to a function returning async::Task<T>
// allocates a coroutine frame.  The publish and read paths are made of several
// such calls, so with the global allocator each message costs several heap
// allocations.  The frame pool keeps the frames of finished coroutines on a
// per-thread free list for each size class and hands them out again, so once a
// thread has run each path once the paths allocate nothing.
//
// task.h installs the pool for every co20::ValueTask, so nothing needs to call
// this directly except tests.
//
// Details:
//  - Each thread has its own free lists, so there are no locks.  A frame
//    freed on a different thread from the one that allocated it just joins
//    the freeing thread's list.
//  - Frames larger than kMaxPooledFrameSize, and frames beyond the cap each
//    list keeps, go to the global allocator.
//  - The pool is disabled under AddressSanitizer and ThreadSanitizer so that
//    they still see every frame's lifetime and can report a use after free.
//    Define SUBSPACE_ASYNC_FRAME_POOL=0 to disable it everywhere, or =1 to
//    force it on.

#include <cstddef>

#ifndef SUBSPACE_ASYNC_FRAME_POOL
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__) ||          \
    defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
#define SUBSPACE_ASYNC_FRAME_POOL 0
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define SUBSPACE_ASYNC_FRAME_POOL 0
#endif
#endif
#endif
#ifndef SUBSPACE_ASYNC_FRAME_POOL
#define SUBSPACE_ASYNC_FRAME_POOL 1
#endif

namespace subspace::async {

// True if coroutine frames are recycled.
inline constexpr bool kFramePoolEnabled = SUBSPACE_ASYNC_FRAME_POOL != 0;

// The largest frame, header included, that the pool recycles.
inline constexpr std::size_t kMaxPooledFrameSize = 4096;

namespace detail {

// Allocate and free a coroutine frame.  DeallocateFrame must be given a
// pointer from AllocateFrame, on any thread.
void *AllocateFrame(std::size_t size);
void DeallocateFrame(void *frame) noexcept;

}  // namespace detail

// The number of times the calling thread had to ask the global allocator for a
// frame.  Pool hits are not counted.  Zero when the pool is disabled.
std::size_t FramePoolHeapAllocations();

}  // namespace subspace::async

#endif  // _COMMON_ASYNC_FRAME_POOL_H
