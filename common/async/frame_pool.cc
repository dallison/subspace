// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/frame_pool.h"

#include <cstdint>
#include <new>

namespace subspace::async {
namespace detail {

#if SUBSPACE_ASYNC_FRAME_POOL

namespace {

// Every block starts with a header that records its size class, so freeing
// does not depend on the compiler passing the frame size.  The header is the
// size of the strictest fundamental alignment so the frame after it is
// suitably aligned.
constexpr std::size_t kHeaderSize = alignof(std::max_align_t);
constexpr std::size_t kGranule = 64;
constexpr std::size_t kNumClasses = kMaxPooledFrameSize / kGranule;
// The most blocks each size class keeps.  This bounds the memory a thread
// holds on to after a burst of deeply nested coroutines.
constexpr std::uint32_t kMaxCachedPerClass = 256;
// Class value for blocks that came straight from the global allocator.
constexpr std::uint32_t kHeapClass = UINT32_MAX;

static_assert(kHeaderSize >= sizeof(std::uint32_t));
static_assert(kMaxPooledFrameSize % kGranule == 0);

struct Header {
  std::uint32_t size_class;
};

// A free block.  The link lives in the frame area so the header stays valid
// while the block is on a list.
struct FreeBlock {
  FreeBlock *next;
};

struct Pool {
  FreeBlock *head[kNumClasses] = {};
  std::uint32_t count[kNumClasses] = {};
  std::size_t heap_allocations = 0;

  ~Pool();
};

// Set when this thread's pool has been destroyed.  A coroutine frame can be
// released from another thread-local object's destructor after that, in which
// case it goes to the global allocator.  Constant initialized and trivially
// destructible, so it is always safe to read.
thread_local bool pool_destroyed = false;

Pool::~Pool() {
  pool_destroyed = true;
  for (std::size_t i = 0; i < kNumClasses; i++) {
    while (head[i] != nullptr) {
      FreeBlock *block = head[i];
      head[i] = block->next;
      ::operator delete(reinterpret_cast<char *>(block) - kHeaderSize);
    }
  }
}

thread_local Pool pool;

Header *HeaderOf(void *frame) {
  return reinterpret_cast<Header *>(static_cast<char *>(frame) - kHeaderSize);
}

}  // namespace

void *AllocateFrame(std::size_t size) {
  const std::size_t total = size + kHeaderSize;
  char *block;
  if (total > kMaxPooledFrameSize || pool_destroyed) {
    block = static_cast<char *>(::operator new(total));
    reinterpret_cast<Header *>(block)->size_class = kHeapClass;
    return block + kHeaderSize;
  }
  const std::uint32_t size_class =
      static_cast<std::uint32_t>((total - 1) / kGranule);
  Pool &p = pool;
  if (FreeBlock *free_block = p.head[size_class]; free_block != nullptr) {
    p.head[size_class] = free_block->next;
    p.count[size_class]--;
    return free_block;
  }
  p.heap_allocations++;
  block = static_cast<char *>(::operator new((size_class + 1) * kGranule));
  reinterpret_cast<Header *>(block)->size_class = size_class;
  return block + kHeaderSize;
}

void DeallocateFrame(void *frame) noexcept {
  const std::uint32_t size_class = HeaderOf(frame)->size_class;
  if (size_class == kHeapClass || pool_destroyed ||
      pool.count[size_class] >= kMaxCachedPerClass) {
    ::operator delete(static_cast<char *>(frame) - kHeaderSize);
    return;
  }
  Pool &p = pool;
  auto *free_block = static_cast<FreeBlock *>(frame);
  free_block->next = p.head[size_class];
  p.head[size_class] = free_block;
  p.count[size_class]++;
}

}  // namespace detail

std::size_t FramePoolHeapAllocations() {
  return detail::pool_destroyed ? 0 : detail::pool.heap_allocations;
}

#else  // !SUBSPACE_ASYNC_FRAME_POOL

void *AllocateFrame(std::size_t size) { return ::operator new(size); }
void DeallocateFrame(void *frame) noexcept { ::operator delete(frame); }

}  // namespace detail

std::size_t FramePoolHeapAllocations() { return 0; }

#endif

}  // namespace subspace::async
