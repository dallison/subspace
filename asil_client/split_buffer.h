// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Split buffer channels keep the message prefixes in one shared memory object
// and each slot's payload in its own.  The ASIL client maps all of them when a
// publisher or subscriber opens, into slot storage that the caller provides.

#pragma once

#include "asil_client/error.h"

#include <cstdint>

namespace subspace {
namespace asil {

// How the creator of a split buffer allocated it.  The values match the
// ClientBufferAllocator enum in proto/subspace.proto.
enum class BufferAllocator : int32_t {
  kUnspecified = 0,
  kMemfd = 1,
  kSplitShm = 2,
  // A custom allocator: mapping the slot needs a SplitBufferAllocator.
  kSplitCallback = 3,
  kSplitBufferFreeTest = 4,
};

// What the server knows about one split buffer slot.
struct SplitBufferInfo {
  // The channel that owns the buffers: the multiplexer of a virtual channel.
  const char *channel_name = nullptr;
  uint64_t session_id = 0;
  uint32_t buffer_index = 0;
  uint32_t slot_id = 0;
  uint64_t full_size = 0;
  uint64_t allocation_size = 0;
  // Defined by the allocator that created the slot.
  uint64_t handle = 0;
  int64_t map_offset = 0;
  BufferAllocator allocator = BufferAllocator::kUnspecified;
  // The descriptor the creator registered with the server, or -1.  It is
  // valid only during the map callback.
  int fd = -1;
};

struct SplitBufferMapping {
  void *address = nullptr;
  // 0 from a map callback means the slot's payload size.
  uint64_t size = 0;
  // For the allocator's use.
  void *private_data = nullptr;
};

// Maps slots that a custom allocator created.  Both functions are called
// only from Open() and Close(), with the context given here.
struct SplitBufferAllocator {
  Error (*map)(void *context, const SplitBufferInfo &info,
               SplitBufferMapping &mapping) = nullptr;
  void (*unmap)(void *context, const SplitBufferInfo &info,
                const SplitBufferMapping &mapping) = nullptr;
  void *context = nullptr;
};

// The mapping of one slot's payload.  The caller provides an array of at
// least the channel's num_slots of these, which must outlive the publisher or
// subscriber.
struct SplitSlot {
  SplitBufferInfo info;
  SplitBufferMapping mapping;
  // The allocator mapped the slot; otherwise the client mapped its
  // descriptor.
  bool allocator_mapped = false;
};

} // namespace asil
} // namespace subspace
