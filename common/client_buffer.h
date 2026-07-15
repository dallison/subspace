// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "toolbelt/fd.h"

#include <cstdint>
#include <string>

namespace subspace {

enum class ClientBufferAllocatorKind {
  kUnspecified = 0,
  kAndroidMemfd = 1,
  kSplitShm = 2,
  kSplitCallback = 3,
  kSplitBufferFreeTest = 4,
};

inline const char *
ClientBufferAllocatorName(ClientBufferAllocatorKind allocator) {
  switch (allocator) {
  case ClientBufferAllocatorKind::kAndroidMemfd:
    return "android_memfd";
  case ClientBufferAllocatorKind::kSplitShm:
    return "split_shm";
  case ClientBufferAllocatorKind::kSplitCallback:
    return "split_callback";
  case ClientBufferAllocatorKind::kSplitBufferFreeTest:
    return "split_buffer_free_test";
  case ClientBufferAllocatorKind::kUnspecified:
  default:
    return "unspecified";
  }
}

// Metadata for memory allocated by a client but cleaned up by the server if the
// client cannot release it itself.  The handle is allocator-defined; plugins
// decide whether they understand it.
struct ClientBufferHandleMetadata {
  std::string channel_name;
  uint64_t session_id = 0;
  uint32_t buffer_index = 0;
  uint32_t slot_id = 0;
  bool is_prefix = false;
  uint64_t full_size = 0;
  uint64_t allocation_size = 0;
  uintptr_t handle = 0;
  std::string shadow_file;
  std::string object_name;
  ClientBufferAllocatorKind allocator = ClientBufferAllocatorKind::kUnspecified;
  int64_t map_offset = 0;
};

struct RegisteredClientBuffer {
  ClientBufferHandleMetadata metadata;
  toolbelt::FileDescriptor fd;
};

} // namespace subspace
