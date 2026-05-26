// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "google/protobuf/any.pb.h"

#include <cstdint>
#include <string>

namespace subspace {

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
  std::string allocator;
  std::string pool_id;
  bool cache_enabled = false;
  uint32_t alignment = 0;
  google::protobuf::Any allocator_metadata;
};

} // namespace subspace
