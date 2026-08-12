// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace subspace {

struct SplitBufferMetadata {
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
  int registration_fd = -1;
  int64_t map_offset = 0;
};

struct SplitBufferMapping {
  uintptr_t handle = 0;
  void *address = nullptr;
  size_t size = 0;
  void *private_data = nullptr;
  int fd = -1;
  int64_t map_offset = 0;
};

struct SplitBufferCallbacks {
  std::function<absl::StatusOr<SplitBufferMapping>(
      const SplitBufferMetadata &metadata)>
      allocate;
  std::function<absl::StatusOr<SplitBufferMapping>(
      const SplitBufferMetadata &metadata)>
      map;
  std::function<absl::Status(const SplitBufferMetadata &metadata,
                             const SplitBufferMapping &mapping)>
      unmap;
  std::function<absl::Status(const SplitBufferMetadata &metadata,
                             const SplitBufferMapping &mapping)>
      free;
};

SplitBufferMetadata SplitBufferMetadataFromProto(
    const ClientBufferHandleMetadataProto &proto);
void SplitBufferMetadataToProto(const SplitBufferMetadata &metadata,
                                ClientBufferHandleMetadataProto *proto);

absl::Status WriteSplitBufferMetadataFile(
    const SplitBufferMetadata &metadata);
absl::StatusOr<SplitBufferMetadata>
ReadSplitBufferMetadataFile(const std::string &shadow_file);

std::string SplitBufferObjectName(const std::string &shadow_file);

// On Android these objects are anonymous memfds: CreateSplitSharedMemoryBuffer
// returns a caller-owned descriptor, and OpenSplitSharedMemoryBuffer is
// unsupported because an anonymous memfd cannot be reopened by name (the client
// shares its descriptors with subscribers via the server instead).  Other
// platforms use named POSIX/Linux shared memory and support reopen by name.
absl::StatusOr<toolbelt::FileDescriptor>
CreateSplitSharedMemoryBuffer(const SplitBufferMetadata &metadata);
absl::StatusOr<toolbelt::FileDescriptor>
OpenSplitSharedMemoryBuffer(const SplitBufferMetadata &metadata, int flags);
absl::Status DestroySplitSharedMemoryBuffer(
    const SplitBufferMetadata &metadata);

} // namespace subspace
