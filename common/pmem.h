// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// PMEM support is provided by General Motors.

#ifndef _xCOMMON_PMEM_H
#define _xCOMMON_PMEM_H

#if !((defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)) ||          \
      (defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM)))
#error "common/pmem.h is only available when PMEM support is compiled in"
#endif

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace subspace {

struct PmemBufferMetadata {
  std::string channel_name;
  uint64_t session_id = 0;
  uint32_t buffer_index = 0;
  uint64_t full_size = 0;
  uint64_t allocation_size = 0;
  uintptr_t pmem_handle = 0;
  std::string shadow_file;
  std::string object_name;
#if (defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)) ||            \
    (defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM))
  uint32_t slot_id = 0;
  bool is_prefix = false;
  uint32_t pmem_alignment = 0;
  std::string pmem_pool_id;
  bool pmem_cache_enabled = false;
#endif
};

struct PmemBufferMapping {
  uintptr_t handle = 0;
  void *address = nullptr;
  size_t size = 0;
  void *private_data = nullptr;
};

struct PmemBufferCallbacks {
  std::function<absl::StatusOr<PmemBufferMapping>(
      const PmemBufferMetadata &metadata)>
      allocate;
  std::function<absl::StatusOr<PmemBufferMapping>(
      const PmemBufferMetadata &metadata)>
      map;
  std::function<absl::Status(const PmemBufferMetadata &metadata,
                             const PmemBufferMapping &mapping)>
      unmap;
  std::function<absl::Status(const PmemBufferMetadata &metadata,
                             const PmemBufferMapping &mapping)>
      free;
};

PmemBufferMetadata FromProto(const PmemBufferMetadataProto &proto);
void ToProto(const PmemBufferMetadata &metadata,
             PmemBufferMetadataProto *proto);

absl::Status WritePmemMetadataFile(const PmemBufferMetadata &metadata);
absl::StatusOr<PmemBufferMetadata>
ReadPmemMetadataFile(const std::string &shadow_file);

std::string QnxPmemObjectName(const std::string &shadow_file);

absl::StatusOr<toolbelt::FileDescriptor>
CreateQnxPmemBuffer(const PmemBufferMetadata &metadata);
absl::StatusOr<toolbelt::FileDescriptor>
OpenQnxPmemBuffer(const PmemBufferMetadata &metadata, int flags);
absl::Status DestroyQnxPmemBuffer(const PmemBufferMetadata &metadata);

} // namespace subspace

#endif // _xCOMMON_PMEM_H
