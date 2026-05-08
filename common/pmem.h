// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xCOMMON_PMEM_H
#define _xCOMMON_PMEM_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace subspace {

struct PmemBufferMetadata {
  std::string channel_name;
  uint64_t session_id = 0;
  uint32_t buffer_index = 0;
  uint64_t full_size = 0;
  uint64_t allocation_size = 0;
  std::string shadow_file;
  std::string object_name;
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
