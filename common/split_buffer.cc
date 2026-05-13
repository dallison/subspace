// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/split_buffer.h"

#include "absl/strings/str_format.h"
#include "common/syscall_shim.h"
#include "common/system_info.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace subspace {
namespace {

absl::Status WriteAll(int fd, const std::string &contents,
                      const std::string &path) {
  const char *p = contents.data();
  size_t remaining = contents.size();
  while (remaining > 0) {
    ssize_t n = write(fd, p, remaining);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return absl::InternalError(absl::StrFormat(
          "Failed to write split buffer metadata %s: %s", path,
          strerror(errno)));
    }
    remaining -= static_cast<size_t>(n);
    p += n;
  }
  return absl::OkStatus();
}

uint64_t StableHash64(const std::string &value) {
  uint64_t hash = 1469598103934665603ULL;
  for (unsigned char c : value) {
    hash ^= c;
    hash *= 1099511628211ULL;
  }
  return hash;
}

} // namespace

SplitBufferMetadata SplitBufferMetadataFromProto(
    const ClientBufferHandleMetadataProto &proto) {
  SplitBufferMetadata metadata;
  metadata.channel_name = proto.channel_name();
  metadata.session_id = proto.session_id();
  metadata.buffer_index = proto.buffer_index();
  metadata.slot_id = proto.slot_id();
  metadata.is_prefix = proto.is_prefix();
  metadata.full_size = proto.full_size();
  metadata.allocation_size = proto.allocation_size();
  metadata.handle = static_cast<uintptr_t>(proto.handle());
  metadata.shadow_file = proto.shadow_file();
  metadata.object_name = proto.object_name();
  return metadata;
}

void SplitBufferMetadataToProto(const SplitBufferMetadata &metadata,
                                ClientBufferHandleMetadataProto *proto) {
  proto->set_channel_name(metadata.channel_name);
  proto->set_session_id(metadata.session_id);
  proto->set_buffer_index(metadata.buffer_index);
  proto->set_slot_id(metadata.slot_id);
  proto->set_is_prefix(metadata.is_prefix);
  proto->set_full_size(metadata.full_size);
  proto->set_allocation_size(metadata.allocation_size);
  proto->set_handle(static_cast<uint64_t>(metadata.handle));
  proto->set_shadow_file(metadata.shadow_file);
  proto->set_object_name(metadata.object_name);
}

absl::Status WriteSplitBufferMetadataFile(
    const SplitBufferMetadata &metadata) {
  ClientBufferHandleMetadataProto proto;
  SplitBufferMetadataToProto(metadata, &proto);

  std::string contents;
  if (!proto.SerializeToString(&contents)) {
    return absl::InternalError("Failed to serialize split buffer metadata");
  }

  std::string tmp = metadata.shadow_file + ".tmp";
  int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create split buffer metadata %s: %s", tmp,
        strerror(errno)));
  }

  absl::Status status = WriteAll(fd, contents, tmp);
  if (close(fd) == -1 && status.ok()) {
    status = absl::InternalError(absl::StrFormat(
        "Failed to close split buffer metadata %s: %s", tmp,
        strerror(errno)));
  }
  if (!status.ok()) {
    unlink(tmp.c_str());
    return status;
  }

  if (rename(tmp.c_str(), metadata.shadow_file.c_str()) == -1) {
    unlink(tmp.c_str());
    return absl::InternalError(absl::StrFormat(
        "Failed to publish split buffer metadata %s: %s",
        metadata.shadow_file, strerror(errno)));
  }
  return absl::OkStatus();
}

absl::StatusOr<SplitBufferMetadata>
ReadSplitBufferMetadataFile(const std::string &shadow_file) {
  int fd = open(shadow_file.c_str(), O_RDONLY);
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open split buffer metadata %s: %s", shadow_file,
        strerror(errno)));
  }

  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    close(fd);
    return absl::InternalError(absl::StrFormat(
        "Failed to stat split buffer metadata %s: %s", shadow_file,
        strerror(errno)));
  }

  std::string contents(static_cast<size_t>(sb.st_size), '\0');
  size_t offset = 0;
  while (offset < contents.size()) {
    ssize_t n = read(fd, contents.data() + offset, contents.size() - offset);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      close(fd);
      return absl::InternalError(absl::StrFormat(
          "Failed to read split buffer metadata %s: %s", shadow_file,
          strerror(errno)));
    }
    if (n == 0) {
      break;
    }
    offset += static_cast<size_t>(n);
  }
  close(fd);

  ClientBufferHandleMetadataProto proto;
  if (!proto.ParseFromString(contents)) {
    return absl::InternalError(absl::StrFormat(
        "Failed to parse split buffer metadata %s", shadow_file));
  }
  return SplitBufferMetadataFromProto(proto);
}

std::string SplitBufferObjectName(const std::string &shadow_file) {
  return absl::StrFormat("subspace_sb_%016x", StableHash64(shadow_file));
}

absl::StatusOr<toolbelt::FileDescriptor>
CreateSplitSharedMemoryBuffer(const SplitBufferMetadata &metadata) {
  int fd = GetSyscallShim().shm_open_fn(metadata.object_name.c_str(),
                                        O_RDWR | O_CREAT | O_EXCL, 0666);
  if (fd == -1) {
    if (errno == EEXIST) {
      return toolbelt::FileDescriptor();
    }
    return absl::InternalError(absl::StrFormat(
        "Failed to create split buffer object %s: %s", metadata.object_name,
        strerror(errno)));
  }

  toolbelt::FileDescriptor shm_fd(fd);
  uint64_t allocation_size =
      metadata.allocation_size != 0 ? metadata.allocation_size
                                    : PageAlignedSize(metadata.full_size);
  if (GetSyscallShim().ftruncate_fn(shm_fd.Fd(),
                                    static_cast<off_t>(allocation_size)) == -1) {
    (void)GetSyscallShim().shm_unlink_fn(metadata.object_name.c_str());
    return absl::InternalError(absl::StrFormat(
        "Failed to size split buffer object %s: %s", metadata.object_name,
        strerror(errno)));
  }

  return shm_fd;
}

absl::StatusOr<toolbelt::FileDescriptor>
OpenSplitSharedMemoryBuffer(const SplitBufferMetadata &metadata, int flags) {
  int fd = GetSyscallShim().shm_open_fn(metadata.object_name.c_str(), flags,
                                        0666);
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open split buffer object %s: %s", metadata.object_name,
        strerror(errno)));
  }
  return toolbelt::FileDescriptor(fd);
}

absl::Status DestroySplitSharedMemoryBuffer(
    const SplitBufferMetadata &metadata) {
  absl::Status status = absl::OkStatus();
  if (GetSyscallShim().shm_unlink_fn(metadata.object_name.c_str()) == -1 &&
      errno != ENOENT) {
    status = absl::InternalError(absl::StrFormat(
        "Failed to unlink split buffer object %s: %s", metadata.object_name,
        strerror(errno)));
  }
  if (unlink(metadata.shadow_file.c_str()) == -1 && errno != ENOENT &&
      status.ok()) {
    status = absl::InternalError(absl::StrFormat(
        "Failed to remove split buffer metadata %s: %s", metadata.shadow_file,
        strerror(errno)));
  }
  return status;
}

} // namespace subspace
