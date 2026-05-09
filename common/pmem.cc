// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// PMEM support is provided by General Motors.

#if !((defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)) ||          \
      (defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM)))
#error "common/pmem.cc is only available when PMEM support is compiled in"
#endif

#include "common/pmem.h"

#include "absl/strings/str_format.h"
#include "common/syscall_shim.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)
#include <sys/mman.h>
#endif

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
          "Failed to write QNX pmem metadata %s: %s", path, strerror(errno)));
    }
    remaining -= static_cast<size_t>(n);
    p += n;
  }
  return absl::OkStatus();
}

uint64_t PageAlignedSize(uint64_t size) {
  long page_size = sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    page_size = 4096;
  }
  uint64_t alignment = static_cast<uint64_t>(page_size);
  return (size + alignment - 1) & ~(alignment - 1);
}

#if defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)
absl::Status AllocateQnxPhysicalPmem(int fd, uint64_t allocation_size,
                                     const std::string &object_name) {
  if (shm_ctl(fd, SHMCTL_ANON | SHMCTL_PHYS, 0, allocation_size) == -1) {
    (void)GetSyscallShim().shm_unlink_fn(object_name.c_str());
    return absl::InternalError(absl::StrFormat(
        "Failed to allocate QNX pmem object %s: %s", object_name,
        strerror(errno)));
  }
  return absl::OkStatus();
}
#endif

#if defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM)
absl::Status AllocateLinuxPmemShim(int fd, uint64_t allocation_size,
                                   const std::string &object_name) {
  if (GetSyscallShim().ftruncate_fn(fd, static_cast<off_t>(allocation_size)) ==
      -1) {
    (void)GetSyscallShim().shm_unlink_fn(object_name.c_str());
    return absl::InternalError(absl::StrFormat(
        "Failed to size Linux PMEM shim object %s: %s", object_name,
        strerror(errno)));
  }
  return absl::OkStatus();
}
#endif

} // namespace

PmemBufferMetadata FromProto(const PmemBufferMetadataProto &proto) {
  PmemBufferMetadata metadata;
  metadata.channel_name = proto.channel_name();
  metadata.session_id = proto.session_id();
  metadata.buffer_index = proto.buffer_index();
  metadata.full_size = proto.full_size();
  metadata.allocation_size = proto.allocation_size();
  metadata.pmem_handle = static_cast<uintptr_t>(proto.pmem_handle());
  metadata.shadow_file = proto.shadow_file();
  metadata.object_name = proto.object_name();
#if (defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)) ||            \
    (defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM))
  metadata.slot_id = proto.slot_id();
  metadata.is_prefix = proto.is_prefix();
  metadata.pmem_alignment = proto.pmem_alignment();
  metadata.pmem_pool_id = proto.pmem_pool_id();
  metadata.pmem_cache_enabled = proto.pmem_cache_enabled();
#endif
  return metadata;
}

void ToProto(const PmemBufferMetadata &metadata,
             PmemBufferMetadataProto *proto) {
  proto->set_channel_name(metadata.channel_name);
  proto->set_session_id(metadata.session_id);
  proto->set_buffer_index(metadata.buffer_index);
  proto->set_full_size(metadata.full_size);
  proto->set_allocation_size(metadata.allocation_size);
  proto->set_pmem_handle(static_cast<uint64_t>(metadata.pmem_handle));
  proto->set_shadow_file(metadata.shadow_file);
  proto->set_object_name(metadata.object_name);
#if (defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)) ||            \
    (defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM))
  proto->set_slot_id(metadata.slot_id);
  proto->set_is_prefix(metadata.is_prefix);
  proto->set_pmem_alignment(metadata.pmem_alignment);
  proto->set_pmem_pool_id(metadata.pmem_pool_id);
  proto->set_pmem_cache_enabled(metadata.pmem_cache_enabled);
#endif
}

absl::Status WritePmemMetadataFile(const PmemBufferMetadata &metadata) {
  PmemBufferMetadataProto proto;
  ToProto(metadata, &proto);

  std::string contents;
  if (!proto.SerializeToString(&contents)) {
    return absl::InternalError("Failed to serialize QNX pmem metadata");
  }

  std::string tmp = metadata.shadow_file + ".tmp";
  int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create QNX pmem metadata %s: %s", tmp, strerror(errno)));
  }

  absl::Status status = WriteAll(fd, contents, tmp);
  if (close(fd) == -1 && status.ok()) {
    status = absl::InternalError(absl::StrFormat(
        "Failed to close QNX pmem metadata %s: %s", tmp, strerror(errno)));
  }
  if (!status.ok()) {
    unlink(tmp.c_str());
    return status;
  }

  if (rename(tmp.c_str(), metadata.shadow_file.c_str()) == -1) {
    unlink(tmp.c_str());
    return absl::InternalError(absl::StrFormat(
        "Failed to publish QNX pmem metadata %s: %s", metadata.shadow_file,
        strerror(errno)));
  }
  return absl::OkStatus();
}

absl::StatusOr<PmemBufferMetadata>
ReadPmemMetadataFile(const std::string &shadow_file) {
  int fd = open(shadow_file.c_str(), O_RDONLY);
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open QNX pmem metadata %s: %s", shadow_file,
        strerror(errno)));
  }

  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    close(fd);
    return absl::InternalError(absl::StrFormat(
        "Failed to stat QNX pmem metadata %s: %s", shadow_file,
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
          "Failed to read QNX pmem metadata %s: %s", shadow_file,
          strerror(errno)));
    }
    if (n == 0) {
      break;
    }
    offset += static_cast<size_t>(n);
  }
  close(fd);

  PmemBufferMetadataProto proto;
  if (!proto.ParseFromString(contents)) {
    return absl::InternalError(
        absl::StrFormat("Failed to parse QNX pmem metadata %s", shadow_file));
  }
  return FromProto(proto);
}

std::string QnxPmemObjectName(const std::string &shadow_file) {
  std::string name = "subspace_qnx_pmem";
  for (char c : shadow_file) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9')) {
      name.push_back(c);
    } else {
      name.push_back('_');
    }
  }
  return name;
}

absl::StatusOr<toolbelt::FileDescriptor>
CreateQnxPmemBuffer(const PmemBufferMetadata &metadata) {
  int fd = GetSyscallShim().shm_open_fn(metadata.object_name.c_str(),
                                        O_RDWR | O_CREAT | O_EXCL, 0666);
  if (fd == -1) {
    if (errno == EEXIST) {
      return toolbelt::FileDescriptor();
    }
    return absl::InternalError(absl::StrFormat(
        "Failed to create QNX pmem object %s: %s", metadata.object_name,
        strerror(errno)));
  }

  toolbelt::FileDescriptor pmem_fd(fd);
  uint64_t allocation_size =
      metadata.allocation_size != 0 ? metadata.allocation_size
                                    : PageAlignedSize(metadata.full_size);

#if defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)
  if (absl::Status status = AllocateQnxPhysicalPmem(
          pmem_fd.Fd(), allocation_size, metadata.object_name);
      !status.ok()) {
    return status;
  }
#elif defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM)
  if (absl::Status status = AllocateLinuxPmemShim(
          pmem_fd.Fd(), allocation_size, metadata.object_name);
      !status.ok()) {
    return status;
  }
#else
  if (GetSyscallShim().ftruncate_fn(pmem_fd.Fd(),
                                    static_cast<off_t>(allocation_size)) == -1) {
    (void)GetSyscallShim().shm_unlink_fn(metadata.object_name.c_str());
    return absl::InternalError(absl::StrFormat(
        "Failed to size QNX pmem object %s: %s", metadata.object_name,
        strerror(errno)));
  }
#endif

  return pmem_fd;
}

absl::StatusOr<toolbelt::FileDescriptor>
OpenQnxPmemBuffer(const PmemBufferMetadata &metadata, int flags) {
  int fd = GetSyscallShim().shm_open_fn(metadata.object_name.c_str(), flags,
                                        0666);
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open QNX pmem object %s: %s", metadata.object_name,
        strerror(errno)));
  }
  return toolbelt::FileDescriptor(fd);
}

absl::Status DestroyQnxPmemBuffer(const PmemBufferMetadata &metadata) {
  absl::Status status = absl::OkStatus();
  if (GetSyscallShim().shm_unlink_fn(metadata.object_name.c_str()) == -1 &&
      errno != ENOENT) {
    status = absl::InternalError(absl::StrFormat(
        "Failed to unlink QNX pmem object %s: %s", metadata.object_name,
        strerror(errno)));
  }
  if (unlink(metadata.shadow_file.c_str()) == -1 && errno != ENOENT &&
      status.ok()) {
    status = absl::InternalError(absl::StrFormat(
        "Failed to remove QNX pmem metadata %s: %s", metadata.shadow_file,
        strerror(errno)));
  }
  return status;
}

} // namespace subspace
