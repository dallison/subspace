// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/client_channel.h"
#include "common/syscall_shim.h"
#include <sys/mman.h>
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX && defined(__APPLE__)
#include <sys/posix_shm.h>
#endif
#include <chrono>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace subspace {

namespace details {

namespace {

#if SUBSPACE_HAS_QNX_PMEM
uint64_t AlignUp(uint64_t size, uint64_t alignment) {
  if (alignment == 0) {
    alignment = 4096;
  }
  return (size + alignment - 1) & ~(alignment - 1);
}
#endif

} // namespace

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX

absl::StatusOr<std::string>
ClientChannel::CreatePosixSharedMemoryFile(const std::string &filename,
                                           off_t size) {
  // Create a file in /tmp and make it the same size as the shared memory.  This
  // will not actually allocate any disk space.
  auto &shim = GetSyscallShim();
  int fd = shim.open_fn(filename.c_str(), O_RDWR | O_CREAT, 0666);
  if (fd < 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to open shadow file %s: %s", filename.c_str(),
                        strerror(errno)));
  }
  if (shim.ftruncate_fn(fd, size) < 0) {
    shim.close_fn(fd);
    return absl::InternalError(
        absl::StrFormat("Failed to truncate shadow file %s to size %zd: %s",
                        filename.c_str(), size, strerror(errno)));
  }
  shim.close_fn(fd);

  return PosixSharedMemoryName(filename);
}
#endif

absl::Status ClientChannel::Map(SharedMemoryFds fds,
                                const toolbelt::FileDescriptor &scb_fd) {
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s", strerror(errno)));
  }

  ccb_ = reinterpret_cast<ChannelControlBlock *>(MapMemory(
      fds.ccb.Fd(), CcbSize(num_slots_), PROT_READ | PROT_WRITE, "CCB"));
  if (ccb_ == MAP_FAILED) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return absl::InternalError(absl::StrFormat(
        "Failed to map ChannelControlBlock: %s", strerror(errno)));
  }

  bcb_ = reinterpret_cast<BufferControlBlock *>(MapMemory(
      fds.bcb.Fd(), sizeof(BufferControlBlock), PROT_READ | PROT_WRITE, "BCB"));
  if (bcb_ == MAP_FAILED) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, CcbSize(num_slots_), "CCB");
    return absl::InternalError(absl::StrFormat(
        "Failed to map BufferControlBlock: %s", strerror(errno)));
  }
  if (debug_) {
    printf("Channel mapped: scb: %p, ccb: %p\n", scb_, ccb_);
    Dump(std::cout);
  }
  return absl::OkStatus();
}

absl::Status ClientChannel::UnmapUnusedBuffers() {
  for (size_t i = 0; i + 1 < buffers_.size(); i++) {
    if (bcb_->refs[i] == 0) {
      UnmapBufferSet(i, *buffers_[i], /*unregister_pmem=*/true);
    }
  }
  return absl::OkStatus();
}

void ClientChannel::UnmapShmBufferSet(BufferSet &buffer) {
  if (buffer.full_size == 0 || buffer.buffer == nullptr) {
    return;
  }
  UnmapMemory(buffer.buffer, buffer.full_size, "buffers");
  buffer.buffer = nullptr;
  buffer.full_size = 0;
  buffer.slot_size = 0;
}

void ClientChannel::UnmapBufferSet(size_t buffer_index, BufferSet &buffer,
                                   bool unregister_pmem) {
#if SUBSPACE_HAS_QNX_PMEM
  if (buffer.IsQnxPmem()) {
    UnmapQnxPmemBufferSet(buffer_index, buffer, unregister_pmem);
    return;
  }
#else
  (void)buffer_index;
  (void)unregister_pmem;
#endif
  if (debug_ && buffer.full_size > 0 && buffer.buffer != nullptr) {
    fprintf(stderr, "%p: Unmapping buffer at index %zd\n", this, buffer_index);
  }
  UnmapShmBufferSet(buffer);
}

#if SUBSPACE_HAS_QNX_PMEM
void ClientChannel::UnmapQnxPmemBufferSet(size_t buffer_index,
                                          BufferSet &buffer,
                                          bool unregister_pmem) {
  for (size_t slot = 0; slot < buffer.pmem_slot_buffers.size(); slot++) {
    if (buffer.pmem_slot_buffers[slot] != nullptr) {
      UnmapMemory(buffer.pmem_slot_buffers[slot], buffer.pmem_slot_sizes[slot],
                  "pmem slot");
      buffer.pmem_slot_buffers[slot] = nullptr;
    }
    if (buffer.owns_qnx_pmem && slot < buffer.pmem_metadata.size()) {
      (void)DestroyQnxPmemBuffer(buffer.pmem_metadata[slot]);
    }
  }

  if (buffer.prefix_buffer != nullptr) {
    UnmapMemory(buffer.prefix_buffer, buffer.prefix_buffer_size,
                "pmem prefixes");
    buffer.prefix_buffer = nullptr;
  }
  if (buffer.owns_qnx_pmem && !buffer.prefix_metadata.object_name.empty()) {
    (void)DestroyQnxPmemBuffer(buffer.prefix_metadata);
  }

  if (unregister_pmem && buffer.owns_qnx_pmem &&
      pmem_unregistration_callback_) {
    (void)pmem_unregistration_callback_(ResolvedName(), session_id_,
                                        static_cast<uint32_t>(buffer_index));
  }

  buffer.full_size = 0;
  buffer.slot_size = 0;
}
#endif

bool ClientChannel::ValidateSlotBuffer(MessageSlot *slot) {
  if (slot->buffer_index < 0) {
    return true;
  }

  char *buf = Buffer(slot->id, false);
  int retries = 1000;
  while (retries-- > 0 && buf == nullptr) {
    CheckReload();
    buf = Buffer(slot->id, false);
  }
  if (buf == nullptr) {
    return false;
  }
  return true;
}

absl::Status ClientChannel::AttachBuffers() {
  // NOTE: the num_buffers variable in the CCB is atomic and could change while
  // we are in or after we are done with this loop.
  int num_buffers = ccb_->num_buffers;
#if SUBSPACE_HAS_QNX_PMEM
  if (UseQnxPmem()) {
    return AttachQnxPmemBuffers(num_buffers);
  }
#endif
  return AttachShmBuffers(num_buffers);
}

absl::Status ClientChannel::AttachShmBuffers(int num_buffers) {
  BufferMapMode mode = MapMode();
  while (buffers_.size() < size_t(num_buffers)) {
    // We need to open the next buffer in the list.  The buffer index is
    size_t buffer_index = buffers_.size();
    auto shm_fd = OpenBuffer(buffer_index);
    if (!shm_fd.ok()) {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
      if (buffers_.size() + 1 < size_t(num_buffers)) {
        // The buffer might have been deleted because there are no
        // references to it.  If we are not the last buffer, this is
        // fine and we just add an empty buffer.
        buffers_.emplace_back(std::make_unique<BufferSet>(0, 0, nullptr));
        continue;
      }
#endif
      return shm_fd.status();
    }
    auto size = GetBufferSize(*shm_fd, buffer_index);
    if (!size.ok()) {
      return size.status();
    }
    if (*size == 0) {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_LINUX
      if (buffers_.size() + 1 < size_t(num_buffers)) {
        // If the size is 0, it means the buffer has been deleted or not yet
        // created.  We just add an empty buffer.
        buffers_.emplace_back(std::make_unique<BufferSet>(0, 0, nullptr));
      }
#endif

      // It's possible the ftruncate has not been called yet, so we try again.
      continue;
    }
    absl::StatusOr<char *> addr;
    uint64_t slot_size = BufferSizeToSlotSize(*size);
    if (slot_size > 0) {
      // Map the shared memory buffer.
      addr = MapBuffer(*shm_fd, *size, mode);
      if (!addr.ok()) {
        return addr.status();
      }
    } else {
      addr = nullptr;
    }
    buffers_.emplace_back(std::make_unique<BufferSet>(*size, slot_size, *addr));
  }
  return absl::OkStatus();
}

#if SUBSPACE_HAS_QNX_PMEM
absl::Status ClientChannel::AttachQnxPmemBuffers(int num_buffers) {
  while (buffers_.size() < size_t(num_buffers)) {
    size_t buffer_index = buffers_.size();
    uint64_t full_size = bcb_->sizes[buffer_index];
    uint64_t slot_size = BufferSizeToSlotSize(full_size);
    auto pmem_buffer =
        OpenQnxPmemBufferSet(buffer_index, full_size, slot_size);
    if (!pmem_buffer.ok()) {
      return pmem_buffer.status();
    }
    buffers_.push_back(std::move(*pmem_buffer));
  }
  return absl::OkStatus();
}
#endif

void ClientChannel::Unmap() {
  if (scb_ == nullptr) {
    // Not yet mapped.
    return;
  }
  Channel::Unmap();

  for (size_t i = 0; i < buffers_.size(); i++) {
    UnmapBufferSet(i, *buffers_[i], /*unregister_pmem=*/false);
  }
  buffers_.clear();
}

void ClientChannel::Dump(std::ostream &os) const {
  Channel::Dump(os);

  os << "Buffers:\n";
  int index = 0;
  for (auto &buffer : buffers_) {
    os << "  (" << index++ << ") " << buffer->full_size << " "
       << buffer->slot_size << " " << reinterpret_cast<void *>(buffer->buffer)
       << std::endl;
  }
}

// Return a valid file descriptor if the shared memory file can be opened with
// the given flags.  If we can't open it because it already exists, we return an
// invalid file descriptor.  If we can't open it for any other reason, we return
// a InternalError with the error message.
static absl::StatusOr<toolbelt::FileDescriptor>
OpenSharedMemoryFile(const std::string &filename, int flags) {
  mode_t old_umask = umask(0);
  int shm_fd = GetSyscallShim().shm_open_fn(filename.c_str(), flags,
                                             S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH);
  umask(old_umask);
  if (shm_fd == -1) {
    if (errno == EEXIST) {
      // File already exists, return an invalid fd.
      return toolbelt::FileDescriptor();
    }
    return absl::InternalError(absl::StrFormat(
        "Failed to open shared memory %s: %s", filename, strerror(errno)));
  }

  return toolbelt::FileDescriptor(shm_fd);
}

absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::CreateBuffer(int buffer_index, size_t size) {
  std::string filename = BufferSharedMemoryName(buffer_index);

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_LINUX
  return CreateLinuxBuffer(filename, size);
#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_QNX_PMEM
  return CreateQnxPmemContiguousBuffer(buffer_index, filename, size);
#else
  return CreatePosixBuffer(filename, size);
#endif
}

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_LINUX
absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::CreateLinuxBuffer(const std::string &filename, size_t size) {
  auto &shim = GetSyscallShim();
  // Open the shared memory file.
  auto shm_fd = OpenSharedMemoryFile(filename, O_RDWR | O_CREAT | O_EXCL);
  if (!shm_fd.ok()) {
    return shm_fd.status();
  }
  if (!shm_fd->Valid()) {
    // Can only happen if the file already exists.
    return *shm_fd;
  }

  // Make it the appropriate size.
  int e = shim.ftruncate_fn(shm_fd->Fd(), off_t(size));
  if (e == -1) {
    (void)shim.shm_unlink_fn(filename.c_str());
    return absl::InternalError(
        absl::StrFormat("Failed to set length of shared memory %s: %s",
                        filename, strerror(errno)));
  }

  std::string shm_filename = "/dev/shm/" + filename;
  // Change the permissions for the file to 777.
  if (shim.chmod_fn(shm_filename.c_str(), 0777) == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to change permissions of shared memory %s: %s", shm_filename, strerror(errno)));
  }

  if (getuid() == 0) {
    // If we are root, change the owner for the file to server's user and group.
    if (shim.chown_fn(shm_filename.c_str(), user_id_, group_id_) == -1) {
      return absl::InternalError(
          absl::StrFormat("Failed to change owner of shared memory %s: %s", shm_filename, strerror(errno)));
    }
  }

  return *shm_fd;
}
#endif

#if SUBSPACE_HAS_QNX_PMEM
absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::CreateQnxPmemContiguousBuffer(int buffer_index,
                                             const std::string &filename,
                                             size_t size) {
  PmemBufferMetadata metadata;
  metadata.channel_name = ResolvedName();
  metadata.session_id = session_id_;
  metadata.buffer_index = buffer_index;
  metadata.full_size = size;
  metadata.allocation_size = AlignUp(size, 4096);
  metadata.shadow_file = filename;
  metadata.object_name = QnxPmemObjectName(filename);

  auto shm_fd = CreateQnxPmemBuffer(metadata);
  if (!shm_fd.ok()) {
    return shm_fd.status();
  }
  if (!shm_fd->Valid()) {
    return *shm_fd;
  }
  if (absl::Status status = WritePmemMetadataFile(metadata); !status.ok()) {
    (void)DestroyQnxPmemBuffer(metadata);
    return status;
  }
  if (pmem_registration_callback_) {
    if (absl::Status status = pmem_registration_callback_(metadata);
        !status.ok()) {
      (void)DestroyQnxPmemBuffer(metadata);
      return status;
    }
  }
  return *shm_fd;
}
#endif

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::CreatePosixBuffer(const std::string &filename, size_t size) {
  auto &shim = GetSyscallShim();
  // On Posix we need to create a shadow file that has the same size as the
  // shared memory file.  This is because the fstat of the shm "file" returns a
  // page aligned size, which is not what we want.  The shadow file is used
  // to determine the size of the shared memory segment.
  absl::StatusOr<std::string> shm_name =
      CreatePosixSharedMemoryFile(filename, off_t(size));
  if (!shm_name.ok()) {
    return shm_name.status();
  }

  // shm_name is the name of the shared memory.
  auto shm_fd = OpenSharedMemoryFile(*shm_name, O_RDWR | O_CREAT | O_EXCL);
  if (!shm_fd.ok()) {
    return shm_fd.status();
  }
  if (!shm_fd->Valid()) {
    // Can only happen if the file already exists.
    return *shm_fd;
  }

  // Make it the appropriate size.
  int e = shim.ftruncate_fn(shm_fd->Fd(), off_t(size));
  if (e == -1) {
    (void)shim.shm_unlink_fn(filename.c_str());
    return absl::InternalError(
        absl::StrFormat("Failed to set length of shared memory %s: %s",
                        filename, strerror(errno)));
  }

  // Change the permissions for the file to 777.
  if (shim.chmod_fn(filename.c_str(), 0777) == -1) {
    return absl::InternalError(
      absl::StrFormat("Failed to change permissions of shared memory %s: %s",  filename, strerror(errno)));

  }

  if (getuid() == 0) {
    // If we are root, change the owner for the file to server's user and group.
    if (shim.chown_fn(filename.c_str(), user_id_, group_id_) == -1) {
      return absl::InternalError(
        absl::StrFormat("Failed to change owner of shared memory %s: %s", filename, strerror(errno)));
    }
  }
  return *shm_fd;
}
#endif

absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::OpenBuffer(int buffer_index) {
  std::string filename = BufferSharedMemoryName(buffer_index);
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_LINUX
  // Open the shared memory file.
  return OpenSharedMemoryFile(filename, O_RDWR);
#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_QNX_PMEM
  auto metadata = ReadPmemMetadataFile(filename);
  if (!metadata.ok()) {
    return metadata.status();
  }
  return OpenQnxPmemBuffer(*metadata, O_RDWR);
#else
  auto shm_name = PosixSharedMemoryName(filename);
  if (!shm_name.ok()) {
    return shm_name.status();
  }
  return OpenSharedMemoryFile(*shm_name, O_RDWR);
#endif
}

absl::StatusOr<size_t>
ClientChannel::GetBufferSize(toolbelt::FileDescriptor &shm_fd,
                             int buffer_index) const {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  auto &shim = GetSyscallShim();
  // On Posix we need to look at the size of the shadow file because it looks
  // like the fstat of the shm "file" returns a page aligned size.
  std::string filename = BufferSharedMemoryName(buffer_index);
  struct stat sb;
  if (shim.stat_fn(filename.c_str(), &sb) == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to get size of shared memory %s: %s", filename,
                        strerror(errno)));
  }
  return sb.st_size;
#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_QNX_PMEM
  (void)shm_fd;
  std::string filename = BufferSharedMemoryName(buffer_index);
  auto metadata = ReadPmemMetadataFile(filename);
  if (!metadata.ok()) {
    return metadata.status();
  }
  return metadata->full_size;
#else
  auto &shim = GetSyscallShim();
  std::string filename = BufferSharedMemoryName(buffer_index);
  struct stat sb;
  if (shim.fstat_fn(shm_fd.Fd(), &sb) == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to get size of shared memory from fd %d: %s",
                        shm_fd.Fd(), strerror(errno)));
  }
  return sb.st_size;
#endif
}

absl::StatusOr<char *>
ClientChannel::MapBuffer(toolbelt::FileDescriptor &shm_fd, size_t size,
                         BufferMapMode mode) {
  int prot =
      mode == BufferMapMode::kReadOnly ? PROT_READ : (PROT_READ | PROT_WRITE);
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_QNX_PMEM && defined(PROT_NOCACHE)
  prot |= PROT_NOCACHE;
#endif
  void *p = MapMemory(shm_fd.Fd(), size, prot, "buffers");
  if (p == MAP_FAILED) {
    return absl::InternalError(
        absl::StrFormat("Failed to map shared memory from fd %d: %s",
                        shm_fd.Fd(), strerror(errno)));
  }
  return reinterpret_cast<char *>(p);
}

#if SUBSPACE_HAS_QNX_PMEM
absl::StatusOr<std::unique_ptr<BufferSet>>
ClientChannel::CreateQnxPmemBufferSet(size_t buffer_index, size_t full_size,
                                      uint64_t slot_size) {
  auto buffer = std::make_unique<BufferSet>(full_size, slot_size, nullptr);
  buffer->owns_qnx_pmem = true;
  uint64_t alignment = PmemAlignment() == 0 ? 4096 : PmemAlignment();
  uint64_t payload_size = AlignUp(slot_size, alignment);

  std::string base = BufferSharedMemoryName(buffer_index);
  std::string prefix_name = base + "_prefix";
  std::string prefix_object_name = QnxPmemObjectName(prefix_name);
  uint64_t prefix_size =
      AlignUp(static_cast<uint64_t>(PrefixSize()) * NumSlots(), 4096);
  auto prefix_fd =
      OpenSharedMemoryFile(prefix_object_name, O_RDWR | O_CREAT | O_EXCL);
  if (!prefix_fd.ok()) {
    return prefix_fd.status();
  }
  if (GetSyscallShim().ftruncate_fn(prefix_fd->Fd(),
                                    static_cast<off_t>(prefix_size)) == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to size QNX PMEM prefix file %s: %s", prefix_object_name,
        strerror(errno)));
  }
  auto prefix_addr = MapBuffer(*prefix_fd, prefix_size, BufferMapMode::kReadWrite);
  if (!prefix_addr.ok()) {
    return prefix_addr.status();
  }
  buffer->prefix_fd = std::move(*prefix_fd);
  buffer->prefix_buffer = *prefix_addr;
  buffer->prefix_buffer_size = prefix_size;
  buffer->prefix_metadata = {
      .channel_name = ResolvedName(),
      .session_id = session_id_,
      .buffer_index = static_cast<uint32_t>(buffer_index),
      .full_size = prefix_size,
      .allocation_size = prefix_size,
      .shadow_file = prefix_name,
      .object_name = prefix_object_name,
      .slot_id = 0,
      .is_prefix = true,
      .pmem_alignment = static_cast<uint32_t>(alignment),
      .pmem_pool_id = PmemPoolId(),
      .pmem_cache_enabled = PmemCacheEnabled(),
  };
  if (absl::Status status = WritePmemMetadataFile(buffer->prefix_metadata);
      !status.ok()) {
    return status;
  }
  if (pmem_registration_callback_) {
    if (absl::Status status = pmem_registration_callback_(buffer->prefix_metadata);
        !status.ok()) {
      return status;
    }
  }

  buffer->pmem_slot_buffers.resize(NumSlots(), nullptr);
  buffer->pmem_slot_sizes.resize(NumSlots(), payload_size);
  buffer->pmem_handles.resize(NumSlots(), 0);
  buffer->pmem_metadata.reserve(NumSlots());
  buffer->pmem_fds.reserve(NumSlots());
  for (int slot = 0; slot < NumSlots(); slot++) {
    PmemBufferMetadata metadata;
    metadata.channel_name = ResolvedName();
    metadata.session_id = session_id_;
    metadata.buffer_index = static_cast<uint32_t>(buffer_index);
    metadata.full_size = payload_size;
    metadata.allocation_size = payload_size;
    metadata.shadow_file =
        absl::StrFormat("%s_slot_%d", base, slot);
    metadata.object_name = QnxPmemObjectName(metadata.shadow_file);
    metadata.slot_id = static_cast<uint32_t>(slot);
    metadata.is_prefix = false;
    metadata.pmem_alignment = static_cast<uint32_t>(alignment);
    metadata.pmem_pool_id = PmemPoolId();
    metadata.pmem_cache_enabled = PmemCacheEnabled();

    auto pmem_fd = CreateQnxPmemBuffer(metadata);
    if (!pmem_fd.ok()) {
      return pmem_fd.status();
    }
    auto addr = MapBuffer(*pmem_fd, payload_size, MapMode());
    if (!addr.ok()) {
      return addr.status();
    }
    if (absl::Status status = WritePmemMetadataFile(metadata); !status.ok()) {
      return status;
    }
    if (pmem_registration_callback_) {
      if (absl::Status status = pmem_registration_callback_(metadata);
          !status.ok()) {
        return status;
      }
    }
    buffer->pmem_handles[slot] =
        static_cast<uintptr_t>(pmem_fd->Fd());
    buffer->pmem_slot_buffers[slot] = *addr;
    buffer->pmem_fds.push_back(std::move(*pmem_fd));
    buffer->pmem_metadata.push_back(std::move(metadata));
  }
  return buffer;
}
#endif

#if SUBSPACE_HAS_QNX_PMEM
absl::StatusOr<std::unique_ptr<BufferSet>>
ClientChannel::OpenQnxPmemBufferSet(size_t buffer_index, size_t full_size,
                                    uint64_t slot_size) {
  auto buffer = std::make_unique<BufferSet>(full_size, slot_size, nullptr);
  uint64_t alignment = PmemAlignment() == 0 ? 4096 : PmemAlignment();
  uint64_t payload_size = AlignUp(slot_size, alignment);
  std::string base = BufferSharedMemoryName(buffer_index);
  std::string prefix_name = base + "_prefix";
  std::string prefix_object_name = QnxPmemObjectName(prefix_name);
  auto prefix_fd = OpenSharedMemoryFile(prefix_object_name, O_RDWR);
  if (!prefix_fd.ok()) {
    return prefix_fd.status();
  }
  uint64_t prefix_size =
      AlignUp(static_cast<uint64_t>(PrefixSize()) * NumSlots(), 4096);
  auto prefix_addr = MapBuffer(*prefix_fd, prefix_size, BufferMapMode::kReadWrite);
  if (!prefix_addr.ok()) {
    return prefix_addr.status();
  }
  buffer->prefix_fd = std::move(*prefix_fd);
  buffer->prefix_buffer = *prefix_addr;
  buffer->prefix_buffer_size = prefix_size;
  buffer->pmem_slot_buffers.resize(NumSlots(), nullptr);
  buffer->pmem_slot_sizes.resize(NumSlots(), payload_size);
  buffer->pmem_handles.resize(NumSlots(), 0);
  buffer->pmem_metadata.reserve(NumSlots());
  buffer->pmem_fds.reserve(NumSlots());
  for (int slot = 0; slot < NumSlots(); slot++) {
    std::string shadow_file = absl::StrFormat("%s_slot_%d", base, slot);
    auto metadata = ReadPmemMetadataFile(shadow_file);
    if (!metadata.ok()) {
      return metadata.status();
    }
    auto pmem_fd = OpenQnxPmemBuffer(*metadata, O_RDWR);
    if (!pmem_fd.ok()) {
      return pmem_fd.status();
    }
    auto addr = MapBuffer(*pmem_fd, payload_size, MapMode());
    if (!addr.ok()) {
      return addr.status();
    }
    buffer->pmem_handles[slot] =
        static_cast<uintptr_t>(pmem_fd->Fd());
    buffer->pmem_slot_buffers[slot] = *addr;
    buffer->pmem_fds.push_back(std::move(*pmem_fd));
    buffer->pmem_metadata.push_back(std::move(*metadata));
  }
  return buffer;
}
#endif

void ClientChannel::TriggerRetirement(int slot_id) {
  if (!has_retirement_triggers_) {
    // No retirement triggers, let's avoid locking the mutex.
    return;
  }
  MessageSlot *slot = GetSlot(slot_id);
  if ((slot->flags & kMessageIsActivation) != 0) {
    // Don't retire activation messages.
    return;
  }
  std::unique_lock<std::mutex> lock(retirement_lock_);
  for (auto &fd : retirement_triggers_) {
    ssize_t n = GetSyscallShim().write_fn(fd.Fd(), &slot_id, sizeof(slot_id));
    // TODO: what to do if this fails?  For now just write an error to stderr.
    if (n < 0) {
      std::cerr << "Failed to trigger retirement for slot " << slot_id << ": "
                << strerror(errno) << std::endl;
    } else if (n != sizeof(slot_id)) {
      std::cerr << "Failed to trigger retirement for slot " << slot_id
                << ": wrote " << n << " bytes, expected " << sizeof(slot_id)
                << " bytes" << std::endl;
    }
  }
}

} // namespace details
} // namespace subspace
