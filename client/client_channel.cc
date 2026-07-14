// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/client_channel.h"
#include "common/syscall_shim.h"
#include "common/system_info.h"
#include <sys/mman.h>
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX && defined(__APPLE__)
#include <sys/posix_shm.h>
#endif
#include <chrono>
#include <optional>
#include <sys/stat.h>
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
#include <sys/syscall.h>
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
#endif
#include <thread>
#include <utility>
#include <unistd.h>

namespace subspace {

namespace details {

namespace {

absl::StatusOr<SplitBufferMetadata>
ReadSplitBufferMetadataFileWithRetry(const std::string &shadow_file) {
  absl::Status status;
  for (int attempt = 0; attempt < 100; attempt++) {
    auto metadata = ReadSplitBufferMetadataFile(shadow_file);
    if (metadata.ok()) {
      return metadata;
    }
    status = metadata.status();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return status;
}

ClientBufferHandleMetadata ClientBufferFromSplitMetadata(
    const SplitBufferMetadata &metadata, ClientBufferAllocatorKind allocator) {
  return {.channel_name = metadata.channel_name,
          .session_id = metadata.session_id,
          .buffer_index = metadata.buffer_index,
          .slot_id = metadata.slot_id,
          .is_prefix = metadata.is_prefix,
          .full_size = metadata.full_size,
          .allocation_size = metadata.allocation_size,
          .handle = metadata.handle,
          .shadow_file = metadata.shadow_file,
          .object_name = metadata.object_name,
          .allocator = allocator};
}

[[maybe_unused]] SplitBufferMetadata SplitMetadataFromClientBuffer(
    const ClientBufferHandleMetadata &metadata) {
  return {.channel_name = metadata.channel_name,
          .session_id = metadata.session_id,
          .buffer_index = metadata.buffer_index,
          .slot_id = metadata.slot_id,
          .is_prefix = metadata.is_prefix,
          .full_size = metadata.full_size,
          .allocation_size = metadata.allocation_size,
          .handle = metadata.handle,
          .shadow_file = metadata.shadow_file,
          .object_name = metadata.object_name};
}

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
  absl::StatusOr<size_t> checked_ccb_size =
      CheckedCcbSize(num_slots_, subscriber_queue_arena_size_);
  if (!checked_ccb_size.ok()) {
    return checked_ccb_size.status();
  }
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s (scb_fd=%d, ccb_fd=%d, "
        "bcb_fd=%d, scb_size=%zu, ccb_size=%zu, bcb_size=%zu)",
        strerror(errno), scb_fd.Fd(), fds.ccb.Fd(), fds.bcb.Fd(),
        sizeof(SystemControlBlock), *checked_ccb_size,
        sizeof(BufferControlBlock)));
  }

  ccb_ = reinterpret_cast<ChannelControlBlock *>(
      MapMemory(fds.ccb.Fd(), *checked_ccb_size, PROT_READ | PROT_WRITE, "CCB"));
  if (ccb_ == MAP_FAILED) {
    int mmap_errno = errno;
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return absl::InternalError(absl::StrFormat(
        "Failed to map ChannelControlBlock: %s (scb_fd=%d, ccb_fd=%d, "
        "bcb_fd=%d, ccb_size=%zu)",
        strerror(mmap_errno), scb_fd.Fd(), fds.ccb.Fd(), fds.bcb.Fd(),
        *checked_ccb_size));
  }
  if (ccb_->version != kChannelControlBlockVersion) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, *checked_ccb_size, "CCB");
    return absl::FailedPreconditionError(absl::StrFormat(
        "unsupported channel control block version %u (expected %u)",
        ccb_->version, kChannelControlBlockVersion));
  }

  bcb_ = reinterpret_cast<BufferControlBlock *>(MapMemory(
      fds.bcb.Fd(), sizeof(BufferControlBlock), PROT_READ | PROT_WRITE, "BCB"));
  if (bcb_ == MAP_FAILED) {
    int mmap_errno = errno;
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, *checked_ccb_size, "CCB");
    return absl::InternalError(absl::StrFormat(
        "Failed to map BufferControlBlock: %s (scb_fd=%d, ccb_fd=%d, "
        "bcb_fd=%d, bcb_size=%zu)",
        strerror(mmap_errno), scb_fd.Fd(), fds.ccb.Fd(), fds.bcb.Fd(),
        sizeof(BufferControlBlock)));
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
      UnmapBufferSet(i, *buffers_[i], /*destroy_owned_buffers=*/false);
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
                                   bool destroy_owned_buffers) {
  if (buffer.IsSplitBuffers()) {
    UnmapSplitBufferSet(buffer_index, buffer, destroy_owned_buffers);
    return;
  }
  (void)buffer_index;
  (void)destroy_owned_buffers;
  if (debug_ && buffer.full_size > 0 && buffer.buffer != nullptr) {
    fprintf(stderr, "%p: Unmapping buffer at index %zd\n", this, buffer_index);
  }
  UnmapShmBufferSet(buffer);
}

void ClientChannel::UnmapSplitBufferSet(size_t buffer_index,
                                        BufferSet &buffer,
                                        bool destroy_owned_buffers) {
  (void)buffer_index;
  for (size_t slot = 0; slot < buffer.split_slot_buffers.size(); slot++) {
    if (buffer.split_slot_buffers[slot] != nullptr) {
      SplitBufferMetadata metadata;
      if (slot < buffer.split_metadata.size()) {
        metadata = buffer.split_metadata[slot];
      }
      SplitBufferMapping mapping = {
          .handle = slot < buffer.split_handles.size() ? buffer.split_handles[slot]
                                                       : metadata.handle,
          .address = buffer.split_slot_buffers[slot],
          .size = slot < buffer.split_slot_sizes.size()
                      ? buffer.split_slot_sizes[slot]
                      : metadata.allocation_size,
          .private_data = slot < buffer.split_private_data.size()
                              ? buffer.split_private_data[slot]
                              : nullptr,
      };
      if (destroy_owned_buffers && buffer.uses_split_callbacks &&
          buffer.owns_split_buffers &&
          SplitBuffersCallbacks().free) {
        (void)SplitBuffersCallbacks().free(metadata, mapping);
      } else if (buffer.uses_split_callbacks && SplitBuffersCallbacks().unmap) {
        (void)SplitBuffersCallbacks().unmap(metadata, mapping);
      } else {
        UnmapMemory(buffer.split_slot_buffers[slot],
                    buffer.split_slot_sizes[slot], "split slot");
      }
      buffer.split_slot_buffers[slot] = nullptr;
    }
  }

  if (buffer.split_prefix_buffer != nullptr) {
    UnmapMemory(buffer.split_prefix_buffer, buffer.split_prefix_buffer_size,
                "split prefixes");
    buffer.split_prefix_buffer = nullptr;
  }
  if (destroy_owned_buffers && buffer.uses_split_callbacks &&
      buffer.owns_split_buffers &&
      client_buffer_unregistration_callback_) {
    (void)client_buffer_unregistration_callback_(
        ResolvedName(), session_id_, static_cast<uint32_t>(buffer_index));
  }

  buffer.full_size = 0;
  buffer.slot_size = 0;
}

bool ClientChannel::ValidateSlotBuffer(MessageSlot *slot) {
  const int buffer_index =
      slot->buffer_index.load(std::memory_order_relaxed);
  if (buffer_index < 0) {
    return true;
  }

  if (static_cast<size_t>(buffer_index) < buffers_.size() &&
      buffers_[buffer_index]->IsSplitBuffers()) {
    return slot->id >= 0 &&
           static_cast<size_t>(slot->id) <
               buffers_[buffer_index]->split_slot_buffers.size() &&
           buffers_[buffer_index]->split_slot_buffers[slot->id] !=
               nullptr;
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
  if (UseSplitBuffers()) {
    return AttachSplitBuffers(num_buffers);
  }
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
    auto buffer_set = std::make_unique<BufferSet>(*size, slot_size, *addr);
    buffer_set->fd = std::move(*shm_fd);
    buffers_.push_back(std::move(buffer_set));
  }
  return absl::OkStatus();
}

absl::Status ClientChannel::AttachSplitBuffers(int num_buffers) {
  while (buffers_.size() < size_t(num_buffers)) {
    size_t buffer_index = buffers_.size();
    uint64_t full_size = bcb_->sizes[buffer_index];
    uint64_t slot_size = BufferSizeToSlotSize(full_size);
    auto split_buffer = OpenSplitBufferSet(buffer_index, full_size, slot_size);
    if (!split_buffer.ok()) {
      return split_buffer.status();
    }
    buffers_.push_back(std::move(*split_buffer));
  }
  return absl::OkStatus();
}

void ClientChannel::Unmap() {
  if (scb_ == nullptr) {
    // Not yet mapped.
    return;
  }
  Channel::Unmap();

  for (size_t i = 0; i < buffers_.size(); i++) {
    UnmapBufferSet(i, *buffers_[i], /*destroy_owned_buffers=*/true);
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

uint64_t ClientChannel::GetVirtualMemoryUsage() const {
  if (!UseSplitBuffers() || ccb_ == nullptr || bcb_ == nullptr) {
    return Channel::GetVirtualMemoryUsage();
  }

  uint64_t size =
      sizeof(SystemControlBlock) +
      CcbSize(num_slots_, subscriber_queue_arena_size_) +
      sizeof(BufferControlBlock);
  for (int i = 0; i < ccb_->num_buffers; i++) {
    if (bcb_->refs[i].load(std::memory_order_relaxed) <= 0) {
      continue;
    }
    if (static_cast<size_t>(i) >= buffers_.size() ||
        !buffers_[i]->IsSplitBuffers()) {
      size += bcb_->sizes[i].load(std::memory_order_relaxed);
      continue;
    }
    const BufferSet &buffer = *buffers_[i];
    size += buffer.split_prefix_buffer_size;
    for (uint64_t slot_size : buffer.split_slot_sizes) {
      size += slot_size;
    }
  }
  return size;
}

// Return a valid file descriptor if the shared memory file can be opened with
// the given flags.  If we can't open it because it already exists, we return an
// invalid file descriptor.  If we can't open it for any other reason, we return
// a InternalError with the error message.
static absl::StatusOr<toolbelt::FileDescriptor>
OpenSharedMemoryFile(const std::string &filename, int flags) {
  mode_t old_umask = umask(0);
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  int shm_fd = GetSyscallShim().open_fn(filename.c_str(), flags,
                                        S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH);
#else
  int shm_fd = GetSyscallShim().shm_open_fn(filename.c_str(), flags,
                                             S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH);
#endif
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

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  return CreateMemfdBuffer(filename, size);
#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_LINUX
  return CreateLinuxBuffer(filename, size);
#else
  return CreatePosixBuffer(filename, size);
#endif
}

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
static absl::StatusOr<toolbelt::FileDescriptor>
CreateAnonymousSharedMemory(const std::string &name, size_t size) {
#ifdef __NR_memfd_create
  int fd = static_cast<int>(syscall(
      __NR_memfd_create, name.c_str(), static_cast<unsigned int>(MFD_CLOEXEC)));
  if (fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create anonymous shared memory %s: %s", name,
        strerror(errno)));
  }
  toolbelt::FileDescriptor shm_fd(fd);
  if (GetSyscallShim().ftruncate_fn(shm_fd.Fd(), off_t(size)) == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to size anonymous shared memory %s: %s", name,
        strerror(errno)));
  }
  return shm_fd;
#else
  return absl::UnimplementedError("memfd_create is not available");
#endif
}

absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::CreateMemfdBuffer(const std::string &filename, size_t size) {
  return CreateAnonymousSharedMemory(filename, size);
}

absl::StatusOr<RegisteredClientBuffer>
ClientChannel::GetRegisteredClientBuffer(uint32_t buffer_index, bool is_prefix,
                                         uint32_t slot_id) {
  if (!client_buffer_lookup_callback_) {
    return absl::InternalError("No client buffer lookup callback registered");
  }
  absl::Status status;
  // The CCB/BCB can make a buffer generation visible before the server has
  // returned the registered memfd for that prefix/slot.  Poll briefly
  // for the registration to catch up before treating it as missing.
  for (int attempt = 0; attempt < 100; attempt++) {
    absl::StatusOr<std::vector<RegisteredClientBuffer>> buffers =
        client_buffer_lookup_callback_(ResolvedName(), session_id_,
                                       buffer_index);
    if (!buffers.ok()) {
      return buffers.status();
    }
    for (RegisteredClientBuffer &buffer : *buffers) {
      if (buffer.metadata.is_prefix == is_prefix &&
          buffer.metadata.slot_id == slot_id) {
        if (!buffer.fd.Valid() &&
            buffer.metadata.allocator !=
                ClientBufferAllocatorKind::kSplitCallback) {
          return absl::InternalError(absl::StrFormat(
              "Registered client buffer %s/%u/%u missing FD", ResolvedName(),
              buffer_index, slot_id));
        }
        return std::move(buffer);
      }
    }
    status = absl::NotFoundError(absl::StrFormat(
        "No registered client buffer for %s buffer %u slot %u prefix %d",
        ResolvedName(), buffer_index, slot_id, is_prefix));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return status;
}
#endif

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

absl::StatusOr<std::unique_ptr<BufferSet>>
ClientChannel::CreateSplitBufferSet(size_t buffer_index, size_t full_size,
                                    uint64_t slot_size) {
  auto buffer = std::make_unique<BufferSet>(full_size, slot_size, nullptr);
  buffer->owns_split_buffers = true;
  uint64_t payload_size = PageAlignedSize(slot_size);

  std::string base = BufferSharedMemoryName(buffer_index);
  std::string metadata_base = base.empty() || base[0] == '/' ? base
                                                            : "/tmp/" + base;
  std::string prefix_name = metadata_base + "_prefix";
  SplitBufferMetadata prefix_metadata = {
      .channel_name = ResolvedName(),
      .session_id = session_id_,
      .buffer_index = static_cast<uint32_t>(buffer_index),
      .slot_id = 0,
      .is_prefix = true,
      .full_size =
          PageAlignedSize(static_cast<uint64_t>(PrefixSize()) * NumSlots()),
      .allocation_size =
          PageAlignedSize(static_cast<uint64_t>(PrefixSize()) * NumSlots()),
      .shadow_file = prefix_name,
      .object_name = SplitBufferObjectName(prefix_name),
  };
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  // Anonymous memfds have no name, so publishers cannot dedup by creating with
  // O_EXCL the way the named backends do.  Every publisher instead creates and
  // registers its own memfd; the server keeps the first registration
  // (first-wins) and we then adopt that authoritative descriptor.  This makes
  // all publishers and subscribers on the channel share one set of split
  // buffers per (channel, buffer_index).
  {
    auto created = CreateAnonymousSharedMemory(prefix_metadata.object_name,
                                               prefix_metadata.allocation_size);
    if (!created.ok()) {
      return created.status();
    }
    prefix_metadata.handle = static_cast<uintptr_t>(created->Fd());
    if (client_buffer_registration_callback_) {
      if (absl::Status status = client_buffer_registration_callback_(
              ClientBufferFromSplitMetadata(
                  prefix_metadata, ClientBufferAllocatorKind::kSplitShm),
              &*created);
          !status.ok()) {
        return status;
      }
    }
    // `created` is closed here; if we won the race the server holds a dup, and
    // if we lost it the unused memfd is released.
  }
  auto prefix_registered =
      GetRegisteredClientBuffer(static_cast<uint32_t>(buffer_index),
                                /*is_prefix=*/true, /*slot_id=*/0);
  if (!prefix_registered.ok()) {
    return prefix_registered.status();
  }
  prefix_metadata.allocation_size = prefix_registered->metadata.allocation_size;
  prefix_metadata.handle = static_cast<uintptr_t>(prefix_registered->fd.Fd());
  auto prefix_addr =
      MapBuffer(prefix_registered->fd, prefix_metadata.allocation_size,
                BufferMapMode::kReadWrite);
  if (!prefix_addr.ok()) {
    return prefix_addr.status();
  }
  buffer->split_prefix_fd = std::move(prefix_registered->fd);
  buffer->split_prefix_buffer = *prefix_addr;
  buffer->split_prefix_buffer_size = prefix_metadata.allocation_size;
  buffer->split_prefix_metadata = std::move(prefix_metadata);
#else
  auto prefix_fd = CreateSplitSharedMemoryBuffer(prefix_metadata);
  if (!prefix_fd.ok()) {
    return prefix_fd.status();
  }
  if (!prefix_fd->Valid()) {
    return OpenSplitBufferSet(buffer_index, full_size, slot_size);
  }
  auto prefix_addr =
      MapBuffer(*prefix_fd, prefix_metadata.allocation_size,
                BufferMapMode::kReadWrite);
  if (!prefix_addr.ok()) {
    return prefix_addr.status();
  }
  prefix_metadata.handle = static_cast<uintptr_t>(prefix_fd->Fd());
  if (absl::Status status = WriteSplitBufferMetadataFile(prefix_metadata);
      !status.ok()) {
    return status;
  }
  if (client_buffer_registration_callback_) {
    if (absl::Status status = client_buffer_registration_callback_(
            ClientBufferFromSplitMetadata(
                prefix_metadata, ClientBufferAllocatorKind::kSplitShm),
            &*prefix_fd);
        !status.ok()) {
      (void)DestroySplitSharedMemoryBuffer(prefix_metadata);
      return status;
    }
  }
  buffer->split_prefix_fd = std::move(*prefix_fd);
  buffer->split_prefix_buffer = *prefix_addr;
  buffer->split_prefix_buffer_size = prefix_metadata.allocation_size;
  buffer->split_prefix_metadata = std::move(prefix_metadata);
#endif

  buffer->split_slot_buffers.resize(NumSlots(), nullptr);
  buffer->split_slot_sizes.resize(NumSlots(), payload_size);
  buffer->split_handles.resize(NumSlots(), 0);
  buffer->split_private_data.resize(NumSlots(), nullptr);
  buffer->split_metadata.reserve(NumSlots());
  buffer->split_fds.reserve(NumSlots());
  buffer->uses_split_callbacks =
      static_cast<bool>(SplitBuffersCallbacks().allocate);
  for (int slot = 0; slot < NumSlots(); slot++) {
    SplitBufferMetadata metadata;
    metadata.channel_name = ResolvedName();
    metadata.session_id = session_id_;
    metadata.buffer_index = static_cast<uint32_t>(buffer_index);
    metadata.slot_id = static_cast<uint32_t>(slot);
    metadata.is_prefix = false;
    metadata.full_size = payload_size;
    metadata.allocation_size = payload_size;
    metadata.shadow_file = absl::StrFormat("%s_slot_%d", metadata_base, slot);
    metadata.object_name = SplitBufferObjectName(metadata.shadow_file);

    char *slot_addr = nullptr;
    uintptr_t slot_handle = 0;
    uint64_t slot_mapped_size = payload_size;
    void *slot_private_data = nullptr;
    std::optional<toolbelt::FileDescriptor> slot_fd;
    bool registered_via_memfd = false;
    if (SplitBuffersCallbacks().allocate) {
      auto mapping = SplitBuffersCallbacks().allocate(metadata);
      if (!mapping.ok()) {
        return mapping.status();
      }
      if (mapping->address == nullptr) {
        return absl::InternalError(
            "Split buffer allocation callback returned an empty mapping");
      }
      slot_addr = static_cast<char *>(mapping->address);
      slot_handle = mapping->handle;
      slot_mapped_size = mapping->size == 0
                             ? payload_size
                             : static_cast<uint64_t>(mapping->size);
      slot_private_data = mapping->private_data;
      metadata.handle = slot_handle;
      metadata.allocation_size = slot_mapped_size;
    } else {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
      // Create and register our own memfd, then adopt the server's first-wins
      // authoritative descriptor so every publisher shares one buffer per slot.
      {
        auto created_fd = CreateAnonymousSharedMemory(metadata.object_name,
                                                      metadata.allocation_size);
        if (!created_fd.ok()) {
          return created_fd.status();
        }
        metadata.handle = static_cast<uintptr_t>(created_fd->Fd());
        if (client_buffer_registration_callback_) {
          if (absl::Status status = client_buffer_registration_callback_(
                  ClientBufferFromSplitMetadata(
                      metadata, ClientBufferAllocatorKind::kSplitShm),
                  &*created_fd);
              !status.ok()) {
            return status;
          }
        }
        // `created_fd` is closed here; if we won the race the server holds a
        // dup, otherwise the unused memfd is released.
      }
      auto registered =
          GetRegisteredClientBuffer(static_cast<uint32_t>(buffer_index),
                                    /*is_prefix=*/false,
                                    static_cast<uint32_t>(slot));
      if (!registered.ok()) {
        return registered.status();
      }
      metadata.allocation_size = registered->metadata.allocation_size;
      auto addr = MapBuffer(registered->fd, payload_size, MapMode());
      if (!addr.ok()) {
        return addr.status();
      }
      slot_addr = *addr;
      slot_handle = static_cast<uintptr_t>(registered->fd.Fd());
      slot_fd.emplace(std::move(registered->fd));
      metadata.handle = slot_handle;
      registered_via_memfd = true;
#else
      auto created_fd = CreateSplitSharedMemoryBuffer(metadata);
      if (!created_fd.ok()) {
        return created_fd.status();
      }
      if (!created_fd->Valid()) {
        return absl::InternalError(absl::StrFormat(
            "Split buffer slot already exists for channel %s slot %d",
            ResolvedName(), slot));
      }
      auto addr = MapBuffer(*created_fd, payload_size, MapMode());
      if (!addr.ok()) {
        return addr.status();
      }
      slot_addr = *addr;
      slot_handle = static_cast<uintptr_t>(created_fd->Fd());
      slot_fd.emplace(std::move(*created_fd));
      metadata.handle = slot_handle;
#endif
    }
#if SUBSPACE_SHMEM_MODE != SUBSPACE_SHMEM_MODE_MEMFD
    if (absl::Status status = WriteSplitBufferMetadataFile(metadata);
        !status.ok()) {
      return status;
    }
#endif
    if (client_buffer_registration_callback_ && !registered_via_memfd) {
      if (absl::Status status = client_buffer_registration_callback_(
              ClientBufferFromSplitMetadata(
                  metadata,
                  buffer->uses_split_callbacks
                      ? ClientBufferAllocatorKind::kSplitCallback
                      : ClientBufferAllocatorKind::kSplitShm),
              slot_fd.has_value() ? &*slot_fd : nullptr);
          !status.ok()) {
        return status;
      }
    }
    buffer->split_handles[slot] = slot_handle;
    buffer->split_slot_buffers[slot] = slot_addr;
    buffer->split_slot_sizes[slot] = slot_mapped_size;
    buffer->split_private_data[slot] = slot_private_data;
    if (slot_fd.has_value()) {
      buffer->split_fds.push_back(std::move(*slot_fd));
    }
    buffer->split_metadata.push_back(std::move(metadata));
  }
  return buffer;
}

absl::StatusOr<std::unique_ptr<BufferSet>>
ClientChannel::OpenSplitBufferSet(size_t buffer_index, size_t full_size,
                                  uint64_t slot_size) {
  auto buffer = std::make_unique<BufferSet>(full_size, slot_size, nullptr);
  uint64_t payload_size = PageAlignedSize(slot_size);
  std::string base = BufferSharedMemoryName(buffer_index);
  std::string metadata_base = base.empty() || base[0] == '/' ? base
                                                            : "/tmp/" + base;
  std::string prefix_name = metadata_base + "_prefix";
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  absl::StatusOr<RegisteredClientBuffer> prefix_registered =
      GetRegisteredClientBuffer(static_cast<uint32_t>(buffer_index),
                                /*is_prefix=*/true, /*slot_id=*/0);
  if (!prefix_registered.ok()) {
    return prefix_registered.status();
  }
  auto prefix_addr =
      MapBuffer(prefix_registered->fd, prefix_registered->metadata.allocation_size,
                BufferMapMode::kReadWrite);
  if (!prefix_addr.ok()) {
    return prefix_addr.status();
  }
  buffer->split_prefix_fd = std::move(prefix_registered->fd);
  buffer->split_prefix_buffer = *prefix_addr;
  buffer->split_prefix_buffer_size =
      prefix_registered->metadata.allocation_size;
  buffer->split_prefix_metadata =
      SplitMetadataFromClientBuffer(prefix_registered->metadata);
#else
  auto prefix_metadata = ReadSplitBufferMetadataFileWithRetry(prefix_name);
  if (!prefix_metadata.ok()) {
    return prefix_metadata.status();
  }
  auto prefix_fd = OpenSplitSharedMemoryBuffer(*prefix_metadata, O_RDWR);
  if (!prefix_fd.ok()) {
    return prefix_fd.status();
  }
  auto prefix_addr =
      MapBuffer(*prefix_fd, prefix_metadata->allocation_size,
                BufferMapMode::kReadWrite);
  if (!prefix_addr.ok()) {
    return prefix_addr.status();
  }
  buffer->split_prefix_fd = std::move(*prefix_fd);
  buffer->split_prefix_buffer = *prefix_addr;
  buffer->split_prefix_buffer_size = prefix_metadata->allocation_size;
  buffer->split_prefix_metadata = std::move(*prefix_metadata);
#endif
  buffer->split_slot_buffers.resize(NumSlots(), nullptr);
  buffer->split_slot_sizes.resize(NumSlots(), payload_size);
  buffer->split_handles.resize(NumSlots(), 0);
  buffer->split_private_data.resize(NumSlots(), nullptr);
  buffer->split_metadata.reserve(NumSlots());
  buffer->split_fds.reserve(NumSlots());
  buffer->uses_split_callbacks = static_cast<bool>(SplitBuffersCallbacks().map);
  for (int slot = 0; slot < NumSlots(); slot++) {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
    absl::StatusOr<RegisteredClientBuffer> registered =
        GetRegisteredClientBuffer(static_cast<uint32_t>(buffer_index),
                                  /*is_prefix=*/false,
                                  static_cast<uint32_t>(slot));
    if (!registered.ok()) {
      return registered.status();
    }
    SplitBufferMetadata metadata =
        SplitMetadataFromClientBuffer(registered->metadata);
#else
    std::string shadow_file = absl::StrFormat("%s_slot_%d", metadata_base, slot);
    auto metadata_or = ReadSplitBufferMetadataFileWithRetry(shadow_file);
    if (!metadata_or.ok()) {
      return metadata_or.status();
    }
    SplitBufferMetadata metadata = std::move(*metadata_or);
#endif
    char *slot_addr = nullptr;
    uintptr_t slot_handle = metadata.handle;
    uint64_t slot_mapped_size = payload_size;
    void *slot_private_data = nullptr;
    if (SplitBuffersCallbacks().map) {
      auto mapping = SplitBuffersCallbacks().map(metadata);
      if (!mapping.ok()) {
        return mapping.status();
      }
      if (mapping->address == nullptr) {
        return absl::InternalError(
            "Split buffer map callback returned an empty mapping");
      }
      slot_addr = static_cast<char *>(mapping->address);
      slot_handle = mapping->handle == 0 ? metadata.handle : mapping->handle;
      slot_mapped_size = mapping->size == 0
                             ? payload_size
                             : static_cast<uint64_t>(mapping->size);
      slot_private_data = mapping->private_data;
    } else {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
      toolbelt::FileDescriptor slot_fd = std::move(registered->fd);
#else
      auto slot_fd_or = OpenSplitSharedMemoryBuffer(metadata, O_RDWR);
      if (!slot_fd_or.ok()) {
        return slot_fd_or.status();
      }
      toolbelt::FileDescriptor slot_fd = std::move(*slot_fd_or);
#endif
      auto addr = MapBuffer(slot_fd, payload_size, MapMode());
      if (!addr.ok()) {
        return addr.status();
      }
      slot_addr = *addr;
      slot_handle = static_cast<uintptr_t>(slot_fd.Fd());
      buffer->split_fds.push_back(std::move(slot_fd));
    }
    buffer->split_handles[slot] = slot_handle;
    buffer->split_slot_buffers[slot] = slot_addr;
    buffer->split_slot_sizes[slot] = slot_mapped_size;
    buffer->split_private_data[slot] = slot_private_data;
    buffer->split_metadata.push_back(std::move(metadata));
  }
  return buffer;
}

absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::OpenBuffer(int buffer_index) {
  std::string filename = BufferSharedMemoryName(buffer_index);
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  absl::StatusOr<RegisteredClientBuffer> buffer =
      GetRegisteredClientBuffer(static_cast<uint32_t>(buffer_index),
                                /*is_prefix=*/false, /*slot_id=*/0);
  if (!buffer.ok()) {
    return buffer.status();
  }
  return std::move(buffer->fd);
#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_LINUX
  // Open the shared memory file.
  return OpenSharedMemoryFile(filename, O_RDWR);
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
  auto &shim = GetSyscallShim();
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
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
#else
  // On Linux and Android, fstat on the fd gives the correct size.
  (void)buffer_index;
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
  void *p = MAP_FAILED;
  for (int attempt = 0; attempt < 100; attempt++) {
    p = MapMemory(shm_fd.Fd(), size, prot, "buffers");
    if (p != MAP_FAILED || errno != EINVAL) {
      break;
    }
    // A concurrent creator can win shm_open(O_CREAT | O_EXCL) but not have
    // completed ftruncate yet.  macOS reports mmap on that fd as EINVAL.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (p == MAP_FAILED) {
    return absl::InternalError(
        absl::StrFormat("Failed to map shared memory from fd %d: %s",
                        shm_fd.Fd(), strerror(errno)));
  }
  return reinterpret_cast<char *>(p);
}

void ClientChannel::TriggerRetirement(int slot_id) {
  if (!has_retirement_triggers_) {
    // No retirement triggers, let's avoid locking the mutex.
    return;
  }
  MessageSlot *slot = GetSlot(slot_id);
  if ((slot->flags.load(std::memory_order_relaxed) &
       kMessageIsActivation) != 0) {
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
