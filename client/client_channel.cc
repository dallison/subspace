#include "client/client_channel.h"
#include <sys/mman.h>
#if defined(__APPLE__)
#include <sys/posix_shm.h>
#endif
#include <sys/stat.h>

namespace subspace {

namespace details {

template <typename BufferSetIter>
static void UnmapBuffers(BufferSetIter first, BufferSetIter last,
                         int num_slots) {
  // Unmap any previously mapped buffers.
  for (; first < last; ++first) {
    int64_t buffers_size = first->full_size;
    if (buffers_size > 0 && first->buffer != nullptr) {
      UnmapMemory(first->buffer, buffers_size, "buffers");
      first->buffer = nullptr;
      first->slot_size = 0;
      first->full_size = 0;
    }
  }
}

#if defined(__APPLE__)

absl::StatusOr<std::string>
ClientChannel::CreateMacOSSharedMemoryFile(const std::string &filename,
                                           off_t size) {
  // Create a file in /tmp and make it the same size as the shared memory.  This
  // will not actually allocate any disk space.
  int fd = open(filename.c_str(), O_RDWR | O_CREAT, 0666);
  if (fd < 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to open shadow file %s: %s", filename.c_str(),
                        strerror(errno)));
  }
  if (ftruncate(fd, size) < 0) {
    close(fd);
    return absl::InternalError(
        absl::StrFormat("Failed to truncate shadow file %s to size %zd: %s",
                        filename.c_str(), size, strerror(errno)));
  }
  close(fd);

  return MacOsSharedMemoryName(filename);
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

void ClientChannel::UnmapUnusedBuffers() {
  for (size_t i = 0; i + 1 < buffers_.size(); i++) {

    if (buffers_[i]->buffer == nullptr) {
      continue;
    }
    if (bcb_->refs[i] == 0) {
      if (buffers_[i]->full_size > 0) {
        if (debug_) {
          printf("%p: Unmapping unused buffers at index %zd\n", this, i);
        }
        UnmapMemory(buffers_[i]->buffer, buffers_[i]->full_size, "buffers");
        buffers_[i]->buffer = nullptr;
        buffers_[i]->full_size = 0;
        buffers_[i]->slot_size = 0;
      }
    }
  }
}

bool ClientChannel::ValidateSlotBuffer(MessageSlot *slot,
                                       std::function<bool()> reload) {
  if (slot->buffer_index < 0) {
    return true;
  }
  if (reload == nullptr) {
    return true;
  }
  char *buf = Buffer(slot->id);
  int retries = 1000;
  while (retries-- > 0 && buf == nullptr) {
    ReloadIfNecessary(reload);
    buf = Buffer(slot->id);
  }
  if (buf == nullptr) {
    return false;
  }
  return true;
}

absl::Status ClientChannel::AttachBuffers() {
  // NOTE: the num_buffers variable in the CCB is atomic and could change while
  // we are in or after we are done with this loop.
  bool map_read_only = IsSubscriber();
  int num_buffers = ccb_->num_buffers;
  while (buffers_.size() < size_t(num_buffers)) {
    // We need to open the next buffer in the list.  The buffer index is
    size_t buffer_index = buffers_.size();
    auto shm_fd = OpenBuffer(buffer_index);
    if (!shm_fd.ok()) {
      return shm_fd.status();
    }
    auto size = GetBufferSize(*shm_fd, buffer_index);
    if (!size.ok()) {
      return size.status();
    }
    if (*size == 0) {
      // It's possible the ftruncate has not been called yet, so we try again.
      continue;
    }
    absl::StatusOr<char *> addr;
    uint64_t slot_size = BufferSizeToSlotSize(*size);
    if (slot_size > 0) {
      // Map the shared memory buffer.
      addr = MapBuffer(*shm_fd, *size, map_read_only);
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

void ClientChannel::Unmap() {
  if (scb_ == nullptr) {
    // Not yet mapped.
    return;
  }
  Channel::Unmap();

  for (auto &buffer : buffers_) {
    if (buffer->full_size > 0 && buffer->buffer != nullptr) {
      UnmapMemory(buffer->buffer, buffer->full_size, "buffers");
    }
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
  int shm_fd =
      shm_open(filename.c_str(), flags, S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH);
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

#if !defined(__APPLE__)
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
  int e = ftruncate(shm_fd->Fd(), off_t(size));
  if (e == -1) {
    (void)shm_unlink(filename.c_str());
    return absl::InternalError(
        absl::StrFormat("Failed to set length of shared memory %s: %s",
                        filename, strerror(errno)));
  }

#else
  // On MacOS we need to create a shadow file that has the same size as the
  // shared memory file.  This is because the fstat of the shm "file" returns a
  // page aligned size, which is not what we want.  The shadow file is used
  // to determine the size of the shared memory segment.
  absl::StatusOr<std::string> shm_name =
      CreateMacOSSharedMemoryFile(filename, off_t(size));
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
  int e = ftruncate(shm_fd->Fd(), off_t(size));
  if (e == -1) {
    (void)shm_unlink(filename.c_str());
    return absl::InternalError(
        absl::StrFormat("Failed to set length of shared memory %s: %s",
                        filename, strerror(errno)));
  }

#endif
  return *shm_fd;
}

absl::StatusOr<toolbelt::FileDescriptor>
ClientChannel::OpenBuffer(int buffer_index) {
  std::string filename = BufferSharedMemoryName(buffer_index);
#if !defined(__APPLE__)
  // Open the shared memory file.
  return OpenSharedMemoryFile(filename, O_RDWR);
#else
  auto shm_name = MacOsSharedMemoryName(filename);
  if (!shm_name.ok()) {
    return shm_name.status();
  }
  return OpenSharedMemoryFile(*shm_name, O_RDWR);
#endif
}

absl::StatusOr<size_t>
ClientChannel::GetBufferSize(toolbelt::FileDescriptor &shm_fd,
                             int buffer_index) const {
#if defined(__APPLE__)
  // On MacOS we need to look at the size of the shadow file because it looks
  // like the fstat of the shm "file" returns a page aligned size.
  std::string filename = BufferSharedMemoryName(buffer_index);
  struct stat sb;
  if (stat(filename.c_str(), &sb) == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to get size of shared memory %s: %s", filename,
                        strerror(errno)));
  }
  return sb.st_size;
#else
  struct stat sb;
  if (fstat(shm_fd.Fd(), &sb) == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to get size of shared memory from fd %d: %s",
                        shm_fd.Fd(), strerror(errno)));
  }
  return sb.st_size;
#endif
}

absl::StatusOr<char *>
ClientChannel::MapBuffer(toolbelt::FileDescriptor &shm_fd, size_t size,
                         bool read_only) {
  int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);
  void *p = MapMemory(shm_fd.Fd(), size, prot, "buffers");
  if (p == MAP_FAILED) {
    return absl::InternalError(
        absl::StrFormat("Failed to map shared memory from fd %d: %s",
                        shm_fd.Fd(), strerror(errno)));
  }
  return reinterpret_cast<char *>(p);
}

} // namespace details
} // namespace subspace
