// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asil_client/channel_memory.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
#include <sys/syscall.h>
#endif

namespace subspace {
namespace asil {
namespace internal {

namespace {

constexpr size_t kMaxCcbSize = size_t{1} << 30;
// How long to wait for another client to finish creating a buffer.
constexpr int kBufferWaitMilliseconds = 100;

void SleepMillisecond() {
  struct timespec ts = {0, 1000000};
  (void)::nanosleep(&ts, nullptr);
}

void *MapFd(int fd, size_t size, int prot) {
  return ::mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
}

uint64_t PageAligned(uint64_t size) {
  const uint64_t page = static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
  return (size + page - 1) & ~(page - 1);
}

#if SUBSPACE_SHMEM_MODE != SUBSPACE_SHMEM_MODE_MEMFD
// Android's libc has no shm_open, and memfd mode never opens a buffer by name.
constexpr mode_t kShmMode = S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH;

int OpenShm(const char *name, int flags) {
  const mode_t old_umask = ::umask(0);
  const int fd = ::shm_open(name, flags, kShmMode);
  const int saved_errno = errno;
  (void)::umask(old_umask);
  errno = saved_errno;
  return fd;
}
#endif

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
// The shm object for a buffer is named after the inode of its shadow file.
Error PosixShmName(const char *shadow_file, char (&out)[64]) {
  struct stat st;
  if (::stat(shadow_file, &st) != 0) {
    return Error::kSharedMemoryError;
  }
  (void)std::snprintf(out, sizeof(out), "/subspace_%llu",
                      static_cast<unsigned long long>(st.st_ino));
  return Error::kOk;
}

// The shadow file records the buffer size.  Its length is not the buffer
// size: fstat of the shm object is page-aligned, and ftruncating the shadow
// file would commit that much memory where /tmp is RAM-backed.  A leading
// NUL keeps the server from reading the record as split-buffer metadata.
constexpr char kPosixShadowMagic[8] = {'\0', 'S', 'S', 'B', 'S', 'I', 'Z', 'E'};

struct PosixShadowRecord {
  char magic[sizeof(kPosixShadowMagic)];
  uint64_t size;
};

Error CreateShadowFile(const char *shadow_file) {
  const int fd = ::open(shadow_file, O_RDWR | O_CREAT | O_CLOEXEC, 0666);
  if (fd < 0) {
    return Error::kSharedMemoryError;
  }
  (void)::close(fd);
  return Error::kOk;
}

Error WriteShadowSize(const char *shadow_file, uint64_t size) {
  const int fd = ::open(shadow_file, O_WRONLY | O_TRUNC | O_CLOEXEC);
  if (fd < 0) {
    return Error::kSharedMemoryError;
  }
  PosixShadowRecord record = {};
  std::memcpy(record.magic, kPosixShadowMagic, sizeof(kPosixShadowMagic));
  record.size = size;
  const ssize_t n = ::write(fd, &record, sizeof(record));
  (void)::close(fd);
  return n == static_cast<ssize_t>(sizeof(record)) ? Error::kOk
                                                   : Error::kSharedMemoryError;
}

// Sets size to 0 when the creator has not recorded it yet.  A shadow file
// longer than the record was sized with ftruncate by an older client.
Error ReadShadowSize(const char *shadow_file, uint64_t &size) {
  const int fd = ::open(shadow_file, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return Error::kSharedMemoryError;
  }
  struct stat st;
  if (::fstat(fd, &st) != 0) {
    (void)::close(fd);
    return Error::kSharedMemoryError;
  }
  PosixShadowRecord record;
  const ssize_t n = ::read(fd, &record, sizeof(record));
  (void)::close(fd);
  if (n == static_cast<ssize_t>(sizeof(record)) &&
      std::memcmp(record.magic, kPosixShadowMagic, sizeof(kPosixShadowMagic)) ==
          0) {
    size = record.size;
    return Error::kOk;
  }
  if (static_cast<uint64_t>(st.st_size) > sizeof(record)) {
    size = static_cast<uint64_t>(st.st_size);
    return Error::kOk;
  }
  size = 0;
  return Error::kOk;
}
#endif

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
int CreateMemfd(const char *name, uint64_t size) {
  constexpr unsigned int kMemfdCloexec = 1;
  const int fd =
      static_cast<int>(::syscall(__NR_memfd_create, name, kMemfdCloexec));
  if (fd < 0) {
    return -1;
  }
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    const int saved_errno = errno;
    (void)::close(fd);
    errno = saved_errno;
    return -1;
  }
  return fd;
}
#endif

} // namespace

uint64_t Now() {
  struct timespec ts;
#if defined(__APPLE__)
  // toolbelt::Now() uses mach_absolute_time(), which is this clock.
  (void)::clock_gettime(CLOCK_UPTIME_RAW, &ts);
#else
  (void)::clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
         static_cast<uint64_t>(ts.tv_nsec);
}

void Trigger(int fd) {
#if defined(__linux__)
  const int64_t value = 1;
  (void)::write(fd, &value, sizeof(value));
#else
  const char value = 'x';
  (void)::write(fd, &value, sizeof(value));
#endif
}

void ClearTrigger(int fd) {
#if defined(__linux__)
  int64_t value;
  (void)::read(fd, &value, sizeof(value));
#else
  char buffer[256];
  while (::read(fd, buffer, sizeof(buffer)) > 0) {
  }
#endif
}

Error WaitReadable(int fd, int timeout_ms) {
  struct pollfd p = {fd, POLLIN, 0};
  for (;;) {
    const int n = ::poll(&p, 1, timeout_ms < 0 ? -1 : timeout_ms);
    if (n > 0) {
      return Error::kOk;
    }
    if (n == 0) {
      return Error::kTimeout;
    }
    if (errno != EINTR) {
      return Error::kInvalidArgument;
    }
  }
}

Error CopyName(const char *name, char (&out)[kMaxChannelNameLength + 1]) {
  if (name == nullptr || name[0] == '\0') {
    return Error::kInvalidArgument;
  }
  const size_t length = std::strlen(name);
  if (length > kMaxChannelNameLength) {
    return Error::kCapacityExceeded;
  }
  std::memcpy(out, name, length + 1);
  return Error::kOk;
}

Error ChannelMemory::Map(shm::SystemControlBlock *scb, int ccb_fd, int bcb_fd,
                         int num_slots, int32_t checksum_size,
                         int32_t metadata_size,
                         uint64_t subscriber_queue_arena_size,
                         const BufferContext *context) {
  if (ccb_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  if (num_slots <= 0 || context == nullptr) {
    return Error::kInvalidArgument;
  }
  if (subscriber_queue_arena_size > kMaxCcbSize) {
    return Error::kLayoutMismatch;
  }
  const size_t ccb_size = shm::CcbSize(num_slots, subscriber_queue_arena_size);
  if (ccb_size > kMaxCcbSize) {
    return Error::kLayoutMismatch;
  }
  void *ccb = MapFd(ccb_fd, ccb_size, PROT_READ | PROT_WRITE);
  if (ccb == MAP_FAILED) {
    return Error::kSharedMemoryError;
  }
  auto *typed_ccb = static_cast<shm::ChannelControlBlock *>(ccb);
  if (typed_ccb->version != shm::kChannelControlBlockVersion ||
      typed_ccb->num_slots != num_slots) {
    (void)::munmap(ccb, ccb_size);
    return Error::kLayoutMismatch;
  }
  void *bcb =
      MapFd(bcb_fd, sizeof(shm::BufferControlBlock), PROT_READ | PROT_WRITE);
  if (bcb == MAP_FAILED) {
    (void)::munmap(ccb, ccb_size);
    return Error::kSharedMemoryError;
  }
  scb_ = scb;
  ccb_ = typed_ccb;
  bcb_ = static_cast<shm::BufferControlBlock *>(bcb);
  context_ = context;
  ccb_size_ = ccb_size;
  queue_arena_size_ = subscriber_queue_arena_size;
  num_slots_ = num_slots;
  checksum_size_ = checksum_size > 0 ? checksum_size : 4;
  metadata_size_ = metadata_size > 0 ? metadata_size : 0;
  prefix_size_ = shm::PrefixSize(checksum_size_, metadata_size_);
  return Error::kOk;
}

void ChannelMemory::Unmap() {
  UnmapBuffers();
  if (bcb_ != nullptr) {
    (void)::munmap(bcb_, sizeof(shm::BufferControlBlock));
    bcb_ = nullptr;
  }
  if (ccb_ != nullptr) {
    (void)::munmap(ccb_, ccb_size_);
    ccb_ = nullptr;
    ccb_size_ = 0;
  }
  scb_ = nullptr;
  context_ = nullptr;
  queue_arena_size_ = 0;
  num_slots_ = 0;
}

void ChannelMemory::UnmapBuffers() {
  if (split_slots_ != nullptr) {
    for (int i = 0; i < num_slots_; i++) {
      SplitSlot &slot = split_slots_[i];
      if (slot.mapping.address != nullptr) {
        if (!slot.allocator_mapped) {
          (void)::munmap(slot.mapping.address,
                         static_cast<size_t>(slot.mapping.size));
        } else if (allocator_.unmap != nullptr) {
          allocator_.unmap(allocator_.context, slot.info, slot.mapping);
        }
      }
      slot = SplitSlot();
    }
    split_slots_ = nullptr;
  }
  allocator_ = SplitBufferAllocator();
  if (base_ != nullptr) {
    (void)::munmap(base_, static_cast<size_t>(size_));
  }
  base_ = nullptr;
  size_ = 0;
  slot_size_ = 0;
  index_ = -1;
}

Error ChannelMemory::BufferName(int index,
                                char (&out)[kMaxChannelNameLength + 64]) const {
  char sanitized[kMaxChannelNameLength + 1];
  if (Error e = CopyName(context_->channel_name, sanitized); e != Error::kOk) {
    return e;
  }
  for (char *p = sanitized; *p != '\0'; p++) {
    if (*p == '/') {
      *p = '.';
    }
  }
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  const char *format = "/tmp/subspace_%llu_%s_%d";
#else
  const char *format = "subspace_%llu_%s_%d";
#endif
  const int n = std::snprintf(
      out, sizeof(out), format,
      static_cast<unsigned long long>(context_->session_id), sanitized, index);
  if (n < 0 || static_cast<size_t>(n) >= sizeof(out)) {
    return Error::kCapacityExceeded;
  }
  return Error::kOk;
}

Error ChannelMemory::NewestIndex(int &index) const {
  const int num_buffers = ccb_->num_buffers.load(std::memory_order_acquire);
  if (num_buffers <= 0) {
    return Error::kNoBuffers;
  }
  if (num_buffers > static_cast<int>(shm::kMaxBuffers)) {
    return Error::kLayoutMismatch;
  }
  index = num_buffers - 1;
  return Error::kOk;
}

// Fetches a buffer from the server.  Its creator registers it just after
// creating it, so it may not be there yet.
Error ChannelMemory::GetRegisteredBuffer(int index, bool is_prefix,
                                         uint32_t slot_id,
                                         BufferReply &reply) const {
  BufferRequest request;
  request.channel_name = context_->channel_name;
  request.session_id = context_->session_id;
  request.buffer_index = static_cast<uint32_t>(index);
  request.is_prefix = is_prefix;
  request.slot_id = slot_id;
  for (int attempt = 0; attempt < kBufferWaitMilliseconds; attempt++) {
    if (Error e = context_->connection->GetBuffer(request, reply);
        e != Error::kOk) {
      return e;
    }
    if (reply.found) {
      return Error::kOk;
    }
    SleepMillisecond();
  }
  return Error::kSharedMemoryError;
}

Error ChannelMemory::MapBuffer(int index, int fd, uint64_t size, int prot) {
  const uint64_t prefixes =
      static_cast<uint64_t>(num_slots_) * static_cast<uint64_t>(prefix_size_);
  if (size <= prefixes) {
    return Error::kLayoutMismatch;
  }
  void *p = MAP_FAILED;
  for (int attempt = 0; attempt < kBufferWaitMilliseconds; attempt++) {
    p = MapFd(fd, static_cast<size_t>(size), prot);
    if (p != MAP_FAILED || errno != EINVAL) {
      break;
    }
    // The creator may not have sized the object yet.
    SleepMillisecond();
  }
  if (p == MAP_FAILED) {
    return Error::kSharedMemoryError;
  }
  index_ = index;
  base_ = static_cast<char *>(p);
  size_ = size;
  slot_size_ = static_cast<int64_t>((size - prefixes) /
                                    static_cast<uint64_t>(num_slots_));
  return Error::kOk;
}

// Opens buffer index, which has already been created.
Error ChannelMemory::OpenBuffer(int index, bool writable, UniqueFd &fd,
                                uint64_t &size) const {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  (void)writable;
  BufferReply reply;
  if (Error e = GetRegisteredBuffer(index, /*is_prefix=*/false, 0, reply);
      e != Error::kOk) {
    return e;
  }
  if (!reply.fd.Valid()) {
    return Error::kSharedMemoryError;
  }
  fd = static_cast<UniqueFd &&>(reply.fd);
  size = reply.full_size;
  if (size == 0) {
    struct stat st;
    if (::fstat(fd.Get(), &st) != 0) {
      return Error::kSharedMemoryError;
    }
    size = static_cast<uint64_t>(st.st_size);
  }
  return Error::kOk;
#else
  char name[kMaxChannelNameLength + 64];
  if (Error e = BufferName(index, name); e != Error::kOk) {
    return e;
  }
  const int flags = writable ? O_RDWR : O_RDONLY;
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  char shm_name[64];
  if (Error e = PosixShmName(name, shm_name); e != Error::kOk) {
    return e;
  }
  fd.Reset(OpenShm(shm_name, flags));
  if (!fd.Valid()) {
    return Error::kSharedMemoryError;
  }
  // The creator writes the shadow record before sizing the shm object, so a
  // nonzero shm size means the record is the buffer's real size.
  size = 0;
  for (int attempt = 0; attempt < kBufferWaitMilliseconds && size == 0;
       attempt++) {
    struct stat shm_st;
    if (::fstat(fd.Get(), &shm_st) != 0) {
      return Error::kSharedMemoryError;
    }
    if (shm_st.st_size > 0) {
      if (Error e = ReadShadowSize(name, size); e != Error::kOk) {
        return e;
      }
    }
    if (size == 0) {
      SleepMillisecond();
    }
  }
  if (size == 0) {
    return Error::kSharedMemoryError;
  }
#else
  fd.Reset(OpenShm(name, flags));
  if (!fd.Valid()) {
    return Error::kSharedMemoryError;
  }
  size = 0;
  for (int attempt = 0; attempt < kBufferWaitMilliseconds && size == 0;
       attempt++) {
    struct stat st;
    if (::fstat(fd.Get(), &st) != 0) {
      return Error::kSharedMemoryError;
    }
    size = static_cast<uint64_t>(st.st_size);
    if (size == 0) {
      // The creator hasn't sized the object yet.
      SleepMillisecond();
    }
  }
#endif
  return Error::kOk;
#endif
}

Error ChannelMemory::AttachBuffer(bool writable) {
  if (ccb_ == nullptr) {
    return Error::kNotInitialized;
  }
  if (index_ != -1) {
    return Error::kAlreadyInitialized;
  }
  int index = -1;
  if (Error e = NewestIndex(index); e != Error::kOk) {
    return e;
  }
  UniqueFd fd;
  uint64_t size = 0;
  if (Error e = OpenBuffer(index, writable, fd, size); e != Error::kOk) {
    return e;
  }
  return MapBuffer(index, fd.Get(), size,
                   writable ? PROT_READ | PROT_WRITE : PROT_READ);
}

// Creates buffer 0 and publishes it in the CCB.  If another publisher got
// there first this maps nothing, and the caller attaches to the channel's
// buffer.
Error ChannelMemory::CreateBuffer(uint64_t full_size) {
  char name[kMaxChannelNameLength + 64];
  if (Error e = BufferName(0, name); e != Error::kOk) {
    return e;
  }
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  UniqueFd fd(CreateMemfd(name, full_size));
  if (!fd.Valid()) {
    return Error::kSharedMemoryError;
  }
#else
  const bool root = ::getuid() == 0;
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  if (Error e = CreateShadowFile(name); e != Error::kOk) {
    return e;
  }
  char shm_name[64];
  if (Error e = PosixShmName(name, shm_name); e != Error::kOk) {
    return e;
  }
  const char *path = name;
  UniqueFd fd(OpenShm(shm_name, O_RDWR | O_CREAT | O_EXCL));
#else
  char path[kMaxChannelNameLength + 80];
  (void)std::snprintf(path, sizeof(path), "/dev/shm/%s", name);
  UniqueFd fd(OpenShm(name, O_RDWR | O_CREAT | O_EXCL));
#endif
  if (!fd.Valid()) {
    // EEXIST: another publisher created the buffer.
    return errno == EEXIST ? Error::kOk : Error::kSharedMemoryError;
  }
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  // Record the size before sizing the shm object, matching the standard
  // client: an attacher that sees a nonzero shm size can trust the record.
  if (Error e = WriteShadowSize(name, full_size); e != Error::kOk) {
    (void)::shm_unlink(shm_name);
    return e;
  }
#endif
  if (::ftruncate(fd.Get(), static_cast<off_t>(full_size)) != 0 ||
      ::chmod(path, 0777) != 0 ||
      (root && ::chown(path, static_cast<uid_t>(context_->user_id),
                       static_cast<gid_t>(context_->group_id)) != 0)) {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
    (void)::shm_unlink(shm_name);
#endif
    return Error::kSharedMemoryError;
  }
#endif
  bcb_->sizes[0].store(full_size, std::memory_order_relaxed);
  int expected = 0;
  if (!ccb_->num_buffers.compare_exchange_strong(
          expected, 1, std::memory_order_release, std::memory_order_relaxed)) {
    return Error::kOk;
  }
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  ClientBuffer registration;
  registration.channel_name = context_->channel_name;
  registration.session_id = context_->session_id;
  registration.buffer_index = 0;
  registration.full_size = full_size;
  if (Error e = context_->connection->RegisterBuffer(registration, fd.Get());
      e != Error::kOk) {
    expected = 1;
    if (ccb_->num_buffers.compare_exchange_strong(expected, 0,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_relaxed)) {
      bcb_->sizes[0].store(0, std::memory_order_relaxed);
    }
    return e;
  }
#endif
  return MapBuffer(0, fd.Get(), full_size, PROT_READ | PROT_WRITE);
}

Error ChannelMemory::CreateOrAttachBuffer(int64_t slot_size) {
  if (ccb_ == nullptr) {
    return Error::kNotInitialized;
  }
  if (index_ != -1) {
    return Error::kAlreadyInitialized;
  }
  const uint64_t full_size =
      static_cast<uint64_t>(num_slots_) *
      (static_cast<uint64_t>(slot_size) + static_cast<uint64_t>(prefix_size_));
  for (int attempt = 0; attempt < kBufferWaitMilliseconds; attempt++) {
    if (ccb_->num_buffers.load(std::memory_order_acquire) == 0) {
      if (Error e = CreateBuffer(full_size); e != Error::kOk) {
        return e;
      }
      if (index_ == -1) {
        // Another publisher created the buffer.  It may not have published
        // it yet.
        SleepMillisecond();
        continue;
      }
    } else if (Error e = AttachBuffer(/*writable=*/true); e != Error::kOk) {
      return e;
    }
    if (slot_size_ < slot_size) {
      UnmapBuffers();
      return Error::kLayoutMismatch;
    }
    return Error::kOk;
  }
  return Error::kSharedMemoryError;
}

Error ChannelMemory::AttachSplitBuffers(bool writable, SplitSlot *slots,
                                        int32_t capacity,
                                        const SplitBufferAllocator &allocator) {
  if (ccb_ == nullptr) {
    return Error::kNotInitialized;
  }
  if (index_ != -1) {
    return Error::kAlreadyInitialized;
  }
  if (slots == nullptr) {
    return Error::kInvalidArgument;
  }
  if (capacity < num_slots_) {
    return Error::kCapacityExceeded;
  }
  int index = -1;
  if (Error e = NewestIndex(index); e != Error::kOk) {
    return e;
  }
  const uint64_t full_size = bcb_->sizes[index].load(std::memory_order_acquire);
  const uint64_t prefixes =
      static_cast<uint64_t>(num_slots_) * static_cast<uint64_t>(prefix_size_);
  if (full_size <= prefixes) {
    return Error::kLayoutMismatch;
  }
  BufferReply prefix;
  if (Error e = GetRegisteredBuffer(index, /*is_prefix=*/true, 0, prefix);
      e != Error::kOk) {
    return e;
  }
  if (!prefix.fd.Valid() || prefix.allocation_size < prefixes) {
    return Error::kLayoutMismatch;
  }
  const int prot = writable ? PROT_READ | PROT_WRITE : PROT_READ;
  void *p =
      MapFd(prefix.fd.Get(), static_cast<size_t>(prefix.allocation_size), prot);
  if (p == MAP_FAILED) {
    return Error::kSharedMemoryError;
  }
  index_ = index;
  base_ = static_cast<char *>(p);
  size_ = prefix.allocation_size;
  slot_size_ = static_cast<int64_t>((full_size - prefixes) /
                                    static_cast<uint64_t>(num_slots_));
  split_slots_ = slots;
  allocator_ = allocator;
  for (int i = 0; i < num_slots_; i++) {
    slots[i] = SplitSlot();
  }
  const uint64_t payload_size = PageAligned(static_cast<uint64_t>(slot_size_));
  for (int i = 0; i < num_slots_; i++) {
    if (Error e = MapSplitSlot(i, payload_size, prot); e != Error::kOk) {
      UnmapBuffers();
      return e;
    }
  }
  return Error::kOk;
}

Error ChannelMemory::MapSplitSlot(int slot_id, uint64_t payload_size,
                                  int prot) {
  BufferReply reply;
  if (Error e = GetRegisteredBuffer(index_, /*is_prefix=*/false,
                                    static_cast<uint32_t>(slot_id), reply);
      e != Error::kOk) {
    return e;
  }
  SplitSlot &slot = split_slots_[slot_id];
  slot.info.channel_name = context_->channel_name;
  slot.info.session_id = context_->session_id;
  slot.info.buffer_index = static_cast<uint32_t>(index_);
  slot.info.slot_id = static_cast<uint32_t>(slot_id);
  slot.info.full_size = reply.full_size;
  slot.info.allocation_size = reply.allocation_size;
  slot.info.handle = reply.handle;
  slot.info.map_offset = reply.map_offset;
  slot.info.allocator = reply.allocator;
  if (reply.allocator == BufferAllocator::kSplitCallback) {
    if (allocator_.map == nullptr) {
      return Error::kUnsupported;
    }
    slot.info.fd = reply.fd.Get();
    SplitBufferMapping mapping;
    const Error e = allocator_.map(allocator_.context, slot.info, mapping);
    slot.info.fd = -1;
    if (e != Error::kOk) {
      return e;
    }
    if (mapping.address == nullptr) {
      return Error::kSharedMemoryError;
    }
    if (mapping.size == 0) {
      mapping.size = payload_size;
    }
    slot.mapping = mapping;
    slot.allocator_mapped = true;
    return mapping.size < static_cast<uint64_t>(slot_size_)
               ? Error::kLayoutMismatch
               : Error::kOk;
  }
  if (!reply.fd.Valid()) {
    return Error::kSharedMemoryError;
  }
  void *p = MapFd(reply.fd.Get(), static_cast<size_t>(payload_size), prot);
  if (p == MAP_FAILED) {
    return Error::kSharedMemoryError;
  }
  slot.mapping.address = p;
  slot.mapping.size = payload_size;
  return Error::kOk;
}

shm::MessagePrefix *ChannelMemory::Prefix(const shm::MessageSlot *slot) const {
  if (!InMappedBuffer(slot)) {
    return nullptr;
  }
  const int64_t stride = split_slots_ != nullptr
                             ? prefix_size_
                             : prefix_size_ + shm::Aligned64(slot_size_);
  return reinterpret_cast<shm::MessagePrefix *>(base_ + stride * slot->id);
}

char *ChannelMemory::Payload(const shm::MessageSlot *slot) const {
  if (!InMappedBuffer(slot)) {
    return nullptr;
  }
  if (split_slots_ != nullptr) {
    return static_cast<char *>(split_slots_[slot->id].mapping.address);
  }
  return reinterpret_cast<char *>(Prefix(slot)) + prefix_size_;
}

void ChannelMemory::TriggerRetirement(const TriggerFdList &triggers,
                                      int32_t slot_id) const {
  if (triggers.Size() == 0 || !ReportsRetirement(slot_id)) {
    return;
  }
  for (int i = 0; i < triggers.Size(); i++) {
    (void)::write(triggers.Get(i), &slot_id, sizeof(slot_id));
  }
}

} // namespace internal
} // namespace asil
} // namespace subspace
