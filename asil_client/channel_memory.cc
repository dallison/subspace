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
#if ASIL_SHM_MODE == ASIL_SHM_MEMFD
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

#if ASIL_SHM_MODE != ASIL_SHM_MEMFD
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

#if ASIL_SHM_MODE == ASIL_SHM_POSIX
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

// The shadow file's length is the buffer's true size.  Only the first
// publisher sets it.
Error CreateShadowFile(const char *shadow_file, uint64_t size) {
  const int fd = ::open(shadow_file, O_RDWR | O_CREAT | O_CLOEXEC, 0666);
  if (fd < 0) {
    return Error::kSharedMemoryError;
  }
  struct stat st;
  Error e = Error::kOk;
  if (::fstat(fd, &st) != 0) {
    e = Error::kSharedMemoryError;
  } else if (st.st_size == 0 &&
             ::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    e = Error::kSharedMemoryError;
  }
  (void)::close(fd);
  return e;
}
#endif

#if ASIL_SHM_MODE == ASIL_SHM_MEMFD
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

bool PushSlotQueue(shm::SlotQueue *queue, int32_t slot_id, uint64_t ordinal) {
  const size_t capacity = queue->capacity;
  if (capacity == 0) {
    return false;
  }
  shm::SlotQueueEntry *entries = queue->Entries();
  shm::SlotQueueEntry *entry = nullptr;
  uint64_t tail = queue->tail.load(std::memory_order_relaxed);
  for (size_t attempt = 0; attempt < shm::kMaxSlotQueueCasAttempts; ++attempt) {
    const uint64_t head = queue->head.load(std::memory_order_acquire);
    if (tail - head >= capacity) {
      if (!queue->drop_oldest) {
        return false;
      }
      // Evict the oldest entry, as InPlaceSlotQueue::DropFront does.
      shm::SlotQueueEntry &front = entries[head % capacity];
      uint64_t expected = head;
      if (front.sequence.load(std::memory_order_acquire) != head + 1 ||
          !queue->head.compare_exchange_weak(expected, head + 1,
                                             std::memory_order_acq_rel,
                                             std::memory_order_relaxed)) {
        return false;
      }
      front.sequence.store(head + capacity, std::memory_order_release);
      queue->overflow_count.fetch_add(1, std::memory_order_release);
      tail = queue->tail.load(std::memory_order_relaxed);
      continue;
    }
    shm::SlotQueueEntry &candidate = entries[tail % capacity];
    if (candidate.sequence.load(std::memory_order_acquire) != tail) {
      return false;
    }
    if (queue->tail.compare_exchange_strong(tail, tail + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
      entry = &candidate;
      break;
    }
  }
  if (entry == nullptr) {
    return false;
  }
  entry->slot_id.store(slot_id, std::memory_order_relaxed);
  entry->ordinal.store(ordinal, std::memory_order_relaxed);
  entry->sequence.store(tail + 1, std::memory_order_release);
  return true;
}

void MarkSlotQueueInsertionFailure(shm::SlotQueue *queue) {
  queue->insertion_failed.store(true, std::memory_order_release);
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
  queue_arena_size_ = static_cast<size_t>(subscriber_queue_arena_size);
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
#if ASIL_SHM_MODE == ASIL_SHM_POSIX
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
  if (num_buffers > shm::kMaxBuffers) {
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
#if ASIL_SHM_MODE == ASIL_SHM_MEMFD
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
#if ASIL_SHM_MODE == ASIL_SHM_POSIX
  char shm_name[64];
  if (Error e = PosixShmName(name, shm_name); e != Error::kOk) {
    return e;
  }
  fd.Reset(OpenShm(shm_name, flags));
  struct stat st;
  if (!fd.Valid() || ::stat(name, &st) != 0) {
    return Error::kSharedMemoryError;
  }
  size = static_cast<uint64_t>(st.st_size);
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
#if ASIL_SHM_MODE == ASIL_SHM_MEMFD
  UniqueFd fd(CreateMemfd(name, full_size));
  if (!fd.Valid()) {
    return Error::kSharedMemoryError;
  }
#else
  const bool root = ::getuid() == 0;
#if ASIL_SHM_MODE == ASIL_SHM_POSIX
  if (Error e = CreateShadowFile(name, full_size); e != Error::kOk) {
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
  if (::ftruncate(fd.Get(), static_cast<off_t>(full_size)) != 0 ||
      ::chmod(path, 0777) != 0 ||
      (root && ::chown(path, static_cast<uid_t>(context_->user_id),
                       static_cast<gid_t>(context_->group_id)) != 0)) {
    return Error::kSharedMemoryError;
  }
#endif
  bcb_->sizes[0].store(full_size, std::memory_order_relaxed);
  int expected = 0;
  if (!ccb_->num_buffers.compare_exchange_strong(
          expected, 1, std::memory_order_release, std::memory_order_relaxed)) {
    return Error::kOk;
  }
#if ASIL_SHM_MODE == ASIL_SHM_MEMFD
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

shm::MessageSlot *ChannelMemory::Slot(int id) const {
  auto *slots = reinterpret_cast<shm::MessageSlot *>(
      reinterpret_cast<char *>(ccb_) + sizeof(shm::ChannelControlBlock));
  return &slots[id];
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

BitsetView ChannelMemory::RetiredSlots() const {
  return BitsetView(reinterpret_cast<char *>(ccb_) +
                        shm::RetiredSlotsOffset(num_slots_),
                    num_slots_);
}

BitsetView ChannelMemory::FreeSlots() const {
  return BitsetView(reinterpret_cast<char *>(ccb_) +
                        shm::FreeSlotsOffset(num_slots_),
                    num_slots_);
}

BitsetView ChannelMemory::AvailableSlots(int sub_id) const {
  return BitsetView(reinterpret_cast<char *>(ccb_) +
                        shm::AvailableSlotsOffset(num_slots_, sub_id),
                    num_slots_);
}

BitsetView ChannelMemory::Subscribers() const {
  return BitsetView(&ccb_->subscribers, shm::kMaxSlotOwners);
}

shm::AvailableSlotQueueIndex *ChannelMemory::QueueIndex() const {
  return reinterpret_cast<shm::AvailableSlotQueueIndex *>(
      reinterpret_cast<char *>(ccb_) + shm::SlotQueueIndexOffset(num_slots_));
}

shm::SlotQueue *ChannelMemory::Queue(int sub_id) const {
  const uint64_t offset =
      QueueIndex()->offsets[sub_id].load(std::memory_order_acquire);
  if (offset == shm::kInvalidSlotQueueOffset || offset > queue_arena_size_ ||
      queue_arena_size_ - offset < sizeof(shm::SlotQueue)) {
    return nullptr;
  }
  auto *queue = reinterpret_cast<shm::SlotQueue *>(
      reinterpret_cast<char *>(ccb_) + shm::SlotQueueArenaOffset(num_slots_) +
      offset);
  const size_t room = queue_arena_size_ - offset - sizeof(shm::SlotQueue);
  if (queue->capacity > room / sizeof(shm::SlotQueueEntry)) {
    return nullptr;
  }
  return queue;
}

int ChannelMemory::NumSubscribers(int vchan_id) const {
  const shm::SubscriberCounter &counter = ccb_->num_subs;
  for (;;) {
    const uint64_t before = counter.sequence.load(std::memory_order_acquire);
    if ((before & 1) != 0) {
      // A server died while updating the count.
      return shm::kMaxSlotOwners;
    }
    const int mux_count = counter.counts[0].load(std::memory_order_relaxed);
    const int count = vchan_id == -1
                          ? mux_count
                          : mux_count + counter.counts[vchan_id + 1].load(
                                            std::memory_order_relaxed);
    if (counter.sequence.load(std::memory_order_acquire) == before) {
      return count;
    }
  }
}

uint64_t ChannelMemory::CleanupGeneration(int vchan_id) const {
  const uint64_t mux_generation =
      ccb_->subscriber_cleanup_generation[0].load(std::memory_order_acquire);
  if (vchan_id == -1) {
    return mux_generation;
  }
  return mux_generation +
         ccb_->subscriber_cleanup_generation[vchan_id + 1].load(
             std::memory_order_acquire);
}

bool ChannelMemory::AtomicIncRefCount(shm::MessageSlot *slot, bool reliable,
                                      int inc, uint64_t ordinal, int vchan_id,
                                      bool retire, bool *retired) {
  if (retired != nullptr) {
    *retired = false;
  }
  ordinal &= shm::kOrdinalMask;
  for (;;) {
    uint64_t ref = slot->refs.load(std::memory_order_relaxed);
    if ((ref & shm::kPubOwned) != 0) {
      return false;
    }
    const uint64_t ref_ordinal =
        (ref >> shm::kOrdinalShift) & shm::kOrdinalMask;
    const int ref_vchan_id = shm::RefsVchanId(ref);
    if (ref_ordinal != 0 && ordinal != 0 &&
        (ref_ordinal != ordinal || ref_vchan_id != vchan_id)) {
      return false;
    }
    uint64_t refs = ref & shm::kRefCountMask;
    uint64_t reliable_refs =
        (ref >> shm::kReliableRefCountShift) & shm::kRefCountMask;
    if (inc < 0 && refs == 0) {
      return true;
    }
    refs = static_cast<uint64_t>(static_cast<int64_t>(refs) + inc);
    if (reliable) {
      reliable_refs =
          static_cast<uint64_t>(static_cast<int64_t>(reliable_refs) + inc);
    }
    uint64_t retired_refs =
        (ref >> shm::kRetiredRefsShift) & shm::kRetiredRefsMask;
    if (retire) {
      retired_refs++;
    }
    const uint64_t new_ref =
        shm::BuildRefsBitField(ref_ordinal, ref_vchan_id, retired_refs) |
        (reliable_refs << shm::kReliableRefCountShift) | refs;
    if (slot->refs.compare_exchange_weak(ref, new_ref,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed)) {
      if (retire && refs == 0 && reliable_refs == 0 &&
          retired_refs >= static_cast<uint64_t>(NumSubscribers(ref_vchan_id)) &&
          RetiredSlots().SetWasClear(slot->id) && retired != nullptr) {
        *retired = true;
      }
      return true;
    }
  }
}

bool ChannelMemory::TryRetireSlot(shm::MessageSlot *slot) {
  if (slot->ordinal.load(std::memory_order_relaxed) == 0) {
    return false;
  }
  const uint64_t refs = slot->refs.load(std::memory_order_acquire);
  if ((refs & shm::kPubOwned) != 0) {
    return false;
  }
  const uint64_t ref_count = refs & shm::kRefCountMask;
  const uint64_t reliable_ref_count =
      (refs >> shm::kReliableRefCountShift) & shm::kRefCountMask;
  const uint64_t retired_refs =
      (refs >> shm::kRetiredRefsShift) & shm::kRetiredRefsMask;
  if (ref_count != 0 || reliable_ref_count != 0 ||
      retired_refs <
          static_cast<uint64_t>(NumSubscribers(shm::RefsVchanId(refs)))) {
    return false;
  }
  return RetiredSlots().SetWasClear(slot->id);
}

void ChannelMemory::TriggerRetirement(const TriggerFdList &triggers,
                                      int32_t slot_id) const {
  if (triggers.Size() == 0) {
    return;
  }
  if (slot_id >= 0 && slot_id < num_slots_ &&
      (Slot(slot_id)->flags.load(std::memory_order_relaxed) &
       shm::kMessageIsActivation) != 0) {
    return;
  }
  for (int i = 0; i < triggers.Size(); i++) {
    (void)::write(triggers.Get(i), &slot_id, sizeof(slot_id));
  }
}

} // namespace internal
} // namespace asil
} // namespace subspace
