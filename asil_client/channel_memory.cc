// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asil_client/channel_memory.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace subspace {
namespace asil {
namespace internal {

namespace {

constexpr size_t kMaxCcbSize = size_t{1} << 30;

void SleepMillisecond() {
  struct timespec ts = {0, 1000000};
  (void)::nanosleep(&ts, nullptr);
}

void *MapFd(int fd, size_t size, int prot) {
  return ::mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
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
                         int32_t metadata_size) {
  if (ccb_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  if (num_slots <= 0) {
    // The channel has no layout yet.  Static channels always have one.
    return Error::kUnsupported;
  }
  const size_t ccb_size = shm::CcbSize(num_slots, 0);
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
  void *bcb = MapFd(bcb_fd, sizeof(shm::BufferControlBlock),
                    PROT_READ | PROT_WRITE);
  if (bcb == MAP_FAILED) {
    (void)::munmap(ccb, ccb_size);
    return Error::kSharedMemoryError;
  }
  scb_ = scb;
  ccb_ = typed_ccb;
  bcb_ = static_cast<shm::BufferControlBlock *>(bcb);
  ccb_size_ = ccb_size;
  num_slots_ = num_slots;
  checksum_size_ = checksum_size > 0 ? checksum_size : 4;
  metadata_size_ = metadata_size > 0 ? metadata_size : 0;
  prefix_size_ = shm::PrefixSize(checksum_size_, metadata_size_);
  return Error::kOk;
}

void ChannelMemory::Unmap() {
  if (buffer_ != nullptr) {
    (void)::munmap(buffer_, buffer_size_);
    buffer_ = nullptr;
    buffer_size_ = 0;
    slot_size_ = 0;
  }
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
  num_slots_ = 0;
}

Error ChannelMemory::BufferName(const char *channel_name, uint64_t session_id,
                                char (&out)[kMaxChannelNameLength + 64]) const {
  char sanitized[kMaxChannelNameLength + 1];
  if (Error e = CopyName(channel_name, sanitized); e != Error::kOk) {
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
  const int n =
      std::snprintf(out, sizeof(out), format,
                    static_cast<unsigned long long>(session_id), sanitized, 0);
  if (n < 0 || static_cast<size_t>(n) >= sizeof(out)) {
    return Error::kCapacityExceeded;
  }
  return Error::kOk;
}

Error ChannelMemory::MapBuffer(int fd, uint64_t size, int prot) {
  const uint64_t prefixes =
      static_cast<uint64_t>(num_slots_) * static_cast<uint64_t>(prefix_size_);
  if (size <= prefixes) {
    return Error::kLayoutMismatch;
  }
  void *p = MAP_FAILED;
  for (int attempt = 0; attempt < 100; attempt++) {
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
  buffer_ = static_cast<char *>(p);
  buffer_size_ = size;
  slot_size_ = static_cast<int64_t>((size - prefixes) /
                                    static_cast<uint64_t>(num_slots_));
  return Error::kOk;
}

Error ChannelMemory::CreateOrAttachBuffer(const char *channel_name,
                                          uint64_t session_id,
                                          int64_t slot_size, int32_t user_id,
                                          int32_t group_id) {
  if (ccb_ == nullptr) {
    return Error::kNotInitialized;
  }
  if (buffer_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  int num_buffers = ccb_->num_buffers.load(std::memory_order_relaxed);
  if (num_buffers > 1) {
    // The channel has been resized.
    return Error::kUnsupported;
  }
  const uint64_t full_size =
      static_cast<uint64_t>(num_slots_) *
      (static_cast<uint64_t>(slot_size) + static_cast<uint64_t>(prefix_size_));

  char name[kMaxChannelNameLength + 64];
  if (Error e = BufferName(channel_name, session_id, name); e != Error::kOk) {
    return e;
  }
  const bool root = ::getuid() == 0;
  uint64_t size = full_size;
#if ASIL_SHM_MODE == ASIL_SHM_POSIX
  if (Error e = CreateShadowFile(name, full_size); e != Error::kOk) {
    return e;
  }
  char shm_name[64];
  if (Error e = PosixShmName(name, shm_name); e != Error::kOk) {
    return e;
  }
  int fd = OpenShm(shm_name, O_RDWR | O_CREAT | O_EXCL);
  if (fd >= 0) {
    if (::ftruncate(fd, static_cast<off_t>(full_size)) != 0 ||
        ::chmod(name, 0777) != 0 ||
        (root && ::chown(name, static_cast<uid_t>(user_id),
                         static_cast<gid_t>(group_id)) != 0)) {
      (void)::close(fd);
      return Error::kSharedMemoryError;
    }
  } else if (errno == EEXIST) {
    fd = OpenShm(shm_name, O_RDWR);
    struct stat st;
    if (fd < 0 || ::stat(name, &st) != 0) {
      if (fd >= 0) {
        (void)::close(fd);
      }
      return Error::kSharedMemoryError;
    }
    size = static_cast<uint64_t>(st.st_size);
  } else {
    return Error::kSharedMemoryError;
  }
#elif ASIL_SHM_MODE == ASIL_SHM_LINUX
  int fd = OpenShm(name, O_RDWR | O_CREAT | O_EXCL);
  if (fd >= 0) {
    char path[kMaxChannelNameLength + 80];
    (void)std::snprintf(path, sizeof(path), "/dev/shm/%s", name);
    if (::ftruncate(fd, static_cast<off_t>(full_size)) != 0 ||
        ::chmod(path, 0777) != 0 ||
        (root && ::chown(path, static_cast<uid_t>(user_id),
                         static_cast<gid_t>(group_id)) != 0)) {
      (void)::close(fd);
      return Error::kSharedMemoryError;
    }
  } else if (errno == EEXIST) {
    fd = OpenShm(name, O_RDWR);
    if (fd < 0) {
      return Error::kSharedMemoryError;
    }
    size = 0;
    for (int attempt = 0; attempt < 100 && size == 0; attempt++) {
      struct stat st;
      if (::fstat(fd, &st) != 0) {
        (void)::close(fd);
        return Error::kSharedMemoryError;
      }
      size = static_cast<uint64_t>(st.st_size);
      if (size == 0) {
        SleepMillisecond();
      }
    }
  } else {
    return Error::kSharedMemoryError;
  }
#else
  (void)root;
  (void)size;
  (void)user_id;
  (void)group_id;
  // Memfd buffers are passed between clients by the server.
  return Error::kUnsupported;
#endif
#if ASIL_SHM_MODE != ASIL_SHM_MEMFD
  const Error e = MapBuffer(fd, size, PROT_READ | PROT_WRITE);
  (void)::close(fd);
  if (e != Error::kOk) {
    return e;
  }
  if (slot_size_ != slot_size) {
    Unmap();
    return Error::kLayoutMismatch;
  }
  bcb_->sizes[0].store(size, std::memory_order_relaxed);
  while (!ccb_->num_buffers.compare_exchange_strong(
      num_buffers, 1, std::memory_order_release, std::memory_order_relaxed)) {
    if (num_buffers > 1) {
      Unmap();
      return Error::kUnsupported;
    }
  }
  return Error::kOk;
#endif
}

Error ChannelMemory::AttachBuffer(const char *channel_name,
                                  uint64_t session_id) {
  if (ccb_ == nullptr) {
    return Error::kNotInitialized;
  }
  if (buffer_ != nullptr) {
    return Error::kOk;
  }
  const int num_buffers = ccb_->num_buffers.load(std::memory_order_acquire);
  if (num_buffers == 0) {
    return Error::kOk;
  }
  if (num_buffers > 1) {
    return Error::kUnsupported;
  }
  char name[kMaxChannelNameLength + 64];
  if (Error e = BufferName(channel_name, session_id, name); e != Error::kOk) {
    return e;
  }
#if ASIL_SHM_MODE == ASIL_SHM_POSIX
  char shm_name[64];
  if (Error e = PosixShmName(name, shm_name); e != Error::kOk) {
    return e;
  }
  const int fd = OpenShm(shm_name, O_RDONLY);
  struct stat st;
  if (fd < 0 || ::stat(name, &st) != 0) {
    if (fd >= 0) {
      (void)::close(fd);
    }
    return Error::kSharedMemoryError;
  }
#elif ASIL_SHM_MODE == ASIL_SHM_LINUX
  const int fd = OpenShm(name, O_RDONLY);
  struct stat st;
  if (fd < 0 || ::fstat(fd, &st) != 0) {
    if (fd >= 0) {
      (void)::close(fd);
    }
    return Error::kSharedMemoryError;
  }
#else
  return Error::kUnsupported;
#endif
#if ASIL_SHM_MODE != ASIL_SHM_MEMFD
  Error e = Error::kOk;
  if (st.st_size > 0) {
    e = MapBuffer(fd, static_cast<uint64_t>(st.st_size), PROT_READ);
  }
  (void)::close(fd);
  return e;
#endif
}

shm::MessageSlot *ChannelMemory::Slot(int id) const {
  auto *slots = reinterpret_cast<shm::MessageSlot *>(
      reinterpret_cast<char *>(ccb_) + sizeof(shm::ChannelControlBlock));
  return &slots[id];
}

shm::MessagePrefix *ChannelMemory::Prefix(const shm::MessageSlot *slot) const {
  const int64_t stride = prefix_size_ + shm::Aligned64(slot_size_);
  return reinterpret_cast<shm::MessagePrefix *>(buffer_ + stride * slot->id);
}

char *ChannelMemory::Payload(const shm::MessageSlot *slot) const {
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

int ChannelMemory::NumSubscribers(int vchan_id) const {
  const shm::SubscriberCounter &counter = ccb_->num_subs;
  for (;;) {
    const uint64_t before = counter.sequence.load(std::memory_order_acquire);
    if ((before & 1) != 0) {
      // A server died while updating the count.
      return shm::kMaxSlotOwners;
    }
    const int mux_count = counter.counts[0].load(std::memory_order_relaxed);
    const int count =
        vchan_id == -1
            ? mux_count
            : mux_count +
                  counter.counts[vchan_id + 1].load(std::memory_order_relaxed);
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
  return mux_generation + ccb_->subscriber_cleanup_generation[vchan_id + 1].load(
                              std::memory_order_acquire);
}

bool ChannelMemory::AtomicIncRefCount(shm::MessageSlot *slot, int inc,
                                      uint64_t ordinal, int vchan_id,
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
    const uint64_t ref_ordinal = (ref >> shm::kOrdinalShift) & shm::kOrdinalMask;
    const int ref_vchan_id = shm::RefsVchanId(ref);
    if (ref_ordinal != 0 && ordinal != 0 &&
        (ref_ordinal != ordinal || ref_vchan_id != vchan_id)) {
      return false;
    }
    uint64_t refs = ref & shm::kRefCountMask;
    const uint64_t reliable_refs =
        (ref >> shm::kReliableRefCountShift) & shm::kRefCountMask;
    if (inc < 0 && refs == 0) {
      return true;
    }
    refs = static_cast<uint64_t>(static_cast<int64_t>(refs) + inc);
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
          retired_refs >=
              static_cast<uint64_t>(NumSubscribers(ref_vchan_id)) &&
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
