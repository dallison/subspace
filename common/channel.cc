// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/channel.h"
#include "absl/strings/str_format.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/mutex.h"
#include <fcntl.h>
#include <sys/mman.h>
#if defined(__APPLE__)
#include <sys/posix_shm.h>
#endif
#include "absl/container/flat_hash_map.h"
#include <cassert>
#include <inttypes.h>
#include <unistd.h>

namespace subspace {

#ifndef NDEBUG
// NOTE: this is not thread safe.  If you want to use multiple
// threads, you will have to add a mutex to access this map.
static absl::flat_hash_map<void *, size_t> mapped_regions;
#endif

// Set this to 1 to print the memory mapping and unmapping calls.
#define SHOW_MMAPS 0

static void *MapMemory(int fd, size_t size, int prot, const char *purpose) {
  void *p = mmap(NULL, size, prot, MAP_SHARED, fd, 0);
#if SHOW_MMAPS
  printf("mapping %s with size %zd: %p -> %p\n", purpose, size, p,
         reinterpret_cast<char *>(p) + size);
#endif
#ifndef NDEBUG
  if (mapped_regions.find(p) != mapped_regions.end()) {
    fprintf(stderr, "Attempting to remap region at %p with size %zd\n", p,
            size);
  }
  mapped_regions[p] = size;
#endif
  return p;
}

static void UnmapMemory(void *p, size_t size, const char *purpose) {
#if SHOW_MMAPS
  printf("unmapping %s with size %zd: %p -> %p\n", purpose, size, p,
         reinterpret_cast<char *>(p) + size);
#endif
#ifndef NDEBUG
  auto it = mapped_regions.find(p);
  if (it == mapped_regions.end()) {
    fprintf(stderr,
            "Attempting to unmap unknown region %s at %p with size %zd\n",
            purpose, p, size);
    return;
  } else if (it->second != size) {
    fprintf(stderr,
            "Attempting to unmap region %s at %p with wrong size %zd/%zd\n",
            purpose, p, it->second, size);
    return;
  }
#endif
  munmap(p, size);
#ifndef NDEBUG

  mapped_regions.erase(p);
#endif
}

static absl::StatusOr<void *> CreateSharedMemory(int id, const char *suffix,
                                                 int64_t size, bool map,
                                                 toolbelt::FileDescriptor &fd) {
  char shm_name[NAME_MAX];
  int pid = getpid();
  size_t len =
      snprintf(shm_name, sizeof(shm_name), "/%d.%s.%d", id, suffix, pid);
  // Can't have a / in a POSIX shared memory name, so we replace them
  // by an underscore.
  for (size_t i = 1; i < len; i++) {
    if (shm_name[i] == '/') {
      shm_name[i] = '_';
    }
  }
  // Remove any existing shared memory.
  shm_unlink(shm_name);

  // Open the shared memory file.
  int shm_fd = shm_open(shm_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (shm_fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open shared memory %s: %s", shm_name, strerror(errno)));
  }

  // Make it the appropriate size.
  int e = ftruncate(shm_fd, size);
  if (e == -1) {
    shm_unlink(shm_name);
    return absl::InternalError(
        absl::StrFormat("Failed to set length of shared memory %s: %s",
                        shm_name, strerror(errno)));
  }

  // Map it into memory if asked
  void *p = nullptr;
  if (map) {
    p = MapMemory(shm_fd, size, PROT_READ | PROT_WRITE, suffix);
    if (p == MAP_FAILED) {
      shm_unlink(shm_name);
      return absl::InternalError(absl::StrFormat(
          "Failed to map shared memory %s: %s", shm_name, strerror(errno)));
    }
  }

  // Don't need the file now.  It stays open and available to be mapped in
  // using the file descriptor.
  shm_unlink(shm_name);
  fd.SetFd(shm_fd);
  return p;
}

static void InitMutex(pthread_mutex_t &mutex) {
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_setpshared(&attr, 1);
#ifdef __linux__
  pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
#endif

  pthread_mutex_init(&mutex, &attr);
  pthread_mutexattr_destroy(&attr);
}

absl::StatusOr<SystemControlBlock *>
CreateSystemControlBlock(toolbelt::FileDescriptor &fd) {
  absl::StatusOr<void *> s = CreateSharedMemory(
      0, "scb", sizeof(SystemControlBlock), /*map=*/true, fd);
  if (!s.ok()) {
    return s.status();
  }
  SystemControlBlock *scb = reinterpret_cast<SystemControlBlock *>(*s);
  memset(&scb->counters, 0, sizeof(scb->counters));
  return scb;
}

Channel::Channel(const std::string &name, int num_slots, int channel_id,
                 std::string type)
    : name_(name), num_slots_(num_slots), channel_id_(channel_id),
      type_(std::move(type)) {}

absl::StatusOr<SharedMemoryFds>
Channel::Allocate(const toolbelt::FileDescriptor &scb_fd, int slot_size,
                  int num_slots) {
  // Unmap existing memory.
  Unmap();

  // If the channel is being remapped (a subscriber that existed
  // before the first publisher), num_slots_ will be zero and we
  // set it here now that we know it.  If num_slots_ was already
  // set we need to make sure that the value passed here is
  // the same as the current value.
  if (num_slots_ != 0) {
    assert(num_slots_ == num_slots);
  } else {
    num_slots_ = num_slots;
  }

  // We are allocating a channel, so we only have one buffer.
  buffers_.clear();

  // Map SCB into process memory.
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s", strerror(errno)));
  }

  SharedMemoryFds fds;

  // One buffer.  The fd will be set when the buffers are allocated in
  // shared memmory.
  fds.buffers.emplace_back(slot_size);

  // Create CCB in shared memory and map into process memory.
  int64_t ccb_size =
      sizeof(ChannelControlBlock) + sizeof(MessageSlot) * num_slots_;
  absl::StatusOr<void *> p =
      CreateSharedMemory(channel_id_, "scb", ccb_size, /*map=*/true, fds.ccb);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return p.status();
  }
  ccb_ = reinterpret_cast<ChannelControlBlock *>(*p);
  memset(ccb_, 0, ccb_size);

  // Create a single buffer but don't map it in.  There is no need to
  // map in the buffers in the server since they will never be used.
  int64_t buffers_size =
      num_slots_ * (Aligned<32>(slot_size) + sizeof(MessagePrefix));
  if (buffers_size == 0) {
    buffers_size = 256;
  }
  p = CreateSharedMemory(channel_id_, "buffers0", buffers_size,
                         /*map=*/false, fds.buffers[0].fd);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, ccb_size, "CCB");
    return p.status();
  }
  buffers_.emplace_back(slot_size, reinterpret_cast<char *>(*p));
  ccb_->num_buffers = 1;

  // Initialize the CCB.
  InitMutex(ccb_->lock);

  // Build CCB data.
  // Copy possibly truncated channel name into CCB for ease
  // of debugging (you can see it in all processes).
  strncpy(ccb_->channel_name, name_.c_str(), kMaxChannelName - 1);
  ccb_->num_slots = num_slots_;
  ccb_->next_ordinal = 1;

  ListInit(&ccb_->active_list);
  ListInit(&ccb_->busy_list);
  ListInit(&ccb_->free_list);

  // Initialize all slots and insert into the free list.
  for (int32_t i = 0; i < num_slots_; i++) {
    MessageSlot *slot = &ccb_->slots[i];
    ListElementInit(&slot->element);
    slot->id = i;
    slot->ref_count = 0;
    slot->reliable_ref_count = 0;
    slot->buffer_index = 0;
    slot->owners.Init();
    ListInsertAtEnd(&ccb_->free_list, &slot->element);
  }

  if (debug_) {
    printf("Channel allocated: scb: %p, ccb: %p, buffers: %p\n", scb_, ccb_,
           buffers_[0].buffer);
    Dump();
  }
  return fds;
}

void Channel::PrintList(const SlotList *list) {
  void *p = FromCCBOffset(list->first);
  while (p != FromCCBOffset(0)) {
    MessageSlot *slot = reinterpret_cast<MessageSlot *>(p);
    printf("%d(%d/%d) ", slot->id, slot->ref_count, slot->reliable_ref_count);
    p = FromCCBOffset(slot->element.next);
  }
  printf("\n");
}

void Channel::PrintLists() {
  printf("Free list: ");
  PrintList(&ccb_->free_list);
  printf("Active list: ");
  PrintList(&ccb_->active_list);
  printf("Busy list: ");
  PrintList(&ccb_->busy_list);
}

absl::Status Channel::Map(SharedMemoryFds fds,
                          const toolbelt::FileDescriptor &scb_fd) {
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s", strerror(errno)));
  }

  int64_t ccb_size =
      sizeof(ChannelControlBlock) + sizeof(MessageSlot) * num_slots_;
  ccb_ = reinterpret_cast<ChannelControlBlock *>(
      MapMemory(fds.ccb.Fd(), ccb_size, PROT_READ | PROT_WRITE, "CCB"));
  if (ccb_ == MAP_FAILED) {
    munmap(scb_, sizeof(SystemControlBlock));
    return absl::InternalError(absl::StrFormat(
        "Failed to map ChannelControlBlock: %s", strerror(errno)));
  }
  int index = 0;
  for (const auto &buffer : fds.buffers) {
    int64_t buffers_size =
        num_slots_ * (Aligned<32>(buffer.slot_size) + sizeof(MessagePrefix));
    if (buffers_size != 0) {
      char *mem = reinterpret_cast<char *>(
          MapMemory(fds.buffers[index].fd.Fd(), buffers_size,
                    PROT_READ | PROT_WRITE, "buffers"));

      if (mem == MAP_FAILED) {
        UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
        UnmapMemory(ccb_, ccb_size, "CCB");
        // Unmap any previously mapped buffers.
        for (int i = 0; i < index; i++) {
          int64_t buffers_size =
              num_slots_ *
              (Aligned<32>(buffers_[i].slot_size) + sizeof(MessagePrefix));
          if (buffers_size > 0 && buffers_[i].buffer != nullptr) {
            UnmapMemory(buffers_[i].buffer, buffers_size, "buffers");
          }
        }
        return absl::InternalError(absl::StrFormat(
            "Failed to map channel buffers: %s", strerror(errno)));
      }
      buffers_.emplace_back(buffer.slot_size, mem);
      index++;
    }
  }

  if (debug_) {
    printf("Channel mapped: scb: %p, ccb: %p\n", scb_, ccb_);
    Dump();
  }
  return absl::OkStatus();
}

void Channel::Unmap() {
  if (scb_ == nullptr) {
    // Not yet mapped.
    return;
  }
  UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");

  for (auto &buffer : buffers_) {
    int64_t buffers_size =
        num_slots_ * (Aligned<32>(buffer.slot_size) + sizeof(MessagePrefix));
    if (buffers_size > 0 && buffer.buffer != nullptr) {
      UnmapMemory(buffer.buffer, buffers_size, "buffers");
    }
  }

  int64_t ccb_size =
      sizeof(ChannelControlBlock) + sizeof(MessageSlot) * num_slots_;
  UnmapMemory(ccb_, ccb_size, "CCB");
}

// Called on server to extend the allocated buffers.
absl::StatusOr<toolbelt::FileDescriptor>
Channel::ExtendBuffers(int32_t new_slot_size) {
  toolbelt::MutexLock lock(&ccb_->lock);

  int64_t buffers_size =
      num_slots_ * (Aligned<32>(new_slot_size) + sizeof(MessagePrefix));

  char buffer_name[32];
  snprintf(buffer_name, sizeof(buffer_name), "buffers%d\n",
           ccb_->num_buffers - 1);
  toolbelt::FileDescriptor fd;
  // Create the shared memory for the buffer but don't map it in.  This is
  // in the server and it is not used here.  The result of a successful
  // creation will be nullptr.
  absl::StatusOr<void *> p = CreateSharedMemory(
      channel_id_, buffer_name, buffers_size, /*map=*/false, fd);

  if (!p.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to map memory for extension: %s",
                        p.status().ToString().c_str()));
  }
  buffers_.emplace_back(new_slot_size, reinterpret_cast<char *>(*p));
  ccb_->num_buffers++;
  return fd;
}

absl::Status Channel::MapNewBuffers(std::vector<SlotBuffer> buffers) {
  size_t start = buffers_.size();
  for (size_t i = start; i < buffers.size(); i++) {
    const SlotBuffer &buffer = buffers[i];

    int64_t buffers_size =
        num_slots_ * (Aligned<32>(buffer.slot_size) + sizeof(MessagePrefix));
    if (buffers_size != 0) {
      char *mem = reinterpret_cast<char *>(MapMemory(
          buffer.fd.Fd(), buffers_size, PROT_READ | PROT_WRITE, "new buffers"));

      if (mem == MAP_FAILED) {
        // Unmap any newly mapped buffers.
        for (size_t i = start; i < buffers_.size(); i++) {
          int64_t buffers_size =
              num_slots_ *
              (Aligned<32>(buffers_[i].slot_size) + sizeof(MessagePrefix));
          if (buffers_size > 0 && buffers_[i].buffer != nullptr) {
            munmap(buffers_[i].buffer, buffers_size);
          }
        }
        return absl::InternalError(absl::StrFormat(
            "Failed to map new channel buffers: %s", strerror(errno)));
      }
      buffers_.emplace_back(buffer.slot_size, mem);
    }
  }
  return absl::OkStatus();
}

void Channel::Dump() {
  printf("SCB:\n");
  toolbelt::Hexdump(scb_, 64);

  printf("CCB:\n");
  int64_t ccb_size =
      sizeof(ChannelControlBlock) + sizeof(MessageSlot) * num_slots_;
  toolbelt::Hexdump(ccb_, ccb_size);
  PrintLists();
  printf("Buffers:\n");
  for (auto &buffer : buffers_) {
    printf("  %d: %p\n", buffer.slot_size, buffer.buffer);
  }
}

void Channel::ClaimPublisherSlot(MessageSlot *slot, int owner, SlotList &list) {
  ListRemove(&list, &slot->element);
  AddToBusyList(slot);
  slot->owners.Set(owner);
  slot->buffer_index = buffers_.size() - 1; // Use biggest buffer.
}

void Channel::SetSlotToBiggestBuffer(MessageSlot *slot) {
  if (slot == nullptr) {
    return;
  }
  slot->buffer_index = buffers_.size() - 1; // Use biggest buffer.
}

MessageSlot *Channel::FindFreeSlotLocked(bool reliable, int owner) {
  // Check if there is a free slot and if so, take it.
  if (ccb_->free_list.first != 0) {
    MessageSlot *slot =
        reinterpret_cast<MessageSlot *>(FromCCBOffset(ccb_->free_list.first));
    ClaimPublisherSlot(slot, owner, ccb_->free_list);
    return slot;
  }

  // No free slot, search for first slot with no references in the
  // active list.
  // If reliable is set, don't go past a slot with a reliable_ref_count
  // or an activation message that hasn't been seen by a subscriber.
  void *p = FromCCBOffset(ccb_->active_list.first);
  while (p != FromCCBOffset(0)) {
    MessageSlot *slot = reinterpret_cast<MessageSlot *>(p);
    if (reliable && slot->reliable_ref_count != 0) {
      // Don't go past slot with reliable reference.
      return nullptr;
    }
    MessagePrefix *prefix = Prefix(slot);
    if ((prefix->flags & (kMessageActivate | kMessageSeen)) ==
        kMessageActivate) {
      // An activation message that hasn't been seen.
      return nullptr;
    }
    if (slot->ref_count == 0) {
      prefix->flags = 0;
      ClaimPublisherSlot(slot, owner, ccb_->active_list);
      return slot;
    }
    p = FromCCBOffset(slot->element.next);
  }
  return nullptr;
}

MessageSlot *Channel::FindFreeSlot(bool reliable, int owner) {
  toolbelt::MutexLock lock(&ccb_->lock);
  return FindFreeSlotLocked(reliable, owner);
}

void Channel::GetStatsCounters(int64_t &total_bytes, int64_t &total_messages) {
  toolbelt::MutexLock lock(&ccb_->lock);
  total_bytes = ccb_->total_bytes;
  total_messages = ccb_->total_messages;
}

Channel::PublishedMessage
Channel::ActivateSlotAndGetAnother(MessageSlot *slot, bool reliable,
                                   bool is_activation, int owner,
                                   bool omit_prefix, bool *notify) {
  toolbelt::MutexLock lock(&ccb_->lock);

  // Move slot from busy list to active list.
  ListRemove(&ccb_->busy_list, &slot->element);
  slot->owners.Clear(owner);
  AddToActiveList(slot);

  // If the previously last element in the active list has been seen by a
  // subscriber we need to notify the subscribers that we've added a new
  // message.  If hasn't been seen, we've already notified the subscribers when
  // we added the slot to the active list.
  MessageSlot *prev =
      reinterpret_cast<MessageSlot *>(FromCCBOffset(slot->element.prev));
  if (notify != nullptr) {
    if (prev == FromCCBOffset(0) || (Prefix(prev)->flags & kMessageSeen) != 0) {
      *notify = true;
    }
  }
  void *buffer = GetBufferAddress(slot);
  MessagePrefix *prefix = reinterpret_cast<MessagePrefix *>(buffer) - 1;

  // Copy message parameters into message prefix in buffer.
  if (omit_prefix) {
    slot->ordinal = prefix->ordinal; // Copy ordinal from prefix.
  } else {
    slot->ordinal = ccb_->next_ordinal++;
    prefix->message_size = slot->message_size;
    prefix->ordinal = slot->ordinal;
    prefix->timestamp = toolbelt::Now();
    prefix->flags = 0;
    if (is_activation) {
      prefix->flags |= kMessageActivate;
    }
  }

  // Update counters.
  ccb_->total_messages++;
  ccb_->total_bytes += slot->message_size;

  // A reliable publisher doesn't allocate a slot until it is asked for.
  if (reliable) {
    return {nullptr, prefix->ordinal, prefix->timestamp};
  }
  // Find a new slot.
  return {FindFreeSlotLocked(reliable, owner), prefix->ordinal,
          prefix->timestamp};
}

inline void IncDecRefCount(MessageSlot *slot, bool reliable, int inc) {
  slot->ref_count += inc;
  if (reliable) {
    slot->reliable_ref_count += inc;
  }
}

void Channel::CleanupSlots(int owner, bool reliable) {
  toolbelt::MutexLock lock(&ccb_->lock);
  // Clean up active list.  Remove references for any slot owned by the
  // owner.
  void *p = FromCCBOffset(ccb_->active_list.first);
  while (p != FromCCBOffset(0)) {
    MessageSlot *slot = reinterpret_cast<MessageSlot *>(p);
    if (slot->owners.IsSet(owner)) {
      slot->owners.Clear(owner);
      IncDecRefCount(slot, reliable, -1);
    }

    p = FromCCBOffset(slot->element.next);
  }

  // Remove any publishers from the busy list.
  p = FromCCBOffset(ccb_->busy_list.first);
  while (p != FromCCBOffset(0)) {
    MessageSlot *slot = reinterpret_cast<MessageSlot *>(p);
    p = FromCCBOffset(slot->element.next);

    if (slot->owners.IsSet(owner)) {
      slot->owners.Clear(owner);
      // Move the slot to the free list.
      ListRemove(&ccb_->busy_list, &slot->element);
      ListInsertAtEnd(&ccb_->free_list, &slot->element);
    }
  }
}

MessageSlot *Channel::NextSlot(MessageSlot *slot, bool reliable, int owner) {
  toolbelt::MutexLock lock(&ccb_->lock);
  if (slot == nullptr) {
    // No current slot, first in list.
    if (ccb_->active_list.first == 0) {
      return nullptr;
    }
    // Take first slot in active list.
    slot =
        reinterpret_cast<MessageSlot *>(FromCCBOffset(ccb_->active_list.first));
    slot->owners.Set(owner);
    IncDecRefCount(slot, reliable, +1);
    Prefix(slot)->flags |= kMessageSeen;
    return slot;
  }
  if (slot->element.next == 0) {
    // No more active slots, keep current slot active.
    return nullptr;
  }
  // Going to move to another slot.  Decrement refs on current slot.
  IncDecRefCount(slot, reliable, -1);
  slot->owners.Clear(owner);

  slot = reinterpret_cast<MessageSlot *>(FromCCBOffset(slot->element.next));
  IncDecRefCount(slot, reliable, +1);
  Prefix(slot)->flags |= kMessageSeen;
  slot->owners.Set(owner);
  return slot;
}

MessageSlot *Channel::LastSlot(MessageSlot *slot, bool reliable, int owner) {
  toolbelt::MutexLock lock(&ccb_->lock);
  if (ccb_->active_list.last == 0) {
    return nullptr;
  }
  if (slot != nullptr) {
    IncDecRefCount(slot, reliable, -1);
    slot->owners.Clear(owner);
  }
  slot = reinterpret_cast<MessageSlot *>(FromCCBOffset(ccb_->active_list.last));
  IncDecRefCount(slot, reliable, +1);
  Prefix(slot)->flags |= kMessageSeen;
  slot->owners.Set(owner);
  return slot;
}

MessageSlot *
Channel::FindActiveSlotByTimestamp(MessageSlot *old_slot, uint64_t timestamp,
                                   bool reliable, int owner,
                                   std::vector<MessageSlot *> &buffer) {
  toolbelt::MutexLock lock(&ccb_->lock);

  // Copy pointers to active list slots into search buffer.  They are already
  // in timestamp order.
  buffer.clear();
  buffer.reserve(NumSlots());
  void *p = FromCCBOffset(ccb_->active_list.first);
  while (p != FromCCBOffset(0)) {
    MessageSlot *slot = reinterpret_cast<MessageSlot *>(p);
    buffer.push_back(slot);
    p = FromCCBOffset(slot->element.next);
  }
  // Apparently, lower_bound will return the first in the range if
  // the value is less than the whole range.  That's unexpected.
  if (buffer.empty() || timestamp < Prefix(buffer.front())->timestamp) {
    return nullptr;
  }

  // Binary search the search buffer.
  auto it = std::lower_bound(
      buffer.begin(), buffer.end(), timestamp,
      [this](MessageSlot *s, uint64_t t) { return Prefix(s)->timestamp < t; });
  if (it == buffer.end()) {
    // Not found, nothing changes.
    return nullptr;
  }
  if (old_slot != nullptr) {
    IncDecRefCount(old_slot, reliable, -1);
    old_slot->owners.Clear(owner);
  }
  MessageSlot *new_slot = *it;
  IncDecRefCount(new_slot, reliable, +1);
  Prefix(new_slot)->flags |= kMessageSeen;
  new_slot->owners.Set(owner);
  return new_slot;
}

} // namespace subspace
