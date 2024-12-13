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
#include <mutex>
#include <unistd.h>

namespace subspace {

// Set this to 1 to print the memory mapping and unmapping calls.
#define SHOW_MMAPS 0

// Set this to 1 to debug calls to map and unmap memory.  This
// is only valid when NDEBUG is not defined (in debug mode).
#define DEBUG_MMAPS 0

#if !NDEBUG && DEBUG_MMAPS
// NOTE: due to C++'s undefined initialization and destruction order
// these can't be actual instances.  They will be allocated on the
// first call to MapMemory and won't be deleted.
static absl::flat_hash_map<void *, size_t> *mapped_regions;
static std::mutex *region_lock;
#endif

void *MapMemory(int fd, size_t size, int prot, const char *purpose) {
  void *p = mmap(NULL, size, prot, MAP_SHARED, fd, 0);
#if SHOW_MMAPS
  printf("%d: mapping %s with size %zd: %p -> %p\n", getpid(), purpose, size, p,
         reinterpret_cast<char *>(p) + size);
#endif
#if !NDEBUG && DEBUG_MMAPS
  if (region_lock == nullptr) {
    region_lock = new std::mutex;
    mapped_regions = new absl::flat_hash_map<void *, size_t>;
  }
  std::unique_lock l(*region_lock);
  if (mapped_regions->find(p) != mapped_regions->end()) {
    fprintf(stderr, "Attempting to remap region at %p with size %zd\n", p,
            size);
  }
  (*mapped_regions)[p] = size;
#endif
  return p;
}

void UnmapMemory(void *p, size_t size, const char *purpose) {
#if SHOW_MMAPS
  printf("%d: unmapping %s with size %zd: %p -> %p\n", getpid(), purpose, size,
         p, reinterpret_cast<char *>(p) + size);
#endif
#if !NDEBUG && DEBUG_MMAPS
  assert(region_lock != nullptr);
  std::unique_lock l(*region_lock);

  auto it = mapped_regions->find(p);
  if (it == mapped_regions->end()) {
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
#if !NDEBUG && DEBUG_MMAPS

  mapped_regions->erase(p);
#endif
}

Channel::Channel(const std::string &name, int num_slots, int channel_id,
                 std::string type)
    : name_(name), num_slots_(num_slots), channel_id_(channel_id),
      type_(std::move(type)) {}

void Channel::Unmap() {
  if (scb_ == nullptr) {
    // Not yet mapped.
    return;
  }
  UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");

  for (auto &buffer : buffers_) {
    int64_t buffers_size =
        sizeof(BufferHeader) +
        num_slots_ * (Aligned<32>(buffer.slot_size) + sizeof(MessagePrefix));
    if (buffers_size > 0 && buffer.buffer != nullptr) {
      UnmapMemory(buffer.buffer, buffers_size, "buffers");
    }
  }
  buffers_.clear();

  UnmapMemory(ccb_, CcbSize(num_slots_), "CCB");
}

bool Channel::AtomicIncRefCount(MessageSlot *slot, bool reliable, int inc) {
  for (;;) {
    uint32_t ref = slot->refs.load(std::memory_order_relaxed);
    if ((ref & kPubOwned) != 0) {
      return false;
    }
    uint32_t new_refs = ref & kRefCountMask;
    uint32_t new_reliable_refs =
        (ref >> kReliableRefCountShift) & kRefCountMask;
    new_refs += inc;
    if (reliable) {
      new_reliable_refs += inc;
    }
    uint32_t new_ref = (new_reliable_refs << kReliableRefCountShift) | new_refs;
    if (slot->refs.compare_exchange_weak(ref, new_ref,
                                         std::memory_order_relaxed)) {
      return true;
    }
    // Another subscriber got there before us.  Try again.
    // Could also be a publisher, in which case the kPubOwned bit will be set.
  }
}

void Channel::DumpSlots() const {
  for (int i = 0; i < num_slots_; i++) {
    const MessageSlot *slot = &ccb_->slots[i];
    uint32_t refs = slot->refs.load(std::memory_order_relaxed);
    int reliable_refs = (refs >> kReliableRefCountShift) & kRefCountMask;
    bool is_pub = (refs & kPubOwned) != 0;
    refs &= kRefCountMask;
    std::cout << "Slot: " << i;
    if (is_pub) {
      std::cout << " publisher " << refs;
    } else {
      std::cout << " refs: " << refs << " reliable refs: " << reliable_refs;
    }
    std::cout << " ordinal: " << slot->ordinal
              << " buffer_index: " << slot->buffer_index
              << " message size: " << slot->message_size << std::endl;
  }
}

void Channel::Dump() const {
  printf("SCB:\n");
  toolbelt::Hexdump(scb_, 64);

  printf("CCB:\n");
  toolbelt::Hexdump(ccb_, CcbSize(num_slots_));

  printf("Slots:\n");
  DumpSlots();

  printf("Buffers:\n");
  int index = 0;
  for (auto &buffer : buffers_) {
    printf("  (%d) %d: %p\n", index++, buffer.slot_size, buffer.buffer);
  }
}

void Channel::DecrementBufferRefs(int buffer_index) {
  BufferHeader *hdr =
      reinterpret_cast<BufferHeader *>(buffers_[buffer_index].buffer +
                                       sizeof(BufferHeader)) -
      1;
  assert(hdr->refs > 0);
  hdr->refs--;
  if (debug_) {
    printf("Decremented buffers refs for buffer %d to %d\n", buffer_index,
           hdr->refs);
  }
}

void Channel::IncrementBufferRefs(int buffer_index) {
  BufferHeader *hdr =
      reinterpret_cast<BufferHeader *>(buffers_[buffer_index].buffer +
                                       sizeof(BufferHeader)) -
      1;
  hdr->refs++;
  if (debug_) {
    printf("Incremented buffers refs for buffer %d to %d\n", buffer_index,
           hdr->refs);
  }
}

void Channel::GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages, uint32_t& max_message_size, uint32_t& total_drops) {
  total_bytes = ccb_->total_bytes;
  total_messages = ccb_->total_messages;
  max_message_size = ccb_->max_message_size;
  total_drops = ccb_->total_drops;
}

void Channel::ReloadIfNecessary(std::function<bool()> reload) {
  if (reload == nullptr) {
    return;
  }
  do {
  } while (reload());
}

void Channel::CleanupSlots(int owner, bool reliable, bool is_pub) {
  if (is_pub) {
    // Look for a slot with kPubOwned set and clear it.
    for (int i = 0; i < NumSlots(); i++) {
      MessageSlot *slot = &ccb_->slots[i];
      uint32_t refs = slot->refs.load(std::memory_order_relaxed);
      if ((refs & kPubOwned) != 0) {
        slot->refs.store(0, std::memory_order_relaxed);
        // Clear the slot in all the subscriber bitsets.
        ccb_->subscribers.Traverse([this, slot](int sub_id) {
          GetAvailableSlots(sub_id).Clear(slot->id);
        });
        return;
      }
    }
  } else {
    // Subscriber.
    // Remove the subscriber from the subscriber bitset.
    ccb_->subscribers.Clear(owner);
    // Go through all the slots and remove the owner from the owners bitset.
    for (int i = 0; i < NumSlots(); i++) {
      MessageSlot *slot = &ccb_->slots[i];
      if (slot->sub_owners.IsSet(owner)) {
        slot->sub_owners.Clear(owner);
        AtomicIncRefCount(slot, reliable, -1);
      }
    }
  }
}

MessageSlot *Channel::FindActiveSlotByTimestamp(
    MessageSlot *old_slot, uint64_t timestamp, bool reliable, int owner,
    std::vector<MessageSlot *> &buffer, std::function<bool()> reload) {
#if 0
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
#else
  return nullptr;
#endif
}

} // namespace subspace
