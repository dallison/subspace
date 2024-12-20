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

bool Channel::AtomicIncRefCount(MessageSlot *slot, bool reliable, int inc, uint64_t ordinal) {
  for (;;) {
    uint64_t ref = slot->refs.load(std::memory_order_relaxed);
    if ((ref & kPubOwned) != 0) {
      return false;
    }
    // Not pub owned.  If the ordinal has changed we can't get the slot because another
    // message has been published into it.  We need to try another slot.  An ordinal of 0
    // in the refs field means that the slot was free and we can keep trying to use it.
    uint64_t ref_ord = (ref >> kOrdinalShift) & kOrdinalMask;
    if (ref_ord != 0 && ordinal != 0 && ref_ord != ordinal) {
      return false;
    }
    uint64_t new_refs = ref & kRefCountMask;
    uint64_t new_reliable_refs =
        (ref >> kReliableRefCountShift) & kRefCountMask;
    new_refs += inc;
    if (reliable) {
      new_reliable_refs += inc;
    }
    ref &= ~kPubOwned;
    uint64_t new_ref = (ordinal << kOrdinalShift) | (new_reliable_refs << kReliableRefCountShift) | new_refs;
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
    uint64_t refs = slot->refs.load(std::memory_order_relaxed);
    int reliable_refs = (refs >> kReliableRefCountShift) & kRefCountMask;
    bool is_pub = (refs & kPubOwned) != 0;
    uint64_t just_refs = refs & kRefCountMask;
    uint64_t ref_ord = (refs >> kOrdinalShift) & kOrdinalMask;

    std::cout << "Slot: " << i;
    if (is_pub) {
      std::cout << " publisher " << just_refs;
    } else {
      std::cout << " refs: " << just_refs << " reliable refs: " << reliable_refs << " ord: " << ref_ord;
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

void Channel::ReloadIfNecessary(const std::function<bool()>& reload) {
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
        AtomicIncRefCount(slot, reliable, -1, 0);
      }
    }
  }
}


} // namespace subspace
