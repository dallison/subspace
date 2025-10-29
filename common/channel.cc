// Copyright 2025 David Allison
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
#include <sys/stat.h>
#endif
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_replace.h"
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
                 std::string type, std::function<bool(Channel *)> reload)
    : name_(name), num_slots_(num_slots), channel_id_(channel_id),
      type_(std::move(type)), reload_callback_(std::move(reload)) {}

void Channel::Unmap() {
  if (scb_ == nullptr) {
    // Not yet mapped.
    return;
  }
  UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
  UnmapMemory(ccb_, CcbSize(num_slots_), "CCB");
  UnmapMemory(bcb_, sizeof(BufferControlBlock), "BCB");
}

std::string Channel::BufferSharedMemoryName(uint64_t session_id,
                                            int buffer_index) const {
  std::string sanitized_name =
      absl::StrReplaceAll(ResolvedName(), {{"/", "."}});

#if defined(__APPLE__)
  // Since you can't actually see any shared memory names in the MacOS
  // filesystem we need to use /tmp to create a shadow file that is mapped to a
  // shared memory name.
  return absl::StrFormat("/tmp/subspace_%d_%s_%d", session_id, sanitized_name,
                         buffer_index);
#else
  return absl::StrFormat("subspace_%d_%s_%d", session_id, sanitized_name,
                         buffer_index);
#endif
}

std::string DecodedRefsBitField(uint64_t refs) {
  std::string result;
  int ref_count = refs & kRefCountMask;
  int reliable_ref_count = (refs >> kReliableRefCountShift) & kRefCountMask;
  int retired_refs = (refs >> kRetiredRefsShift) & kRetiredRefsMask;
  int vchan_id = (refs >> kVchanIdShift) & kVchanIdMask;
  uint64_t ordinal = (refs >> kOrdinalShift) & kOrdinalMask;

  absl::StrAppendFormat(&result,
                        "ref_count: %d, reliable_ref_count: %d, "
                        "retired_refs: %d, vchan_id: %d, ordinal: %d",
                        ref_count, reliable_ref_count, retired_refs, vchan_id,
                        ordinal);
  return result;
}

bool Channel::AtomicIncRefCount(MessageSlot *slot, bool reliable, int inc,
                                uint64_t ordinal, int vchan_id, bool retire,
                                std::function<void()> retire_callback) {
  for (;;) {
    uint64_t ref = slot->refs.load(std::memory_order_relaxed);
    if ((ref & kPubOwned) != 0) {
      return false;
    }
    ordinal &= kOrdinalMask;
    // Not pub owned.  If the ordinal has changed we can't get the slot because
    // another message has been published into it.  We need to try another slot.
    // An ordinal of 0 in the refs field means that the slot was free and we can
    // keep trying to use it.
    uint64_t ref_ord = (ref >> kOrdinalShift) & kOrdinalMask;
    if (ref_ord != 0 && ordinal != 0 && ref_ord != ordinal) {
      return false;
    }
    int ref_vchan_id = (ref >> kVchanIdShift) & kVchanIdMask;
    if (ref_vchan_id == (1 << kVchanIdSize) - 1) {
      // This is a special case where the vchan_id is invalid.
      ref_vchan_id = -1;
    }
    if (ref_ord != 0 && ordinal != 0 && ref_vchan_id != vchan_id) {
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
    int retired_refs = (ref >> kRetiredRefsShift) & kRetiredRefsMask;
    if (retire) {
      retired_refs++;
    }
    uint64_t new_ref = BuildRefsBitField(ref_ord, ref_vchan_id, retired_refs) |
                       (new_reliable_refs << kReliableRefCountShift) | new_refs;
    if (new_ref == -1ULL) {
      abort();
    }
    if (slot->refs.compare_exchange_weak(ref, new_ref,
                                         std::memory_order_relaxed)) {
      // std::cerr << slot->id << " retire: " << retire
      //           << " retired_refs: " << retired_refs
      //           << " num subs: " << NumSubscribers(vchan_id)
      //           << " new_refs: " << new_refs
      //           << " new_reliable_refs: " << new_reliable_refs << std::endl;
      if (new_refs == 0 && new_reliable_refs == 0 &&
          retired_refs >= NumSubscribers(ref_vchan_id)) {
        // All subscribers have seen the slot, retire it.
        RetiredSlots().Set(slot->id);
        if (retire_callback) {
          // std::cerr << "Calling retire callback for slot " << slot->id
          //           << std::endl;
          retire_callback();
        }
      }
      return true;
    }
    // Another subscriber got there before us.  Try again.
    // Could also be a publisher, in which case the kPubOwned bit will be set.
  }
}

void MessageSlot::Dump(std::ostream &os) const {
  uint64_t l_refs = refs.load(std::memory_order_relaxed);
  int reliable_refs = (l_refs >> kReliableRefCountShift) & kRefCountMask;
  bool is_pub = (l_refs & kPubOwned) != 0;
  uint64_t just_refs = l_refs & kRefCountMask;
  uint64_t ref_ord = (l_refs >> kOrdinalShift) & kOrdinalMask;

  os << this << " Slot: " << id;
  if (is_pub) {
    os << " publisher " << just_refs;
  } else {
    os << " refs: " << just_refs << " reliable refs: " << reliable_refs
       << " ord: " << ref_ord;
  }
  os << " ordinal: " << ordinal << " buffer_index: " << buffer_index
     << " vchan_id: " << vchan_id << " timestamp: " << timestamp
     << " message size: " << message_size << " raw refs: " << std::hex << refs
     << " flags: " << flags << std::dec << "\n";
}

void Channel::DumpSlots(std::ostream &os) const {
  for (int i = 0; i < num_slots_; i++) {
    const MessageSlot *slot = &ccb_->slots[i];
    slot->Dump(os);
  }
  os << "Retired slots: ";
  RetiredSlots().Print(os);
}

void Channel::Dump(std::ostream &os) const {
  os << "SCB:\n";
  toolbelt::Hexdump(scb_, 64);

  os << "CCB:\n";
  toolbelt::Hexdump(ccb_, CcbSize(num_slots_));

  os << "Slots:\n";
  DumpSlots(os);
}

void Channel::DecrementBufferRefs(int buffer_index) {
  assert(bcb_->refs[buffer_index] > 0);
  bcb_->refs[buffer_index]--;
  if (debug_) {
    printf("Decremented buffers refs for buffer %d to %d\n", buffer_index,
           bcb_->refs[buffer_index].load());
  }
}

void Channel::IncrementBufferRefs(int buffer_index) {
  bcb_->refs[buffer_index]++;
  if (debug_) {
    printf("Incremented buffers refs for buffer %d to %d\n", buffer_index,
           bcb_->refs[buffer_index].load());
  }
}

void Channel::GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages,
                               uint32_t &max_message_size,
                               uint32_t &total_drops) {
  total_bytes = ccb_->total_bytes;
  total_messages = ccb_->total_messages;
  max_message_size = ccb_->max_message_size;
  total_drops = ccb_->total_drops;
}

uint64_t Channel::GetVirtualMemoryUsage() const {
  uint64_t size = sizeof(SystemControlBlock) + CcbSize(num_slots_) +
                  sizeof(BufferControlBlock);
  for (int i = 0; i < ccb_->num_buffers; i++) {
    if (bcb_->refs[i] > 0) {
      size += bcb_->sizes[i];
    }
  }
  return size;
}

void Channel::CleanupSlots(int owner, bool reliable, bool is_pub,
                           int vchan_id) {
  if (is_pub) {
    // Look for a slot with kPubOwned set and clear it.
    for (int i = 0; i < NumSlots(); i++) {
      MessageSlot *slot = &ccb_->slots[i];
      uint64_t refs = slot->refs.load(std::memory_order_relaxed);
      // Is the slot owned by this publisher?
      if (refs == (kPubOwned | uint64_t(owner))) {
        // Owned by this publisher, clear slot.
        slot->ordinal = 0;
        slot->refs =
            0; // Sequentially consistent because we've changed the ordinal too.

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
    ccb_->num_subs.RemoveSubscriber(vchan_id);

    // Go through all the slots and remove the owner from the owners bitset.
    for (int i = 0; i < NumSlots(); i++) {
      MessageSlot *slot = &ccb_->slots[i];
      if (slot->sub_owners.IsSet(owner)) {
        slot->sub_owners.Clear(owner);
        AtomicIncRefCount(slot, reliable, -1, 0, 0, true);
      }
    }
  }
}

#if defined(__APPLE__)
absl::StatusOr<std::string>
Channel::MacOsSharedMemoryName(const std::string &shadow_file) {
  struct stat st;
  int e = ::stat(shadow_file.c_str(), &st);
  if (e == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to determine MacOS shm name for %s: %s",
                        shadow_file, strerror(errno)));
  }
  // Use the inode number (unique per file) to make the shm file name.
  return absl::StrFormat("subspace_%d", st.st_ino);
}
#endif

} // namespace subspace
