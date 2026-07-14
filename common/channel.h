
// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xCOMMON_CHANNEL_H
#define _xCOMMON_CHANNEL_H

#include <stdio.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "common/atomic_bitset.h"
#include "toolbelt/bitset.h"
#include "toolbelt/fd.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <string>

namespace subspace {

// Shared-memory backends.  These name the *mechanism*, not the platform:
//   POSIX  - a named shm_open object plus a /tmp shadow file.
//   LINUX  - a named /dev/shm object (mapped by name, server-free).
//   MEMFD  - an anonymous memfd_create object (no name; the fd is passed to
//            other clients via the server).  This is a Linux mechanism that
//            Android must use because it has no /dev/shm.
#define SUBSPACE_SHMEM_MODE_POSIX 1
#define SUBSPACE_SHMEM_MODE_LINUX 2
#define SUBSPACE_SHMEM_MODE_MEMFD 3

// The backend is chosen at build time.  A build may either define
// SUBSPACE_SHMEM_MODE directly, or (on Linux) define SUBSPACE_LINUX_USE_MEMFD
// to opt the Linux build into the anonymous memfd backend instead of the
// default /dev/shm one.  See //:linux_memfd (Bazel) and the
// SUBSPACE_LINUX_USE_MEMFD CMake option.
#ifndef SUBSPACE_SHMEM_MODE
#if defined(__ANDROID__)
// Android has no /dev/shm, so it always uses anonymous memfd shared memory.
#define SUBSPACE_SHMEM_MODE SUBSPACE_SHMEM_MODE_MEMFD
#elif defined(__linux__)
#if defined(SUBSPACE_LINUX_USE_MEMFD)
// Opt-in: anonymous memfd shared memory on Linux (no /dev/shm files).
#define SUBSPACE_SHMEM_MODE SUBSPACE_SHMEM_MODE_MEMFD
#else
// Default on Linux: named /dev/shm shared memory.
#define SUBSPACE_SHMEM_MODE SUBSPACE_SHMEM_MODE_LINUX
#endif
#else
// On other systems, including QNX and macOS, use a /tmp shadow file and
// regular shared memory.
#define SUBSPACE_SHMEM_MODE SUBSPACE_SHMEM_MODE_POSIX
#endif
#endif

// Flag for flags field in MessagePrefix.
constexpr int kMessageActivate = 1;      // This is a reliable activation message.
constexpr int kMessageBridged = 2;       // This message came from the bridge.
constexpr int kMessageHasChecksum = 4;   // This message has a checksum.
constexpr int kMessageCrossMachine = 8;  // This is a cross-machine (tunnel) message.

// Maximum values for checksum_size and metadata_size (must fit in uint16_t).
constexpr int32_t kMaxChecksumSize = 0xFFFF;
constexpr int32_t kMaxMetadataSize = 0xFFFF;

// This is stored immediately before the channel buffer in shared
// memory.  It is transferred intact across the TCP bridges.
// 32 bytes long.
//
// Since this is used primarily for channel bridging, we
// include 4 bytes of padding at offset 0 so that that the
// Socket::SendMessage function has somewhere to put the
// length of the message and avoid 2 sends to the socket.
//
// Note that this precludes us mapping the subscriber's
// channel in read-only memory since the bridge will need
// to write to the padding address when it calls
// Socket::SendMessage.
//
// On the receiving end of the bridge, the padding is
// not received and will not be written to.
struct MessagePrefix {
  int32_t padding; // Padding for Socket::SendMessage.
  int32_t slot_id; // The slot ID this message is in.
  uint64_t message_size;
  uint64_t ordinal;
  uint64_t timestamp;
  int64_t flags;
  int32_t vchan_id;
  uint16_t checksum_size;
  uint16_t metadata_size;
  uint32_t checksum;
  char padding3[64 - 52]; // Align to 64 bytes.

  bool IsActivation() const { return (flags & kMessageActivate) != 0; }
  void SetIsActivation() { flags |= kMessageActivate; }
  bool IsBridged() const { return (flags & kMessageBridged) != 0; }
  void SetIsBridged() { flags |= kMessageBridged; }
  bool HasChecksum() const { return (flags & kMessageHasChecksum) != 0; }
  void SetHasChecksum() { flags |= kMessageHasChecksum; }
  bool IsCrossMachine() const { return (flags & kMessageCrossMachine) != 0; }
  void SetIsCrossMachine() { flags |= kMessageCrossMachine; }
};

static_assert(sizeof(MessagePrefix) == 64,
              "MessagePrefix size is not 64 bytes");

// Flags for MessageSlot flags.
constexpr int kMessageSeen = 1; // Message has been seen by any subscriber.
constexpr int kMessageIsActivation = 2; // This is an activation message.
constexpr int kMessageSeenByReliable =
    4; // Message has been seen by a reliable subscriber.

// We need a max channels number because the size of things in
// shared memory needs to be fixed.
constexpr int kMaxChannels = 1024;

// Maximum number of owners for a lot.  One per subscriber reference
// and publisher reference.  Best if it's a multiple of 64 because
// it's used as the size in a toolbelt::BitSet.
constexpr int kMaxSlotOwners = 1024;
// Default per-subscriber queue depth used whenever the publisher provisions a
// non-empty arena and the subscriber does not request an override.
constexpr int kDefaultSubscriberQueueSize = 16;
// Default packed arena size selected by publisher client APIs. This fits 100
// default-sized (16-entry) queues. Explicitly selecting zero keeps the
// available-slot bitset path and omits the queue arena.
constexpr uint64_t kDefaultSubscriberQueueArenaSize = 64'000;
constexpr size_t kDefaultMaxAvailableSlotQueueCapacity = 1024;
constexpr size_t kMaxSlotQueueCasAttempts = 64;
constexpr uint32_t kChannelControlBlockVersion = 4;
constexpr size_t kMaxChannelControlBlockSize = 1ULL << 30;

// This limits the number of virtual channels.  Each virtual channel
// needs its own ordinal counter in the CCB (8 bytes each).
// This is stored in the refs field as a 10-bit value.  If all bits are set
// this indicates -1 (no vchan_id).
constexpr int kMaxVchanId = 1023;

// Max length of a channel name in shared memory.  A name longer
// this this will be truncated but the full name will be available
// in process memory.
constexpr size_t kMaxChannelName = 64;

constexpr size_t kMaxBuffers = 1024;

// Ref count bits.
constexpr uint64_t kRefCountMask = 0x3ff;
constexpr uint64_t kRefCountShift = 10;
constexpr uint64_t kReliableRefCountShift = 10;
constexpr uint64_t kPubOwned = 1ULL << 63;
constexpr uint64_t kRefsMask = (1ULL << 20) - 1; // 20 bits
constexpr uint64_t kReliableRefsMask = ((1ULL << 10) - 1)
                                       << kReliableRefCountShift;
constexpr uint64_t kRetiredRefsSize = 10;
constexpr uint64_t kRetiredRefsMask = (1ULL << kRetiredRefsSize) - 1;
constexpr uint64_t kRetiredRefsShift = 20;

// Virtual channel ID is just above retired refs and is 10 bits long.
constexpr uint64_t kVchanIdShift = kRetiredRefsShift + kRetiredRefsSize;
constexpr uint64_t kVchanIdSize = 10;
constexpr uint64_t kVchanIdMask = (1ULL << kVchanIdSize) - 1;

// We put the bottom 23 bits of the ordinal just above the
// vchan ID field.  This is to ensure that a publisher
// hasn't published another message in the slot before a subscriber
// increments the ref count.
constexpr uint64_t kOrdinalSize = 23;
constexpr uint64_t kOrdinalMask = (1ULL << kOrdinalSize) - 1;
constexpr uint64_t kOrdinalShift = 40;

// Combine an ordinal and a vchan_id into a single 64 bit field, shifted
// to the correct position for the refs field.
inline uint64_t BuildRefsBitField(uint64_t ordinal, int vchan_id,
                                  int retired_refs) {
  return ((ordinal & kOrdinalMask) << kOrdinalShift) |
         (ordinal == 0 ? 0 : ((vchan_id & kVchanIdMask) << kVchanIdShift)) |
         ((retired_refs & kRetiredRefsMask) << kRetiredRefsShift);
}
std::string DecodedRefsBitField(uint64_t refs);

// Aligned to given power of 2.
template <int64_t alignment = 64> int64_t Aligned(int64_t v) {
  return (v + (alignment - 1)) & ~(alignment - 1);
}

void *MapMemory(int fd, size_t size, int prot, const char *purpose);
void UnmapMemory(void *p, size_t size, const char *purpose);

// This is a global (to a server) structure in shared memory that holds
// counts for the number of updates to publishers and subscribers on
// that server.  The server updates these counts when a publisher or
// subscriber is created or deleted.  The purpose is to allow a client
// to check if it needs to update its local information about the
// channel by contacting the server.  Things such as trigger file
// descriptors are distributed by the server to clients.
//
// This is in shared memory, but it is only ever written by the
// server.
struct ChannelCounters {
  uint16_t num_pub_updates;   // Number of updates to publishers.
  uint16_t num_sub_updates;   // Number of updates to subscribers.
  uint16_t num_pubs;          // Current number of publishers.
  uint16_t num_reliable_pubs; // Current number of reliable publishers.
  uint16_t num_subs;          // Current number of subscribers.
  uint16_t num_reliable_subs; // Current number of reliable subscribers.
  uint16_t num_resizes;       // Number of times channel has been resized.
};

struct SystemControlBlock {
  ChannelCounters counters[kMaxChannels];
};

// This is the meta data for a slot.
struct MessageSlot {
  std::atomic<uint64_t> refs; // Number of subscribers referring to this slot.
  std::atomic<uint64_t> ordinal;      // Message ordinal held currently in slot.
  std::atomic<uint64_t> message_size; // Size of message held in slot.
  int32_t id;                 // Unique ID for slot (0...num_slots-1).
  std::atomic<int16_t> buffer_index; // Index of buffer.
  std::atomic<int16_t> vchan_id;     // Virtual channel ID.
  AtomicBitSet<kMaxSlotOwners> sub_owners; // One bit per subscriber.
  std::atomic<uint64_t> timestamp;          // Timestamp of message.
  std::atomic<uint32_t> flags;
  std::atomic<int32_t>
      bridged_slot_id; // Slot ID of other side of bridge.

  void Dump(std::ostream &os) const;
};
static_assert(sizeof(MessageSlot) == 184);
static_assert(offsetof(MessageSlot, refs) == 0);
static_assert(offsetof(MessageSlot, ordinal) == 8);
static_assert(offsetof(MessageSlot, message_size) == 16);
static_assert(offsetof(MessageSlot, id) == 24);
static_assert(offsetof(MessageSlot, buffer_index) == 28);
static_assert(offsetof(MessageSlot, vchan_id) == 30);
static_assert(offsetof(MessageSlot, sub_owners) == 32);
static_assert(offsetof(MessageSlot, timestamp) == 168);
static_assert(offsetof(MessageSlot, flags) == 176);
static_assert(offsetof(MessageSlot, bridged_slot_id) == 180);

struct ActiveSlot {
  MessageSlot *slot;
  uint64_t ordinal;
  uint64_t timestamp;
  int vchan_id;
};

struct QueuedSlot {
  int32_t slot_id;
  uint64_t ordinal;
};

struct SlotQueueEntry {
  // Sequence number used to publish an entry after its payload is written and
  // to mark it reusable after the consumer has popped it.
  std::atomic<uint64_t> sequence;
  std::atomic<uint64_t> ordinal;
  std::atomic<int32_t> slot_id;
};
static_assert(sizeof(SlotQueueEntry) == 24);
static_assert(offsetof(SlotQueueEntry, sequence) == 0);
static_assert(offsetof(SlotQueueEntry, ordinal) == 8);
static_assert(offsetof(SlotQueueEntry, slot_id) == 16);

// A bounded MPSC queue stored in shared memory after the available-slots
// bitsets. Publishers push slot IDs as they publish; the single owning
// subscriber pops them to avoid scanning its bitset. For unreliable subscribers
// this queue is the hot-path source of truth; the bitset is still maintained for
// reliable mode and diagnostics while the queue path is proven out.
class InPlaceSlotQueue {
public:
  InPlaceSlotQueue(size_t capacity, bool drop_oldest = true) {
    Init(capacity, drop_oldest);
  }

  // Initialize queue metadata and mark every ring entry as free. `capacity`
  // is the number of SlotQueueEntry objects laid out immediately after this
  // header in shared memory.
  void Init(size_t capacity, bool drop_oldest = true) {
    capacity_ = capacity;
    head_.store(0, std::memory_order_relaxed);
    tail_.store(0, std::memory_order_relaxed);
    overflow_count_.store(0, std::memory_order_relaxed);
    insertion_failed_.store(false, std::memory_order_relaxed);
    drop_oldest_ = drop_oldest;
    for (size_t i = 0; i < capacity_; i++) {
      entries_[i].sequence.store(i, std::memory_order_relaxed);
      entries_[i].ordinal.store(0, std::memory_order_relaxed);
      entries_[i].slot_id.store(-1, std::memory_order_relaxed);
    }
  }

  size_t Capacity() const { return capacity_; }
  uint64_t Head() const { return head_.load(std::memory_order_acquire); }
  uint64_t Tail() const { return tail_.load(std::memory_order_acquire); }

  // Push a published slot. Multiple publishers may call this concurrently.
  // If an explicit queue is full, evict its oldest hint and enqueue the newest
  // one. An inherited queue instead rejects the hint so its subscriber recovers
  // every unread ordinal from the authoritative bitset.
  bool Push(int32_t slot_id, uint64_t ordinal,
            bool report_insertion_failure = true) {
    if (capacity_ == 0) {
      if (report_insertion_failure) {
        MarkInsertionFailure();
      }
      return false;
    }

    SlotQueueEntry *entry = nullptr;
    uint64_t tail = tail_.load(std::memory_order_relaxed);
    for (size_t attempt = 0; attempt < kMaxSlotQueueCasAttempts; ++attempt) {
      const uint64_t head = head_.load(std::memory_order_acquire);
      if (tail - head >= capacity_) {
        // Inherited queues preserve legacy no-drop delivery through the
        // authoritative bitset. Do not expose a newer queue entry before the
        // subscriber observes the fallback signal.
        if (!drop_oldest_) {
          if (report_insertion_failure) {
            MarkInsertionFailure();
          }
          return false;
        }
        if (!DropFront()) {
          if (report_insertion_failure) {
            MarkInsertionFailure();
          }
          return false;
        }
        overflow_count_.fetch_add(1, std::memory_order_release);
        tail = tail_.load(std::memory_order_relaxed);
        continue;
      }
      SlotQueueEntry &candidate = entries_[tail % capacity_];
      // A consumer publishes the reusable sequence after advancing head_. It
      // may be paused or terminated between those operations. Do not reserve
      // the entry until it is reusable: reserving first would force this
      // producer to wait indefinitely for that consumer.
      if (candidate.sequence.load(std::memory_order_acquire) != tail) {
        if (report_insertion_failure) {
          MarkInsertionFailure();
        }
        return false;
      }
      if (tail_.compare_exchange_strong(tail, tail + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
        entry = &candidate;
        break;
      }
    }
    if (entry == nullptr) {
      if (report_insertion_failure) {
        MarkInsertionFailure();
      }
      return false;
    }

    entry->slot_id.store(slot_id, std::memory_order_relaxed);
    entry->ordinal.store(ordinal, std::memory_order_relaxed);
    entry->sequence.store(tail + 1, std::memory_order_release);
    return true;
  }

  void MarkInsertionFailure() {
    insertion_failed_.store(true, std::memory_order_release);
  }

  // Read the oldest queued slot without consuming it. This lets poll-driven
  // subscribers stop at the end of a stable drain snapshot without losing the
  // first newer message.
  bool TryPeek(QueuedSlot &slot) {
    if (capacity_ == 0) {
      return false;
    }

    const uint64_t head = head_.load(std::memory_order_acquire);
    SlotQueueEntry &entry = entries_[head % capacity_];
    if (entry.sequence.load(std::memory_order_acquire) != head + 1) {
      return false;
    }

    QueuedSlot candidate = {
        entry.slot_id.load(std::memory_order_relaxed),
        entry.ordinal.load(std::memory_order_relaxed),
    };
    if (entry.sequence.load(std::memory_order_acquire) != head + 1) {
      return false;
    }
    slot = candidate;
    return true;
  }

  // Drop the oldest queued slot. Producers use this on overflow, and the
  // subscriber uses it to discard stale entries whose slot has been reused.
  bool DropFront() {
    if (capacity_ == 0) {
      return false;
    }

    uint64_t head = head_.load(std::memory_order_relaxed);
    for (size_t attempt = 0; attempt < kMaxSlotQueueCasAttempts; ++attempt) {
      SlotQueueEntry &entry = entries_[head % capacity_];
      if (entry.sequence.load(std::memory_order_acquire) != head + 1) {
        return false;
      }
      if (head_.compare_exchange_weak(head, head + 1,
                                      std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
        entry.sequence.store(head + capacity_, std::memory_order_release);
        return true;
      }
    }
    return false;
  }

  // Discard a bounded snapshot of queued hints. The available-slot bitset
  // remains authoritative, so subscribers use this after switching to bitset
  // recovery following queue overflow or insertion failure.
  void DiscardAll() {
    for (size_t i = 0; i < capacity_; ++i) {
      if (!DropFront()) {
        return;
      }
    }
  }

  // Pop one slot for the owning subscriber. There is exactly one consumer per
  // queue, but producers may advance head_ to evict on overflow, so the
  // consumer claims the front entry with a CAS.
  bool TryPop(QueuedSlot &slot) {
    if (capacity_ == 0) {
      return false;
    }

    uint64_t head = head_.load(std::memory_order_relaxed);
    for (size_t attempt = 0; attempt < kMaxSlotQueueCasAttempts; ++attempt) {
      SlotQueueEntry &entry = entries_[head % capacity_];
      if (entry.sequence.load(std::memory_order_acquire) != head + 1) {
        return false;
      }
      QueuedSlot candidate = {
          entry.slot_id.load(std::memory_order_relaxed),
          entry.ordinal.load(std::memory_order_relaxed),
      };
      if (head_.compare_exchange_weak(head, head + 1,
                                      std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
        slot = candidate;
        entry.sequence.store(head + capacity_, std::memory_order_release);
        return true;
      }
    }
    return false;
  }

  // Return and clear the number of older queued slots evicted to preserve the
  // newest data.
  uint32_t ConsumeOverflow() {
    return overflow_count_.exchange(0, std::memory_order_acq_rel);
  }

  uint32_t OverflowCount() const {
    return overflow_count_.load(std::memory_order_acquire);
  }

  bool ConsumeInsertionFailure() {
    return insertion_failed_.exchange(false, std::memory_order_acq_rel);
  }

  bool InsertionFailed() const {
    return insertion_failed_.load(std::memory_order_acquire);
  }

private:
  // Fixed ring capacity for this queue, capped independently of the channel's
  // slot count to keep shared-memory usage bounded.
  size_t capacity_ = 0;
  // Next sequence number the single subscriber will try to pop.
  std::atomic<uint64_t> head_{0};
  // Next sequence number producers will reserve for Push().
  std::atomic<uint64_t> tail_{0};
  std::atomic<uint32_t> overflow_count_{0};
  std::atomic<bool> insertion_failed_{false};
  // Explicit queues drop their oldest hint on overflow. Inherited queues leave
  // the queue unchanged and force the subscriber to recover from its bitset.
  bool drop_oldest_ = true;
  // Flexible array of `capacity_` entries stored immediately after the header.
  SlotQueueEntry entries_[0];
};
static_assert(sizeof(InPlaceSlotQueue) == 32);

inline size_t SizeofSlotQueue(size_t capacity) {
  return sizeof(InPlaceSlotQueue) + sizeof(SlotQueueEntry) * capacity;
}

constexpr uint64_t kInvalidSlotQueueOffset =
    std::numeric_limits<uint64_t>::max();

inline int ResolveSubscriberQueueSize(int num_slots,
                                      int subscriber_queue_size) {
  if (num_slots <= 0 || subscriber_queue_size <= 0) {
    return 0;
  }
  return subscriber_queue_size;
}

struct BufferControlBlock {
  std::atomic<int32_t>
      refs[kMaxBuffers]; // Number of references to this buffer.
  std::atomic<uint64_t>
      sizes[kMaxBuffers]; // Number of references to this buffer.
};

// Given a message prefix and a buffer containing the message data return three
// spans covering the data to be checksummed:
//   [0] prefix fields before the checksum (slot_id through padding2)
//   [1] user metadata area (immediately after the checksum storage)
//   [2] message payload
inline std::array<absl::Span<const uint8_t>, 3>
GetMessageChecksumData(MessagePrefix *prefix, void *buffer,
                       size_t message_size, int32_t checksum_size,
                       int32_t metadata_size) {
  const auto *base = reinterpret_cast<const uint8_t *>(prefix);
  std::array<absl::Span<const uint8_t>, 3> data = {
      absl::Span<const uint8_t>(base + offsetof(MessagePrefix, slot_id),
                                offsetof(MessagePrefix, checksum) -
                                    offsetof(MessagePrefix, slot_id)),
      absl::Span<const uint8_t>(
          base + offsetof(MessagePrefix, checksum) + checksum_size,
          metadata_size),
      absl::Span<const uint8_t>(reinterpret_cast<const uint8_t *>(buffer),
                                message_size)};
  return data;
}

// Get a span over the checksum storage area within the prefix.
inline absl::Span<std::byte> GetChecksumSpan(MessagePrefix *prefix,
                                             int32_t checksum_size) {
  return absl::Span<std::byte>(
      reinterpret_cast<std::byte *>(&prefix->checksum), checksum_size);
}

inline absl::Span<const std::byte>
GetChecksumSpan(const MessagePrefix *prefix, int32_t checksum_size) {
  return absl::Span<const std::byte>(
      reinterpret_cast<const std::byte *>(&prefix->checksum), checksum_size);
}

// Get a span over the user metadata area, which immediately follows
// the checksum area in the prefix extensions.
inline absl::Span<std::byte> GetMetadataSpan(MessagePrefix *prefix,
                                             int32_t checksum_size,
                                             int32_t metadata_size) {
  return absl::Span<std::byte>(
      reinterpret_cast<std::byte *>(&prefix->checksum) + checksum_size,
      metadata_size);
}

inline absl::Span<const std::byte>
GetMetadataSpan(const MessagePrefix *prefix, int32_t checksum_size,
                int32_t metadata_size) {
  return absl::Span<const std::byte>(
      reinterpret_cast<const std::byte *>(&prefix->checksum) + checksum_size,
      metadata_size);
}

// This counts the number of subscribers given a virtual channel id.
class SubscriberCounter {
public:
  void AddSubscriber(int vchan_id) { num_subs_[vchan_id + 1]++; }

  void RemoveSubscriber(int vchan_id) { num_subs_[vchan_id + 1]--; }

  // If vchan_id is valid we also count the number of subscribers to the
  // multiplexer itself.
  int NumSubscribers(int vchan_id) {
    int n = num_subs_[0];
    if (vchan_id == -1) {
      return n;
    }
    return n + num_subs_[vchan_id + 1];
  }

private:
  // Vchan ID -1 means invalid vchan ID so we just use element 0 for that.
  std::array<int, kMaxVchanId + 1> num_subs_ = {};
};

class OrdinalAccumulator {
public:
  void Init(int v) {
    for (int i = 0; i < kMaxVchanId + 1; i++) {
      ordinals_[i] = v;
    }
  }
  uint64_t Next(int vchan_id) { return ordinals_[vchan_id + 1]++; }

private:
  std::array<std::atomic<uint64_t>, kMaxVchanId + 1> ordinals_ = {};
};

class ActivationTracker {
public:
  void Activate(int vchan_id) { activations_.Set(vchan_id + 1); }
  bool IsActivated(int vchan_id) const {
    return activations_.IsSet(vchan_id + 1);
  }

private:
  AtomicBitSet<kMaxVchanId + 1> activations_; // One bit per vchan_id.
};

// The control data for a channel.  This memory is
// allocated by the server and mapped into the process
// for all publishers and subscribers.  Each mapped CCB is mapped
// at a virtual address chosen by the OS.
//
// This is in shared memory so no pointers are possible.
struct ChannelControlBlock {          // a.k.a CCB
  char channel_name[kMaxChannelName]; // So that you can see the name in a
                                      // debugger or hexdump.
  int num_slots;
  int subscriber_queue_size; // Fixed inherited per-subscriber queue capacity.
  uint32_t version;
  OrdinalAccumulator ordinals; // Ordinal accumulator for virtual channels.
  ActivationTracker activation_tracker; // Tracks which vchan_ids have been
                                        // activated by a publisher.
  int buffer_index;                     // Which buffer in buffers array to use.
  std::atomic<int> num_buffers; // Size of buffers array in shared memory.
  AtomicBitSet<kMaxSlotOwners> subscribers; // One bit per subscriber.

  // Given a subscriber ID, what is the vchan ID associated with it.
  std::array<int16_t, kMaxSlotOwners> sub_vchan_ids;

  SubscriberCounter num_subs;

  // Statistics counters.
  std::atomic<uint64_t> total_bytes;
  // Number of completed publications, including activation messages. This is
  // also the version stamp for subscriber delivery snapshots.
  std::atomic<uint64_t> total_messages;
  std::atomic<uint32_t> max_message_size;
  std::atomic<uint32_t> total_drops;

  // If true there are no more free slots and there's no need to check
  // the bitset for them (there will never be another free slot)
  std::atomic<bool> free_slots_exhausted;

  // Variable number of MessageSlot structs (num_slots long).
  MessageSlot slots[0];
  // Followed by:
  // AtomicBitSet<0> retiredSlots[num_slots];
  // Followed by:
  // AtomicBitSet<0> freeSlots[num_slots];
  // Followed by:
  // AtomicBitSet<0> availableSlots[kMaxSlotOwners];
  // Followed by:
  // AvailableSlotQueueIndex availableSlotQueueIndex;
  // Followed by:
  // A packed arena of variable-capacity InPlaceSlotQueue objects.
  //
};
static_assert(offsetof(ChannelControlBlock, version) == 72);

// Locates each subscriber's variable-capacity queue in the packed queue arena.
// Offsets are relative to the start of the arena.
struct AvailableSlotQueueIndex {
  std::atomic<uint64_t> next_offset;
  std::array<std::atomic<uint64_t>, kMaxSlotOwners> offsets;
  std::array<std::atomic<uint32_t>, kMaxSlotOwners> active_publishers;
};
static_assert(offsetof(AvailableSlotQueueIndex, next_offset) == 0);
static_assert(offsetof(AvailableSlotQueueIndex, offsets) == 8);
static_assert(offsetof(AvailableSlotQueueIndex, active_publishers) == 8200);
static_assert(sizeof(AvailableSlotQueueIndex) == 12296);

inline size_t AvailableSlotsSize(int num_slots) {
  return SizeofAtomicBitSet(num_slots) * kMaxSlotOwners;
}

inline size_t AvailableSlotQueueIndexSize() {
  return Aligned(sizeof(AvailableSlotQueueIndex));
}

enum class SlotQueueBlockState : uint32_t {
  kAllocated = 0,
  kRetired = 1,
  kFree = 2,
};

struct alignas(64) SlotQueueBlockHeader {
  uint64_t block_size = 0;
  std::atomic<uint32_t> state{
      static_cast<uint32_t>(SlotQueueBlockState::kFree)};
  uint32_t reserved = 0;
  AtomicBitSet<kMaxSlotOwners> waiting_publishers;
};
static_assert(sizeof(SlotQueueBlockHeader) == 192);
static_assert(offsetof(SlotQueueBlockHeader, waiting_publishers) == 16);

inline size_t SlotQueueBlockHeaderSize() {
  return Aligned(sizeof(SlotQueueBlockHeader));
}

inline size_t SlotQueueBlockSize(size_t capacity) {
  return SlotQueueBlockHeaderSize() + Aligned(SizeofSlotQueue(capacity));
}

inline size_t CcbSize(int num_slots, uint64_t subscriber_queue_arena_size) {
  return Aligned(sizeof(ChannelControlBlock) +
                 num_slots * sizeof(MessageSlot)) +
         Aligned(SizeofAtomicBitSet(num_slots)) * 2 +
         AvailableSlotsSize(num_slots) +
         AvailableSlotQueueIndexSize() +
         static_cast<size_t>(subscriber_queue_arena_size);
}

inline size_t CcbSize(int num_slots) {
  return CcbSize(num_slots, /*subscriber_queue_arena_size=*/0);
}

inline absl::StatusOr<size_t>
CheckedCcbSize(int num_slots, uint64_t subscriber_queue_arena_size) {
  if (num_slots < 0) {
    return absl::InvalidArgumentError("num_slots must be non-negative");
  }
  const size_t slots = static_cast<size_t>(num_slots);
  if (slots > kMaxChannelControlBlockSize / sizeof(MessageSlot)) {
    return absl::ResourceExhaustedError(
        "num_slots exceeds the channel control block limit");
  }
  if (slots > (std::numeric_limits<size_t>::max() -
               sizeof(ChannelControlBlock)) /
                  sizeof(MessageSlot)) {
    return absl::ResourceExhaustedError("channel control block size overflow");
  }
  const size_t base_size = CcbSize(num_slots, 0);
  if (base_size > kMaxChannelControlBlockSize ||
      subscriber_queue_arena_size >
          kMaxChannelControlBlockSize - base_size) {
    return absl::ResourceExhaustedError(
        "channel control block exceeds the 1 GiB limit");
  }
  return base_size + static_cast<size_t>(subscriber_queue_arena_size);
}

struct SlotBuffer {
  SlotBuffer(int32_t slot_sz) : slot_size(slot_sz) {}
  SlotBuffer(int32_t slot_sz, toolbelt::FileDescriptor f)
      : slot_size(slot_sz), fd(std::move(f)) {}
  int32_t slot_size;
  toolbelt::FileDescriptor fd;
};

// This holds the shared memory file descriptors for a channel.
// ccb: Channel Control Block
// buffers: message buffer memory.
struct SharedMemoryFds {
  SharedMemoryFds() = default;
  SharedMemoryFds(toolbelt::FileDescriptor ccb_fd,
                  toolbelt::FileDescriptor bcb_fd)
      : ccb(std::move(ccb_fd)), bcb(std::move(bcb_fd)) {}
  SharedMemoryFds(const SharedMemoryFds &) = delete;

  SharedMemoryFds(SharedMemoryFds &&c) {
    ccb = std::move(c.ccb);
    bcb = std::move(c.bcb);
  }
  SharedMemoryFds &operator=(const SharedMemoryFds &) = delete;

  SharedMemoryFds &operator=(SharedMemoryFds &&c) {
    ccb = std::move(c.ccb);
    bcb = std::move(c.bcb);
    return *this;
  }

  toolbelt::FileDescriptor ccb; // Channel Control Block.
  toolbelt::FileDescriptor bcb; // Buffer Control Block.
};

// This is the representation of a channel as seen by a publisher
// or subscriber.  There is one of these objects per publisher
// and per subscriber.  The object is created by the client
// after communicating with the server for it to allocate
// the shared memory, or get the file descriptors of existing
// shared memory.
//
// The server allocates the shared memory for a channel and
// keeps the file descriptors for the POSIX shared memory,
// which it distributes to the clients upon request.  Clients
// use mmap to map the shared memory into their address
// space.  If there are multiple publishers or subscribers in the
// same process, each of them maps in the shared memory. No attempt
// is made to share the Channel objects.
class Channel : public std::enable_shared_from_this<Channel> {
public:
  struct PublishedMessage {
    MessageSlot *new_slot;
    uint64_t ordinal;
    uint64_t timestamp;
  };

  Channel(const std::string &name, int num_slots, int channel_id,
          int subscriber_queue_size, uint64_t subscriber_queue_arena_size,
          std::string type,
          std::function<bool(Channel *)> reload = nullptr);
  virtual ~Channel() { Unmap(); }

  virtual void Unmap();

  const std::string &Name() const { return name_; }

  virtual std::string ResolvedName() const = 0;

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
  static absl::StatusOr<std::string>
  PosixSharedMemoryName(const std::string &shadow_file);
#endif
  // For debug, prints the contents of the three linked lists in
  // shared memory,
  void PrintLists() const;

  // A placeholder is a channel created for a subscriber where there are
  // no publishers and thus the shared memory is not yet valid.
  bool IsPlaceholder() const { return NumSlots() == 0; }

  std::string BufferSharedMemoryName(uint64_t session_id,
                                     int buffer_index) const;

  void RegisterSubscriber(int sub_id, int vchan_id, bool is_new) {
    ccb_->sub_vchan_ids[sub_id] = vchan_id;
    if (is_new && !IsPlaceholder()) {
      GetAvailableSlots(sub_id).ClearAll();
    }
    ccb_->subscribers.Set(sub_id);
    if (is_new && !IsPlaceholder()) {
      SeedAvailableSlotQueue(sub_id, vchan_id);
    }
    SubscriberCounter num_subs;
    ccb_->subscribers.Traverse([this, &num_subs](size_t id) {
      num_subs.AddSubscriber(ccb_->sub_vchan_ids[id]);
    });
    ccb_->num_subs = num_subs;
  }

  int GetSubVchanId(int32_t i) const { return ccb_->sub_vchan_ids[i]; }

  void SeedAvailableSlotQueue(int sub_id, int vchan_id) {
    InPlaceAtomicBitset &bits = GetAvailableSlots(sub_id);
    InPlaceSlotQueue *queue = GetAvailableSlotQueueAddress(sub_id);
    auto visible = [vchan_id](MessageSlot &slot) {
      const uint64_t refs = slot.refs.load(std::memory_order_acquire);
      if ((refs & kPubOwned) != 0) {
        return false;
      }
      const uint64_t ordinal = slot.ordinal.load(std::memory_order_relaxed);
      const int buffer_index =
          slot.buffer_index.load(std::memory_order_relaxed);
      if (ordinal == 0 || buffer_index == -1) {
        return false;
      }
      const int slot_vchan_id =
          slot.vchan_id.load(std::memory_order_relaxed);
      if (vchan_id != -1 && slot_vchan_id != -1 &&
          vchan_id != slot_vchan_id) {
        return false;
      }
      return true;
    };

    uint64_t last_ordinal = 0;
    for (;;) {
      MessageSlot *best = nullptr;
      for (int i = 0; i < NumSlots(); i++) {
        MessageSlot &slot = ccb_->slots[i];
        if (!visible(slot)) {
          continue;
        }
        const uint64_t ordinal =
            slot.ordinal.load(std::memory_order_relaxed);
        if (ordinal <= last_ordinal) {
          continue;
        }
        if (best == nullptr ||
            ordinal < best->ordinal.load(std::memory_order_relaxed)) {
          best = &slot;
        }
      }
      if (best == nullptr) {
        return;
      }
      bits.Set(best->id);
      if (queue != nullptr) {
        queue->Push(best->id,
                    best->ordinal.load(std::memory_order_relaxed));
      }
      last_ordinal = best->ordinal.load(std::memory_order_relaxed);
    }
  }

  void DumpSlots(std::ostream &os) const;
  virtual void Dump(std::ostream &os) const;

  // Compute the prefix size in bytes from checksum and metadata sizes.
  // The result is rounded up to a 64-byte boundary.
  static int32_t ComputePrefixSize(int32_t checksum_size,
                                   int32_t metadata_size) {
    return static_cast<int32_t>(
        Aligned<64>(offsetof(MessagePrefix, checksum) + checksum_size +
                    metadata_size));
  }

  // Total prefix area size in bytes (always a multiple of 64).
  //
  // These accessors are virtual so that VirtualChannel (in the server) can
  // forward them to its underlying multiplexer: storage and slot layout are
  // owned by the mux, so all virtual channels sharing a mux must agree on
  // the prefix layout.  Plain (non-mux) channels and client-side channels
  // simply use their own fields.
  virtual int32_t PrefixSize() const { return prefix_size_; }
  virtual void SetPrefixSize(int32_t size) { prefix_size_ = size; }

  virtual int32_t ChecksumSize() const { return checksum_size_; }
  virtual void SetChecksumSize(int32_t size) { checksum_size_ = size; }

  virtual int32_t MetadataSize() const { return metadata_size_; }
  virtual void SetMetadataSize(int32_t size) { metadata_size_ = size; }

  uint64_t BufferSizeToSlotSize(uint64_t size) const {
    if (size < NumSlots() * static_cast<uint64_t>(PrefixSize())) {
      return 0;
    }
    return (size - NumSlots() * static_cast<uint64_t>(PrefixSize())) /
           NumSlots();
  };

  uint64_t SlotSizeToBufferSize(uint64_t slot_size) const {
    return NumSlots() * (slot_size + PrefixSize());
  }

  // Get the number of slots in the channel (can't be changed)
  int NumSlots() const { return num_slots_; }
  virtual void SetNumSlots(int n) { num_slots_ = n; }
  virtual int SubscriberQueueSize() const { return subscriber_queue_size_; }
  virtual void SetSubscriberQueueSize(int n) {
    subscriber_queue_size_ = ResolveSubscriberQueueSize(num_slots_, n);
  }
  virtual uint64_t SubscriberQueueArenaSize() const {
    return subscriber_queue_arena_size_;
  }
  virtual void SetSubscriberQueueArenaSize(uint64_t size) {
    subscriber_queue_arena_size_ = size;
  }
  std::string SlotType() const { return type_; }

  void CleanupSlots(int owner, bool reliable, bool is_pub, int vchan_id);

  int NumSubscribers(int vchan_id) const {
    return ccb_->num_subs.NumSubscribers(vchan_id);
  }

  int GetChannelId() const { return channel_id_; }

  int NumUpdates() const { return num_updates_; }
  void SetNumUpdates(int num_updates) {
    num_updates_ = static_cast<uint16_t>(num_updates);
  }

  SystemControlBlock *GetScb() const { return scb_; }
  ChannelControlBlock *GetCcb() const { return ccb_; }

  // Gets the statistics counters.
  void GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages,
                        uint32_t &max_message_size, uint32_t &total_drops);

  void SetDebug(bool v) { debug_ = v; }

  virtual MessagePrefix *Prefix(MessageSlot */*slot*/) { return nullptr; }

  bool AtomicIncRefCount(MessageSlot *slot, bool reliable, int inc,
                         uint64_t ordinal, int vchan_id, bool retire,
                         std::function<void()> retire_callback = {});

  void SetType(std::string type) { type_ = std::move(type); }
  const std::string Type() const { return type_; }

  std::string_view TypeView() const { return type_; }

  char *EndOfSlots() const {
    return reinterpret_cast<char *>(ccb_) +
           Aligned(sizeof(ChannelControlBlock) +
                   num_slots_ * sizeof(MessageSlot));
  }
  char *EndOfRetiredSlots() const {
    return EndOfSlots() + Aligned(SizeofAtomicBitSet(num_slots_));
  }
  char *EndOfFreeSlots() const {
    return EndOfRetiredSlots() + Aligned(SizeofAtomicBitSet(num_slots_));
  }
  char *EndOfAvailableSlots() const {
    return EndOfFreeSlots() + AvailableSlotsSize(num_slots_);
  }
  char *EndOfAvailableSlotQueueIndex() const {
    return EndOfAvailableSlots() + AvailableSlotQueueIndexSize();
  }

  InPlaceAtomicBitset *RetiredSlotsAddr() {
    return reinterpret_cast<InPlaceAtomicBitset *>(EndOfSlots());
  }

  InPlaceAtomicBitset &RetiredSlots() {
    return *reinterpret_cast<InPlaceAtomicBitset *>(EndOfSlots());
  }

  const InPlaceAtomicBitset &RetiredSlots() const {
    return *reinterpret_cast<const InPlaceAtomicBitset *>(EndOfSlots());
  }

  InPlaceAtomicBitset *FreeSlotsAddr() {
    return reinterpret_cast<InPlaceAtomicBitset *>(EndOfRetiredSlots());
  }

  InPlaceAtomicBitset &FreeSlots() {
    return *reinterpret_cast<InPlaceAtomicBitset *>(EndOfRetiredSlots());
  }

  const InPlaceAtomicBitset &FreeSlots() const {
    return *reinterpret_cast<const InPlaceAtomicBitset *>(EndOfRetiredSlots());
  }

  InPlaceAtomicBitset &GetAvailableSlots(int sub_id) {
    return *GetAvailableSlotsAddress(sub_id);
  }

  InPlaceAtomicBitset *GetAvailableSlotsAddress(int sub_id) {
    return reinterpret_cast<InPlaceAtomicBitset *>(
        EndOfFreeSlots() + SizeofAtomicBitSet(num_slots_) * sub_id);
  }

  AvailableSlotQueueIndex *GetAvailableSlotQueueIndexAddress() {
    return reinterpret_cast<AvailableSlotQueueIndex *>(EndOfAvailableSlots());
  }

  const AvailableSlotQueueIndex *GetAvailableSlotQueueIndexAddress() const {
    return reinterpret_cast<const AvailableSlotQueueIndex *>(
        EndOfAvailableSlots());
  }

  InPlaceSlotQueue *GetAvailableSlotQueueAddress(int sub_id) {
    uint64_t offset = GetAvailableSlotQueueIndexAddress()
                          ->offsets[sub_id]
                          .load(std::memory_order_acquire);
    if (offset == kInvalidSlotQueueOffset) {
      return nullptr;
    }
    return reinterpret_cast<InPlaceSlotQueue *>(
        EndOfAvailableSlotQueueIndex() + offset);
  }

  const InPlaceSlotQueue *GetAvailableSlotQueueAddress(int sub_id) const {
    uint64_t offset = GetAvailableSlotQueueIndexAddress()
                          ->offsets[sub_id]
                          .load(std::memory_order_acquire);
    if (offset == kInvalidSlotQueueOffset) {
      return nullptr;
    }
    return reinterpret_cast<const InPlaceSlotQueue *>(
        EndOfAvailableSlotQueueIndex() + offset);
  }

  virtual int SubscriberQueueSize(int sub_id) const {
    const InPlaceSlotQueue *queue = GetAvailableSlotQueueAddress(sub_id);
    return queue == nullptr ? 0 : static_cast<int>(queue->Capacity());
  }

  void BeginSubscriberQueuePublish(int pub_id) {
    GetAvailableSlotQueueIndexAddress()
        ->active_publishers[pub_id]
        .fetch_add(1, std::memory_order_seq_cst);
  }

  void EndSubscriberQueuePublish(int pub_id) {
    auto &counter =
        GetAvailableSlotQueueIndexAddress()->active_publishers[pub_id];
    uint32_t active = counter.load(std::memory_order_seq_cst);
    while (active != 0) {
      if (counter.compare_exchange_strong(active, active - 1,
                                          std::memory_order_seq_cst,
                                          std::memory_order_seq_cst)) {
        return;
      }
    }
  }

  bool IsActivated(int vchan_id) const {
    return ccb_->activation_tracker.IsActivated(vchan_id);
  }

  virtual uint64_t GetVirtualMemoryUsage() const;

protected:
  int32_t ToCCBOffset(void *addr) const {
    return (int32_t)(reinterpret_cast<char *>(addr) -
                     reinterpret_cast<char *>(ccb_));
  }

  void *FromCCBOffset(int32_t offset) const {
    return reinterpret_cast<char *>(ccb_) + offset;
  }

  void DecrementBufferRefs(int buffer_index);
  void IncrementBufferRefs(int buffer_index);

  void CheckReload() {
    if (reload_callback_ == nullptr) {
      return;
    }
    for (;;) {
      if (!reload_callback_(this)) {
        break;
      }
    }
  }

  std::string name_;
  int num_slots_;
  int subscriber_queue_size_;
  uint64_t subscriber_queue_arena_size_;

  int channel_id_; // ID allocated from server.
  std::string type_;
  int32_t prefix_size_ = 64;
  int32_t checksum_size_ = 4;
  int32_t metadata_size_ = 0;

  uint16_t num_updates_ = 0;

  SystemControlBlock *scb_ = nullptr;
  ChannelControlBlock *ccb_ = nullptr;
  BufferControlBlock *bcb_ = nullptr;
  bool debug_ = false;
  std::function<bool(Channel *)> reload_callback_;
};

} // namespace subspace
#endif /* _xCOMMON_CHANNEL_H */
