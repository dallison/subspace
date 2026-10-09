// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// The Subspace shared memory layout as seen by the ASIL client.
//
// These structures mirror common/channel.h byte for byte so that the ASIL
// client can share channels with the standard clients.  They are declared
// here, rather than included, so that the ASIL client depends only on the C++
// standard library and POSIX.  layout_test.cc checks every size and offset
// against common/channel.h.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

// The shared memory backend.  This follows the selection in common/channel.h.
#define ASIL_SHM_POSIX 1
#define ASIL_SHM_LINUX 2
#define ASIL_SHM_MEMFD 3

#if defined(SUBSPACE_SHMEM_MODE)
#define ASIL_SHM_MODE SUBSPACE_SHMEM_MODE
#elif defined(__ANDROID__)
#define ASIL_SHM_MODE ASIL_SHM_MEMFD
#elif defined(__linux__) && defined(SUBSPACE_LINUX_USE_MEMFD)
#define ASIL_SHM_MODE ASIL_SHM_MEMFD
#elif defined(__linux__)
#define ASIL_SHM_MODE ASIL_SHM_LINUX
#else
#define ASIL_SHM_MODE ASIL_SHM_POSIX
#endif

#ifndef SUBSPACE_MAX_CHANNELS
#define SUBSPACE_MAX_CHANNELS 1024
#endif

namespace subspace {
namespace asil {
namespace shm {

static_assert(sizeof(void *) == 8, "Subspace shared memory needs 64 bits");

constexpr int kMaxChannels = SUBSPACE_MAX_CHANNELS;
constexpr int kMaxSlotOwners = 1024;
constexpr int kMaxVchanId = 1023;
constexpr int kMaxBuffers = 1024;
constexpr size_t kMaxChannelName = 64;
constexpr uint32_t kChannelControlBlockVersion = 5;
constexpr uint64_t kInvalidSlotQueueOffset = ~uint64_t{0};

// MessagePrefix flags.
constexpr int64_t kMessageActivate = 1;
constexpr int64_t kMessageHasChecksum = 4;

// MessageSlot flags.
constexpr uint32_t kMessageSeen = 1;
constexpr uint32_t kMessageIsActivation = 2;

// Fields of MessageSlot::refs.
constexpr uint64_t kRefCountMask = 0x3ff;
constexpr uint64_t kReliableRefCountShift = 10;
constexpr uint64_t kPubOwned = uint64_t{1} << 63;
constexpr uint64_t kRefsMask = (uint64_t{1} << 20) - 1;
constexpr uint64_t kRetiredRefsMask = (uint64_t{1} << 10) - 1;
constexpr uint64_t kRetiredRefsShift = 20;
constexpr uint64_t kVchanIdMask = (uint64_t{1} << 10) - 1;
constexpr uint64_t kVchanIdShift = 30;
constexpr uint64_t kOrdinalMask = (uint64_t{1} << 23) - 1;
constexpr uint64_t kOrdinalShift = 40;

inline uint64_t BuildRefsBitField(uint64_t ordinal, int vchan_id,
                                  uint64_t retired_refs) {
  const uint64_t vchan_bits =
      ordinal == 0 ? 0
                   : ((static_cast<uint64_t>(vchan_id) & kVchanIdMask)
                      << kVchanIdShift);
  return ((ordinal & kOrdinalMask) << kOrdinalShift) | vchan_bits |
         ((retired_refs & kRetiredRefsMask) << kRetiredRefsShift);
}

// The vchan_id stored in refs, where all ones means -1.
inline int RefsVchanId(uint64_t refs) {
  const uint64_t v = (refs >> kVchanIdShift) & kVchanIdMask;
  return v == kVchanIdMask ? -1 : static_cast<int>(v);
}

constexpr int64_t Aligned64(int64_t v) { return (v + 63) & ~int64_t{63}; }

constexpr size_t BitsToWords(size_t bits) {
  return bits == 0 ? 0 : ((bits - 1) / 64) + 1;
}

// AtomicBitSet<N>: the bit count followed by the words.
constexpr size_t SizeofBitset(size_t bits) {
  return sizeof(size_t) + sizeof(uint64_t) * BitsToWords(bits);
}

template <size_t kBits> struct FixedBitset {
  size_t num_bits;
  std::atomic<uint64_t> words[BitsToWords(kBits)];
};

struct MessagePrefix {
  int32_t padding;
  int32_t slot_id;
  uint64_t message_size;
  uint64_t ordinal;
  uint64_t timestamp;
  int64_t flags;
  int32_t vchan_id;
  uint16_t checksum_size;
  uint16_t metadata_size;
  uint32_t checksum;
  char padding3[12];
};
static_assert(sizeof(MessagePrefix) == 64);
static_assert(offsetof(MessagePrefix, slot_id) == 4);
static_assert(offsetof(MessagePrefix, checksum) == 48);

struct ChannelCounters {
  uint16_t num_pub_updates;
  uint16_t num_sub_updates;
  uint16_t num_pubs;
  uint16_t num_reliable_pubs;
  uint16_t num_subs;
  uint16_t num_reliable_subs;
  uint16_t num_resizes;
};
static_assert(sizeof(ChannelCounters) == 14);

struct SystemControlBlock {
  ChannelCounters counters[kMaxChannels];
};

struct MessageSlot {
  std::atomic<uint64_t> refs;
  std::atomic<uint64_t> ordinal;
  std::atomic<uint64_t> message_size;
  int32_t id;
  std::atomic<int16_t> buffer_index;
  std::atomic<int16_t> vchan_id;
  FixedBitset<kMaxSlotOwners> sub_owners;
  std::atomic<uint64_t> timestamp;
  std::atomic<uint32_t> flags;
  std::atomic<int32_t> bridged_slot_id;
};
static_assert(sizeof(MessageSlot) == 184);
static_assert(offsetof(MessageSlot, id) == 24);
static_assert(offsetof(MessageSlot, sub_owners) == 32);
static_assert(offsetof(MessageSlot, timestamp) == 168);
static_assert(offsetof(MessageSlot, bridged_slot_id) == 180);

struct SubscriberCounter {
  std::atomic<uint64_t> sequence;
  std::atomic<int32_t> counts[kMaxVchanId + 1];
};

struct ChannelControlBlock {
  char channel_name[kMaxChannelName];
  int32_t num_slots;
  int32_t subscriber_queue_size;
  uint32_t version;
  std::atomic<uint64_t> ordinals[kMaxVchanId + 1];
  FixedBitset<kMaxVchanId + 1> activation_tracker;
  int32_t buffer_index;
  std::atomic<int32_t> num_buffers;
  FixedBitset<kMaxSlotOwners> subscribers;
  int16_t sub_vchan_ids[kMaxSlotOwners];
  SubscriberCounter num_subs;
  std::atomic<uint64_t> subscriber_cleanup_generation[kMaxVchanId + 1];
  std::atomic<uint64_t> total_bytes;
  std::atomic<uint64_t> total_messages;
  std::atomic<uint64_t> max_message_size;
  std::atomic<uint32_t> total_drops;
  std::atomic<bool> free_slots_exhausted;
  // Followed by num_slots MessageSlots.
};
static_assert(offsetof(ChannelControlBlock, version) == 72);

struct AvailableSlotQueueIndex {
  std::atomic<uint64_t> next_offset;
  std::atomic<uint64_t> offsets[kMaxSlotOwners];
  std::atomic<uint32_t> active_publishers[kMaxSlotOwners];
};
static_assert(sizeof(AvailableSlotQueueIndex) == 12296);

struct BufferControlBlock {
  std::atomic<int32_t> refs[kMaxBuffers];
  std::atomic<uint64_t> sizes[kMaxBuffers];
};

// Offsets of the regions that follow the slots in the CCB.  A CCB holds the
// header and slots, the retired and free slot bitsets, one available-slot
// bitset per subscriber, the subscriber queue index and the queue arena.
inline size_t RetiredSlotsOffset(int num_slots) {
  return static_cast<size_t>(
      Aligned64(static_cast<int64_t>(sizeof(ChannelControlBlock) +
                                     num_slots * sizeof(MessageSlot))));
}

inline size_t FreeSlotsOffset(int num_slots) {
  return RetiredSlotsOffset(num_slots) +
         static_cast<size_t>(Aligned64(
             static_cast<int64_t>(SizeofBitset(num_slots))));
}

inline size_t AvailableSlotsOffset(int num_slots, int sub_id) {
  return FreeSlotsOffset(num_slots) +
         static_cast<size_t>(Aligned64(
             static_cast<int64_t>(SizeofBitset(num_slots)))) +
         SizeofBitset(num_slots) * static_cast<size_t>(sub_id);
}

inline size_t SlotQueueIndexOffset(int num_slots) {
  return AvailableSlotsOffset(num_slots, kMaxSlotOwners);
}

inline size_t CcbSize(int num_slots, uint64_t subscriber_queue_arena_size) {
  return SlotQueueIndexOffset(num_slots) +
         static_cast<size_t>(
             Aligned64(static_cast<int64_t>(sizeof(AvailableSlotQueueIndex)))) +
         static_cast<size_t>(subscriber_queue_arena_size);
}

inline int32_t PrefixSize(int32_t checksum_size, int32_t metadata_size) {
  return static_cast<int32_t>(Aligned64(
      static_cast<int64_t>(offsetof(MessagePrefix, checksum)) + checksum_size +
      metadata_size));
}

} // namespace shm
} // namespace asil
} // namespace subspace
