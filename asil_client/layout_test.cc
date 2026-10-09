// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Checks that the ASIL client's view of shared memory matches
// common/channel.h, and that its checksum matches the standard client's.

#include "asil_client/channel_memory.h"
#include "asil_client/checksum.h"
#include "asil_client/shm_layout.h"
#include "client/checksum.h"
#include "common/channel.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <vector>

namespace {

namespace shm = ::subspace::asil::shm;

#define EXPECT_SAME_OFFSET(type, field)                                        \
  EXPECT_EQ(offsetof(::subspace::type, field), offsetof(shm::type, field))     \
      << #type "::" #field

TEST(LayoutTest, Constants) {
  EXPECT_EQ(::subspace::kMaxChannels, shm::kMaxChannels);
  EXPECT_EQ(::subspace::kMaxSlotOwners, shm::kMaxSlotOwners);
  EXPECT_EQ(::subspace::kMaxVchanId, shm::kMaxVchanId);
  EXPECT_EQ(static_cast<int>(::subspace::kMaxBuffers), shm::kMaxBuffers);
  EXPECT_EQ(::subspace::kChannelControlBlockVersion,
            shm::kChannelControlBlockVersion);
  EXPECT_EQ(::subspace::kInvalidSlotQueueOffset, shm::kInvalidSlotQueueOffset);
  EXPECT_EQ(::subspace::kMessageActivate, shm::kMessageActivate);
  EXPECT_EQ(::subspace::kMessageHasChecksum, shm::kMessageHasChecksum);
  EXPECT_EQ(static_cast<uint32_t>(::subspace::kMessageSeen), shm::kMessageSeen);
  EXPECT_EQ(static_cast<uint32_t>(::subspace::kMessageIsActivation),
            shm::kMessageIsActivation);
  EXPECT_EQ(static_cast<uint32_t>(::subspace::kMessageSeenByReliable),
            shm::kMessageSeenByReliable);
  EXPECT_EQ(::subspace::kMaxSlotQueueCasAttempts,
            shm::kMaxSlotQueueCasAttempts);
  EXPECT_EQ(::subspace::kPubOwned, shm::kPubOwned);
  EXPECT_EQ(::subspace::kRefsMask, shm::kRefsMask);
  EXPECT_EQ(::subspace::kRefCountMask, shm::kRefCountMask);
  EXPECT_EQ(::subspace::kReliableRefCountShift, shm::kReliableRefCountShift);
  EXPECT_EQ(::subspace::kRetiredRefsShift, shm::kRetiredRefsShift);
  EXPECT_EQ(::subspace::kRetiredRefsMask, shm::kRetiredRefsMask);
  EXPECT_EQ(::subspace::kVchanIdShift, shm::kVchanIdShift);
  EXPECT_EQ(::subspace::kVchanIdMask, shm::kVchanIdMask);
  EXPECT_EQ(::subspace::kOrdinalShift, shm::kOrdinalShift);
  EXPECT_EQ(::subspace::kOrdinalMask, shm::kOrdinalMask);
}

TEST(LayoutTest, MessagePrefix) {
  EXPECT_EQ(sizeof(::subspace::MessagePrefix), sizeof(shm::MessagePrefix));
  EXPECT_SAME_OFFSET(MessagePrefix, slot_id);
  EXPECT_SAME_OFFSET(MessagePrefix, message_size);
  EXPECT_SAME_OFFSET(MessagePrefix, ordinal);
  EXPECT_SAME_OFFSET(MessagePrefix, timestamp);
  EXPECT_SAME_OFFSET(MessagePrefix, flags);
  EXPECT_SAME_OFFSET(MessagePrefix, vchan_id);
  EXPECT_SAME_OFFSET(MessagePrefix, checksum_size);
  EXPECT_SAME_OFFSET(MessagePrefix, metadata_size);
  EXPECT_SAME_OFFSET(MessagePrefix, checksum);
}

TEST(LayoutTest, MessageSlot) {
  EXPECT_EQ(sizeof(::subspace::MessageSlot), sizeof(shm::MessageSlot));
  EXPECT_SAME_OFFSET(MessageSlot, refs);
  EXPECT_SAME_OFFSET(MessageSlot, ordinal);
  EXPECT_SAME_OFFSET(MessageSlot, message_size);
  EXPECT_SAME_OFFSET(MessageSlot, id);
  EXPECT_SAME_OFFSET(MessageSlot, buffer_index);
  EXPECT_SAME_OFFSET(MessageSlot, vchan_id);
  EXPECT_SAME_OFFSET(MessageSlot, sub_owners);
  EXPECT_SAME_OFFSET(MessageSlot, timestamp);
  EXPECT_SAME_OFFSET(MessageSlot, flags);
  EXPECT_SAME_OFFSET(MessageSlot, bridged_slot_id);
}

TEST(LayoutTest, ControlBlocks) {
  EXPECT_EQ(sizeof(::subspace::ChannelCounters), sizeof(shm::ChannelCounters));
  EXPECT_SAME_OFFSET(ChannelCounters, num_pub_updates);
  EXPECT_SAME_OFFSET(ChannelCounters, num_sub_updates);
  EXPECT_EQ(sizeof(::subspace::SystemControlBlock),
            sizeof(shm::SystemControlBlock));
  EXPECT_EQ(sizeof(::subspace::BufferControlBlock),
            sizeof(shm::BufferControlBlock));
  EXPECT_SAME_OFFSET(BufferControlBlock, sizes);
  EXPECT_EQ(sizeof(::subspace::AvailableSlotQueueIndex),
            sizeof(shm::AvailableSlotQueueIndex));
  EXPECT_SAME_OFFSET(AvailableSlotQueueIndex, offsets);
  EXPECT_SAME_OFFSET(AvailableSlotQueueIndex, active_publishers);
}

TEST(LayoutTest, ChannelControlBlock) {
  EXPECT_EQ(sizeof(::subspace::ChannelControlBlock),
            sizeof(shm::ChannelControlBlock));
  EXPECT_EQ(offsetof(::subspace::ChannelControlBlock, slots),
            sizeof(shm::ChannelControlBlock));
  EXPECT_SAME_OFFSET(ChannelControlBlock, num_slots);
  EXPECT_SAME_OFFSET(ChannelControlBlock, subscriber_queue_size);
  EXPECT_SAME_OFFSET(ChannelControlBlock, version);
  EXPECT_SAME_OFFSET(ChannelControlBlock, ordinals);
  EXPECT_SAME_OFFSET(ChannelControlBlock, activation_tracker);
  EXPECT_SAME_OFFSET(ChannelControlBlock, buffer_index);
  EXPECT_SAME_OFFSET(ChannelControlBlock, num_buffers);
  EXPECT_SAME_OFFSET(ChannelControlBlock, subscribers);
  EXPECT_SAME_OFFSET(ChannelControlBlock, sub_vchan_ids);
  EXPECT_SAME_OFFSET(ChannelControlBlock, num_subs);
  EXPECT_SAME_OFFSET(ChannelControlBlock, subscriber_cleanup_generation);
  EXPECT_SAME_OFFSET(ChannelControlBlock, total_bytes);
  EXPECT_SAME_OFFSET(ChannelControlBlock, total_messages);
  EXPECT_SAME_OFFSET(ChannelControlBlock, max_message_size);
  EXPECT_SAME_OFFSET(ChannelControlBlock, total_drops);
  EXPECT_SAME_OFFSET(ChannelControlBlock, free_slots_exhausted);
  EXPECT_EQ(sizeof(::subspace::SubscriberCounter),
            sizeof(shm::SubscriberCounter));
  EXPECT_EQ(sizeof(::subspace::OrdinalAccumulator),
            sizeof(shm::ChannelControlBlock::ordinals));
  EXPECT_EQ(sizeof(::subspace::ActivationTracker),
            sizeof(shm::ChannelControlBlock::activation_tracker));
  EXPECT_EQ(sizeof(::subspace::SubscriberCleanupGeneration),
            sizeof(shm::ChannelControlBlock::subscriber_cleanup_generation));
}

TEST(LayoutTest, CcbRegions) {
  for (int num_slots : {1, 2, 7, 8, 63, 64, 65, 100, 1000, 4096}) {
    SCOPED_TRACE(num_slots);
    const size_t bitset = ::subspace::SizeofAtomicBitSet(num_slots);
    EXPECT_EQ(shm::SizeofBitset(num_slots), bitset);
    const size_t retired = static_cast<size_t>(
        ::subspace::Aligned(sizeof(::subspace::ChannelControlBlock) +
                            num_slots * sizeof(::subspace::MessageSlot)));
    EXPECT_EQ(shm::RetiredSlotsOffset(num_slots), retired);
    const size_t free_slots =
        retired + static_cast<size_t>(::subspace::Aligned(bitset));
    EXPECT_EQ(shm::FreeSlotsOffset(num_slots), free_slots);
    const size_t available =
        free_slots + static_cast<size_t>(::subspace::Aligned(bitset));
    EXPECT_EQ(shm::AvailableSlotsOffset(num_slots, 0), available);
    EXPECT_EQ(shm::AvailableSlotsOffset(num_slots, 5), available + 5 * bitset);
    EXPECT_EQ(shm::SlotQueueIndexOffset(num_slots),
              available + ::subspace::AvailableSlotsSize(num_slots));
    EXPECT_EQ(shm::SlotQueueArenaOffset(num_slots),
              shm::SlotQueueIndexOffset(num_slots) +
                  ::subspace::AvailableSlotQueueIndexSize());
    EXPECT_EQ(shm::CcbSize(num_slots, 0), ::subspace::CcbSize(num_slots, 0));
    EXPECT_EQ(shm::CcbSize(num_slots, 64000),
              ::subspace::CcbSize(num_slots, 64000));
  }
}

TEST(LayoutTest, PrefixSize) {
  for (int32_t checksum_size : {0, 4, 16, 17}) {
    for (int32_t metadata_size : {0, 1, 16, 100}) {
      EXPECT_EQ(
          shm::PrefixSize(checksum_size, metadata_size),
          ::subspace::Channel::ComputePrefixSize(checksum_size, metadata_size));
    }
  }
}

TEST(LayoutTest, RefsBitField) {
  for (uint64_t ordinal : {0ULL, 1ULL, 12345ULL, (1ULL << 23) + 7}) {
    for (int vchan_id : {-1, 0, 5, 1022}) {
      for (int retired : {0, 1, 1023}) {
        EXPECT_EQ(shm::BuildRefsBitField(ordinal, vchan_id, retired),
                  ::subspace::BuildRefsBitField(ordinal, vchan_id, retired));
      }
    }
  }
}

TEST(LayoutTest, SlotQueue) {
  EXPECT_EQ(sizeof(::subspace::InPlaceSlotQueue), sizeof(shm::SlotQueue));
  EXPECT_EQ(sizeof(::subspace::SlotQueueEntry), sizeof(shm::SlotQueueEntry));
  EXPECT_SAME_OFFSET(SlotQueueEntry, sequence);
  EXPECT_SAME_OFFSET(SlotQueueEntry, ordinal);
  EXPECT_SAME_OFFSET(SlotQueueEntry, slot_id);
}

// A queue in a buffer, initialized by the standard client and used through
// both views.
struct QueueMemory {
  explicit QueueMemory(size_t capacity, bool drop_oldest)
      : memory(::subspace::SizeofSlotQueue(capacity) / sizeof(uint64_t) + 1),
        standard(new (memory.data())::subspace::InPlaceSlotQueue(capacity,
                                                                 drop_oldest)),
        asil(reinterpret_cast<shm::SlotQueue *>(memory.data())) {}

  std::vector<uint64_t> memory;
  ::subspace::InPlaceSlotQueue *standard;
  shm::SlotQueue *asil;
};

TEST(SlotQueueTest, AsilPushesStandardPops) {
  QueueMemory queue(4, /*drop_oldest=*/true);
  EXPECT_EQ(4u, queue.asil->capacity);
  for (int32_t i = 0; i < 6; i++) {
    EXPECT_TRUE(
        ::subspace::asil::internal::PushSlotQueue(queue.asil, i, 100 + i));
  }
  // The two oldest entries made room for the newest.
  EXPECT_EQ(2u, queue.standard->OverflowCount());
  for (int32_t i = 2; i < 6; i++) {
    ::subspace::QueuedSlot slot;
    ASSERT_TRUE(queue.standard->TryPop(slot));
    EXPECT_EQ(i, slot.slot_id);
    EXPECT_EQ(static_cast<uint64_t>(100 + i), slot.ordinal);
  }
  ::subspace::QueuedSlot slot;
  EXPECT_FALSE(queue.standard->TryPop(slot));

  // Entries wrap around the ring.
  EXPECT_TRUE(::subspace::asil::internal::PushSlotQueue(queue.asil, 9, 109));
  ASSERT_TRUE(queue.standard->TryPop(slot));
  EXPECT_EQ(9, slot.slot_id);
}

TEST(SlotQueueTest, InheritedQueueRejectsWhenFull) {
  QueueMemory queue(2, /*drop_oldest=*/false);
  EXPECT_TRUE(::subspace::asil::internal::PushSlotQueue(queue.asil, 0, 1));
  EXPECT_TRUE(::subspace::asil::internal::PushSlotQueue(queue.asil, 1, 2));
  EXPECT_FALSE(::subspace::asil::internal::PushSlotQueue(queue.asil, 2, 3));
  EXPECT_FALSE(queue.standard->InsertionFailed());
  ::subspace::asil::internal::MarkSlotQueueInsertionFailure(queue.asil);
  EXPECT_TRUE(queue.standard->InsertionFailed());
  EXPECT_EQ(0u, queue.standard->OverflowCount());

  // A standard push is visible to the ASIL view.
  QueueMemory other(2, /*drop_oldest=*/true);
  ASSERT_TRUE(other.standard->Push(7, 70));
  EXPECT_EQ(1u, other.asil->tail.load());
  EXPECT_EQ(7, other.asil->Entries()[0].slot_id.load());
  EXPECT_EQ(70u, other.asil->Entries()[0].ordinal.load());
}

TEST(ChecksumTest, MatchesStandardClient) {
  std::mt19937 random(42);
  for (size_t length : {0, 1, 3, 4, 7, 8, 9, 63, 64, 1000}) {
    SCOPED_TRACE(length);
    std::vector<uint8_t> data(length);
    for (uint8_t &byte : data) {
      byte = static_cast<uint8_t>(random());
    }
    std::array<absl::Span<const uint8_t>, 3> spans = {
        absl::Span<const uint8_t>(data.data(), length / 3),
        absl::Span<const uint8_t>(data.data() + length / 3, length / 3),
        absl::Span<const uint8_t>(data.data() + 2 * (length / 3),
                                  length - 2 * (length / 3))};
    uint32_t expected = 0;
    ::subspace::CalculateCRC32Checksum(
        spans, absl::Span<std::byte>(reinterpret_cast<std::byte *>(&expected),
                                     sizeof(expected)));
    uint32_t crc = 0xFFFFFFFF;
    for (const auto &span : spans) {
      crc = ::subspace::asil::Crc32(crc, span.data(), span.size());
    }
    EXPECT_EQ(expected, ~crc);
  }
}

} // namespace
