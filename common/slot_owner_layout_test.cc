#include "common/channel.h"

#include <cstddef>

#include <gtest/gtest.h>

// This file is compiled only with SUBSPACE_MAX_SLOT_OWNERS=256. The sizes
// below are the shared-memory layout at that count. Hardcoding the 1024-owner
// sizes (MessageSlot 320, AvailableSlotQueueIndex 12296, SlotQueueBlockHeader
// 192) fails this build.

namespace {

// The object's header is num_bits_ padded to the words' alignment: 8 bytes on
// every target, including 32-bit ones where size_t is 4.
constexpr size_t BitsetBytes(size_t bits) {
  return sizeof(subspace::InPlaceAtomicBitset) +
         sizeof(uint64_t) * ((bits + 63) / 64);
}

}  // namespace

TEST(SlotOwnerLayoutTest, TwoHundredFiftySixOwners) {
  static_assert(subspace::kMaxSlotOwners == 256);
  static_assert(BitsetBytes(256) == sizeof(subspace::AtomicBitSet<256>));
  static_assert(offsetof(subspace::MessageSlot, sub_owners) == 32);
  static_assert(offsetof(subspace::MessageSlot, timestamp) == 72);
  static_assert(offsetof(subspace::MessageSlot, flags) == 80);
  static_assert(offsetof(subspace::MessageSlot, bridged_slot_id) == 84);
  static_assert(offsetof(subspace::MessageSlot, pending_subs) == 88);
  static_assert(sizeof(subspace::MessageSlot) == 128);
  static_assert(offsetof(subspace::AvailableSlotQueueIndex, offsets) == 8);
  static_assert(offsetof(subspace::AvailableSlotQueueIndex, active_publishers) ==
                2056);
  static_assert(sizeof(subspace::AvailableSlotQueueIndex) == 3080);
  static_assert(offsetof(subspace::SlotQueueBlockHeader, waiting_publishers) ==
                16);
  static_assert(sizeof(subspace::SlotQueueBlockHeader) == 64);
  EXPECT_EQ(256, subspace::kMaxSlotOwners);
}
