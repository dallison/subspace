#include "common/atomic_bitset.h"
#include "common/channel.h"
#include "common/fast_ring_buffer.h"

#include <cstddef>
#include <memory>
#include <new>

#include <gtest/gtest.h>

TEST(CommonTest, AtomicBitset) {
  subspace::AtomicBitSet<6144> bitset;
  bitset.Set(0);
  bitset.Set(1);
  bitset.Set(63);
  bitset.Set(6143);
  EXPECT_TRUE(bitset.IsSet(0));
  EXPECT_TRUE(bitset.IsSet(1));
  EXPECT_TRUE(bitset.IsSet(63));
  EXPECT_TRUE(bitset.IsSet(6143));
  EXPECT_FALSE(bitset.IsSet(2));
  bitset.Clear(0);
  bitset.Clear(1);
  bitset.Clear(63);
  EXPECT_FALSE(bitset.IsSet(0));
  EXPECT_FALSE(bitset.IsSet(1));
  EXPECT_FALSE(bitset.IsSet(63));
}

TEST(CommonTest, AtomicBitsetTraverse) {
  subspace::AtomicBitSet<130> bitset;
  bitset.Set(0);
  bitset.Set(1);
  bitset.Set(63);
  bitset.Set(64);
  bitset.Set(66);
  bitset.Set(129);
  int count = 0;
  bitset.Traverse([&count](int /*i*/) { count++; });
  EXPECT_EQ(count, 6);
}

TEST(CommonTest, FastRingBuffer) {
  subspace::FastRingBuffer<int, 3> buffer;
  buffer.Insert(1);
  buffer.Insert(2);
  buffer.Insert(3);
  EXPECT_TRUE(buffer.Contains(1));
  EXPECT_TRUE(buffer.Contains(2));
  EXPECT_TRUE(buffer.Contains(3));
  buffer.Insert(4);
  EXPECT_FALSE(buffer.Contains(1));
  EXPECT_TRUE(buffer.Contains(2));
  EXPECT_TRUE(buffer.Contains(3));
  EXPECT_TRUE(buffer.Contains(4));
}

TEST(CommonTest, InPlaceSlotQueueEvictsOldestOnOverflow) {
  constexpr size_t kCapacity = 2;
  std::unique_ptr<std::byte[]> storage(
      new std::byte[subspace::SizeofSlotQueue(kCapacity)]);
  auto *queue = new (storage.get()) subspace::InPlaceSlotQueue(kCapacity);

  EXPECT_TRUE(queue->Push(1, 10));
  EXPECT_TRUE(queue->Push(2, 20));
  EXPECT_TRUE(queue->Push(3, 30));
  EXPECT_TRUE(queue->ConsumeOverflow());

  subspace::QueuedSlot slot;
  ASSERT_TRUE(queue->TryPop(slot));
  EXPECT_EQ(slot.slot_id, 2);
  EXPECT_EQ(slot.ordinal, 20);
  ASSERT_TRUE(queue->TryPop(slot));
  EXPECT_EQ(slot.slot_id, 3);
  EXPECT_EQ(slot.ordinal, 30);
  EXPECT_FALSE(queue->TryPop(slot));
}

TEST(CommonTest, BitsetTraverse1) {
  subspace::AtomicBitSet<10000> bitset;
  for (int i = 0; i < 10000; i++) {
    bitset.Set(i);
    int count = 0;
    bitset.Traverse([&count](int /*i*/) {
      // std::cerr << i << std::endl;
      count++;
    });
    ASSERT_EQ(count, i + 1);
  }
}

TEST(CommonTest, BitsetTraverse2) {
  subspace::AtomicBitSet<10000> bitset;
  bitset.Set(0);
  bitset.Set(1);
  bitset.Set(63);
  bitset.Set(64);
  bitset.Set(66);
  bitset.Set(6143);
  bitset.Set(6144);
  int count = 0;
  int bits[] = {0, 1, 63, 64, 66, 6143, 6144};
  bitset.Traverse([&count, &bits](int i) {
    EXPECT_EQ(bits[count], i);
    count++;
  });
  EXPECT_EQ(count, 7);
}
