// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Checks the shared-memory protocol code in common/ that the ASIL client and
// the standard client share: the channel control block regions, finding and
// publishing a slot, reading a message, and the message checksum.

#include "asil_client/shm_layout.h"
#include "client/checksum.h"
#include "common/channel.h"
#include "common/channel_view.h"
#include "common/message_checksum.h"
#include "common/publish.h"
#include "common/slot_search.h"
#include "common/subscribe.h"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <new>
#include <random>
#include <set>
#include <vector>

namespace {

namespace shm = ::subspace::asil::shm;
using ::subspace::ChannelControlBlock;
using ::subspace::ChannelView;
using ::subspace::InPlaceSlotQueue;
using ::subspace::MessagePrefix;
using ::subspace::MessageSlot;
using ::subspace::PublishedSlot;
using ::subspace::SlotSearch;

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
    EXPECT_EQ(shm::CcbSize(num_slots, 64000),
              shm::SlotQueueArenaOffset(num_slots) + 64000);
  }
}

// A channel control block in process memory, set up as the server does.
class TestChannel {
public:
  static constexpr int kPublisherId = 2;

  TestChannel(int num_slots, uint64_t queue_arena_size)
      : memory_(::subspace::CcbSize(num_slots, queue_arena_size) /
                    sizeof(Block) +
                1),
        view_(reinterpret_cast<ChannelControlBlock *>(memory_.data()),
              num_slots, queue_arena_size) {
    ChannelControlBlock *ccb = view_.Ccb();
    new (&ccb->num_subs)::subspace::SubscriberCounter();
    new (&ccb->subscriber_cleanup_generation)::subspace::
        SubscriberCleanupGeneration();
    ccb->num_slots = num_slots;
    ccb->ordinals.Init(1);
    new (&ccb->subscribers)
        ::subspace::AtomicBitSet<::subspace::kMaxSlotOwners>();
    auto *index = new (view_.QueueIndex())::subspace::AvailableSlotQueueIndex;
    index->next_offset.store(0);
    for (auto &offset : index->offsets) {
      offset.store(::subspace::kInvalidSlotQueueOffset);
    }
    for (auto &active : index->active_publishers) {
      active.store(0);
    }
    for (int i = 0; i < num_slots; i++) {
      MessageSlot *slot = view_.Slot(i);
      slot->id = i;
      slot->vchan_id.store(-1);
      slot->bridged_slot_id.store(-1);
    }
  }

  ChannelView &View() { return view_; }
  ChannelControlBlock *Ccb() { return view_.Ccb(); }

  void AddSubscriber(int sub_id, int vchan_id) {
    Ccb()->sub_vchan_ids[sub_id] = vchan_id;
    Ccb()->num_subs.AddSubscriber(vchan_id);
    Ccb()->subscribers.Set(sub_id);
  }

  // Places a queue at offset in the queue arena.  The capacity recorded in
  // the queue can be more than the arena holds.
  InPlaceSlotQueue *AddQueue(int sub_id, uint64_t offset, size_t capacity,
                             bool drop_oldest) {
    char *arena = reinterpret_cast<char *>(Ccb()) +
                  ::subspace::SlotQueueArenaOffset(view_.NumSlots());
    auto *queue = new (arena + offset) InPlaceSlotQueue(capacity, drop_oldest);
    view_.QueueIndex()->offsets[sub_id].store(offset);
    return queue;
  }

  // Publishes the slot as a publisher does once it has written the message.
  // The message's timestamp defaults to 1000 + slot_id.
  PublishedSlot Publish(int slot_id, int vchan_id, uint64_t size,
                        bool is_activation = false, uint64_t timestamp = 0) {
    MessageSlot *slot = view_.Slot(slot_id);
    slot->refs.store(::subspace::kPubOwned);
    slot->vchan_id.store(static_cast<int16_t>(vchan_id));
    slot->message_size.store(size);
    slot->bridged_slot_id.store(slot_id);
    ::subspace::StampSlot(Ccb(), slot,
                          timestamp != 0 ? timestamp : 1000 + slot_id);
    ::subspace::SubscriberQueuePublishScope publishing(view_.QueueIndex(),
                                                       kPublisherId);
    EXPECT_EQ(1u, view_.QueueIndex()->active_publishers[kPublisherId].load());
    return ::subspace::CommitPublishedSlot(view_, slot, vchan_id, is_activation,
                                           publishing);
  }

private:
  struct alignas(64) Block {
    char bytes[64];
  };
  std::vector<Block> memory_;
  ChannelView view_;
};

TEST(PublishTest, DeliversToSubscribersOnTheVirtualChannel) {
  TestChannel channel(/*num_slots=*/8, /*queue_arena_size=*/4096);
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.AddSubscriber(1, /*vchan_id=*/3);
  channel.AddSubscriber(2, /*vchan_id=*/4);
  InPlaceSlotQueue *queue = channel.AddQueue(1, /*offset=*/0, /*capacity=*/4,
                                             /*drop_oldest=*/true);

  const PublishedSlot published = channel.Publish(5, /*vchan_id=*/3, 100);
  EXPECT_EQ(1u, published.ordinal);
  EXPECT_EQ(1005u, published.timestamp);
  EXPECT_EQ(5, published.retirement_slot_id);
  EXPECT_FALSE(published.retired);

  ChannelView &view = channel.View();
  EXPECT_TRUE(view.AvailableSlots(0).IsSet(5));
  EXPECT_TRUE(view.AvailableSlots(1).IsSet(5));
  EXPECT_FALSE(view.AvailableSlots(2).IsSet(5));

  ::subspace::QueuedSlot queued;
  ASSERT_TRUE(queue->TryPop(queued));
  EXPECT_EQ(5, queued.slot_id);
  EXPECT_EQ(1u, queued.ordinal);
  EXPECT_FALSE(queue->InsertionFailed());

  EXPECT_EQ(::subspace::BuildRefsBitField(1, 3, 0),
            view.Slot(5)->refs.load());
  EXPECT_EQ(1u, channel.Ccb()->total_messages.load());
  EXPECT_EQ(100u, channel.Ccb()->total_bytes);
  EXPECT_EQ(100u, channel.Ccb()->max_message_size);
  EXPECT_EQ(0u, view.QueueIndex()
                    ->active_publishers[TestChannel::kPublisherId]
                    .load());
}

TEST(PublishTest, ActivationIsNotCountedAsData) {
  TestChannel channel(/*num_slots=*/4, /*queue_arena_size=*/0);
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.Publish(0, /*vchan_id=*/-1, 1, /*is_activation=*/true);
  EXPECT_TRUE(channel.View().AvailableSlots(0).IsSet(0));
  EXPECT_EQ(1u, channel.Ccb()->total_messages.load());
  EXPECT_EQ(0u, channel.Ccb()->total_bytes);
}

TEST(PublishTest, FullQueueRecordsInsertionFailure) {
  TestChannel channel(/*num_slots=*/4, /*queue_arena_size=*/4096);
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  InPlaceSlotQueue *queue = channel.AddQueue(0, /*offset=*/0, /*capacity=*/1,
                                             /*drop_oldest=*/false);
  channel.Publish(0, /*vchan_id=*/-1, 10);
  EXPECT_FALSE(queue->InsertionFailed());
  channel.Publish(1, /*vchan_id=*/-1, 10);
  EXPECT_TRUE(queue->InsertionFailed());

  // The bitset still records both messages.
  EXPECT_TRUE(channel.View().AvailableSlots(0).IsSet(0));
  EXPECT_TRUE(channel.View().AvailableSlots(0).IsSet(1));
}

TEST(PublishTest, QueueOutsideTheArenaIsIgnored) {
  const uint64_t arena = ::subspace::SizeofSlotQueue(4);
  TestChannel channel(/*num_slots=*/4, arena);
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.AddSubscriber(1, /*vchan_id=*/-1);
  // Queue 0 claims more entries than the arena holds.
  channel.AddQueue(0, /*offset=*/0, /*capacity=*/5, /*drop_oldest=*/true);
  // Queue 1 starts past the end of the arena.
  channel.View().QueueIndex()->offsets[1].store(arena + 8);

  EXPECT_EQ(nullptr, channel.View().Queue(0));
  EXPECT_EQ(nullptr, channel.View().Queue(1));
  EXPECT_EQ(nullptr, channel.View().Queue(2));

  channel.Publish(2, /*vchan_id=*/-1, 10);
  EXPECT_TRUE(channel.View().AvailableSlots(0).IsSet(2));
  EXPECT_TRUE(channel.View().AvailableSlots(1).IsSet(2));
}

// What a slot search asks of its client, recorded.
class TestHooks : public ::subspace::SlotSearchHooks {
public:
  explicit TestHooks(int num_slots) : prefixes_(num_slots) {}

  bool UsableSlot(MessageSlot *slot) { return unusable.count(slot->id) == 0; }
  void Embargo(int slot_id) { embargoed.insert(slot_id); }
  bool Embargoed(int slot_id) const { return embargoed.count(slot_id) != 0; }
  void AssignBuffer(MessageSlot *slot) { slot->buffer_index.store(0); }
  MessagePrefix *Prefix(MessageSlot *slot) { return &prefixes_[slot->id]; }
  void ForcedReuse(MessageSlot *slot) { forced_reuse.push_back(slot->id); }

  std::set<int> unusable;
  std::set<int> embargoed;
  std::vector<int> forced_reuse;

private:
  std::vector<MessagePrefix> prefixes_;
};

SlotSearch Search() {
  SlotSearch search;
  search.owner = TestChannel::kPublisherId;
  return search;
}

constexpr uint64_t kOwnedByPublisher =
    ::subspace::kPubOwned | TestChannel::kPublisherId;

// A subscriber reads the message in slot_id and releases it.
void ReadAndRelease(ChannelView &view, int sub_id, int slot_id,
                    bool reliable) {
  MessageSlot *slot = view.Slot(slot_id);
  ASSERT_TRUE(::subspace::ClaimMessageReference(
      view, slot, sub_id, reliable, slot->ordinal.load(),
      slot->vchan_id.load()));
  view.AvailableSlots(sub_id).Clear(slot_id);
  ::subspace::MarkMessageSeen(slot, reliable);
  EXPECT_TRUE(::subspace::ReleaseMessageReference(view, slot, sub_id, reliable)
                  .released);
}

// A subscriber clears its slot's retired bit only after it has released the
// slot, so a publisher can claim and publish in between, leaving the bit on
// an unread message.
TEST(SlotSearchTest, StaleRetiredBitOnAnUnreadMessageIsNotTaken) {
  for (bool reliable : {false, true}) {
    SCOPED_TRACE(reliable);
    TestChannel channel(/*num_slots=*/4, /*queue_arena_size=*/0);
    ChannelView &view = channel.View();
    channel.AddSubscriber(0, /*vchan_id=*/-1);
    for (int i = 0; i < 4; i++) {
      channel.Publish(i, /*vchan_id=*/-1, 10);
    }
    view.RetiredSlots().Set(3);
    const uint64_t unread_refs = view.Slot(3)->refs.load();

    TestHooks hooks(4);
    const ::subspace::ChannelCounters counters{};
    MessageSlot *slot =
        reliable
            ? ::subspace::ClaimSlotReliable(view, Search(), counters, hooks)
            : ::subspace::ClaimSlotUnreliable(view, Search(), hooks);

    // The oldest message goes instead.
    ASSERT_NE(nullptr, slot);
    EXPECT_EQ(0, slot->id);
    EXPECT_EQ(std::vector<int>{0}, hooks.forced_reuse);
    EXPECT_EQ(unread_refs, view.Slot(3)->refs.load());
    EXPECT_TRUE(view.AvailableSlots(0).IsSet(3));
    EXPECT_FALSE(view.RetiredSlots().IsSet(3));
  }
}

TEST(SlotSearchTest, ClaimedSlotIsReset) {
  TestChannel channel(/*num_slots=*/2, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.AddSubscriber(1, /*vchan_id=*/-1);
  channel.Publish(0, /*vchan_id=*/-1, 10);
  channel.Publish(1, /*vchan_id=*/-1, 10);
  ReadAndRelease(view, 0, 0, /*reliable=*/false);
  ReadAndRelease(view, 1, 0, /*reliable=*/false);
  ASSERT_TRUE(view.RetiredSlots().IsSet(0));

  TestHooks hooks(2);
  hooks.Prefix(view.Slot(0))->flags = ::subspace::kMessageActivate;
  SlotSearch search = Search();
  search.vchan_id = 5;
  MessageSlot *slot = ::subspace::ClaimSlotUnreliable(view, search, hooks);
  ASSERT_EQ(view.Slot(0), slot);
  EXPECT_EQ(kOwnedByPublisher, slot->refs.load());
  EXPECT_EQ(0u, slot->ordinal.load());
  EXPECT_EQ(0u, slot->timestamp.load());
  EXPECT_EQ(5, slot->vchan_id.load());
  EXPECT_EQ(0, slot->buffer_index.load());
  EXPECT_EQ(0, hooks.Prefix(slot)->flags);
  EXPECT_EQ(5, hooks.Prefix(slot)->vchan_id);
  EXPECT_TRUE(hooks.forced_reuse.empty());
  EXPECT_FALSE(view.RetiredSlots().IsSet(0));
}

TEST(SlotSearchTest, ForcedReuseForgetsTheUnreadMessage) {
  TestChannel channel(/*num_slots=*/2, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.AddSubscriber(1, /*vchan_id=*/-1);
  channel.Publish(0, /*vchan_id=*/-1, 10);
  channel.Publish(1, /*vchan_id=*/-1, 10);

  TestHooks hooks(2);
  MessageSlot *slot = ::subspace::ClaimSlotUnreliable(view, Search(), hooks);
  ASSERT_EQ(view.Slot(0), slot);
  EXPECT_EQ(std::vector<int>{0}, hooks.forced_reuse);
  EXPECT_FALSE(view.AvailableSlots(0).IsSet(0));
  EXPECT_FALSE(view.AvailableSlots(1).IsSet(0));
  EXPECT_TRUE(view.AvailableSlots(0).IsSet(1));
}

TEST(SlotSearchTest, UnreliablePrefersRetiredSlotsOnlyWhileSubscribed) {
  for (bool subscribed : {false, true}) {
    for (bool prefer_retired : {false, true}) {
      SCOPED_TRACE(testing::Message() << subscribed << " " << prefer_retired);
      TestChannel channel(/*num_slots=*/4, /*queue_arena_size=*/0);
      ChannelView &view = channel.View();
      view.FreeSlots().Set(2);
      view.FreeSlots().Set(3);
      if (subscribed) {
        channel.AddSubscriber(0, /*vchan_id=*/-1);
      }
      channel.Publish(0, /*vchan_id=*/-1, 10);
      channel.Publish(1, /*vchan_id=*/-1, 10);
      if (subscribed) {
        ReadAndRelease(view, 0, 1, /*reliable=*/false);
      } else {
        view.RetiredSlots().Set(1);
      }
      ASSERT_TRUE(view.RetiredSlots().IsSet(1));

      TestHooks hooks(4);
      SlotSearch search = Search();
      search.prefer_retired = prefer_retired;
      MessageSlot *slot = ::subspace::ClaimSlotUnreliable(view, search, hooks);
      ASSERT_NE(nullptr, slot);
      EXPECT_EQ(subscribed && prefer_retired ? 1 : 2, slot->id);
    }
  }
}

// With nothing subscribed every message retires when it's published.
TEST(SlotSearchTest, IdlePublisherRecyclesTheOldestRetiredSlot) {
  TestChannel channel(/*num_slots=*/4, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  const uint64_t timestamps[] = {400, 300, 100, 200};
  for (int i = 0; i < 4; i++) {
    channel.Publish(i, /*vchan_id=*/-1, 10, /*is_activation=*/false,
                    timestamps[i]);
    view.RetiredSlots().Set(i);
  }
  TestHooks hooks(4);
  MessageSlot *slot = ::subspace::ClaimSlotUnreliable(view, Search(), hooks);
  ASSERT_NE(nullptr, slot);
  EXPECT_EQ(2, slot->id);
}

TEST(SlotSearchTest, UnusableSlotIsGivenBackAndEmbargoed) {
  TestChannel channel(/*num_slots=*/3, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  for (int i = 0; i < 3; i++) {
    channel.Publish(i, /*vchan_id=*/-1, 10);
  }
  ReadAndRelease(view, 0, 0, /*reliable=*/false);
  const uint64_t retired_refs = view.Slot(0)->refs.load();

  TestHooks hooks(3);
  hooks.unusable.insert(0);
  MessageSlot *slot = ::subspace::ClaimSlotUnreliable(view, Search(), hooks);
  ASSERT_NE(nullptr, slot);
  EXPECT_EQ(1, slot->id);
  EXPECT_EQ(std::set<int>{0}, hooks.embargoed);
  EXPECT_EQ(retired_refs, view.Slot(0)->refs.load());
}

// A reliable publisher never overtakes a message that a reliable subscriber
// hasn't seen.
TEST(SlotSearchTest, ReliablePublisherStopsAtAnUnseenMessage) {
  TestChannel channel(/*num_slots=*/4, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  ::subspace::ChannelCounters counters{};
  counters.num_reliable_subs = 1;
  for (int i = 0; i < 4; i++) {
    channel.Publish(i, /*vchan_id=*/-1, 10);
  }
  for (int i : {0, 2}) {
    MessageSlot *s = view.Slot(i);
    ASSERT_TRUE(::subspace::SkipUnreadMessage(view, s, 0, /*reliable=*/true,
                                              s->ordinal.load(),
                                              s->vchan_id.load()));
  }

  TestHooks hooks(4);
  MessageSlot *slot =
      ::subspace::ClaimSlotReliable(view, Search(), counters, hooks);
  ASSERT_NE(nullptr, slot);
  EXPECT_EQ(0, slot->id);
  // Slot 2 was seen, but it's newer than the unseen message in slot 1.
  EXPECT_EQ(nullptr,
            ::subspace::ClaimSlotReliable(view, Search(), counters, hooks));
}

TEST(SlotSearchTest, ReliablePublisherDoesNotTakeAHeldSlot) {
  TestChannel channel(/*num_slots=*/2, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  ::subspace::ChannelCounters counters{};
  counters.num_reliable_subs = 1;
  channel.Publish(0, /*vchan_id=*/-1, 10);
  channel.Publish(1, /*vchan_id=*/-1, 10);
  MessageSlot *held = view.Slot(0);
  ASSERT_TRUE(::subspace::ClaimMessageReference(
      view, held, 0, /*reliable=*/true, held->ordinal.load(), -1));
  ::subspace::MarkMessageSeen(held, /*reliable=*/true);
  ReadAndRelease(view, 0, 1, /*reliable=*/true);

  TestHooks hooks(2);
  MessageSlot *slot =
      ::subspace::ClaimSlotReliable(view, Search(), counters, hooks);
  ASSERT_NE(nullptr, slot);
  EXPECT_EQ(1, slot->id);
  EXPECT_EQ(nullptr,
            ::subspace::ClaimSlotReliable(view, Search(), counters, hooks));
}

TEST(SubscribeTest, CommittedOrdinal) {
  TestChannel channel(/*num_slots=*/1, /*queue_arena_size=*/0);
  MessageSlot *slot = channel.View().Slot(0);
  EXPECT_EQ(0u, ::subspace::CommittedOrdinal(slot));
  channel.Publish(0, /*vchan_id=*/-1, 10);
  EXPECT_EQ(1u, ::subspace::CommittedOrdinal(slot));
  // A publisher reusing the slot zeroes the ordinal first.
  slot->ordinal.store(0);
  EXPECT_EQ(0u, ::subspace::CommittedOrdinal(slot));
  slot->ordinal.store(2);
  EXPECT_EQ(0u, ::subspace::CommittedOrdinal(slot));
  slot->ordinal.store(1);
  slot->refs.store(kOwnedByPublisher);
  EXPECT_EQ(0u, ::subspace::CommittedOrdinal(slot));
}

TEST(SubscribeTest, LastReleaseRetiresTheSlot) {
  TestChannel channel(/*num_slots=*/2, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.AddSubscriber(1, /*vchan_id=*/-1);
  channel.Publish(1, /*vchan_id=*/-1, 10);
  MessageSlot *slot = view.Slot(1);
  slot->bridged_slot_id.store(7);

  // Nothing to release without a reference.
  EXPECT_FALSE(
      ::subspace::ReleaseMessageReference(view, slot, 0, false).released);

  for (int sub_id : {0, 1}) {
    ASSERT_TRUE(::subspace::ClaimMessageReference(view, slot, sub_id, false,
                                                  1, -1));
    EXPECT_TRUE(slot->sub_owners.IsSet(sub_id));
  }
  const ::subspace::ReleasedMessage first =
      ::subspace::ReleaseMessageReference(view, slot, 0, false);
  EXPECT_TRUE(first.released);
  EXPECT_FALSE(first.retired);
  EXPECT_FALSE(slot->sub_owners.IsSet(0));

  const ::subspace::ReleasedMessage last =
      ::subspace::ReleaseMessageReference(view, slot, 1, false);
  EXPECT_TRUE(last.released);
  EXPECT_TRUE(last.retired);
  EXPECT_EQ(7, last.retirement_slot_id);
  EXPECT_TRUE(view.RetiredSlots().IsSet(1));
}

TEST(SubscribeTest, UnclaimLeavesTheMessageUnread) {
  TestChannel channel(/*num_slots=*/1, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.Publish(0, /*vchan_id=*/-1, 10);
  MessageSlot *slot = view.Slot(0);
  const uint64_t refs = slot->refs.load();
  ASSERT_TRUE(::subspace::ClaimMessageReference(view, slot, 0, false, 1, -1));
  ::subspace::UnclaimMessageReference(view, slot, 0, false);
  EXPECT_EQ(refs, slot->refs.load());
  EXPECT_FALSE(slot->sub_owners.IsSet(0));
  EXPECT_TRUE(view.AvailableSlots(0).IsSet(0));
  EXPECT_FALSE(view.RetiredSlots().IsSet(0));
}

TEST(SubscribeTest, SkippingARecycledSlotKeepsItsBit) {
  TestChannel channel(/*num_slots=*/1, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  channel.AddSubscriber(0, /*vchan_id=*/-1);
  channel.Publish(0, /*vchan_id=*/-1, 10);
  channel.Publish(0, /*vchan_id=*/-1, 10);
  MessageSlot *slot = view.Slot(0);

  // The subscriber's snapshot saw ordinal 1, which has been overwritten.
  EXPECT_FALSE(::subspace::SkipUnreadMessage(view, slot, 0, true, 1, -1));
  EXPECT_TRUE(view.AvailableSlots(0).IsSet(0));
  EXPECT_EQ(0, slot->flags.load() & ::subspace::kMessageSeenByReliable);

  EXPECT_TRUE(::subspace::SkipUnreadMessage(view, slot, 0, true, 2, -1));
  EXPECT_FALSE(view.AvailableSlots(0).IsSet(0));
  EXPECT_NE(0, slot->flags.load() & ::subspace::kMessageSeenByReliable);
  EXPECT_EQ(::subspace::BuildRefsBitField(2, -1, 0), slot->refs.load());
}

TEST(SubscribeTest, ActivationRetirementIsNotReported) {
  TestChannel channel(/*num_slots=*/2, /*queue_arena_size=*/0);
  ChannelView &view = channel.View();
  view.Slot(0)->flags.store(::subspace::kMessageIsActivation);
  EXPECT_FALSE(view.ReportsRetirement(0));
  EXPECT_TRUE(view.ReportsRetirement(1));
  // A bridged message names a slot in another channel.
  EXPECT_TRUE(view.ReportsRetirement(5));
  EXPECT_TRUE(view.ReportsRetirement(-1));
}

TEST(SubscribeTest, VirtualChannelsMatch) {
  EXPECT_TRUE(::subspace::VirtualChannelsMatch(-1, -1));
  EXPECT_TRUE(::subspace::VirtualChannelsMatch(-1, 3));
  EXPECT_TRUE(::subspace::VirtualChannelsMatch(3, -1));
  EXPECT_TRUE(::subspace::VirtualChannelsMatch(3, 3));
  EXPECT_FALSE(::subspace::VirtualChannelsMatch(3, 4));
}

TEST(PublishTest, EndNeverTakesActivePublishersBelowZero) {
  TestChannel channel(/*num_slots=*/1, /*queue_arena_size=*/0);
  ::subspace::AvailableSlotQueueIndex *index = channel.View().QueueIndex();
  std::atomic<uint32_t> &active = index->active_publishers[3];

  ::subspace::BeginSubscriberQueuePublish(index, 3);
  ::subspace::BeginSubscriberQueuePublish(index, 3);
  ::subspace::EndSubscriberQueuePublish(index, 3);
  EXPECT_EQ(1u, active.load());

  // The server zeroes the count when it removes the publisher.
  active.store(0);
  ::subspace::EndSubscriberQueuePublish(index, 3);
  EXPECT_EQ(0u, active.load());
}

// A prefix followed by its checksum storage and metadata.
struct PrefixBuffer {
  PrefixBuffer(int32_t checksum_size, int32_t metadata_size)
      : memory(::subspace::PrefixSize(checksum_size, metadata_size) /
                   sizeof(uint64_t) +
               1),
        prefix(new (memory.data()) MessagePrefix()) {}

  std::vector<uint64_t> memory;
  MessagePrefix *prefix;
};

uint32_t StandardChecksum(const MessagePrefix *prefix,
                          const std::vector<uint8_t> &payload,
                          int32_t checksum_size, int32_t metadata_size) {
  const ::subspace::MessageChecksumData data =
      ::subspace::GetMessageChecksumData(prefix, payload.data(), payload.size(),
                                         checksum_size, metadata_size);
  uint32_t checksum = 0;
  ::subspace::CalculateCRC32Checksum(
      data.Spans(), absl::Span<std::byte>(
                        reinterpret_cast<std::byte *>(&checksum),
                        sizeof(checksum)));
  return checksum;
}

TEST(ChecksumTest, MatchesStandardClient) {
  std::mt19937 random(42);
  for (int32_t checksum_size : {4, 16}) {
    for (int32_t metadata_size : {0, 1, 16, 100}) {
      for (size_t length : {0, 1, 7, 64, 1000}) {
        SCOPED_TRACE(testing::Message() << checksum_size << " "
                                        << metadata_size << " " << length);
        PrefixBuffer buffer(checksum_size, metadata_size);
        MessagePrefix *prefix = buffer.prefix;
        prefix->slot_id = static_cast<int32_t>(random() % 100);
        prefix->message_size = length;
        prefix->ordinal = random();
        prefix->timestamp = random();
        prefix->vchan_id = static_cast<int32_t>(random() % 10) - 1;
        prefix->checksum_size = static_cast<uint16_t>(checksum_size);
        prefix->metadata_size = static_cast<uint16_t>(metadata_size);
        prefix->SetHasChecksum();
        uint8_t *metadata = const_cast<uint8_t *>(
            ::subspace::ChecksummedMetadata(prefix, checksum_size));
        for (int32_t i = 0; i < metadata_size; i++) {
          metadata[i] = static_cast<uint8_t>(random());
        }
        std::vector<uint8_t> payload(length);
        for (uint8_t &byte : payload) {
          byte = static_cast<uint8_t>(random());
        }

        EXPECT_EQ(StandardChecksum(prefix, payload, checksum_size,
                                   metadata_size),
                  ::subspace::MessageChecksum(prefix, payload.data(), length,
                                              checksum_size, metadata_size));
      }
    }
  }
}

// The bridge and the tunnel set flags after the publisher has computed the
// checksum.
TEST(ChecksumTest, TransportFlagsDoNotInvalidateTheChecksum) {
  PrefixBuffer buffer(/*checksum_size=*/4, /*metadata_size=*/8);
  MessagePrefix *prefix = buffer.prefix;
  prefix->slot_id = 2;
  prefix->message_size = 5;
  prefix->ordinal = 17;
  prefix->timestamp = 99;
  prefix->checksum_size = 4;
  prefix->metadata_size = 8;
  prefix->SetHasChecksum();
  const std::vector<uint8_t> payload = {1, 2, 3, 4, 5};
  ::subspace::SetMessageChecksum(prefix, payload.data(), payload.size(), 4, 8);

  prefix->SetIsBridged();
  prefix->SetIsCrossMachine();
  prefix->flags |= int64_t{1} << 40;
  EXPECT_TRUE(::subspace::MessageChecksumMatches(prefix, payload.data(),
                                                 payload.size(), 4, 8));
  const ::subspace::MessageChecksumData data =
      ::subspace::GetMessageChecksumData(prefix, payload.data(), payload.size(),
                                         4, 8);
  EXPECT_TRUE(::subspace::VerifyCRC32Checksum(
      data.Spans(), ::subspace::GetChecksumSpan(
                        static_cast<const MessagePrefix *>(prefix), 4)));

  prefix->SetIsActivation();
  EXPECT_FALSE(::subspace::MessageChecksumMatches(prefix, payload.data(),
                                                  payload.size(), 4, 8));
}

} // namespace
