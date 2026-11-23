// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

// Names for the shared-memory protocol. The types live in common/shm_layout.h
// and are the same types the regular client uses. The layout is valid on
// 32-bit targets: size_t fields are padded so 64-bit atomics stay aligned.

#pragma once

#include "common/shm_layout.h"

namespace subspace {
namespace asil {
namespace shm {

using ::subspace::ActivationTracker;
using ::subspace::AvailableSlotQueueIndex;
using ::subspace::AvailableSlotsOffset;
using ::subspace::BitsToWords;
using ::subspace::BufferControlBlock;
using ::subspace::BuildRefsBitField;
using ::subspace::CcbSize;
using ::subspace::ChannelControlBlock;
using ::subspace::ChannelCounters;
using ::subspace::FreeSlotsOffset;
using ::subspace::InPlaceSlotQueue;
using ::subspace::MessagePrefix;
using ::subspace::MessageSlot;
using ::subspace::OrdinalAccumulator;
using ::subspace::PrefixSize;
using ::subspace::RefsVchanId;
using ::subspace::RetiredSlotsOffset;
using ::subspace::SizeofAtomicBitSet;
using ::subspace::SlotQueueArenaOffset;
using ::subspace::SlotQueueEntry;
using ::subspace::SlotQueueIndexOffset;
using ::subspace::SubscriberCleanupGeneration;
using ::subspace::SubscriberCounter;
using ::subspace::SystemControlBlock;
using SlotQueue = ::subspace::InPlaceSlotQueue;

using ::subspace::kChannelControlBlockVersion;
using ::subspace::kInvalidSlotQueueOffset;
using ::subspace::kMaxBuffers;
using ::subspace::kMaxChannelName;
using ::subspace::kMaxChannels;
using ::subspace::kMaxSlotOwners;
using ::subspace::kMaxSlotQueueCasAttempts;
using ::subspace::kMaxVchanId;
using ::subspace::kMessageActivate;
using ::subspace::kMessageHasChecksum;
using ::subspace::kMessageIsActivation;
using ::subspace::kMessageSeen;
using ::subspace::kMessageSeenByReliable;
using ::subspace::kOrdinalMask;
using ::subspace::kOrdinalShift;
using ::subspace::kPubOwned;
using ::subspace::kRefCountMask;
using ::subspace::kRefsMask;
using ::subspace::kReliableRefCountShift;
using ::subspace::kPendingMask;
using ::subspace::kPendingShift;
using ::subspace::PendingCount;
using ::subspace::kVchanIdMask;
using ::subspace::kVchanIdShift;

inline size_t SizeofBitset(size_t bits) {
  return ::subspace::SizeofAtomicBitSet(bits);
}

constexpr int64_t Aligned64(int64_t v) { return ::subspace::Aligned<64>(v); }

} // namespace shm
} // namespace asil
} // namespace subspace
