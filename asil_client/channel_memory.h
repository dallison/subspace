// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Internal to the ASIL client: the mapped shared memory of one channel and the
// slot operations that publishers and subscribers share.

#pragma once

#include "asil_client/bitset.h"
#include "asil_client/error.h"
#include "asil_client/server_connection.h"
#include "asil_client/shm_layout.h"

#include <cstddef>
#include <cstdint>

namespace subspace {
namespace asil {

constexpr size_t kMaxChannelNameLength = 255;

namespace internal {

// The clock used for message timestamps, which must match toolbelt::Now().
uint64_t Now();

// Wakes the owner of a trigger fd: an eventfd on Linux, a pipe elsewhere.
void Trigger(int fd);
// Drains a poll fd.
void ClearTrigger(int fd);

// Copies name into a fixed buffer.
Error CopyName(const char *name, char (&out)[kMaxChannelNameLength + 1]);

class ChannelMemory {
public:
  ChannelMemory() = default;
  ~ChannelMemory() { Unmap(); }
  ChannelMemory(const ChannelMemory &) = delete;
  ChannelMemory &operator=(const ChannelMemory &) = delete;

  // Maps the CCB and BCB.  The SCB is mapped by the client.
  Error Map(shm::SystemControlBlock *scb, int ccb_fd, int bcb_fd,
            int num_slots, int32_t checksum_size, int32_t metadata_size);
  void Unmap();

  // Publishers create the channel's buffer, or attach to the buffer another
  // publisher created.  The buffer's slot size must be slot_size.
  Error CreateOrAttachBuffer(const char *channel_name, uint64_t session_id,
                             int64_t slot_size, int32_t user_id,
                             int32_t group_id);
  // Subscribers attach to the buffer read-only once a publisher has made it.
  // Returns kOk without a buffer when there isn't one yet.
  Error AttachBuffer(const char *channel_name, uint64_t session_id);
  bool HasBuffer() const { return buffer_ != nullptr; }

  int NumSlots() const { return num_slots_; }
  int64_t SlotSize() const { return slot_size_; }
  int32_t ChecksumSize() const { return checksum_size_; }
  int32_t MetadataSize() const { return metadata_size_; }

  shm::ChannelControlBlock *Ccb() const { return ccb_; }
  shm::BufferControlBlock *Bcb() const { return bcb_; }
  const shm::ChannelCounters &Counters(int channel_id) const {
    return scb_->counters[channel_id];
  }

  shm::MessageSlot *Slot(int id) const;
  shm::MessagePrefix *Prefix(const shm::MessageSlot *slot) const;
  char *Payload(const shm::MessageSlot *slot) const;

  BitsetView RetiredSlots() const;
  BitsetView FreeSlots() const;
  BitsetView AvailableSlots(int sub_id) const;
  BitsetView Subscribers() const;
  shm::AvailableSlotQueueIndex *QueueIndex() const;

  int NumSubscribers(int vchan_id) const;
  uint64_t CleanupGeneration(int vchan_id) const;

  // Channel::AtomicIncRefCount.  Sets *retired when this call retired the
  // slot.
  bool AtomicIncRefCount(shm::MessageSlot *slot, int inc, uint64_t ordinal,
                         int vchan_id, bool retire, bool *retired);
  // Channel::TryRetireSlot.
  bool TryRetireSlot(shm::MessageSlot *slot);

  // Tells publishers that asked for retirement notification that slot_id is
  // free.
  void TriggerRetirement(const TriggerFdList &triggers, int32_t slot_id) const;

private:
  Error BufferName(const char *channel_name, uint64_t session_id,
                   char (&out)[kMaxChannelNameLength + 64]) const;
  Error MapBuffer(int fd, uint64_t size, int prot);

  shm::SystemControlBlock *scb_ = nullptr;
  shm::ChannelControlBlock *ccb_ = nullptr;
  shm::BufferControlBlock *bcb_ = nullptr;
  size_t ccb_size_ = 0;
  int num_slots_ = 0;
  int32_t checksum_size_ = 4;
  int32_t metadata_size_ = 0;
  int32_t prefix_size_ = 64;

  char *buffer_ = nullptr;
  uint64_t buffer_size_ = 0;
  int64_t slot_size_ = 0;
};

} // namespace internal
} // namespace asil
} // namespace subspace
