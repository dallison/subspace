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
#include "asil_client/split_buffer.h"

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

// Waits until fd is readable.  A negative timeout waits forever.
Error WaitReadable(int fd, int timeout_ms);

// Copies name into a fixed buffer.
Error CopyName(const char *name, char (&out)[kMaxChannelNameLength + 1]);

// Where a channel's message buffers live.
struct BufferContext {
  // The channel that owns the buffers: the multiplexer of a virtual channel.
  const char *channel_name = nullptr;
  uint64_t session_id = 0;
  int32_t user_id = -1;
  int32_t group_id = -1;
  // Passes memfd buffers between clients.
  ServerConnection *connection = nullptr;
};

// InPlaceSlotQueue::Push with report_insertion_failure false.
bool PushSlotQueue(shm::SlotQueue *queue, int32_t slot_id, uint64_t ordinal);
// InPlaceSlotQueue::MarkInsertionFailure.
void MarkSlotQueueInsertionFailure(shm::SlotQueue *queue);

class ChannelMemory {
public:
  ChannelMemory() = default;
  ~ChannelMemory() { Unmap(); }
  ChannelMemory(const ChannelMemory &) = delete;
  ChannelMemory &operator=(const ChannelMemory &) = delete;

  // Maps the CCB and BCB.  The SCB is mapped by the client.  The context must
  // outlive the mapping.
  Error Map(shm::SystemControlBlock *scb, int ccb_fd, int bcb_fd, int num_slots,
            int32_t checksum_size, int32_t metadata_size,
            uint64_t subscriber_queue_arena_size, const BufferContext *context);
  void Unmap();
  bool Mapped() const { return ccb_ != nullptr; }

  // A publisher or subscriber maps one buffer, the channel's newest, when it
  // opens.  Nothing is mapped after that.

  // Maps the newest buffer, creating the channel's first buffer if there is
  // none.  The buffer's slots must hold at least slot_size bytes.
  Error CreateOrAttachBuffer(int64_t slot_size);
  // Maps the newest buffer.  Returns kNoBuffers if there is none.
  Error AttachBuffer(bool writable);
  // Maps the newest split buffer set: the prefixes, and each slot's payload
  // into slots, which needs room for NumSlots() entries.  The allocator maps
  // slots that a custom allocator created.  Returns kNoBuffers if there are
  // none.
  Error AttachSplitBuffers(bool writable, SplitSlot *slots, int32_t capacity,
                           const SplitBufferAllocator &allocator);
  // The mapped buffer, or -1.
  int BufferIndex() const { return index_; }
  // Whether slot's message is in the mapped buffer.
  bool InMappedBuffer(const shm::MessageSlot *slot) const {
    return index_ != -1 &&
           slot->buffer_index.load(std::memory_order_relaxed) == index_;
  }

  int NumSlots() const { return num_slots_; }
  // The slot size of the mapped buffer.
  int64_t SlotSize() const { return slot_size_; }
  int32_t ChecksumSize() const { return checksum_size_; }
  int32_t MetadataSize() const { return metadata_size_; }

  shm::ChannelControlBlock *Ccb() const { return ccb_; }
  shm::BufferControlBlock *Bcb() const { return bcb_; }
  const shm::ChannelCounters &Counters(int channel_id) const {
    return scb_->counters[channel_id];
  }

  shm::MessageSlot *Slot(int id) const;
  // The prefix and payload of slot, or null when it isn't in the mapped
  // buffer.
  shm::MessagePrefix *Prefix(const shm::MessageSlot *slot) const;
  char *Payload(const shm::MessageSlot *slot) const;

  BitsetView RetiredSlots() const;
  BitsetView FreeSlots() const;
  BitsetView AvailableSlots(int sub_id) const;
  BitsetView Subscribers() const;
  shm::AvailableSlotQueueIndex *QueueIndex() const;
  // The subscriber's queue, or null if it has none.
  shm::SlotQueue *Queue(int sub_id) const;

  int NumSubscribers(int vchan_id) const;
  uint64_t CleanupGeneration(int vchan_id) const;

  // Channel::AtomicIncRefCount.  Sets *retired when this call retired the
  // slot.
  bool AtomicIncRefCount(shm::MessageSlot *slot, bool reliable, int inc,
                         uint64_t ordinal, int vchan_id, bool retire,
                         bool *retired);
  // Channel::TryRetireSlot.
  bool TryRetireSlot(shm::MessageSlot *slot);

  // Tells publishers that asked for retirement notification that slot_id is
  // free.
  void TriggerRetirement(const TriggerFdList &triggers, int32_t slot_id) const;

private:
  Error BufferName(int index, char (&out)[kMaxChannelNameLength + 64]) const;
  Error NewestIndex(int &index) const;
  Error OpenBuffer(int index, bool writable, UniqueFd &fd,
                   uint64_t &size) const;
  Error MapBuffer(int index, int fd, uint64_t size, int prot);
  Error CreateBuffer(uint64_t size);
  Error GetRegisteredBuffer(int index, bool is_prefix, uint32_t slot_id,
                            BufferReply &reply) const;
  Error MapSplitSlot(int slot_id, uint64_t payload_size, int prot);
  void UnmapBuffers();

  shm::SystemControlBlock *scb_ = nullptr;
  shm::ChannelControlBlock *ccb_ = nullptr;
  shm::BufferControlBlock *bcb_ = nullptr;
  const BufferContext *context_ = nullptr;
  size_t ccb_size_ = 0;
  size_t queue_arena_size_ = 0;
  int num_slots_ = 0;
  int32_t checksum_size_ = 4;
  int32_t metadata_size_ = 0;
  int32_t prefix_size_ = 64;

  // The mapped buffer: the whole buffer, or the prefixes of a split buffer
  // set.
  int index_ = -1;
  char *base_ = nullptr;
  uint64_t size_ = 0;
  int64_t slot_size_ = 0;
  // The caller's slot storage for a split buffer set, or null.
  SplitSlot *split_slots_ = nullptr;
  SplitBufferAllocator allocator_;
};

} // namespace internal
} // namespace asil
} // namespace subspace
