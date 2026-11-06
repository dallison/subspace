
// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xCOMMON_CHANNEL_H
#define _xCOMMON_CHANNEL_H

#include <stdio.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "common/channel_view.h"
#include "common/message_checksum.h"
#include "common/publish.h"
#include "common/shm_layout.h"
#include "toolbelt/bitset.h"
#include "toolbelt/fd.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <string>

namespace subspace {


// Slot sizes are 64 bit internally: the wire protocol, the shared memory
// layout and all of the buffer arithmetic use 64 bit values regardless of how
// the client is built.  The C and C++ client APIs, however, expose a 32 bit
// slot size by default so that code written against the older API keeps
// compiling.  Define SUBSPACE_64BIT_SLOT_SIZE to widen the API types and allow
// slots larger than 2GB to be requested and reported.  See //:slot_size_64
// (Bazel) and the SUBSPACE_64BIT_SLOT_SIZE CMake option.
//
// The two builds interoperate: a client built without the macro can attach to
// a channel whose slots are larger than 2GB, but its API will report a
// truncated slot size, so use the macro on both sides if you need large slots.
#if defined(SUBSPACE_64BIT_SLOT_SIZE)
using SlotSizeType = int64_t;
#else
using SlotSizeType = int32_t;
#endif

std::string DecodedRefsBitField(uint64_t refs);


void *MapMemory(int fd, size_t size, int prot, const char *purpose);
void UnmapMemory(void *p, size_t size, const char *purpose);


// The data covered by a message checksum, as three spans:
//   [0] prefix fields before the checksum (slot_id through metadata_size),
//       with flags masked to kChecksummedMessageFlags
//   [1] user metadata area (immediately after the checksum storage)
//   [2] message payload
//
// Span 0 points at a masked copy held in this object, not at the prefix, so
// the object must outlive any use of Spans().  It cannot be copied or moved
// because that would leave span 0 pointing at the old copy.
class MessageChecksumData {
public:
  MessageChecksumData(const MessagePrefix *prefix, const void *buffer,
                      size_t message_size, int32_t checksum_size,
                      int32_t metadata_size)
      : prefix_fields_(prefix) {
    spans_ = {absl::Span<const uint8_t>(prefix_fields_.Data(),
                                        prefix_fields_.Size()),
              absl::Span<const uint8_t>(
                  ChecksummedMetadata(prefix, checksum_size), metadata_size),
              absl::Span<const uint8_t>(
                  reinterpret_cast<const uint8_t *>(buffer), message_size)};
  }

  MessageChecksumData(const MessageChecksumData &) = delete;
  MessageChecksumData &operator=(const MessageChecksumData &) = delete;

  const std::array<absl::Span<const uint8_t>, 3> &Spans() const {
    return spans_;
  }

private:
  ChecksummedPrefixFields prefix_fields_;
  std::array<absl::Span<const uint8_t>, 3> spans_;
};

inline MessageChecksumData
GetMessageChecksumData(const MessagePrefix *prefix, const void *buffer,
                       size_t message_size, int32_t checksum_size,
                       int32_t metadata_size) {
  return MessageChecksumData(prefix, buffer, message_size, checksum_size,
                             metadata_size);
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
  SlotBuffer(int64_t slot_sz) : slot_size(slot_sz) {}
  SlotBuffer(int64_t slot_sz, toolbelt::FileDescriptor f)
      : slot_size(slot_sz), fd(std::move(f)) {}
  int64_t slot_size;
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
    const bool was_registered = ccb_->subscribers.IsSet(sub_id);
    const bool register_membership = is_new || !was_registered;
    SubscriberCounter num_subs;
    ccb_->subscribers.Traverse([this, &num_subs](size_t id) {
      num_subs.AddSubscriber(ccb_->sub_vchan_ids[id]);
    });
    if (register_membership && !was_registered) {
      num_subs.AddSubscriber(vchan_id);
      if (!IsPlaceholder()) {
        GetAvailableSlots(sub_id).ClearAll();
      }
      // Publish the increased retirement threshold before making the
      // subscriber visible to publishers.
    }
    ccb_->num_subs = num_subs;
    ccb_->subscribers.SetSeqCst(sub_id);
    if (register_membership && !was_registered && !IsPlaceholder()) {
      SeedAvailableSlotQueue(sub_id, vchan_id);
    }
  }

  int GetSubVchanId(int32_t i) const { return ccb_->sub_vchan_ids[i]; }

  void SeedAvailableSlotQueue(int sub_id, int vchan_id) {
    InPlaceAtomicBitset &bits = GetAvailableSlots(sub_id);
    InPlaceSlotQueue *queue = GetAvailableSlotQueueAddress(sub_id);
    // Include a non-zero publisher-owned generation. This closes the inverse
    // registration race: a publisher may have snapshotted subscribers before
    // this subscriber is registered, while the registration scan happens
    // before that publisher commits. Consumers preserve the bit/queue entry
    // until kPubOwned is cleared.
    auto visible = [vchan_id](MessageSlot &slot) {
      const uint64_t ordinal = slot.ordinal.load(std::memory_order_relaxed);
      const int buffer_index =
          slot.buffer_index.load(std::memory_order_relaxed);
      if (ordinal == 0 || buffer_index == -1) {
        return false;
      }
      return VirtualChannelsMatch(
          vchan_id, slot.vchan_id.load(std::memory_order_relaxed));
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
        queue->Push(best->id, best->ordinal.load(std::memory_order_relaxed),
                    /*report_insertion_failure=*/true,
                    [this, sub_id](const QueuedSlot &evicted) {
                      return View().ForgetEvictedSlot(
                          View().AvailableSlots(sub_id), evicted,
                          [](int32_t) {});
                    });
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
    return ::subspace::PrefixSize(checksum_size, metadata_size);
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

  // The prefix size is derived from the checksum and metadata sizes rather
  // than chosen independently, so set all three together; updating them
  // separately lets a channel report a prefix that disagrees with the layout
  // it describes.  Goes through the virtual setters, so a server-side
  // VirtualChannel still forwards the layout to its multiplexer.
  void SetPrefixLayout(int32_t checksum_size, int32_t metadata_size) {
    SetChecksumSize(checksum_size);
    SetMetadataSize(metadata_size);
    SetPrefixSize(ComputePrefixSize(checksum_size, metadata_size));
  }

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

  // The mapped channel control block, for the slot operations shared with
  // other clients.
  ChannelView View() const {
    return ChannelView(ccb_, num_slots_, SubscriberQueueArenaSize());
  }

  bool TryRetireSlot(MessageSlot *slot) { return View().TryRetireSlot(slot); }

  bool RefsShowRetired(const MessageSlot *slot, uint64_t refs) const {
    return View().RefsShowRetired(slot, refs);
  }

  int NumSubscribers(int vchan_id) const {
    return ccb_->num_subs.NumSubscribers(vchan_id);
  }

  uint64_t SubscriberCleanupGenerationFor(int vchan_id) const {
    return ccb_->subscriber_cleanup_generation.Get(vchan_id);
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
                        uint64_t &max_message_size, uint32_t &total_drops);

  void SetDebug(bool v) { debug_ = v; }

  virtual MessagePrefix *Prefix(MessageSlot */*slot*/) { return nullptr; }

  bool AtomicIncRefCount(MessageSlot *slot, bool reliable, int inc,
                         uint64_t ordinal, int vchan_id, bool retire,
                         std::function<void()> retire_callback = {});

  void SetType(std::string type) { type_ = std::move(type); }
  const std::string Type() const { return type_; }

  std::string_view TypeView() const { return type_; }

  char *EndOfSlots() const {
    return reinterpret_cast<char *>(ccb_) + RetiredSlotsOffset(num_slots_);
  }
  char *EndOfRetiredSlots() const {
    return reinterpret_cast<char *>(ccb_) + FreeSlotsOffset(num_slots_);
  }
  char *EndOfFreeSlots() const {
    return reinterpret_cast<char *>(ccb_) + AvailableSlotsOffset(num_slots_, 0);
  }
  char *EndOfAvailableSlots() const {
    return reinterpret_cast<char *>(ccb_) + SlotQueueIndexOffset(num_slots_);
  }
  char *EndOfAvailableSlotQueueIndex() const {
    return reinterpret_cast<char *>(ccb_) + SlotQueueArenaOffset(num_slots_);
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
        reinterpret_cast<char *>(ccb_) +
        AvailableSlotsOffset(num_slots_, sub_id));
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
    ::subspace::BeginSubscriberQueuePublish(
        GetAvailableSlotQueueIndexAddress(), pub_id);
  }

  void EndSubscriberQueuePublish(int pub_id) {
    ::subspace::EndSubscriberQueuePublish(GetAvailableSlotQueueIndexAddress(),
                                          pub_id);
  }

  bool IsActivated(int vchan_id) const {
    return ccb_->activation_tracker.IsActivated(vchan_id);
  }

  virtual uint64_t GetVirtualMemoryUsage() const;

protected:
  // Unmaps the channel and buffer control blocks and leaves the system
  // control block mapped. A reload uses this so other threads can keep
  // reading channel counters from the system control block.
  void UnmapChannelMemory();

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
