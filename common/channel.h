
// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xCOMMON_CHANNEL_H
#define _xCOMMON_CHANNEL_H

#include <stdio.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/atomic_bitset.h"
#include "toolbelt/bitset.h"
#include "toolbelt/fd.h"

#include <atomic>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

namespace subspace {

// Flag for flags field in MessagePrefix.
constexpr int kMessageActivate = 1;    // This is a reliable activation message.
constexpr int kMessageBridged = 2;     // This message came from the bridge.
constexpr int kMessageHasChecksum = 4; // This message has a checksum.

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
  uint32_t checksum;
  char padding2[64 - 48]; // Align to 64 bytes.

  bool IsActivation() const { return (flags & kMessageActivate) != 0; }
  void SetIsActivation() { flags |= kMessageActivate; }
  bool IsBridged() const { return (flags & kMessageBridged) != 0; }
  void SetIsBridged() { flags |= kMessageBridged; }
  bool HasChecksum() const { return (flags & kMessageHasChecksum) != 0; }
  void SetHasChecksum() { flags |= kMessageHasChecksum; }
};

static_assert(sizeof(MessagePrefix) == 64,
              "MessagePrefix size is not 64 bytes");

// Flags for MessageSlot flags.
constexpr int kMessageSeen = 1;         // Message has been seen.
constexpr int kMessageIsActivation = 2; // This is an activation message.

// We need a max channels number because the size of things in
// shared memory needs to be fixed.
constexpr int kMaxChannels = 1024;

// Maximum number of owners for a lot.  One per subscriber reference
// and publisher reference.  Best if it's a multiple of 64 because
// it's used as the size in a toolbelt::BitSet.
constexpr int kMaxSlotOwners = 1024;

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
  uint64_t ordinal;           // Message ordinal held currently in slot.
  uint64_t message_size;      // Size of message held in slot.
  int32_t id;                 // Unique ID for slot (0...num_slots-1).
  int16_t buffer_index;       // Index of buffer.
  int16_t vchan_id;           // Virtual channel ID.
  AtomicBitSet<kMaxSlotOwners> sub_owners; // One bit per subscriber.
  uint64_t timestamp;                      // Timestamp of message.
  uint32_t flags;
  int32_t bridged_slot_id; // Slot ID of other side of bridge.

  void Dump(std::ostream &os) const;
};

struct ActiveSlot {
  MessageSlot *slot;
  uint64_t ordinal;
  uint64_t timestamp;
  int vchan_id;
};

struct BufferControlBlock {
  std::atomic<int32_t>
      refs[kMaxBuffers]; // Number of references to this buffer.
  std::atomic<uint64_t>
      sizes[kMaxBuffers]; // Number of references to this buffer.
};

// Given a message prefix and a buffer containing the message data return a vector of spans
// that can be used to calculate the checksum.
inline std::array<absl::Span<const uint8_t>, 2>
GetMessageChecksumData(MessagePrefix *prefix, void *buffer,
                       size_t message_size) {
  std::array<absl::Span<const uint8_t>, 2> data = {
      absl::Span<const uint8_t>(reinterpret_cast<const uint8_t *>(
                                    prefix) + offsetof(MessagePrefix, slot_id),
                                offsetof(MessagePrefix, checksum) -
                                    offsetof(MessagePrefix, slot_id)),
      absl::Span<const uint8_t>(reinterpret_cast<const uint8_t *>(buffer),
                                message_size)};
  return data;
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
  //
};

inline size_t AvailableSlotsSize(int num_slots) {
  return SizeofAtomicBitSet(num_slots) * kMaxSlotOwners;
}

inline size_t CcbSize(int num_slots) {
  return Aligned(sizeof(ChannelControlBlock) +
                 num_slots * sizeof(MessageSlot)) +
         Aligned(SizeofAtomicBitSet(num_slots)) * 2 +
         AvailableSlotsSize(num_slots);
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
          std::string type, std::function<bool(Channel *)> reload = nullptr);
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
    ccb_->subscribers.Set(sub_id);
    ccb_->sub_vchan_ids[sub_id] = vchan_id;
    if (is_new) {
      ccb_->num_subs.AddSubscriber(vchan_id);
    }
  }

  int GetSubVchanId(int32_t i) const { return ccb_->sub_vchan_ids[i]; }

  void DumpSlots(std::ostream &os) const;
  virtual void Dump(std::ostream &os) const;

  uint64_t BufferSizeToSlotSize(uint64_t size) const {
    if (size < NumSlots() * sizeof(MessagePrefix)) {
      return 0;
    }
    return (size - NumSlots() * sizeof(MessagePrefix)) / NumSlots();
  };

  uint64_t SlotSizeToBufferSize(uint64_t slot_size) const {
    return NumSlots() * (slot_size + sizeof(MessagePrefix));
  }

  // Get the number of slots in the channel (can't be changed)
  int NumSlots() const { return num_slots_; }
  virtual void SetNumSlots(int n) { num_slots_ = n; }

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

  virtual MessagePrefix *Prefix(MessageSlot *slot) { return nullptr; }

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

  int channel_id_; // ID allocated from server.
  std::string type_;

  uint16_t num_updates_ = 0;

  SystemControlBlock *scb_ = nullptr;
  ChannelControlBlock *ccb_ = nullptr;
  BufferControlBlock *bcb_ = nullptr;
  bool debug_ = false;
  std::function<bool(Channel *)> reload_callback_;
};

} // namespace subspace
#endif /* _xCOMMON_CHANNEL_H */
