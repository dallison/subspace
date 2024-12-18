
// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_CHANNEL_H
#define __COMMON_CHANNEL_H

#include <stdio.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/atomic_bitset.h"
#include "toolbelt/bitset.h"
#include "toolbelt/fd.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace subspace {

// Max message size for comms with server.
static constexpr size_t kMaxMessage = 4096;

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
  uint32_t message_size;
  uint64_t ordinal;
  uint64_t timestamp;
  int64_t flags;
};

// Flag for flags field in MessagePrefix.
constexpr int kMessageActivate = 1; // This is a reliable activation message.
constexpr int kMessageBridged = 2;  // This message came from the bridge.
constexpr int kMessageSeen = 4;     // Message has been seen.

// We need a max channels number because the size of things in
// shared memory needs to be fixed.
constexpr int kMaxChannels = 1024;

// Maximum number of owners for a lot.  One per subscriber reference
// and publisher reference.  Best if it's a multiple of 64 because
// it's used as the size in a toolbelt::BitSet.
constexpr int kMaxSlotOwners = 1024;

// Max length of a channel name in shared memory.  A name longer
// this this will be truncated but the full name will be available
// in process memory.
constexpr size_t kMaxChannelName = 64;

// Ref count bits.
constexpr uint32_t kRefCountMask = 0x3ff;
constexpr uint32_t kRefCountShift = 10;
constexpr uint32_t kReliableRefCountShift = 10;
constexpr uint32_t kPubOwned = 0x80000000;
constexpr uint32_t kRefsMask = 0xfffff; // 20 bits

// We put the bottom 11 bits of the ordinal just above the
// reliable ref count field.  This is to ensure that a publisher
// hasn't published another message in the slot before a subscriber
// increments the ref count.
constexpr uint32_t kOrdinalMask = 0x7ff;    // 11 bits
constexpr uint32_t kOrdinalShift = 20;

// Aligned to given power of 2.
template <int64_t alignment> int64_t Aligned(int64_t v) {
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
};

struct SystemControlBlock {
  ChannelCounters counters[kMaxChannels];
};

// This is the meta data for a slot.  It is always in a linked list.
struct MessageSlot {
  int32_t id;                 // Unique ID for slot (0...num_slots-1).
  std::atomic<uint32_t> refs; // Number of subscribers referring to this slot.
  uint64_t ordinal;           // Message ordinal held currently in slot.
  uint64_t message_size;      // Size of message held in slot.
  int32_t buffer_index;       // Index of buffer.
  AtomicBitSet<kMaxSlotOwners> sub_owners; // One bit per subscriber.
};

struct ActiveSlot {
  MessageSlot *slot;
  uint64_t ordinal;
  uint64_t timestamp;
};

// This is located just before the prefix of the first slot's buffer.  It
// is 64 bits long to align the prefix to 64 bits.
struct BufferHeader {
  int32_t refs;    // Number of references to this buffer.
  int32_t padding; // Align to 64 bits.
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
  std::atomic<int64_t> next_ordinal; // Next ordinal to use.
  int buffer_index;     // Which buffer in buffers array to use.
  int num_buffers;      // Size of buffers array in shared memory.
  AtomicBitSet<kMaxSlotOwners> subscribers; // One bit per subscriber.

  // Statistics counters.
  std::atomic<uint64_t> total_bytes;
  std::atomic<uint64_t> total_messages;
  std::atomic<uint32_t> max_message_size;
  std::atomic<uint32_t> total_drops;

  // Variable number of MessageSlot structs (num_slots long).
  MessageSlot slots[0];
  // Followed by:
  // AtomicBitSet<0> availableSlots[kMaxSlotOwners]; 
};

inline size_t AvailableSlotsSize(int num_slots) {
  return SizeofAtomicBitSet(num_slots) * kMaxSlotOwners;
}

inline size_t CcbSize(int num_slots) {
  return sizeof(ChannelControlBlock) + num_slots * sizeof(MessageSlot) +
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
  SharedMemoryFds(toolbelt::FileDescriptor ccb_fd, std::vector<SlotBuffer> bufs)
      : ccb(std::move(ccb_fd)), buffers(std::move(bufs)) {}
  SharedMemoryFds(const SharedMemoryFds &) = delete;

  SharedMemoryFds(SharedMemoryFds &&c) {
    ccb = std::move(c.ccb);
    buffers = std::move(c.buffers);
  }
  SharedMemoryFds &operator=(const SharedMemoryFds &) = delete;

  SharedMemoryFds &operator=(SharedMemoryFds &&c) {
    ccb = std::move(c.ccb);
    buffers = std::move(c.buffers);
    return *this;
  }

  toolbelt::FileDescriptor ccb;    // Channel Control Block.
  std::vector<SlotBuffer> buffers; // Message Buffers.
};

struct BufferSet {
  BufferSet() = default;
  BufferSet(int32_t slot_sz, char *buf) : slot_size(slot_sz), buffer(buf) {}
  int32_t slot_size = 0;
  char *buffer = nullptr;
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
          std::string type);
  ~Channel() { Unmap(); }

  void Unmap();

  const std::string &Name() const { return name_; }

  // For debug, prints the contents of the three linked lists in
  // shared memory,
  void PrintLists() const;

  // A placeholder is a channel created for a subscriber where there are
  // no publishers and thus the shared memory is not yet valid.
  bool IsPlaceholder() const { return NumSlots() == 0; }

  // What is the address of the message buffer (after the MessagePrefix)
  // for the slot given a slot id.
  void *GetBufferAddress(int slot_id) const {
    return Buffer(slot_id) +
           (sizeof(MessagePrefix) + Aligned<32>(SlotSize(slot_id))) * slot_id +
           sizeof(MessagePrefix);
  }

  // Gets the address for the message buffer given a slot pointer.
  void *GetBufferAddress(MessageSlot *slot) const {
    if (slot == nullptr) {
      return nullptr;
    }
    return Buffer(slot->id) +
           (sizeof(MessagePrefix) + Aligned<32>(SlotSize(slot->id))) *
               slot->id +
           sizeof(MessagePrefix);
  }

  void ReloadIfNecessary(const std::function<bool()>& reload);

  // Get a pointer to the MessagePrefix for a given slot.
  MessagePrefix *Prefix(MessageSlot *slot, const std::function<bool()>& reload) {
    ReloadIfNecessary(reload);
    MessagePrefix *p = reinterpret_cast<MessagePrefix *>(
        Buffer(slot->id) +
        (sizeof(MessagePrefix) + Aligned<32>(SlotSize(slot->id))) * slot->id);
    return p;
  }

 MessagePrefix *Prefix(MessageSlot *slot) const {
    MessagePrefix *p = reinterpret_cast<MessagePrefix *>(
        Buffer(slot->id) +
        (sizeof(MessagePrefix) + Aligned<32>(SlotSize(slot->id))) * slot->id);
    return p;
  }

  void RegisterSubscriber(int sub_id) { ccb_->subscribers.Set(sub_id); }

  void DumpSlots() const;
  void Dump() const;

  // Get the size associated with the given slot id.
  int SlotSize(int slot_id) const {
    return buffers_.empty()
               ? 0
               : buffers_[ccb_->slots[slot_id].buffer_index].slot_size;
  }

  int SlotSize(MessageSlot *slot) const {
    if (slot == nullptr) {
      return 0;
    }
    return buffers_.empty()
               ? 0
               : buffers_[ccb_->slots[slot->id].buffer_index].slot_size;
  }
  // Get the biggest slot size for the channel.
  int SlotSize() const {
    return buffers_.empty() ? 0 : buffers_.back().slot_size;
  }

  // Get the number of slots in the channel (can't be changed)
  int NumSlots() const { return num_slots_; }
  void SetNumSlots(int n) { num_slots_ = n; }

  // Get the buffer associated with the given slot id.  The first buffer
  // starts immediately after the buffer header.
  char *Buffer(int slot_id) const {
    int index = ccb_->slots[slot_id].buffer_index;
    if (index < 0 || index >= buffers_.size()) {
      std::cerr << "Invalid buffer index for slot " << slot_id << ": " << index
                << std::endl;
      abort();
    }
    return buffers_.empty() ? nullptr
                            : (buffers_[index].buffer + sizeof(BufferHeader));
  }
  void CleanupSlots(int owner, bool reliable, bool is_pub);

  int GetChannelId() const { return channel_id_; }

  int NumUpdates() const { return num_updates_; }
  void SetNumUpdates(int num_updates) {
    num_updates_ = static_cast<uint16_t>(num_updates);
  }

  SystemControlBlock *GetScb() const { return scb_; }
  ChannelControlBlock *GetCcb() const { return ccb_; }

  // Gets the statistics counters.
  void GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages, uint32_t& max_message_size, uint32_t& total_drops);

  void SetDebug(bool v) { debug_ = v; }

  bool AtomicIncRefCount(MessageSlot *slot, bool reliable, int inc, int ordinal);

  void SetType(std::string type) { type_ = std::move(type); }
  const std::string Type() const { return type_; }

  bool BuffersChanged() const {
    return ccb_->num_buffers != static_cast<int>(buffers_.size());
  }

  const std::vector<BufferSet> &GetBuffers() const { return buffers_; }

  char *EndOfSlots() const {
    return reinterpret_cast<char *>(ccb_) +
           Aligned<32>(sizeof(ChannelControlBlock) +
                       num_slots_ * sizeof(MessageSlot));
  }

  InPlaceAtomicBitset &GetAvailableSlots(int sub_id) {
    return *GetAvailableSlotsAddress(sub_id);
  }

  InPlaceAtomicBitset *GetAvailableSlotsAddress(int sub_id) {
    return reinterpret_cast<InPlaceAtomicBitset *>(
        EndOfSlots() + SizeofAtomicBitSet(num_slots_) * sub_id);
  }

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

  std::string name_;
  int num_slots_;

  int channel_id_; // ID allocated from server.
  std::string type_;

  uint16_t num_updates_ = 0;

  SystemControlBlock *scb_ = nullptr;
  ChannelControlBlock *ccb_ = nullptr;
  std::vector<BufferSet> buffers_;
  bool debug_ = false;
};

} // namespace subspace
#endif /* __COMMON_CHANNEL_H */
