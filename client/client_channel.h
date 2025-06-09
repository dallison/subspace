// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __CLIENT_CLIENT_CHANNEL_H
#define __CLIENT_CLIENT_CHANNEL_H

#include "client/options.h"
#include "common/channel.h"
#include "coroutine.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"
#include "toolbelt/sockets.h"
#include "toolbelt/triggerfd.h"
#include <sys/poll.h>

// Notification strategy
// ---------------------
// Each subscriber and reliable publisher has a TriggerFd, which is
// either implemented as a pipe or an eventfd.  The idea is that the
// program can wait for the event to be triggered for a new
// message or the ability to send another message.
//
// For a subscriber, the event is triggered when there are messages
// for the subscriber to read.  Once triggered, the subscriber
// needs to read all the available messages before going back into
// wait mode.
//
// For a reliable publisher, the publisher should wait for its
// event if it fails to get a buffer to send a message.  The
// subscribers will trigger the event when they have read all
// the aviailable messages.
//
// The naive approach to get this working is for the publisher
// to trigger the subscribers events for every message it sends, but
// this would mean a write to a pipe every time a message is
// sent.  That's a system call and takes some time.
// Instead we take the approach that the publisher only triggers when
// it publishes a message immediately after a message that has been
// seen by a subscriber.  In other words, if a publisher is publishing
// a bunch of messages at once, only the first one will result in
// the subscribers being triggered.  All the other messages are
// buffered ahead for the subscriber to pick up when it starts
// reading.
//
// Practically, this means that the publisher will notify the
// subscriber if the last message in the active list before
// it adds a new one has the kMessageSeen flag set (has been
// seen by a subscriber).  If the publisher is publishing slowly
// it will trigger the subscribers for every message.
//
// For reliable publisher triggers, the naive design would trigger
// the publisher every time it reads a message.  The better
// approach is for the subscriber to only trigger
// the event when it has finished reading all the messages that
// are available.  When the publisher receives the event it can then
// fill up all the slots that have been read before it runs out
// of slots again (if it's going fast).
//
// Using this strategy, we limit the number of event triggers and
// thus improve the latency of the system by eliminating unnecessary
// system calls to write to a pipe or eventfd.
//
// As of time of writing, the latency for a message on the same
// computer using shared memory is about 0.25 microseconds on a Mac M1.

namespace subspace {

class ClientImpl;

namespace details {

struct BufferSet {
  BufferSet() = default;
  BufferSet(uint64_t full_sz, uint64_t slot_sz, char *buf) : full_size(full_sz), slot_size(slot_sz), buffer(buf) {}
  uint64_t full_size = 0;
  uint64_t slot_size = 0;
  char *buffer = nullptr;
};


// This is a channel as seen by a client.  It's going to be either
// a publisher or a subscriber, as defined as the subclasses.
class ClientChannel : public Channel {
public:
  ClientChannel(const std::string &name, int num_slots, int channel_id,
                int vchan_id, std::string shm_prefix, std::string type)
      : Channel(name, num_slots, channel_id, std::move(type)), vchan_id_(vchan_id), shm_prefix_(std::move(shm_prefix)) {}
  virtual ~ClientChannel() = default;
  MessageSlot *CurrentSlot() const { return slot_; }
  const ChannelCounters &GetCounters() const {
    return GetScb()->counters[GetChannelId()];
  }

  void Unmap() override;

  void Dump(std::ostream& os) override;

  // Client-side channel mapping.  The SharedMemoryFds contains the
  // file descriptors for the CCB and buffers.  The num_slots_
  // member variable contains either 0 or the
  // channel size parameters.
  absl::Status Map(SharedMemoryFds fds, const toolbelt::FileDescriptor &scb_fd);

  absl::Status MapNewBuffers(std::vector<SlotBuffer> buffers);
  void UnmapUnusedBuffers();

  int VirtualChannelId() const { return vchan_id_; }

    // What is the address of the message buffer (after the MessagePrefix)
  // for the slot given a slot id.
  void *GetBufferAddress(int slot_id) const {
    return Buffer(slot_id) +
           (sizeof(MessagePrefix) + Aligned<64>(SlotSize(slot_id))) * slot_id +
           sizeof(MessagePrefix);
  }

  // Gets the address for the message buffer given a slot pointer.
  void *GetBufferAddress(MessageSlot *slot) const {
    if (slot == nullptr) {
      return nullptr;
    }
    return Buffer(slot->id) +
           (sizeof(MessagePrefix) + Aligned<64>(SlotSize(slot->id))) *
               slot->id +
           sizeof(MessagePrefix);
  }

  // Get a pointer to the MessagePrefix for a given slot.
  MessagePrefix *Prefix(MessageSlot *slot,
                        const std::function<bool()> &reload) {
    ReloadIfNecessary(reload);
    MessagePrefix *p = reinterpret_cast<MessagePrefix *>(
        Buffer(slot->id) +
        (sizeof(MessagePrefix) + Aligned<64>(SlotSize(slot->id))) * slot->id);
    return p;
  }

  MessagePrefix *Prefix(MessageSlot *slot) const {
    MessagePrefix *p = reinterpret_cast<MessagePrefix *>(
        Buffer(slot->id) +
        (sizeof(MessagePrefix) + Aligned<64>(SlotSize(slot->id))) * slot->id);
    return p;
  }

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

   bool BuffersChanged() const {
    return ccb_->num_buffers != static_cast<int>(buffers_.size());
  }

  const std::vector<BufferSet> &GetBuffers() const { return buffers_; }


protected:
  virtual bool IsSubscriber() const { return false; }
  virtual bool IsPublisher() const { return false; }

  void SetSlot(MessageSlot *slot) { slot_ = slot; }
  void *GetCurrentBufferAddress() { return GetBufferAddress(slot_); }
  bool ValidateSlotBuffer(MessageSlot *slot, std::function<bool()> reload);

  void SetMessageSize(int64_t message_size) {
    slot_->message_size = message_size;
  }

  bool IsVirtual() const { return vchan_id_ != -1; }


    absl::StatusOr<toolbelt::FileDescriptor> CreateBuffer(int buffer_index, size_t size);
    absl::StatusOr<toolbelt::FileDescriptor> OpenBuffer(int buffer_index);
    absl::StatusOr<size_t> GetBufferSize(toolbelt::FileDescriptor& shm_fd) const;
    absl::StatusOr<char*>
    MapBuffer(toolbelt::FileDescriptor& shm_fd, size_t size, bool read_only);

    std::string BufferSharedMemoryName(int buffer_index) const {
        return fmt::format("{}_buffer_{}_{}", shm_prefix_, GetChannelId(), buffer_index);
    }

protected:
  MessageSlot *slot_ = nullptr; // Current slot.
  int vchan_id_ = -1;       // Virtual channel ID.
  std::string shm_prefix_;
  std::vector<std::unique_ptr<BufferSet>> buffers_ = {};
};

} // namespace details
} // namespace subspace

#endif // __CLIENT_CLIENT_CHANNEL_H
