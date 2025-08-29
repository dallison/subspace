// Copyright 2025 David Allison
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

#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <thread>

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
  BufferSet(uint64_t full_sz, uint64_t slot_sz, char *buf)
      : full_size(full_sz), slot_size(slot_sz), buffer(buf) {}
  uint64_t full_size = 0;
  uint64_t slot_size = 0;
  char *buffer = nullptr;
};

// This is a channel as seen by a client.  It's going to be either
// a publisher or a subscriber, as defined as the subclasses.
class ClientChannel : public Channel {
public:
  ClientChannel(const std::string &name, int num_slots, int channel_id,
                int vchan_id, uint64_t session_id, std::string type, std::function<bool(Channel*)> reload)
      : Channel(name, num_slots, channel_id, std::move(type), std::move(reload)),
        vchan_id_(vchan_id), session_id_(std::move(session_id)) {}
  virtual ~ClientChannel() = default;
  MessageSlot *CurrentSlot() const { return slot_; }
  const ChannelCounters &GetCounters() const {
    return GetScb()->counters[GetChannelId()];
  }

  void Unmap() override;

  void Dump(std::ostream &os) const override;

  // Client-side channel mapping.  The SharedMemoryFds contains the
  // file descriptors for the CCB and buffers.  The num_slots_
  // member variable contains either 0 or the
  // channel size parameters.
  absl::Status Map(SharedMemoryFds fds, const toolbelt::FileDescriptor &scb_fd);

  absl::Status MapNewBuffers(std::vector<SlotBuffer> buffers);
  absl::Status UnmapUnusedBuffers();

  int VirtualChannelId() const { return vchan_id_; }

  // What is the address of the message buffer (after the MessagePrefix)
  // for the slot given a slot id.
  void *GetBufferAddress(int slot_id) {
    return Buffer(slot_id) +
           (sizeof(MessagePrefix) + Aligned<64>(SlotSize(slot_id))) * slot_id +
           sizeof(MessagePrefix);
  }

  // Gets the address for the message buffer given a slot pointer.
  void *GetBufferAddress(MessageSlot *slot) {
    if (slot == nullptr) {
      return nullptr;
    }
    return Buffer(slot->id) +
           (sizeof(MessagePrefix) + Aligned<64>(SlotSize(slot->id))) *
               slot->id +
           sizeof(MessagePrefix);
  }

  // Get a pointer to the MessagePrefix for a given slot.
  MessagePrefix *Prefix(MessageSlot *slot) override {
    MessagePrefix *p = reinterpret_cast<MessagePrefix *>(
        Buffer(slot->id) +
        (sizeof(MessagePrefix) + Aligned<64>(SlotSize(slot->id))) * slot->id);
    return p;
  }

  // Get the size associated with the given slot id.
  int SlotSize(int slot_id) const {
    return buffers_.empty()
               ? 0
               : buffers_[ccb_->slots[slot_id].buffer_index]->slot_size;
  }

  int SlotSize(MessageSlot *slot) const {
    if (slot == nullptr) {
      return 0;
    }
    if (buffers_.empty()) {
      return 0;
    }
    if (ccb_->slots[slot->id].buffer_index < 0 ||
        ccb_->slots[slot->id].buffer_index >= buffers_.size()) {
      return 0;
    }
    return buffers_[ccb_->slots[slot->id].buffer_index]->slot_size;
  }
  // Get the biggest slot size for the channel.
  int SlotSize() const {
    return buffers_.empty() ? 0 : buffers_.back()->slot_size;
  }

  // Get the buffer associated with the given slot id.
  char *Buffer(int slot_id,
               bool abort_on_range = true) {
    // While we are trying to get the buffer a publisher might be adding
    // more buffers. Since we are going to abort if the index isn't in
    // range we should try very hard to make it so.
    constexpr int kMaxRetries = 1000;
    int retries = 0;
    while (retries < kMaxRetries) {
      size_t index = ccb_->slots[slot_id].buffer_index;
      if (index >= 0 && index < buffers_.size()) {
        return buffers_.empty() ? nullptr : (buffers_[index]->buffer);
      }
      CheckReload();
      retries++;
      std::this_thread::yield();
    }
    if (abort_on_range) {
      // If the index is out of range, we have a problem.
      // This should never happen.
      int index = ccb_->slots[slot_id].buffer_index;
      std::cerr << this << " Invalid buffer index for slot " << slot_id << ": "
                << index << " there are " << buffers_.size() << " buffers" << std::endl;
      std::cerr << this << "Channel: " << name_ << " from " << (IsPublisher() ? "publisher" : "subscriber") << std::endl;
      DumpSlots(std::cerr);
      abort();
    }
    return nullptr;
  }

  bool BuffersChanged() const {
    return ccb_->num_buffers != static_cast<int>(buffers_.size());
  }

  const std::vector<std::unique_ptr<BufferSet>> &GetBuffers() const {
    return buffers_;
  }

  absl::Status AttachBuffers();

  void ClearRetirementTriggers() {
    std::unique_lock<std::mutex> lock(retirement_lock_);
    retirement_triggers_.clear();
    has_retirement_triggers_ = false;
  }

  void AddRetirementTrigger(toolbelt::FileDescriptor fd) {
    std::unique_lock<std::mutex> lock(retirement_lock_);
    retirement_triggers_.emplace_back(std::move(fd));
    has_retirement_triggers_ = true;
  }

protected:
  void TriggerRetirement(int slot_id);

  virtual bool IsSubscriber() const { return false; }
  virtual bool IsPublisher() const { return false; }

  void SetSlot(MessageSlot *slot) { slot_ = slot; }
  void *GetCurrentBufferAddress() {
    return GetBufferAddress(slot_);
  }
  bool ValidateSlotBuffer(MessageSlot *slot);

  void SetMessageSize(int64_t message_size) {
    slot_->message_size = message_size;
  }

  bool IsVirtual() const { return vchan_id_ != -1; }
  virtual bool IsBridge() const { return false; }

  absl::StatusOr<toolbelt::FileDescriptor> CreateBuffer(int buffer_index,
                                                        size_t size);
  absl::StatusOr<toolbelt::FileDescriptor> OpenBuffer(int buffer_index);
  absl::StatusOr<size_t> GetBufferSize(toolbelt::FileDescriptor &shm_fd,
                                       int buffer_index) const;
  absl::StatusOr<char *> MapBuffer(toolbelt::FileDescriptor &shm_fd,
                                   size_t size, bool read_only);

  std::string BufferSharedMemoryName(int buffer_index) const {
    return Channel::BufferSharedMemoryName(session_id_, buffer_index);
  }

  absl::Status ZeroOutSharedMemoryFile(int buffer_index) const;

#if defined(__APPLE__)
  absl::StatusOr<std::string>
  CreateMacOSSharedMemoryFile(const std::string &filename, off_t size);
#endif

protected:
  MessageSlot *slot_ = nullptr; // Current slot.
  int vchan_id_ = -1;           // Virtual channel ID.
  uint64_t session_id_;
  std::vector<std::unique_ptr<BufferSet>> buffers_ = {};

  // Retirement triggers.  Although these are not in shared memory,
  // the retirement of a slot can occur in any thread so we need
  // a mutex.  But we don't want to lock the mutex if there are none
  // so we use an atomic boolean to check this.
  //
  // It is going to be very rare that the retirement triggers are changed
  // while an active messages is being destructed (a cause of a retirement).
  // The use of an atomic is not needed since single loads and stores are
  // atomic on Intel and ARM processors but it's free and serves to
  // document the intent that this is a flag that is checked before
  // locking the mutex (and also TSAN won't nag about it).
  std::atomic<bool> has_retirement_triggers_{false};
  std::vector<toolbelt::FileDescriptor> retirement_triggers_ = {};
  std::mutex retirement_lock_;
};

} // namespace details
} // namespace subspace

#endif // __CLIENT_CLIENT_CHANNEL_H
