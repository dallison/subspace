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

// This is a channel as seen by a client.  It's going to be either
// a publisher or a subscriber, as defined as the subclasses.
class ClientChannel : public Channel {
public:
  ClientChannel(const std::string &name, int num_slots, int channel_id,
                std::string type)
      : Channel(name, num_slots, channel_id, std::move(type)) {}
  virtual ~ClientChannel() = default;
  MessageSlot *CurrentSlot() const { return slot_; }
  const ChannelCounters &GetCounters() const {
    return GetScb()->counters[GetChannelId()];
  }

  // Client-side channel mapping.  The SharedMemoryFds contains the
  // file descriptors for the CCB and buffers.  The num_slots_
  // member variable contains either 0 or the
  // channel size parameters.
  absl::Status Map(SharedMemoryFds fds, const toolbelt::FileDescriptor &scb_fd);

  absl::Status MapNewBuffers(std::vector<SlotBuffer> buffers);
  void UnmapUnusedBuffers();

protected:
  virtual bool IsSubscriber() const { return false; }
  virtual bool IsPublisher() const { return false; }

  void SetSlot(MessageSlot *slot) { slot_ = slot; }
  void *GetCurrentBufferAddress() { return GetBufferAddress(slot_); }

  void SetMessageSize(int64_t message_size) {
    slot_->message_size = message_size;
  }

protected:
  MessageSlot *slot_ = nullptr; // Current slot.
};




} // namespace details
} // namespace subspace

#endif // __CLIENT_CLIENT_CHANNEL_H
