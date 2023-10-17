// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __CLIENT_CLIENT_CHANNEL_H
#define __CLIENT_CLIENT_CHANNEL_H

#include "client/options.h"
#include "common/channel.h"
#include "common/triggerfd.h"
#include "coroutine.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"
#include "toolbelt/sockets.h"
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

class Client;

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

// This is a publisher.  It maps in the channel's memory and allows
// messages to be published.
class PublisherImpl : public ClientChannel {
public:
  PublisherImpl(const std::string &name, int num_slots, int channel_id,
                int publisher_id, std::string type,
                const PublisherOptions &options)
      : ClientChannel(name, num_slots, channel_id, std::move(type)),
        publisher_id_(publisher_id), options_(options) {}

  bool IsReliable() const { return options_.IsReliable(); }
  bool IsLocal() const { return options_.IsLocal(); }
  bool IsFixedSize() const { return options_.IsFixedSize(); }

private:
  friend class ::subspace::Client;

  bool IsPublisher() const override { return true; }

  PublishedMessage ActivateSlotAndGetAnother(bool reliable, bool is_activation,
                                             bool omit_prefix,
                                             bool *notify = nullptr) {
    return Channel::ActivateSlotAndGetAnother(
        slot_, reliable, is_activation, publisher_id_, omit_prefix, notify);
  }
  void ClearSubscribers() { subscribers_.clear(); }
  void AddSubscriber(toolbelt::FileDescriptor fd) {
    subscribers_.emplace_back(toolbelt::FileDescriptor(), std::move(fd));
  }
  size_t NumSubscribers() { return subscribers_.size(); }

  void SetTriggerFd(toolbelt::FileDescriptor fd) {
    trigger_.SetTriggerFd(std::move(fd));
  }
  void SetPollFd(toolbelt::FileDescriptor fd) {
    trigger_.SetPollFd(std::move(fd));
  }

  toolbelt::FileDescriptor &GetPollFd() { return trigger_.GetPollFd(); }

  void TriggerSubscribers() {
    for (auto &fd : subscribers_) {
      fd.Trigger();
    }
  }
  int GetPublisherId() const { return publisher_id_; }

  void ClearPollFd() { trigger_.Clear(); }

  TriggerFd trigger_;
  int publisher_id_;
  std::vector<TriggerFd> subscribers_;
  PublisherOptions options_;
};

// A subscriber reads messages from a channel.  It maps the channel
// shared memory.
class SubscriberImpl : public ClientChannel {
public:
  SubscriberImpl(const std::string &name, int num_slots, int channel_id,
                 int subscriber_id, std::string type,
                 const SubscriberOptions &options)
      : ClientChannel(name, num_slots, channel_id, std::move(type)),
        subscriber_id_(subscriber_id), options_(options) {}

  int64_t CurrentOrdinal() const {
    return CurrentSlot() == nullptr ? -1 : CurrentSlot()->ordinal;
  }
  int64_t Timestamp() const {
    return CurrentSlot() == nullptr ? 0 : Prefix(CurrentSlot())->timestamp;
  }
  bool IsReliable() const { return options_.IsReliable(); }

  int32_t SlotSize() const { return Channel::SlotSize(CurrentSlot()); }

  void IncDecSharedPtrCount(int inc) { num_shared_ptrs_ += inc; }
  int NumSharedPtrs() const { return num_shared_ptrs_; }
  int MaxSharedPtrs() const { return options_.MaxSharedPtrs(); }
  bool CheckSharedPtrCount() const {
    return num_shared_ptrs_ < options_.MaxSharedPtrs();
  }

  bool LockForShared(MessageSlot* slot, int64_t ordinal) {
    return LockForSharedInternal(slot, ordinal, IsReliable());
  }

private:
  friend class ::subspace::Client;

  bool IsSubscriber() const override { return true; }

  void ClearPublishers() { reliable_publishers_.clear(); }
  void AddPublisher(toolbelt::FileDescriptor fd) {
    reliable_publishers_.emplace_back(toolbelt::FileDescriptor(),
                                      std::move(fd));
  }
  size_t NumReliablePublishers() { return reliable_publishers_.size(); }

  void SetTriggerFd(toolbelt::FileDescriptor fd) {
    trigger_.SetTriggerFd(std::move(fd));
  }
  void SetPollFd(toolbelt::FileDescriptor fd) {
    trigger_.SetPollFd(std::move(fd));
  }
  int GetSubscriberId() const { return subscriber_id_; }
  void TriggerReliablePublishers() {
    for (auto &fd : reliable_publishers_) {
      fd.Trigger();
    }
  }
  void Trigger() { trigger_.Trigger(); }

  MessageSlot *NextSlot() {
    return Channel::NextSlot(CurrentSlot(), IsReliable(), subscriber_id_);
  }

  MessageSlot *LastSlot() {
    return Channel::LastSlot(CurrentSlot(), IsReliable(), subscriber_id_);
  }

  toolbelt::FileDescriptor &GetPollFd() { return trigger_.GetPollFd(); }
  void ClearPollFd() { trigger_.Clear(); }

  MessageSlot *FindMessage(uint64_t timestamp) {
    MessageSlot *slot =
        FindActiveSlotByTimestamp(CurrentSlot(), timestamp, IsReliable(),
                                  GetSubscriberId(), search_buffer_);
    if (slot != nullptr) {
      SetSlot(slot);
    }
    return slot;
  }

  int subscriber_id_;
  TriggerFd trigger_;
  std::vector<TriggerFd> reliable_publishers_;
  SubscriberOptions options_;
  int num_shared_ptrs_ = 0;

  // It is rare that subscribers need to search for messges by timestamp.  This
  // will keep the memory allocation to the first search on a subscriber.  Most
  // subscribers won't use this.
  std::vector<MessageSlot *> search_buffer_;
};
} // namespace details
} // namespace subspace

#endif // __CLIENT_CLIENT_CHANNEL_H
