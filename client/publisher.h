// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "client/client_channel.h"
#include "client/message.h"

namespace subspace {
namespace details {

// This is a publisher.  It maps in the channel's memory and allows
// messages to be published.
class PublisherImpl : public ClientChannel {
public:
  PublisherImpl(const std::string &name, int num_slots, int channel_id,
                int publisher_id, int vchan_id, uint64_t session_id,
                std::string type, const PublisherOptions &options,
                std::function<bool(Channel *)> reload, int user_id, int group_id)
      : ClientChannel(name, num_slots, channel_id, vchan_id,
                      std::move(session_id), std::move(type),
                      std::move(reload), user_id, group_id),
        publisher_id_(publisher_id), options_(options) {}

  bool IsReliable() const { return options_.IsReliable(); }
  bool IsLocal() const { return options_.IsLocal(); }
  bool IsFixedSize() const { return options_.IsFixedSize(); }

  MessageSlot *FindFreeSlotUnreliable(int owner);
  MessageSlot *FindFreeSlotReliable(int owner);

  void SetSlotToBiggestBuffer(MessageSlot *slot);

  absl::Status CreateOrAttachBuffers(uint64_t slot_size);

  void SetRetirementFd(toolbelt::FileDescriptor fd) {
    retirement_fd_ = std::move(fd);
  }

  // This is the read end of a pipe into which will be written the slot ids
  // for retired slots.
  const toolbelt::FileDescriptor &GetRetirementFd() const {
    return retirement_fd_;
  }

  void SetOnSendCallback(
      std::function<absl::StatusOr<int64_t>(void *buffer, int64_t size)>
          callback) {
    on_send_callback_ = std::move(callback);
  }

  std::string Mux() const { return options_.Mux(); }

private:
  friend class ::subspace::ClientImpl;

  bool IsPublisher() const override { return true; }
  bool IsBridge() const override { return options_.IsBridge(); }
  BufferMapMode MapMode() const override { return BufferMapMode::kReadWrite; }

  std::string ResolvedName() const override {
    return IsVirtual() ? options_.mux : Name();
  }

  // A publisher is done with its busy slot (it now contains a message).  The
  // slot is moved from the busy list to the end of the active list and other
  // slot is obtained, unless reliable is set to true.  The new slot
  // is returned and this will be nullptr if reliable is true (reliable
  // publishers get a new slot later).  The function fills in the
  // MessagePrefix.
  //
  // The args are:
  // reliable: this is for a reliable publisher, so don't allocate a new slot
  // is_activation: this is an activation message for a reliable publisher
  // owner: the ID of the publisher
  // omit_prefix: don't fill in the MessagePrefix (used when a message
  //              is read from the bridge)
  // notify: set to true if we should notify the subscribers.
  //
  // Locks the CCB.
  Channel::PublishedMessage
  ActivateSlotAndGetAnother(MessageSlot *slot, bool reliable,
                            bool is_activation, int owner, bool omit_prefix,
                            bool use_prefix_slot_id);

  Channel::PublishedMessage ActivateSlotAndGetAnother(bool reliable,
                                                      bool is_activation,
                                                      bool omit_prefix,
                                                      bool use_prefix_slot_id) {
    return ActivateSlotAndGetAnother(slot_, reliable, is_activation,
                                     publisher_id_, omit_prefix,
                                     use_prefix_slot_id);
  }

  void ClearSubscribers() { subscribers_.clear(); }
  void AddSubscriber(toolbelt::FileDescriptor fd) {
    subscribers_.emplace_back(toolbelt::FileDescriptor(), std::move(fd));
  }

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

  toolbelt::TriggerFd trigger_;
  int publisher_id_;
  std::vector<toolbelt::TriggerFd> subscribers_;
  PublisherOptions options_;
  toolbelt::FileDescriptor retirement_fd_ = {};
  std::function<absl::StatusOr<int64_t>(void *buffer, int64_t size)>
      on_send_callback_ = nullptr;

};

} // namespace details
} // namespace subspace
