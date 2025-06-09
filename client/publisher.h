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
                int publisher_id, int vchan_id, std::string shm_prefix, std::string type,
                const PublisherOptions &options)
      : ClientChannel(name, num_slots, channel_id, vchan_id, std::move(shm_prefix), std::move(type)),
        publisher_id_(publisher_id), options_(options) {}

  bool IsReliable() const { return options_.IsReliable(); }
  bool IsLocal() const { return options_.IsLocal(); }
  bool IsFixedSize() const { return options_.IsFixedSize(); }

 MessageSlot *FindFreeSlotUnreliable(int owner, std::function<bool()> reload);
  MessageSlot *FindFreeSlotReliable(int owner, std::function<bool()> reload);

    void SetSlotToBiggestBuffer(MessageSlot *slot);

        cruise::Status CreateOrAttachBuffers(uint64_t slot_size);

private:
  friend class ::subspace::ClientImpl;

  bool IsPublisher() const override { return true; }

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
  Channel::PublishedMessage ActivateSlotAndGetAnother(MessageSlot *slot, bool reliable,
                                             bool is_activation, int owner,
                                             bool omit_prefix, bool *notify,
                                             std::function<bool()> reload);

  Channel::PublishedMessage ActivateSlotAndGetAnother(bool reliable, bool is_activation,
                                    bool omit_prefix, bool *notify,
                                    std::function<bool()> reload) {
    return ActivateSlotAndGetAnother(slot_, reliable, is_activation,
                                              publisher_id_, omit_prefix,
                                              notify, std::move(reload));
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
};

} // namespace details
} // namespace subspace
