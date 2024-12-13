#pragma once
#include "client/client_channel.h"
#include "client/message.h"
#include "common/fast_ring_buffer.h"

namespace subspace {
namespace details {

// A subscriber reads messages from a channel.  It maps the channel
// shared memory.
class SubscriberImpl : public ClientChannel {
public:
  SubscriberImpl(const std::string &name, int num_slots, int channel_id,
                 int subscriber_id, std::string type,
                 const SubscriberOptions &options)
      : ClientChannel(name, num_slots, channel_id, std::move(type)),
        subscriber_id_(subscriber_id), options_(options) {}

  std::shared_ptr<SubscriberImpl> shared_from_this() {
    return std::static_pointer_cast<SubscriberImpl>(
        Channel::shared_from_this());
  }

  int64_t CurrentOrdinal() const {
    return CurrentSlot() == nullptr ? -1 : CurrentSlot()->ordinal;
  }
  int64_t Timestamp() const { return Timestamp(CurrentSlot()); }
  int64_t Timestamp(MessageSlot *slot) const {
    return slot == nullptr ? 0 : Prefix(slot)->timestamp;
  }
  bool IsReliable() const { return options_.IsReliable(); }

  int32_t SlotSize() const { return Channel::SlotSize(CurrentSlot()); }

  bool AddActiveMessage(MessageSlot *slot) {
    int old = num_active_messages.fetch_add(1);
    if (old >= options_.MaxActiveMessages()) {
      num_active_messages.fetch_sub(1);
      return false;
    }
    return true;
  }

  void RemoveActiveMessage(MessageSlot *slot) {
    slot->sub_owners.Clear(subscriber_id_);
    AtomicIncRefCount(slot, IsReliable(), -1);
    // Clear the bit in the subscriber bitset.
    GetAvailableSlots(subscriber_id_).Clear(slot->id);

    if (--num_active_messages < options_.MaxActiveMessages()) {
      Trigger();
      if (NumSlots() == 1 && IsReliable()) {
        TriggerReliablePublishers();
      }
    }
  }

  int NumActiveMessages() const { return num_active_messages; }
  int MaxActiveMessages() const { return options_.MaxActiveMessages(); }
  bool CheckActiveMessageCount() const {
    return num_active_messages < options_.MaxActiveMessages();
  }

  MessageSlot *FindUnseenOrdinal(const std::vector<ActiveSlot> &active_slots);
  void PopulateActiveSlots(InPlaceAtomicBitset &bits);

  // A subscriber wants to find a slot with a message in it.  There are
  // two ways to get this:
  // NextSlot: gets the next slot in the active list
  // LastSlot: gets the last slot in the active list.
  // Can return nullptr if there is no slot.
  // If reliable is true, the reliable_ref_count in the MessageSlot will
  // be manipulated.  The owner is the subscriber ID.
  MessageSlot *NextSlot(MessageSlot *slot, bool reliable, int owner,
                        std::function<bool()> reload);
  MessageSlot *LastSlot(MessageSlot *slot, bool reliable, int owner,
                        std::function<bool()> reload);

  std::shared_ptr<ActiveMessage> GetActiveMessage() { return active_message_; }

  void SetActiveMessage(size_t len, MessageSlot *slot, const void *buf,
                        uint64_t ord, int64_t ts) {
    active_message_.reset();
    active_message_ = std::make_shared<ActiveMessage>(
        ActiveMessage{shared_from_this(), len, slot, buf, ord, ts});
  }

  void DecrementSlotRef(MessageSlot *slot) {
    AtomicIncRefCount(slot, IsReliable(), -1);
  }

  bool SlotExpired(MessageSlot *slot, uint32_t ordinal) {
    return slot->ordinal != ordinal;
  }

  std::shared_ptr<ActiveMessage> LockWeakMessage(MessageSlot* slot, uint64_t ordinal) {
    if (slot == nullptr) {
      return nullptr;
    }
    if (slot->ordinal != ordinal) {
      return nullptr;
    }
    // If we are still holding on to the same active message, return it.
    if (active_message_->slot == slot && active_message_->ordinal == ordinal) {
      return active_message_;
    }
    return std::make_shared<ActiveMessage>(
        ActiveMessage{shared_from_this(), slot->message_size,
                      slot, GetBufferAddress(slot), slot->ordinal,
                      Timestamp(slot)});
  }

  int DetectDrops();

private:
  friend class ::subspace::ClientImpl;

  bool IsSubscriber() const override { return true; }

  void ClearPublishers() {
    std::unique_lock<std::mutex> lock(reliable_publishers_mutex_);
    reliable_publishers_.clear();
  }
  void AddPublisher(toolbelt::FileDescriptor fd) {
    std::unique_lock<std::mutex> lock(reliable_publishers_mutex_);
    reliable_publishers_.emplace_back(toolbelt::FileDescriptor(),
                                      std::move(fd));
  }
  size_t NumReliablePublishers() {
    std::unique_lock<std::mutex> lock(reliable_publishers_mutex_);
    return reliable_publishers_.size();
  }

  void SetTriggerFd(toolbelt::FileDescriptor fd) {
    trigger_.SetTriggerFd(std::move(fd));
  }
  void SetPollFd(toolbelt::FileDescriptor fd) {
    trigger_.SetPollFd(std::move(fd));
  }
  int GetSubscriberId() const { return subscriber_id_; }
  void TriggerReliablePublishers() {
    std::unique_lock<std::mutex> lock(reliable_publishers_mutex_);
    for (auto &fd : reliable_publishers_) {
      fd.Trigger();
    }
  }
  void Trigger() { trigger_.Trigger(); }

  MessageSlot *NextSlot(std::function<bool()> reload) {
    return NextSlot(CurrentSlot(), IsReliable(), subscriber_id_,
                    std::move(reload));
  }

  MessageSlot *LastSlot(std::function<bool()> reload) {
    return LastSlot(CurrentSlot(), IsReliable(), subscriber_id_,
                    std::move(reload));
  }

  toolbelt::FileDescriptor &GetPollFd() { return trigger_.GetPollFd(); }
  void ClearPollFd() { trigger_.Clear(); }

  MessageSlot *FindMessage(uint64_t timestamp) {
    MessageSlot *slot =
        FindActiveSlotByTimestamp(CurrentSlot(), timestamp, IsReliable(),
                                  GetSubscriberId(), search_buffer_, nullptr);
    if (slot != nullptr) {
      SetSlot(slot);
    }
    return slot;
  }

  int subscriber_id_;
  toolbelt::TriggerFd trigger_;
  std::vector<toolbelt::TriggerFd> reliable_publishers_;
  SubscriberOptions options_;
  std::atomic<int> num_active_messages = 0;
  std::mutex reliable_publishers_mutex_;

  // It is rare that subscribers need to search for messges by timestamp.  This
  // will keep the memory allocation to the first search on a subscriber.  Most
  // subscribers won't use this.
  std::vector<MessageSlot *> search_buffer_;

  // The subscriber holds on to an active message for the slot it has just
  // read.  A shared pointer to this active message is returned to caller.
  std::shared_ptr<ActiveMessage> active_message_;

    // We keep track of a limited number of ordinals we've seen.
  FastRingBuffer<uint64_t, 2000> seen_ordinals_;
  uint64_t last_ordinal_seen_ = 0;
};
} // namespace details
} // namespace subspace
