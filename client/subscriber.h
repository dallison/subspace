// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once
#include "client/client_channel.h"
#include "client/message.h"
#include "common/fast_ring_buffer.h"
#include <mutex>

namespace subspace {
namespace details {

struct OrdinalAndVchanId {
  uint64_t ordinal;
  int vchan_id;
  bool operator==(const OrdinalAndVchanId &o) const {
    return ordinal == o.ordinal && vchan_id == o.vchan_id;
  }
  bool operator!=(const OrdinalAndVchanId &o) const {
    return ordinal != o.ordinal || vchan_id != o.vchan_id;
  }
  bool operator<(const OrdinalAndVchanId &o) const {
    if (vchan_id < o.vchan_id) {
      return true;
    }
    if (vchan_id > o.vchan_id) {
      return false;
    }
    return ordinal < o.ordinal;
  }
};

template <typename H> inline H AbslHashValue(H h, const OrdinalAndVchanId &x) {
  return H::combine(std::move(h), x.ordinal, x.vchan_id);
}

// A subscriber reads messages from a channel.  It maps the channel
// shared memory.
class SubscriberImpl : public ClientChannel {
public:
  SubscriberImpl(const std::string &name, int num_slots, int channel_id,
                 int subscriber_id, int vchan_id, uint64_t session_id,
                 std::string type, const SubscriberOptions &options,
                 std::function<bool(Channel *)> reload, int user_id, int group_id)
      : ClientChannel(name, num_slots, channel_id, vchan_id,
                      std::move(session_id), std::move(type),
                      std::move(reload), user_id, group_id),
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
    return slot == nullptr ? 0 : slot->timestamp;
  }
  bool IsReliable() const { return options_.IsReliable(); }

  int32_t SlotSize() const { return ClientChannel::SlotSize(CurrentSlot()); }

  bool IsPlaceholder() const { return ClientChannel::NumSlots() == 0; }

  bool AddActiveMessage(MessageSlot *slot);
  void RemoveActiveMessage(MessageSlot *slot);

  int NumActiveMessages() const { return num_active_messages_; }
  int MaxActiveMessages() const { return options_.MaxActiveMessages(); }
  bool CheckActiveMessageCount() const {
    return num_active_messages_ < options_.MaxActiveMessages();
  }

  // This is the configured virtual channel ID, not the value assigned when the
  // subscriber is created by the server.  The difference is that the configured
  // value is normally -1 and this allows the server to pick a vchan ID.  The
  // use of a configured ID is to allow a multiplexer subscriber to determine
  // which channel a vchanId corresponds to when it receives messages.
  int ConfiguredVchanId() const { return options_.vchan_id; }

  const ActiveSlot *
  FindUnseenOrdinal();
  void PopulateActiveSlots(InPlaceAtomicBitset &bits);

  void ClaimSlot(MessageSlot *slot, int vchan_id, bool was_newest);
  void RememberOrdinal(uint64_t ordinal, int vchan_id);
  void CollectVisibleSlots(InPlaceAtomicBitset &bits);

  void IgnoreActivation(MessageSlot *slot) {
    RememberOrdinal(slot->ordinal, slot->vchan_id);
    DecrementSlotRef(slot, true);
    slot->flags |= kMessageSeen;
  }
  // A subscriber wants to find a slot with a message in it.  There are
  // two ways to get this:
  // NextSlot: gets the next slot in the active list
  // LastSlot: gets the last slot in the active list.
  // Can return nullptr if there is no slot.
  // If reliable is true, the reliable_ref_count in the MessageSlot will
  // be manipulated.  The owner is the subscriber ID.
  MessageSlot *NextSlot(MessageSlot *slot, bool reliable, int owner);
  MessageSlot *LastSlot(MessageSlot *slot, bool reliable, int owner);

  std::shared_ptr<ActiveMessage> GetActiveMessage() { return active_message_; }

  void ClearActiveMessage() { active_message_.reset(); }

  std::shared_ptr<ActiveMessage> SetActiveMessage(size_t len, MessageSlot *slot,
                                                  const void *buf, uint64_t ord,
                                                  int64_t ts, int vchan_id,
                                                  bool is_activation, bool checksum_error) {
    active_message_.reset();
    active_message_ = std::make_shared<ActiveMessage>(ActiveMessage{
        shared_from_this(), len, slot, buf, ord, ts, vchan_id, is_activation, checksum_error});
    return active_message_;
  }

  void DecrementSlotRef(MessageSlot *slot, bool retire) {
    AtomicIncRefCount(slot, IsReliable(), -1, slot->ordinal & kOrdinalMask,
                      vchan_id_, retire);
  }

  bool SlotExpired(MessageSlot *slot, uint32_t ordinal) {
    return slot->ordinal != ordinal;
  }

  std::shared_ptr<ActiveMessage> LockWeakMessage(MessageSlot *slot,
                                                 uint64_t ordinal) {
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
    auto msg = std::make_shared<ActiveMessage>(ActiveMessage{
        shared_from_this(), slot->message_size, slot, GetBufferAddress(slot),
        slot->ordinal, Timestamp(slot), slot->vchan_id, false, false});
    if (msg->length == 0) {
      // Failed to get an active message, return an empty shared_ptr.
      return nullptr;
    }
    return msg;
  }

  int DetectDrops(int vchan_id);

  // Search the active list for a message with the given timestamp.  If found,
  // take ownership of the slot found.  Return nullptr if nothing found in which
  // no slot ownership changes are done.  This uses the memory inside buffer
  // to perform a fast search of the slots.  The caller keeps onership of the
  // buffer, but this function will modify it.  This is to avoid memory
  // allocation for every search or buffer allocation for every subscriber when
  // searches are rare.
  MessageSlot *FindActiveSlotByTimestamp(MessageSlot *old_slot,
                                         uint64_t timestamp, bool reliable,
                                         int owner,
                                         std::vector<ActiveSlot> &buffer);

  void Trigger() { trigger_.Trigger(); }
  void Untrigger() { trigger_.Clear(); }

  void TriggerReliablePublishers() {
    std::unique_lock<std::mutex> lock(reliable_publishers_mutex_);
    for (auto &fd : reliable_publishers_) {
      fd.Trigger();
    }
  }

  void SetOnReceiveCallback(std::function<absl::StatusOr<int64_t>(void* buffer, int64_t size)> callback) {
    on_receive_callback_ = std::move(callback);
  }

  std::string Mux() const { return options_.Mux(); }

  bool ValidateChecksum(const std::array<absl::Span<const uint8_t>, 2>& data, uint32_t checksum) {
    if (!options_.Checksum()) {
      return true;
    }
    return VerifyChecksum(data, checksum);
  }

  bool PassChecksumErrors() const { return options_.PassChecksumErrors(); }

private:
  friend class ::subspace::ClientImpl;

  struct OrdinalTracker {
    FastRingBuffer<OrdinalAndVchanId, 10000> ordinals;
    uint64_t last_ordinal_seen = 0;
  };

  BufferMapMode MapMode() const override {
    return options_.IsBridge() || options_.ReadWrite()
               ? BufferMapMode::kReadWrite
               : BufferMapMode::kReadOnly;
  }

  bool IsSubscriber() const override { return true; }
  bool IsBridge() const override { return options_.IsBridge(); }

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

  MessageSlot *NextSlot() {
    return NextSlot(CurrentSlot(), IsReliable(), subscriber_id_);
  }

  MessageSlot *LastSlot() {
    return LastSlot(CurrentSlot(), IsReliable(), subscriber_id_);
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

  OrdinalTracker &GetOrdinalTracker(int vchan_id);

  std::string ResolvedName() const override {
    return IsVirtual() ? options_.mux : Name();
  }

  int subscriber_id_;
  toolbelt::TriggerFd trigger_;
  std::vector<toolbelt::TriggerFd> reliable_publishers_;
  SubscriberOptions options_;
  std::atomic<int> num_active_messages_{0};
  std::mutex reliable_publishers_mutex_;

  // It is rare that subscribers need to search for messges by timestamp.  This
  // will keep the memory allocation to the first search on a subscriber.  Most
  // subscribers won't use this.
  std::vector<ActiveSlot> search_buffer_;

  // The subscriber holds on to an active message for the slot it has just
  // read.  A shared pointer to this active message is returned to caller.
  std::shared_ptr<ActiveMessage> active_message_;

  // We keep track of a limited number of ordinals we've seen.
  // One of these per virtual channel.  If there are no virtual channels
  // we will use vchan_id -1.
  absl::flat_hash_map<int, std::unique_ptr<OrdinalTracker>> ordinal_trackers_;

  // The callback to call when a message is received.
  std::function<void(SubscriberImpl *, Message)> message_callback_;
  std::function<absl::StatusOr<int64_t>(void* buffer, int64_t size)> on_receive_callback_ = nullptr;
};
} // namespace details
} // namespace subspace
