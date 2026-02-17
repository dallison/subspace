// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/message.h"
#include "client/subscriber.h"
#include "absl/strings/str_format.h"

namespace subspace {

ActiveMessage::~ActiveMessage() {}

void ActiveMessage::ResetInternal() {
  length = 0;
  buffer = nullptr;
  ordinal = -1;
  timestamp = 0;
  vchan_id = -1;
  is_activation = false;
  checksum_error = false;
}

void ActiveMessage::Release(int ref_count) {
  if (sub.expired()) {
    return;
  }
  std::shared_ptr<details::SubscriberImpl> subr = sub.lock();
  if (length != 0 && ref_count == 0) {
    // The message must be reset before releasing the slot since another message
    // might take the slot once we release and before we reset it.
    ResetInternal();
    subr->RemoveActiveMessage(slot);
  }
}

void ActiveMessage::Set(size_t len, const void *buf, uint64_t ord, int64_t ts,
                          int vid, bool activation, bool cs_error) {
  length = len;
  buffer = buf;
  ordinal = ord;
  timestamp = ts;
  vchan_id = vid;
  is_activation = activation;
  checksum_error = cs_error;

  std::shared_ptr<details::SubscriberImpl> subr = sub.lock();
  if (subr == nullptr) {
    return;
  }

  if (!subr->AddActiveMessage(slot)) {
    ResetInternal();
  }
}

std::string Message::ChannelType() const {
  if (active_message == nullptr || active_message->sub.expired()) {
    return "";
  }
  std::shared_ptr<details::SubscriberImpl> subr = active_message->sub.lock();
  if (subr == nullptr) {
    return "";
  }
  return subr->SlotType();
}

int Message::NumSlots() const {
  if (active_message == nullptr || active_message->sub.expired()) {
    return 0;
  }
  std::shared_ptr<details::SubscriberImpl> subr = active_message->sub.lock();
  if (subr == nullptr) {
    return 0;
  }
  return subr->NumSlots();
}

uint64_t Message::SlotSize() const {
  if (active_message == nullptr || active_message->sub.expired()) {
    return 0;
  }
  std::shared_ptr<details::SubscriberImpl> subr = active_message->sub.lock();
  if (subr == nullptr) {
    return 0;
  }
  return subr->SlotSize();
}

Message::Message(std::shared_ptr<ActiveMessage> msg)
    : active_message(std::move(msg)), length(active_message->length),
      buffer(active_message->buffer), ordinal(active_message->ordinal),
      timestamp(active_message->timestamp), vchan_id(active_message->vchan_id),
      is_activation(active_message->is_activation),
      slot_id(active_message->slot != nullptr ? active_message->slot->id : -1),
      checksum_error(active_message->checksum_error) {
  active_message->IncRef();
}

} // namespace subspace
