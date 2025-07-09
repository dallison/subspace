// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/message.h"
#include "client/subscriber.h"

namespace subspace {
ActiveMessage::ActiveMessage(std::shared_ptr<details::SubscriberImpl> subr,
                             size_t len, MessageSlot *slot_ptr, const void *buf,
                             uint64_t ord, int64_t ts, int vid, bool activation)
    : sub(std::move(subr)), length(len), slot(slot_ptr), buffer(buf),
      ordinal(ord), timestamp(ts), vchan_id(vid), is_activation(activation) {
  if (slot == nullptr) {
    return;
  }
  if (!sub->AddActiveMessage(slot)) {
    ResetInternal();
  }
}

ActiveMessage::~ActiveMessage() { Release(); }

void ActiveMessage::Release() {
  if (sub != nullptr) {
    sub->RemoveActiveMessage(slot);
  }
  ResetInternal();
}

Message::Message(std::shared_ptr<ActiveMessage> msg)
    : active_message(std::move(msg)), length(active_message->length),
      buffer(active_message->buffer), ordinal(active_message->ordinal),
      timestamp(active_message->timestamp), vchan_id(active_message->vchan_id),
      is_activation(active_message->is_activation),
      slot_id(active_message->slot != nullptr ? active_message->slot->id : -1) {
}

} // namespace subspace
