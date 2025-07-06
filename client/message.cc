// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/message.h"
#include "client/subscriber.h"

namespace subspace {
ActiveMessage::ActiveMessage(std::shared_ptr<details::SubscriberImpl> subr, size_t len,
                 MessageSlot *slot_ptr, const void *buf, uint64_t ord, int64_t ts, int vid, bool activation)
    : sub(std::move(subr)), length(len), slot(slot_ptr), buffer(buf), ordinal(ord),
      timestamp(ts), vchan_id(vid), is_activation(activation) {
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
} // namespace subspace
