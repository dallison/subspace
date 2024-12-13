#include "client/message.h"
#include "client/subscriber.h"

namespace subspace {
ActiveMessage::ActiveMessage(std::shared_ptr<details::SubscriberImpl> sub, size_t len,
                 MessageSlot *slot, const void *buf, uint64_t ord, int64_t ts)
    : sub(sub), length(len), slot(slot), buffer(buf), ordinal(ord),
      timestamp(ts) {
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
