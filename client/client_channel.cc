#include "client/client_channel.h"
#include <sys/mman.h>
#if defined(__APPLE__)
#include <sys/posix_shm.h>
#endif

namespace subspace {

namespace details {

template <typename BufferSetIter>
static void UnmapBuffers(BufferSetIter first, BufferSetIter last,
                         int num_slots) {
  // Unmap any previously mapped buffers.
  for (; first < last; ++first) {
    int64_t buffers_size =
        sizeof(BufferHeader) +
        num_slots * (Aligned<32>(first->slot_size) + sizeof(MessagePrefix));
    if (buffers_size > 0 && first->buffer != nullptr) {
      UnmapMemory(first->buffer, buffers_size, "buffers");
      first->buffer = nullptr;
      first->slot_size = 0;
    }
  }
}

absl::Status ClientChannel::Map(SharedMemoryFds fds,
                          const toolbelt::FileDescriptor &scb_fd) {
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s", strerror(errno)));
  }

  ccb_ = reinterpret_cast<ChannelControlBlock *>(
      MapMemory(fds.ccb.Fd(), CcbSize(num_slots_), PROT_READ | PROT_WRITE, "CCB"));
  if (ccb_ == MAP_FAILED) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return absl::InternalError(absl::StrFormat(
        "Failed to map ChannelControlBlock: %s", strerror(errno)));
  }
  int index = 0;
  for (const auto &buffer : fds.buffers) {
    int64_t buffers_size =
        sizeof(BufferHeader) +
        num_slots_ * (Aligned<32>(buffer.slot_size) + sizeof(MessagePrefix));
    if (buffers_size != 0) {
      char *mem = reinterpret_cast<char *>(
          MapMemory(fds.buffers[index].fd.Fd(), buffers_size,
                    PROT_READ | PROT_WRITE, "buffers"));

      if (mem == MAP_FAILED) {
        UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
        UnmapMemory(ccb_, CcbSize(num_slots_), "CCB");
        // Unmap any previously mapped buffers.
        UnmapBuffers(buffers_.begin(), buffers_.begin() + index, num_slots_);
        return absl::InternalError(absl::StrFormat(
            "Failed to map channel buffers: %s", strerror(errno)));
      }
      buffers_.emplace_back(buffer.slot_size, mem);
      index++;
    }
  }

  if (debug_) {
    printf("Channel mapped: scb: %p, ccb: %p\n", scb_, ccb_);
    Dump();
  }
  return absl::OkStatus();
}


absl::Status ClientChannel::MapNewBuffers(std::vector<SlotBuffer> buffers) {
  size_t start = buffers_.size();
  if (debug_) {
    printf("Mapping new buffers starting at %zd\n", start);
  }
  for (size_t i = start; i < buffers.size(); i++) {
    const SlotBuffer &buffer = buffers[i];

    int64_t buffers_size =
        sizeof(BufferHeader) +
        num_slots_ * (Aligned<32>(buffer.slot_size) + sizeof(MessagePrefix));
    if (buffers_size != 0) {
      char *mem = reinterpret_cast<char *>(MapMemory(
          buffer.fd.Fd(), buffers_size, PROT_READ | PROT_WRITE, "new buffers"));

      if (mem == MAP_FAILED) {
        // Unmap any newly mapped buffers.
        UnmapBuffers(buffers_.begin() + start, buffers_.end(), num_slots_);
        return absl::InternalError(absl::StrFormat(
            "Failed to map new channel buffers: %s", strerror(errno)));
      }
      buffers_.emplace_back(buffer.slot_size, mem);
    }
  }
  return absl::OkStatus();
}

void ClientChannel::UnmapUnusedBuffers() {
  for (size_t i = 0; i + 1 < buffers_.size(); i++) {
    if (buffers_[i].buffer == nullptr) {
      continue;
    }
    BufferHeader *hdr = reinterpret_cast<BufferHeader *>(buffers_[i].buffer +
                                                         sizeof(BufferHeader)) -
                        1;
    if (hdr->refs == 0) {
      int64_t buffers_size = sizeof(BufferHeader) +
                             num_slots_ * (Aligned<32>(buffers_[i].slot_size) +
                                           sizeof(MessagePrefix));
      if (buffers_size > 0) {
        if (debug_) {
          printf("%p: Unmapping unused buffers at index %zd\n", this, i);
        }
        UnmapMemory(buffers_[i].buffer, buffers_size, "buffers");
        buffers_[i].buffer = nullptr;
        buffers_[i].slot_size = 0;
      }
    }
  }
}

bool ClientChannel::ValidateSlotBuffer(MessageSlot *slot,
                                              std::function<bool()> reload) {
  if (slot->buffer_index < 0) {
    return true;
  }
  if (reload == nullptr) {
    return true;
  }
  char* buf = Buffer(slot->id);
  int retries = 1000;
  while (retries-- > 0 && buf == nullptr) {
    ReloadIfNecessary(reload);
    buf = Buffer(slot->id);
  }
  if (buf == nullptr) {
    return false;
  }
  return true;
}

} // namespace details
} // namespace subspace
