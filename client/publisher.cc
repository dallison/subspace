// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/publisher.h"
#include "client/checksum.h"
#include "client_channel.h"
#include "common/client_buffer.h"
#include "common/message_checksum.h"
#include "common/publish.h"
#include "toolbelt/clock.h"
#include <algorithm>
#include <atomic>
#include <thread>
namespace subspace {
namespace details {

class SubscriberQueuePublishGuard {
public:
  explicit SubscriberQueuePublishGuard(PublisherImpl &publisher)
      : depth_(publisher),
        scope_(publisher.GetAvailableSlotQueueIndexAddress(),
               publisher.GetPublisherId()) {}

  const SubscriberQueuePublishScope &Scope() const { return scope_; }

private:
  class Depth {
  public:
    explicit Depth(PublisherImpl &publisher) : publisher_(publisher) {
      publisher_.BeginQueuePublishDepth();
    }
    ~Depth() { publisher_.EndQueuePublishDepth(); }

  private:
    PublisherImpl &publisher_;
  };

  // The depth is counted before shared memory and released after it, so it
  // is never less than this publisher's share of active_publishers.
  Depth depth_;
  SubscriberQueuePublishScope scope_;
};

absl::Status PublisherImpl::CreateOrAttachBuffers(uint64_t final_slot_size) {
  if (final_slot_size == 0) {
    // If we are being asked for a slot size of 0, we will just use 64 bytes.
    // This is the minimum slot size we can use.
    final_slot_size = 64;
  }
  size_t final_buffer_size = size_t(SlotSizeToBufferSize(final_slot_size));
  uint64_t current_slot_size = 0;
  int num_buffers = ccb_->num_buffers.load(std::memory_order_relaxed);

  for (;;) {
    while (current_slot_size < final_slot_size ||
           buffers_.size() < size_t(num_buffers)) {
      size_t buffer_index = buffers_.size();
      if (UseSplitBuffers()) {
        auto split_buffer =
            CreateSplitBufferSet(buffer_index, final_buffer_size,
                                 final_slot_size);
        if (!split_buffer.ok()) {
          return split_buffer.status();
        }
        bcb_->sizes[buffers_.size()].store(final_buffer_size,
                                           std::memory_order_relaxed);
        buffers_.push_back(std::move(*split_buffer));
        current_slot_size = final_slot_size;
        continue;
      }
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
      bool opened_existing_buffer = buffer_index < size_t(num_buffers);
      absl::StatusOr<toolbelt::FileDescriptor> shm_fd =
          opened_existing_buffer
              ? OpenBuffer(buffer_index)
              : CreateBuffer(buffer_index, final_buffer_size);
#else
      bool opened_existing_buffer = false;
      auto shm_fd = CreateBuffer(buffer_index, final_buffer_size);
#endif
      if (!shm_fd.ok()) {
        return shm_fd.status();
      }
      if (!shm_fd->Valid()) {
        // The named shared memory already exists so we attach to it instead of
        // creating it.
        opened_existing_buffer = true;
        shm_fd = OpenBuffer(buffer_index);
        if (!shm_fd.ok()) {
          return shm_fd.status();
        }
      }
      if (opened_existing_buffer) {
        // Attaching to a buffer another publisher already created.  Its real
        // size may be larger than our requested size (for example the channel
        // was resized before we joined), so use the buffer's actual size and
        // record it under its own index rather than clobbering a neighbouring
        // slot with our smaller requested size.
        auto size = GetBufferSize(*shm_fd, buffer_index);
        if (!size.ok()) {
          return size.status();
        }
        current_slot_size = BufferSizeToSlotSize(*size);
        absl::StatusOr<char *> addr;
        if (current_slot_size > 0) {
          addr = MapBuffer(*shm_fd, *size, BufferMapMode::kReadWrite);
          if (!addr.ok()) {
            return addr.status();
          }
        } else {
          addr = nullptr;
        }
        auto buffer_set =
            std::make_unique<BufferSet>(*size, current_slot_size, *addr);
        buffer_set->fd = std::move(*shm_fd);
        buffers_.push_back(std::move(buffer_set));
        bcb_->sizes[buffer_index].store(*size, std::memory_order_relaxed);
      } else {
        // We successfully created the shared memory.
        bcb_->sizes[buffer_index].store(final_buffer_size,
                                        std::memory_order_relaxed);
        auto addr =
            MapBuffer(*shm_fd, final_buffer_size, BufferMapMode::kReadWrite);
        if (!addr.ok()) {
          return addr.status();
        }
        auto buffer_set = std::make_unique<BufferSet>(
            final_buffer_size, final_slot_size, *addr);
        buffer_set->fd = std::move(*shm_fd);
        buffers_.push_back(std::move(buffer_set));
        current_slot_size = final_slot_size;
      }
    }
    int new_num_buffers = int(buffers_.size());
    // Update the atomic numBuffers in the CCB.  If this fails it means
    // something else got there before us and we just go back and remap any new
    // buffers.
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
    int old_num_buffers = num_buffers;
#endif
    if (ccb_->num_buffers.compare_exchange_strong(num_buffers, new_num_buffers,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed)) {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
      if (!UseSplitBuffers() && client_buffer_registration_callback_) {
        for (int i = old_num_buffers; i < new_num_buffers; i++) {
          BufferSet &buffer = *buffers_[i];
          ClientBufferHandleMetadata metadata = {
              .channel_name = ResolvedName(),
              .session_id = session_id_,
              .buffer_index = static_cast<uint32_t>(i),
              .slot_id = 0,
              .is_prefix = false,
              .full_size = buffer.full_size,
              .allocation_size = buffer.full_size,
              .handle = static_cast<uintptr_t>(buffer.fd.Fd()),
              .allocator = ClientBufferAllocatorKind::kAndroidMemfd,
          };
          if (absl::Status status =
                  client_buffer_registration_callback_(metadata, &buffer.fd);
              !status.ok()) {
            int expected_num_buffers = new_num_buffers;
            if (ccb_->num_buffers.compare_exchange_strong(
                    expected_num_buffers, old_num_buffers,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
              for (int j = old_num_buffers; j < new_num_buffers; j++) {
                bcb_->sizes[j].store(0, std::memory_order_relaxed);
              }
            }
            const size_t rollback_to =
                std::min(static_cast<size_t>(old_num_buffers),
                         buffers_.size());
            for (size_t j = rollback_to; j < buffers_.size(); j++) {
              UnmapBufferSet(j, *buffers_[j],
                             /*destroy_owned_buffers=*/false);
            }
            buffers_.resize(rollback_to);
            return status;
          }
        }
      }
#endif
      // We successfully updated the number of buffers in the CCB.
      break;
    }
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
    if (!UseSplitBuffers()) {
      for (size_t i = 0; i < buffers_.size(); i++) {
        UnmapBufferSet(i, *buffers_[i], /*destroy_owned_buffers=*/false);
      }
      buffers_.clear();
      current_slot_size = 0;
    }
#endif
    // Another thread has updated the number of buffers in the CCB.  We need to
    // retry.
  }
  return absl::OkStatus();
}

void PublisherImpl::SetSlotToBiggestBuffer(MessageSlot *slot) {
  if (slot == nullptr) {
    return;
  }
  const int old_buffer_index =
      slot->buffer_index.load(std::memory_order_relaxed);
  if (old_buffer_index != -1) {
    // If the slot has a buffer (it's not in the free list), decrement the
    // refs for the buffer.
    if (bcb_->refs[old_buffer_index].load(std::memory_order_relaxed) > 0) {
      DecrementBufferRefs(old_buffer_index);
    }
  }
  const int new_buffer_index = buffers_.size() - 1;
  slot->buffer_index.store(new_buffer_index, std::memory_order_relaxed);
  IncrementBufferRefs(new_buffer_index);
}

// Other publishers can add buffers that this one hasn't mapped yet, so a
// claimed slot is checked before it's used.
class PublisherImpl::SearchHooks : public SlotSearchHooks {
public:
  explicit SearchHooks(PublisherImpl &publisher) : publisher_(publisher) {}

  void BeforeAttempt() { publisher_.CheckReload(); }
  bool UsableSlot(MessageSlot *slot) {
    return publisher_.ValidateSlotBuffer(slot);
  }
  void Embargo(int slot_id) { publisher_.embargoed_slots_.Set(slot_id); }
  bool Embargoed(int slot_id) const {
    return publisher_.embargoed_slots_.IsSet(slot_id);
  }
  void AssignBuffer(MessageSlot *slot) {
    publisher_.SetSlotToBiggestBuffer(slot);
  }
  MessagePrefix *Prefix(MessageSlot *slot) { return publisher_.Prefix(slot); }
  void ForcedReuse(MessageSlot *slot) {
    if (publisher_.NotifyRetirementOnForcedReuse()) {
      publisher_.TriggerRetirement(slot->id);
    }
  }

private:
  PublisherImpl &publisher_;
};

uint64_t PublisherImpl::RegisterLease(MessageSlot *slot) {
  if (slot == nullptr) {
    return 0;
  }
  uint64_t lease_id = next_lease_id_++;
  if (lease_id == 0) {
    lease_id = next_lease_id_++;
  }
  leased_slots_[slot->id] = lease_id;
  return lease_id;
}

MessageSlot *PublisherImpl::FindLease(int32_t slot_id,
                                      uint64_t lease_id) const {
  auto it = leased_slots_.find(slot_id);
  if (it == leased_slots_.end() || it->second != lease_id) {
    return nullptr;
  }
  MessageSlot *slot = GetSlot(slot_id);
  if (slot == nullptr ||
      slot->refs.load(std::memory_order_acquire) !=
          (kPubOwned | uint64_t(publisher_id_))) {
    return nullptr;
  }
  return slot;
}

void PublisherImpl::RemoveLease(int32_t slot_id) {
  leased_slots_.erase(slot_id);
}

MessageSlot *PublisherImpl::ClaimRetiredSlot(int32_t slot_id) {
  MessageSlot *slot = GetSlot(slot_id);
  if (slot == nullptr || !RetiredSlots().IsSet(slot_id)) {
    return nullptr;
  }

  uint64_t old_refs = slot->refs.load(std::memory_order_acquire);
  if (!RefsShowRetired(slot, old_refs)) {
    return nullptr;
  }
  uint64_t expected = BuildRefsBitField(
      slot->ordinal, (old_refs >> kVchanIdShift) & kVchanIdMask,
      (old_refs >> kRetiredRefsShift) & kRetiredRefsMask);
  if (!slot->refs.compare_exchange_strong(
          expected, kPubOwned | uint64_t(publisher_id_),
          std::memory_order_acquire, std::memory_order_relaxed)) {
    return nullptr;
  }
  RetiredSlots().Clear(slot_id);
  SearchHooks hooks(*this);
  ResetClaimedSlot(View(), slot, vchan_id_, hooks);
  return slot;
}

MessageSlot *PublisherImpl::ClaimAnyRetiredSlot() {
  MessageSlot *claimed = nullptr;
  RetiredSlots().Traverse([this, &claimed](size_t slot_id) {
    if (claimed == nullptr) {
      claimed = ClaimRetiredSlot(static_cast<int32_t>(slot_id));
    }
  });
  return claimed;
}

bool PublisherImpl::ReleaseLeasedSlot(MessageSlot *slot) {
  if (slot == nullptr ||
      slot->refs.load(std::memory_order_acquire) !=
          (kPubOwned | uint64_t(publisher_id_))) {
    return false;
  }
  slot->ordinal = 0;
  slot->timestamp = 0;
  slot->refs.store(0, std::memory_order_release);
  ForgetMessage(View(), slot);
  RetiredSlots().Set(slot->id);
  return true;
}

void PublisherImpl::RetirePublishedSlotImmediately(MessageSlot *slot) {
  if (slot == nullptr) {
    return;
  }
  const int32_t retirement_slot_id =
      slot->bridged_slot_id.load(std::memory_order_relaxed);
  if (TryRetireSlot(slot)) {
    TriggerRetirement(retirement_slot_id);
  }
}

SlotSearch PublisherImpl::SearchFor(int owner) const {
  SlotSearch search;
  search.owner = owner;
  search.vchan_id = vchan_id_;
  search.prefer_retired = options_.PreferRetiredSlots();
  search.reuse_retired = !ForTunnel();
  return search;
}

MessageSlot *PublisherImpl::FindFreeSlotUnreliable(int owner) {
  embargoed_slots_.ClearAll();
  SearchHooks hooks(*this);
  return ClaimSlotUnreliable(View(), SearchFor(owner), hooks);
}

MessageSlot *PublisherImpl::FindFreeSlotReliable(int owner) {
  embargoed_slots_.ClearAll();
  SearchHooks hooks(*this);
  return ClaimSlotReliable(View(), SearchFor(owner), GetCounters(), hooks);
}

Channel::PublishedMessage PublisherImpl::ActivateSlotAndGetAnother(
    MessageSlot *slot, bool reliable, bool is_activation, int owner,
    bool omit_prefix, bool use_prefix_slot_id, bool for_tunnel,
    bool acquire_next, bool use_prefix_vchan_id) {
  void *buffer = GetBufferAddress(slot);
  MessagePrefix *prefix = Prefix(slot);

  StampSlot(ccb_, slot, toolbelt::Now());

  // Copy message parameters into message prefix in buffer.
  if (omit_prefix) {
    if (for_tunnel) {
      prefix->SetIsCrossMachine();
    }
    slot->timestamp.store(prefix->timestamp, std::memory_order_relaxed);
    slot->vchan_id.store(use_prefix_vchan_id ? prefix->vchan_id : vchan_id_,
                         std::memory_order_relaxed);
    // The bridged_slot_id is the slot is used for the retirement notification.
    slot->bridged_slot_id.store(
        use_prefix_slot_id ? prefix->slot_id : slot->id,
        std::memory_order_relaxed);
  } else {
    FillPrefix(ccb_, slot, prefix, ChecksumSize(), MetadataSize(),
               is_activation);
    if (for_tunnel) {
      prefix->SetIsCrossMachine();
    }
    if (options_.Checksum()) {
      prefix->SetHasChecksum();
      const uint64_t message_size =
          slot->message_size.load(std::memory_order_relaxed);
      if (checksum_callback_ != nullptr) {
        auto data = GetMessageChecksumData(prefix, buffer, message_size,
                                           ChecksumSize(), MetadataSize());
        checksum_callback_(data.Spans(),
                           GetChecksumSpan(prefix, ChecksumSize()));
      } else {
        SetMessageChecksum(prefix, buffer, message_size, ChecksumSize(),
                           MetadataSize());
      }
    }
  }

  PublishedSlot published;
  {
    SubscriberQueuePublishGuard publish_guard(*this);
    published = CommitPublishedSlot(View(), slot, vchan_id_, is_activation,
                                    publish_guard.Scope(),
                                    [this](int32_t retirement_slot_id) {
                                      TriggerRetirement(retirement_slot_id);
                                    });
  }
  if (published.retired) {
    TriggerRetirement(published.retirement_slot_id);
  }

  if (!acquire_next) {
    return {nullptr, published.ordinal, published.timestamp};
  }

  // A reliable publisher doesn't allocate a slot until it is asked for.
  if (reliable) {
    CheckReload();
    return {nullptr, 0, 0};
  }
  // Find a new slot.x
  MessageSlot *new_slot = FindFreeSlotUnreliable(owner);

  return {new_slot, published.ordinal, published.timestamp};
}

} // namespace details
} // namespace subspace
