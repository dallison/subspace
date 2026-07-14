// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/server_channel.h"
#include "absl/strings/str_format.h"
#include "server/client_handler.h"
#include "server/server.h"
#include <cerrno>
#include <csignal>
#include <utility>
#include <sys/mman.h>
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
#include <sys/syscall.h>
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
#endif
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX && defined(__APPLE__)
#include <sys/posix_shm.h>
#endif

namespace subspace {
namespace {

bool ProcessDefinitelyDead(uint64_t process_id) {
  if (process_id == 0) {
    return false;
  }
  errno = 0;
  return kill(static_cast<pid_t>(process_id), 0) == -1 && errno == ESRCH;
}

} // namespace

ServerChannel::~ServerChannel() {
  if (is_virtual_ || skip_cleanup_) {
    return;
  }
  // Clear the channel counters in the SCB.
  memset(&GetScb()->counters[GetChannelId()], 0, sizeof(ChannelCounters));
}

static absl::StatusOr<void *> CreateSharedMemory(int id, const char *suffix,
                                                 int64_t size, bool map,
                                                 toolbelt::FileDescriptor &fd,
                                                 [[maybe_unused]] uint64_t session_id = 0) {
  char shm_file[NAME_MAX]; // Unique file in file system.
  int tmpfd;

#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
  snprintf(shm_file, sizeof(shm_file), "subspace.%d.%s", id, suffix);
#ifdef __NR_memfd_create
  tmpfd = static_cast<int>(syscall(
      __NR_memfd_create, shm_file, static_cast<unsigned int>(MFD_CLOEXEC)));
  if (tmpfd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create anonymous shared memory %s: %s", shm_file,
        strerror(errno)));
  }
#else
  return absl::UnimplementedError("memfd_create is not available");
#endif

  int e = ftruncate(tmpfd, size);
  if (e == -1) {
    close(tmpfd);
    return absl::InternalError(
        absl::StrFormat("Failed to set length of shared memory %s: %s",
                        shm_file, strerror(errno)));
  }

  void *p = nullptr;
  if (map) {
    p = MapMemory(tmpfd, size, PROT_READ | PROT_WRITE, suffix);
    if (p == MAP_FAILED) {
      close(tmpfd);
      return absl::InternalError(absl::StrFormat(
          "Failed to map shared memory %s: %s", shm_file, strerror(errno)));
    }
  }

  fd.SetFd(tmpfd);
  return p;

#else
  char *shm_name;          // Name passed to shm_* (starts with /)
  // Create a shadow file in /tmp for uniqueness.  The shm object name is
  // derived from this path and unlinked while the shadow file stays around for
  // lifetime/cleanup bookkeeping.
  snprintf(shm_file, sizeof(shm_file), "/tmp/%08x.%d.%s.XXXXXX",
           static_cast<uint32_t>(session_id), id, suffix);
  tmpfd = mkstemp(shm_file);
  shm_name = shm_file + 4; // After /tmp
  // Remove any existing shared memory.
  shm_unlink(shm_name);

  // Open the shared memory file.
  int shm_fd = shm_open(shm_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (shm_fd == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open shared memory %s: %s", shm_name, strerror(errno)));
  }

  // Make it the appropriate size.
  int e = ftruncate(shm_fd, size);
  if (e == -1) {
    shm_unlink(shm_name);
    return absl::InternalError(
        absl::StrFormat("Failed to set length of shared memory %s: %s",
                        shm_name, strerror(errno)));
  }

  // Map it into memory if asked
  void *p = nullptr;
  if (map) {
    p = MapMemory(shm_fd, size, PROT_READ | PROT_WRITE, suffix);
    if (p == MAP_FAILED) {
      shm_unlink(shm_name);
      return absl::InternalError(absl::StrFormat(
          "Failed to map shared memory %s: %s", shm_name, strerror(errno)));
    }
  }

  // Don't need the file now.  It stays open and available to be mapped in
  // using the file descriptor.
  shm_unlink(shm_name);
  fd.SetFd(shm_fd);
  (void)close(tmpfd);
  return p;
#endif
}

absl::StatusOr<SystemControlBlock *>
CreateSystemControlBlock(toolbelt::FileDescriptor &fd, uint64_t session_id) {
  absl::StatusOr<void *> s = CreateSharedMemory(
      0, "scb", sizeof(SystemControlBlock), /*map=*/true, fd, session_id);
  if (!s.ok()) {
    return s.status();
  }
  SystemControlBlock *scb = reinterpret_cast<SystemControlBlock *>(*s);
  memset(&scb->counters, 0, sizeof(scb->counters));
  return scb;
}

absl::Status ServerChannel::ValidateOrSetSplitBufferOptions(
    const SplitBufferOptions &options, bool set_if_missing,
    const char *user_type) {
  if (!split_buffer_options_set_) {
    if (set_if_missing || options.use_split_buffers ||
        options.split_buffers_over_bridge) {
      split_buffer_options_ = options;
      split_buffer_options_set_ = true;
    }
    return absl::OkStatus();
  }

  if (split_buffer_options_.use_split_buffers != options.use_split_buffers) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Inconsistent split-buffer mode for %s on channel %s", user_type,
        Name()));
  }
  if (split_buffer_options_.split_buffers_over_bridge !=
      options.split_buffers_over_bridge) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Inconsistent bridge split-buffer mode for %s on channel %s", user_type,
        Name()));
  }
  return absl::OkStatus();
}

absl::Status ServerChannel::ValidateOrSetMaxPublishers(
    int32_t max_publishers, bool set_if_missing, const char *user_type) {
  if (max_publishers < 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Invalid max_publishers %d for %s on channel %s: value must be "
        "non-negative",
        max_publishers, user_type, Name()));
  }
  if (!max_publishers_set_) {
    if (set_if_missing || max_publishers > 0) {
      max_publishers_ = max_publishers;
      max_publishers_set_ = true;
    }
    return absl::OkStatus();
  }
  if (max_publishers_ != max_publishers) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Inconsistent max_publishers for %s on channel %s: already %d, not %d",
        user_type, Name(), max_publishers_, max_publishers));
  }
  return absl::OkStatus();
}

void ServerChannel::RemoveBuffer(uint64_t session_id, Server *server) {
  if (ccb_ == nullptr) {
    return;
  }
  for (int i = 0; i < ccb_->num_buffers; i++) {
    std::string filename = BufferSharedMemoryName(session_id, i);
    auto group = client_buffers_.find(
        ClientBufferGroupKey{session_id, static_cast<uint32_t>(i)});
    if (group != client_buffers_.end()) {
      for (const auto &[slot, buffer] : group->second) {
        const ClientBufferHandleMetadata &metadata = buffer.metadata;
        bool plugin_freed = false;
        if (server != nullptr) {
          absl::StatusOr<bool> freed =
              server->FreeClientBufferWithPlugins(metadata);
          if (freed.ok()) {
            plugin_freed = *freed;
          }
        }
        if (!plugin_freed && !metadata.object_name.empty()) {
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
          // Anonymous fd-backed memfd buffers are released when the server's
          // registered fd is closed.
#else
          (void)shm_unlink(metadata.object_name.c_str());
#endif
        }
        if (!metadata.shadow_file.empty()) {
          (void)remove(metadata.shadow_file.c_str());
        }
      }
    }
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
    auto shm_name = PosixSharedMemoryName(filename);
    if (shm_name.ok()) {
      (void)shm_unlink(shm_name->c_str());
    }
    remove(filename.c_str());
#elif SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD
    (void)unlink(filename.c_str());
#else
    (void)shm_unlink(filename.c_str());
#endif
  }
  client_buffers_.clear();
}

uint64_t ServerChannel::GetVirtualMemoryUsage() const {
  if (!split_buffer_options_set_ || !split_buffer_options_.use_split_buffers ||
      ccb_ == nullptr || bcb_ == nullptr) {
    return Channel::GetVirtualMemoryUsage();
  }

  uint64_t split_buffer_size = 0;
  ForEachClientBuffer([&](const RegisteredClientBuffer &buffer) {
    const ClientBufferHandleMetadata &metadata = buffer.metadata;
    if (metadata.buffer_index >= static_cast<uint32_t>(ccb_->num_buffers)) {
      return;
    }
    if (bcb_->refs[metadata.buffer_index].load(std::memory_order_relaxed) <=
        0) {
      return;
    }
    split_buffer_size += metadata.allocation_size;
  });

  if (split_buffer_size == 0) {
    return Channel::GetVirtualMemoryUsage();
  }
  return sizeof(SystemControlBlock) + CcbSize(num_slots_, subscriber_queue_size_) +
         sizeof(BufferControlBlock) + split_buffer_size;
}

absl::StatusOr<SharedMemoryFds>
ServerChannel::Allocate(const toolbelt::FileDescriptor &scb_fd,
                        [[maybe_unused]] int slot_size, int num_slots,
                        int subscriber_queue_size, int initial_ordinal) {
  // Unmap existing memory.
  Unmap();

  // If the channel is being remapped (a subscriber that existed
  // before the first publisher), num_slots_ will be zero and we
  // set it here now that we know it.  If num_slots_ was already
  // set we need to make sure that the value passed here is
  // the same as the current value.
  if (num_slots_ != 0) {
    assert(num_slots_ == num_slots);
  } else {
    num_slots_ = num_slots;
  }
  SetSubscriberQueueSize(subscriber_queue_size);

  // Map SCB into process memory.
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s", strerror(errno)));
  }

  SharedMemoryFds fds;

  // Create CCB in shared memory and map into process memory.
  absl::StatusOr<size_t> checked_ccb_size =
      CheckedCcbSize(num_slots_, subscriber_queue_size_);
  if (!checked_ccb_size.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return checked_ccb_size.status();
  }
  absl::StatusOr<void *> p = CreateSharedMemory(
      channel_id_, "ccb", *checked_ccb_size,
      /*map=*/true, fds.ccb, session_id_);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return p.status();
  }
  ccb_ = reinterpret_cast<ChannelControlBlock *>(*p);
  ccb_->num_subs = SubscriberCounter();

  // Create buffer control block.
  p = CreateSharedMemory(channel_id_, "bcb", sizeof(BufferControlBlock),
                         /*map=*/true, fds.bcb, session_id_);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, CcbSize(num_slots_, subscriber_queue_size_), "CCB");
    return p.status();
  }
  bcb_ = reinterpret_cast<BufferControlBlock *>(*p);
  ccb_->num_buffers = 0;

  // Build CCB data.
  // Copy possibly truncated channel name into CCB for ease
  // of debugging (you can see it in all processes).
  strncpy(ccb_->channel_name, name_.c_str(), kMaxChannelName - 1);
  ccb_->num_slots = num_slots_;
  ccb_->subscriber_queue_size = subscriber_queue_size_;
  ccb_->version = kChannelControlBlockVersion;

  // Initialize all ordinals.
  ccb_->ordinals.Init(initial_ordinal);

  new (&ccb_->subscribers) AtomicBitSet<kMaxSlotOwners>();
  auto *queue_index =
      new (GetAvailableSlotQueueIndexAddress()) AvailableSlotQueueIndex;
  queue_index->next_offset.store(0, std::memory_order_relaxed);
  for (auto &offset : queue_index->offsets) {
    offset.store(kInvalidSlotQueueOffset, std::memory_order_relaxed);
  }
  for (auto &active : queue_index->active_publishers) {
    active.store(0, std::memory_order_relaxed);
  }

  // Initialize all slots
  for (int32_t i = 0; i < num_slots_; i++) {
    MessageSlot *slot = &ccb_->slots[i];
    slot->id = i;
    slot->refs.store(0, std::memory_order_relaxed);
    slot->ordinal.store(0, std::memory_order_relaxed);
    slot->message_size.store(0, std::memory_order_relaxed);
    slot->vchan_id.store(-1, std::memory_order_relaxed);
    slot->buffer_index.store(-1,
                             std::memory_order_relaxed); // No buffer in the free list.
    slot->timestamp.store(0, std::memory_order_relaxed);
    slot->flags.store(0, std::memory_order_relaxed);
    slot->bridged_slot_id.store(-1, std::memory_order_relaxed);
    new (&slot->sub_owners) AtomicBitSet<kMaxSlotOwners>();
  }

  // Initialize the available slots for each subscriber.
  if (num_slots_ > 0) {
    // No retired slots initially.
    new (RetiredSlotsAddr()) InPlaceAtomicBitset(num_slots_);

    // All slots are initially free.
    new (FreeSlotsAddr()) InPlaceAtomicBitset(num_slots_);
    FreeSlots().SetAll();

    for (int i = 0; i < kMaxSlotOwners; i++) {
      new (GetAvailableSlotsAddress(i)) InPlaceAtomicBitset(num_slots_);
    }
  }

  if (debug_) {
    printf("Channel allocated: scb: %p, ccb: %p, bcb: %p\n", scb_, ccb_, bcb_);
    Dump(std::cout);
  }
  return fds;
}

absl::Status
ServerChannel::MapExisting(const toolbelt::FileDescriptor &scb_fd,
                           toolbelt::FileDescriptor ccb_fd,
                           toolbelt::FileDescriptor bcb_fd) {
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map recovered SCB: %s", strerror(errno)));
  }

  absl::StatusOr<size_t> checked_ccb_size =
      CheckedCcbSize(num_slots_, subscriber_queue_size_);
  if (!checked_ccb_size.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return checked_ccb_size.status();
  }
  ccb_ = reinterpret_cast<ChannelControlBlock *>(MapMemory(
      ccb_fd.Fd(), *checked_ccb_size, PROT_READ | PROT_WRITE, "CCB"));
  if (ccb_ == MAP_FAILED) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return absl::InternalError(absl::StrFormat(
        "Failed to map recovered CCB: %s", strerror(errno)));
  }
  if (ccb_->version != kChannelControlBlockVersion) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, *checked_ccb_size, "CCB");
    return absl::FailedPreconditionError(absl::StrFormat(
        "unsupported channel control block version %u (expected %u)",
        ccb_->version, kChannelControlBlockVersion));
  }
  AvailableSlotQueueIndex *queue_index = GetAvailableSlotQueueIndexAddress();
  const uint64_t arena_size = AvailableSlotQueuesSize(SubscriberQueueSize());
  const uint64_t next_offset =
      queue_index->next_offset.load(std::memory_order_acquire);
  if (next_offset > arena_size) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, *checked_ccb_size, "CCB");
    return absl::FailedPreconditionError(
        "recovered subscriber queue arena high-water mark is out of range");
  }
  char *arena = EndOfAvailableSlotQueueIndex();
  for (uint64_t offset = 0; offset < next_offset;) {
    auto *block = reinterpret_cast<SlotQueueBlockHeader *>(arena + offset);
    const uint32_t state = block->state.load(std::memory_order_acquire);
    if (block->block_size < SlotQueueBlockSize(0) ||
        block->block_size > next_offset - offset ||
        state > static_cast<uint32_t>(SlotQueueBlockState::kFree)) {
      UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
      UnmapMemory(ccb_, *checked_ccb_size, "CCB");
      return absl::FailedPreconditionError(
          "recovered subscriber queue arena contains a corrupt block");
    }
    offset += block->block_size;
  }
  for (int sub_id = 0; sub_id < kMaxSlotOwners; ++sub_id) {
    const uint64_t offset =
        queue_index->offsets[sub_id].load(std::memory_order_acquire);
    if (offset == kInvalidSlotQueueOffset) {
      continue;
    }
    if (offset < SlotQueueBlockHeaderSize() || offset >= next_offset) {
      UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
      UnmapMemory(ccb_, *checked_ccb_size, "CCB");
      return absl::FailedPreconditionError(
          "recovered subscriber queue offset is out of range");
    }
    auto *block = reinterpret_cast<SlotQueueBlockHeader *>(
        arena + offset - SlotQueueBlockHeaderSize());
    auto *queue = reinterpret_cast<InPlaceSlotQueue *>(arena + offset);
    if (block->state.load(std::memory_order_acquire) !=
            static_cast<uint32_t>(SlotQueueBlockState::kAllocated) ||
        queue->Capacity() > kDefaultMaxAvailableSlotQueueCapacity ||
        Aligned(SizeofSlotQueue(queue->Capacity())) >
            block->block_size - SlotQueueBlockHeaderSize()) {
      UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
      UnmapMemory(ccb_, *checked_ccb_size, "CCB");
      return absl::FailedPreconditionError(
          "recovered subscriber queue metadata is inconsistent");
    }
  }

  bcb_ = reinterpret_cast<BufferControlBlock *>(MapMemory(
      bcb_fd.Fd(), sizeof(BufferControlBlock), PROT_READ | PROT_WRITE, "BCB"));
  if (bcb_ == MAP_FAILED) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, *checked_ccb_size, "CCB");
    return absl::InternalError(absl::StrFormat(
        "Failed to map recovered BCB: %s", strerror(errno)));
  }

  shared_memory_fds_ =
      SharedMemoryFds(std::move(ccb_fd), std::move(bcb_fd));
  return absl::OkStatus();
}

std::vector<toolbelt::FileDescriptor>
ServerChannel::GetSubscriberTriggerFds() const {
  std::vector<toolbelt::FileDescriptor> r;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsSubscriber()) {
      r.push_back(user->GetTriggerFd());
    }
  }
  return r;
}

std::vector<toolbelt::FileDescriptor>
ServerChannel::GetReliablePublisherTriggerFds() const {
  std::vector<toolbelt::FileDescriptor> r;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher() && user->IsReliable()) {
      r.push_back(user->GetTriggerFd());
    }
  }
  return r;
}

std::vector<toolbelt::FileDescriptor> ServerChannel::GetRetirementFds() const {
  std::vector<toolbelt::FileDescriptor> r;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      // The publisher probably won't have a retirement fd.
      auto &fd =
          static_cast<PublisherUser *>(user.get())->GetRetirementFdWriter();
      if (!fd.Valid()) {
        continue;
      }
      r.push_back(fd);
    }
  }
  return r;
}
// User ids are allocated from the multiplexer as all virtual channels
// on the mux share the same CCB.
absl::StatusOr<int> ServerChannel::AllocateUserId(const char *type) {
  return user_ids_.Allocate(type);
}

absl::StatusOr<PublisherUser *>
ServerChannel::AddPublisher(ClientHandler *handler, bool is_reliable,
                            bool is_local, bool is_bridge, bool for_tunnel,
                            bool is_fixed_size, uint64_t process_id) {
  absl::StatusOr<int> user_id = AllocateUserId("publisher");
  if (!user_id.ok()) {
    return user_id.status();
  }
  std::unique_ptr<PublisherUser> pub = std::make_unique<PublisherUser>(
      handler, *user_id, is_reliable, is_local, is_bridge, for_tunnel,
      is_fixed_size);
  pub->SetProcessId(process_id);
  absl::Status status = pub->Init();
  if (!status.ok()) {
    return status;
  }
  PublisherUser *result = pub.get();
  AddUser(*user_id, std::move(pub));

  return result;
}

absl::StatusOr<SubscriberUser *>
ServerChannel::AddSubscriber(ClientHandler *handler, bool is_reliable,
                             bool is_bridge, bool for_tunnel,
                             int max_active_messages,
                             int subscriber_queue_size, uint64_t process_id) {
  absl::StatusOr<int> user_id = AllocateUserId("subscriber");
  if (!user_id.ok()) {
    return user_id.status();
  }
  if (absl::Status status =
          AllocateSubscriberQueue(*user_id, subscriber_queue_size);
      !status.ok()) {
    RemoveUserId(*user_id);
    return status;
  }
  std::unique_ptr<SubscriberUser> sub = std::make_unique<SubscriberUser>(
      handler, *user_id, is_reliable, is_bridge, for_tunnel,
      max_active_messages, subscriber_queue_size);
  sub->SetProcessId(process_id);
  absl::Status status = sub->Init();
  if (!status.ok()) {
    RetireSubscriberQueue(*user_id);
    RemoveUserId(*user_id);
    return status;
  }
  SubscriberUser *result = sub.get();
  AddUser(*user_id, std::move(sub));
  return result;
}

absl::Status
ServerChannel::AllocateSubscriberQueue(int sub_id,
                                       int subscriber_queue_size) {
  if (IsVirtual()) {
    return static_cast<VirtualChannel *>(this)
        ->GetMux()
        ->AllocateSubscriberQueue(sub_id, subscriber_queue_size);
  }
  if (IsPlaceholder()) {
    return absl::OkStatus();
  }
  const int capacity =
      ResolveSubscriberQueueSize(NumSlots(), subscriber_queue_size == 0
                                                 ? SubscriberQueueSize()
                                                 : subscriber_queue_size);
  AvailableSlotQueueIndex *index = GetAvailableSlotQueueIndexAddress();
  if (capacity == 0) {
    index->offsets[sub_id].store(kInvalidSlotQueueOffset,
                                 std::memory_order_release);
    return absl::OkStatus();
  }

  const size_t allocation_size =
      SlotQueueBlockSize(static_cast<size_t>(capacity));
  const size_t arena_size = AvailableSlotQueuesSize(SubscriberQueueSize());
  uint64_t next_offset =
      index->next_offset.load(std::memory_order_relaxed);
  char *arena = EndOfAvailableSlotQueueIndex();
  auto block_at = [arena](uint64_t offset) {
    return reinterpret_cast<SlotQueueBlockHeader *>(arena + offset);
  };
  auto state_of = [](SlotQueueBlockHeader *block) {
    return static_cast<SlotQueueBlockState>(
        block->state.load(std::memory_order_acquire));
  };

  // Publishers that started after subscriber retirement cannot observe the
  // retired subscriber bit. Once every publisher that was active at retirement
  // has left its traversal, the block is safe to reuse.
  for (uint64_t offset = 0; offset < next_offset;) {
    SlotQueueBlockHeader *block = block_at(offset);
    if (block->block_size < SlotQueueBlockSize(0) ||
        block->block_size > next_offset - offset) {
      return absl::InternalError(
          absl::StrFormat("corrupt subscriber queue arena for channel %s",
                          Name()));
    }
    if (state_of(block) == SlotQueueBlockState::kRetired) {
      block->waiting_publishers.Traverse([block, index](int pub_id) {
        if (index->active_publishers[pub_id].load(
                std::memory_order_seq_cst) == 0) {
          block->waiting_publishers.Clear(pub_id);
        }
      });
      if (block->waiting_publishers.IsEmpty()) {
        block->state.store(static_cast<uint32_t>(SlotQueueBlockState::kFree),
                           std::memory_order_release);
      }
    }
    offset += block->block_size;
  }

  // Coalesce adjacent safe free blocks to avoid permanent fragmentation when
  // subscribers churn between different queue capacities.
  for (uint64_t offset = 0; offset < next_offset;) {
    SlotQueueBlockHeader *block = block_at(offset);
    if (state_of(block) == SlotQueueBlockState::kFree) {
      while (offset + block->block_size < next_offset) {
        SlotQueueBlockHeader *next = block_at(offset + block->block_size);
        if (state_of(next) != SlotQueueBlockState::kFree) {
          break;
        }
        block->block_size += next->block_size;
      }
    }
    offset += block->block_size;
  }

  uint64_t block_offset = kInvalidSlotQueueOffset;
  uint64_t best_size = std::numeric_limits<uint64_t>::max();
  for (uint64_t offset = 0; offset < next_offset;) {
    SlotQueueBlockHeader *block = block_at(offset);
    if (state_of(block) == SlotQueueBlockState::kFree &&
        block->block_size >= allocation_size && block->block_size < best_size) {
      block_offset = offset;
      best_size = block->block_size;
    }
    offset += block->block_size;
  }

  if (block_offset == kInvalidSlotQueueOffset) {
    if (allocation_size > arena_size ||
        next_offset > arena_size - allocation_size) {
      return absl::ResourceExhaustedError(absl::StrFormat(
          "subscriber queue capacity %d does not fit in channel %s queue arena "
          "(%zu of %zu bytes remain)",
          capacity, Name(),
          arena_size - std::min<size_t>(next_offset, arena_size), arena_size));
    }
    block_offset = next_offset;
    SlotQueueBlockHeader *block =
        new (arena + block_offset) SlotQueueBlockHeader;
    block->block_size = allocation_size;
    next_offset += allocation_size;
    index->next_offset.store(next_offset, std::memory_order_relaxed);
  } else {
    SlotQueueBlockHeader *block = block_at(block_offset);
    const uint64_t remainder = block->block_size - allocation_size;
    if (remainder >= SlotQueueBlockSize(0)) {
      block->block_size = allocation_size;
      SlotQueueBlockHeader *split =
          new (arena + block_offset + allocation_size) SlotQueueBlockHeader;
      split->block_size = remainder;
      split->state.store(
          static_cast<uint32_t>(SlotQueueBlockState::kFree),
          std::memory_order_relaxed);
    }
  }

  SlotQueueBlockHeader *block = block_at(block_offset);
  block->waiting_publishers.ClearAll();
  block->state.store(
      static_cast<uint32_t>(SlotQueueBlockState::kAllocated),
      std::memory_order_relaxed);
  const uint64_t queue_offset = block_offset + SlotQueueBlockHeaderSize();
  new (arena + queue_offset)
      InPlaceSlotQueue(static_cast<size_t>(capacity),
                       /*drop_oldest=*/subscriber_queue_size != 0);
  index->offsets[sub_id].store(queue_offset, std::memory_order_release);
  return absl::OkStatus();
}

void ServerChannel::RetireSubscriberQueue(int sub_id) {
  if (IsVirtual()) {
    static_cast<VirtualChannel *>(this)->GetMux()->RetireSubscriberQueue(sub_id);
    return;
  }
  if (IsPlaceholder()) {
    return;
  }
  AvailableSlotQueueIndex *index = GetAvailableSlotQueueIndexAddress();
  const uint64_t queue_offset =
      index->offsets[sub_id].exchange(kInvalidSlotQueueOffset,
                                      std::memory_order_seq_cst);
  if (queue_offset == kInvalidSlotQueueOffset ||
      queue_offset < SlotQueueBlockHeaderSize()) {
    return;
  }
  const uint64_t block_offset = queue_offset - SlotQueueBlockHeaderSize();
  if (block_offset >=
      index->next_offset.load(std::memory_order_acquire)) {
    return;
  }
  auto *block = reinterpret_cast<SlotQueueBlockHeader *>(
      EndOfAvailableSlotQueueIndex() + block_offset);
  block->waiting_publishers.ClearAll();
  for (int pub_id = 0; pub_id < kMaxSlotOwners; ++pub_id) {
    if (index->active_publishers[pub_id].load(std::memory_order_seq_cst) != 0) {
      block->waiting_publishers.Set(pub_id);
    }
  }
  block->state.store(
      static_cast<uint32_t>(block->waiting_publishers.IsEmpty()
                                ? SlotQueueBlockState::kFree
                                : SlotQueueBlockState::kRetired),
      std::memory_order_release);
}

absl::Status ServerChannel::ReconcileSubscriberQueueArena() {
  if (IsVirtual()) {
    return static_cast<VirtualChannel *>(this)
        ->GetMux()
        ->ReconcileSubscriberQueueArena();
  }
  if (IsPlaceholder()) {
    return absl::OkStatus();
  }

  AvailableSlotQueueIndex *index = GetAvailableSlotQueueIndexAddress();
  const uint64_t next_offset =
      index->next_offset.load(std::memory_order_acquire);
  char *arena = EndOfAvailableSlotQueueIndex();
  auto block_at = [arena](uint64_t offset) {
    return reinterpret_cast<SlotQueueBlockHeader *>(arena + offset);
  };

  absl::flat_hash_map<uint64_t, int> owners;
  for (int sub_id = 0; sub_id < kMaxSlotOwners; ++sub_id) {
    const uint64_t queue_offset =
        index->offsets[sub_id].load(std::memory_order_acquire);
    if (queue_offset == kInvalidSlotQueueOffset) {
      continue;
    }
    if (!ccb_->subscribers.IsSet(sub_id)) {
      RetireSubscriberQueue(sub_id);
      continue;
    }
    const uint64_t block_offset = queue_offset - SlotQueueBlockHeaderSize();
    if (!owners.emplace(block_offset, sub_id).second) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "subscriber queue arena for channel %s has duplicate ownership of "
          "block offset %llu",
          Name(), static_cast<unsigned long long>(block_offset)));
    }
  }

  for (uint64_t offset = 0; offset < next_offset;) {
    SlotQueueBlockHeader *block = block_at(offset);
    SlotQueueBlockState state = static_cast<SlotQueueBlockState>(
        block->state.load(std::memory_order_acquire));
    if (state == SlotQueueBlockState::kAllocated &&
        !owners.contains(offset)) {
      // A crash may occur after constructing a block but before publishing its
      // subscriber offset, or after clearing the offset but before retirement.
      // Retire conservatively against every publisher that may still hold an
      // arena pointer.
      block->waiting_publishers.ClearAll();
      for (int pub_id = 0; pub_id < kMaxSlotOwners; ++pub_id) {
        if (index->active_publishers[pub_id].load(
                std::memory_order_seq_cst) != 0) {
          block->waiting_publishers.Set(pub_id);
        }
      }
      state = block->waiting_publishers.IsEmpty()
                  ? SlotQueueBlockState::kFree
                  : SlotQueueBlockState::kRetired;
      block->state.store(static_cast<uint32_t>(state),
                         std::memory_order_release);
    }
    if (state == SlotQueueBlockState::kRetired) {
      block->waiting_publishers.Traverse([block, index](int pub_id) {
        if (index->active_publishers[pub_id].load(
                std::memory_order_seq_cst) == 0) {
          block->waiting_publishers.Clear(pub_id);
        }
      });
      if (block->waiting_publishers.IsEmpty()) {
        block->state.store(static_cast<uint32_t>(SlotQueueBlockState::kFree),
                           std::memory_order_release);
      }
    }
    offset += block->block_size;
  }

  for (uint64_t offset = 0; offset < next_offset;) {
    SlotQueueBlockHeader *block = block_at(offset);
    if (static_cast<SlotQueueBlockState>(
            block->state.load(std::memory_order_acquire)) ==
        SlotQueueBlockState::kFree) {
      while (offset + block->block_size < next_offset) {
        SlotQueueBlockHeader *next = block_at(offset + block->block_size);
        if (static_cast<SlotQueueBlockState>(
                next->state.load(std::memory_order_acquire)) !=
            SlotQueueBlockState::kFree) {
          break;
        }
        block->block_size += next->block_size;
      }
    }
    offset += block->block_size;
  }
  return absl::OkStatus();
}

void ServerChannel::ClearPublisherQueueHazardIfDead(int publisher_id,
                                                    uint64_t process_id) {
  if (!ProcessDefinitelyDead(process_id) || IsPlaceholder()) {
    return;
  }
  ServerChannel *storage_channel =
      IsVirtual()
          ? static_cast<ServerChannel *>(
                static_cast<VirtualChannel *>(this)->GetMux())
          : this;
  storage_channel->GetAvailableSlotQueueIndexAddress()
      ->active_publishers[publisher_id]
      .store(0, std::memory_order_seq_cst);
}

void ServerChannel::CleanupSlots(int owner, bool reliable, bool is_pub,
                                 int vchan_id) {
  if (!is_pub) {
    ccb_->subscribers.ClearSeqCst(owner);
  }
  Channel::CleanupSlots(owner, reliable, is_pub, vchan_id);
  if (!is_pub) {
    RetireSubscriberQueue(owner);
  }
}

std::vector<std::string> ServerChannel::RegisterExistingSubscribers() {
  std::vector<std::string> warnings;
  for (auto &[id, user] : users_) {
    if (user == nullptr || !user->IsSubscriber()) {
      continue;
    }
    auto *sub = static_cast<SubscriberUser *>(user.get());
    if (absl::Status status =
            AllocateSubscriberQueue(id, sub->SubscriberQueueSize());
        !status.ok()) {
      // The arena was provisioned from the publisher default and should fit
      // every default-sized subscriber. A pre-publisher override can still
      // exceed that budget, so leave that subscriber on the bitset path.
      RetireSubscriberQueue(id);
      warnings.push_back(absl::StrFormat(
          "Subscriber %d on channel %s requested queue capacity %d but the "
          "publisher-provisioned arena cannot fit it; using the bitset path: %s",
          id, Name(), sub->SubscriberQueueSize(), status.ToString()));
    }
    RegisterSubscriber(id, GetVirtualChannelId(), /*is_new=*/true);
  }
  return warnings;
}

std::vector<std::string> ChannelMultiplexer::RegisterExistingSubscribers() {
  std::vector<std::string> warnings =
      ServerChannel::RegisterExistingSubscribers();
  for (VirtualChannel *vchan : virtual_channels_) {
    std::vector<std::string> vchan_warnings =
        vchan->RegisterExistingSubscribers();
    warnings.insert(warnings.end(), vchan_warnings.begin(),
                    vchan_warnings.end());
  }
  return warnings;
}

void ServerChannel::TriggerAllSubscribers() {
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsSubscriber()) {
      user->Trigger();
    }
  }
}

void ServerChannel::RemoveUser(Server *server, int user_id) {
  if (IsVirtual()) {
    ChannelMultiplexer *mux = static_cast<VirtualChannel *>(this)->GetMux();
    mux->RemoveUserId(user_id);
  }
  auto it = users_.find(user_id);
  if (it == users_.end()) {
    return;
  }

  User *user = it->second.get();
  if (user == nullptr) {
    users_.erase(it);
    return;
  }
  if (user->IsPublisher()) {
    server->OnRemovePublisher(Name(), user->GetId());
    server->ForEachShadow(
        [this, &user](const std::unique_ptr<ShadowReplicator> &shadow) {
          shadow->SendRemovePublisher(Name(), user->GetId());
        });
  } else {
    server->OnRemoveSubscriber(Name(), user->GetId());
    server->ForEachShadow(
        [this, &user](const std::unique_ptr<ShadowReplicator> &shadow) {
          shadow->SendRemoveSubscriber(Name(), user->GetId());
        });
  }
  CleanupSlots(user->GetId(), user->IsReliable(), user->IsPublisher(),
               GetVirtualChannelId());
  if (user->IsPublisher() && !IsPlaceholder()) {
    ServerChannel *storage_channel =
        IsVirtual()
            ? static_cast<ServerChannel *>(
                  static_cast<VirtualChannel *>(this)->GetMux())
            : this;
    // RemoveUser is an explicit client request serialized with publication, so
    // no local SubscriberQueuePublishGuard can still be live.
    storage_channel->GetAvailableSlotQueueIndexAddress()
        ->active_publishers[user->GetId()]
        .store(0, std::memory_order_seq_cst);
  }
  RemoveUserId(user->GetId());
  RecordUpdate(user->IsPublisher(), /*add=*/false, user->IsReliable());
  if (user->IsPublisher()) {
    TriggerAllSubscribers();
  }
  users_.erase(it);
  if (IsEmpty()) {
    server->RemoveChannel(this);
  }
  server->SendChannelDirectory();
}

void ServerChannel::RemoveAllUsersFor(ClientHandler *handler) {
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->GetHandler() == handler) {
      CleanupSlots(user->GetId(), user->IsReliable(), user->IsPublisher(),
                   GetVirtualChannelId());
      if (user->IsPublisher() && !IsPlaceholder() &&
          ProcessDefinitelyDead(user->ProcessId())) {
        ServerChannel *storage_channel =
            IsVirtual()
                ? static_cast<ServerChannel *>(
                      static_cast<VirtualChannel *>(this)->GetMux())
                : this;
        storage_channel->GetAvailableSlotQueueIndexAddress()
            ->active_publishers[user->GetId()]
            .store(0, std::memory_order_seq_cst);
      }
      RemoveUserId(user->GetId());
      RecordUpdate(user->IsPublisher(), /*add=*/false, user->IsReliable());
      if (user->IsPublisher()) {
        TriggerAllSubscribers();
      }
      user.reset();
    }
  }
}

void ServerChannel::CountUsers(int &num_pubs, int &num_subs,
                               int &num_bridge_pubs, int &num_bridge_subs,
                               int &num_tunnel_pubs,
                               int &num_tunnel_subs) const {
  num_pubs = num_subs = num_bridge_pubs = num_bridge_subs = 0;
  num_tunnel_pubs = num_tunnel_subs = 0;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      num_pubs++;
      if (user->IsBridge()) {
        num_bridge_pubs++;
      }
      if (user->ForTunnel()) {
        num_tunnel_pubs++;
      }
    } else {
      num_subs++;
      if (user->IsBridge()) {
        num_bridge_subs++;
      }
      if (user->ForTunnel()) {
        num_tunnel_subs++;
      }
    }
  }
}

// Channel is public if there are any public publishers.
bool ServerChannel::IsLocal() const {
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      PublisherUser *pub = static_cast<PublisherUser *>(user.get());
      if (pub->IsLocal()) {
        return true;
      }
    }
  }
  return false;
}

// Channel is reliable if there are any reliable publishers.
bool ServerChannel::IsReliable() const {
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      PublisherUser *pub = static_cast<PublisherUser *>(user.get());
      if (pub->IsReliable()) {
        return true;
      }
    }
  }
  return false;
}

// Channel is fixed_size if there are any fixed size publishers.  If one is
// fixed size, they all must be.
bool ServerChannel::IsFixedSize() const {
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      PublisherUser *pub = static_cast<PublisherUser *>(user.get());
      if (pub->IsFixedSize()) {
        return true;
      }
    }
  }
  return false;
}

bool ServerChannel::IsBridgePublisher() const {
  int num_pubs = 0;
  int num_bridge_pubs = 0;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      num_pubs++;
      PublisherUser *pub = static_cast<PublisherUser *>(user.get());
      if (pub->IsBridge()) {
        num_bridge_pubs++;
      }
    }
  }
  return num_pubs == num_bridge_pubs;
}

bool ServerChannel::IsBridgeSubscriber() const {
  int num_subs = 0;
  int num_bridge_subs = 0;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsSubscriber()) {
      num_subs++;
      SubscriberUser *sub = static_cast<SubscriberUser *>(user.get());
      if (sub->IsBridge()) {
        num_bridge_subs++;
      }
    }
  }
  return num_subs == num_bridge_subs;
}

ServerChannel::CapacityInfo ServerChannel::HasSufficientCapacityInternal(
    int new_max_active_messages) const {
  if (NumSlots() == 0) {
    return CapacityInfo{true, 0, 0, 0, 0};
  }
  // Count number of publishers and subscribers.
  int num_pubs, num_subs, num_bridge_pubs, num_bridge_subs;
  int num_tunnel_pubs, num_tunnel_subs;
  CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs,
             num_tunnel_pubs, num_tunnel_subs);

  // Add in the total active message maximums.
  int max_active_messages = new_max_active_messages;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsSubscriber()) {
      SubscriberUser *sub = static_cast<SubscriberUser *>(user.get());
      max_active_messages += sub->MaxActiveMessages() - 1;
    }
  }
  int slots_needed = num_pubs + num_subs + max_active_messages + 1;
  return CapacityInfo{slots_needed <= NumSlots() - 1, num_pubs, num_subs,
                      max_active_messages, slots_needed};
}

absl::Status
ServerChannel::HasSufficientCapacity(int new_max_active_messages) const {
  auto info = HasSufficientCapacityInternal(new_max_active_messages);
  if (info.capacity_ok) {
    return absl::OkStatus();
  }
  return CapacityError(info);
}

absl::Status ServerChannel::CapacityError(const CapacityInfo &info) const {
  std::string message = absl::StrFormat(
      "there are %d slots with %d publisher%s and %d "
      "subscriber%s with %d additional active message%s; you "
      "need at least %d slots",
      NumSlots(), info.num_pubs, (info.num_pubs == 1 ? "" : "s"), info.num_subs,
      (info.num_subs == 1 ? "" : "s"), info.max_active_messages,
      (info.max_active_messages == 1 ? "" : "s"), info.slots_needed + 1);

  auto append_users = [this, &message](bool publishers) {
    message += publishers ? "; publishers=[" : "; subscribers=[";
    bool first = true;
    for (int id = 0; id < kMaxUsers; ++id) {
      auto it = users_.find(id);
      if (it == users_.end() || it->second == nullptr ||
          it->second->IsPublisher() != publishers) {
        continue;
      }
      const User &user = *it->second;
      const ClientHandler *handler = user.GetHandler();
      const std::string client_name =
          handler == nullptr ? std::string("<disconnected>")
                             : handler->ClientName();
      message += absl::StrFormat(
          "%s{pid=%llu, client=\"%s\"", first ? "" : ", ",
          static_cast<unsigned long long>(user.ProcessId()), client_name);
      if (user.IsSubscriber()) {
        const auto &subscriber = static_cast<const SubscriberUser &>(user);
        message += absl::StrFormat(
            ", max_active_messages=%d", subscriber.MaxActiveMessages());
      }
      message += "}";
      first = false;
    }
    message += "]";
  };
  append_users(/*publishers=*/true);
  append_users(/*publishers=*/false);
  return absl::InternalError(message);
}

void ServerChannel::GetChannelInfo(subspace::ChannelInfoProto *info) {
  info->set_name(Name());
  info->set_slot_size(SlotSize());
  info->set_num_slots(NumSlots());
  info->set_subscriber_queue_size(SubscriberQueueSize());
  info->set_type(Type());
  info->set_channel_id(GetChannelId());

  int num_pubs, num_subs, num_bridge_pubs, num_bridge_subs;
  int num_tunnel_pubs, num_tunnel_subs;
  CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs,
             num_tunnel_pubs, num_tunnel_subs);
  info->set_num_pubs(num_pubs);
  info->set_num_subs(num_subs);
  info->set_num_bridge_pubs(num_bridge_pubs);
  info->set_num_bridge_subs(num_bridge_subs);
  info->set_num_tunnel_pubs(num_tunnel_pubs);
  info->set_num_tunnel_subs(num_tunnel_subs);

  info->set_is_reliable(IsReliable());
  if (IsVirtual()) {
    info->set_is_virtual(true);
    VirtualChannel *vchan = static_cast<VirtualChannel *>(this);
    info->set_vchan_id(GetVirtualChannelId());
    info->set_mux(vchan->GetMux()->Name());
  }

  for (const auto &[id, user] : users_) {
    if (user == nullptr || user->GetHandler() == nullptr) {
      continue;
    }
    ChannelParticipantInfoProto *participant = info->add_participants();
    participant->set_id(id);
    participant->set_pid(user->ProcessId());
    participant->set_program_name(user->GetHandler()->ClientName());
    participant->set_is_publisher(user->IsPublisher());
    participant->set_is_subscriber(user->IsSubscriber());
    participant->set_is_reliable(user->IsReliable());
    participant->set_is_bridge(user->IsBridge());
    participant->set_for_tunnel(user->ForTunnel());
  }
}

std::vector<ResizeInfo> ServerChannel::GetResizeInfo() const {
  std::vector<ResizeInfo> info;
  // Derive the resize information from the bcb contents.  Since the server
  // isn't aware of resize operations we have to derive the information.
  if (bcb_ == nullptr || ccb_ == nullptr) {
    // No buffers, no resizes.
    return info;
  }
  uint64_t previous_buffer_size = 0;
  for (int i = 0; i < ccb_->num_buffers; i++) {
    if (previous_buffer_size == 0) {
      previous_buffer_size = bcb_->sizes[i];
      continue;
    }
    ResizeInfo resize_info;

    resize_info.new_slot_size = BufferSizeToSlotSize(bcb_->sizes[i]);
    resize_info.old_slot_size = BufferSizeToSlotSize(previous_buffer_size);
    previous_buffer_size = bcb_->sizes[i];
    info.push_back(resize_info);
  }
  scb_->counters[GetChannelId()].num_resizes = ccb_->num_buffers - 1;
  return info;
}

void ServerChannel::GetChannelStats(subspace::ChannelStatsProto *stats) {
  stats->set_channel_name(Name());
  uint64_t total_bytes, total_messages;
  uint32_t max_message_size, total_drops;
  GetStatsCounters(total_bytes, total_messages, max_message_size, total_drops);
  stats->set_total_bytes(total_bytes);
  stats->set_total_messages(total_messages);
  stats->set_slot_size(SlotSize());
  stats->set_num_slots(NumSlots());
  stats->set_max_message_size(max_message_size);
  stats->set_total_drops(total_drops);

  int num_pubs, num_subs, num_bridge_pubs, num_bridge_subs;
  int num_tunnel_pubs, num_tunnel_subs;
  CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs,
             num_tunnel_pubs, num_tunnel_subs);
  stats->set_num_pubs(num_pubs);
  stats->set_num_subs(num_subs);
  stats->set_num_bridge_pubs(num_bridge_pubs);
  stats->set_num_bridge_subs(num_bridge_subs);
}

ChannelCounters &ServerChannel::RecordUpdate(bool is_pub, bool add,
                                             bool reliable) {
  SystemControlBlock *scb = GetScb();
  int channel_id = GetChannelId();
  ChannelCounters &counters = scb->counters[channel_id];
  if (skip_cleanup_ && !add) {
    return counters;
  }
  int inc = add ? 1 : -1;
  if (is_pub) {
    SetNumUpdates(++counters.num_pub_updates);
    counters.num_pubs += inc;
    if (reliable) {
      counters.num_reliable_pubs += inc;
    }
  } else {
    SetNumUpdates(++counters.num_sub_updates);
    counters.num_subs += inc;
    if (reliable) {
      counters.num_reliable_subs += inc;
    }
  }
  return counters;
}

absl::StatusOr<std::unique_ptr<VirtualChannel>>
ChannelMultiplexer::CreateVirtualChannel([[maybe_unused]] Server &server,
                                         const std::string &name,
                                         int vchan_id) {
  if (vchan_id == -1) {
    while (vchan_ids_.contains(next_vchan_id_)) {
      next_vchan_id_++;
    }
    vchan_id = next_vchan_id_;
  } else {
    if (vchan_ids_.contains(vchan_id)) {
      return absl::InternalError(
          absl::StrFormat("Virtual channel %d already exists", vchan_id));
    }
  }
  if (vchan_id >= kMaxVchanId) {
    return absl::InternalError(absl::StrFormat(
        "Virtual channel id %d is beyond max virtual channels (%d)", vchan_id,
        kMaxVchanId));
  }
  auto v = std::make_unique<VirtualChannel>(this, vchan_id, name,
                                            SlotSize(), Type(), session_id_);
  virtual_channels_.insert(v.get());
  vchan_ids_.insert(vchan_id);
  return v;
}
void ChannelMultiplexer::RemoveVirtualChannel(VirtualChannel *vchan) {
  vchan_ids_.erase(vchan->GetVirtualChannelId());
  virtual_channels_.erase(vchan);
}

void ChannelMultiplexer::CountUsers(int &num_pubs, int &num_subs,
                                    int &num_bridge_pubs, int &num_bridge_subs,
                                    int &num_tunnel_pubs,
                                    int &num_tunnel_subs) const {
  int total_pubs = 0;
  int total_subs = 0;
  int total_bridge_pubs = 0;
  int total_bridge_subs = 0;
  int total_tunnel_pubs = 0;
  int total_tunnel_subs = 0;

  for (auto vchan : virtual_channels_) {
    int vchan_pubs, vchan_subs, vchan_bridge_pubs, vchan_bridge_subs;
    int vchan_tunnel_pubs, vchan_tunnel_subs;
    vchan->GetUserCount(vchan_pubs, vchan_subs, vchan_bridge_pubs,
                        vchan_bridge_subs, vchan_tunnel_pubs,
                        vchan_tunnel_subs);
    total_pubs += vchan_pubs;
    total_subs += vchan_subs;
    total_bridge_pubs += vchan_bridge_pubs;
    total_bridge_subs += vchan_bridge_subs;
    total_tunnel_pubs += vchan_tunnel_pubs;
    total_tunnel_subs += vchan_tunnel_subs;
  }
  // Add the counts from the multiplexer itself.
  ServerChannel::CountUsers(num_pubs, num_subs, num_bridge_pubs,
                            num_bridge_subs, num_tunnel_pubs, num_tunnel_subs);
  num_pubs += total_pubs;
  num_subs += total_subs;
  num_bridge_pubs += total_bridge_pubs;
  num_bridge_subs += total_bridge_subs;
  num_tunnel_pubs += total_tunnel_pubs;
  num_tunnel_subs += total_tunnel_subs;
}
} // namespace subspace