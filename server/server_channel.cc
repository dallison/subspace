// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/server_channel.h"
#include "absl/strings/str_format.h"
#include "server/server.h"
#include <sys/mman.h>
#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_POSIX
#include <sys/posix_shm.h>
#endif

namespace subspace {

ServerChannel::~ServerChannel() {
  if (is_virtual_) {
    return;
  }
  // Clear the channel counters in the SCB.
  memset(&GetScb()->counters[GetChannelId()], 0, sizeof(ChannelCounters));
}

static absl::StatusOr<void *> CreateSharedMemory(int id, const char *suffix,
                                                 int64_t size, bool map,
                                                 toolbelt::FileDescriptor &fd,
                                                 int session_id = 0) {
  char shm_file[NAME_MAX]; // Unique file in file system.
  char *shm_name;          // Name passed to shm_* (starts with /)
  int tmpfd;
#if defined(__linux__)
  // On Linux we have actual files in /dev/shm so we can create a unique file.
  snprintf(shm_file, sizeof(shm_file), "/dev/shm/%d.%s.XXXXXX", id, suffix);
  tmpfd = mkstemp(shm_file);
  shm_name = shm_file + 8; // After /dev/shm
#else
  // On other systems (BSD, MacOS, etc), we need to use a file in /tmp.
  // This is just used to ensure uniqueness.
  snprintf(shm_file, sizeof(shm_file), "/tmp/%d.%d.%s.XXXXXX", session_id, id,
           suffix);
  tmpfd = mkstemp(shm_file);
  shm_name = shm_file + 4; // After /tmp
#endif
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
}

absl::StatusOr<SystemControlBlock *>
CreateSystemControlBlock(toolbelt::FileDescriptor &fd) {
  absl::StatusOr<void *> s = CreateSharedMemory(
      0, "scb", sizeof(SystemControlBlock), /*map=*/true, fd, 0);
  if (!s.ok()) {
    return s.status();
  }
  SystemControlBlock *scb = reinterpret_cast<SystemControlBlock *>(*s);
  memset(&scb->counters, 0, sizeof(scb->counters));
  return scb;
}

absl::StatusOr<SharedMemoryFds>
ServerChannel::Allocate(const toolbelt::FileDescriptor &scb_fd, int slot_size,
                        int num_slots, int initial_ordinal) {
  SubscriberCounter num_subs;

  if (scb_ != nullptr) {
    num_subs = ccb_->num_subs;
  }
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

  // Map SCB into process memory.
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s", strerror(errno)));
  }

  SharedMemoryFds fds;

  // Create CCB in shared memory and map into process memory.
  absl::StatusOr<void *> p =
      CreateSharedMemory(channel_id_, "ccb", CcbSize(num_slots_), /*map=*/true,
                         fds.ccb, session_id_);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return p.status();
  }
  ccb_ = reinterpret_cast<ChannelControlBlock *>(*p);
  ccb_->num_subs = num_subs;

  // Create buffer control block.
  p = CreateSharedMemory(channel_id_, "bcb", sizeof(BufferControlBlock),
                         /*map=*/true, fds.bcb, session_id_);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, CcbSize(num_slots_), "CCB");
    return p.status();
  }
  bcb_ = reinterpret_cast<BufferControlBlock *>(*p);
  ccb_->num_buffers = 0;

  // Build CCB data.
  // Copy possibly truncated channel name into CCB for ease
  // of debugging (you can see it in all processes).
  strncpy(ccb_->channel_name, name_.c_str(), kMaxChannelName - 1);
  ccb_->num_slots = num_slots_;

  // Initialize all ordinals.
  ccb_->ordinals.Init(initial_ordinal);

  new (&ccb_->subscribers) AtomicBitSet<kMaxSlotOwners>();

  // Initialize all slots
  for (int32_t i = 0; i < num_slots_; i++) {
    MessageSlot *slot = &ccb_->slots[i];
    slot->id = i;
    slot->refs = 0;
    slot->vchan_id = -1;
    slot->buffer_index = -1; // No buffer in the free list.
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
                            bool is_local, bool is_bridge, bool is_fixed_size) {
  absl::StatusOr<int> user_id = AllocateUserId("publisher");
  if (!user_id.ok()) {
    return user_id.status();
  }
  std::unique_ptr<PublisherUser> pub = std::make_unique<PublisherUser>(
      handler, *user_id, is_reliable, is_local, is_bridge, is_fixed_size);
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
                             bool is_bridge, int max_active_messages) {
  absl::StatusOr<int> user_id = AllocateUserId("subscriber");
  if (!user_id.ok()) {
    return user_id.status();
  }
  std::unique_ptr<SubscriberUser> sub = std::make_unique<SubscriberUser>(
      handler, *user_id, is_reliable, is_bridge, max_active_messages);
  absl::Status status = sub->Init();
  if (!status.ok()) {
    return status;
  }
  SubscriberUser *result = sub.get();
  AddUser(*user_id, std::move(sub));
  return result;
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
  } else {
    server->OnRemoveSubscriber(Name(), user->GetId());
  }
  CleanupSlots(user->GetId(), user->IsReliable(), user->IsPublisher(),
               GetVirtualChannelId());
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
                               int &num_bridge_pubs,
                               int &num_bridge_subs) const {
  num_pubs = num_subs = num_bridge_pubs = num_bridge_subs = 0;
  for (auto &[id, user] : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      num_pubs++;
      if (user->IsBridge()) {
        num_bridge_pubs++;
      }
    } else {
      num_subs++;
      if (user->IsBridge()) {
        num_bridge_subs++;
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
  CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs);

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
  return absl::InternalError(absl::StrFormat(
      "there are %d slots with %d publisher%s and %d "
      "subscriber%s with %d additional active message%s; you "
      "need at least %d slots",
      NumSlots(), info.num_pubs, (info.num_pubs == 1 ? "" : "s"), info.num_subs,
      (info.num_subs == 1 ? "" : "s"), info.max_active_messages,
      (info.max_active_messages == 1 ? "" : "s"), info.slots_needed + 1));
}

void ServerChannel::GetChannelInfo(subspace::ChannelInfoProto *info) {
  info->set_name(Name());
  info->set_slot_size(SlotSize());
  info->set_num_slots(NumSlots());
  info->set_type(Type());

  int num_pubs, num_subs, num_bridge_pubs, num_bridge_subs;
  CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs);
  info->set_num_pubs(num_pubs);
  info->set_num_subs(num_subs);
  info->set_num_bridge_pubs(num_bridge_pubs);
  info->set_num_bridge_subs(num_bridge_subs);

  info->set_is_reliable(IsReliable());
  if (IsVirtual()) {
    info->set_is_virtual(true);
    VirtualChannel *vchan = static_cast<VirtualChannel *>(this);
    info->set_vchan_id(GetVirtualChannelId());
    info->set_mux(vchan->GetMux()->Name());
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
  CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs);
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
ChannelMultiplexer::CreateVirtualChannel(Server &server,
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
                                    int &num_bridge_pubs,
                                    int &num_bridge_subs) const {
  int total_pubs = 0;
  int total_subs = 0;
  int total_bridge_pubs = 0;
  int total_bridge_subs = 0;

  for (auto vchan : virtual_channels_) {
    int vchan_pubs, vchan_subs, vchan_bridge_pubs, vchan_bridge_subs;
    vchan->GetUserCount(vchan_pubs, vchan_subs, vchan_bridge_pubs,
                        vchan_bridge_subs);
    total_pubs += vchan_pubs;
    total_subs += vchan_subs;
    total_bridge_pubs += vchan_bridge_pubs;
    total_bridge_subs += vchan_bridge_subs;
  }
  // Add the counts from the multiplexer itself.
  ServerChannel::CountUsers(num_pubs, num_subs, num_bridge_pubs,
                            num_bridge_subs);
  num_pubs += total_pubs;
  num_subs += total_subs;
  num_bridge_pubs += total_bridge_pubs;
  num_bridge_subs += total_bridge_subs;
}
} // namespace subspace