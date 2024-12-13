// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/server_channel.h"
#include "absl/strings/str_format.h"
#include "server/server.h"
#include <sys/mman.h>
#if defined(__APPLE__)
#include <sys/posix_shm.h>
#endif

namespace subspace {

ServerChannel::~ServerChannel() {
  // Clear the channel counters in the SCB.
  memset(&GetScb()->counters[GetChannelId()], 0, sizeof(ChannelCounters));
}


static absl::StatusOr<void *> CreateSharedMemory(int id, const char *suffix,
                                                 int64_t size, bool map,
                                                 toolbelt::FileDescriptor &fd) {
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
  snprintf(shm_file, sizeof(shm_file), "/tmp/%d.%s.XXXXXX", id, suffix);
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
      0, "scb", sizeof(SystemControlBlock), /*map=*/true, fd);
  if (!s.ok()) {
    return s.status();
  }
  SystemControlBlock *scb = reinterpret_cast<SystemControlBlock *>(*s);
  memset(&scb->counters, 0, sizeof(scb->counters));
  return scb;
}

absl::StatusOr<SharedMemoryFds>
ServerChannel::Allocate(const toolbelt::FileDescriptor &scb_fd, int slot_size,
                  int num_slots) {
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

  // We are allocating a channel, so we only have one buffer.
  buffers_.clear();

  // Map SCB into process memory.
  scb_ = reinterpret_cast<SystemControlBlock *>(MapMemory(
      scb_fd.Fd(), sizeof(SystemControlBlock), PROT_READ | PROT_WRITE, "SCB"));
  if (scb_ == MAP_FAILED) {
    return absl::InternalError(absl::StrFormat(
        "Failed to map SystemControlBlock: %s", strerror(errno)));
  }

  SharedMemoryFds fds;

  // One buffer.  The fd will be set when the buffers are allocated in
  // shared memmory.
  fds.buffers.emplace_back(slot_size);

  // Create CCB in shared memory and map into process memory.
  absl::StatusOr<void *> p =
      CreateSharedMemory(channel_id_, "ccb", CcbSize(num_slots_), /*map=*/true, fds.ccb);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    return p.status();
  }
  ccb_ = reinterpret_cast<ChannelControlBlock *>(*p);

  // Create a single buffer but don't map it in.  There is no need to
  // map in the buffers in the server since they will never be used.
  int64_t buffers_size =
      sizeof(BufferHeader) +
      num_slots_ * (Aligned<32>(slot_size) + sizeof(MessagePrefix));
  if (buffers_size == 0) {
    buffers_size = 256;
  }
  p = CreateSharedMemory(channel_id_, "buffers0", buffers_size,
                         /*map=*/false, fds.buffers[0].fd);
  if (!p.ok()) {
    UnmapMemory(scb_, sizeof(SystemControlBlock), "SCB");
    UnmapMemory(ccb_, CcbSize(num_slots_), "CCB");
    return p.status();
  }
  buffers_.emplace_back(slot_size, reinterpret_cast<char *>(*p));
  ccb_->num_buffers = 1;

  // Build CCB data.
  // Copy possibly truncated channel name into CCB for ease
  // of debugging (you can see it in all processes).
  strncpy(ccb_->channel_name, name_.c_str(), kMaxChannelName - 1);
  ccb_->num_slots = num_slots_;
  ccb_->next_ordinal = 1;
  new (&ccb_->subscribers) AtomicBitSet<kMaxSlotOwners>();

  // Initialize all slots and insert into the free list.
  for (int32_t i = 0; i < num_slots_; i++) {
    MessageSlot *slot = &ccb_->slots[i];
    slot->id = i;
    slot->refs = 0;
    slot->buffer_index = -1; // No buffer in the free list.
    new (&slot->sub_owners) AtomicBitSet<kMaxSlotOwners>();
  }

  // Initialize the available slots for each subscriber.
  for (int i = 0; i < kMaxSlotOwners; i++) {
    new (GetAvailableSlotsAddress(i))
        InPlaceAtomicBitset(num_slots_);
  }

  if (debug_) {
    printf("Channel allocated: scb: %p, ccb: %p, buffers: %p\n", scb_, ccb_,
           buffers_[0].buffer);
    Dump();
  }
  return fds;
}

// Called on server to extend the allocated buffers.
absl::StatusOr<toolbelt::FileDescriptor>
ServerChannel::ExtendBuffers(int32_t new_slot_size) {
  int64_t buffers_size =
      sizeof(BufferHeader) +
      num_slots_ * (Aligned<32>(new_slot_size) + sizeof(MessagePrefix));

  char buffer_name[32];
  snprintf(buffer_name, sizeof(buffer_name), "buffers%d\n",
           ccb_->num_buffers - 1);
  toolbelt::FileDescriptor fd;
  // Create the shared memory for the buffer but don't map it in.  This is
  // in the server and it is not used here.  The result of a successful
  // creation will be nullptr.
  absl::StatusOr<void *> p = CreateSharedMemory(
      channel_id_, buffer_name, buffers_size, /*map=*/false, fd);

  if (!p.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to map memory for extension: %s",
                        p.status().ToString().c_str()));
  }
  buffers_.emplace_back(new_slot_size, reinterpret_cast<char *>(*p));
  ccb_->num_buffers++;
  return fd;
}

std::vector<toolbelt::FileDescriptor>
ServerChannel::GetSubscriberTriggerFds() const {
  std::vector<toolbelt::FileDescriptor> r;
  for (auto &user : users_) {
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
  for (auto &user : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher() && user->IsReliable()) {
      r.push_back(user->GetTriggerFd());
    }
  }
  return r;
}

absl::StatusOr<PublisherUser *>
ServerChannel::AddPublisher(ClientHandler *handler, bool is_reliable,
                            bool is_local, bool is_bridge, bool is_fixed_size) {
  absl::StatusOr<int> user_id = user_ids_.Allocate("publisher");
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
  if (*user_id >= users_.size()) {
    users_.resize(*user_id + 1);
  }
  users_[*user_id] = std::move(pub);
  return result;
}

absl::StatusOr<SubscriberUser *>
ServerChannel::AddSubscriber(ClientHandler *handler, bool is_reliable,
                             bool is_bridge, int max_active_messages) {
  absl::StatusOr<int> user_id = user_ids_.Allocate("subscriber");
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
  if (*user_id >= users_.size()) {
    users_.resize(*user_id + 1);
  }
  users_[*user_id] = std::move(sub);

  return result;
}

void ServerChannel::TriggerAllSubscribers() {
  for (auto &user : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsSubscriber()) {
      user->Trigger();
    }
  }
}

void ServerChannel::RemoveUser(int user_id) {
  for (auto &user : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->GetId() == user_id) {
      CleanupSlots(user->GetId(), user->IsReliable(), user->IsPublisher());
      user_ids_.Clear(user->GetId());
      RecordUpdate(user->IsPublisher(), /*add=*/false, user->IsReliable());
      if (user->IsPublisher()) {
        TriggerAllSubscribers();
      }
      user.reset();
      return;
    }
  }
}

void ServerChannel::RemoveAllUsersFor(ClientHandler *handler) {
  for (auto &user : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->GetHandler() == handler) {
      CleanupSlots(user->GetId(), user->IsReliable(), user->IsPublisher());
      user_ids_.Clear(user->GetId());
      RecordUpdate(user->IsPublisher(), /*add=*/false, user->IsReliable());
      if (user->IsPublisher()) {
        TriggerAllSubscribers();
      }
      user.reset();
    }
  }
}

void ServerChannel::CountUsers(int &num_pubs, int &num_subs) const {
  num_pubs = num_subs = 0;
  for (auto &user : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsPublisher()) {
      num_pubs++;
    } else {
      num_subs++;
    }
  }
}

// Channel is public if there are any public publishers.
bool ServerChannel::IsLocal() const {
  for (auto &user : users_) {
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
  for (auto &user : users_) {
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
  for (auto &user : users_) {
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
  for (auto &user : users_) {
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
  for (auto &user : users_) {
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

absl::Status ServerChannel::HasSufficientCapacity(int new_max_active_messages) const {
  if (NumSlots() == 0) {
    return absl::OkStatus();
  }
  // Count number of publishers and subscribers.
  int num_pubs, num_subs;
  CountUsers(num_pubs, num_subs);

  // Add in the total active message maximums.
  int max_active_messages = new_max_active_messages;
  for (auto &user : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsSubscriber()) {
      SubscriberUser *sub = static_cast<SubscriberUser *>(user.get());
      max_active_messages += sub->MaxActiveMessages() - 1;
    }
  }
  int slots_needed = num_pubs + num_subs + max_active_messages + 1;
  if (slots_needed <= (NumSlots() - 1)) {
    return absl::OkStatus();
  }
  max_active_messages -= new_max_active_messages;    // Adjust for error message.

  return absl::InternalError(
      absl::StrFormat("there are %d slots with %d publisher%s and %d "
                      "subscriber%s with %d additional active message%s; you need at least %d slots",
                      NumSlots(), num_pubs, (num_pubs == 1 ? "" : "s"),
                      num_subs, (num_subs == 1 ? "" : "s"), max_active_messages,
                      (max_active_messages == 1 ? "" : "s"),
                      slots_needed+1));
}

void ServerChannel::GetChannelInfo(subspace::ChannelInfo *info) {
  info->set_name(Name());
  info->set_slot_size(SlotSize());
  info->set_num_slots(NumSlots());
  info->set_type(Type());
}

void ServerChannel::GetChannelStats(subspace::ChannelStats *stats) {
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
  
  int num_pubs, num_subs;
  CountUsers(num_pubs, num_subs);
  stats->set_num_pubs(num_pubs);
  stats->set_num_subs(num_subs);
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

void ServerChannel::AddBuffer(int slot_size, toolbelt::FileDescriptor fd) {
  shared_memory_fds_.buffers.push_back({slot_size, std::move(fd)});
}

} // namespace subspace