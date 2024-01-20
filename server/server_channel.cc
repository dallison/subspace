// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/server_channel.h"
#include "absl/strings/str_format.h"
#include "server/server.h"

namespace subspace {

ServerChannel::~ServerChannel() {
  // Clear the channel counters in the SCB.
  memset(&GetScb()->counters[GetChannelId()], 0, sizeof(ChannelCounters));
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
                            bool is_local, bool is_bridge) {
  absl::StatusOr<int> user_id = user_ids_.Allocate("publisher");
  if (!user_id.ok()) {
    return user_id.status();
  }
  std::unique_ptr<PublisherUser> pub = std::make_unique<PublisherUser>(
      handler, *user_id, is_reliable, is_local, is_bridge);
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
                             bool is_bridge, int max_shared_ptrs) {
  absl::StatusOr<int> user_id = user_ids_.Allocate("subscriber");
  if (!user_id.ok()) {
    return user_id.status();
  }
  std::unique_ptr<SubscriberUser> sub = std::make_unique<SubscriberUser>(
      handler, *user_id, is_reliable, is_bridge, max_shared_ptrs);
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
      CleanupSlots(user->GetId(), user->IsReliable());
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
      CleanupSlots(user->GetId(), user->IsReliable());
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

absl::Status ServerChannel::HasSufficientCapacity(int new_max_ptrs) const {
  if (NumSlots() == 0) {
    return absl::OkStatus();
  }
  // Count number of publishers and subscribers.
  int num_pubs, num_subs;
  CountUsers(num_pubs, num_subs);

  // Add in the total shared ptr maximums.
  int max_shared_ptrs = new_max_ptrs;
  for (auto &user : users_) {
    if (user == nullptr) {
      continue;
    }
    if (user->IsSubscriber()) {
      SubscriberUser *sub = static_cast<SubscriberUser *>(user.get());
      max_shared_ptrs += sub->MaxSharedPtrs();
    }
  }
  int slots_needed = num_pubs + num_subs + max_shared_ptrs + 1;
  if (slots_needed <= (NumSlots() - 1)) {
    return absl::OkStatus();
  }
  max_shared_ptrs -= new_max_ptrs;    // Adjust for error message.

  return absl::InternalError(
      absl::StrFormat("there are %d slots with %d publisher%s and %d "
                      "subscriber%s with %d shared pointer%s; you need at least %d slots",
                      NumSlots(), num_pubs, (num_pubs == 1 ? "" : "s"),
                      num_subs, (num_subs == 1 ? "" : "s"), max_shared_ptrs,
                      (max_shared_ptrs == 1 ? "" : "s"),
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
  int64_t total_bytes, total_messages;
  GetStatsCounters(total_bytes, total_messages);
  stats->set_total_bytes(total_bytes);
  stats->set_total_messages(total_messages);
  stats->set_slot_size(SlotSize());
  stats->set_num_slots(NumSlots());

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