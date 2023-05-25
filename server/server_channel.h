// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __SERVER_SERVER_CHANNEL_H
#define __SERVER_SERVER_CHANNEL_H

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/bitset.h"
#include "common/channel.h"
#include "common/fd.h"
#include "common/sockets.h"
#include "common/triggerfd.h"
#include "proto/subspace.pb.h"
#include <memory>
#include <vector>

namespace subspace {
constexpr int kMaxUsers = kMaxSlotOwners;

class ClientHandler;
class Server;

// A user is a publisher or subscriber on a channel.  Each user has a
// unique (per channel) user id.  A user might have a trigger fd
// associated with it (subscribers always have one, but only
// reliable publishers have one).
class User {
public:
  User(ClientHandler *handler, int id, bool is_reliable, bool is_bridge)
      : handler_(handler), id_(id), is_reliable_(is_reliable),
        is_bridge_(is_bridge) {}
  virtual ~User() = default;

  absl::Status Init() { return trigger_fd_.Open(); }

  int GetId() const { return id_; }
  FileDescriptor &GetPollFd() { return trigger_fd_.GetPollFd(); }
  FileDescriptor &GetTriggerFd() { return trigger_fd_.GetTriggerFd(); }
  virtual bool IsSubscriber() const { return false; }
  virtual bool IsPublisher() const { return false; }
  ClientHandler *GetHandler() const { return handler_; }
  bool IsReliable() const { return is_reliable_; }
  bool IsBridge() const { return is_bridge_; }
  void Trigger() { trigger_fd_.Trigger();  }

private:
  ClientHandler *handler_;
  int id_;
  TriggerFd trigger_fd_;
  bool is_reliable_;
  bool is_bridge_; // This is used to send or receive over a bridge.
};

class SubscriberUser : public User {
public:
  SubscriberUser(ClientHandler *handler, int id, bool is_reliable,
                 bool is_bridge)
      : User(handler, id, is_reliable, is_bridge) {}
  bool IsSubscriber() const override { return true; }
};

class PublisherUser : public User {
public:
  PublisherUser(ClientHandler *handler, int id, bool is_reliable,
                bool is_public, bool is_bridge)
      : User(handler, id, is_reliable, is_bridge), is_public_(is_public) {}

  bool IsPublisher() const override { return true; }
  bool IsPublic() const { return is_public_; }

private:
  bool is_public_;
};

// This is endpoint transmitting the data for a channel.  It holds an internet
// address and whether the transmitter is or not.  It is absl hashable.
class ChannelTransmitter {
public:
  ChannelTransmitter(const InetAddress &addr, bool reliable)
      : addr_(addr), reliable_(reliable) {}

private:
  InetAddress addr_;
  bool reliable_;

  // Provide support for Abseil hashing.
  friend bool operator==(const ChannelTransmitter &a,
                         const ChannelTransmitter &b);
  template <typename H>
  friend H AbslHashValue(H h, const ChannelTransmitter &a);
};

inline bool operator==(const ChannelTransmitter &a,
                       const ChannelTransmitter &b) {
  return a.addr_ == b.addr_ && a.reliable_ == b.reliable_;
}

template <typename H> inline H AbslHashValue(H h, const ChannelTransmitter &a) {
  H addr_hash = AbslHashValue(std::move(h), a.addr_);
  return H::combine(std::move(addr_hash), a.reliable_);
}

// This is a channel maintained by the server.  The server creates the shared
// memory for the channel and distributes the file descriptor associated with
// it.
class ServerChannel : public Channel {
public:
  ServerChannel(int id, const std::string &name, int slot_size, int num_slots, std::string type)
      : Channel(name, slot_size, num_slots, id, std::move(type)) {}

  ~ServerChannel();

  absl::StatusOr<PublisherUser *> AddPublisher(ClientHandler *handler,
                                               bool is_reliable, bool is_public,
                                               bool is_bridge);
  absl::StatusOr<SubscriberUser *>
  AddSubscriber(ClientHandler *handler, bool is_reliable, bool is_bridge);

  // Get the file descriptors for all subscriber triggers.
  std::vector<FileDescriptor> GetSubscriberTriggerFds() const;

  // Get the file descriptors for all reliable publisher triggers.
  std::vector<FileDescriptor> GetReliablePublisherTriggerFds() const;

  // Translate a user id into a User pointer.  The pointer ownership
  // is kept by the ServerChannel.
  absl::StatusOr<User *> GetUser(int id) {
    if (id < 0 || id >= users_.size()) {
      return absl::InternalError("Invalid user id");
    }
    return users_[id].get();
  }

  // Record an update to a channel in the SCB.  Args:
  // is_pub: this is a publisher
  // add: the publisher or subscriber is being added
  // reliable: change the reliable counters.
  ChannelCounters &RecordUpdate(bool is_pub, bool add, bool reliable);

  void RemoveUser(int user_id);
  void RemoveAllUsersFor(ClientHandler *handler);
  bool IsEmpty() const { return user_ids_.IsEmpty(); }
  absl::Status HasSufficientCapacity() const;
  void CountUsers(int &num_pubs, int &num_subs) const;
  void GetChannelInfo(subspace::ChannelInfo *info);
  void GetChannelStats(subspace::ChannelStats *stats);
  void TriggerAllSubscribers();

  // This is true if all publishers are bridge publishers.
  bool IsBridgePublisher() const;
  bool IsBridgeSubscriber() const;

  // Determine if the given address is registered as a bridge
  // publisher.
  bool IsBridged(const InetAddress &addr, bool reliable) const {
    return bridged_publishers_.contains(ChannelTransmitter(addr, reliable));
  }

  void AddBridgedAddress(const InetAddress &addr, bool reliable) {
    bridged_publishers_.emplace(addr, reliable);
  }

  void RemoveBridgedAddress(const InetAddress &addr, bool reliable) {
    bridged_publishers_.erase(ChannelTransmitter(addr, reliable));
  }

  bool IsPublic() const;
  bool IsReliable() const;

  void SetSharedMemoryFds(SharedMemoryFds fds) {
    shared_memory_fds_ = std::move(fds);
  }

  const SharedMemoryFds &GetFds() { return shared_memory_fds_; }

private:
  std::vector<std::unique_ptr<User>> users_;
  BitSet<kMaxUsers> user_ids_;
  absl::flat_hash_set<ChannelTransmitter> bridged_publishers_;
  SharedMemoryFds shared_memory_fds_;
};
} // namespace subspace
#endif // __SERVER_SERVER_CHANNEL_H
