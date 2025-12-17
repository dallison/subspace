// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSERVERSERVER_CHANNEL_H
#define _xSERVERSERVER_CHANNEL_H

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/channel.h"
#include "proto/subspace.pb.h"
#include "toolbelt/bitset.h"
#include "toolbelt/fd.h"
#include "toolbelt/pipe.h"
#include "toolbelt/sockets.h"
#include "toolbelt/triggerfd.h"
#include <memory>
#include <sys/mman.h>
#include <vector>

namespace subspace {
constexpr int kMaxUsers = kMaxSlotOwners;

class ClientHandler;
class Server;

absl::StatusOr<SystemControlBlock *>
CreateSystemControlBlock(toolbelt::FileDescriptor &fd);

struct ResizeInfo {
  int old_slot_size;
  int new_slot_size;
};

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
  toolbelt::FileDescriptor &GetPollFd() { return trigger_fd_.GetPollFd(); }
  toolbelt::FileDescriptor &GetTriggerFd() {
    return trigger_fd_.GetTriggerFd();
  }
  virtual bool IsSubscriber() const { return false; }
  virtual bool IsPublisher() const { return false; }
  ClientHandler *GetHandler() const { return handler_; }
  bool IsReliable() const { return is_reliable_; }
  bool IsBridge() const { return is_bridge_; }
  void Trigger() { trigger_fd_.Trigger(); }

private:
  ClientHandler *handler_;
  int id_;
  toolbelt::TriggerFd trigger_fd_;
  bool is_reliable_;
  bool is_bridge_; // This is used to send or receive over a bridge.
};

class SubscriberUser : public User {
public:
  SubscriberUser(ClientHandler *handler, int id, bool is_reliable,
                 bool is_bridge, int max_active_messages)
      : User(handler, id, is_reliable, is_bridge),
        max_active_messages_(max_active_messages) {}
  bool IsSubscriber() const override { return true; }
  int MaxActiveMessages() const { return max_active_messages_; }

private:
  int max_active_messages_;
};

class PublisherUser : public User {
public:
  PublisherUser(ClientHandler *handler, int id, bool is_reliable, bool is_local,
                bool is_bridge, bool is_fixed_size)
      : User(handler, id, is_reliable, is_bridge), is_local_(is_local),
        is_fixed_size_(is_fixed_size) {}

  bool IsPublisher() const override { return true; }
  bool IsLocal() const { return is_local_; }
  bool IsFixedSize() const { return is_fixed_size_; }

  toolbelt::FileDescriptor &GetRetirementFdWriter() {
    return retirement_pipe_.WriteFd();
  }

  toolbelt::FileDescriptor &GetRetirementFdReader() {
    return retirement_pipe_.ReadFd();
  }

  absl::Status AllocateRetirementFd() {
#if defined(__linux__)
    auto p = toolbelt::Pipe::CreateWithFlags(O_DIRECT);
#else
    auto p = toolbelt::Pipe::Create();
#endif
    if (!p.ok()) {
      return p.status();
    }
    retirement_pipe_ = std::move(*p);
    return absl::OkStatus();
  }

private:
  bool is_local_;
  bool is_fixed_size_;
  toolbelt::Pipe
      retirement_pipe_; // For notifying publisher of slot retirement.
};

// This is endpoint transmitting the data for a channel.  It holds an internet
// address and whether the transmitter is or not.  It is absl hashable.
class ChannelTransmitter {
public:
  ChannelTransmitter(const toolbelt::SocketAddress &addr, bool reliable)
      : addr_(addr), reliable_(reliable) {}

private:
  toolbelt::SocketAddress addr_;
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
  ServerChannel(int id, const std::string &name, int num_slots,
                std::string type, bool is_virtual, int session_id)
      : Channel(name, num_slots, id, std::move(type)), is_virtual_(is_virtual),
        session_id_(session_id) {}

  virtual ~ServerChannel();

  absl::StatusOr<PublisherUser *> AddPublisher(ClientHandler *handler,
                                               bool is_reliable, bool is_local,
                                               bool is_bridge,
                                               bool is_fixed_size);
  absl::StatusOr<SubscriberUser *> AddSubscriber(ClientHandler *handler,
                                                 bool is_reliable,
                                                 bool is_bridge,
                                                 int max_active_messages);

  virtual std::string Type() const { return Channel::Type(); }
  virtual void SetType(const std::string &type) { Channel::SetType(type); }

  virtual absl::StatusOr<int> AllocateUserId(const char *type);

  virtual void AddUserId(int id) { user_ids_.Set(id); }
  virtual void RemoveUserId(int id) { user_ids_.Clear(id); }
  void AddUser(int id, std::unique_ptr<User> user) {
    users_[id] = std::move(user);
    AddUserId(id);
  }

  void SetLastKnownSlotSize(int32_t slot_size) {
    last_known_slot_size_ = slot_size;
  }

  virtual bool IsMux() const { return false; }
  virtual int GetVirtualChannelId() const { return -1; }
  virtual int GetChannelId() const { return Channel::GetChannelId(); }
  virtual bool IsPlaceholder() const { return Channel::IsPlaceholder(); }
  bool IsVirtual() const { return is_virtual_; }
  virtual void RegisterSubscriber(int sub_id, int vchan_id, bool is_new) {
    Channel::RegisterSubscriber(sub_id, vchan_id, is_new);
  }

  std::string ResolvedName() const override { return Name(); }

  // Get the file descriptors for all subscriber triggers.
  std::vector<toolbelt::FileDescriptor> GetSubscriberTriggerFds() const;

  // Get the file descriptors for all reliable publisher triggers.
  std::vector<toolbelt::FileDescriptor> GetReliablePublisherTriggerFds() const;

  std::vector<toolbelt::FileDescriptor> GetRetirementFds() const;

  // Translate a user id into a User pointer.  The pointer ownership
  // is kept by the ServerChannel.
  absl::StatusOr<User *> GetUser(int id) {
    auto it = users_.find(id);
    if (it == users_.end()) {
      return absl::InternalError("Invalid user id");
    }
    return it->second.get();
  }

  // Record an update to a channel in the SCB.  Args:
  // is_pub: this is a publisher
  // add: the publisher or subscriber is being added
  // reliable: change the reliable counters.
  virtual ChannelCounters &RecordUpdate(bool is_pub, bool add, bool reliable);

  void RemoveUser(Server *server, int user_id);
  void RemoveAllUsersFor(ClientHandler *handler);
  virtual bool IsEmpty() const { return user_ids_.IsEmpty(); }
  virtual absl::Status HasSufficientCapacity(int new_max_active_messages) const;
  virtual void CountUsers(int &num_pubs, int &num_subs, int &num_bridge_pubs,
                          int &num_bridge_subs) const;
  virtual void GetChannelInfo(subspace::ChannelInfoProto *info);
  virtual void GetChannelStats(subspace::ChannelStatsProto *stats);
  void TriggerAllSubscribers();

  std::vector<ResizeInfo> GetResizeInfo() const;

  virtual int SlotSize() const {
    if (ccb_->num_buffers == 0) {
      return last_known_slot_size_;
    }
    uint64_t size = bcb_->sizes[ccb_->num_buffers - 1];
    if (size == 0) {
      return last_known_slot_size_;
    }
    last_known_slot_size_ = BufferSizeToSlotSize(size);
    return last_known_slot_size_;
  }

  virtual int NumSlots() const { return Channel::NumSlots(); }
  virtual void CleanupSlots(int owner, bool reliable, bool is_pub,
                            int vchan_id) {
    Channel::CleanupSlots(owner, reliable, is_pub, vchan_id);
  }

  virtual void RemoveBuffer(uint64_t session_id) {
    for (int i = 0; i < ccb_->num_buffers; i++) {
      std::string filename = BufferSharedMemoryName(session_id, i);
#if defined(__APPLE__)
      auto shm_name = MacOsSharedMemoryName(filename);
      if (shm_name.ok()) {
        (void)shm_unlink(shm_name->c_str());
      }
      remove(filename.c_str());
#else
      (void)shm_unlink(filename.c_str());
#endif
    }
  }
  // This is true if all publishers are bridge publishers.
  bool IsBridgePublisher() const;
  bool IsBridgeSubscriber() const;

  // Determine if the given address is registered as a bridge
  // publisher.
  bool IsBridged(const toolbelt::SocketAddress &addr, bool reliable) const {
    return bridged_publishers_.contains(ChannelTransmitter(addr, reliable));
  }

  void AddBridgedAddress(const toolbelt::SocketAddress &addr, bool reliable) {
    bridged_publishers_.emplace(addr, reliable);
  }

  void RemoveBridgedAddress(const toolbelt::SocketAddress &addr,
                            bool reliable) {
    bridged_publishers_.erase(ChannelTransmitter(addr, reliable));
  }

  bool IsLocal() const;
  bool IsReliable() const;
  bool IsFixedSize() const;

  virtual void SetSharedMemoryFds(SharedMemoryFds fds) {
    shared_memory_fds_ = std::move(fds);
  }

  virtual const SharedMemoryFds &GetFds() { return shared_memory_fds_; }
  uint64_t GetVirtualMemoryUsage() const override { return Channel::GetVirtualMemoryUsage(); }

  // Allocate the shared memory for a channel.  The num_slots_
  // and slot_size_ member variables will either be 0 (for a subscriber
  // to channel with no publishers), or will contain the channel
  // size parameters.  Unless there's an error, it returns the
  // file descriptors for the allocated CCB and buffers.  The
  // SCB has already been allocated and will be mapped in for
  // this channel.  This is only used in the server.
  virtual absl::StatusOr<SharedMemoryFds>
  Allocate(const toolbelt::FileDescriptor &scb_fd, int slot_size, int num_slots,
           int initial_ordinal);

  struct CapacityInfo {
    bool capacity_ok;
    int num_pubs;
    int num_subs;
    int max_active_messages;
    int slots_needed;
  };

  CapacityInfo HasSufficientCapacityInternal(int new_max_active_messages) const;
  absl::Status CapacityError(const CapacityInfo &info) const;

  virtual void GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages,
                                uint32_t &max_message_size,
                                uint32_t &total_drops) {
    Channel::GetStatsCounters(total_bytes, total_messages, max_message_size,
                              total_drops);
  }

protected:
  absl::flat_hash_map<int, std::unique_ptr<User>> users_;
  toolbelt::BitSet<kMaxUsers> user_ids_;
  absl::flat_hash_set<ChannelTransmitter> bridged_publishers_;
  SharedMemoryFds shared_memory_fds_;
  bool is_virtual_ = false;
  int session_id_;
  mutable int32_t last_known_slot_size_ = 0;
};

class VirtualChannel;

class ChannelMultiplexer : public ServerChannel {
public:
  ChannelMultiplexer(int id, const std::string &name, int num_slots,
                     std::string type, int session_id)
      : ServerChannel(id, name, num_slots, type, false, session_id) {}

  absl::StatusOr<std::unique_ptr<VirtualChannel>>
  CreateVirtualChannel(Server &server, const std::string &name, int vchan_id);

  void RemoveVirtualChannel(VirtualChannel *vchan);

  bool IsMux() const override { return true; }
  bool IsEmpty() const override {
    return virtual_channels_.empty() && ServerChannel::IsEmpty();
  }

  void RemoveBuffer(uint64_t session_id) override {
    if (!virtual_channels_.empty()) {
      return;
    }
    ServerChannel::RemoveBuffer(session_id);
  }

  void CountUsers(int &num_pubs, int &num_subs, int &num_bridge_pubs,
                  int &num_bridge_subs) const override;

private:
  int next_vchan_id_ = 0;
  absl::flat_hash_set<VirtualChannel *> virtual_channels_;
  absl::flat_hash_set<int> vchan_ids_;
};

// A virtual channel is a channel that is multiplexed on another channel for its
// storage.  Publishers and subscribers create created on the virtual channel
// but the storage is on the multiplexer channel.
//
// Virtual channels share the same channel id as the multiplexer channel so any
// updates to the multiplexer SCB will be seen by all virtual channels.
class VirtualChannel : public ServerChannel {
public:
  VirtualChannel(ChannelMultiplexer *mux, int vchan_id,
                 const std::string &name, int num_slots, std::string type,
                 int session_id)
      : ServerChannel(mux->GetChannelId(), name, num_slots, type, true,
                      session_id),
        mux_(mux), vchan_id_(vchan_id) {}

  std::string Type() const override { return mux_->Type(); }
  void SetType(const std::string &type) override { mux_->SetType(type); }

  // Users (pubs and subs) are assigned ids from the multiplexer but owned
  // by this channel.  This is because they all share the same CCB and
  // each needs a unique ID for the multiplexer.  However they are not
  // users of the multiplexer.
  absl::StatusOr<int> AllocateUserId(const char *type) override {
    return mux_->AllocateUserId(type);
  }

  void RemoveUserId(int id) override {
    mux_->RemoveUserId(id);
    user_ids_.Clear(id);
  }

  void CountUsers(int &num_pubs, int &num_subs, int &num_bridge_pubs,
                  int &num_bridge_subs) const override {
    mux_->CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs);
  }
  ChannelMultiplexer *GetMux() const { return mux_; }
  int GetVirtualChannelId() const override { return vchan_id_; }

  bool IsPlaceholder() const override { return mux_->IsPlaceholder(); }

  const SharedMemoryFds &GetFds() override { return mux_->GetFds(); }

  int SlotSize() const override { return mux_->SlotSize(); }
  int NumSlots() const override { return mux_->NumSlots(); }
  int GetChannelId() const override { return mux_->GetChannelId(); }

  std::string ResolvedName() const override { return mux_->ResolvedName(); }
  void RemoveBuffer(uint64_t session_id) override {
    mux_->RemoveBuffer(session_id);
  }

  absl::Status
  HasSufficientCapacity(int new_max_active_messages) const override {
    return mux_->HasSufficientCapacity(new_max_active_messages);
  };

  ChannelCounters &RecordUpdate(bool is_pub, bool add, bool reliable) override {
    return mux_->RecordUpdate(is_pub, add, reliable);
  }

  void CleanupSlots(int owner, bool reliable, bool is_pub,
                    int vchan_id) override {
    mux_->CleanupSlots(owner, reliable, is_pub, vchan_id);
  }

  void RegisterSubscriber(int sub_id, int vchan_id, bool is_new) override {
    mux_->RegisterSubscriber(sub_id, vchan_id, is_new);
  }

  uint64_t GetVirtualMemoryUsage() const override { return 0; }

  void GetUserCount(int &num_pubs, int &num_subs, int &num_bridge_pubs,
                    int &num_bridge_subs) const {
    ServerChannel::CountUsers(num_pubs, num_subs, num_bridge_pubs,
                              num_bridge_subs);
  }

  void GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages,
                        uint32_t &max_message_size, uint32_t &total_drops) override {
    mux_->GetStatsCounters(total_bytes, total_messages, max_message_size,
                           total_drops);
  }

private:
  ChannelMultiplexer *mux_;
  int vchan_id_;
};

} // namespace subspace
#endif // _xSERVERSERVER_CHANNEL_H
