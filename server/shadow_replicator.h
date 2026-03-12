// Copyright 2023-2026 David Allison
// Shadow support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSERVER_SHADOW_REPLICATOR_H
#define _xSERVER_SHADOW_REPLICATOR_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"
#include "toolbelt/logging.h"
#include "toolbelt/sockets.h"
#include <string>
#include <vector>

namespace subspace {

class ServerChannel;
class PublisherUser;
class SubscriberUser;

struct RecoveredPublisher {
  int id = 0;
  bool is_reliable = false;
  bool is_local = false;
  bool is_bridge = false;
  bool is_fixed_size = false;
  bool notify_retirement = false;
  toolbelt::FileDescriptor poll_fd;
  toolbelt::FileDescriptor trigger_fd;
  toolbelt::FileDescriptor retirement_read_fd;
  toolbelt::FileDescriptor retirement_write_fd;
};

struct RecoveredSubscriber {
  int id = 0;
  bool is_reliable = false;
  bool is_bridge = false;
  int max_active_messages = 0;
  toolbelt::FileDescriptor trigger_fd;
  toolbelt::FileDescriptor poll_fd;
};

struct RecoveredChannel {
  std::string name;
  int channel_id = 0;
  int slot_size = 0;
  int num_slots = 0;
  std::string type;
  bool is_local = false;
  bool is_reliable = false;
  bool is_fixed_size = false;
  int checksum_size = 0;
  int metadata_size = 0;
  std::string mux;
  int vchan_id = -1;
  toolbelt::FileDescriptor ccb_fd;
  toolbelt::FileDescriptor bcb_fd;
  std::vector<RecoveredPublisher> publishers;
  std::vector<RecoveredSubscriber> subscribers;
};

struct RecoveredState {
  uint64_t session_id = 0;
  toolbelt::FileDescriptor scb_fd;
  std::vector<RecoveredChannel> channels;
};

// Sends server state change events to an optional shadow process via a Unix
// domain socket.  The shadow process can use the replicated state to recover
// after a server crash.  If the shadow is not configured or not reachable,
// all Send* methods are silent no-ops.
class ShadowReplicator {
public:
  explicit ShadowReplicator(const std::string &shadow_socket_name,
                            toolbelt::Logger &logger);

  absl::Status Connect();
  void Close();
  bool Connected() const { return connected_; }

  // Read the state dump that the shadow sends on connection.
  absl::StatusOr<RecoveredState> ReceiveStateDump();

  void SendInit(uint64_t session_id,
                const toolbelt::FileDescriptor &scb_fd);
  void SendCreateChannel(ServerChannel *channel);
  void SendRemoveChannel(const std::string &name, int channel_id);
  void SendAddPublisher(const std::string &channel_name,
                        const PublisherUser *pub);
  void SendRemovePublisher(const std::string &channel_name, int pub_id);
  void SendAddSubscriber(const std::string &channel_name,
                         const SubscriberUser *sub);
  void SendRemoveSubscriber(const std::string &channel_name, int sub_id);

private:
  void SendEvent(const ShadowEvent &event,
                 const std::vector<toolbelt::FileDescriptor> &fds = {});

  absl::StatusOr<ShadowEvent>
  ReceiveEvent(std::vector<toolbelt::FileDescriptor> &fds);

  toolbelt::UnixSocket socket_;
  std::string shadow_socket_name_;
  bool connected_ = false;
  toolbelt::Logger &logger_;
};

} // namespace subspace

#endif // _xSERVER_SHADOW_REPLICATOR_H
