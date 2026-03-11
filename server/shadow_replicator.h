// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSERVER_SHADOW_REPLICATOR_H
#define _xSERVER_SHADOW_REPLICATOR_H

#include "absl/status/status.h"
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

  toolbelt::UnixSocket socket_;
  std::string shadow_socket_name_;
  bool connected_ = false;
  toolbelt::Logger &logger_;
};

} // namespace subspace

#endif // _xSERVER_SHADOW_REPLICATOR_H
