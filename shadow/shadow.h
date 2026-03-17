// Copyright 2023-2026 David Allison
// Shadow support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSHADOW_SHADOW_H
#define _xSHADOW_SHADOW_H

#include "absl/container/flat_hash_map.h"
#include "co/coroutine.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"
#include "toolbelt/logging.h"
#include "toolbelt/sockets.h"
#include <string>

namespace subspace {

struct ShadowPublisher {
  int id = 0;
  bool is_reliable = false;
  bool is_local = false;
  bool is_bridge = false;
  bool for_tunnel = false;
  bool is_fixed_size = false;
  bool notify_retirement = false;
  toolbelt::FileDescriptor poll_fd;
  toolbelt::FileDescriptor trigger_fd;
  toolbelt::FileDescriptor retirement_read_fd;
  toolbelt::FileDescriptor retirement_write_fd;
};

struct ShadowSubscriber {
  int id = 0;
  bool is_reliable = false;
  bool is_bridge = false;
  bool for_tunnel = false;
  int max_active_messages = 0;
  toolbelt::FileDescriptor trigger_fd;
  toolbelt::FileDescriptor poll_fd;
};

struct ShadowChannel {
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
  absl::flat_hash_map<int, ShadowPublisher> publishers;
  absl::flat_hash_map<int, ShadowSubscriber> subscribers;
};

// The Shadow process maintains a mirror of the server's channel database.
// It listens on a Unix domain socket and receives ShadowEvent messages
// from the server, updating its internal state accordingly.  Uses
// coroutine-based non-blocking I/O, matching the server's architecture.
class Shadow {
public:
  Shadow(co::CoroutineScheduler &scheduler, const std::string &socket_name);

  absl::Status Run();
  void Stop();

  uint64_t GetSessionId() const { return session_id_; }
  const toolbelt::FileDescriptor &GetScbFd() const { return scb_fd_; }

  const absl::flat_hash_map<std::string, ShadowChannel> &GetChannels() const {
    return channels_;
  }

  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }

  // Optional fd to write a byte to after each event is processed.
  // Used by tests to avoid sleeping.
  void SetNotifyFd(int fd) { notify_fd_ = fd; }

private:
  void NotifyEvent();
  void ListenerCoroutine();
  void ClientCoroutine(std::shared_ptr<toolbelt::UnixSocket> client_socket);
  absl::Status SendStateDump(toolbelt::UnixSocket &socket);
  absl::Status SendEvent(toolbelt::UnixSocket &socket,
                         const ShadowEvent &event,
                         const std::vector<toolbelt::FileDescriptor> &fds = {});

  absl::Status HandleEvent(const ShadowEvent &event,
                           std::vector<toolbelt::FileDescriptor> &fds);
  absl::Status HandleInit(const ShadowInit &msg,
                          std::vector<toolbelt::FileDescriptor> &fds);
  absl::Status HandleCreateChannel(const ShadowCreateChannel &msg,
                                   std::vector<toolbelt::FileDescriptor> &fds);
  absl::Status HandleRemoveChannel(const ShadowRemoveChannel &msg);
  absl::Status HandleAddPublisher(const ShadowAddPublisher &msg,
                                  std::vector<toolbelt::FileDescriptor> &fds);
  absl::Status HandleRemovePublisher(const ShadowRemovePublisher &msg);
  absl::Status HandleAddSubscriber(const ShadowAddSubscriber &msg,
                                   std::vector<toolbelt::FileDescriptor> &fds);
  absl::Status HandleRemoveSubscriber(const ShadowRemoveSubscriber &msg);

  co::CoroutineScheduler &scheduler_;
  std::string socket_name_;
  uint64_t session_id_ = 0;
  toolbelt::FileDescriptor scb_fd_;
  absl::flat_hash_map<std::string, ShadowChannel> channels_;
  toolbelt::Logger logger_;
  toolbelt::UnixSocket listen_socket_;
  int notify_fd_ = -1;
};

} // namespace subspace

#endif // _xSHADOW_SHADOW_H
