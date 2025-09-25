// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSERVERSERVER_H
#define _xSERVERSERVER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client/message.h"
#include "client_handler.h"
#include "coroutine.h"
#include "proto/subspace.pb.h"
#include "server/server_channel.h"
#include "toolbelt/bitset.h"
#include "toolbelt/clock.h"
#include "toolbelt/fd.h"
#include "toolbelt/logging.h"
#include <memory>
#include <vector>

namespace subspace {

// Values written to the notify_fd when the server is ready and
// is stopped.
constexpr int64_t kServerReady = 1;
constexpr int64_t kServerStopped = 2;

// The Subspace server.
// This is a single-threaded, coroutine-based server that maintains shared
// memory IPC channels and communicates with other servers to allow for
// cross-computer IPC.
class Server {
public:
  // The notify_fd is a file descriptor that the server will write to
  // when it is ready to run (after the socket has been created) and when
  // it is shutting down.  It will write 8 bytes to it so it can be
  // either a pipe or an eventfd.  If notify_fd is -1 then it won't be used.
  // The values are written in host byte order.
  Server(co::CoroutineScheduler &scheduler, const std::string &socket_name,
         const std::string &interface, int disc_port, int peer_port, bool local,
         int notify_fd = -1);
  // This constructor can be used when you have a single peer server to talk to.
  Server(co::CoroutineScheduler &scheduler, const std::string &socket_name,
         const std::string &interface, const toolbelt::InetAddress &peer,
         int disc_port, int peer_port, bool local, int notify_fd = -1);
  ~Server();
  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }
  absl::Status Run();
  void Stop();

  uint64_t GetVirtualMemoryUsage() const;

  uint64_t GetSessionId() const { return session_id_; }

  absl::StatusOr<toolbelt::FileDescriptor> CreateBridgeNotificationPipe();

  void CleanupFilesystem();
  void CleanupAfterSession();

private:
  friend class ClientHandler;
  friend class ServerChannel;
  friend class VirtualChannel;
  static constexpr size_t kDiscoveryBufferSize = 1024;

  absl::Status HandleIncomingConnection(toolbelt::UnixSocket &listen_socket,
                                        co::Coroutine *c);

  // Create a channel in both process and shared memory.  For a placeholder
  // subscriber, the channel parameters are not known, so slot_size and
  // num_slots will be zero.
  absl::StatusOr<ServerChannel *> CreateChannel(const std::string &channel_name,
                                                int slot_size, int num_slots,
                                                const std::string &mux,
                                                int vchan_id, std::string type);
  absl::StatusOr<ServerChannel *>
  CreateMultiplexer(const std::string &channel_name, int slot_size,
                    int num_slots, std::string type);
  absl::Status RemapChannel(ServerChannel *channel, int slot_size,
                            int num_slots);
  ServerChannel *FindChannel(const std::string &channel_name);
  void RemoveChannel(ServerChannel *channel);
  void RemoveAllUsersFor(ClientHandler *handler);
  void CloseHandler(ClientHandler *handler);
  void ListenerCoroutine(toolbelt::UnixSocket &listen_socket, co::Coroutine *c);
  void ChannelDirectoryCoroutine(co::Coroutine *c);
  void SendChannelDirectory();
  void StatisticsCoroutine(co::Coroutine *c);
  void DiscoveryReceiverCoroutine(co::Coroutine *c);
  void PublisherCoroutine(co::Coroutine *c);
  void SendQuery(const std::string &channel_name);
  void SendAdvertise(const std::string &channel_name, bool reliable);
  void BridgeTransmitterCoroutine(ServerChannel *channel, bool pub_reliable,
                                  bool sub_reliable,
                                  toolbelt::SocketAddress subscriber,
                                  bool notify_retirement, co::Coroutine *c);
  void BridgeReceiverCoroutine(std::string channel_name, bool sub_reliable,
                               toolbelt::InetAddress publisher,
                               co::Coroutine *c);
  void RetirementCoroutine(
      const std::string &channel_name, toolbelt::FileDescriptor &&retirement_fd,
      std::unique_ptr<toolbelt::StreamSocket> retirement_transmitter,
      co::Coroutine *c);

  void RetirementReceiverCoroutine(
      toolbelt::StreamSocket &retirement_listener,
      std::vector<std::shared_ptr<ActiveMessage>> &active_retirement_msgs,
      co::Coroutine *c);

  void SubscribeOverBridge(ServerChannel *channel, bool reliable,
                           toolbelt::InetAddress publisher);
  void IncomingQuery(const Discovery::Query &query,
                     const toolbelt::InetAddress &sender);
  void IncomingAdvertise(const Discovery::Advertise &advertise,
                         const toolbelt::InetAddress &sender);
  void IncomingSubscribe(const Discovery::Subscribe &subscribe,
                         const toolbelt::InetAddress &sender);
  void GratuitousAdvertiseCoroutine(co::Coroutine *c);
  absl::Status SendSubscribeMessage(const std::string &channel_name,
                                    bool reliable,
                                    toolbelt::InetAddress publisher,
                                    toolbelt::StreamSocket &receiver_listener,
                                    char *buffer, size_t buffer_size,
                                    co::Coroutine *c);

  static uint64_t AllocateSessionId() { return toolbelt::Now(); }
  std::string socket_name_;
  uint64_t session_id_;
  std::vector<std::unique_ptr<ClientHandler>> client_handlers_;
  bool running_ = false;
  std::string server_id_;
  std::string hostname_;
  std::string interface_;
  toolbelt::SocketAddress my_address_;
  toolbelt::InetAddress peer_address_;
  int discovery_port_;
  int discovery_peer_port_;
  bool local_;
  toolbelt::FileDescriptor notify_fd_;

  absl::flat_hash_map<std::string, std::unique_ptr<ServerChannel>> channels_;

  SystemControlBlock *scb_;
  toolbelt::FileDescriptor scb_fd_;
  toolbelt::BitSet<kMaxChannels> channel_ids_;
  co::CoroutineScheduler &co_scheduler_;

  // All coroutines are owned by this set.
  absl::flat_hash_set<std::unique_ptr<co::Coroutine>> coroutines_;

  toolbelt::TriggerFd channel_directory_trigger_fd_;
  toolbelt::InetAddress discovery_addr_;
  toolbelt::UDPSocket discovery_transmitter_;
  toolbelt::UDPSocket discovery_receiver_;
  toolbelt::Logger logger_;

  // Optional pipe to allow test to be notified when discovery sets up a
  // new connection.  The server will send an encoded protobuf Subscribed
  // message through this pipe if it is set up.
  toolbelt::Pipe bridge_notification_pipe_;
};

} // namespace subspace

#endif // _xSERVERSERVER_H
