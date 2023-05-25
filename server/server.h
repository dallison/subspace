// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __SERVER_SERVER_H
#define __SERVER_SERVER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client_handler.h"
#include "common/bitset.h"
#include "common/fd.h"
#include "coroutine.h"
#include "proto/subspace.pb.h"
#include "server/server_channel.h"
#include <memory>
#include <vector>
#include "common/logging.h"

namespace subspace {

// The Subspace server.
// This is a single-threaded, coroutine-based server that maintains shared
// memory Subspace channels and communicates with other servers to allow from
// cross-computer Subspace.
class Server {
public:
  Server(co::CoroutineScheduler &scheduler, const std::string &socket_name,
         const std::string &interface, int disc_port, int peer_port, bool local);
  void SetLogLevel(const std::string& level) { logger_.SetLogLevel(level); }
  absl::Status Run();
  void Stop();

private:
  friend class ClientHandler;
  friend class ServerChannel;
  static constexpr size_t kDiscoveryBufferSize = 1024;

  absl::Status HandleIncomingConnection(UnixSocket &listen_socket,
                                        co::Coroutine *c);
  absl::StatusOr<ServerChannel *> CreateChannel(const std::string &channel_name,
                                                int slot_size, int num_slots,
                                                std::string type);
  absl::Status RemapChannel(ServerChannel *channel, int slot_size,
                            int num_slots);
  ServerChannel *FindChannel(const std::string &channel_name);
  void RemoveChannel(ServerChannel *channel);
  void RemoveAllUsersFor(ClientHandler *handler);
  void CloseHandler(ClientHandler *handler);
  void ListenerCoroutine(UnixSocket listen_socket, co::Coroutine *c);
  void ChannelDirectoryCoroutine(co::Coroutine *c);
  void SendChannelDirectory();
  void StatisticsCoroutine(co::Coroutine *c);
  void DiscoveryReceiverCoroutine(co::Coroutine *c);
  void PublisherCoroutine(co::Coroutine *c);
  void SendQuery(const std::string &channel_name);
  void SendAdvertise(const std::string &channel_name, bool reliable);
  void BridgeTransmitterCoroutine(ServerChannel *channel, bool pub_reliable,
                                  bool sub_reliable, InetAddress subscriber,
                                  co::Coroutine *c);
  void BridgeReceiverCoroutine(std::string channel_name, bool sub_reliable,
                               InetAddress publisher, co::Coroutine *c);
  void SubscribeOverBridge(ServerChannel *channel, bool reliable, InetAddress publisher);
  void IncomingQuery(const Discovery::Query &query,
                            const InetAddress &sender);
  void IncomingAdvertise(const Discovery::Advertise &advertise,
                        const InetAddress &sender);
  void IncomingSubscribe(const Discovery::Subscribe &subscribe,
                         const InetAddress &sender);
  void GratuitousAdvertiseCoroutine(co::Coroutine *c);
  absl::Status SendSubscribeMessage(const std::string &channel_name,
                                    bool reliable, InetAddress publisher,
                                    TCPSocket &receiver_listener, char *buffer,
                                    size_t buffer_size, co::Coroutine *c);
  std::string socket_name_;
  std::vector<std::unique_ptr<ClientHandler>> client_handlers_;
  bool running_ = false;
  std::string server_id_;
  std::string hostname_;
  std::string interface_;
  int discovery_port_;
  int discovery_peer_port_;
  bool local_;

  absl::flat_hash_map<std::string, std::unique_ptr<ServerChannel>> channels_;

  SystemControlBlock *scb_;
  FileDescriptor scb_fd_;
  BitSet<kMaxChannels> channel_ids_;
  co::CoroutineScheduler &co_scheduler_;

  // All coroutines are owned by this set.
  absl::flat_hash_set<std::unique_ptr<co::Coroutine>> coroutines_;

  TriggerFd channel_directory_trigger_fd_;
  InetAddress discovery_addr_;
  UDPSocket discovery_transmitter_;
  UDPSocket discovery_receiver_;
  Logger logger_;
};

} // namespace subspace

#endif // __SERVER_SERVER_H
