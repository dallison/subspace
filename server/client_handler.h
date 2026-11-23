// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSERVERCLIENT_HANDLER_H
#define _xSERVERCLIENT_HANDLER_H

#include "absl/status/status.h"
#include "common/async/context.h"
#include "common/async/socket.h"
#include "common/channel.h"
#include "co/coroutine.h"
#include "proto/subspace.pb.h"
#include "proto/subspace.phaser.h"
#include "toolbelt/sockets.h"
#include <cstdint>
#include <sys/poll.h>
#include <vector>

namespace subspace {

class Server;

class ClientHandler {
public:
  ClientHandler(Server *server, async::UnixSocket socket)
      : server_(server), socket_(std::move(socket)) {}
  ~ClientHandler();

  // Run the client handler receiver in a coroutine.  Terminates
  // when the connection to the client is closed.  The Context drives the
  // cooperative socket I/O for the active backend.
  void Run(async::Context ctx);
  const std::string &ClientName() const { return client_name_; }

private:
  std::string GetTotalVM();

  absl::Status HandleMessage(const subspace::phaser::Request &req,
                             subspace::phaser::Response &resp,
                             std::vector<toolbelt::FileDescriptor> &fds);

  // These individual handler functions set any errors in the response
  // message instead of returning them to the caller.  This allows the
  // connection to remain open to the client and the client will be
  // able to display or handle the error as appropriate.
  void HandleInit(const subspace::phaser::InitRequest &req,
                  subspace::phaser::InitResponse *response,
                  std::vector<toolbelt::FileDescriptor> &fds);
  void HandleCreatePublisher(const subspace::phaser::CreatePublisherRequest &request,
                             subspace::phaser::CreatePublisherResponse *response,
                             std::vector<toolbelt::FileDescriptor> &fds);
  void HandleCreateSubscriber(const subspace::phaser::CreateSubscriberRequest &req,
                              subspace::phaser::CreateSubscriberResponse *response,
                              std::vector<toolbelt::FileDescriptor> &fds);
  void HandleGetTriggers(const subspace::phaser::GetTriggersRequest &req,
                         subspace::phaser::GetTriggersResponse *response,
                         std::vector<toolbelt::FileDescriptor> &fds);

  void HandleRemovePublisher(const subspace::phaser::RemovePublisherRequest &req,
                             subspace::phaser::RemovePublisherResponse *response,
                             std::vector<toolbelt::FileDescriptor> &fds);
  void HandleRemoveSubscriber(const subspace::phaser::RemoveSubscriberRequest &req,
                              subspace::phaser::RemoveSubscriberResponse *response,
                              std::vector<toolbelt::FileDescriptor> &fds);
  void HandleGetChannelInfo(const subspace::phaser::GetChannelInfoRequest &req,
                            subspace::phaser::GetChannelInfoResponse *response,
                            std::vector<toolbelt::FileDescriptor> &fds);
  void HandleGetChannelStats(const subspace::phaser::GetChannelStatsRequest &req,
                             subspace::phaser::GetChannelStatsResponse *response,
                             std::vector<toolbelt::FileDescriptor> &fds);
  absl::Status
  HandleRegisterClientBuffer(const subspace::phaser::RegisterClientBufferRequest &req,
                             std::vector<toolbelt::FileDescriptor> &fds);
  absl::Status HandleUnregisterClientBuffer(
      const subspace::phaser::UnregisterClientBufferRequest &req);
  void HandleGetClientBuffers(const subspace::phaser::GetClientBuffersRequest &req,
                              subspace::phaser::GetClientBuffersResponse *response,
                              std::vector<toolbelt::FileDescriptor> &fds);
  Server *server_;
  async::UnixSocket socket_;
  std::string client_name_;
  // Set from InitRequest.asil. With a static channel config, this connection's
  // publishers can use only the configured channels.
  bool asil_client_ = false;
};

} // namespace subspace

#endif // _xSERVERCLIENT_HANDLER_H