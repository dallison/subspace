// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/client_handler.h"
#include "absl/strings/str_format.h"
#include "server/server.h"

namespace subspace {

ClientHandler::~ClientHandler() { server_->RemoveAllUsersFor(this); }

void ClientHandler::Run(co::Coroutine *c) {
  // The data is placed 4 bytes into the buffer.  The first 4
  // bytes of the buffer are used by SendMessage and ReceiveMessage
  // for the length of the data.
  char *sendbuf = buffer_ + sizeof(int32_t);
  constexpr size_t kSendBufLen = sizeof(buffer_) - sizeof(int32_t);
  for (;;) {
    absl::StatusOr<ssize_t> n_recv =
        socket_.ReceiveMessage(buffer_, sizeof(buffer_), c);
    if (!n_recv.ok()) {
      return;
    }
    subspace::Request request;
    if (request.ParseFromArray(buffer_, *n_recv)) {
      std::vector<toolbelt::FileDescriptor> fds;
      subspace::Response response;
      if (absl::Status s = HandleMessage(request, response, fds); !s.ok()) {
        server_->logger_.Log(toolbelt::LogLevel::kError, "%s\n",
                             s.ToString().c_str());
        return;
      }

      if (!response.SerializeToArray(sendbuf, kSendBufLen)) {
        server_->logger_.Log(toolbelt::LogLevel::kError,
                             "Failed to serialize response\n");
        return;
      }
      size_t msglen = response.ByteSizeLong();
      absl::StatusOr<ssize_t> n_sent = socket_.SendMessage(sendbuf, msglen, c);
      if (!n_sent.ok()) {
        return;
      }
      if (absl::Status status = socket_.SendFds(fds, c); !status.ok()) {
        server_->logger_.Log(toolbelt::LogLevel::kError, "%s\n",
                             status.ToString().c_str());
        return;
      }
    } else {
      server_->logger_.Log(toolbelt::LogLevel::kError,
                           "Failed to parse message\n");
      return;
    }
  }
}

absl::Status
ClientHandler::HandleMessage(const subspace::Request &req,
                             subspace::Response &resp,
                             std::vector<toolbelt::FileDescriptor> &fds) {
  switch (req.request_case()) {
  case subspace::Request::kInit:
    HandleInit(req.init(), resp.mutable_init(), fds);
    break;
  case subspace::Request::kCreatePublisher:
    HandleCreatePublisher(req.create_publisher(),
                          resp.mutable_create_publisher(), fds);
    break;

  case subspace::Request::kCreateSubscriber:
    HandleCreateSubscriber(req.create_subscriber(),
                           resp.mutable_create_subscriber(), fds);
    break;

  case subspace::Request::kGetTriggers:
    HandleGetTriggers(req.get_triggers(), resp.mutable_get_triggers(), fds);
    break;

  case subspace::Request::kRemovePublisher:
    HandleRemovePublisher(req.remove_publisher(),
                          resp.mutable_remove_publisher(), fds);
    break;

  case subspace::Request::kRemoveSubscriber:
    HandleRemoveSubscriber(req.remove_subscriber(),
                           resp.mutable_remove_subscriber(), fds);
    break;

  case subspace::Request::REQUEST_NOT_SET:
    return absl::InternalError("Protocol error: unknown request");
  }
  return absl::OkStatus();
}

void ClientHandler::HandleInit(const subspace::InitRequest &req,
                               subspace::InitResponse *response,
                               std::vector<toolbelt::FileDescriptor> &fds) {
  response->set_scb_fd_index(0);
  fds.push_back(server_->scb_fd_);
  client_name_ = req.client_name();
  response->set_session_id(server_->GetSessionId());
}

void ClientHandler::HandleCreatePublisher(
    const subspace::CreatePublisherRequest &req,
    subspace::CreatePublisherResponse *response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    absl::StatusOr<ServerChannel *> ch = server_->CreateChannel(
        req.channel_name(), req.slot_size(), req.num_slots(), req.mux(),
        req.vchan_id(), req.type());
    if (!ch.ok()) {
      response->set_error(ch.status().ToString());
      return;
    }
    channel = *ch;
  } else if (channel->IsPlaceholder()) {
    // Channel exists, but it's just a placeholder.  Remap the memory now
    // that we know the slots.
    absl::Status status =
        server_->RemapChannel(channel, req.slot_size(), req.num_slots());
    if (!status.ok()) {
      response->set_error(status.ToString());
      return;
    }
  }
  if (req.mux().empty() && channel->IsMux()) {
    response->set_error(
        absl::StrFormat("Cannot create publisher to multiplexer channel %s",
                        req.channel_name()));
    return;
  }
  if (!req.mux().empty()) {
    ServerChannel *mux = server_->FindChannel(req.mux());
    if (mux == nullptr || !mux->IsMux()) {
      response->set_error(absl::StrFormat(
          "Channel %s is not a multiplexer, but a multiplexer was "
          "specified for the publisher",
          req.channel_name()));
      return;
    }
  }
  // Check the virtuality settings.  We can't mix virtual and non-virtual
  // channels with the same name or on different multiplexer channels.
  if (req.mux().empty() && channel->IsVirtual()) {
    response->set_error(
        absl::StrFormat("Channel %s is virtual, but no multiplexer was "
                        "specified for the publisher",
                        req.channel_name()));
    return;
  }
  if (!req.mux().empty() && !channel->IsVirtual()) {
    response->set_error(
        absl::StrFormat("Channel %s is not virtual, but a multiplexer was "
                        "specified for the publisher",
                        req.channel_name()));
    return;
  }
  if (channel->IsVirtual()) {
    VirtualChannel *vchan = static_cast<VirtualChannel *>(channel);
    if (vchan->GetMux()->Name() != req.mux()) {
      response->set_error(absl::StrFormat(
          "Virtual channels with the same name must use the same multiplexer "
          "channel;"
          "channel %s is multiplexer %s, not %s",
          req.channel_name(), vchan->GetMux()->Name(), req.mux()));
      return;
    }
  }

  // Check that the channel types match, if they are provided and
  // already set in the channel.
  if (!req.type().empty() && !channel->Type().empty() &&
      channel->Type() != req.type()) {
    response->set_error(
        absl::StrFormat("Inconsistent channel types for channel %s: "
                        "type has been set as %s, not %s\n",
                        req.channel_name(), channel->Type(), req.type()));
    return;
  }
  if (channel->Type().empty()) {
    channel->SetType(req.type());
  }

  // Check capacity of channel for unreliable channels.
  if (!req.is_reliable()) {
    absl::Status cap_ok = channel->HasSufficientCapacity(0);
    if (!cap_ok.ok()) {
      response->set_error(absl::StrFormat(
          "Insufficient capacity to add a new publisher to channel %s: %s",
          req.channel_name(), cap_ok.ToString()));
      return;
    }
  }

  int num_pubs, num_subs;
  channel->CountUsers(num_pubs, num_subs);
  // Check consistency of publisher parameters.
  if (num_pubs > 0) {
    if (req.is_fixed_size() != channel->IsFixedSize()) {
      response->set_error(
          absl::StrFormat("Inconsistent publisher parameters for channel %s: "
                          "all publishers must be either fixed size or not",
                          req.channel_name()));
      return;
    }

    // We check only the number of slots since the slot size can change
    // over time.
    if ((req.is_fixed_size() && channel->SlotSize() != req.slot_size()) ||
        channel->NumSlots() != req.num_slots()) {
      response->set_error(absl::StrFormat(
          "Inconsistent publisher parameters for channel %s: "
          "existing: %d/%d, new: %d/%d",
          req.channel_name(), channel->SlotSize(), channel->NumSlots(),
          req.slot_size(), req.num_slots()));
      return;
    }

    if (channel->IsLocal() != req.is_local()) {
      response->set_error(
          absl::StrFormat("Inconsistent publisher parameters for channel %s: "
                          "all publishers must be either local or not",
                          req.channel_name()));
      return;
    }
  }

  // Create the publisher.
  absl::StatusOr<PublisherUser *> publisher =
      channel->AddPublisher(this, req.is_reliable(), req.is_local(),
                            req.is_bridge(), req.is_fixed_size());
  if (!publisher.ok()) {
    response->set_error(publisher.status().ToString());
    return;
  }
  response->set_channel_id(channel->GetChannelId());
  response->set_type(channel->Type());
  response->set_vchan_id(channel->GetVirtualChannelId());

  PublisherUser *pub = *publisher;
  response->set_publisher_id(pub->GetId());

  // Copy the shared memory file descriptors.
  const SharedMemoryFds &channel_fds = channel->GetFds();

  response->set_ccb_fd_index(0);
  fds.push_back(channel_fds.ccb);
  response->set_bcb_fd_index(1);
  fds.push_back(channel_fds.bcb);

  int fd_index = 2;

  // Copy the publisher poll and triggers fds.
  response->set_pub_poll_fd_index(fd_index++);
  fds.push_back(pub->GetPollFd());
  response->set_pub_trigger_fd_index(fd_index++);
  fds.push_back(pub->GetTriggerFd());

  // Add subscriber trigger indexes.
  std::vector<toolbelt::FileDescriptor> sub_fds =
      channel->GetSubscriberTriggerFds();
  for (auto &fd : sub_fds) {
    response->add_sub_trigger_fd_indexes(fd_index++);
    fds.push_back(fd);
  }

  if (channel->IsVirtual()) {
    // Also send back the channel multiplexer's subsciber fds so that
    // a subscriber to the whole multiplexer can be triggered when a
    // message is published from any of its virtual channels.
    VirtualChannel *vchan = static_cast<VirtualChannel *>(channel);
    std::vector<toolbelt::FileDescriptor> mux_fds =
        vchan->GetMux()->GetSubscriberTriggerFds();
    for (auto &fd : mux_fds) {
      response->add_sub_trigger_fd_indexes(fd_index++);
      fds.push_back(fd);
    }
  }

  if (!req.is_bridge() && req.is_local()) {
    server_->SendAdvertise(req.channel_name(), req.is_reliable());
  }
  ChannelCounters &counters =
      channel->RecordUpdate(/*is_pub=*/true, /*add=*/true, req.is_reliable());
  response->set_num_sub_updates(counters.num_sub_updates);
}

void ClientHandler::HandleCreateSubscriber(
    const subspace::CreateSubscriberRequest &req,
    subspace::CreateSubscriberResponse *response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    // No channel exists, map an empty channel.
    absl::StatusOr<ServerChannel *> ch = server_->CreateChannel(
        req.channel_name(), 0, 0, req.mux(), req.vchan_id(), req.type());
    if (!ch.ok()) {
      response->set_error(ch.status().ToString());
      return;
    }
    channel = *ch;
  } else {
    // Check that the channel types match, if they are provided and
    // already set in the channel.
    if (!req.type().empty() && !channel->Type().empty() &&
        channel->Type() != req.type()) {
      response->set_error(
          absl::StrFormat("Inconsistent channel types for channel %s: "
                          "type has been set as %s, not %s\n",
                          req.channel_name(), channel->Type(), req.type()));
      return;
    }
    if (channel->Type().empty()) {
      channel->SetType(req.type());
    }
  }
  // Check the virtuality settings.  We can't mix virtual and non-virtual
  // channels with the same name or on different multiplexer channels.
  if (req.mux().empty() && channel->IsVirtual()) {
    response->set_error(
        absl::StrFormat("Channel %s is virtual, but no multiplexer was "
                        "specified for the subscriber",
                        req.channel_name()));
    return;
  }
  if (!req.mux().empty() && !channel->IsVirtual()) {
    response->set_error(
        absl::StrFormat("Channel %s is not virtual, but a multiplexer was "
                        "specified for the subscriber",
                        req.channel_name()));
    return;
  }
  if (channel->IsVirtual()) {
    VirtualChannel *vchan = static_cast<VirtualChannel *>(channel);
    if (vchan->GetMux()->Name() != req.mux()) {
      response->set_error(absl::StrFormat(
          "Virtual channels with the same name must use the same multiplexer "
          "channel;"
          "channel %s is multiplexer %s, not %s",
          req.channel_name(), vchan->GetMux()->Name(), req.mux()));
      return;
    }
  }

  SubscriberUser *sub;
  if (req.subscriber_id() != -1) {
    // This is an existing subscriber.
    absl::StatusOr<User *> user = channel->GetUser(req.subscriber_id());
    if (!user.ok()) {
      response->set_error(user.status().ToString());
      return;
    }
    sub = static_cast<SubscriberUser *>(*user);
  } else {
    if (!req.is_reliable()) {
      absl::Status cap_ok =
          channel->HasSufficientCapacity(req.max_active_messages() - 1);
      if (!cap_ok.ok()) {
        response->set_error(absl::StrFormat(
            "Insufficient capacity to add a new subscriber to channel %s: %s",
            req.channel_name(), cap_ok.ToString()));
        return;
      }
    }
    // Create the subscriber.
    absl::StatusOr<SubscriberUser *> subscriber = channel->AddSubscriber(
        this, req.is_reliable(), req.is_bridge(), req.max_active_messages());
    if (!subscriber.ok()) {
      response->set_error(subscriber.status().ToString());
      return;
    }
    sub = *subscriber;
  }
  channel->RegisterSubscriber(sub->GetId(), channel->GetVirtualChannelId(),
                              req.subscriber_id() == -1);

  response->set_channel_id(channel->GetChannelId());
  response->set_subscriber_id(sub->GetId());
  response->set_type(channel->Type());
  response->set_vchan_id(channel->GetVirtualChannelId());

  const SharedMemoryFds &channel_fds = channel->GetFds();

  response->set_ccb_fd_index(0);
  fds.push_back(channel_fds.ccb);
  response->set_bcb_fd_index(1);
  fds.push_back(channel_fds.bcb);

  int fd_index = 2;

  response->set_trigger_fd_index(fd_index++);
  fds.push_back(sub->GetTriggerFd());

  response->set_poll_fd_index(fd_index++);
  fds.push_back(sub->GetPollFd());

  response->set_slot_size(channel->SlotSize());
  response->set_num_slots(channel->NumSlots());
  // Add publisher trigger indexes.
  std::vector<toolbelt::FileDescriptor> pub_fds =
      channel->GetReliablePublisherTriggerFds();
  for (auto &fd : pub_fds) {
    response->add_reliable_pub_trigger_fd_indexes(fd_index++);
    fds.push_back(fd);
  }

  if (channel->IsVirtual()) {
    // Also send back the channel multiplexer's subsciber fds so that
    // a subscriber to the whole multiplexer can be triggered when a
    // message is published from any of its virtual channels.
    VirtualChannel *vchan = static_cast<VirtualChannel *>(channel);
    std::vector<toolbelt::FileDescriptor> mux_fds =
        vchan->GetMux()->GetReliablePublisherTriggerFds();
    for (auto &fd : mux_fds) {
      response->add_reliable_pub_trigger_fd_indexes(fd_index++);
      fds.push_back(fd);
    }
  }

  if (!req.is_bridge()) {
    // Send Query to subscribe to public channels on other servers.
    server_->SendQuery(req.channel_name());
  }
  ChannelCounters &counters =
      channel->RecordUpdate(/*is_pub=*/false, /*add=*/true, req.is_reliable());
  response->set_num_pub_updates(counters.num_pub_updates);
}

void ClientHandler::HandleGetTriggers(
    const subspace::GetTriggersRequest &req,
    subspace::GetTriggersResponse *response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    response->set_error(
        absl::StrFormat("No such channel %s", req.channel_name()));
    return;
  }
  int index = 0;
  std::vector<toolbelt::FileDescriptor> pub_fds =
      channel->GetReliablePublisherTriggerFds();
  for (auto &fd : pub_fds) {
    response->add_reliable_pub_trigger_fd_indexes(index++);
    fds.push_back(fd);
  }

  std::vector<toolbelt::FileDescriptor> sub_fds =
      channel->GetSubscriberTriggerFds();
  for (auto &fd : sub_fds) {
    response->add_sub_trigger_fd_indexes(index++);
    fds.push_back(fd);
  }

  if (channel->IsVirtual()) {
    // Also send back the channel multiplexer's subsciber and publisher fds.
    VirtualChannel *vchan = static_cast<VirtualChannel *>(channel);
    std::vector<toolbelt::FileDescriptor> mux_fds =
        vchan->GetMux()->GetSubscriberTriggerFds();
    for (auto &fd : mux_fds) {
      response->add_sub_trigger_fd_indexes(index++);
      fds.push_back(fd);
    }

    mux_fds = vchan->GetMux()->GetReliablePublisherTriggerFds();
    for (auto &fd : mux_fds) {
      response->add_reliable_pub_trigger_fd_indexes(index++);
      fds.push_back(fd);
    }
  }
}

void ClientHandler::HandleRemovePublisher(
    const subspace::RemovePublisherRequest &req,
    subspace::RemovePublisherResponse *response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    response->set_error(
        absl::StrFormat("No such channel %s", req.channel_name()));
    return;
  }
  channel->RemoveUser(server_, req.publisher_id());
}

void ClientHandler::HandleRemoveSubscriber(
    const subspace::RemoveSubscriberRequest &req,
    subspace::RemoveSubscriberResponse *response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    response->set_error(
        absl::StrFormat("No such channel %s", req.channel_name()));
    return;
  }
  channel->RemoveUser(server_, req.subscriber_id());
}

} // namespace subspace
