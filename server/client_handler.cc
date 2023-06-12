// Copyright 2023 David Allison
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
    absl::StatusOr<ssize_t> n =
        socket_.ReceiveMessage(buffer_, sizeof(buffer_), c);
    if (!n.ok()) {
      server_->CloseHandler(this);
      return;
    }
    subspace::Request request;
    if (request.ParseFromArray(buffer_, *n)) {
      std::vector<toolbelt::FileDescriptor> fds;
      subspace::Response response;
      if (absl::Status s = HandleMessage(request, response, fds); !s.ok()) {
        fprintf(stderr, "%s\n", s.ToString().c_str());
        server_->CloseHandler(this);
        return;
      }

      if (!response.SerializeToArray(sendbuf, kSendBufLen)) {
        fprintf(stderr, "Failed to serialize response\n");
        server_->CloseHandler(this);
        return;
      }
      size_t msglen = response.ByteSizeLong();
      absl::StatusOr<ssize_t> n = socket_.SendMessage(sendbuf, msglen, c);
      if (!n.ok()) {
        server_->CloseHandler(this);
        return;
      }
      if (absl::Status status = socket_.SendFds(fds, c); !status.ok()) {
        fprintf(stderr, "%s\n", status.ToString().c_str());
        server_->CloseHandler(this);
        return;
      }
    } else {
      fprintf(stderr, "Failed to parse message\n");
      server_->CloseHandler(this);
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

  case subspace::Request::kResize:
    HandleResize(req.resize(), resp.mutable_resize(), fds);
    break;

  case subspace::Request::kGetBuffers:
    HandleGetBuffers(req.get_buffers(), resp.mutable_get_buffers(), fds);
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
}

void ClientHandler::HandleCreatePublisher(
    const subspace::CreatePublisherRequest &req,
    subspace::CreatePublisherResponse *response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    absl::StatusOr<ServerChannel *> ch = server_->CreateChannel(
        req.channel_name(), req.slot_size(), req.num_slots(), req.type());
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

  // Check capacity of channel.
  absl::Status cap_ok = channel->HasSufficientCapacity();
  if (!cap_ok.ok()) {
    response->set_error(absl::StrFormat(
        "Insufficient capcacity to add a new publisher to channel %s: %s",
        req.channel_name(), cap_ok.ToString()));
    return;
  }

  int num_pubs, num_subs;
  channel->CountUsers(num_pubs, num_subs);

  // Check consistency of publisher parameters.
  if (num_pubs > 0) {
    if (channel->SlotSize() != req.slot_size() ||
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
  absl::StatusOr<PublisherUser *> publisher = channel->AddPublisher(
      this, req.is_reliable(), req.is_local(), req.is_bridge());
  if (!publisher.ok()) {
    response->set_error(publisher.status().ToString());
    return;
  }
  response->set_channel_id(channel->GetChannelId());
  response->set_type(channel->Type());

  PublisherUser *pub = *publisher;
  response->set_publisher_id(pub->GetId());

  // Copy the shared memory file descriptors.
  const SharedMemoryFds &channel_fds = channel->GetFds();

  response->set_ccb_fd_index(0);
  fds.push_back(channel_fds.ccb);

  int fd_index = 1;
  for (const auto &buffer : channel_fds.buffers) {
    auto *info = response->add_buffers();
    info->set_slot_size(buffer.slot_size);
    info->set_fd_index(fd_index++);
    fds.push_back(buffer.fd);
  }

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
    absl::StatusOr<ServerChannel *> ch =
        server_->CreateChannel(req.channel_name(), 0, 0, req.type());
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
  absl::Status cap_ok = channel->HasSufficientCapacity();
  if (!cap_ok.ok()) {
    response->set_error(absl::StrFormat(
        "Insufficient capcacity to add a new subscriber to channel %s: %s",
        req.channel_name(), cap_ok.ToString()));
    return;
  }
  SubscriberUser *sub;
  if (req.subscriber_id() != -1) {
    // This is an exsiting subscriber.
    absl::StatusOr<User *> user = channel->GetUser(req.subscriber_id());
    if (!user.ok()) {
      response->set_error(user.status().ToString());
      return;
    }
    sub = static_cast<SubscriberUser *>(*user);
  } else {
    // Create the subscriber.
    absl::StatusOr<SubscriberUser *> subscriber =
        channel->AddSubscriber(this, req.is_reliable(), req.is_bridge());
    if (!subscriber.ok()) {
      response->set_error(subscriber.status().ToString());
      return;
    }
    sub = *subscriber;
  }
  response->set_channel_id(channel->GetChannelId());
  response->set_subscriber_id(sub->GetId());
  response->set_type(channel->Type());

  const SharedMemoryFds &channel_fds = channel->GetFds();

  response->set_ccb_fd_index(0);
  fds.push_back(channel_fds.ccb);

  int fd_index = 1;
  for (const auto &buffer : channel_fds.buffers) {
    auto *info = response->add_buffers();
    info->set_slot_size(buffer.slot_size);
    info->set_fd_index(fd_index++);
    fds.push_back(buffer.fd);
  }

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
  channel->RemoveUser(req.publisher_id());
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
  channel->RemoveUser(req.subscriber_id());
}

void ClientHandler::HandleResize(const subspace::ResizeRequest &req,
                                 subspace::ResizeResponse *response,
                                 std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    response->set_error(
        absl::StrFormat("No such channel %s", req.channel_name()));
    return;
  }
  absl::StatusOr<toolbelt::FileDescriptor> fd =
      channel->ExtendBuffers(req.new_slot_size());
  if (!fd.ok()) {
    response->set_error(absl::StrFormat(
        "Failed to resize channel %s to %d byte: %s", req.channel_name(),
        req.new_slot_size(), fd.status().ToString()));
    return;
  }
  channel->AddBuffer(req.new_slot_size(), std::move(*fd));
  const SharedMemoryFds &channel_fds = channel->GetFds();

  int fd_index = 0;
  for (const auto &buffer : channel_fds.buffers) {
    auto *info = response->add_buffers();
    info->set_slot_size(buffer.slot_size);
    info->set_fd_index(fd_index++);
    fds.push_back(buffer.fd);
  }
}

void ClientHandler::HandleGetBuffers(
    const subspace::GetBuffersRequest &req,
    subspace::GetBuffersResponse *response,
    std::vector<toolbelt::FileDescriptor> &fds) {
  ServerChannel *channel = server_->FindChannel(req.channel_name());
  if (channel == nullptr) {
    response->set_error(
        absl::StrFormat("No such channel %s", req.channel_name()));
    return;
  }
  const SharedMemoryFds &channel_fds = channel->GetFds();

  int fd_index = 0;
  for (const auto &buffer : channel_fds.buffers) {
    auto *info = response->add_buffers();
    info->set_slot_size(buffer.slot_size);
    info->set_fd_index(fd_index++);
    fds.push_back(buffer.fd);
  }
}

} // namespace subspace
