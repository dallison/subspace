// Copyright 2023-2026 David Allison
// Shadow support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "shadow/shadow.h"
#include "absl/strings/str_format.h"
#include <unistd.h>

namespace subspace {

Shadow::Shadow(co::CoroutineScheduler &scheduler,
               const std::string &socket_name)
    : scheduler_(scheduler), socket_name_(socket_name), logger_("shadow") {}

void Shadow::Stop() { scheduler_.Stop(); }

void Shadow::NotifyEvent() {
  if (notify_fd_ >= 0) {
    char c = 0;
    (void)::write(notify_fd_, &c, 1);
  }
}

absl::Status Shadow::Run() {
#ifndef __linux__
  remove(socket_name_.c_str());
#endif
  absl::Status status = listen_socket_.Bind(socket_name_, true);
  if (!status.ok()) {
    return status;
  }

  logger_.Log(toolbelt::LogLevel::kDebug, "Shadow listening on %s",
              socket_name_.c_str());

  NotifyEvent();

  scheduler_.Spawn([this]() { ListenerCoroutine(); },
                   {.name = "Shadow listener"});

  scheduler_.Run();
  return absl::OkStatus();
}

void Shadow::ListenerCoroutine() {
  for (;;) {
    absl::StatusOr<toolbelt::UnixSocket> accepted =
        listen_socket_.Accept(co::self);
    if (!accepted.ok()) {
      break;
    }

    logger_.Log(toolbelt::LogLevel::kDebug,
                "Shadow accepted connection from server");

    auto client =
        std::make_shared<toolbelt::UnixSocket>(std::move(*accepted));

    scheduler_.Spawn([this, client]() { ClientCoroutine(client); },
                     {.name = "Shadow client"});
  }
}

void Shadow::ClientCoroutine(
    std::shared_ptr<toolbelt::UnixSocket> client_socket) {
  // Send current state dump to the newly connected server.
  if (absl::Status s = SendStateDump(*client_socket); !s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Shadow: failed to send state dump: %s",
                s.ToString().c_str());
    return;
  }

  for (;;) {
    absl::StatusOr<std::vector<char>> receive_buffer =
        client_socket->ReceiveVariableLengthMessage(co::self);
    if (!receive_buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kError, "Shadow receive failed: %s",
                  receive_buffer.status().ToString().c_str());
      break;
    }
    if (receive_buffer->empty()) {
      logger_.Log(toolbelt::LogLevel::kDebug, "Server disconnected");
      break;
    }

    ShadowEvent event;
    if (!event.ParseFromArray(receive_buffer->data(),
                              int(receive_buffer->size()))) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Shadow: failed to parse event");
      break;
    }

    bool has_fds = event.has_init() || event.has_create_channel() ||
                   event.has_add_publisher() || event.has_add_subscriber();

    std::vector<toolbelt::FileDescriptor> fds;
    if (has_fds) {
      absl::Status fd_status = client_socket->ReceiveFds(fds, co::self);
      if (!fd_status.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Shadow: ReceiveFds failed: %s",
                    fd_status.ToString().c_str());
        break;
      }
    }

    absl::Status handle_status = HandleEvent(event, fds);
    if (!handle_status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Shadow: event handling failed: %s",
                  handle_status.ToString().c_str());
    }
    NotifyEvent();
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Shadow waiting for new server connection");
}

absl::Status Shadow::HandleEvent(const ShadowEvent &event,
                                 std::vector<toolbelt::FileDescriptor> &fds) {
  switch (event.event_case()) {
  case ShadowEvent::kInit:
    return HandleInit(event.init(), fds);
  case ShadowEvent::kCreateChannel:
    return HandleCreateChannel(event.create_channel(), fds);
  case ShadowEvent::kRemoveChannel:
    return HandleRemoveChannel(event.remove_channel());
  case ShadowEvent::kAddPublisher:
    return HandleAddPublisher(event.add_publisher(), fds);
  case ShadowEvent::kRemovePublisher:
    return HandleRemovePublisher(event.remove_publisher());
  case ShadowEvent::kAddSubscriber:
    return HandleAddSubscriber(event.add_subscriber(), fds);
  case ShadowEvent::kRemoveSubscriber:
    return HandleRemoveSubscriber(event.remove_subscriber());
  case ShadowEvent::kStateDump:
  case ShadowEvent::kStateDone:
    return absl::OkStatus();
  case ShadowEvent::EVENT_NOT_SET:
    return absl::InternalError("Shadow: received event with no type set");
  }
  return absl::OkStatus();
}

absl::Status Shadow::HandleInit(const ShadowInit &msg,
                                std::vector<toolbelt::FileDescriptor> &fds) {
  if (fds.size() < 1) {
    return absl::InternalError("Shadow: init event missing SCB fd");
  }
  session_id_ = msg.session_id();
  scb_fd_ = std::move(fds[0]);

  channels_.clear();

  logger_.Log(toolbelt::LogLevel::kDebug, "Shadow: init session_id=%lu",
              session_id_);
  return absl::OkStatus();
}

absl::Status
Shadow::HandleCreateChannel(const ShadowCreateChannel &msg,
                            std::vector<toolbelt::FileDescriptor> &fds) {
  if (fds.size() < 2) {
    return absl::InternalError(
        absl::StrFormat("Shadow: create_channel '%s' missing CCB/BCB fds",
                        msg.channel_name()));
  }

  ShadowChannel ch{
      .name = msg.channel_name(),
      .channel_id = msg.channel_id(),
      .slot_size = msg.slot_size(),
      .num_slots = msg.num_slots(),
      .type = msg.type(),
      .is_local = msg.is_local(),
      .is_reliable = msg.is_reliable(),
      .is_fixed_size = msg.is_fixed_size(),
      .checksum_size = msg.checksum_size(),
      .metadata_size = msg.metadata_size(),
      .mux = msg.mux(),
      .vchan_id = msg.vchan_id(),
      .ccb_fd = std::move(fds[0]),
      .bcb_fd = std::move(fds[1]),
  };

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Shadow: create channel '%s' id=%d slots=%d/%d",
              ch.name.c_str(), ch.channel_id, ch.num_slots, ch.slot_size);

  channels_.emplace(ch.name, std::move(ch));
  return absl::OkStatus();
}

absl::Status
Shadow::HandleRemoveChannel(const ShadowRemoveChannel &msg) {
  logger_.Log(toolbelt::LogLevel::kDebug, "Shadow: remove channel '%s' id=%d",
              msg.channel_name().c_str(), msg.channel_id());

  channels_.erase(msg.channel_name());
  return absl::OkStatus();
}

absl::Status
Shadow::HandleAddPublisher(const ShadowAddPublisher &msg,
                           std::vector<toolbelt::FileDescriptor> &fds) {
  size_t expected_fds = msg.notify_retirement() ? 4 : 2;
  if (fds.size() < expected_fds) {
    return absl::InternalError(
        absl::StrFormat("Shadow: add_publisher '%s' pub_id=%d expected %d fds, "
                        "got %d",
                        msg.channel_name(), msg.publisher_id(), expected_fds,
                        fds.size()));
  }

  auto it = channels_.find(msg.channel_name());
  if (it == channels_.end()) {
    return absl::InternalError(
        absl::StrFormat("Shadow: add_publisher for unknown channel '%s'",
                        msg.channel_name()));
  }

  ShadowPublisher pub{
      .id = msg.publisher_id(),
      .is_reliable = msg.is_reliable(),
      .is_local = msg.is_local(),
      .is_bridge = msg.is_bridge(),
      .for_tunnel = msg.for_tunnel(),
      .is_fixed_size = msg.is_fixed_size(),
      .notify_retirement = msg.notify_retirement(),
      .poll_fd = std::move(fds[0]),
      .trigger_fd = std::move(fds[1]),
      .retirement_read_fd = msg.notify_retirement() ? std::move(fds[2])
                                                    : toolbelt::FileDescriptor(),
      .retirement_write_fd = msg.notify_retirement() ? std::move(fds[3])
                                                     : toolbelt::FileDescriptor(),
  };

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Shadow: add publisher '%s' pub_id=%d reliable=%d",
              msg.channel_name().c_str(), pub.id, pub.is_reliable);

  it->second.publishers.emplace(pub.id, std::move(pub));
  return absl::OkStatus();
}

absl::Status
Shadow::HandleRemovePublisher(const ShadowRemovePublisher &msg) {
  auto it = channels_.find(msg.channel_name());
  if (it == channels_.end()) {
    return absl::OkStatus();
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Shadow: remove publisher '%s' pub_id=%d",
              msg.channel_name().c_str(), msg.publisher_id());

  it->second.publishers.erase(msg.publisher_id());
  return absl::OkStatus();
}

absl::Status
Shadow::HandleAddSubscriber(const ShadowAddSubscriber &msg,
                            std::vector<toolbelt::FileDescriptor> &fds) {
  if (fds.size() < 2) {
    return absl::InternalError(
        absl::StrFormat("Shadow: add_subscriber '%s' sub_id=%d missing fds",
                        msg.channel_name(), msg.subscriber_id()));
  }

  auto it = channels_.find(msg.channel_name());
  if (it == channels_.end()) {
    return absl::InternalError(
        absl::StrFormat("Shadow: add_subscriber for unknown channel '%s'",
                        msg.channel_name()));
  }

  ShadowSubscriber sub{
      .id = msg.subscriber_id(),
      .is_reliable = msg.is_reliable(),
      .is_bridge = msg.is_bridge(),
      .for_tunnel = msg.for_tunnel(),
      .max_active_messages = msg.max_active_messages(),
      .trigger_fd = std::move(fds[0]),
      .poll_fd = std::move(fds[1]),
  };

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Shadow: add subscriber '%s' sub_id=%d reliable=%d",
              msg.channel_name().c_str(), sub.id, sub.is_reliable);

  it->second.subscribers.emplace(sub.id, std::move(sub));
  return absl::OkStatus();
}

absl::Status
Shadow::HandleRemoveSubscriber(const ShadowRemoveSubscriber &msg) {
  auto it = channels_.find(msg.channel_name());
  if (it == channels_.end()) {
    return absl::OkStatus();
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Shadow: remove subscriber '%s' sub_id=%d",
              msg.channel_name().c_str(), msg.subscriber_id());

  it->second.subscribers.erase(msg.subscriber_id());
  return absl::OkStatus();
}

absl::Status
Shadow::SendEvent(toolbelt::UnixSocket &socket, const ShadowEvent &event,
                  const std::vector<toolbelt::FileDescriptor> &fds) {
  size_t msglen = event.ByteSizeLong();
  std::vector<char> buffer(sizeof(int32_t) + msglen);
  if (!event.SerializeToArray(buffer.data() + sizeof(int32_t), msglen)) {
    return absl::InternalError("Shadow: failed to serialize event for dump");
  }

  absl::StatusOr<ssize_t> n =
      socket.SendMessage(buffer.data() + sizeof(int32_t), msglen);
  if (!n.ok()) {
    return n.status();
  }

  if (!fds.empty()) {
    absl::Status status = socket.SendFds(fds);
    if (!status.ok()) {
      return status;
    }
  }
  return absl::OkStatus();
}

absl::Status Shadow::SendStateDump(toolbelt::UnixSocket &socket) {
  // 1. Send the header.
  {
    ShadowEvent event;
    auto *dump = event.mutable_state_dump();
    dump->set_session_id(session_id_);
    dump->set_num_channels(static_cast<int32_t>(channels_.size()));

    std::vector<toolbelt::FileDescriptor> fds;
    if (session_id_ != 0 && scb_fd_.Valid()) {
      fds.push_back(scb_fd_);
    }
    if (absl::Status s = SendEvent(socket, event, fds); !s.ok()) {
      return s;
    }
  }

  // 2. For each channel, send ShadowCreateChannel + FDs, then its
  //    publishers and subscribers.
  for (auto &[name, ch] : channels_) {
    // Channel.
    {
      ShadowEvent event;
      auto *msg = event.mutable_create_channel();
      msg->set_channel_name(ch.name);
      msg->set_channel_id(ch.channel_id);
      msg->set_slot_size(ch.slot_size);
      msg->set_num_slots(ch.num_slots);
      msg->set_type(ch.type);
      msg->set_is_local(ch.is_local);
      msg->set_is_reliable(ch.is_reliable);
      msg->set_is_fixed_size(ch.is_fixed_size);
      msg->set_checksum_size(ch.checksum_size);
      msg->set_metadata_size(ch.metadata_size);
      msg->set_mux(ch.mux);
      msg->set_vchan_id(ch.vchan_id);

      std::vector<toolbelt::FileDescriptor> fds;
      fds.push_back(ch.ccb_fd);
      fds.push_back(ch.bcb_fd);
      if (absl::Status s = SendEvent(socket, event, fds); !s.ok()) {
        return s;
      }
    }

    // Publishers.
    for (auto &[pub_id, pub] : ch.publishers) {
      ShadowEvent event;
      auto *msg = event.mutable_add_publisher();
      msg->set_channel_name(ch.name);
      msg->set_publisher_id(pub.id);
      msg->set_is_reliable(pub.is_reliable);
      msg->set_is_local(pub.is_local);
      msg->set_is_bridge(pub.is_bridge);
      msg->set_for_tunnel(pub.for_tunnel);
      msg->set_is_fixed_size(pub.is_fixed_size);
      msg->set_notify_retirement(pub.notify_retirement);

      std::vector<toolbelt::FileDescriptor> fds;
      fds.push_back(pub.poll_fd);
      fds.push_back(pub.trigger_fd);
      if (pub.notify_retirement) {
        fds.push_back(pub.retirement_read_fd);
        fds.push_back(pub.retirement_write_fd);
      }
      if (absl::Status s = SendEvent(socket, event, fds); !s.ok()) {
        return s;
      }
    }

    // Subscribers.
    for (auto &[sub_id, sub] : ch.subscribers) {
      ShadowEvent event;
      auto *msg = event.mutable_add_subscriber();
      msg->set_channel_name(ch.name);
      msg->set_subscriber_id(sub.id);
      msg->set_is_reliable(sub.is_reliable);
      msg->set_is_bridge(sub.is_bridge);
      msg->set_for_tunnel(sub.for_tunnel);
      msg->set_max_active_messages(sub.max_active_messages);

      std::vector<toolbelt::FileDescriptor> fds;
      fds.push_back(sub.trigger_fd);
      fds.push_back(sub.poll_fd);
      if (absl::Status s = SendEvent(socket, event, fds); !s.ok()) {
        return s;
      }
    }
  }

  // 3. Send the done marker.
  {
    ShadowEvent event;
    event.mutable_state_done();
    if (absl::Status s = SendEvent(socket, event); !s.ok()) {
      return s;
    }
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Shadow: sent state dump (session_id=%lu, %d channels)",
              session_id_, static_cast<int>(channels_.size()));
  return absl::OkStatus();
}

} // namespace subspace
