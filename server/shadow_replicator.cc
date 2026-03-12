// Copyright 2023-2026 David Allison
// Shadow support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/shadow_replicator.h"
#include "server/server_channel.h"
#include <arpa/inet.h>

namespace subspace {

ShadowReplicator::ShadowReplicator(const std::string &shadow_socket_name,
                                   toolbelt::Logger &logger)
    : shadow_socket_name_(shadow_socket_name), logger_(logger) {}

absl::Status ShadowReplicator::Connect() {
  absl::Status status = socket_.Connect(shadow_socket_name_);
  if (!status.ok()) {
    return status;
  }
  connected_ = true;
  return absl::OkStatus();
}

void ShadowReplicator::Close() {
  if (connected_) {
    socket_.Close();
    connected_ = false;
  }
}

void ShadowReplicator::SendEvent(
    const ShadowEvent &event,
    const std::vector<toolbelt::FileDescriptor> &fds) {
  if (!connected_) {
    return;
  }

  size_t msglen = event.ByteSizeLong();
  std::vector<char> buffer(sizeof(int32_t) + msglen);
  if (!event.SerializeToArray(buffer.data() + sizeof(int32_t), msglen)) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Shadow: failed to serialize event");
    return;
  }

  absl::StatusOr<ssize_t> n =
      socket_.SendMessage(buffer.data() + sizeof(int32_t), msglen);
  if (!n.ok()) {
    logger_.Log(toolbelt::LogLevel::kError, "Shadow: send failed: %s",
                n.status().ToString().c_str());
    Close();
    return;
  }

  if (!fds.empty()) {
    absl::Status status = socket_.SendFds(fds);
    if (!status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError, "Shadow: SendFds failed: %s",
                  status.ToString().c_str());
      Close();
      return;
    }
  }
}

void ShadowReplicator::SendInit(uint64_t session_id,
                                const toolbelt::FileDescriptor &scb_fd) {
  ShadowEvent event;
  auto *init = event.mutable_init();
  init->set_session_id(session_id);

  std::vector<toolbelt::FileDescriptor> fds;
  fds.push_back(scb_fd);
  SendEvent(event, fds);
}

void ShadowReplicator::SendCreateChannel(ServerChannel *channel) {
  ShadowEvent event;
  auto *msg = event.mutable_create_channel();
  msg->set_channel_name(channel->Name());
  msg->set_channel_id(channel->GetChannelId());
  msg->set_slot_size(channel->SlotSize());
  msg->set_num_slots(channel->NumSlots());
  msg->set_type(channel->Type());
  msg->set_is_local(channel->IsLocal());
  msg->set_is_reliable(channel->IsReliable());
  msg->set_is_fixed_size(channel->IsFixedSize());
  msg->set_checksum_size(channel->ChecksumSize());
  msg->set_metadata_size(channel->MetadataSize());

  if (channel->IsVirtual()) {
    const auto *vchan = static_cast<const VirtualChannel *>(channel);
    msg->set_mux(vchan->GetMux()->Name());
    msg->set_vchan_id(vchan->GetVirtualChannelId());
  }

  const SharedMemoryFds &channel_fds = channel->GetFds();
  std::vector<toolbelt::FileDescriptor> fds;
  fds.push_back(channel_fds.ccb);
  fds.push_back(channel_fds.bcb);
  SendEvent(event, fds);
}

void ShadowReplicator::SendRemoveChannel(const std::string &name,
                                         int channel_id) {
  ShadowEvent event;
  auto *msg = event.mutable_remove_channel();
  msg->set_channel_name(name);
  msg->set_channel_id(channel_id);
  SendEvent(event);
}

void ShadowReplicator::SendAddPublisher(const std::string &channel_name,
                                        const PublisherUser *pub) {
  ShadowEvent event;
  auto *msg = event.mutable_add_publisher();
  msg->set_channel_name(channel_name);
  msg->set_publisher_id(pub->GetId());
  msg->set_is_reliable(pub->IsReliable());
  msg->set_is_local(pub->IsLocal());
  msg->set_is_bridge(pub->IsBridge());
  msg->set_is_fixed_size(pub->IsFixedSize());

  std::vector<toolbelt::FileDescriptor> fds;
  fds.push_back(const_cast<PublisherUser *>(pub)->GetPollFd());
  fds.push_back(const_cast<PublisherUser *>(pub)->GetTriggerFd());

  bool has_retirement =
      const_cast<PublisherUser *>(pub)->GetRetirementFdReader().Valid();
  msg->set_notify_retirement(has_retirement);
  if (has_retirement) {
    fds.push_back(const_cast<PublisherUser *>(pub)->GetRetirementFdReader());
    fds.push_back(const_cast<PublisherUser *>(pub)->GetRetirementFdWriter());
  }

  SendEvent(event, fds);
}

void ShadowReplicator::SendRemovePublisher(const std::string &channel_name,
                                           int pub_id) {
  ShadowEvent event;
  auto *msg = event.mutable_remove_publisher();
  msg->set_channel_name(channel_name);
  msg->set_publisher_id(pub_id);
  SendEvent(event);
}

void ShadowReplicator::SendAddSubscriber(const std::string &channel_name,
                                         const SubscriberUser *sub) {
  ShadowEvent event;
  auto *msg = event.mutable_add_subscriber();
  msg->set_channel_name(channel_name);
  msg->set_subscriber_id(sub->GetId());
  msg->set_is_reliable(sub->IsReliable());
  msg->set_is_bridge(sub->IsBridge());
  msg->set_max_active_messages(sub->MaxActiveMessages());

  std::vector<toolbelt::FileDescriptor> fds;
  fds.push_back(const_cast<SubscriberUser *>(sub)->GetTriggerFd());
  fds.push_back(const_cast<SubscriberUser *>(sub)->GetPollFd());
  SendEvent(event, fds);
}

void ShadowReplicator::SendRemoveSubscriber(const std::string &channel_name,
                                            int sub_id) {
  ShadowEvent event;
  auto *msg = event.mutable_remove_subscriber();
  msg->set_channel_name(channel_name);
  msg->set_subscriber_id(sub_id);
  SendEvent(event);
}

absl::StatusOr<ShadowEvent> ShadowReplicator::ReceiveEvent(
    std::vector<toolbelt::FileDescriptor> &fds) {
  absl::StatusOr<std::vector<char>> recv =
      socket_.ReceiveVariableLengthMessage();
  if (!recv.ok()) {
    return recv.status();
  }
  if (recv->empty()) {
    return absl::UnavailableError("Shadow disconnected during state dump");
  }

  ShadowEvent event;
  if (!event.ParseFromArray(recv->data(), static_cast<int>(recv->size()))) {
    return absl::InternalError("Failed to parse shadow event");
  }

  bool has_fds = event.has_create_channel() ||
                 event.has_add_publisher() || event.has_add_subscriber();
  if (has_fds) {
    absl::Status s = socket_.ReceiveFds(fds);
    if (!s.ok()) {
      return s;
    }
  }
  return event;
}

absl::StatusOr<RecoveredState> ShadowReplicator::ReceiveStateDump() {
  if (!connected_) {
    return absl::FailedPreconditionError("Not connected to shadow");
  }

  RecoveredState state;

  // 1. Read the ShadowStateDump header.
  {
    std::vector<toolbelt::FileDescriptor> fds;
    absl::StatusOr<ShadowEvent> event = ReceiveEvent(fds);
    if (!event.ok()) {
      return event.status();
    }
    if (!event->has_state_dump()) {
      return absl::InternalError(
          "Expected ShadowStateDump header, got different event");
    }
    const auto &dump = event->state_dump();
    state.session_id = dump.session_id();
    if (state.session_id != 0) {
      std::vector<toolbelt::FileDescriptor> scb_fds;
      absl::Status s = socket_.ReceiveFds(scb_fds);
      if (!s.ok()) {
        return s;
      }
      if (!scb_fds.empty()) {
        state.scb_fd = std::move(scb_fds[0]);
      }
    }
  }

  // 2. Read channel/publisher/subscriber events until ShadowStateDone.
  for (;;) {
    std::vector<toolbelt::FileDescriptor> fds;
    absl::StatusOr<ShadowEvent> event = ReceiveEvent(fds);
    if (!event.ok()) {
      return event.status();
    }

    if (event->has_state_done()) {
      break;
    }

    if (event->has_create_channel()) {
      const auto &msg = event->create_channel();
      if (fds.size() < 2) {
        return absl::InternalError("create_channel in dump missing FDs");
      }
      RecoveredChannel ch;
      ch.name = msg.channel_name();
      ch.channel_id = msg.channel_id();
      ch.slot_size = msg.slot_size();
      ch.num_slots = msg.num_slots();
      ch.type = msg.type();
      ch.is_local = msg.is_local();
      ch.is_reliable = msg.is_reliable();
      ch.is_fixed_size = msg.is_fixed_size();
      ch.checksum_size = msg.checksum_size();
      ch.metadata_size = msg.metadata_size();
      ch.mux = msg.mux();
      ch.vchan_id = msg.vchan_id();
      ch.ccb_fd = std::move(fds[0]);
      ch.bcb_fd = std::move(fds[1]);
      state.channels.push_back(std::move(ch));
      continue;
    }

    if (event->has_add_publisher()) {
      const auto &msg = event->add_publisher();
      size_t expected = msg.notify_retirement() ? 4 : 2;
      if (fds.size() < expected) {
        return absl::InternalError("add_publisher in dump missing FDs");
      }
      RecoveredPublisher pub;
      pub.id = msg.publisher_id();
      pub.is_reliable = msg.is_reliable();
      pub.is_local = msg.is_local();
      pub.is_bridge = msg.is_bridge();
      pub.is_fixed_size = msg.is_fixed_size();
      pub.notify_retirement = msg.notify_retirement();
      pub.poll_fd = std::move(fds[0]);
      pub.trigger_fd = std::move(fds[1]);
      if (msg.notify_retirement()) {
        pub.retirement_read_fd = std::move(fds[2]);
        pub.retirement_write_fd = std::move(fds[3]);
      }
      // Attach to the last channel (dump sends publishers right after
      // their channel).
      if (state.channels.empty()) {
        return absl::InternalError("add_publisher before any channel");
      }
      auto &target = state.channels.back();
      if (target.name != msg.channel_name()) {
        // Find the right channel.
        bool found = false;
        for (auto &c : state.channels) {
          if (c.name == msg.channel_name()) {
            c.publishers.push_back(std::move(pub));
            found = true;
            break;
          }
        }
        if (!found) {
          return absl::InternalError(absl::StrFormat(
              "add_publisher for unknown channel '%s'", msg.channel_name()));
        }
      } else {
        target.publishers.push_back(std::move(pub));
      }
      continue;
    }

    if (event->has_add_subscriber()) {
      const auto &msg = event->add_subscriber();
      if (fds.size() < 2) {
        return absl::InternalError("add_subscriber in dump missing FDs");
      }
      RecoveredSubscriber sub;
      sub.id = msg.subscriber_id();
      sub.is_reliable = msg.is_reliable();
      sub.is_bridge = msg.is_bridge();
      sub.max_active_messages = msg.max_active_messages();
      sub.trigger_fd = std::move(fds[0]);
      sub.poll_fd = std::move(fds[1]);
      if (state.channels.empty()) {
        return absl::InternalError("add_subscriber before any channel");
      }
      auto &target = state.channels.back();
      if (target.name != msg.channel_name()) {
        bool found = false;
        for (auto &c : state.channels) {
          if (c.name == msg.channel_name()) {
            c.subscribers.push_back(std::move(sub));
            found = true;
            break;
          }
        }
        if (!found) {
          return absl::InternalError(absl::StrFormat(
              "add_subscriber for unknown channel '%s'", msg.channel_name()));
        }
      } else {
        target.subscribers.push_back(std::move(sub));
      }
      continue;
    }

    return absl::InternalError("Unexpected event type in state dump");
  }

  logger_.Log(toolbelt::LogLevel::kInfo,
              "Received state dump from shadow (session_id=%lu, %d channels)",
              state.session_id, static_cast<int>(state.channels.size()));
  return state;
}

} // namespace subspace
