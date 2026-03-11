// Copyright 2023-2026 David Allison
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

} // namespace subspace
