// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "server/shadow_replicator.h"

namespace subspace {

absl::Status Server::RecoverFromShadow(RecoveredState &state) {
  auto configure_channel = [this](ServerChannel *channel,
                                  RecoveredChannel &rch,
                                  bool map_storage) -> absl::Status {
    channel->SetDebug(logger_.GetLogLevel() <=
                      toolbelt::LogLevel::kVerboseDebug);
    channel->SetLastKnownSlotSize(rch.slot_size);
    // The prefix size is derived, not replicated, so it has to be recomputed
    // here or the recovered channel keeps the default 64 and reports a
    // prefix that disagrees with its own checksum/metadata sizes.
    channel->SetPrefixLayout(rch.checksum_size, rch.metadata_size);
    if (rch.is_local) {
      channel->LatchLocal();
    }
    if (rch.hidden) {
      if (rch.telemetry_target.empty()) {
        return absl::InvalidArgumentError(
            "Recovered hidden telemetry channel has no target");
      }
      channel->SetTelemetryTarget(rch.telemetry_target);
    }
    if (rch.has_split_buffer_options) {
      if (absl::Status s = channel->ValidateOrSetSplitBufferOptions(
              {.use_split_buffers = rch.use_split_buffers,
               .split_buffers_over_bridge = rch.split_buffers_over_bridge},
              /*set_if_missing=*/true, "shadow recovery");
          !s.ok()) {
        return s;
      }
    }
    if (rch.has_max_publishers) {
      if (absl::Status s = channel->ValidateOrSetMaxPublishers(
              rch.max_publishers, /*set_if_missing=*/true, "shadow recovery");
          !s.ok()) {
        return s;
      }
    }
    if (rch.has_max_subscribers) {
      if (absl::Status s = channel->ValidateOrSetMaxSubscribers(
              rch.max_subscribers, /*set_if_missing=*/true,
              "shadow recovery");
          !s.ok()) {
        return s;
      }
    }
    if (rch.has_max_slot_size) {
      if (absl::Status s = channel->ValidateOrSetMaxSlotSize(
              rch.max_slot_size, /*set_if_missing=*/true, "shadow recovery");
          !s.ok()) {
        return s;
      }
    }

    if (map_storage) {
      if (absl::Status s = channel->MapExisting(
              scb_fd_, std::move(rch.ccb_fd), std::move(rch.bcb_fd));
          !s.ok()) {
        return s;
      }
      for (RegisteredClientBuffer &buffer : rch.client_buffers) {
        channel->RegisterClientBuffer(std::move(buffer.metadata),
                                      std::move(buffer.fd));
      }
    }
    return absl::OkStatus();
  };

  auto restore_users = [this](ServerChannel *channel, RecoveredChannel &rch) {
    for (auto &rpub : rch.publishers) {
      if (channel->IsTelemetryChannel()) {
        // Telemetry publishers are ephemeral and recreated by their coroutine.
        continue;
      }
      auto pub = std::make_unique<PublisherUser>(
          nullptr, rpub.id, rpub.is_reliable, rpub.is_local, rpub.is_bridge,
          rpub.for_tunnel, rpub.is_fixed_size,
          rpub.max_outstanding_slot_leases);
      pub->SetProcessId(rpub.process_id);

      toolbelt::TriggerFd tfd(rpub.poll_fd, rpub.trigger_fd);
      pub->SetTriggerFd(std::move(tfd));

      if (rpub.notify_retirement) {
        toolbelt::Pipe pipe;
        pipe.ReadFd() = std::move(rpub.retirement_read_fd);
        pipe.WriteFd() = std::move(rpub.retirement_write_fd);
        pub->SetRetirementPipe(std::move(pipe));
      }

      channel->AddUser(rpub.id, std::move(pub));
      channel->ClearPublisherQueueHazardIfDead(rpub.id, rpub.process_id);
    }

    for (auto &rsub : rch.subscribers) {
      auto sub = std::make_unique<SubscriberUser>(
          nullptr, rsub.id, rsub.is_reliable, rsub.is_bridge, rsub.for_tunnel,
          rsub.max_active_messages, rsub.subscriber_queue_size, rsub.is_local);
      sub->SetProcessId(rsub.process_id);

      toolbelt::TriggerFd tfd(rsub.trigger_fd, rsub.poll_fd);
      sub->SetTriggerFd(std::move(tfd));

      channel->AddUser(rsub.id, std::move(sub));
    }
    for (const std::string &warning :
         channel->RegisterExistingSubscribers()) {
      logger_.Log(toolbelt::LogLevel::kWarning, "%s", warning.c_str());
    }
  };

  absl::flat_hash_set<std::string> mux_names;
  for (const RecoveredChannel &rch : state.channels) {
    if (!rch.mux.empty()) {
      mux_names.insert(rch.mux);
    }
  }

  absl::flat_hash_map<std::string, ChannelMultiplexer *> recovered_muxes;
  absl::flat_hash_map<int, std::string> physical_channel_ids;

  // Recover physical channels first so virtual channels can attach to their
  // single shared CCB/BCB mapping.
  for (RecoveredChannel &rch : state.channels) {
    if (!rch.mux.empty()) {
      continue;
    }
    if (channels_.contains(rch.name) ||
        physical_channel_ids.contains(rch.channel_id)) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "duplicate recovered physical channel mapping for %s (id=%d)",
          rch.name, rch.channel_id));
    }
    physical_channel_ids.emplace(rch.channel_id, rch.name);
    channel_ids_.Set(rch.channel_id);
    ServerChannel *channel = nullptr;
    if (mux_names.contains(rch.name)) {
      auto *mux = new ChannelMultiplexer(
          rch.channel_id, rch.name, rch.num_slots,
          rch.subscriber_queue_arena_size == 0 ? 0
                                               : kDefaultSubscriberQueueSize,
          rch.subscriber_queue_arena_size, rch.type, session_id_, logger_);
      channel = mux;
      recovered_muxes.emplace(rch.name, mux);
    } else {
      channel = new ServerChannel(
          rch.channel_id, rch.name, rch.num_slots,
          rch.subscriber_queue_arena_size == 0 ? 0
                                               : kDefaultSubscriberQueueSize,
          rch.subscriber_queue_arena_size, rch.type, false, session_id_,
          logger_);
    }
    if (absl::Status status = configure_channel(channel, rch, true);
        !status.ok()) {
      delete channel;
      return status;
    }
    restore_users(channel, rch);
    channels_.emplace(rch.name, channel);
    if (channel->IsTelemetryChannel()) {
      num_telemetry_channels_.fetch_add(1, std::memory_order_relaxed);
    }
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Recovered channel '%s' (id=%d, %d pubs, %d subs)",
                rch.name.c_str(), rch.channel_id,
                static_cast<int>(rch.publishers.size()),
                static_cast<int>(rch.subscribers.size()));
  }

  // Older shadow state may contain only virtual-channel records. In that case
  // infer and create the physical mux from the first virtual record.
  for (RecoveredChannel &rch : state.channels) {
    if (rch.mux.empty()) {
      continue;
    }
    ChannelMultiplexer *mux = nullptr;
    auto mux_it = recovered_muxes.find(rch.mux);
    if (mux_it == recovered_muxes.end()) {
      if (channels_.contains(rch.mux) ||
          physical_channel_ids.contains(rch.channel_id)) {
        return absl::FailedPreconditionError(absl::StrFormat(
            "duplicate recovered mux mapping for %s (id=%d)", rch.mux,
            rch.channel_id));
      }
      physical_channel_ids.emplace(rch.channel_id, rch.mux);
      channel_ids_.Set(rch.channel_id);
      mux = new ChannelMultiplexer(
          rch.channel_id, rch.mux, rch.num_slots,
          rch.subscriber_queue_arena_size == 0 ? 0
                                               : kDefaultSubscriberQueueSize,
          rch.subscriber_queue_arena_size, rch.type, session_id_, logger_);
      if (absl::Status status = configure_channel(mux, rch, true);
          !status.ok()) {
        delete mux;
        return status;
      }
      channels_.emplace(rch.mux, mux);
      recovered_muxes.emplace(rch.mux, mux);
    } else {
      mux = mux_it->second;
      if (rch.channel_id != mux->GetChannelId() ||
          rch.num_slots != mux->NumSlots() ||
          rch.subscriber_queue_arena_size !=
              mux->SubscriberQueueArenaSize()) {
        return absl::FailedPreconditionError(absl::StrFormat(
            "inconsistent recovered virtual channel %s for mux %s", rch.name,
            rch.mux));
      }
    }

    if (channels_.contains(rch.name)) {
      return absl::FailedPreconditionError(
          absl::StrFormat("duplicate recovered virtual channel %s", rch.name));
    }
    absl::StatusOr<std::unique_ptr<VirtualChannel>> recovered_vchan =
        mux->CreateVirtualChannel(*this, rch.name, rch.vchan_id);
    if (!recovered_vchan.ok()) {
      return recovered_vchan.status();
    }
    VirtualChannel *vchan = recovered_vchan->get();
    if (absl::Status status = configure_channel(vchan, rch, false);
        !status.ok()) {
      return status;
    }
    restore_users(vchan, rch);
    for (const auto &entry : vchan->GetUsers()) {
      mux->AddUserId(entry.first);
    }
    channels_.emplace(rch.name, std::move(*recovered_vchan));
    logger_.Log(toolbelt::LogLevel::kInfo,
                "Recovered virtual channel '%s' on mux '%s' "
                "(%d pubs, %d subs)",
                rch.name.c_str(), rch.mux.c_str(),
                static_cast<int>(rch.publishers.size()),
                static_cast<int>(rch.subscribers.size()));
  }
  for (auto &[name, channel] : channels_) {
    (void)name;
    if (!channel->IsVirtual()) {
      if (absl::Status status = channel->ReconcileSubscriberQueueArena();
          !status.ok()) {
        return status;
      }
    }
  }
  for (auto &[name, channel] : channels_) {
    if (!channel->IsTelemetryChannel()) {
      continue;
    }
    // Rebuild runtime-only state from the recovered hidden channel and users.
    auto state = std::make_shared<TelemetryState>();
    state->hidden_channel_name = name;
    state->snapshot_requested = true;
    if (absl::Status status = state->trigger.Open(); !status.ok()) {
      return status;
    }
    for (const auto &[id, user] : channel->GetUsers()) {
      (void)id;
      if (user != nullptr && user->IsSubscriber()) {
        ++state->subscriber_count;
      }
    }
    if (ServerChannel *target = FindChannel(channel->TelemetryTarget());
        IsPublicChannel(target)) {
      state->target_channel_id = target->GetChannelId();
      uint64_t total_bytes = 0;
      uint64_t total_messages = 0;
      uint64_t max_message_size = 0;
      target->GetStatsCounters(total_bytes, total_messages, max_message_size,
                               state->last_total_drops);
      state->resize_count = target->GetResizeInfo().size();
    }
    telemetry_states_[channel->TelemetryTarget()] = std::move(state);
  }
  return absl::OkStatus();
}

void Server::SetShadowSocket(const std::string &socket_name) {
  SetShadowSockets(socket_name, "");
}

void Server::SetShadowSockets(const std::string &primary,
                              const std::string &secondary) {
  if (!primary.empty()) {
    primary_shadow_replicator_ =
        std::make_unique<ShadowReplicator>(primary, logger_);
  }
  if (!secondary.empty()) {
    secondary_shadow_replicator_ =
        std::make_unique<ShadowReplicator>(secondary, logger_);
  }
}

} // namespace subspace
