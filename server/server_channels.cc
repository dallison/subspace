// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "proto/subspace.pb.h"
#include "server/client_handler.h"
#include "server/tunnel.h"
#include "toolbelt/clock.h"
#include <vector>

namespace subspace {

void Server::ForeachChannel(std::function<void(ServerChannel *)> func) {
  for (auto &channel : channels_) {
    if (IsPublicChannel(channel.second.get())) {
      func(channel.second.get());
    }
  }
}

absl::StatusOr<ServerChannel *>
Server::CreateMultiplexer(const std::string &channel_name, int64_t slot_size,
                          int num_slots,
                          uint64_t subscriber_queue_arena_size,
                          std::string type, int32_t checksum_size,
                          int32_t metadata_size) {
  const int subscriber_queue_size =
      subscriber_queue_arena_size == 0 ? 0 : kDefaultSubscriberQueueSize;
  absl::StatusOr<int> channel_id = channel_ids_.Allocate("mux");
  if (!channel_id.ok()) {
    return channel_id.status();
  }
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Creating multiplexer %s with %d slots", channel_name.c_str(),
              num_slots);
  ServerChannel *channel = new ChannelMultiplexer(
      *channel_id, channel_name, num_slots, subscriber_queue_size,
      subscriber_queue_arena_size, std::move(type), session_id_, logger_);
  channel->SetDebug(logger_.GetLogLevel() <= toolbelt::LogLevel::kVerboseDebug);
  // Establish the prefix layout before allocating.  Allocation is what makes
  // the channel visible: it is immediately followed by replication to the
  // shadows, which copy the checksum and metadata sizes off the channel and
  // are never sent them again.
  channel->SetPrefixLayout(checksum_size, metadata_size);

  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots,
                        subscriber_queue_arena_size, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  channels_.emplace(std::make_pair(channel_name, channel));
  OnNewChannel(channel_name);
  ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
    s->SendCreateChannel(channel);
  });
  return channel;
}

absl::StatusOr<ServerChannel *>
Server::CreateChannel(const std::string &channel_name, int64_t slot_size,
                      int num_slots, uint64_t subscriber_queue_arena_size,
                      const std::string &mux, int vchan_id, std::string type,
                      bool hidden, std::string telemetry_target,
                      int32_t checksum_size, int32_t metadata_size) {
  const int subscriber_queue_size =
      subscriber_queue_arena_size == 0 ? 0 : kDefaultSubscriberQueueSize;
  if (!mux.empty()) {
    ServerChannel *mux_channel = FindChannel(mux);
    if (mux_channel == nullptr) {
      // No mux found, create one.  A subscriber-first request creates it as
      // a placeholder and passes the default layout, which is what a
      // placeholder should carry until a publisher establishes the real one.
      absl::StatusOr<ServerChannel *> m =
          CreateMultiplexer(mux, slot_size, num_slots,
                            subscriber_queue_arena_size, type, checksum_size,
                            metadata_size);
      if (!m.ok()) {
        return m.status();
      }
      mux_channel = *m;
    }
    if (!mux_channel->IsMux()) {
      return absl::InternalError(
          absl::StrFormat("Channel %s is not a multiplexer", mux));
    }
    if (!mux_channel->IsPlaceholder() && num_slots > 0 &&
        subscriber_queue_arena_size !=
            mux_channel->SubscriberQueueArenaSize()) {
      return absl::InternalError(absl::StrFormat(
          "Inconsistent publisher parameters for mux %s: subscriber queue "
          "arena size is %llu, not %llu",
          mux,
          static_cast<unsigned long long>(
              mux_channel->SubscriberQueueArenaSize()),
          static_cast<unsigned long long>(subscriber_queue_arena_size)));
    }
    // A request with no slots and no slot size is a subscriber attaching
    // before any publisher exists.  It carries no layout of its own and
    // inherits whatever the mux eventually settles on, so it must leave the
    // mux as a placeholder: re-running Allocate() here would tear down and
    // recreate the CCB/BCB underneath the subscribers already attached to
    // this mux, for no gain.
    const bool inherit_layout = (slot_size == 0 && num_slots == 0);
    if (mux_channel->IsPlaceholder() && !inherit_layout) {
      // Install the publisher's prefix layout *before* allocating, so the
      // mux is never observable as allocated-but-stale.  Allocation makes
      // the mux non-placeholder and notifies shadow replicas, and both
      // shadows and concurrently connecting subscribers read the checksum
      // and metadata sizes straight off the mux; if they run first they
      // would latch the placeholder's 4/0 defaults instead of the layout
      // this publisher is about to establish.  Virtual channels forward
      // their layout accessors to the mux, so this covers the siblings a
      // subscriber may already have created.
      const int32_t previous_checksum_size = mux_channel->ChecksumSize();
      const int32_t previous_metadata_size = mux_channel->MetadataSize();
      const int32_t previous_prefix_size = mux_channel->PrefixSize();
      // 4/0 are the sizes a channel is constructed with and mean "no layout
      // established yet".  A placeholder mux that already carries a real
      // layout (one rebuilt by shadow recovery, for instance) keeps it; a
      // publisher that disagrees is rejected by the consistency check in
      // ClientHandler::HandleCreatePublisher rather than silently
      // overwriting it here.
      if (previous_checksum_size == 4 && previous_metadata_size == 0) {
        mux_channel->SetPrefixLayout(checksum_size, metadata_size);
      }
      // Remap the memory now that we know the slots.
      absl::Status status =
          RemapChannel(mux_channel, slot_size, num_slots,
                       subscriber_queue_arena_size);
      if (!status.ok()) {
        // Allocation failed and left the mux a placeholder again; roll the
        // layout back too so the next publisher can retry the promotion
        // from a clean state rather than being rejected for disagreeing
        // with a layout that was never actually established.
        mux_channel->SetChecksumSize(previous_checksum_size);
        mux_channel->SetMetadataSize(previous_metadata_size);
        mux_channel->SetPrefixSize(previous_prefix_size);
        return status;
      }
    }
    // Create a virtual channel associated with the multiplexer.
    ChannelMultiplexer *m = static_cast<ChannelMultiplexer *>(mux_channel);
    absl::StatusOr<std::unique_ptr<VirtualChannel>> vchan =
        m->CreateVirtualChannel(*this, channel_name, vchan_id);
    if (!vchan.ok()) {
      return vchan.status();
    }
    ServerChannel *channel = vchan->get();

    logger_.Log(toolbelt::LogLevel::kDebug,
                "Creating virtual channel %s with %d slots using mux %s",
                channel_name.c_str(), num_slots, mux.c_str());
    // The channels_ map owns all the server channels.
    channels_.emplace(std::make_pair(channel_name, std::move(*vchan)));
    // A virtual channel shares its multiplexer's counters, which already hold
    // traffic from before the virtual channel existed.
    ChannelStatsProto mux_stats;
    channel->GetChannelStats(&mux_stats);
    statistics_totals_[channel_name] = {
        .bytes = uint64_t(mux_stats.total_bytes()),
        .messages = uint64_t(mux_stats.total_messages()),
        .drops = mux_stats.total_drops()};
    OnNewChannel(channel_name);
    ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
      s->SendCreateChannel(channel);
    });
    return channel;
  }

  absl::StatusOr<int> channel_id = channel_ids_.Allocate("channel");
  if (!channel_id.ok()) {
    return channel_id.status();
  }
  ServerChannel *channel =
      new ServerChannel(*channel_id, channel_name, num_slots,
                        subscriber_queue_size, subscriber_queue_arena_size,
                        std::move(type), false, session_id_, logger_);
  if (hidden) {
    channel->SetTelemetryTarget(std::move(telemetry_target));
  }
  channel->SetDebug(logger_.GetLogLevel() <= toolbelt::LogLevel::kVerboseDebug);
  channel->SetLastKnownSlotSize(slot_size);
  // As in CreateMultiplexer: the layout has to be on the channel before the
  // allocation that publishes it to the shadows.
  channel->SetPrefixLayout(checksum_size, metadata_size);

  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots,
                        subscriber_queue_arena_size, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  channels_.emplace(std::make_pair(channel_name, channel));
  if (channel->IsTelemetryChannel()) {
    num_telemetry_channels_.fetch_add(1, std::memory_order_relaxed);
  }
  OnNewChannel(channel_name);
  ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
    s->SendCreateChannel(channel);
  });

  return channel;
}

uint64_t Server::GetVirtualMemoryUsage() const {
  uint64_t size = 0;
  for (const auto &channel : channels_) {
    size += channel.second->GetVirtualMemoryUsage();
  }
  return size;
}

absl::Status Server::RemapChannel(ServerChannel *channel, int64_t slot_size,
                                  int num_slots,
                                  uint64_t subscriber_queue_arena_size) {
  if (channel->IsVirtual()) {
    ChannelMultiplexer *mux = static_cast<VirtualChannel *>(channel)->GetMux();
    logger_.Log(toolbelt::LogLevel::kDebug,
                "Remapping multiplexer %s with %d slots",
                channel->Name().c_str(), num_slots);
    return RemapChannel(mux, slot_size, num_slots,
                        subscriber_queue_arena_size);
  }
  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots,
                        subscriber_queue_arena_size, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetLastKnownSlotSize(slot_size);
  channel->SetSharedMemoryFds(std::move(*fds));
  for (const std::string &warning : channel->RegisterExistingSubscribers()) {
    logger_.Log(toolbelt::LogLevel::kWarning, "%s", warning.c_str());
  }
  // Remapping replaces the CCB/BCB FDs; shadow recovery must receive the
  // refreshed descriptors instead of retaining the placeholder mappings.
  ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
    s->SendCreateChannel(channel);
  });
  return absl::OkStatus();
}

ServerChannel *Server::FindChannel(const std::string &channel_name) {
  auto it = channels_.find(channel_name);
  if (it == channels_.end()) {
    return nullptr;
  }
  return it->second.get();
}

void Server::RemoveChannel(ServerChannel *channel) {
  const bool telemetry = channel->IsTelemetryChannel();
  const std::string telemetry_target =
      telemetry ? channel->TelemetryTarget() : std::string();
  if (telemetry) {
    auto it = telemetry_states_.find(telemetry_target);
    if (it != telemetry_states_.end() &&
        it->second->hidden_channel_name == channel->Name()) {
      telemetry_states_.erase(it);
    }
  }
  OnRemoveChannel(channel->Name());
  statistics_totals_.erase(channel->Name());
  ForEachShadow([channel](const std::unique_ptr<ShadowReplicator> &s) {
    s->SendRemoveChannel(channel->Name(), channel->GetChannelId());
  });
  if (!simulate_crash_) {
    channel->RemoveBuffer(session_id_, this);
  }
  channel_ids_.Clear(channel->GetChannelId());
  auto it = channels_.find(channel->Name());
  if (it == channels_.end()) {
    return;
  }
  if (it->second->IsVirtual()) {
    auto vchan = static_cast<VirtualChannel *>(it->second.get());
    ChannelMultiplexer *mux = vchan->GetMux();
    mux->RemoveVirtualChannel(vchan);
    if (mux->IsEmpty()) {
      RemoveChannel(mux);
    }
  }
  channels_.erase(it);
  if (telemetry) {
    num_telemetry_channels_.fetch_sub(1, std::memory_order_relaxed);
  }
  SendChannelDirectory();
}

void Server::RemoveAllUsersFor(ClientHandler *handler) {
  std::vector<ServerChannel *> empty_channels;
  for (auto &channel : channels_) {
    channel.second->RemoveAllUsersFor(this, handler);
    if (channel.second->IsEmpty() && !channel.second->IsTelemetryChannel()) {
      empty_channels.push_back(channel.second.get());
    }
  }
  // toolbelt::Now remove all empty channels.
  for (auto *channel : empty_channels) {
    RemoveChannel(channel);
  }
}

void Server::ChannelDirectoryCoroutine(async::Context ctx) {
  // Coroutine aware client.
  Client client(ctx);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(
        toolbelt::LogLevel::kError,
        "Failed to initialize Subspace client for channel directory: %s",
        status.ToString().c_str());
    return;
  }
  constexpr int kDirectorySlotSize = 1024;
  constexpr int kDirectoryNumSlots = 32;

  absl::StatusOr<Publisher> channel_directory = client.CreatePublisher(
      "/subspace/ChannelDirectory", kDirectorySlotSize, kDirectoryNumSlots,
      PublisherOptions()
          .SetType("subspace.ChannelDirectory")
          .SetUseSplitBuffers(false));
  if (!channel_directory.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create channel directory channel: %s",
                channel_directory.status().ToString().c_str());
    return;
  }
  while (!shutting_down_) {
    (void)async::WaitReadable(ctx,
                              channel_directory_trigger_fd_.GetPollFd().Fd());
    channel_directory_trigger_fd_.Clear();

    ChannelDirectory directory;
    directory.set_server_id(server_id_);
    for (auto &channel : channels_) {
      if (!IsPublicChannel(channel.second.get())) {
        continue;
      }
      auto info = directory.add_channels();
      channel.second->GetChannelInfo(info);
    }
    int64_t length = directory.ByteSizeLong();
    absl::StatusOr<void *> buffer =
        channel_directory->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get channel directory buffer: %s",
                  buffer.status().ToString().c_str());
      return;
    }
    bool ok = directory.SerializeToArray(*buffer, length);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel directory");
      continue;
    }
    absl::StatusOr<const Message> s = channel_directory->PublishMessage(length);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish channel directory: %s",
                  s.status().ToString().c_str());
    }
  }
}

void Server::SendChannelDirectory() {
  channel_directory_trigger_fd_.Trigger();
  if (tunnel_ != nullptr) {
    tunnel_->ChannelsChanged();
  }
}

void Server::StatisticsCoroutine(async::Context ctx) {
  Client client(ctx);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to initialize Subspace client for statistics: %s",
                status.ToString().c_str());
    return;
  }
  constexpr int kStatsSlotSize = 1024;
  constexpr int kStatsNumSlots = 32;

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "/subspace/Statistics", kStatsSlotSize, kStatsNumSlots,
      PublisherOptions()
          .SetType("subspace.Statistics")
          .SetUseSplitBuffers(false));
  if (!pub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create statistics channel: %s",
                pub.status().ToString().c_str());
    return;
  }

  auto delivered_statistics_messages = [this]() -> uint64_t {
    auto it = channels_.find("/subspace/Statistics");
    if (it == channels_.end()) {
      return 0;
    }
    ChannelStatsProto stats;
    it->second->GetChannelStats(&stats);
    return uint64_t(stats.total_messages());
  };

  constexpr int kPeriodSecs = 2;
  uint64_t sequence = 0;
  while (!shutting_down_) {
    async::Sleep(ctx, kPeriodSecs);
    Statistics stats;
    stats.set_timestamp(toolbelt::Now());
    stats.set_server_id(server_id_);
    stats.set_machine_name(machine_name_);
    stats.set_sequence(sequence);
    absl::flat_hash_map<std::string, StatisticsTotals> totals;
    for (auto &channel : channels_) {
      if (!IsPublicChannel(channel.second.get())) {
        continue;
      }
      auto s = stats.add_channels();
      channel.second->GetChannelStats(s);
      StatisticsTotals current = {.bytes = uint64_t(s->total_bytes()),
                                  .messages = uint64_t(s->total_messages()),
                                  .drops = s->total_drops()};
      StatisticsTotals interval = current;
      // RemoveChannel forgets a removed channel, so a channel created again
      // under the same name counts from its creation.  The 64-bit counters
      // never wrap, so one that went backwards belongs to a channel that was
      // replaced some other way and also counts from its creation.  The
      // 32-bit drop counter wraps, so its count is taken modulo 2^32.
      auto it = statistics_totals_.find(s->channel_name());
      if (it != statistics_totals_.end() &&
          current.bytes >= it->second.bytes &&
          current.messages >= it->second.messages) {
        interval = {.bytes = current.bytes - it->second.bytes,
                    .messages = current.messages - it->second.messages,
                    .drops = uint32_t(current.drops - it->second.drops)};
      }
      totals[s->channel_name()] = current;
      s->set_total_bytes(int64_t(interval.bytes));
      s->set_total_messages(int64_t(interval.messages));
      s->set_total_drops(interval.drops);
    }
    int64_t length = stats.ByteSizeLong();

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get channel stats buffer: %s",
                  buffer.status().ToString().c_str());
      return;
    }
    bool ok = stats.SerializeToArray(*buffer, length);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel stats");
      continue;
    }
    const uint64_t delivered_before = delivered_statistics_messages();
    absl::StatusOr<const Message> s = pub->PublishMessage(length);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish statistics: %s",
                  s.status().ToString().c_str());
    }
    // PublishMessage can fail after it delivered the message, so the
    // channel's message count decides whether this interval was delivered.
    // Until then the previous totals stay, so the next message also counts
    // this interval.
    if (delivered_statistics_messages() <= delivered_before) {
      continue;
    }
    // A virtual channel created while this message was being published
    // keeps the start totals CreateChannel gave it.
    for (auto &[name, previous] : statistics_totals_) {
      if (!totals.contains(name) && channels_.contains(name)) {
        totals.emplace(name, previous);
      }
    }
    statistics_totals_ = std::move(totals);
    ++sequence;
  }
}

} // namespace subspace
