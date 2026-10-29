// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "proto/subspace.pb.h"
#include <chrono>
#include <limits>
#include <optional>
#include <vector>

namespace subspace {

static constexpr int kTelemetrySlotSize = 1024;
static constexpr int kTelemetryNumSlots = 32;

uint64_t Server::TelemetryParticipantKey(const User *user) {
  // Publisher and subscriber ids are allocated independently.
  return (uint64_t(user->IsPublisher()) << 32) | uint32_t(user->GetId());
}

ServerChannel *Server::FindTelemetryChannel(const std::string &target_name) {
  auto it = telemetry_states_.find(target_name);
  if (it == telemetry_states_.end()) {
    return nullptr;
  }
  ServerChannel *channel = FindChannel(it->second->hidden_channel_name);
  if (channel == nullptr || !channel->IsTelemetryChannel() ||
      channel->TelemetryTarget() != target_name) {
    return nullptr;
  }
  return channel;
}

absl::StatusOr<ServerChannel *>
Server::FindOrCreateTelemetryChannel(const std::string &target_name) {
  if (ServerChannel *channel = FindTelemetryChannel(target_name);
      channel != nullptr) {
    return channel;
  }

  ServerChannel *target = FindChannel(target_name);
  if (!IsPublicChannel(target)) {
    return absl::NotFoundError(
        absl::StrFormat("No such channel %s", target_name));
  }

  // The name is internal; add a suffix if a real channel already uses it.
  std::string hidden_name = "/subspace/telemetry/" + target_name;
  for (int suffix = 1; FindChannel(hidden_name) != nullptr; ++suffix) {
    hidden_name =
        absl::StrFormat("/subspace/telemetry/%s#%d", target_name, suffix);
  }

  absl::StatusOr<ServerChannel *> hidden =
      CreateChannel(hidden_name, kTelemetrySlotSize, kTelemetryNumSlots, 0, "",
                    -1, "subspace.Telemetry", /*hidden=*/true, target_name);
  if (!hidden.ok()) {
    return hidden.status();
  }

  auto state = std::make_shared<TelemetryState>();
  state->hidden_channel_name = hidden_name;
  state->snapshot_requested = true;
  state->target_channel_id = target->GetChannelId();
  if (absl::Status status = state->trigger.Open(); !status.ok()) {
    RemoveChannel(*hidden);
    return status;
  }
  uint64_t total_bytes = 0;
  uint64_t total_messages = 0;
  uint64_t max_message_size = 0;
  // Subsequent reports contain deltas from these initial counters.
  target->GetStatsCounters(total_bytes, total_messages, max_message_size,
                           state->last_total_drops);
  state->resize_count = target->GetResizeInfo().size();
  telemetry_states_.emplace(target_name, std::move(state));
  return *hidden;
}

void Server::TelemetrySubscriberAdded(ServerChannel *channel) {
  if (channel == nullptr || !channel->IsTelemetryChannel()) {
    return;
  }
  auto it = telemetry_states_.find(channel->TelemetryTarget());
  if (it == telemetry_states_.end()) {
    return;
  }
  std::shared_ptr<TelemetryState> state = it->second;
  ++state->subscriber_count;
  state->snapshot_requested = true;
  // The first subscriber starts the per-target coroutine and publisher.
  if (!state->coroutine_running) {
    StartTelemetryCoroutine(channel->TelemetryTarget(), state);
  }
  state->trigger.Trigger();
}

void Server::TelemetrySubscriberRemoved(ServerChannel *channel) {
  if (channel == nullptr || !channel->IsTelemetryChannel()) {
    return;
  }
  auto it = telemetry_states_.find(channel->TelemetryTarget());
  if (it == telemetry_states_.end()) {
    return;
  }
  std::shared_ptr<TelemetryState> state = it->second;
  if (state->subscriber_count > 0) {
    --state->subscriber_count;
  }
  if (state->subscriber_count == 0) {
    // Wake the coroutine immediately so it can tear down its publisher.
    state->trigger.Trigger();
  }
}

void Server::RecordTelemetryParticipantChange(ServerChannel *channel,
                                              User *user, bool added) {
  if (!IsPublicChannel(channel) || user == nullptr) {
    return;
  }
  auto state_it = telemetry_states_.find(channel->Name());
  if (state_it == telemetry_states_.end()) {
    return;
  }

  TelemetryState &state = *state_it->second;
  uint64_t key = TelemetryParticipantKey(user);
  std::string name;
  if (user->GetHandler() != nullptr) {
    name = user->GetHandler()->ClientName();
  } else if (auto it = state.participants.find(key);
             it != state.participants.end()) {
    // Disconnect cleanup clears the handler before all removal paths run.
    name = it->second;
  }

  if (added) {
    state.participants[key] = name;
  } else {
    auto it = state.participants.find(key);
    if (it != state.participants.end()) {
      name = it->second;
      state.participants.erase(it);
    }
  }
  state.pending_participants.push_back(
      {.name = std::move(name),
       .publisher = user->IsPublisher(),
       .change = added ? Telemetry::ADDED : Telemetry::REMOVED});
}

void Server::QueueTelemetrySnapshot(const std::string &target_name,
                                    TelemetryState &state) {
  ServerChannel *target = FindChannel(target_name);
  if (!IsPublicChannel(target)) {
    return;
  }
  for (const auto &[id, user] : target->GetUsers()) {
    (void)id;
    if (user == nullptr || user->GetHandler() == nullptr) {
      continue;
    }
    uint64_t key = TelemetryParticipantKey(user.get());
    std::string name = user->GetHandler()->ClientName();
    state.participants[key] = name;
    state.pending_participants.push_back({.name = std::move(name),
                                          .publisher = user->IsPublisher(),
                                          .change = Telemetry::NONE});
  }
}

void Server::ReconcileTelemetryParticipants(const std::string &target_name,
                                            TelemetryState &state) {
  // Reconciliation catches disconnect and recovery paths that do not emit the
  // normal add/remove callbacks.
  absl::flat_hash_map<uint64_t, std::pair<std::string, bool>> current;
  ServerChannel *target = FindChannel(target_name);
  if (IsPublicChannel(target)) {
    for (const auto &[id, user] : target->GetUsers()) {
      (void)id;
      if (user == nullptr || user->GetHandler() == nullptr) {
        continue;
      }
      current.emplace(TelemetryParticipantKey(user.get()),
                      std::make_pair(user->GetHandler()->ClientName(),
                                     user->IsPublisher()));
    }
  }

  for (const auto &[key, participant] : current) {
    auto known = state.participants.find(key);
    if (known == state.participants.end()) {
      state.participants.emplace(key, participant.first);
      state.pending_participants.push_back({.name = participant.first,
                                            .publisher = participant.second,
                                            .change = Telemetry::ADDED});
    } else if (known->second != participant.first) {
      state.pending_participants.push_back({.name = known->second,
                                            .publisher = participant.second,
                                            .change = Telemetry::REMOVED});
      known->second = participant.first;
      state.pending_participants.push_back({.name = participant.first,
                                            .publisher = participant.second,
                                            .change = Telemetry::ADDED});
    }
  }

  for (auto it = state.participants.begin(); it != state.participants.end();) {
    if (current.contains(it->first)) {
      ++it;
      continue;
    }
    bool publisher = (it->first >> 32) != 0;
    state.pending_participants.push_back({.name = it->second,
                                          .publisher = publisher,
                                          .change = Telemetry::REMOVED});
    auto erase_it = it++;
    state.participants.erase(erase_it);
  }
}

void Server::StartTelemetryCoroutine(
    const std::string &target_name,
    const std::shared_ptr<TelemetryState> &state) {
  if (state->coroutine_running) {
    return;
  }
  state->coroutine_running = true;
  runtime_.SpawnOnStrand(
      [this, target_name, state](async::Context ctx) {
        TelemetryCoroutine(ctx, target_name, state);
      },
      {.name = absl::StrFormat("Channel telemetry for %s", target_name),
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
}

void Server::StartRecoveredTelemetryCoroutines() {
  for (const auto &[target_name, state] : telemetry_states_) {
    if (state->subscriber_count > 0) {
      StartTelemetryCoroutine(target_name, state);
      state->trigger.Trigger();
    }
  }
}

void Server::TelemetryCoroutine(async::Context ctx, std::string target_name,
                                std::shared_ptr<TelemetryState> state) {
  Client client(ctx);
  absl::Status status =
      client.Init(socket_name_, "subspace-telemetry-internal");
  if (!status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to initialize telemetry client for %s: %s",
                target_name.c_str(), status.ToString().c_str());
    state->coroutine_running = false;
    return;
  }

  // Keeping the publisher local ties its lifetime to this target's coroutine.
  std::optional<Publisher> publisher;
  while (!shutting_down_) {
    // Triggers handle lifecycle changes; timeout is the batching period.
    absl::Status wait_status = async::WaitReadable(
        ctx, state->trigger.GetPollFd().Fd(), std::chrono::seconds(1));
    const bool publish_batch = absl::IsDeadlineExceeded(wait_status);
    if (wait_status.ok()) {
      state->trigger.Clear();
    }
    if (shutting_down_) {
      break;
    }

    auto current = telemetry_states_.find(target_name);
    if (current == telemetry_states_.end() || current->second != state) {
      break;
    }

    if (state->subscriber_count == 0) {
      // Publisher destruction can yield, so recheck the shared state afterward
      // in case another subscriber arrived while it was being removed.
      publisher.reset();
      current = telemetry_states_.find(target_name);
      if (current == telemetry_states_.end() || current->second != state) {
        break;
      }
      if (state->subscriber_count == 0) {
        ServerChannel *hidden = FindChannel(state->hidden_channel_name);
        if (hidden != nullptr && hidden->IsEmpty()) {
          RemoveChannel(hidden);
        }
        break;
      }
      continue;
    }

    if (!publisher.has_value()) {
      absl::StatusOr<Publisher> created = client.CreatePublisher(
          state->hidden_channel_name, kTelemetrySlotSize, kTelemetryNumSlots,
          PublisherOptions()
              .SetType("subspace.Telemetry")
              .SetUseSplitBuffers(false));
      if (!created.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to create telemetry publisher for %s: %s",
                    target_name.c_str(), created.status().ToString().c_str());
        continue;
      }
      publisher.emplace(std::move(*created));

      current = telemetry_states_.find(target_name);
      if (current == telemetry_states_.end() || current->second != state) {
        break;
      }
      if (state->subscriber_count == 0) {
        continue;
      }
    }

    if (!publish_batch) {
      continue;
    }

    PublishTelemetryBatch(target_name, state, *publisher);
  }

  publisher.reset();
  auto current = telemetry_states_.find(target_name);
  if (current != telemetry_states_.end() && current->second == state) {
    state->coroutine_running = false;
  }
}

void Server::PublishTelemetryBatch(const std::string &target_name,
                                   const std::shared_ptr<TelemetryState> &state,
                                   Publisher &publisher) {
  if (state->snapshot_requested) {
    // A new watcher receives the current participants before later deltas.
    QueueTelemetrySnapshot(target_name, *state);
    state->snapshot_requested = false;
  }
  ReconcileTelemetryParticipants(target_name, *state);

  Telemetry telemetry;
  for (const PendingTelemetryParticipant &participant :
       state->pending_participants) {
    if (participant.publisher) {
      auto *entry = telemetry.add_publishers();
      entry->set_name(participant.name);
      entry->set_change(participant.change);
    } else {
      auto *entry = telemetry.add_subscribers();
      entry->set_name(participant.name);
      entry->set_change(participant.change);
    }
  }

  uint32_t current_drops = state->last_total_drops;
  size_t current_resize_count = state->resize_count;
  ServerChannel *target = FindChannel(target_name);
  if (IsPublicChannel(target)) {
    uint64_t total_bytes = 0;
    uint64_t total_messages = 0;
    uint64_t max_message_size = 0;
    target->GetStatsCounters(total_bytes, total_messages, max_message_size,
                             current_drops);
    std::vector<ResizeInfo> resize_info = target->GetResizeInfo();
    current_resize_count = resize_info.size();
    if (target->GetChannelId() != state->target_channel_id) {
      // A recreated target starts new counter baselines.
      state->target_channel_id = target->GetChannelId();
    } else {
      uint32_t drop_delta = current_drops - state->last_total_drops;
      while (drop_delta > 0) {
        uint32_t chunk =
            std::min(drop_delta, uint32_t(std::numeric_limits<int32_t>::max()));
        telemetry.add_drops()->set_num_drops(int32_t(chunk));
        drop_delta -= chunk;
      }
      for (size_t i = state->resize_count; i < resize_info.size(); ++i) {
        telemetry.add_resizes()->set_new_size(resize_info[i].new_slot_size);
      }
    }
  } else {
    // Force fresh baselines if the target is recreated before the next tick.
    state->target_channel_id = -1;
    current_drops = 0;
    current_resize_count = 0;
  }

  if (telemetry.publishers().empty() && telemetry.subscribers().empty() &&
      telemetry.drops().empty() && telemetry.resizes().empty()) {
    state->last_total_drops = current_drops;
    state->resize_count = current_resize_count;
    return;
  }

  // Publishing yields. Commit only the entries present before that yield;
  // callbacks may append newer changes while the message is sent.
  const std::string hidden_channel_name = state->hidden_channel_name;
  const size_t pending_participant_count = state->pending_participants.size();
  int64_t length = telemetry.ByteSizeLong();
  absl::StatusOr<void *> buffer = publisher.GetMessageBuffer(int32_t(length));
  if (!buffer.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to get telemetry buffer for %s: %s",
                target_name.c_str(), buffer.status().ToString().c_str());
    return;
  }
  if (!telemetry.SerializeToArray(*buffer, int(length))) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to serialize telemetry for %s", target_name.c_str());
    publisher.CancelPublish();
    return;
  }
  absl::StatusOr<const Message> published =
      publisher.PublishMessage(int32_t(length));
  if (!published.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to publish telemetry for %s: %s", target_name.c_str(),
                published.status().ToString().c_str());
    return;
  }

  auto current = telemetry_states_.find(target_name);
  if (current != telemetry_states_.end() && current->second == state &&
      state->hidden_channel_name == hidden_channel_name) {
    size_t erase_count =
        std::min(pending_participant_count, state->pending_participants.size());
    state->pending_participants.erase(
        state->pending_participants.begin(),
        state->pending_participants.begin() + erase_count);
    state->last_total_drops = current_drops;
    state->resize_count = current_resize_count;
  }
}

} // namespace subspace
