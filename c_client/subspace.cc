// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "c_client/subspace.h"
#include "client/checksum.h"
#include "client/client.h"
#include "client/message.h"
#include "client/options.h"
#include <array>
#include <chrono>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

struct ClientCache {
  std::vector<std::string> strings;
  std::vector<SubspaceChannelInfo> infos;
  std::vector<SubspaceChannelStats> stats;
};

struct HandleCache {
  std::string string;
  std::string dump;
  std::vector<SubspaceMessage> messages;
};

std::unordered_map<void *, ClientCache> client_caches;
std::unordered_map<void *, HandleCache> publisher_caches;
std::unordered_map<void *, HandleCache> subscriber_caches;

std::string StringFromPointer(const char *data, size_t length) {
  if (data == nullptr || length == 0) {
    return {};
  }
  return std::string(data, length);
}

std::shared_ptr<subspace::Client> *ClientPtr(SubspaceClient client) {
  return reinterpret_cast<std::shared_ptr<subspace::Client> *>(client.client);
}

std::shared_ptr<subspace::Publisher> *
PublisherPtr(SubspacePublisher publisher) {
  return reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
}

std::shared_ptr<subspace::Subscriber> *
SubscriberPtr(SubspaceSubscriber subscriber) {
  return reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
}

SubspaceString ToCString(const std::string &s) {
  return {.data = s.data(), .length = s.size()};
}

SubspaceChannelCounters ToCCounters(const subspace::ChannelCounters &counters) {
  return {
      .num_pub_updates = counters.num_pub_updates,
      .num_sub_updates = counters.num_sub_updates,
      .num_pubs = counters.num_pubs,
      .num_reliable_pubs = counters.num_reliable_pubs,
      .num_subs = counters.num_subs,
      .num_reliable_subs = counters.num_reliable_subs,
      .num_resizes = counters.num_resizes,
  };
}

SubspaceMessage EmptyMessage() {
  return {.message = nullptr,
          .length = 0,
          .buffer = nullptr,
          .ordinal = uint64_t(-1),
          .timestamp = 0,
          .vchan_id = -1,
          .is_activation = false,
          .slot_id = -1,
          .checksum_error = false};
}

SubspaceMessage ToCMessage(const subspace::Message &msg,
                           void *message_handle = nullptr) {
  return {.message = message_handle,
          .length = msg.length,
          .buffer = msg.buffer,
          .ordinal = msg.ordinal,
          .timestamp = msg.timestamp,
          .vchan_id = msg.vchan_id,
          .is_activation = msg.is_activation,
          .slot_id = msg.slot_id,
          .checksum_error = msg.checksum_error};
}

SubspaceMessage TakeCMessage(subspace::Message &&msg) {
  subspace::Message *stored = new subspace::Message(std::move(msg));
  auto *handle = new std::shared_ptr<subspace::Message>(stored);
  return ToCMessage(*stored, handle);
}

void FreeMessageStorage(SubspaceMessage *message) {
  if (message == nullptr || message->message == nullptr) {
    return;
  }
  auto msg_ptr =
      reinterpret_cast<std::shared_ptr<subspace::Message> *>(message->message);
  delete msg_ptr;
  *message = EmptyMessage();
}

SubspaceChannelInfo ToCChannelInfo(const subspace::ChannelInfo &info,
                                   const std::string &channel_name,
                                   const std::string &type) {
  return {.channel_name = ToCString(channel_name),
          .num_publishers = info.num_publishers,
          .num_subscribers = info.num_subscribers,
          .num_bridge_pubs = info.num_bridge_pubs,
          .num_bridge_subs = info.num_bridge_subs,
          .num_tunnel_pubs = info.num_tunnel_pubs,
          .num_tunnel_subs = info.num_tunnel_subs,
          .type = ToCString(type),
          .slot_size = info.slot_size,
          .num_slots = info.num_slots,
          .reliable = info.reliable};
}

SubspaceChannelStats ToCChannelStats(const subspace::ChannelStats &stats,
                                     const std::string &channel_name) {
  return {.channel_name = ToCString(channel_name),
          .total_bytes = stats.total_bytes,
          .total_messages = stats.total_messages,
          .max_message_size = stats.max_message_size};
}

subspace::ReadMode ToCppReadMode(SubspaceReadMode mode) {
  return mode == kSubspaceReadNewest ? subspace::ReadMode::kReadNewest
                                     : subspace::ReadMode::kReadNext;
}

subspace::ChecksumCallback
ToCppChecksumCallback(SubspaceChecksumCallback callback, void *user_data) {
  return [callback,
          user_data](const std::array<absl::Span<const uint8_t>, 3> &data,
                     absl::Span<std::byte> checksum) {
    SubspaceChecksumSpan spans[3];
    for (size_t i = 0; i < data.size(); i++) {
      spans[i] = {
          .data = data[i].data(),
          .size = data[i].size(),
      };
    }
    callback(spans, data.size(), reinterpret_cast<uint8_t *>(checksum.data()),
             checksum.size(), user_data);
  };
}

SubspaceSplitBufferInfo ToCInfo(const subspace::SplitBufferMetadata &metadata) {
  return {
      .channel_name = metadata.channel_name.c_str(),
      .session_id = metadata.session_id,
      .buffer_index = metadata.buffer_index,
      .slot_id = metadata.slot_id,
      .is_prefix = metadata.is_prefix,
      .full_size = metadata.full_size,
      .allocation_size = metadata.allocation_size,
      .handle = metadata.handle,
  };
}

SubspaceSplitBufferMapping
ToCMapping(const subspace::SplitBufferMapping &mapping) {
  return {.handle = mapping.handle,
          .address = mapping.address,
          .size = mapping.size,
          .private_data = mapping.private_data};
}

subspace::SplitBufferMapping
ToCppMapping(const SubspaceSplitBufferMapping &mapping) {
  return {.handle = mapping.handle,
          .address = mapping.address,
          .size = mapping.size,
          .private_data = mapping.private_data};
}

subspace::SplitBufferCallbacks
ToCppSplitCallbacks(SubspaceSplitBufferCallbacks callbacks) {
  subspace::SplitBufferCallbacks cpp_callbacks;
  if (callbacks.allocate != nullptr) {
    cpp_callbacks.allocate =
        [callbacks](const subspace::SplitBufferMetadata &metadata)
        -> absl::StatusOr<subspace::SplitBufferMapping> {
      SubspaceSplitBufferInfo info = ToCInfo(metadata);
      SubspaceSplitBufferMapping mapping = {};
      if (!callbacks.allocate(&info, &mapping, callbacks.user_data)) {
        return absl::InternalError("Split-buffer allocation callback failed");
      }
      return ToCppMapping(mapping);
    };
  }
  if (callbacks.map != nullptr) {
    cpp_callbacks.map =
        [callbacks](const subspace::SplitBufferMetadata &metadata)
        -> absl::StatusOr<subspace::SplitBufferMapping> {
      SubspaceSplitBufferInfo info = ToCInfo(metadata);
      SubspaceSplitBufferMapping mapping = {};
      mapping.handle = metadata.handle;
      if (!callbacks.map(&info, &mapping, callbacks.user_data)) {
        return absl::InternalError("Split-buffer map callback failed");
      }
      return ToCppMapping(mapping);
    };
  }
  if (callbacks.unmap != nullptr) {
    cpp_callbacks.unmap =
        [callbacks](
            const subspace::SplitBufferMetadata &metadata,
            const subspace::SplitBufferMapping &mapping) -> absl::Status {
      SubspaceSplitBufferInfo info = ToCInfo(metadata);
      SubspaceSplitBufferMapping c_mapping = ToCMapping(mapping);
      if (!callbacks.unmap(&info, &c_mapping, callbacks.user_data)) {
        return absl::InternalError("Split-buffer unmap callback failed");
      }
      return absl::OkStatus();
    };
  }
  if (callbacks.free != nullptr) {
    cpp_callbacks.free =
        [callbacks](
            const subspace::SplitBufferMetadata &metadata,
            const subspace::SplitBufferMapping &mapping) -> absl::Status {
      SubspaceSplitBufferInfo info = ToCInfo(metadata);
      SubspaceSplitBufferMapping c_mapping = ToCMapping(mapping);
      if (!callbacks.free(&info, &c_mapping, callbacks.user_data)) {
        return absl::InternalError("Split-buffer free callback failed");
      }
      return absl::OkStatus();
    };
  }
  return cpp_callbacks;
}

} // namespace

#if defined(__cplusplus)
extern "C" {
#endif

thread_local SubspaceError subspace_error = {};

void subspace_clear_error(void) { subspace_error.error_message[0] = '\0'; }

void subspace_set_error(const char *error_message) {
  if (error_message == nullptr) {
    return;
  }
  strncpy(subspace_error.error_message, error_message,
          sizeof(subspace_error.error_message) - 1);
  subspace_error.error_message[sizeof(subspace_error.error_message) - 1] = '\0';
}

char *subspace_get_last_error(void) { return subspace_error.error_message; }

bool subspace_has_error(void) {
  return subspace_error.error_message[0] != '\0';
}

// The SubspaceClient struct contains a pointer to a
// std::shared_ptr<subspace::Client>.

SubspaceClient subspace_create_client(void) {
#if defined(__ANDROID__)
  return subspace_create_client_with_socket_and_name(
      "/data/local/tmp/subspace", "");
#else
  return subspace_create_client_with_socket_and_name("/tmp/subspace", "");
#endif
}
SubspaceClient subspace_create_client_with_socket(const char *socket_name) {
  return subspace_create_client_with_socket_and_name(socket_name, "");
}

SubspaceClient
subspace_create_client_with_socket_and_name(const char *socket_name,
                                            const char *client_name) {
  subspace_clear_error();
  SubspaceClient client;
  memset(&client, 0, sizeof(client));
  std::shared_ptr<subspace::Client> sclient =
      std::make_shared<subspace::Client>();
  absl::Status status = sclient->Init(socket_name, client_name);
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return client;
  }
  // Copy the shared ptr to the heap.
  client.client = reinterpret_cast<void *>(
      new std::shared_ptr<subspace::Client>(std::move(sclient)));
  return client;
}

bool subspace_set_client_debug(SubspaceClient client, bool debug) {
  subspace_clear_error();
  if (client.client == nullptr) {
    subspace_set_error("Invalid client");
    return false;
  }
  (*ClientPtr(client))->SetDebug(debug);
  return true;
}

bool subspace_set_client_thread_safe(SubspaceClient client, bool thread_safe) {
  subspace_clear_error();
  if (client.client == nullptr) {
    subspace_set_error("Invalid client");
    return false;
  }
  (*ClientPtr(client))->SetThreadSafe(thread_safe);
  return true;
}

bool subspace_channel_exists(SubspaceClient client, const char *channel_name,
                             bool *exists) {
  subspace_clear_error();
  if (exists != nullptr) {
    *exists = false;
  }
  if (client.client == nullptr || channel_name == nullptr ||
      exists == nullptr) {
    subspace_set_error("Invalid channel exists arguments");
    return false;
  }
  auto status_or_exists = (*ClientPtr(client))->ChannelExists(channel_name);
  if (!status_or_exists.ok()) {
    subspace_set_error(status_or_exists.status().ToString().c_str());
    return false;
  }
  *exists = *status_or_exists;
  return true;
}

bool subspace_get_channel_counters(SubspaceClient client,
                                   const char *channel_name,
                                   SubspaceChannelCounters *counters) {
  subspace_clear_error();
  if (counters != nullptr) {
    memset(counters, 0, sizeof(*counters));
  }
  if (client.client == nullptr || channel_name == nullptr ||
      counters == nullptr) {
    subspace_set_error("Invalid channel counters arguments");
    return false;
  }
  auto status_or_counters =
      (*ClientPtr(client))->GetChannelCounters(channel_name);
  if (!status_or_counters.ok()) {
    subspace_set_error(status_or_counters.status().ToString().c_str());
    return false;
  }
  *counters = ToCCounters(*status_or_counters);
  return true;
}

bool subspace_get_channel_info(SubspaceClient client, const char *channel_name,
                               SubspaceChannelInfo *info) {
  subspace_clear_error();
  if (info != nullptr) {
    memset(info, 0, sizeof(*info));
  }
  if (client.client == nullptr || channel_name == nullptr || info == nullptr) {
    subspace_set_error("Invalid channel info arguments");
    return false;
  }
  auto status_or_info = (*ClientPtr(client))->GetChannelInfo(channel_name);
  if (!status_or_info.ok()) {
    subspace_set_error(status_or_info.status().ToString().c_str());
    return false;
  }
  ClientCache &cache = client_caches[client.client];
  cache.strings.clear();
  cache.strings.push_back(status_or_info->channel_name);
  cache.strings.push_back(status_or_info->type);
  *info = ToCChannelInfo(*status_or_info, cache.strings[0], cache.strings[1]);
  return true;
}

bool subspace_get_all_channel_info(SubspaceClient client,
                                   SubspaceChannelInfo **infos, size_t *count) {
  subspace_clear_error();
  if (infos != nullptr) {
    *infos = nullptr;
  }
  if (count != nullptr) {
    *count = 0;
  }
  if (client.client == nullptr || infos == nullptr || count == nullptr) {
    subspace_set_error("Invalid channel info arguments");
    return false;
  }
  auto status_or_infos = (*ClientPtr(client))->GetChannelInfo();
  if (!status_or_infos.ok()) {
    subspace_set_error(status_or_infos.status().ToString().c_str());
    return false;
  }
  ClientCache &cache = client_caches[client.client];
  cache.strings.clear();
  cache.infos.clear();
  cache.strings.reserve(status_or_infos->size() * 2);
  for (const auto &info : *status_or_infos) {
    cache.strings.push_back(info.channel_name);
    cache.strings.push_back(info.type);
  }
  cache.infos.reserve(status_or_infos->size());
  for (size_t i = 0; i < status_or_infos->size(); i++) {
    cache.infos.push_back(ToCChannelInfo(
        (*status_or_infos)[i], cache.strings[i * 2], cache.strings[i * 2 + 1]));
  }
  *infos = cache.infos.data();
  *count = cache.infos.size();
  return true;
}

bool subspace_get_channel_stats(SubspaceClient client, const char *channel_name,
                                SubspaceChannelStats *stats) {
  subspace_clear_error();
  if (stats != nullptr) {
    memset(stats, 0, sizeof(*stats));
  }
  if (client.client == nullptr || channel_name == nullptr || stats == nullptr) {
    subspace_set_error("Invalid channel stats arguments");
    return false;
  }
  auto status_or_stats = (*ClientPtr(client))->GetChannelStats(channel_name);
  if (!status_or_stats.ok()) {
    subspace_set_error(status_or_stats.status().ToString().c_str());
    return false;
  }
  ClientCache &cache = client_caches[client.client];
  cache.strings.clear();
  cache.strings.push_back(status_or_stats->channel_name);
  *stats = ToCChannelStats(*status_or_stats, cache.strings[0]);
  return true;
}

bool subspace_get_all_channel_stats(SubspaceClient client,
                                    SubspaceChannelStats **stats,
                                    size_t *count) {
  subspace_clear_error();
  if (stats != nullptr) {
    *stats = nullptr;
  }
  if (count != nullptr) {
    *count = 0;
  }
  if (client.client == nullptr || stats == nullptr || count == nullptr) {
    subspace_set_error("Invalid channel stats arguments");
    return false;
  }
  auto status_or_stats = (*ClientPtr(client))->GetChannelStats();
  if (!status_or_stats.ok()) {
    subspace_set_error(status_or_stats.status().ToString().c_str());
    return false;
  }
  ClientCache &cache = client_caches[client.client];
  cache.strings.clear();
  cache.stats.clear();
  cache.strings.reserve(status_or_stats->size());
  for (const auto &stats : *status_or_stats) {
    cache.strings.push_back(stats.channel_name);
  }
  cache.stats.reserve(status_or_stats->size());
  for (size_t i = 0; i < status_or_stats->size(); i++) {
    cache.stats.push_back(
        ToCChannelStats((*status_or_stats)[i], cache.strings[i]));
  }
  *stats = cache.stats.data();
  *count = cache.stats.size();
  return true;
}

SubspaceSubscriberOptions subspace_subscriber_options_default(void) {
  SubspaceSubscriberOptions options = {
      .reliable = false,
      .bridge = false,
      .for_tunnel = false,
      .type = {.type = nullptr, .type_length = 0},
      .max_active_messages = 1,
      .pass_activation = false,
      .log_dropped_messages = false,
      .read_write = false,
      .mux = nullptr,
      .mux_length = 0,
      .vchan_id = -1,
      .checksum = false,
      .pass_checksum_errors = false,
      .keep_active_message = false,
      .split_callbacks = {},
  };
  return options;
}

SubspacePublisherOptions subspace_publisher_options_default(int32_t slot_size,
                                                            int num_slots) {
  SubspacePublisherOptions options = {
      .slot_size = slot_size,
      .num_slots = num_slots,
      .local = false,
      .reliable = false,
      .bridge = false,
      .for_tunnel = false,
      .fixed_size = false,
      .type = {.type = nullptr, .type_length = 0},
      .activate = false,
      .mux = nullptr,
      .mux_length = 0,
      .vchan_id = -1,
      .notify_retirement = false,
      .checksum = false,
      .checksum_size = 4,
      .metadata_size = 0,
      .prefer_retired_slots = true,
      .use_split_buffers = false,
      .split_buffers_over_bridge = false,
      .max_publishers = 0,
      .split_callbacks = {},
  };
  return options;
}

SubspaceSubscriber
subspace_create_subscriber(SubspaceClient client, const char *channel_name,
                           SubspaceSubscriberOptions options) {
  subspace::SubscriberOptions subspace_options = {
      .reliable = options.reliable,
      .bridge = options.bridge,
      .for_tunnel = options.for_tunnel,
      .type = StringFromPointer(options.type.type, options.type.type_length),
      .max_active_messages = options.max_active_messages,
      .log_dropped_messages = options.log_dropped_messages,
      .pass_activation = options.pass_activation,
      .read_write = options.read_write,
      .mux = StringFromPointer(options.mux, options.mux_length),
      .vchan_id = options.vchan_id,
      .checksum = options.checksum,
      .pass_checksum_errors = options.pass_checksum_errors,
      .keep_active_message = options.keep_active_message,
      .split_buffer_callbacks = ToCppSplitCallbacks(options.split_callbacks),
  };
  subspace_clear_error();
  SubspaceSubscriber subscriber;
  memset(&subscriber, 0, sizeof(subscriber));
  std::shared_ptr<subspace::Client> *client_ptr =
      reinterpret_cast<std::shared_ptr<subspace::Client> *>(client.client);
  absl::StatusOr<subspace::Subscriber> status_or_sub =
      (*client_ptr)->CreateSubscriber(channel_name, subspace_options);
  if (!status_or_sub.ok()) {
    subspace_set_error(status_or_sub.status().ToString().c_str());
    return subscriber;
  };

  subspace::Subscriber *sub_ptr =
      new subspace::Subscriber(std::move(*status_or_sub));
  subscriber.subscriber = reinterpret_cast<void *>(
      new std::shared_ptr<subspace::Subscriber>(sub_ptr));
  return subscriber;
}

SubspacePublisher subspace_create_publisher(SubspaceClient client,
                                            const char *channel_name,
                                            SubspacePublisherOptions options) {
  subspace::PublisherOptions subspace_options = {
      .slot_size = options.slot_size,
      .num_slots = options.num_slots,
      .local = options.local,
      .reliable = options.reliable,
      .bridge = options.bridge,
      .for_tunnel = options.for_tunnel,
      .fixed_size = options.fixed_size,
      .type = StringFromPointer(options.type.type, options.type.type_length),
      .activate = options.activate,
      .mux = StringFromPointer(options.mux, options.mux_length),
      .vchan_id = options.vchan_id,
      .notify_retirement = options.notify_retirement,
      .checksum = options.checksum,
      .checksum_size = options.checksum_size,
      .metadata_size = options.metadata_size,
      .prefer_retired_slots = options.prefer_retired_slots,
      .max_publishers = options.max_publishers,
      .use_split_buffers = options.use_split_buffers,
      .split_buffers_over_bridge = options.split_buffers_over_bridge,
      .split_buffer_callbacks = ToCppSplitCallbacks(options.split_callbacks),
  };
  subspace_clear_error();
  SubspacePublisher publisher;
  memset(&publisher, 0, sizeof(publisher));
  std::shared_ptr<subspace::Client> *client_ptr =
      reinterpret_cast<std::shared_ptr<subspace::Client> *>(client.client);
  absl::StatusOr<subspace::Publisher> status_or_pub =
      (*client_ptr)->CreatePublisher(channel_name, subspace_options);
  if (!status_or_pub.ok()) {
    subspace_set_error(status_or_pub.status().ToString().c_str());
    return publisher;
  }
  subspace::Publisher *pub_ptr =
      new subspace::Publisher(std::move(*status_or_pub));
  publisher.publisher = reinterpret_cast<void *>(
      new std::shared_ptr<subspace::Publisher>(pub_ptr));
  return publisher;
}

SubspaceMessage subspace_read_message_with_mode(SubspaceSubscriber subscriber,
                                                SubspaceReadMode mode) {
  SubspaceMessage message = EmptyMessage();
  if (subscriber.subscriber == nullptr) {
    return message;
  }
  subspace_clear_error();
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::StatusOr<subspace::Message> status_or_msg =
      (*sub_ptr)->ReadMessage(ToCppReadMode(mode));
  if (!status_or_msg.ok()) {
    subspace_set_error(status_or_msg.status().ToString().c_str());
    return message;
  }
  // No data available: return the empty SubspaceMessage as-is. Don't
  // allocate the smart-pointer wrapper, otherwise every poll without a
  // message leaks one shared_ptr<Message> + one Message — the caller
  // would have to call subspace_free_message on every empty read.
  if (status_or_msg->length == 0) {
    return message;
  }
  return TakeCMessage(std::move(*status_or_msg));
}

SubspaceMessage subspace_read_message(SubspaceSubscriber subscriber) {
  return subspace_read_message_with_mode(subscriber, kSubspaceReadNext);
}

SubspaceMessage subspace_find_message(SubspaceSubscriber subscriber,
                                      uint64_t timestamp) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return EmptyMessage();
  }
  auto sub_ptr = SubscriberPtr(subscriber);
  absl::StatusOr<subspace::Message> status_or_msg =
      (*sub_ptr)->FindMessage(timestamp);
  if (!status_or_msg.ok()) {
    subspace_set_error(status_or_msg.status().ToString().c_str());
    return EmptyMessage();
  }
  if (status_or_msg->length == 0) {
    return EmptyMessage();
  }
  return TakeCMessage(std::move(*status_or_msg));
}

bool subspace_get_all_messages(SubspaceSubscriber subscriber,
                               SubspaceReadMode mode,
                               SubspaceMessage **messages, size_t *count) {
  subspace_clear_error();
  if (messages != nullptr) {
    *messages = nullptr;
  }
  if (count != nullptr) {
    *count = 0;
  }
  if (subscriber.subscriber == nullptr || messages == nullptr ||
      count == nullptr) {
    subspace_set_error("Invalid get-all-messages arguments");
    return false;
  }
  HandleCache &cache = subscriber_caches[subscriber.subscriber];
  for (SubspaceMessage &message : cache.messages) {
    FreeMessageStorage(&message);
  }
  cache.messages.clear();

  auto sub_ptr = SubscriberPtr(subscriber);
  absl::StatusOr<std::vector<subspace::Message>> status_or_messages =
      (*sub_ptr)->GetAllMessages(ToCppReadMode(mode));
  if (!status_or_messages.ok()) {
    subspace_set_error(status_or_messages.status().ToString().c_str());
    return false;
  }
  cache.messages.reserve(status_or_messages->size());
  for (subspace::Message &message : *status_or_messages) {
    if (message.length != 0) {
      cache.messages.push_back(TakeCMessage(std::move(message)));
    }
  }
  *messages = cache.messages.data();
  *count = cache.messages.size();
  return true;
}

bool subspace_free_messages(SubspaceMessage *messages, size_t count) {
  subspace_clear_error();
  if (messages == nullptr && count != 0) {
    subspace_set_error("Invalid messages parameter");
    return false;
  }
  for (size_t i = 0; i < count; i++) {
    FreeMessageStorage(&messages[i]);
  }
  return true;
}

bool subspace_free_message(SubspaceMessage *message) {
  subspace_clear_error();
  if (message->message == nullptr) {
    subspace_set_error("Invalid message parameter");
    return false;
  }
  FreeMessageStorage(message);
  return true;
}

bool subspace_register_subscriber_callback(SubspaceSubscriber subscriber,
                                           void (*callback)(SubspaceSubscriber,
                                                            SubspaceMessage)) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback parameter");
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);

  absl::Status status = (*sub_ptr)->RegisterMessageCallback(
      [subscriber, callback](const subspace::Subscriber *,
                             subspace::Message msg) {
        // This takes ownership of the message.  It must be freed by calline
        // subspace_free_message. by the callback function.
        callback(subscriber, TakeCMessage(std::move(msg)));
      });
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_remove_subscriber_callback(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  subspace_clear_error();
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  auto status = (*sub_ptr)->UnregisterMessageCallback();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_register_dropped_message_callback(
    SubspaceSubscriber subscriber,
    void (*callback)(SubspaceSubscriber, int64_t)) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback parameter");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->RegisterDroppedMessageCallback(
      [subscriber, callback](const subspace::Subscriber *, int64_t count) {
        callback(subscriber, count);
      });
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

// Unregister the callback and return true is successful.
bool subspace_remove_dropped_message_callback(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->UnregisterDroppedMessageCallback();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_process_all_messages(SubspaceSubscriber subscriber) {
  return subspace_process_all_messages_with_mode(subscriber, kSubspaceReadNext);
}

bool subspace_process_all_messages_with_mode(SubspaceSubscriber subscriber,
                                             SubspaceReadMode mode) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->ProcessAllMessages(ToCppReadMode(mode));
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_invoke_subscriber_callback(SubspaceSubscriber subscriber,
                                         SubspaceMessage message) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  if (message.message == nullptr) {
    subspace_set_error("Invalid message");
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  auto msg_ptr =
      reinterpret_cast<std::shared_ptr<subspace::Message> *>(message.message);
  // InvokeMessageCallback takes the Message by value; the copy increments the
  // active-message refcount so the caller's SubspaceMessage retains ownership
  // and is still its responsibility to subspace_free_message.
  (*sub_ptr)->InvokeMessageCallback(**msg_ptr);
  return true;
}

bool subspace_clear_active_message(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  (*SubscriberPtr(subscriber))->ClearActiveMessage();
  return true;
}

bool subspace_trigger_reliable_publishers(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  (*SubscriberPtr(subscriber))->TriggerReliablePublishers();
  return true;
}

bool subspace_trigger_subscriber(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  (*SubscriberPtr(subscriber))->Trigger();
  return true;
}

bool subspace_untrigger_subscriber(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  (*SubscriberPtr(subscriber))->Untrigger();
  return true;
}

SubspaceMessageBuffer subspace_get_message_buffer(SubspacePublisher publisher,
                                                  size_t max_size) {
  subspace_clear_error();
  SubspaceMessageBuffer message_buffer;
  memset(&message_buffer, 0, sizeof(message_buffer));
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher ");
    return message_buffer;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::StatusOr<void *> status_or_buffer =
      (*pub_ptr)->GetMessageBuffer(max_size);
  if (!status_or_buffer.ok()) {
    subspace_set_error(status_or_buffer.status().ToString().c_str());
    return message_buffer;
  }
  auto buffer = std::move(*status_or_buffer);
  message_buffer.buffer = buffer;
  message_buffer.buffer_size = max_size;
  return message_buffer;
}

const SubspaceMessage subspace_publish_message(SubspacePublisher publisher,
                                               size_t message_size) {
  subspace_clear_error();
  SubspaceMessage published_message = EmptyMessage();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return published_message;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::StatusOr<const subspace::Message> status_or_msg =
      (*pub_ptr)->PublishMessage(message_size);
  if (!status_or_msg.ok()) {
    subspace_set_error(status_or_msg.status().ToString().c_str());
    return published_message;
  }
  return ToCMessage(*status_or_msg);
}

const SubspaceMessage
subspace_publish_message_with_prefix(SubspacePublisher publisher,
                                     size_t message_size,
                                     bool use_slot_id_from_prefix) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return EmptyMessage();
  }
  auto pub_ptr = PublisherPtr(publisher);
  absl::StatusOr<const subspace::Message> status_or_msg =
      (*pub_ptr)->PublishMessageWithPrefix(message_size,
                                           use_slot_id_from_prefix);
  if (!status_or_msg.ok()) {
    subspace_set_error(status_or_msg.status().ToString().c_str());
    return EmptyMessage();
  }
  return ToCMessage(*status_or_msg);
}

bool subspace_cancel_publish(SubspacePublisher publisher) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  (*PublisherPtr(publisher))->CancelPublish();
  return true;
}

bool subspace_remove_subscriber(SubspaceSubscriber *subscriber) {
  subspace_clear_error();
  if (subscriber->subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber->subscriber);
  auto cache_it = subscriber_caches.find(subscriber->subscriber);
  if (cache_it != subscriber_caches.end()) {
    for (SubspaceMessage &message : cache_it->second.messages) {
      FreeMessageStorage(&message);
    }
    subscriber_caches.erase(cache_it);
  }
  delete sub_ptr;
  subscriber->subscriber = nullptr;
  return true;
}

bool subspace_remove_publisher(SubspacePublisher *publisher) {
  if (publisher->publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher->publisher);
  publisher_caches.erase(publisher->publisher);
  delete pub_ptr;
  publisher->publisher = nullptr;
  return true;
}

bool subspace_remove_client(SubspaceClient *client) {
  subspace_clear_error();
  if (client->client == nullptr) {
    subspace_set_error("Invalid client");
    return false;
  }
  auto client_ptr = ClientPtr(*client);
  client_caches.erase(client->client);
  delete client_ptr;
  client->client = nullptr;
  return true;
}

bool subspace_wait_for_subscriber(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->Wait();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_wait_for_subscriber_with_timeout(SubspaceSubscriber subscriber,
                                               uint64_t timeout_ms) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->Wait(std::chrono::milliseconds(timeout_ms));
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

int subspace_wait_for_subscriber_with_fd(SubspaceSubscriber subscriber,
                                         int fd) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return -1;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::StatusOr<int> status = (*sub_ptr)->Wait(toolbelt::FileDescriptor(fd));
  if (!status.ok()) {
    subspace_set_error(status.status().ToString().c_str());
    return -1;
  }
  return *status;
}

int subspace_wait_for_subscriber_with_fd_and_timeout(
    SubspaceSubscriber subscriber, int fd, uint64_t timeout_ms) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return -1;
  }
  auto sub_ptr = SubscriberPtr(subscriber);
  absl::StatusOr<int> status = (*sub_ptr)->Wait(
      toolbelt::FileDescriptor(fd), std::chrono::milliseconds(timeout_ms));
  if (!status.ok()) {
    subspace_set_error(status.status().ToString().c_str());
    return -1;
  }
  return *status;
}

bool subspace_wait_for_publisher(SubspacePublisher publisher) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::Status status = (*pub_ptr)->Wait();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_wait_for_publisher_with_timeout(SubspacePublisher publisher,
                                              uint64_t timeout_ms) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  absl::Status status =
      (*PublisherPtr(publisher))->Wait(std::chrono::milliseconds(timeout_ms));
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

int subspace_wait_for_publisher_with_fd(SubspacePublisher publisher, int fd) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return -1;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::StatusOr<int> status = (*pub_ptr)->Wait(toolbelt::FileDescriptor(fd));
  if (!status.ok()) {
    subspace_set_error(status.status().ToString().c_str());
    return -1;
  }
  return *status;
}

int subspace_wait_for_publisher_with_fd_and_timeout(SubspacePublisher publisher,
                                                    int fd,
                                                    uint64_t timeout_ms) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return -1;
  }
  absl::StatusOr<int> status =
      (*PublisherPtr(publisher))
          ->Wait(toolbelt::FileDescriptor(fd),
                 std::chrono::milliseconds(timeout_ms));
  if (!status.ok()) {
    subspace_set_error(status.status().ToString().c_str());
    return -1;
  }
  return *status;
}

bool subspace_register_resize_callback(SubspacePublisher publisher,
                                       bool (*callback)(SubspacePublisher,
                                                        int32_t, int32_t)) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::Status status = (*pub_ptr)->RegisterResizeCallback(
      [publisher, callback](subspace::Publisher * /*pub*/, int32_t old_size,
                            int32_t new_size) -> absl::Status {
        // Call the callback with the publisher and the old and new sizes.
        bool ok = callback(publisher, old_size, new_size);
        return ok ? absl::OkStatus()
                  : absl::InternalError("Resize callback prevented resize");
      });
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_unregister_resize_callback(SubspacePublisher publisher) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::Status status = (*pub_ptr)->UnregisterResizeCallback();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_register_publisher_on_send_callback(
    SubspacePublisher publisher, SubspaceBufferTransformCallback callback,
    void *user_data) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback");
    return false;
  }
  (*PublisherPtr(publisher))
      ->SetOnSendCallback(
          [callback, user_data](void *buffer,
                                int64_t size) -> absl::StatusOr<int64_t> {
            int64_t out_size = size;
            if (!callback(buffer, size, &out_size, user_data)) {
              return absl::InternalError("On-send callback failed");
            }
            return out_size;
          });
  return true;
}

bool subspace_unregister_publisher_on_send_callback(
    SubspacePublisher publisher) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  (*PublisherPtr(publisher))->ClearOnSendCallback();
  return true;
}

bool subspace_register_subscriber_on_receive_callback(
    SubspaceSubscriber subscriber, SubspaceBufferTransformCallback callback,
    void *user_data) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback");
    return false;
  }
  (*SubscriberPtr(subscriber))
      ->SetOnReceiveCallback(
          [callback, user_data](void *buffer,
                                int64_t size) -> absl::StatusOr<int64_t> {
            int64_t out_size = size;
            if (!callback(buffer, size, &out_size, user_data)) {
              return absl::InternalError("On-receive callback failed");
            }
            return out_size;
          });
  return true;
}

bool subspace_unregister_subscriber_on_receive_callback(
    SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  (*SubscriberPtr(subscriber))->ClearOnReceiveCallback();
  return true;
}

bool subspace_register_publisher_checksum_callback(
    SubspacePublisher publisher, SubspaceChecksumCallback callback,
    void *user_data) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid checksum callback");
    return false;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  (*pub_ptr)->SetChecksumCallback(ToCppChecksumCallback(callback, user_data));
  return true;
}

bool subspace_unregister_publisher_checksum_callback(
    SubspacePublisher publisher) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  (*pub_ptr)->ResetChecksumCallback();
  return true;
}

bool subspace_register_subscriber_checksum_callback(
    SubspaceSubscriber subscriber, SubspaceChecksumCallback callback,
    void *user_data) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid checksum callback");
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  (*sub_ptr)->SetChecksumCallback(ToCppChecksumCallback(callback, user_data));
  return true;
}

bool subspace_unregister_subscriber_checksum_callback(
    SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  (*sub_ptr)->ResetChecksumCallback();
  return true;
}

SubspaceTypeInfo subspace_get_subscriber_type(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  SubspaceTypeInfo type_info = {.type = nullptr, .type_length = 0};
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return type_info;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  auto type = (*sub_ptr)->TypeView();
  type_info.type = type.data();
  type_info.type_length = type.size();
  return type_info;
}

struct pollfd subspace_get_subscriber_poll_fd(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  struct pollfd pollFd = {.fd = -1, .events = 0};
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return pollFd;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  pollFd = (*sub_ptr)->GetPollFd();
  return pollFd;
}

struct pollfd subspace_get_publisher_poll_fd(SubspacePublisher publisher) {
  subspace_clear_error();
  struct pollfd pollFd = {.fd = -1, .events = 0};
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return pollFd;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  pollFd = (*pub_ptr)->GetPollFd();
  return pollFd;
}

int subspace_get_publisher_fd(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return -1;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  return (*pub_ptr)->GetFileDescriptor().Fd();
}

int subspace_get_publisher_retirement_fd(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return -1;
  }
  return (*PublisherPtr(publisher))->GetRetirementFd().Fd();
}

bool subspace_is_publisher_reliable(SubspacePublisher publisher) {
  return publisher.publisher != nullptr &&
         (*PublisherPtr(publisher))->IsReliable();
}

bool subspace_is_publisher_local(SubspacePublisher publisher) {
  return publisher.publisher != nullptr &&
         (*PublisherPtr(publisher))->IsLocal();
}

bool subspace_is_publisher_fixed_size(SubspacePublisher publisher) {
  return publisher.publisher != nullptr &&
         (*PublisherPtr(publisher))->IsFixedSize();
}

bool subspace_is_publisher_for_tunnel(SubspacePublisher publisher) {
  return publisher.publisher != nullptr &&
         (*PublisherPtr(publisher))->ForTunnel();
}

bool subspace_publisher_uses_split_buffers(SubspacePublisher publisher) {
  return publisher.publisher != nullptr &&
         (*PublisherPtr(publisher))->UsesSplitBuffers();
}

int32_t subspace_get_publisher_slot_size(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return 0;
  }
  return (*PublisherPtr(publisher))->SlotSize();
}

int32_t subspace_get_publisher_num_slots(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return 0;
  }
  return (*PublisherPtr(publisher))->NumSlots();
}

SubspaceString subspace_get_publisher_name(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return {};
  }
  HandleCache &cache = publisher_caches[publisher.publisher];
  cache.string = (*PublisherPtr(publisher))->Name();
  return ToCString(cache.string);
}

SubspaceString subspace_get_publisher_type(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return {};
  }
  HandleCache &cache = publisher_caches[publisher.publisher];
  cache.string = (*PublisherPtr(publisher))->Type();
  return ToCString(cache.string);
}

SubspaceString subspace_get_publisher_mux(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return {};
  }
  HandleCache &cache = publisher_caches[publisher.publisher];
  cache.string = (*PublisherPtr(publisher))->Mux();
  return ToCString(cache.string);
}

int subspace_get_publisher_virtual_channel_id(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return -1;
  }
  return (*PublisherPtr(publisher))->VirtualChannelId();
}

int subspace_get_publisher_num_subscribers(SubspacePublisher publisher,
                                           int vchan_id) {
  if (publisher.publisher == nullptr) {
    return 0;
  }
  return (*PublisherPtr(publisher))->NumSubscribers(vchan_id);
}

int subspace_get_publisher_current_slot_id(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return -1;
  }
  return (*PublisherPtr(publisher))->CurrentSlotId();
}

uint64_t
subspace_get_publisher_virtual_memory_usage(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return 0;
  }
  return (*PublisherPtr(publisher))->GetVirtualMemoryUsage();
}

bool subspace_get_publisher_stats_counters(SubspacePublisher publisher,
                                           uint64_t *total_bytes,
                                           uint64_t *total_messages,
                                           uint32_t *max_message_size,
                                           uint32_t *total_drops) {
  if (total_bytes != nullptr) {
    *total_bytes = 0;
  }
  if (total_messages != nullptr) {
    *total_messages = 0;
  }
  if (max_message_size != nullptr) {
    *max_message_size = 0;
  }
  if (total_drops != nullptr) {
    *total_drops = 0;
  }
  if (publisher.publisher == nullptr || total_bytes == nullptr ||
      total_messages == nullptr || max_message_size == nullptr ||
      total_drops == nullptr) {
    return false;
  }
  (*PublisherPtr(publisher))
      ->GetStatsCounters(*total_bytes, *total_messages, *max_message_size,
                         *total_drops);
  return true;
}

bool subspace_get_publisher_counters(SubspacePublisher publisher,
                                     SubspaceChannelCounters *counters) {
  if (counters != nullptr) {
    memset(counters, 0, sizeof(*counters));
  }
  if (publisher.publisher == nullptr || counters == nullptr) {
    return false;
  }
  *counters = ToCCounters((*PublisherPtr(publisher))->GetCounters());
  return true;
}

bool subspace_get_publisher_channel_counters(
    SubspacePublisher publisher, SubspaceChannelCounters *counters) {
  if (counters != nullptr) {
    memset(counters, 0, sizeof(*counters));
  }
  if (publisher.publisher == nullptr || counters == nullptr) {
    return false;
  }
  *counters = ToCCounters((*PublisherPtr(publisher))->GetChannelCounters());
  return true;
}

SubspaceString
subspace_get_publisher_buffer_shared_memory_name(SubspacePublisher publisher,
                                                 int buffer_index) {
  if (publisher.publisher == nullptr) {
    return {};
  }
  HandleCache &cache = publisher_caches[publisher.publisher];
  cache.string =
      (*PublisherPtr(publisher))->BufferSharedMemoryName(buffer_index);
  return ToCString(cache.string);
}

int subspace_get_subscriber_fd(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return -1;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->GetFileDescriptor().Fd();
}

int32_t subspace_get_subscriber_slot_size(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->SlotSize();
}

int subspace_get_subscriber_num_slots(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->NumSlots();
}

int64_t subspace_get_subscriber_current_ordinal(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return -1;
  }
  return (*SubscriberPtr(subscriber))->GetCurrentOrdinal();
}

int64_t subspace_get_subscriber_timestamp(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  return (*SubscriberPtr(subscriber))->Timestamp();
}

int subspace_get_subscriber_num_active_messages(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  return (*SubscriberPtr(subscriber))->NumActiveMessages();
}

bool subspace_is_subscriber_placeholder(SubspaceSubscriber subscriber) {
  return subscriber.subscriber != nullptr &&
         (*SubscriberPtr(subscriber))->IsPlaceholder();
}

bool subspace_is_subscriber_reliable(SubspaceSubscriber subscriber) {
  return subscriber.subscriber != nullptr &&
         (*SubscriberPtr(subscriber))->IsReliable();
}

bool subspace_is_subscriber_for_tunnel(SubspaceSubscriber subscriber) {
  return subscriber.subscriber != nullptr &&
         (*SubscriberPtr(subscriber))->ForTunnel();
}

bool subspace_subscriber_uses_split_buffers(SubspaceSubscriber subscriber) {
  return subscriber.subscriber != nullptr &&
         (*SubscriberPtr(subscriber))->UsesSplitBuffers();
}

int subspace_get_subscriber_virtual_channel_id(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return -1;
  }
  return (*SubscriberPtr(subscriber))->VirtualChannelId();
}

int subspace_get_subscriber_configured_vchan_id(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return -1;
  }
  return (*SubscriberPtr(subscriber))->ConfiguredVchanId();
}

SubspaceString subspace_get_subscriber_name(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return {};
  }
  HandleCache &cache = subscriber_caches[subscriber.subscriber];
  cache.string = (*SubscriberPtr(subscriber))->Name();
  return ToCString(cache.string);
}

SubspaceString subspace_get_subscriber_mux(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return {};
  }
  HandleCache &cache = subscriber_caches[subscriber.subscriber];
  cache.string = (*SubscriberPtr(subscriber))->Mux();
  return ToCString(cache.string);
}

int subspace_get_subscriber_num_subscribers(SubspaceSubscriber subscriber,
                                            int vchan_id) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  return (*SubscriberPtr(subscriber))->NumSubscribers(vchan_id);
}

uint64_t
subspace_get_subscriber_virtual_memory_usage(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  return (*SubscriberPtr(subscriber))->GetVirtualMemoryUsage();
}

bool subspace_get_subscriber_counters(SubspaceSubscriber subscriber,
                                      SubspaceChannelCounters *counters) {
  if (counters != nullptr) {
    memset(counters, 0, sizeof(*counters));
  }
  if (subscriber.subscriber == nullptr || counters == nullptr) {
    return false;
  }
  *counters = ToCCounters((*SubscriberPtr(subscriber))->GetCounters());
  return true;
}

bool subspace_get_subscriber_channel_counters(
    SubspaceSubscriber subscriber, SubspaceChannelCounters *counters) {
  if (counters != nullptr) {
    memset(counters, 0, sizeof(*counters));
  }
  if (subscriber.subscriber == nullptr || counters == nullptr) {
    return false;
  }
  *counters = ToCCounters((*SubscriberPtr(subscriber))->GetChannelCounters());
  return true;
}

void *subspace_get_publisher_metadata(SubspacePublisher publisher,
                                      size_t *out_size) {
  subspace_clear_error();
  if (out_size != nullptr) {
    *out_size = 0;
  }
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return nullptr;
  }
  // The publisher contains a pointer to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::Span<std::byte> span = (*pub_ptr)->GetMetadata();
  if (out_size != nullptr) {
    *out_size = span.size();
  }
  return span.data();
}

const void *subspace_get_subscriber_metadata(SubspaceSubscriber subscriber,
                                             size_t *out_size) {
  subspace_clear_error();
  if (out_size != nullptr) {
    *out_size = 0;
  }
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return nullptr;
  }
  // The subscriber contains a pointer to a shared_ptr to a
  // subspace::Subscriber. Subscriber::GetMetadata returns a span over the
  // most recently read slot's metadata region; it remains valid until the
  // next read on the same subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Span<const std::byte> span = (*sub_ptr)->GetMetadata();
  if (out_size != nullptr) {
    *out_size = span.size();
  }
  return span.data();
}

int32_t subspace_get_publisher_metadata_size(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return 0;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  return (*pub_ptr)->MetadataSize();
}

int32_t subspace_get_subscriber_metadata_size(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->MetadataSize();
}

int32_t subspace_get_publisher_prefix_size(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return 0;
  }
  return (*PublisherPtr(publisher))->PrefixSize();
}

int32_t subspace_get_subscriber_prefix_size(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  return (*SubscriberPtr(subscriber))->PrefixSize();
}

int32_t subspace_get_publisher_checksum_size(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return 0;
  }
  return (*PublisherPtr(publisher))->ChecksumSize();
}

int32_t subspace_get_subscriber_checksum_size(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  return (*SubscriberPtr(subscriber))->ChecksumSize();
}

SubspaceMessageSlot
subspace_get_publisher_current_slot(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return {};
  }
  return {.slot = (*PublisherPtr(publisher))->CurrentSlot()};
}

SubspaceMessagePrefix subspace_get_publisher_prefix(SubspacePublisher publisher,
                                                    SubspaceMessageSlot slot) {
  if (publisher.publisher == nullptr) {
    return {};
  }
  auto *message_slot = reinterpret_cast<subspace::MessageSlot *>(slot.slot);
  return {.prefix = (*PublisherPtr(publisher))->Prefix(message_slot)};
}

SubspaceMessageSlot subspace_get_subscriber_slot(SubspaceSubscriber subscriber,
                                                 int slot_id) {
  if (subscriber.subscriber == nullptr) {
    return {};
  }
  return {.slot = (*SubscriberPtr(subscriber))->GetSlot(slot_id)};
}

SubspaceMessagePrefix
subspace_get_subscriber_prefix(SubspaceSubscriber subscriber,
                               SubspaceMessageSlot slot) {
  if (subscriber.subscriber == nullptr) {
    return {};
  }
  auto *message_slot = reinterpret_cast<subspace::MessageSlot *>(slot.slot);
  return {.prefix = (*SubscriberPtr(subscriber))->Prefix(message_slot)};
}

bool subspace_snapshot_message_slot(SubspaceMessageSlot slot,
                                    SubspaceMessageSlotSnapshot *snapshot) {
  if (snapshot != nullptr) {
    memset(snapshot, 0, sizeof(*snapshot));
  }
  if (slot.slot == nullptr || snapshot == nullptr) {
    return false;
  }
  auto *message_slot = reinterpret_cast<subspace::MessageSlot *>(slot.slot);
  *snapshot = {.id = message_slot->id,
               .ordinal = message_slot->ordinal,
               .message_size = message_slot->message_size,
               .buffer_index = message_slot->buffer_index,
               .vchan_id = message_slot->vchan_id,
               .timestamp = message_slot->timestamp,
               .flags = message_slot->flags,
               .bridged_slot_id = message_slot->bridged_slot_id};
  return true;
}

bool subspace_snapshot_message_prefix(SubspaceMessagePrefix prefix,
                                      SubspaceMessagePrefixSnapshot *snapshot) {
  if (snapshot != nullptr) {
    memset(snapshot, 0, sizeof(*snapshot));
  }
  if (prefix.prefix == nullptr || snapshot == nullptr) {
    return false;
  }
  auto *message_prefix =
      reinterpret_cast<subspace::MessagePrefix *>(prefix.prefix);
  *snapshot = {.slot_id = message_prefix->slot_id,
               .message_size = message_prefix->message_size,
               .ordinal = message_prefix->ordinal,
               .timestamp = message_prefix->timestamp,
               .flags = message_prefix->flags,
               .vchan_id = message_prefix->vchan_id,
               .checksum_size = message_prefix->checksum_size,
               .metadata_size = message_prefix->metadata_size,
               .checksum = message_prefix->checksum};
  return true;
}

bool subspace_dump_publisher_slots(SubspacePublisher publisher,
                                   const char **data, size_t *length) {
  if (data != nullptr) {
    *data = nullptr;
  }
  if (length != nullptr) {
    *length = 0;
  }
  if (publisher.publisher == nullptr || data == nullptr || length == nullptr) {
    return false;
  }
  std::ostringstream os;
  (*PublisherPtr(publisher))->DumpSlots(os);
  HandleCache &cache = publisher_caches[publisher.publisher];
  cache.dump = os.str();
  *data = cache.dump.data();
  *length = cache.dump.size();
  return true;
}

bool subspace_dump_subscriber_slots(SubspaceSubscriber subscriber,
                                    const char **data, size_t *length) {
  if (data != nullptr) {
    *data = nullptr;
  }
  if (length != nullptr) {
    *length = 0;
  }
  if (subscriber.subscriber == nullptr || data == nullptr ||
      length == nullptr) {
    return false;
  }
  std::ostringstream os;
  (*SubscriberPtr(subscriber))->DumpSlots(os);
  HandleCache &cache = subscriber_caches[subscriber.subscriber];
  cache.dump = os.str();
  *data = cache.dump.data();
  *length = cache.dump.size();
  return true;
}

bool subspace_get_publisher_addresses(SubspacePublisher publisher,
                                      void ***addresses, size_t *count) {
  subspace_clear_error();
  if (addresses != nullptr) {
    *addresses = nullptr;
  }
  if (count != nullptr) {
    *count = 0;
  }
  if (publisher.publisher == nullptr || addresses == nullptr ||
      count == nullptr) {
    subspace_set_error("Invalid publisher address arguments");
    return false;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  if (!(*pub_ptr)->GetAddresses(addresses, count)) {
    subspace_set_error("Unable to get publisher addresses");
    return false;
  }
  return true;
}

bool subspace_get_subscriber_addresses(SubspaceSubscriber subscriber,
                                       void ***addresses, size_t *count) {
  subspace_clear_error();
  if (addresses != nullptr) {
    *addresses = nullptr;
  }
  if (count != nullptr) {
    *count = 0;
  }
  if (subscriber.subscriber == nullptr || addresses == nullptr ||
      count == nullptr) {
    subspace_set_error("Invalid subscriber address arguments");
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  if (!(*sub_ptr)->GetAddresses(addresses, count)) {
    subspace_set_error("Unable to get subscriber addresses");
    return false;
  }
  return true;
}

bool subspace_get_publisher_split_buffer_handle_from_address(
    SubspacePublisher publisher, const void *address, uintptr_t *handle) {
  if (handle != nullptr) {
    *handle = 0;
  }
  if (publisher.publisher == nullptr || address == nullptr ||
      handle == nullptr) {
    return false;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  return (*pub_ptr)->GetSplitBufferHandleFromAddress(address, handle);
}

bool subspace_get_publisher_split_buffer_handles(SubspacePublisher publisher,
                                                 uintptr_t **handles,
                                                 size_t *count) {
  if (handles != nullptr) {
    *handles = nullptr;
  }
  if (count != nullptr) {
    *count = 0;
  }
  if (publisher.publisher == nullptr || handles == nullptr ||
      count == nullptr) {
    return false;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  return (*pub_ptr)->GetSplitBufferHandles(handles, count);
}

bool subspace_get_subscriber_split_buffer_handle_from_address(
    SubspaceSubscriber subscriber, const void *address, uintptr_t *handle) {
  if (handle != nullptr) {
    *handle = 0;
  }
  if (subscriber.subscriber == nullptr || address == nullptr ||
      handle == nullptr) {
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->GetSplitBufferHandleFromAddress(address, handle);
}

bool subspace_get_subscriber_split_buffer_handles(SubspaceSubscriber subscriber,
                                                  uintptr_t **handles,
                                                  size_t *count) {
  if (handles != nullptr) {
    *handles = nullptr;
  }
  if (count != nullptr) {
    *count = 0;
  }
  if (subscriber.subscriber == nullptr || handles == nullptr ||
      count == nullptr) {
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->GetSplitBufferHandles(handles, count);
}

#if defined(__cplusplus)
} // extern "C"
#endif
