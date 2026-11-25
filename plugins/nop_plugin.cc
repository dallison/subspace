#include "client/client.h"
#include "common/async/wait.h"
#include "server/server.h"

#include <chrono>
#include <cstring>

namespace nop_plugin {

constexpr int kHeartbeatSlotSize = 256;
constexpr int kHeartbeatNumSlots = 16;
constexpr uint64_t kHeartbeatPeriodNs = 1000000000ULL; // 1 second

subspace::async::Task<void>
HeartbeatCoroutine(subspace::Server &server, subspace::PluginContext *ctx,
                   subspace::async::Context coro_ctx) {
  if (server.ShuttingDown()) {
    ASYNC_RETURN;
  }

  subspace::Client client(coro_ctx);
  absl::Status status = SERVER_CLIENT_CALL(
      (client.Init(server.GetSocketName())),
      client.InitAsync(server.GetSocketName()));
  if (!status.ok()) {
    ctx->logger.Log(toolbelt::LogLevel::kError,
                    "NOP plugin: failed to init client for heartbeat: %s",
                    status.ToString().c_str());
    ASYNC_RETURN;
  }

  absl::StatusOr<subspace::Publisher> pub = SERVER_CLIENT_CALL(
      (client.CreatePublisher("/nop/Heartbeat", kHeartbeatSlotSize,
                              kHeartbeatNumSlots)),
      client.CreatePublisherAsync("/nop/Heartbeat", kHeartbeatSlotSize,
                                  kHeartbeatNumSlots));
  if (!pub.ok()) {
    ctx->logger.Log(toolbelt::LogLevel::kError,
                    "NOP plugin: failed to create heartbeat publisher: %s",
                    pub.status().ToString().c_str());
    ASYNC_RETURN;
  }

  uint64_t seq = 0;
  for (;;) {
    // Wait up to one period for the shutdown fd.  Ok() means it became
    // readable (shutting down); DeadlineExceeded means it is time to emit a
    // heartbeat.
    absl::Status wait_status = ASYNC_AWAIT(subspace::async::WaitReadable(
        coro_ctx, server.GetShutdownTriggerFd(),
        std::chrono::nanoseconds(kHeartbeatPeriodNs)));
    if (wait_status.ok()) {
      break;
    }

    absl::StatusOr<void *> buffer = SERVER_CLIENT_CALL(
        (pub->GetMessageBuffer()), pub->GetMessageBufferAsync());
    if (!buffer.ok()) {
      ctx->logger.Log(toolbelt::LogLevel::kError,
                      "NOP plugin: failed to get heartbeat buffer: %s",
                      buffer.status().ToString().c_str());
      continue;
    }
    memcpy(*buffer, &seq, sizeof(seq));
    absl::StatusOr<const subspace::Message> msg = SERVER_CLIENT_CALL(
        (pub->PublishMessage(sizeof(seq))),
        pub->PublishMessageAsync(sizeof(seq)));
    if (!msg.ok()) {
      ctx->logger.Log(toolbelt::LogLevel::kError,
                      "NOP plugin: failed to publish heartbeat: %s",
                      msg.status().ToString().c_str());
    }
    seq++;
  }
  SERVER_CLIENT_CLOSE(*pub);
}

absl::Status OnStartup(subspace::Server & /*s*/, const std::string &name,
                       subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin %s started\n",
                  name.c_str());
  return absl::OkStatus();
}
void OnReady(subspace::Server &s, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin ready\n");

  s.SpawnCoroutine(
      [&s, ctx](subspace::async::Context coro_ctx)
          -> subspace::async::Task<void> {
        ASYNC_AWAIT(HeartbeatCoroutine(s, ctx, coro_ctx));
      },
      {.name = "NOP heartbeat", .interrupt_fd = s.GetShutdownTriggerFd()});
}

void OnShutdown(subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin shutting down\n");
}
void OnNewChannel(subspace::Server & /*s*/, const std::string &channel_name,
                  subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin: new channel %s\n",
                  channel_name.c_str());
}
void OnRemoveChannel(subspace::Server & /*s*/, const std::string &channel_name,
                     subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin: remove channel %s\n",
                  channel_name.c_str());
}
void OnNewPublisher(subspace::Server & /*s*/, const std::string &channel_name,
                    int publisher_id, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: new publisher %d on channel %s\n", publisher_id,
                  channel_name.c_str());
}
void OnRemovePublisher(subspace::Server & /*s*/,
                       const std::string &channel_name, int publisher_id,
                       subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: remove publisher %d on channel %s\n",
                  publisher_id, channel_name.c_str());
}
void OnNewSubscriber(subspace::Server & /*s*/, const std::string &channel_name,
                     int subscriber_id, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: new subscriber %d on channel %s\n",
                  subscriber_id, channel_name.c_str());
}
void OnRemoveSubscriber(subspace::Server & /*s*/,
                        const std::string &channel_name, int subscriber_id,
                        subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: remove subscriber %d on channel %s\n",
                  subscriber_id, channel_name.c_str());
}

absl::StatusOr<bool> OnFreeClientBuffer(
    subspace::Server & /*s*/,
    const subspace::ClientBufferHandleMetadata &metadata,
    subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: ignoring client buffer for channel %s buffer %u "
                  "slot %u handle 0x%zx\n",
                  metadata.channel_name.c_str(), metadata.buffer_index,
                  metadata.slot_id, static_cast<size_t>(metadata.handle));
  return false;
}

absl::StatusOr<subspace::PluginPayload>
OnSend(subspace::Server & /*s*/, const std::string &channel_name,
       subspace::PluginPayload payload, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: onSend channel %s length %zu\n",
                  channel_name.c_str(), payload.bytes.size());
  return payload;
}

subspace::PluginReceiveResult
OnReceive(subspace::Server & /*s*/, const std::string &channel_name,
          std::byte * /*data*/, size_t length, size_t /*max_length*/,
          int32_t slot_id, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: onReceive channel %s length %zu slot %d\n",
                  channel_name.c_str(), length, slot_id);
  subspace::PluginReceiveResult result;
  result.length = static_cast<ssize_t>(length);
  return result;
}

} // namespace nop_plugin

extern "C" {
subspace::PluginInterface *NOP_Create() {
  subspace::PluginInterfaceFunctions functions = {
      .onStartup = nop_plugin::OnStartup,
      .onReady = nop_plugin::OnReady,
      .onShutdown = nop_plugin::OnShutdown,
      .onNewChannel = nop_plugin::OnNewChannel,
      .onRemoveChannel = nop_plugin::OnRemoveChannel,
      .onNewPublisher = nop_plugin::OnNewPublisher,
      .onRemovePublisher = nop_plugin::OnRemovePublisher,
      .onNewSubscriber = nop_plugin::OnNewSubscriber,
      .onRemoveSubscriber = nop_plugin::OnRemoveSubscriber,
      .onFreeClientBuffer = nop_plugin::OnFreeClientBuffer,
      .onSend = nop_plugin::OnSend,
      .onReceive = nop_plugin::OnReceive,
  };
  auto iface = new subspace::PluginInterface(
      functions, std::make_unique<subspace::PluginContext>("nop_plugin"));
  return iface;
}
}
