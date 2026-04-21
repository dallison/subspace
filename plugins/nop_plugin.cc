#include "client/client.h"
#include "server/server.h"

#include <cstring>

namespace nop_plugin {

constexpr int kHeartbeatSlotSize = 256;
constexpr int kHeartbeatNumSlots = 16;
constexpr uint64_t kHeartbeatPeriodNs = 1000000000ULL; // 1 second

void HeartbeatCoroutine(subspace::Server &server,
                        subspace::PluginContext *ctx) {
  if (server.ShuttingDown()) {
    return;
  }

  subspace::Client client(co::self);
  absl::Status status = client.Init(server.GetSocketName());
  if (!status.ok()) {
    ctx->logger.Log(toolbelt::LogLevel::kError,
                    "NOP plugin: failed to init client for heartbeat: %s",
                    status.ToString().c_str());
    return;
  }

  absl::StatusOr<subspace::Publisher> pub = client.CreatePublisher(
      "/nop/Heartbeat", kHeartbeatSlotSize, kHeartbeatNumSlots);
  if (!pub.ok()) {
    ctx->logger.Log(toolbelt::LogLevel::kError,
                    "NOP plugin: failed to create heartbeat publisher: %s",
                    pub.status().ToString().c_str());
    return;
  }

  uint64_t seq = 0;
  for (;;) {
    int fd = co::Wait(server.GetShutdownTriggerFd(), POLLIN, kHeartbeatPeriodNs);
    if (fd != -1) {
      break;
    }

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    if (!buffer.ok()) {
      ctx->logger.Log(toolbelt::LogLevel::kError,
                      "NOP plugin: failed to get heartbeat buffer: %s",
                      buffer.status().ToString().c_str());
      continue;
    }
    memcpy(*buffer, &seq, sizeof(seq));
    absl::StatusOr<const subspace::Message> msg =
        pub->PublishMessage(sizeof(seq));
    if (!msg.ok()) {
      ctx->logger.Log(toolbelt::LogLevel::kError,
                      "NOP plugin: failed to publish heartbeat: %s",
                      msg.status().ToString().c_str());
    }
    seq++;
  }
}

absl::Status OnStartup(subspace::Server & /*s*/, const std::string &name,
                       subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin %s started\n",
                  name.c_str());
  return absl::OkStatus();
}
void OnReady(subspace::Server &s, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin ready\n");

  s.GetScheduler().Spawn(
      [&s, ctx]() { HeartbeatCoroutine(s, ctx); },
      {.name = "NOP heartbeat",
       .interrupt_fd = s.GetShutdownTriggerFd()});
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
  };
  auto iface = new subspace::PluginInterface(
      functions, std::make_unique<subspace::PluginContext>("nop_plugin"));
  return iface;
}
}
