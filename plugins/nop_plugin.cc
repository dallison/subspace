#include "server/server.h"

namespace nop_plugin {

absl::Status OnStartup(subspace::Server &s, const std::string &name,
                       subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin %s started\n",
                  name.c_str());
  return absl::OkStatus();
}
void OnReady(subspace::Server &s, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin ready\n");
}

void OnShutdown(subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin shutting down\n");
}
void OnNewChannel(subspace::Server &s, const std::string &channel_name,
                  subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin: new channel %s\n",
                  channel_name.c_str());
}
void OnRemoveChannel(subspace::Server &s, const std::string &channel_name,
                     subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo, "NOP plugin: remove channel %s\n",
                  channel_name.c_str());
}
void OnNewPublisher(subspace::Server &s, const std::string &channel_name,
                    int publisher_id, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: new publisher %d on channel %s\n", publisher_id,
                  channel_name.c_str());
}
void OnRemovePublisher(subspace::Server &s, const std::string &channel_name,
                       int publisher_id, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: remove publisher %d on channel %s\n",
                  publisher_id, channel_name.c_str());
}
void OnNewSubscriber(subspace::Server &s, const std::string &channel_name,
                     int subscriber_id, subspace::PluginContext *ctx) {
  ctx->logger.Log(toolbelt::LogLevel::kInfo,
                  "NOP plugin: new subscriber %d on channel %s\n",
                  subscriber_id, channel_name.c_str());
}
void OnRemoveSubscriber(subspace::Server &s, const std::string &channel_name,
                        int subscriber_id, subspace::PluginContext *ctx) {
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
