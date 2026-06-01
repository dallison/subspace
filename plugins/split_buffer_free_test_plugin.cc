#include "server/server.h"

#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

namespace split_buffer_free_test_plugin {

absl::Status OnStartup(subspace::Server & /*s*/, const std::string & /*name*/,
                       subspace::PluginContext * /*ctx*/) {
  return absl::OkStatus();
}

void OnReady(subspace::Server & /*s*/, subspace::PluginContext * /*ctx*/) {}
void OnShutdown(subspace::PluginContext * /*ctx*/) {}
void OnNewChannel(subspace::Server & /*s*/, const std::string & /*channel_name*/,
                  subspace::PluginContext * /*ctx*/) {}
void OnRemoveChannel(subspace::Server & /*s*/,
                     const std::string & /*channel_name*/,
                     subspace::PluginContext * /*ctx*/) {}
void OnNewPublisher(subspace::Server & /*s*/,
                    const std::string & /*channel_name*/,
                    int /*publisher_id*/, subspace::PluginContext * /*ctx*/) {}
void OnRemovePublisher(subspace::Server & /*s*/,
                       const std::string & /*channel_name*/,
                       int /*publisher_id*/, subspace::PluginContext * /*ctx*/) {}
void OnNewSubscriber(subspace::Server & /*s*/,
                     const std::string & /*channel_name*/,
                     int /*subscriber_id*/, subspace::PluginContext * /*ctx*/) {}
void OnRemoveSubscriber(subspace::Server & /*s*/,
                        const std::string & /*channel_name*/,
                        int /*subscriber_id*/,
                        subspace::PluginContext * /*ctx*/) {}

absl::StatusOr<bool> OnFreeClientBuffer(
    subspace::Server & /*s*/,
    const subspace::ClientBufferHandleMetadata &metadata,
    subspace::PluginContext * /*ctx*/) {
  if (metadata.allocator !=
          subspace::ClientBufferAllocatorKind::kSplitCallback &&
      metadata.allocator !=
          subspace::ClientBufferAllocatorKind::kSplitBufferFreeTest) {
    return false;
  }

  const char *log_path = std::getenv("SUBSPACE_SPLIT_BUFFER_FREE_TEST_LOG");
  if (log_path == nullptr || log_path[0] == '\0') {
    return absl::InternalError("SUBSPACE_SPLIT_BUFFER_FREE_TEST_LOG is not set");
  }

  std::ofstream log(log_path, std::ios::app);
  if (!log.is_open()) {
    return absl::InternalError("failed to open split-buffer free test log");
  }
  log << metadata.channel_name << " " << metadata.session_id << " "
      << metadata.buffer_index << " " << metadata.slot_id << " "
      << metadata.handle << " "
      << subspace::ClientBufferAllocatorName(metadata.allocator) << "\n";
  return true;
}

} // namespace split_buffer_free_test_plugin

extern "C" {
subspace::PluginInterface *SPLIT_BUFFER_FREE_TEST_Create() {
  subspace::PluginInterfaceFunctions functions = {
      .onStartup = split_buffer_free_test_plugin::OnStartup,
      .onReady = split_buffer_free_test_plugin::OnReady,
      .onShutdown = split_buffer_free_test_plugin::OnShutdown,
      .onNewChannel = split_buffer_free_test_plugin::OnNewChannel,
      .onRemoveChannel = split_buffer_free_test_plugin::OnRemoveChannel,
      .onNewPublisher = split_buffer_free_test_plugin::OnNewPublisher,
      .onRemovePublisher = split_buffer_free_test_plugin::OnRemovePublisher,
      .onNewSubscriber = split_buffer_free_test_plugin::OnNewSubscriber,
      .onRemoveSubscriber = split_buffer_free_test_plugin::OnRemoveSubscriber,
      .onFreeClientBuffer = split_buffer_free_test_plugin::OnFreeClientBuffer,
  };
  return new subspace::PluginInterface(
      functions,
      std::make_unique<subspace::PluginContext>(
          "split_buffer_free_test_plugin"));
}
}
