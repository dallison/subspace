// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include <atomic>

namespace subspace {

// In multithreaded tests we can't dlclose the plugins because the dynamic
// linker doesn't play well with threads.
static std::atomic<bool> close_plugins_on_shutdown = false;
void ClosePluginsOnShutdown() { close_plugins_on_shutdown = true; }

bool ShouldClosePluginsOnShutdown() { return close_plugins_on_shutdown; }

absl::Status Server::LoadPlugin(const std::string &name,
                                const std::string &path) {
  void *handle = nullptr;
  bool builtin = (path == "BUILTIN");
  if (!builtin) {
    handle = dlopen(path.c_str(), RTLD_LAZY);
    if (handle == nullptr) {
      return absl::InternalError(
          absl::StrFormat("Can't open plugin file %s: %s", path, dlerror()));
    }
  }
  // Form the name of the init function and find it in the shared object.
  std::string interfaceFunc = absl::StrFormat("%s_Create", name);
  void *func = dlsym(builtin ? RTLD_DEFAULT : handle, interfaceFunc.c_str());
  if (func == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Can't find plugin initialization symbol %s: %s",
                        interfaceFunc, dlerror()));
  }
  // Call the init function to get the interface.
  using InitFunc = PluginInterface *(*)();
  InitFunc init = reinterpret_cast<InitFunc>(func);
  std::unique_ptr<PluginInterface> interface(init());

  return RegisterPlugin(name, handle, std::move(interface));
}

absl::Status
Server::LoadBuiltinPlugin(const std::string &name,
                          std::unique_ptr<PluginInterface> interface) {
  return RegisterPlugin(name, nullptr, std::move(interface));
}

absl::Status
Server::RegisterPlugin(const std::string &name, void *handle,
                       std::unique_ptr<PluginInterface> interface) {
  // Call the OnStartup function in the loaded plugin.
  absl::Status status = interface->OnStartup(*this, name);
  if (!status.ok()) {
    return status;
  }
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO
  interface->SetScheduler(runtime_.scheduler());
#endif
  plugins_.push_back(
      std::make_unique<Plugin>(name, handle, std::move(interface)));
  return absl::OkStatus();
}

absl::Status Server::UnloadPlugin(const std::string &name) {
  auto it = std::find_if(
      plugins_.begin(), plugins_.end(),
      [&name](const std::unique_ptr<Plugin> &p) { return p->name == name; });
  if (it == plugins_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("No such plugin %s loaded", name.c_str()));
  }
  plugins_.erase(it);
  return absl::OkStatus();
}

void Server::OnReady() {
  for (const auto &plugin : plugins_) {
    plugin->interface->OnReady(*this);
  }
}

void Server::OnNewChannel(const std::string &channel_name) {
  if (!IsPublicChannel(FindChannel(channel_name))) {
    return;
  }
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewChannel(*this, channel_name);
  }
}

void Server::OnRemoveChannel(const std::string &channel_name) {
  if (!IsPublicChannel(FindChannel(channel_name))) {
    return;
  }
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemoveChannel(*this, channel_name);
  }
}

void Server::OnNewPublisher(const std::string &channel_name, int publisher_id) {
  ServerChannel *channel = FindChannel(channel_name);
  if (channel != nullptr) {
    absl::StatusOr<User *> user = channel->GetUser(publisher_id);
    if (user.ok()) {
      RecordTelemetryParticipantChange(channel, *user, /*added=*/true);
    }
  }
  if (!IsPublicChannel(channel)) {
    return;
  }
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewPublisher(*this, channel_name, publisher_id);
  }
}

void Server::OnRemovePublisher(const std::string &channel_name,
                               int publisher_id) {
  ServerChannel *channel = FindChannel(channel_name);
  if (channel != nullptr) {
    absl::StatusOr<User *> user = channel->GetUser(publisher_id);
    if (user.ok()) {
      RecordTelemetryParticipantChange(channel, *user, /*added=*/false);
    }
  }
  if (!IsPublicChannel(channel)) {
    return;
  }
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemovePublisher(*this, channel_name, publisher_id);
  }
}

void Server::OnNewSubscriber(const std::string &channel_name,
                             int subscriber_id) {
  ServerChannel *channel = FindChannel(channel_name);
  if (channel != nullptr) {
    absl::StatusOr<User *> user = channel->GetUser(subscriber_id);
    if (user.ok()) {
      RecordTelemetryParticipantChange(channel, *user, /*added=*/true);
    }
  }
  if (!IsPublicChannel(channel)) {
    return;
  }
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewSubscriber(*this, channel_name, subscriber_id);
  }
}

void Server::OnRemoveSubscriber(const std::string &channel_name,
                                int subscriber_id) {
  ServerChannel *channel = FindChannel(channel_name);
  if (channel != nullptr) {
    absl::StatusOr<User *> user = channel->GetUser(subscriber_id);
    if (user.ok()) {
      RecordTelemetryParticipantChange(channel, *user, /*added=*/false);
    }
  }
  if (!IsPublicChannel(channel)) {
    return;
  }
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemoveSubscriber(*this, channel_name, subscriber_id);
  }
}

absl::StatusOr<bool> Server::FreeClientBufferWithPlugins(
    const ClientBufferHandleMetadata &metadata) {
  for (const auto &plugin : plugins_) {
    absl::StatusOr<bool> freed =
        plugin->interface->OnFreeClientBuffer(*this, metadata);
    if (!freed.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Plugin %s failed to free client buffer for channel %s "
                  "buffer %u slot %u handle 0x%zx: %s",
                  plugin->name.c_str(), metadata.channel_name.c_str(),
                  metadata.buffer_index, metadata.slot_id,
                  static_cast<size_t>(metadata.handle),
                  freed.status().ToString().c_str());
      continue;
    }
    if (*freed) {
      return true;
    }
  }
  return false;
}

} // namespace subspace
