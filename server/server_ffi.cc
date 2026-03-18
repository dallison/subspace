// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/server_ffi.h"
#include "co/coroutine.h"
#include "server/server.h"
#include <memory>
#include <string>

struct SubspaceServerHandle {
  co::CoroutineScheduler scheduler;
  std::unique_ptr<subspace::Server> server;
  std::string socket_name;
};

extern "C" {

SubspaceServerHandle *subspace_server_create(const char *socket_name,
                                             int notify_fd) {
  auto *handle = new SubspaceServerHandle();
  handle->socket_name = socket_name;
  subspace::ClosePluginsOnShutdown();
  handle->server = std::make_unique<subspace::Server>(
      handle->scheduler, handle->socket_name, "",
      /*disc_port=*/0, /*peer_port=*/0, /*local=*/true, notify_fd);
  return handle;
}

int subspace_server_run(SubspaceServerHandle *handle) {
  absl::Status status = handle->server->Run();
  return status.ok() ? 0 : -1;
}

void subspace_server_stop(SubspaceServerHandle *handle) {
  handle->server->Stop();
}

void subspace_server_cleanup_after_session(SubspaceServerHandle *handle) {
  handle->server->CleanupAfterSession();
}

void subspace_server_destroy(SubspaceServerHandle *handle) { delete handle; }

} // extern "C"
