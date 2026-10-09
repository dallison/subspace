// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asil_client/client.h"

#include <sys/mman.h>

namespace subspace {
namespace asil {

Client::~Client() {
  if (scb_ != nullptr) {
    (void)::munmap(scb_, sizeof(shm::SystemControlBlock));
  }
}

Error Client::Init(ServerConnection &connection, const char *client_name) {
  if (scb_ != nullptr) {
    return Error::kAlreadyInitialized;
  }
  InitReply reply;
  if (Error e = connection.Init(client_name, reply); e != Error::kOk) {
    return e;
  }
  void *scb = ::mmap(nullptr, sizeof(shm::SystemControlBlock), PROT_READ,
                     MAP_SHARED, reply.scb.Get(), 0);
  if (scb == MAP_FAILED) {
    return Error::kSharedMemoryError;
  }
  connection_ = &connection;
  scb_ = static_cast<shm::SystemControlBlock *>(scb);
  session_id_ = reply.session_id;
  user_id_ = reply.user_id;
  group_id_ = reply.group_id;
  return Error::kOk;
}

const char *Client::LastServerError() const {
  return connection_ == nullptr ? "" : connection_->LastServerError();
}

} // namespace asil
} // namespace subspace
