#include "rpc/server/rpc_server.h"
#include "proto/subspace.pb.h"

namespace subspace {
RpcServer::RpcServer(std::string name) : name_(std::move(name)) {}

void RpcServer::Run() {}
} // namespace subspace
