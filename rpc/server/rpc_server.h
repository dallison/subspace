#pragma once
#include "absl/container/flat_hash_map.h"
#include "coroutine.h"
#include "client/client.h"
namespace subspace {
class RpcServer {
public:
  RpcServer(std::string name);

  void Run();

  absl::Status
  RegisterMethod(const std::string &method, const std::string &type,
                 std::function<absl::Status(const subspace::RpcRequest &,
                                            subspace::RpcResponse &)>
                     callback) {
    if (methods_.find(method) != methods_.end()) {
      return absl::AlreadyExistsError("Method already registered: " + method);
    }
    methods_[method] = std::move(callback);
    return absl::OkStatus();
  }

  absl::Status UnregisterMethod(const std::string &method) {
    auto it = methods_.find(method);
    if (it == methods_.end()) {
      return absl::NotFoundError("Method not found: " + method);
    }
    methods_.erase(it);
    return absl::OkStatus();
  }

private:
  std::string name_;
  co::CoroutineScheduler scheduler_;
  std::shared_ptr<Client> subspace_client_;
  absl::flat_hash_map<std::string,
                      std::function<absl::Status(const subspace::RpcRequest &,
                                                 subspace::RpcResponse &)>>
      methods_;
};

} // namespace subspace