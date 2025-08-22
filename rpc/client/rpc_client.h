#pragma once

#include "client/client.h"

namespace subspace {

class RpcClient {
public:
  RpcClient(std::string service,
            std::string subspace_server_socket = "/tmp/subspace");
  ~RpcClient();

  absl::Status Init(co::Coroutine* c = nullptr);

  absl::StatusOr<google::protobuf::Any>
  InvokeMethod(const std::string &name, const google::protobuf::Any &request, co::Coroutine* c = nullptr);

private:
  struct Method {
    std::string name;
    std::string request_type;
    std::string response_type;
    int32_t slot_size;
    int32_t num_slots;
    std::shared_ptr<subspace::Publisher> request_publisher;
    std::shared_ptr<subspace::Subscriber> response_subscriber;
  };

  absl::Status OpenService(co::Coroutine* c);
  absl::Status CloseService(co::Coroutine* c);
  absl::Status PublishServerRequest(
      const subspace::RpcServerRequest &req, co::Coroutine *c = nullptr);
  absl::StatusOr<subspace::RpcServerResponse> ReadServerResponse(co::Coroutine *c = nullptr);

  std::string service_;
  std::string subspace_server_socket_;
  std::shared_ptr<subspace::Client> client_;
  absl::flat_hash_map<std::string, std::shared_ptr<Method>> methods_;
  int session_id_ = 0;
  int next_request_id_ = 0;
  std::shared_ptr<subspace::Publisher> service_pub_;
  std::shared_ptr<subspace::Subscriber> service_sub_;
};

} // namespace subspace
