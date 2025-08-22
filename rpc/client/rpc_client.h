#pragma once

#include "client/client.h"
#include "toolbelt/logging.h"
#include <chrono>

namespace subspace {

class RpcClient {
public:
  RpcClient(std::string service, uint64_t client_id,
            std::string subspace_server_socket = "/tmp/subspace");
  ~RpcClient();

  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }
  absl::Status Open(co::Coroutine *c = nullptr) {
    return Open(std::chrono::nanoseconds(0), c);
  }
  absl::Status Open(std::chrono::nanoseconds timeout,
                    co::Coroutine *c = nullptr);

  absl::Status Close(co::Coroutine *c = nullptr) {
    return Close(std::chrono::nanoseconds(0), c);
  }
  absl::Status Close(std::chrono::nanoseconds timeout,
                     co::Coroutine *c = nullptr);

  absl::StatusOr<google::protobuf::Any>
  InvokeMethod(const std::string &name, const google::protobuf::Any &request,
               co::Coroutine *c = nullptr) {
    return InvokeMethod(name, request, std::chrono::nanoseconds(0), c);
  }

  absl::StatusOr<google::protobuf::Any>
  InvokeMethod(const std::string &name, const google::protobuf::Any &request,
               std::chrono::nanoseconds timeout, co::Coroutine *c = nullptr);

  template <typename Request, typename Response>
  absl::StatusOr<Response> Call(const std::string &method,
                                const Request &request,
                                co::Coroutine *c = nullptr) {
    return Call<Request, Response>(method, request, std::chrono::nanoseconds(0),
                                   c);
  }

  template <typename Request, typename Response>
  absl::StatusOr<Response>
  Call(const std::string &method, const Request &request,
       std::chrono::nanoseconds timeout, co::Coroutine *c = nullptr) {
    google::protobuf::Any any;
    any.PackFrom(request);
    auto r = InvokeMethod(method, any, timeout, c);
    if (!r.ok()) {
      return absl::InternalError(absl::StrFormat("Failed to invoke method: %s",
                                                 r.status().ToString()));
    }
    Response resp;
    std::cerr << "Received response: " << r->DebugString() << std::endl;
    if (!r->UnpackTo(&resp)) {
      return absl::InternalError("3 Failed to unpack response");
    }
    return resp;
  }

  // Call with a raw message
  absl::StatusOr<std::vector<char>> Call(const std::string &method,
                                         const std::vector<char> &request,
                                         std::chrono::nanoseconds timeout,
                                         co::Coroutine *c = nullptr) {
    RawMessage raw_request;
    raw_request.set_data(request.data(), request.size());
    google::protobuf::Any any;
    any.PackFrom(raw_request);
    auto r = InvokeMethod(method, any, timeout, c);
    if (!r.ok()) {
      return absl::InternalError(absl::StrFormat("Failed to invoke method: %s",
                                                 r.status().ToString()));
    }
    RawMessage resp;
    if (!r->UnpackTo(&resp)) {
      return absl::InternalError("1 Failed to unpack response");
    }
    return std::vector<char>(resp.data().begin(), resp.data().end());
  }

  // Call void method.
  template <typename Request>
  absl::Status Call(const std::string &method, const Request &request,
                    co::Coroutine *c = nullptr) {
    google::protobuf::Any any;
    any.PackFrom(request);
    auto r = InvokeMethod(method, any, c);
    if (!r.ok()) {
      return absl::InternalError(absl::StrFormat("Failed to invoke method: %s",
                                                 r.status().ToString()));
    }
    VoidMessage resp;
    if (!r->UnpackTo(&resp)) {
      return absl::InternalError("2 Failed to unpack response");
    }
    return absl::OkStatus();
  }

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

  absl::Status OpenService(std::chrono::nanoseconds timeout, co::Coroutine *c);
  absl::Status CloseService(std::chrono::nanoseconds timeout, co::Coroutine *c);
  absl::Status PublishServerRequest(const subspace::RpcServerRequest &req,
                                    std::chrono::nanoseconds timeout,
                                    co::Coroutine *c = nullptr);
  absl::StatusOr<subspace::RpcServerResponse>
  ReadServerResponse(int request_id, std::chrono::nanoseconds timeout,
                     co::Coroutine *c = nullptr);

  std::string service_;
  uint64_t client_id_;
  std::string subspace_server_socket_;
  toolbelt::Logger logger_;
  co::Coroutine *coroutine_ = nullptr;
  std::shared_ptr<subspace::Client> client_;
  absl::flat_hash_map<std::string, std::shared_ptr<Method>> methods_;
  int session_id_ = 0;
  int next_request_id_ = 0;
  std::shared_ptr<subspace::Publisher> service_pub_;
  std::shared_ptr<subspace::Subscriber> service_sub_;
};

} // namespace subspace
