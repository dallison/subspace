#pragma once

#include "client/client.h"
#include "toolbelt/logging.h"

#include <chrono>
#include <type_traits>
#include <string_view>

namespace subspace {

class RpcClient {
public:
  // Create an RPC client.  The arguments are:
  // service: the RPC service to use - there must be an RPC server running on
  //     this service.
  // client_id: the ID of the client - unique per client (getpid() or
  //     gettid() maybe)
  // subspace_server_socket: the socket path for the subspace server
  RpcClient(std::string service, uint64_t client_id,
            std::string subspace_server_socket = "/tmp/subspace");
  ~RpcClient();

  // Set the logger level.  Valid levels are:
  // verbose, debug, info, warning, error
  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }

  // Open the RPC client and connect to the service passed to the constructor.
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

  absl::StatusOr<int> FindMethod(std::string_view method);

  template <typename Response>
  using CallResult = std::conditional_t<
      std::is_same_v<void, Response>, absl::Status, absl::StatusOr<Response>>;

  // Call with request and response.  A timeout will result in an error.
  // Request type can be:
  //  - A protobuf message type, or
  //  - A container of bytes (with data() and size() members).
  // Response type can be:
  //  - A protobuf message type, or
  //  - `void` (default if omitted) to ignore/discard the response, or
  //  - A container of bytes (constructible from two iterators).
  // For non-void response types, returns absl::StatusOr<Response>,
  // otherwise, returns absl::Status.
  template <typename Request, typename Response = void>
  CallResult<Response> Call(int method_id, const Request &request,
                            std::chrono::nanoseconds timeout,
                            co::Coroutine *c = nullptr);

  // Call with request and response, same as above, except this will
  // block (the coroutine if provided) and there is no timeout.
  template <typename Request, typename Response = void>
  CallResult<Response> Call(int method_id, const Request &request,
                            co::Coroutine *c = nullptr) {
    return Call<Request, Response>(method_id, request,
                                   std::chrono::nanoseconds(0), c);
  }

  // Call with request and response, same as above, but using the method's
  // name rather than its id.  A timeout will result in an error.
  template <typename Request, typename Response = void>
  CallResult<Response>
  Call(std::string_view method_name, const Request &request,
       std::chrono::nanoseconds timeout, co::Coroutine *c = nullptr);

  // Call with request and response, same as above, but using the method's
  // name rather than its id.  This will block (the coroutine if provided)
  // and there is no timeout.
  template <typename Request, typename Response = void>
  CallResult<Response> Call(std::string_view method_name,
                            const Request &request,
                            co::Coroutine *c = nullptr) {
    return Call<Request, Response>(method_name, request,
                                   std::chrono::nanoseconds(0), c);
  }

private:
  struct Method {
    std::string name;
    int id;
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

  // Low level method invocation functions that takes untyped protobuf.Any
  // request and response.
  absl::StatusOr<google::protobuf::Any>
  InvokeMethod(int method_id, const google::protobuf::Any &request,
               co::Coroutine *c = nullptr) {
    return InvokeMethod(method_id, request, std::chrono::nanoseconds(0), c);
  }

  absl::StatusOr<google::protobuf::Any>
  InvokeMethod(int method_id, const google::protobuf::Any &request,
               std::chrono::nanoseconds timeout, co::Coroutine *c = nullptr);

  std::string service_;
  uint64_t client_id_;
  std::string subspace_server_socket_;
  toolbelt::Logger logger_;
  co::Coroutine *coroutine_ = nullptr;
  std::shared_ptr<subspace::Client> client_;
  absl::flat_hash_map<int, std::shared_ptr<Method>> methods_;
  absl::flat_hash_map<std::string_view, int> method_name_to_id_;
  int session_id_ = 0;
  int next_request_id_ = 0;
  std::shared_ptr<subspace::Publisher> service_pub_;
  std::shared_ptr<subspace::Subscriber> service_sub_;
};

// Call with request and response.  Both types must be protobuf messages.  A
// timeout will result in an error.
template <typename Request, typename Response>
inline RpcClient::CallResult<Response>
RpcClient::Call(int method_id, const Request &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  google::protobuf::Any any;
  if constexpr (std::is_base_of_v<google::protobuf::Message, Request>) {
    any.PackFrom(request);
  } else {
    RawMessage raw_request;
    raw_request.set_data(request.data(), request.size());
    any.PackFrom(raw_request);
  }
  auto r = InvokeMethod(method_id, any, timeout, c);
  if (!r.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to invoke method: %s", r.status().ToString()));
  }
  if constexpr (std::is_base_of_v<google::protobuf::Message, Response>) {
    Response resp;
    if (!r->UnpackTo(&resp)) {
      return absl::InternalError(
          absl::StrFormat("Failed to unpack response of type %s",
                          Response::descriptor()->full_name()));
    }
    return resp;
  } else if constexpr (std::is_same_v<Response, void>) {
    VoidMessage resp;
    if (!r->UnpackTo(&resp)) {
      return absl::InternalError("Failed to unpack void response");
    }
    return absl::OkStatus();
  } else {
    RawMessage resp;
    if (!r->UnpackTo(&resp)) {
      return absl::InternalError("Failed to unpack raw bytes response");
    }
    return Response(resp.data().begin(), resp.data().end());
  }
}

// Call with request and response.  Both types must be protobuf messages.  A
// timeout will result in an error.
template <typename Request, typename Response>
inline RpcClient::CallResult<Response>
RpcClient::Call(std::string_view method_name, const Request &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: %s", method_name));
  }
  return Call<Request, Response>(*method_id, request, timeout, c);
}

} // namespace subspace
