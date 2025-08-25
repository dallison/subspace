#pragma once

#include "client/client.h"
#include "toolbelt/logging.h"
#include <chrono>

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

  absl::StatusOr<int> FindMethod(const std::string &method);

  // Call with request and response (both protobuf types).  This will
  // block (the coroutine if provided) and there is no timeout.
  template <typename Request, typename Response>
  absl::StatusOr<Response> Call(int method_id, const Request &request,
                                co::Coroutine *c = nullptr) {
    return Call<Request, Response>(method_id, request,
                                   std::chrono::nanoseconds(0), c);
  }

  // Call with request and response.  Both types must be protobuf messages.  A
  // timeout will result in an error.
  template <typename Request, typename Response>
  absl::StatusOr<Response> Call(int method_id, const Request &request,
                                std::chrono::nanoseconds timeout,
                                co::Coroutine *c = nullptr);

  // Call with a raw message.  You will get a raw buffer back.
  absl::StatusOr<std::vector<char>> Call(int method_id,
                                         const std::vector<char> &request,
                                         std::chrono::nanoseconds timeout,
                                         co::Coroutine *c = nullptr);

  // Call void method.
  template <typename Request>
  absl::Status Call(int method_id, const Request &request,
                    co::Coroutine *c = nullptr) {
    return Call(method_id, request, std::chrono::nanoseconds(0), c);
  }

  // Call void method.
  template <typename Request>
  absl::Status Call(int method_id, const Request &request,
                    std::chrono::nanoseconds timeout,
                    co::Coroutine *c = nullptr);

  // Call with a void raw message.  There is a response internally but you don't
  // see it.
  template <>
  absl::Status Call(int method_id, const std::vector<char> &request,
                    co::Coroutine *c) {
    return Call<std::vector<char>>(method_id, request,
                                   std::chrono::nanoseconds(0), c);
  }

  template <>
  absl::Status Call(int method_id, const std::vector<char> &request,
                    std::chrono::nanoseconds timeout, co::Coroutine *c);

  // Calls with method name.
  // Call with request and response (both protobuf types).  This will
  // block (the coroutine if provided) and there is no timeout.
  template <typename Request, typename Response>
  absl::StatusOr<Response> Call(const std::string &method_name,
                                const Request &request,
                                co::Coroutine *c = nullptr);

  // Call with request and response.  Both types must be protobuf messages.  A
  // timeout will result in an error.
  template <typename Request, typename Response>
  absl::StatusOr<Response>
  Call(const std::string &method_name, const Request &request,
       std::chrono::nanoseconds timeout, co::Coroutine *c = nullptr);

  // Call with a raw message.  You will get a raw buffer back.
  absl::StatusOr<std::vector<char>> Call(const std::string &method_name,
                                         const std::vector<char> &request,
                                         std::chrono::nanoseconds timeout,
                                         co::Coroutine *c = nullptr);

  // Call void method.
  template <typename Request>
  absl::Status Call(const std::string &method_name, const Request &request,
                    co::Coroutine *c = nullptr) {
    return Call(method_name, request, std::chrono::nanoseconds(0), c);
  }

  // Call void method.
  template <typename Request>
  absl::Status Call(const std::string &method_name, const Request &request,
                    std::chrono::nanoseconds timeout,
                    co::Coroutine *c = nullptr);

  // Call with a void raw message.  There is a response internally but you don't
  // see it.
  template <>
  absl::Status Call(const std::string &method_name,
                    const std::vector<char> &request, co::Coroutine *c) {
    return Call<std::vector<char>>(method_name, request,
                                   std::chrono::nanoseconds(0), c);
  }

  template <>
  absl::Status Call(const std::string &method_name,
                    const std::vector<char> &request,
                    std::chrono::nanoseconds timeout, co::Coroutine *c);

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
  absl::flat_hash_map<std::string, int> method_name_to_id_;
  int session_id_ = 0;
  int next_request_id_ = 0;
  std::shared_ptr<subspace::Publisher> service_pub_;
  std::shared_ptr<subspace::Subscriber> service_sub_;
};

// Call with request and response.  Both types must be protobuf messages.  A
// timeout will result in an error.
template <typename Request, typename Response>
inline absl::StatusOr<Response>
RpcClient::Call(int method_id, const Request &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  google::protobuf::Any any;
  any.PackFrom(request);
  auto r = InvokeMethod(method_id, any, timeout, c);
  if (!r.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to invoke method: %s", r.status().ToString()));
  }
  Response resp;
  if (!r->UnpackTo(&resp)) {
    return absl::InternalError("3 Failed to unpack response");
  }
  return resp;
}

// Call with a raw message.  You will get a raw buffer back.
inline absl::StatusOr<std::vector<char>>
RpcClient::Call(int method_id, const std::vector<char> &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  RawMessage raw_request;
  raw_request.set_data(request.data(), request.size());
  google::protobuf::Any any;
  any.PackFrom(raw_request);
  auto r = InvokeMethod(method_id, any, timeout, c);
  if (!r.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to invoke method: %s", r.status().ToString()));
  }
  RawMessage resp;
  if (!r->UnpackTo(&resp)) {
    return absl::InternalError("1 Failed to unpack response");
  }
  return std::vector<char>(resp.data().begin(), resp.data().end());
}

// Call void method.  There is an internal response message sent but it is
// absorbed.
template <typename Request>
inline absl::Status RpcClient::Call(int method_id, const Request &request,
                                    std::chrono::nanoseconds timeout,
                                    co::Coroutine *c) {
  google::protobuf::Any any;
  any.PackFrom(request);
  auto r = InvokeMethod(method_id, any, timeout, c);
  if (!r.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to invoke method: %s", r.status().ToString()));
  }
  VoidMessage resp;
  if (!r->UnpackTo(&resp)) {
    return absl::InternalError("2 Failed to unpack response");
  }
  return absl::OkStatus();
}

// Call with a void raw message.  There is a response internally but you don't
// see it.
template <>
inline absl::Status
RpcClient::Call(int method_id, const std::vector<char> &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  RawMessage raw_request;
  raw_request.set_data(request.data(), request.size());
  google::protobuf::Any any;
  any.PackFrom(raw_request);
  auto r = InvokeMethod(method_id, any, timeout, c);
  if (!r.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to invoke method: %s", r.status().ToString()));
  }
  VoidMessage resp;
  if (!r->UnpackTo(&resp)) {
    return absl::InternalError("2 Failed to unpack response");
  }
  return absl::OkStatus();
}

template <typename Request, typename Response>
inline absl::StatusOr<Response> RpcClient::Call(const std::string &method_name,
                                         const Request &request,
                                         co::Coroutine *c) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: %s", method_name));
  }
  return Call<Request,Response>(*method_id, request, c);
}

// Call with request and response.  Both types must be protobuf messages.  A
// timeout will result in an error.
template <typename Request, typename Response>
inline absl::StatusOr<Response>
RpcClient::Call(const std::string &method_name, const Request &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: %s", method_name));
  }
  return Call<Request,Response>(*method_id, request, timeout, c);
}

// Call with a raw message.  You will get a raw buffer back.
inline absl::StatusOr<std::vector<char>>
RpcClient::Call(const std::string &method_name,
                const std::vector<char> &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: %s", method_name));
  }
  return Call(*method_id, request, timeout, c);
}

// Call void method.
template <typename Request>
inline absl::Status
RpcClient::Call(const std::string &method_name, const Request &request,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: %s", method_name));
  }
  return Call<Request>(*method_id, request, timeout, c);
}

template <>
inline absl::Status RpcClient::Call(const std::string &method_name,
                             const std::vector<char> &request,
                             std::chrono::nanoseconds timeout,
                             co::Coroutine *c) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: %s", method_name));
  }
  RawMessage raw_request;
  raw_request.set_data(request.data(), request.size());
  google::protobuf::Any any;
  any.PackFrom(raw_request);
  auto r = InvokeMethod(*method_id, any, timeout, c);
  if (!r.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to invoke method: %s", r.status().ToString()));
  }
  VoidMessage resp;
  if (!r->UnpackTo(&resp)) {
    return absl::InternalError("2 Failed to unpack response");
  }
  return absl::OkStatus();
}
} // namespace subspace
