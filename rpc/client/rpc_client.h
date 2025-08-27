#pragma once

#include "client/client.h"
#include "toolbelt/logging.h"
#include <chrono>

namespace subspace {

class RpcClient;

constexpr int32_t kCancelChannelSlotSize = 64;
constexpr int32_t kCancelChannelNumSlots = 8;

namespace client_internal {
struct Method {
  std::string name;
  int id;
  std::string request_type;
  std::string response_type;
  int32_t slot_size;
  int32_t num_slots;
  std::shared_ptr<subspace::Publisher> request_publisher;
  std::shared_ptr<subspace::Subscriber> response_subscriber;
  std::shared_ptr<subspace::Publisher> cancel_publisher;
};
} // namespace client_internal

template <typename Response> class ResponseReceiver {
public:
  ResponseReceiver() = default;
  virtual ~ResponseReceiver() = default;

  // Called when a response is received.
  virtual void OnResponse(const Response &response) = 0;

  virtual void OnFinish() = 0;

  // Called when an error occurs.
  virtual void OnError(const absl::Status &status) = 0;

  virtual void OnCancel() = 0;

  bool IsCancelled() const { return cancelled_; }

  absl::Status Cancel(std::chrono::nanoseconds timeout,
                      co::Coroutine *c = nullptr);

  absl::Status Cancel(co::Coroutine *c = nullptr) {
    return Cancel(std::chrono::nanoseconds(0), c);
  }

  void SetInvokationDetails(std::shared_ptr<RpcClient> client,
                            uint64_t client_id, int session_id, int request_id,
                            std::shared_ptr<client_internal::Method> method) {
    if (client_ != nullptr) {
      return;
    }
    client_id_ = client_id;
    session_id_ = session_id;
    request_id_ = request_id;
    method_ = method;
    client_ = client;
  }

private:
  bool cancelled_ = false;
  uint64_t client_id_ = 0;
  int session_id_ = 0;
  int request_id_ = 0;
  std::shared_ptr<client_internal::Method> method_ = nullptr;
  std::shared_ptr<RpcClient> client_ = nullptr;
};

class RpcClient : public std::enable_shared_from_this<RpcClient> {
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

  // Call with a void raw message.  There is a response internally but you
  // don't see it.
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

  // Call with a void raw message.  There is a response internally but you
  // don't see it.
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

  // Call a streaming function.  A class derived from the ResponseReceiver
  // interface is provided and will be used to receive responses.  This
  // function will block until all streaming responses have been received.
  template <typename Request, typename Response>
  absl::Status Call(int method_id, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    std::chrono::nanoseconds timeout,
                    co::Coroutine *c = nullptr);

  template <typename Request, typename Response>
  absl::Status Call(const std::string &method_name, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    std::chrono::nanoseconds timeout,
                    co::Coroutine *c = nullptr);

  template <typename Request, typename Response>
  absl::Status Call(int method_id, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    co::Coroutine *c = nullptr) {
    return Call(method_id, request, receiver, std::chrono::nanoseconds(0), c);
  }

  template <typename Request, typename Response>
  absl::Status Call(const std::string &method_name, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    co::Coroutine *c = nullptr) {
    return Call(method_name, request, receiver, std::chrono::nanoseconds(0), c);
  }

  absl::Status CancelRequest(uint64_t client_id, int session_id, int request_id,
                             std::shared_ptr<client_internal::Method> method,
                             std::chrono::nanoseconds timeout,
                             co::Coroutine *c);

private:
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

  absl::Status
  InvokeMethod(int method_id, const google::protobuf::Any &request,
               std::function<void(std::shared_ptr<RpcClient>, uint64_t, int,
                                  int, std::shared_ptr<client_internal::Method>,
                                  const RpcResponse *)>
                   response_handler,
               std::chrono::nanoseconds timeout, co::Coroutine *c = nullptr);

  std::string service_;
  uint64_t client_id_;
  std::string subspace_server_socket_;
  toolbelt::Logger logger_;
  co::Coroutine *coroutine_ = nullptr;
  std::shared_ptr<subspace::Client> client_;
  absl::flat_hash_map<int, std::shared_ptr<client_internal::Method>> methods_;
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
  return Call<Request, Response>(*method_id, request, c);
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
  return Call<Request, Response>(*method_id, request, timeout, c);
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

template <typename Request, typename Response>
inline absl::Status RpcClient::Call(int method_id, const Request &request,
                                    ResponseReceiver<Response> &receiver,
                                    std::chrono::nanoseconds timeout,
                                    co::Coroutine *c) {
  google::protobuf::Any any;
  any.PackFrom(request);
  auto status = InvokeMethod(
      method_id, any,
      [&receiver](std::shared_ptr<RpcClient> client, uint64_t client_id,
                  int session_id, int request_id,
                  std::shared_ptr<client_internal::Method> method,
                  const RpcResponse *response) {
        // For cancellation we need to store the IDs for the outgoing request.
        receiver.SetInvokationDetails(client, client_id, session_id, request_id,
                                      method);
        if (response->is_cancelled()) {
          receiver.OnCancel();
        } else if (!response->error().empty()) {
          receiver.OnError(absl::InternalError(response->error()));
        } else if (response->is_last() && !response->has_result()) {
          receiver.OnFinish();
        } else {
          Response resp;
          if (!response->result().UnpackTo(&resp)) {
            receiver.OnError(absl::InternalError("5 Failed to unpack response"));
          } else {
            receiver.OnResponse(resp);
          }
        }
      },
      timeout, c);
  if (!status.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to invoke method: %s", status.ToString()));
  }
  return absl::OkStatus();
}

template <typename Request, typename Response>
inline absl::Status
RpcClient::Call(const std::string &method_name, const Request &request,
                ResponseReceiver<Response> &receiver,
                std::chrono::nanoseconds timeout, co::Coroutine *c) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: %s", method_name));
  }
  return Call(*method_id, request, receiver, timeout, c);
}

template <typename Response>
inline absl::Status
ResponseReceiver<Response>::Cancel(std::chrono::nanoseconds timeout,
                                   co::Coroutine *c) {
  if (IsCancelled()) {
    return absl::OkStatus();
  }
  if (auto status = client_->CancelRequest(client_id_, session_id_, request_id_,
                                           method_, timeout, c);
      !status.ok()) {
    return status;
  }
  cancelled_ = true;
  return absl::OkStatus();
}
} // namespace subspace
