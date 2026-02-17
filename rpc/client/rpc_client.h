// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "client/client.h"
#include "toolbelt/logging.h"

#include <chrono>
#include <type_traits>
#include <string_view>

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

template <typename Response> class ResponseReceiverBase {
public:
  ResponseReceiverBase() = default;
  virtual ~ResponseReceiverBase() = default;

  // Called when a response is received.
  virtual void OnResponse(Response &&response) = 0;
};

template <> class ResponseReceiverBase<void> {
public:
  ResponseReceiverBase() = default;
  virtual ~ResponseReceiverBase() = default;

  // Called when a response is received.
  virtual void OnResponse() = 0;
};
} // namespace client_internal

template <typename Response> class ResponseReceiver : public client_internal::ResponseReceiverBase<Response> {
public:
  ResponseReceiver() = default;
  ~ResponseReceiver() override = default;

  // Called when a response is received.
  // From ResponseReceiverBase:
  //   virtual void OnResponse(Response &&response) = 0;
  // Or, if Response == void:
  //   virtual void OnResponse() = 0;

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

  void SetInvocationDetails(std::shared_ptr<RpcClient> client,
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

  // Call a streaming function.  A class derived from the ResponseReceiver
  // interface is provided and will be used to receive responses.  This
  // function will block until all streaming responses have been received.
  // Request type can be:
  //  - A protobuf message type, or
  //  - A container of bytes (with data() and size() members).
  // Response type can be:
  //  - A protobuf message type, or
  //  - `void` (default if omitted) to ignore/discard the response, or
  //  - A container of bytes (constructible from two iterators).
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

  // This is only used for error reporting so a linear search is fine.
  std::string MethodName(int id) const {
    for (const auto &m : methods_) {
      if (m.second->id == id) {
        return m.second->name;
      }
    }
    return "unknown";
  }

  void Destroy() {
    client_.reset();
    service_sub_.reset();
    service_pub_.reset();
    methods_.clear();
    closed_ = true;
  }

  std::string service_;
  uint64_t client_id_;
  std::string subspace_server_socket_;
  toolbelt::Logger logger_;
  co::Coroutine *coroutine_ = nullptr;
  std::shared_ptr<subspace::Client> client_;
  absl::flat_hash_map<int, std::shared_ptr<client_internal::Method>> methods_;
  absl::flat_hash_map<std::string_view, int> method_name_to_id_;
  int session_id_ = 0;
  int next_request_id_ = 0;
  std::shared_ptr<subspace::Publisher> service_pub_;
  std::shared_ptr<subspace::Subscriber> service_sub_;
  bool closed_ = false;
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
        absl::StrFormat("Failed to invoke method '%s': %s", MethodName(method_id), r.status().ToString()));
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
        absl::StrFormat("No such method: '%s'", method_name));
  }
  return Call<Request, Response>(*method_id, request, timeout, c);
}

template <typename Request, typename Response>
inline absl::Status RpcClient::Call(int method_id, const Request &request,
                                    ResponseReceiver<Response> &receiver,
                                    std::chrono::nanoseconds timeout,
                                    co::Coroutine *c) {
  google::protobuf::Any any;
  if constexpr (std::is_base_of_v<google::protobuf::Message, Request>) {
    any.PackFrom(request);
  } else {
    RawMessage raw_request;
    raw_request.set_data(request.data(), request.size());
    any.PackFrom(raw_request);
  }
  any.PackFrom(request);
  auto status = InvokeMethod(
      method_id, any,
      [&receiver](std::shared_ptr<RpcClient> client, uint64_t client_id,
                  int session_id, int request_id,
                  std::shared_ptr<client_internal::Method> method,
                  const RpcResponse *response) {
        // For cancellation we need to store the IDs for the outgoing request.
        receiver.SetInvocationDetails(client, client_id, session_id, request_id,
                                      method);
        if (response->is_cancelled()) {
          receiver.OnCancel();
        } else if (!response->error().empty()) {
          receiver.OnError(absl::InternalError(response->error()));
        } else if (response->is_last() && !response->has_result()) {
          receiver.OnFinish();
        } else {
          if constexpr (std::is_base_of_v<google::protobuf::Message, Response>) {
            Response resp;
            if (!response->result().UnpackTo(&resp)) {
              receiver.OnError(absl::InternalError(
                  absl::StrFormat("Failed to unpack response of type %s",
                                  Response::descriptor()->full_name())));
            } else {
              receiver.OnResponse(std::move(resp));
            }
          } else if constexpr (std::is_same_v<Response, void>) {
            VoidMessage resp;
            if (!response->result().UnpackTo(&resp)) {
              receiver.OnError(absl::InternalError("Failed to unpack void response"));
            } else {
              receiver.OnResponse();
            }
          } else {
            RawMessage resp;
            if (!response->result().UnpackTo(&resp)) {
              receiver.OnError(absl::InternalError("Failed to unpack raw bytes response"));
            } else {
              receiver.OnResponse(Response(resp.data().begin(), resp.data().end()));
            }
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
        absl::StrFormat("No such method: '%s'", method_name));
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
