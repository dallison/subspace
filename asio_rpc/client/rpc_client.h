// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "client/client.h"
#include "rpc/common/rpc_common.h"
#include "toolbelt/logging.h"

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <chrono>
#include <string_view>
#include <type_traits>

namespace subspace::asio_rpc {

class RpcClient;

template <typename Response>
class ResponseReceiver
    : public client_internal::ResponseReceiverBase<Response> {
public:
  ResponseReceiver() = default;
  ~ResponseReceiver() override = default;

  virtual void OnFinish() = 0;
  virtual void OnError(const absl::Status &status) = 0;
  virtual void OnCancel() = 0;

  bool IsCancelled() const { return cancelled_; }

  absl::Status Cancel(std::chrono::nanoseconds timeout,
                      boost::asio::yield_context yield);

  absl::Status Cancel(boost::asio::yield_context yield) {
    return Cancel(std::chrono::nanoseconds(0), yield);
  }

  // Blocking cancel (no coroutine).
  absl::Status Cancel(std::chrono::nanoseconds timeout = {});

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
  RpcClient(std::string service, uint64_t client_id,
            std::string subspace_server_socket = "/tmp/subspace");
  ~RpcClient();

  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }

  // Open with yield_context.
  absl::Status Open(boost::asio::yield_context yield) {
    return Open(std::chrono::nanoseconds(0), yield);
  }
  absl::Status Open(std::chrono::nanoseconds timeout,
                    boost::asio::yield_context yield);

  // Blocking open (no coroutine).
  absl::Status Open(std::chrono::nanoseconds timeout = {});

  // Close with yield_context.
  absl::Status Close(boost::asio::yield_context yield) {
    return Close(std::chrono::nanoseconds(0), yield);
  }
  absl::Status Close(std::chrono::nanoseconds timeout,
                     boost::asio::yield_context yield);

  // Blocking close (no coroutine).
  absl::Status Close(std::chrono::nanoseconds timeout = {});

  absl::StatusOr<int> FindMethod(std::string_view method);

  template <typename Response>
  using CallResult = std::conditional_t<std::is_same_v<void, Response>,
                                        absl::Status, absl::StatusOr<Response>>;

  template <typename Request, typename Response = void>
  CallResult<Response> Call(int method_id, const Request &request,
                            std::chrono::nanoseconds timeout,
                            boost::asio::yield_context yield);

  template <typename Request, typename Response = void>
  CallResult<Response> Call(int method_id, const Request &request,
                            boost::asio::yield_context yield) {
    return Call<Request, Response>(method_id, request,
                                  std::chrono::nanoseconds(0), yield);
  }

  template <typename Request, typename Response = void>
  CallResult<Response> Call(std::string_view method_name,
                            const Request &request,
                            std::chrono::nanoseconds timeout,
                            boost::asio::yield_context yield);

  template <typename Request, typename Response = void>
  CallResult<Response> Call(std::string_view method_name,
                            const Request &request,
                            boost::asio::yield_context yield) {
    return Call<Request, Response>(method_name, request,
                                  std::chrono::nanoseconds(0), yield);
  }

  // Streaming calls.
  template <typename Request, typename Response>
  absl::Status Call(int method_id, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    std::chrono::nanoseconds timeout,
                    boost::asio::yield_context yield);

  template <typename Request, typename Response>
  absl::Status Call(const std::string &method_name, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    std::chrono::nanoseconds timeout,
                    boost::asio::yield_context yield);

  template <typename Request, typename Response>
  absl::Status Call(int method_id, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    boost::asio::yield_context yield) {
    return Call(method_id, request, receiver, std::chrono::nanoseconds(0),
                yield);
  }

  template <typename Request, typename Response>
  absl::Status Call(const std::string &method_name, const Request &request,
                    ResponseReceiver<Response> &receiver,
                    boost::asio::yield_context yield) {
    return Call(method_name, request, receiver, std::chrono::nanoseconds(0),
                yield);
  }

  absl::Status CancelRequest(uint64_t client_id, int session_id,
                              int request_id,
                              std::shared_ptr<client_internal::Method> method,
                              std::chrono::nanoseconds timeout,
                              boost::asio::yield_context yield);

  // Blocking cancel (for ResponseReceiver::Cancel without yield).
  absl::Status CancelRequest(uint64_t client_id, int session_id,
                              int request_id,
                              std::shared_ptr<client_internal::Method> method,
                              std::chrono::nanoseconds timeout);

private:
  absl::Status OpenService(std::chrono::nanoseconds timeout,
                           boost::asio::yield_context yield);
  absl::Status CloseService(std::chrono::nanoseconds timeout,
                            boost::asio::yield_context yield);
  absl::Status PublishServerRequest(const subspace::RpcServerRequest &req,
                                    std::chrono::nanoseconds timeout,
                                    boost::asio::yield_context yield);
  absl::StatusOr<subspace::RpcServerResponse>
  ReadServerResponse(int request_id, std::chrono::nanoseconds timeout,
                     boost::asio::yield_context yield);

  absl::StatusOr<google::protobuf::Any>
  InvokeMethod(int method_id, const google::protobuf::Any &request,
               boost::asio::yield_context yield) {
    return InvokeMethod(method_id, request, std::chrono::nanoseconds(0), yield);
  }

  absl::StatusOr<google::protobuf::Any>
  InvokeMethod(int method_id, const google::protobuf::Any &request,
               std::chrono::nanoseconds timeout,
               boost::asio::yield_context yield);

  absl::Status
  InvokeMethod(int method_id, const google::protobuf::Any &request,
               std::function<void(std::shared_ptr<RpcClient>, uint64_t, int,
                                  int, std::shared_ptr<client_internal::Method>,
                                  const RpcResponse *)>
                   response_handler,
               std::chrono::nanoseconds timeout,
               boost::asio::yield_context yield);

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
  std::shared_ptr<subspace::Client> client_;
  absl::flat_hash_map<int, std::shared_ptr<client_internal::Method>> methods_;
  absl::flat_hash_map<std::string_view, int> method_name_to_id_;
  int session_id_ = 0;
  int next_request_id_ = 0;
  std::shared_ptr<subspace::Publisher> service_pub_;
  std::shared_ptr<subspace::Subscriber> service_sub_;
  bool closed_ = false;
};

template <typename Request, typename Response>
inline RpcClient::CallResult<Response>
RpcClient::Call(int method_id, const Request &request,
                std::chrono::nanoseconds timeout,
                boost::asio::yield_context yield) {
  google::protobuf::Any any;
  if constexpr (std::is_base_of_v<google::protobuf::Message, Request>) {
    any.PackFrom(request);
  } else {
    RawMessage raw_request;
    raw_request.set_data(request.data(), request.size());
    any.PackFrom(raw_request);
  }
  auto r = InvokeMethod(method_id, any, timeout, yield);
  if (!r.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to invoke method '%s': %s", MethodName(method_id),
        r.status().ToString()));
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

template <typename Request, typename Response>
inline RpcClient::CallResult<Response>
RpcClient::Call(std::string_view method_name, const Request &request,
                std::chrono::nanoseconds timeout,
                boost::asio::yield_context yield) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: '%s'", method_name));
  }
  return Call<Request, Response>(*method_id, request, timeout, yield);
}

template <typename Request, typename Response>
inline absl::Status RpcClient::Call(int method_id, const Request &request,
                                    ResponseReceiver<Response> &receiver,
                                    std::chrono::nanoseconds timeout,
                                    boost::asio::yield_context yield) {
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
        receiver.SetInvocationDetails(client, client_id, session_id, request_id,
                                      method);
        if (response->is_cancelled()) {
          receiver.OnCancel();
        } else if (!response->error().empty()) {
          receiver.OnError(absl::InternalError(response->error()));
        } else if (response->is_last() && !response->has_result()) {
          receiver.OnFinish();
        } else {
          if constexpr (std::is_base_of_v<google::protobuf::Message,
                                          Response>) {
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
              receiver.OnError(
                  absl::InternalError("Failed to unpack void response"));
            } else {
              receiver.OnResponse();
            }
          } else {
            RawMessage resp;
            if (!response->result().UnpackTo(&resp)) {
              receiver.OnError(
                  absl::InternalError("Failed to unpack raw bytes response"));
            } else {
              receiver.OnResponse(
                  Response(resp.data().begin(), resp.data().end()));
            }
          }
        }
      },
      timeout, yield);
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
                std::chrono::nanoseconds timeout,
                boost::asio::yield_context yield) {
  auto method_id = FindMethod(method_name);
  if (!method_id.ok()) {
    return absl::InternalError(
        absl::StrFormat("No such method: '%s'", method_name));
  }
  return Call(*method_id, request, receiver, timeout, yield);
}

template <typename Response>
inline absl::Status
ResponseReceiver<Response>::Cancel(std::chrono::nanoseconds timeout,
                                   boost::asio::yield_context yield) {
  if (IsCancelled()) {
    return absl::OkStatus();
  }
  if (auto status = client_->CancelRequest(client_id_, session_id_,
                                           request_id_, method_, timeout,
                                           yield);
      !status.ok()) {
    return status;
  }
  cancelled_ = true;
  return absl::OkStatus();
}

template <typename Response>
inline absl::Status
ResponseReceiver<Response>::Cancel(std::chrono::nanoseconds timeout) {
  if (IsCancelled()) {
    return absl::OkStatus();
  }
  if (auto status = client_->CancelRequest(client_id_, session_id_,
                                           request_id_, method_, timeout);
      !status.ok()) {
    return status;
  }
  cancelled_ = true;
  return absl::OkStatus();
}

} // namespace subspace::asio_rpc
