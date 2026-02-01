// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "rpc/client/rpc_client.h"
#include "rpc/server/rpc_server.h"

namespace subspace {

RpcClient::RpcClient(std::string service, uint64_t client_id,
                     std::string subspace_server_socket)
    : service_(std::move(service)), client_id_(client_id),
      subspace_server_socket_(std::move(subspace_server_socket)),
      logger_("rpcclient") {}

RpcClient::~RpcClient() {
  if (session_id_ != 0) {
    auto status = CloseService(std::chrono::nanoseconds(0), coroutine_);
    if (!status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError, "Error closing RPC client: %s",
                  status.ToString().c_str());
    }
  }
}

absl::StatusOr<int> RpcClient::FindMethod(std::string_view method) {
  auto it = method_name_to_id_.find(method);
  if (it == method_name_to_id_.end()) {
    return absl::NotFoundError("Method not found");
  }
  return it->second;
}

absl::Status RpcClient::Open(std::chrono::nanoseconds timeout,
                             co::Coroutine *c) {
  if (session_id_ != 0) {
    return absl::FailedPreconditionError("Session already initialized");
  }
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Initializing RPC client for service: %s", service_.c_str());
  coroutine_ = c;
  auto client = Client::Create(subspace_server_socket_);
  if (!client.ok()) {
    return absl::InternalError(absl::StrFormat("Failed to create client: %s",
                                               client.status().ToString()));
  }
  client_ = std::move(*client);

  return OpenService(timeout, c);
}

absl::Status RpcClient::Close(std::chrono::nanoseconds timeout,
                              co::Coroutine *c) {
  if (session_id_ == 0) {
    return absl::FailedPreconditionError("Session not initialized");
  }
  auto status = CloseService(timeout, coroutine_);
  if (!status.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to close service: %s", status.ToString()));
  }
  session_id_ = 0;
  return absl::OkStatus();
}

absl::Status
RpcClient::PublishServerRequest(const subspace::RpcServerRequest &req,
                                std::chrono::nanoseconds timeout,
                                co::Coroutine *c) {
  uint64_t length = req.ByteSizeLong();
  absl::StatusOr<void *> buffer;
  for (;;) {
    if (service_pub_->NumSubscribers() == 0) {
      Destroy();
      return absl::InternalError("No subscribers; is the RPC server running?");
    }
    buffer = service_pub_->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to get message buffer: %s", buffer.status().ToString()));
    }
    if (*buffer != nullptr) {
      break;
    }
    // Can't send yet, wait and try again.
    auto wait_status = service_pub_->Wait(timeout, c);
    if (!wait_status.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to wait for message buffer: %s", wait_status.ToString()));
    }
  }

  if (!req.SerializeToArray(*buffer, int32_t(length))) {
    return absl::InternalError(absl::StrFormat(
        "Failed to serialize request: %s", buffer.status().ToString()));
  }
  auto pub_status = service_pub_->PublishMessage(int32_t(length));
  if (!pub_status.ok()) {
    return absl::InternalError(absl::StrFormat("Failed to publish request: %s",
                                               pub_status.status().ToString()));
  }
  logger_.Log(toolbelt::LogLevel::kDebug, "Published RPC request: %s",
              req.DebugString().c_str());
  return absl::OkStatus();
}

absl::StatusOr<subspace::RpcServerResponse>
RpcClient::ReadServerResponse(int request_id, std::chrono::nanoseconds timeout,
                              co::Coroutine *c) {
  // Wait for the response and read it.
  for (;;) {
    auto wait_status = service_sub_->Wait(timeout, c);
    if (!wait_status.ok()) {
      return absl::InternalError("Timeout waiting for response");
    }
    for (;;) {
      auto m = service_sub_->ReadMessage();
      if (!m.ok()) {
        return absl::InternalError(absl::StrFormat("Failed to read message: %s",
                                                   m.status().ToString()));
      }
      if (m->length > 0) {
        subspace::Message msg = std::move(*m);
        subspace::RpcServerResponse response;
        if (!response.ParseFromArray(msg.buffer, msg.length)) {
          return absl::InternalError("Failed to parse RPC server response");
        }
        if (response.client_id() == client_id_ &&
            response.request_id() == request_id) {
          logger_.Log(toolbelt::LogLevel::kDebug,
                      "Received RPC server response: %s",
                      response.DebugString().c_str());
          return response;
        }
        continue;
      }
      break;
    }
  }
}

absl::Status RpcClient::OpenService(std::chrono::nanoseconds timeout,
                                    co::Coroutine *c) {
  logger_.Log(toolbelt::LogLevel::kDebug, "Opening service");
  subspace::RpcServerRequest req;
  req.set_client_id(client_id_);
  int request_id = next_request_id_++;
  req.set_request_id(request_id);
  req.mutable_open();

  auto client = subspace::Client::Create(subspace_server_socket_, service_);
  if (!client.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create subspace client: %s", client.status().ToString()));
  }
  client_ = std::move(*client);
  std::string request_name = absl::StrFormat("/rpc/%s/request", service_);
  logger_.Log(toolbelt::LogLevel::kDebug, "Creating publisher to channel: %s",
              request_name.c_str());
  auto pub = client_->CreatePublisher(request_name,
                                      {.slot_size = kRpcRequestSlotSize,
                                       .num_slots = kRpcRequestNumSlots,
                                       .reliable = true,
                                       .type = "subspace.RpcServerRequest"});
  if (!pub.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create request publisher: %s", pub.status().ToString()));
  }
  service_pub_ = std::make_shared<subspace::Publisher>(std::move(*pub));

  auto sub = client_->CreateSubscriber(
      absl::StrFormat("/rpc/%s/response", service_),
      {.reliable = true, .type = "subspace.RpcServerResponse"});
  if (!sub.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create response subscriber: %s", sub.status().ToString()));
  }

  service_sub_ = std::make_shared<subspace::Subscriber>(std::move(*sub));

  logger_.Log(toolbelt::LogLevel::kDebug, "Sending open request");
  auto pub_status = PublishServerRequest(req, timeout, c);
  if (!pub_status.ok()) {
    return pub_status;
  }

  auto resp = ReadServerResponse(request_id, timeout, c);
  if (!resp.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to read server response: %s", resp.status().ToString()));
  }

  subspace::RpcServerResponse response = std::move(*resp);
  if (!response.has_open()) {
    return absl::InternalError("RPC server response is missing open");
  }
  session_id_ = response.open().session_id();

  // Populate the methods.
  for (const auto &method : response.open().methods()) {
    auto m = std::make_shared<client_internal::Method>();
    m->name = method.name();
    m->id = method.id();
    m->request_type = method.request_channel().type();
    m->response_type = method.response_channel().type();
    m->slot_size = method.request_channel().slot_size();
    m->num_slots = method.request_channel().num_slots();

    auto pub = client_->CreatePublisher(
        method.request_channel().name(), m->slot_size, m->num_slots,
        {.reliable = true, .type = method.request_channel().type()});
    if (!pub.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to create request publisher: %s", pub.status().ToString()));
    }
    m->request_publisher =
        std::make_shared<subspace::Publisher>(std::move(*pub));

    auto sub = client_->CreateSubscriber(
        method.response_channel().name(),
        {.reliable = true, .type = method.response_channel().type()});
    if (!sub.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to create response subscriber: %s", sub.status().ToString()));
    }
    m->response_subscriber =
        std::make_shared<subspace::Subscriber>(std::move(*sub));

    if (!method.cancel_channel().empty()) {
      auto cpub = client_->CreatePublisher(method.cancel_channel(),
                                           kCancelChannelSlotSize,
                                           kCancelChannelNumSlots,
                                           {
                                               .reliable = true,
                                           });
      if (!cpub.ok()) {
        return absl::InternalError(absl::StrFormat(
            "Failed to create cancel publisher: %s", cpub.status().ToString()));
      }
      m->cancel_publisher =
          std::make_shared<subspace::Publisher>(std::move(*cpub));
    }
    method_name_to_id_[m->name] = m->id;
    methods_[m->id] = std::move(m);
  }
  logger_.Log(toolbelt::LogLevel::kInfo,
              "Opened service %s with session ID: %d", service_.c_str(),
              session_id_);
  return absl::OkStatus();
}

absl::Status RpcClient::CloseService(std::chrono::nanoseconds timeout,
                                     co::Coroutine *c) {
  if (closed_) {
    return absl::InternalError("Client is closed");
  }
  subspace::RpcServerRequest req;
  req.set_client_id(client_id_);
  int request_id = next_request_id_++;
  req.set_request_id(request_id);
  req.mutable_close()->set_session_id(session_id_);

  auto pub_status = PublishServerRequest(req, timeout, c);
  if (!pub_status.ok()) {
    return pub_status;
  }

  auto resp = ReadServerResponse(request_id, timeout, c);
  if (!resp.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to read server response: %s", resp.status().ToString()));
  }

  subspace::RpcServerResponse response = std::move(*resp);
  if (!response.has_close()) {
    return absl::InternalError("RPC server response is missing close");
  }
  return absl::OkStatus();
}

absl::StatusOr<google::protobuf::Any>
RpcClient::InvokeMethod(int method_id, const google::protobuf::Any &request,
                        std::chrono::nanoseconds timeout, co::Coroutine *c) {
  if (closed_) {
    return absl::InternalError("Client is closed");
  }
  if (c == nullptr) {
    c = coroutine_;
  }
  auto it = methods_.find(method_id);
  if (it == methods_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("Method %d not found", method_id));
  }

  const auto &method = it->second;

  subspace::RpcRequest req;
  int request_id = ++next_request_id_;
  req.set_client_id(client_id_);
  req.set_session_id(session_id_);
  req.set_request_id(request_id);
  req.set_method(method_id);
  *req.mutable_argument() = request;

  absl::StatusOr<void *> buffer;
  for (;;) {
    if (method->request_publisher->NumSubscribers() == 0) {
      Destroy();
      return absl::InternalError("No subscribers; is the RPC server running?");
    }
    buffer = method->request_publisher->GetMessageBuffer(
        int32_t(req.ByteSizeLong()));
    if (!buffer.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to get message buffer: %s", buffer.status().ToString()));
    }
    if (*buffer != nullptr) {
      break;
    }
    // Can't send yet, wait and try again.
    auto wait_status = method->request_publisher->Wait(timeout, c);
    if (!wait_status.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to wait for message buffer: %s", wait_status.ToString()));
    }
  }

  // We can serialize and publish now.
  uint64_t request_size = req.ByteSizeLong();
  if (!req.SerializeToArray(*buffer, int32_t(request_size))) {
    return absl::InternalError(absl::StrFormat(
        "Failed to serialize request: %s", req.SerializeAsString()));
  }
  auto pub_status =
      method->request_publisher->PublishMessage(int32_t(request_size));
  if (!pub_status.ok()) {
    return absl::InternalError(absl::StrFormat("Failed to publish request: %s",
                                               pub_status.status().ToString()));
  }

  // Wait for the response.
  for (;;) {
    auto wait_status = method->response_subscriber->Wait(timeout, c);
    if (!wait_status.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Error waiting for response: %s", wait_status.ToString()));
    }
    for (;;) {
      auto m = method->response_subscriber->ReadMessage();
      if (!m.ok()) {
        return absl::InternalError(absl::StrFormat("Failed to read message: %s",
                                                   m.status().ToString()));
      }
      if (m->length > 0) {
        subspace::Message msg = std::move(*m);
        subspace::RpcResponse rpc_response;
        if (!rpc_response.ParseFromArray(msg.buffer, msg.length)) {
          return absl::InternalError("Failed to parse RPC response");
        }
        if (rpc_response.client_id() == client_id_ &&
            rpc_response.request_id() == request_id &&
            rpc_response.session_id() == session_id_) {
          logger_.Log(toolbelt::LogLevel::kDebug, "Received RPC response: %s",
                      rpc_response.DebugString().c_str());
          if (!rpc_response.error().empty()) {
            return absl::InternalError(
                absl::StrFormat("%s", rpc_response.error()));
          }
          return rpc_response.result();
        }
        continue;
      }
      break;
    }
  }
}

absl::Status RpcClient::InvokeMethod(
    int method_id, const google::protobuf::Any &request,
    std::function<void(std::shared_ptr<RpcClient>, uint64_t, int, int,
                       std::shared_ptr<client_internal::Method>,
                       const RpcResponse *)>
        response_handler,
    std::chrono::nanoseconds timeout, co::Coroutine *c) {
  if (closed_) {
    return absl::InternalError("Client is closed");
  }
  if (c == nullptr) {
    c = coroutine_;
  }
  auto it = methods_.find(method_id);
  if (it == methods_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("Method %d not found", method_id));
  }

  const auto &method = it->second;

  subspace::RpcRequest req;
  int request_id = ++next_request_id_;
  req.set_client_id(client_id_);
  req.set_session_id(session_id_);
  req.set_request_id(request_id);
  req.set_method(method_id);
  *req.mutable_argument() = request;

  absl::StatusOr<void *> buffer;
  for (;;) {
    if (method->request_publisher->NumSubscribers() == 0) {
      Destroy();
      return absl::InternalError("No subscribers; is the RPC server running?");
    }
    buffer = method->request_publisher->GetMessageBuffer(
        int32_t(req.ByteSizeLong()));
    if (!buffer.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to get message buffer: %s", buffer.status().ToString()));
    }
    if (*buffer != nullptr) {
      break;
    }
    // Can't send yet, wait and try again.
    auto wait_status = method->request_publisher->Wait(timeout, c);
    if (!wait_status.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to wait for message buffer: %s", wait_status.ToString()));
    }
  }

  // We can serialize and publish now.
  uint64_t request_size = req.ByteSizeLong();
  if (!req.SerializeToArray(*buffer, int32_t(request_size))) {
    return absl::InternalError(absl::StrFormat(
        "Failed to serialize request: %s", req.SerializeAsString()));
  }
  auto pub_status =
      method->request_publisher->PublishMessage(int32_t(request_size));
  if (!pub_status.ok()) {
    return absl::InternalError(absl::StrFormat("Failed to publish request: %s",
                                               pub_status.status().ToString()));
  }
  // We have sent the method invocation request.  Now we receive all the
  // responses.

  for (;;) {
    auto wait_status = method->response_subscriber->Wait(timeout, c);
    if (!wait_status.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Error waiting for response: %s", wait_status.ToString()));
    }
    for (;;) {
      auto m = method->response_subscriber->ReadMessage();
      if (!m.ok()) {
        return absl::InternalError(absl::StrFormat("Failed to read message: %s",
                                                   m.status().ToString()));
      }
      if (m->length > 0) {
        subspace::Message msg = std::move(*m);
        subspace::RpcResponse rpc_response;
        if (!rpc_response.ParseFromArray(msg.buffer, msg.length)) {
          return absl::InternalError("Failed to parse RPC response");
        }
        if (rpc_response.client_id() == client_id_ &&
            rpc_response.request_id() == request_id &&
            rpc_response.session_id() == session_id_) {
          logger_.Log(toolbelt::LogLevel::kDebug, "Received RPC response: %s",
                      rpc_response.DebugString().c_str());
          if (!rpc_response.error().empty()) {
            return absl::InternalError(
                absl::StrFormat("%s", rpc_response.error()));
          }
          response_handler(shared_from_this(), client_id_, session_id_,
                           request_id, method, &rpc_response);
          // We stop receiving response if this is the last one, cancelled or
          // errored
          if (rpc_response.is_last() || rpc_response.is_cancelled() ||
              !rpc_response.error().empty()) {
            return absl::OkStatus();
          }
        }
        continue;
      }
      break;
    }
  }
  return absl::OkStatus();
}

absl::Status
RpcClient::CancelRequest(uint64_t client_id, int session_id, int request_id,
                         std::shared_ptr<client_internal::Method> method,
                         std::chrono::nanoseconds timeout, co::Coroutine *c) {
  if (closed_) {
    return absl::InternalError("Client is closed");
  }
  subspace::RpcCancelRequest req;
  req.set_client_id(client_id_);
  req.set_session_id(session_id_);
  req.set_request_id(request_id);

  absl::StatusOr<void *> buffer;
  for (;;) {
    if (method->cancel_publisher->NumSubscribers() == 0) {
      Destroy();
      return absl::InternalError("No subscribers; is the RPC server running?");
    }
    buffer =
        method->cancel_publisher->GetMessageBuffer(int32_t(req.ByteSizeLong()));
    if (!buffer.ok()) {
      return absl::InternalError(
          absl::StrFormat("Failed to get message buffer for cancel: %s",
                          buffer.status().ToString()));
    }
    if (*buffer != nullptr) {
      break;
    }
    // Can't send yet, wait and try again.
    auto wait_status = method->cancel_publisher->Wait(timeout, c);
    if (!wait_status.ok()) {
      return absl::InternalError(
          absl::StrFormat("Failed to wait for message buffer for cancel: %s",
                          wait_status.ToString()));
    }
  }

  // We can serialize and publish now.
  uint64_t request_size = req.ByteSizeLong();
  if (!req.SerializeToArray(*buffer, int32_t(request_size))) {
    return absl::InternalError(absl::StrFormat(
        "Failed to serialize cancel request: %s", req.SerializeAsString()));
  }
  auto pub_status =
      method->cancel_publisher->PublishMessage(int32_t(request_size));
  if (!pub_status.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to publish cancel request: %s",
                        pub_status.status().ToString()));
  }
  return absl::OkStatus();
}

} // namespace subspace
