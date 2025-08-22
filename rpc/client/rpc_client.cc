#include "rpc/client/rpc_client.h"
#include "rpc/server/rpc_server.h"

namespace subspace {

RpcClient::RpcClient(std::string service, std::string subspace_server_socket)
    : service_(std::move(service)),
      subspace_server_socket_(std::move(subspace_server_socket)) {}

RpcClient::~RpcClient() {}

absl::Status RpcClient::Init(co::Coroutine *c) {
  if (session_id_ != 0) {
    return absl::FailedPreconditionError("Session already initialized");
  }
  auto client = Client::Create(subspace_server_socket_);
  if (!client.ok()) {
    return absl::InternalError(absl::StrFormat("Failed to create client: %s",
                                               client.status().ToString()));
  }
  client_ = std::move(*client);

  return OpenService(c);
}

absl::Status
RpcClient::PublishServerRequest(const subspace::RpcServerRequest &req,
                                co::Coroutine *c) {
  uint64_t length = req.ByteSizeLong();
  absl::StatusOr<void *> buffer;
  for (;;) {
    buffer = service_pub_->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to get message buffer: %s", buffer.status().ToString()));
    }
    if (*buffer == nullptr) {
      // Can't send yet, wait and try again.
      auto wait_status = service_pub_->Wait(c);
      if (!wait_status.ok()) {
        return absl::InternalError(absl::StrFormat(
            "Failed to wait for message buffer: %s", wait_status.ToString()));
      }
      continue;
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
  return absl::OkStatus();
}

absl::StatusOr<subspace::RpcServerResponse>
RpcClient::ReadServerResponse(co::Coroutine *c) {
  // Wait for the response and read it.
  subspace::Message msg;
  // TODO: timeout here.
  for (;;) {
    if (!service_sub_->Wait(c).ok()) {
      return absl::InternalError("Timeout waiting for response");
    }
    auto m = service_sub_->ReadMessage();
    if (!m.ok()) {
      return absl::InternalError(
          absl::StrFormat("Failed to read message: %s", m.status().ToString()));
    }
    if (m->length > 0) {
      msg = std::move(*m);
      break;
    }
  }

  subspace::RpcServerResponse response;
  if (!response.ParseFromArray(msg.buffer, msg.length)) {
    return absl::InternalError("Failed to parse RPC server response");
  }
  return response;
}

absl::Status RpcClient::OpenService(co::Coroutine *c) {
  subspace::RpcServerRequest req;
  req.mutable_open();

  auto client = subspace::Client::Create(subspace_server_socket_, service_);
  if (!client.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create subspace client: %s", client.status().ToString()));
  }
  client_ = std::move(*client);

  auto pub =
      client_->CreatePublisher(absl::StrFormat("/rpc/%s/request", service_),
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

  auto pub_status = PublishServerRequest(req, c);
  if (!pub_status.ok()) {
    return pub_status;
  }

  auto resp = ReadServerResponse(c);
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
    auto m = std::make_shared<Method>();
    m->name = method.name();
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
    methods_[m->name] = std::move(m);
  }
  return absl::OkStatus();
}

absl::Status RpcClient::CloseService(co::Coroutine *c) {
  subspace::RpcServerRequest req;
  req.mutable_close()->set_session_id(session_id_);

  auto pub_status = PublishServerRequest(req, c);
  if (!pub_status.ok()) {
    return pub_status;
  }

  auto resp = ReadServerResponse(c);
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
RpcClient::InvokeMethod(const std::string &name,
                        const google::protobuf::Any &request,
                        co::Coroutine *c) {
  auto it = methods_.find(name);
  if (it == methods_.end()) {
    return absl::NotFoundError(absl::StrFormat("Method not found: %s", name));
  }

  const auto &method = it->second;

  subspace::RpcRequest req;
  int request_id = ++next_request_id_;
  req.set_session_id(session_id_);
  req.set_request_id(request_id);
  req.set_method(name);
  auto *args = req.mutable_arguments();
  args->PackFrom(request);

  absl::StatusOr<void *> buffer;
  for (;;) {
    buffer = method->request_publisher->GetMessageBuffer(
        int32_t(req.ByteSizeLong()));
    if (!buffer.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Failed to get message buffer: %s", buffer.status().ToString()));
    }
    if (*buffer == nullptr) {
      // Can't send yet, wait and try again.
      auto wait_status = method->request_publisher->Wait(c);
      if (!wait_status.ok()) {
        return absl::InternalError(absl::StrFormat(
            "Failed to wait for message buffer: %s", wait_status.ToString()));
      }
      continue;
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
  subspace::Message msg;
  // TODO: timeout here.
  for (;;) {
    auto wait_status = method->response_subscriber->Wait(c);
    if (!wait_status.ok()) {
      return absl::InternalError(absl::StrFormat(
          "Error waiting for response: %s", wait_status.ToString()));
    }
    auto m = method->response_subscriber->ReadMessage();
    if (!m.ok()) {
      return absl::InternalError(
          absl::StrFormat("Failed to read message: %s", m.status().ToString()));
    }
    if (m->length > 0) {
      msg = std::move(*m);
      break;
    }
  }

  subspace::RpcResponse rpc_response;
  if (!rpc_response.ParseFromArray(msg.buffer, msg.length)) {
    return absl::InternalError("Failed to parse RPC response");
  }
  if (rpc_response.session_id() != session_id_) {
    return absl::InternalError(
        absl::StrFormat("Response session ID mismatch: expected %d, got %d",
                        session_id_, rpc_response.session_id()));
  }
  if (rpc_response.request_id() != request_id) {
    return absl::InternalError(
        absl::StrFormat("Response request ID mismatch: expected %d, got %d",
                        request_id, rpc_response.request_id()));
  }
  return rpc_response.result();
}

} // namespace subspace
