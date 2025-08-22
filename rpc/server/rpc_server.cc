#include "rpc/server/rpc_server.h"
#include "proto/subspace.pb.h"

namespace subspace {
RpcServer::RpcServer(std::string name, std::string server_socket)
    : name_(std::move(name)), server_socket_(std::move(server_socket)) {}

void RpcServer::Stop() {
  running_ = false;
  scheduler_.Stop();
}

absl::Status RpcServer::CreateChannels() {
  auto client = subspace::Client::Create(server_socket_, name_);
  if (!client.ok()) {
    return client.status();
  }
  client_ = std::move(*client);

  auto receiver =
      client_->CreateSubscriber(absl::StrFormat("/rpc/%s/request", name_),
                                {.reliable = true,
                                 .max_active_messages = 1,
                                 .type = "subspace.RpcServerRequest"});
  if (!receiver.ok()) {
    return receiver.status();
  }
  request_receiver_ =
      std::make_shared<subspace::Subscriber>(std::move(*receiver));

  auto publisher =
      client_->CreatePublisher(absl::StrFormat("/rpc/%s/response", name_),
                               {.slot_size = kRpcResponseSlotSize,
                                .num_slots = kRpcResponseNumSlots,
                                .reliable = true,
                                .type = "subspace.RpcServerResponse"});
  if (!publisher.ok()) {
    return publisher.status();
  }
  response_publisher_ =
      std::make_shared<subspace::Publisher>(std::move(*publisher));
  return absl::OkStatus();
}

absl::Status RpcServer::Run() {
  scheduler_.SetCompletionCallback(
      [this](co::Coroutine *c) { coroutines_.erase(c); });

  absl::Status status = CreateChannels();
  if (!status.ok()) {
    return status;
  }

  running_ = true;
  AddCoroutine(std::make_unique<co::Coroutine>(
      scheduler_, [this](co::Coroutine *c) { ListenerCoroutine(c); },
      "RPC listener"));

  scheduler_.Run();

  return absl::OkStatus();
}

void RpcServer::ListenerCoroutine(co::Coroutine *c) {
  while (running_) {
    auto fd = request_receiver_->Wait(c);
    if (!fd.ok()) {
      continue;
    }
    for (;;) {
      absl::StatusOr<subspace::Message> msg = request_receiver_->ReadMessage();
      if (!msg.ok()) {
        break;
      }
      if (msg->length == 0) {
        break;
      }
      if (auto status = HandleIncomingRpcServerRequest(std::move(*msg), c);
          !status.ok()) {
        // Log error but keep going.
        std::cerr << "Error handling RPC request: " << status.ToString()
                  << std::endl;
      }
    }
  }
}

absl::Status RpcServer::HandleIncomingRpcServerRequest(subspace::Message msg,
                                                       co::Coroutine *c) {
  subspace::RpcServerRequest request;
  if (!request.ParseFromArray(msg.buffer, msg.length)) {
    return absl::InvalidArgumentError("Failed to parse RpcRequest");
  }
  std::cerr << "Received RPC request: " << request.DebugString() << std::endl;

  subspace::RpcServerResponse response;
  switch (request.request_case()) {
  case subspace::RpcServerRequest::kOpen: {
    if (auto status = HandleOpen(request.open(), response.mutable_open(), c);
        !status.ok()) {
      response.set_error(status.ToString());
    }
    break;
  }
  case subspace::RpcServerRequest::kClose: {
    if (auto status = HandleClose(request.close(), response.mutable_close(), c);
        !status.ok()) {
      response.set_error(status.ToString());
    }
    break;
  }
  default:
    response.set_error("Unknown request type");
    break;
  }

  // Publish the response
  return PublishRpcServerResponse(response, c);
}

absl::Status
RpcServer::PublishRpcServerResponse(const subspace::RpcServerResponse &response,
                                    co::Coroutine *c) {
  uint64_t length = response.ByteSizeLong();
  // This is a reliable publisher so we keep trying to publish until we can.
  for (;;) {
    absl::StatusOr<void *> buffer =
        response_publisher_->GetMessageBuffer(length);
    if (!buffer.ok()) {
      return buffer.status();
    }
    if (*buffer == nullptr) {
      // Buffer is not ready, wait and try again.
      if (auto status = response_publisher_->Wait(c); !status.ok()) {
        return status;
      }
      continue;
    }

    // We got a buffer, fill it in and send it.
    if (!response.SerializeToArray(*buffer, length)) {
      return absl::InternalError("Failed to serialize RpcServerResponse");
    }
    auto result = response_publisher_->PublishMessage(length);
    if (!result.ok()) {
      return result.status();
    }
    std::cerr << "Published RPC response: " << response.DebugString()
              << std::endl;
    return absl::OkStatus();
  }
}

absl::Status RpcServer::HandleOpen(const subspace::RpcOpenRequest &request,
                                   subspace::RpcOpenResponse *response,
                                   co::Coroutine *c) {
  std::cerr << "Handling Open request: " << request.DebugString() << std::endl;
  auto s = CreateSession();
  if (!s.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to create session: %s", s.status().ToString()));
  }
  auto session = *s;
  response->set_session_id(session->id);

  for (auto &[name, method] : session->methods) {
    auto *m = response->add_methods();
    m->set_name(method->method->name);
    auto *req = m->mutable_request_channel();
    req->set_name(
        absl::StrFormat("%s/%d", method->method->request_channel, session->id));
    req->set_type(method->method->request_type);
    req->set_slot_size(method->method->slot_size);
    req->set_num_slots(method->method->num_slots);

    auto *res = m->mutable_response_channel();
    res->set_name(absl::StrFormat("%s/%d", method->method->response_channel,
                                  session->id));
    res->set_type(method->method->response_type);
  }
  std::cerr << "Open response: " << response->DebugString() << std::endl;
  return absl::OkStatus();
}

absl::Status RpcServer::HandleClose(const subspace::RpcCloseRequest &request,
                                    subspace::RpcCloseResponse *response,
                                    co::Coroutine *c) {
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<RpcServer::Session>> RpcServer::CreateSession() {
  std::cerr << "Creating session\n";
  auto session = std::make_shared<Session>();
  session->id = ++session_id_;
  for (auto &[name, method] : methods_) {
    auto method_instance = std::make_shared<MethodInstance>();
    method_instance->method = method;
    absl::StatusOr<subspace::Subscriber> sub = client_->CreateSubscriber(
        absl::StrFormat("%s/%d", method->request_channel, session->id),
        {.reliable = true, .type = method->request_type});
    if (!sub.ok()) {
      std::cerr << "Failed to create subscriber for method " << method->name
                << ": " << sub.status().ToString() << std::endl;
      return sub.status();
    }
    method_instance->request_subscriber =
        std::make_shared<subspace::Subscriber>(std::move(*sub));

    absl::StatusOr<subspace::Publisher> pub = client_->CreatePublisher(
        absl::StrFormat("%s/%d", method->response_channel, session->id),
        {.slot_size = method->slot_size,
         .num_slots = method->num_slots,
         .reliable = true,
         .type = method->response_type});
    if (!pub.ok()) {
      std::cerr << "Failed to create publisher for method " << method->name
                << ": " << pub.status().ToString() << std::endl;
      return pub.status();
    }
    method_instance->response_publisher =
        std::make_shared<subspace::Publisher>(std::move(*pub));

    session->methods.insert({method->name, method_instance});

    std::cerr << "Adding coroutine for method " << method_instance->method->name
              << " in session " << session->id << std::endl;
    AddCoroutine(std::make_unique<co::Coroutine>(
        scheduler_, [server = shared_from_this(), session,
                     method_instance](co::Coroutine *c) {
        std::cerr << "Starting coroutine for method "
                  << method_instance->method->name << " in session "
                  << session->id << std::endl;
          server->SessionMethodCoroutine(server, session, method_instance, c);
        }));
  }
  sessions_[session->id] = session;
  std::cerr << "Created session " << session->id << std::endl;
  return session;
}

absl::Status RpcServer::DestroySession(int session_id) {
  // Clean up session resources here.
  sessions_.erase(session_id);
  return absl::OkStatus();
}

void RpcServer::SessionMethodCoroutine(std::shared_ptr<RpcServer> server,
                                       std::shared_ptr<Session> session,
                                       std::shared_ptr<MethodInstance> method,

                                       co::Coroutine *c) {
  std::cerr << "Running method coroutine for session "
            << session->id << std::endl;
  while (running_) {
    auto s = method->request_subscriber->Wait(c);
    if (!s.ok()) {
      std::cerr << "Error waiting for request: " << s.ToString() << std::endl;
      return;
    }
    auto m = method->request_subscriber->ReadMessage();
    if (!m.ok()) {
      std::cerr << "Error reading message: " << m.status().ToString()
                << std::endl;
      return;
    }
    if (m->length == 0) {
      // No message, continue waiting.
      continue;
    }
    subspace::RpcRequest request;
    if (!request.ParseFromArray(m->buffer, m->length)) {
      std::cerr << "Error parsing request for method " << method->method->name
                << std::endl;
      continue;
    }
    if (request.session_id() != session->id) {
      std::cerr << "Received request for session " << request.session_id()
                << ", but this coroutine is for session " << session->id
                << std::endl;
      continue;
    }
    auto method = session->methods.find(request.method());
    if (method == session->methods.end()) {
      std::cerr << "Unknown method: " << request.method() << std::endl;
      continue;
    }
    subspace::RpcResponse response;
    response.set_session_id(session->id);
    response.set_request_id(request.request_id());
    auto *result = response.mutable_result();
    absl::Status method_status =
        method->second->method->callback(request.arguments(), result);
    if (!method_status.ok()) {
      response.set_error(absl::StrFormat("Error executing method %s: %s",
                                         method->second->method->name,
                                         method_status.ToString()));
    }

    uint64_t length = response.ByteSizeLong();
    for (;;) {
      absl::StatusOr<void *> buffer =
          method->second->response_publisher->GetMessageBuffer(int32_t(length));
      if (!buffer.ok()) {
        response.set_error(absl::StrFormat(
            "Error getting buffer for method %s: %s",
            method->second->method->name, buffer.status().ToString()));
        break;
      }
      if (*buffer == nullptr) {
        // Buffer is not ready, wait and try again.
        if (auto status = method->second->response_publisher->Wait(c);
            !status.ok()) {
          std::cerr << "Error waiting for buffer: " << status.ToString()
                    << std::endl;
          break;
        }
        continue;
      }
      // We got a buffer, fill it in and send it.
      if (!response.SerializeToArray(*buffer, length)) {
        std::cerr << "Error serializing response for method "
                  << method->second->method->name << std::endl;
        break;
      }
      auto pub_result =
          method->second->response_publisher->PublishMessage(length);
      if (!pub_result.ok()) {
        std::cerr << "Error publishing response for method "
                  << method->second->method->name << ": "
                  << pub_result.status().ToString() << std::endl;
      }
    }
  }
}

} // namespace subspace
