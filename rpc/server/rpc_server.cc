#include "rpc/server/rpc_server.h"
#include "proto/subspace.pb.h"
#include <inttypes.h>
#include <stdio.h>

namespace subspace {
RpcServer::RpcServer(std::string service_name, std::string subspace_server_socket)
    : name_(std::move(service_name)),
      subspace_server_socket_(std::move(subspace_server_socket)),
      logger_("rpcserver") {
  logger_.Log(toolbelt::LogLevel::kInfo, "RpcServer created for service: %s",
              name_.c_str());
  auto p = toolbelt::Pipe::Create();
  if (!p.ok()) {
    logger_.Log(toolbelt::LogLevel::kError, "Failed to create interrupt pipe");
    return;
  }
  interrupt_pipe_ = std::move(*p);
}

void RpcServer::Stop() {
  running_ = false;
  interrupt_pipe_.Close();
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method, const std::string &request_type,
    const std::string &response_type, int32_t slot_size, int32_t num_slots,
    std::function<absl::Status(const google::protobuf::Any &,
                               google::protobuf::Any *, co::Coroutine*)>
        callback) {
  if (methods_.find(method) != methods_.end()) {
    return absl::AlreadyExistsError("Method already registered: " + method);
  }
  methods_[method] =
      std::make_shared<Method>(this, method, request_type, response_type,
                               slot_size, num_slots, std::move(callback));
  return absl::OkStatus();
}

// Register void method.
absl::Status RpcServer::RegisterMethod(
    const std::string &method, const std::string &request_type,
    int32_t slot_size, int32_t num_slots,
    absl::Status (*callback)(const google::protobuf::Any &, co::Coroutine*)) {
  if (methods_.find(method) != methods_.end()) {
    return absl::AlreadyExistsError("Method already registered: " + method);
  }
  methods_[method] = std::make_shared<Method>(
      this, method, request_type, "subspace.VoidMessage", slot_size, num_slots,
      [callback](const google::protobuf::Any &req, google::protobuf::Any *res, co::Coroutine *c) {
        auto status = callback(req, c);
        if (!status.ok()) {
          return status;
        }
        // The response for this void method is a VoidMessage packed
        // into a google.protobuf.Any.
        res->PackFrom(VoidMessage());
        return absl::OkStatus();
      });
  return absl::OkStatus();
}

absl::Status RpcServer::CreateChannels() {
  auto client = subspace::Client::Create(subspace_server_socket_, name_);
  if (!client.ok()) {
    return client.status();
  }
  client_ = std::move(*client);
  std::string request_name = absl::StrFormat("/rpc/%s/request", name_);
  logger_.Log(toolbelt::LogLevel::kDebug, "Creating subscriber for %s",
              request_name.c_str());
  auto receiver = client_->CreateSubscriber(
      request_name, {.reliable = true,
                     .max_active_messages = 1,
                     .type = "subspace.RpcServerRequest"});
  if (!receiver.ok()) {
    return receiver.status();
  }
  request_receiver_ =
      std::make_shared<subspace::Subscriber>(std::move(*receiver));

  std::string response_name = absl::StrFormat("/rpc/%s/response", name_);
  logger_.Log(toolbelt::LogLevel::kDebug, "Creating publisher for %s",
              response_name.c_str());
  auto publisher = client_->CreatePublisher(
      response_name, {.slot_size = kRpcResponseSlotSize,
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

absl::Status RpcServer::Run(co::CoroutineScheduler *scheduler) {
  if (scheduler != nullptr) {
    scheduler_ = scheduler;
  } else {
    scheduler_ = &local_scheduler_;
  }
  scheduler_->SetCompletionCallback(
      [this](co::Coroutine *c) { coroutines_.erase(c); });

  absl::Status status = CreateChannels();
  if (!status.ok()) {
    return status;
  }

  running_ = true;
  AddCoroutine(std::make_unique<co::Coroutine>(
      *scheduler_,
      [server = shared_from_this()](co::Coroutine *c) {
        ListenerCoroutine(server, c);
      },
      "RPC listener"));

  // Block and run the scheduler if we are not given one.  If we are given
  // a scheduler, it wil be run by the caller.
  if (scheduler == nullptr) {
    scheduler_->Run();
  }

  return absl::OkStatus();
}

void RpcServer::ListenerCoroutine(std::shared_ptr<RpcServer> server,
                                  co::Coroutine *c) {
  while (server->running_) {
    auto fd =
        server->request_receiver_->Wait(server->interrupt_pipe_.ReadFd(), c);

    if (!fd.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for request: %s",
                          fd.status().ToString().c_str());
      continue;
    }
    if (*fd == server->interrupt_pipe_.ReadFd().Fd()) {
      break;
    }

    for (;;) {
      server->logger_.Log(toolbelt::LogLevel::kVerboseDebug,
                          "Incoming message in server");
      absl::StatusOr<subspace::Message> msg =
          server->request_receiver_->ReadMessage();
      if (!msg.ok()) {
        break;
      }
      if (msg->length == 0) {
        break;
      }
      if (auto status =
              server->HandleIncomingRpcServerRequest(std::move(*msg), c);
          !status.ok()) {
        // Log error but keep going.
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error handling RPC request: %s",
                            status.ToString().c_str());
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
  logger_.Log(toolbelt::LogLevel::kDebug, "Received RPC request: %s",
              request.DebugString().c_str());

  subspace::RpcServerResponse response;
  response.set_client_id(request.client_id());
  response.set_request_id(request.request_id());
  switch (request.request_case()) {
  case subspace::RpcServerRequest::kOpen: {
    if (auto status = HandleOpen(request.client_id(), request.open(),
                                 response.mutable_open(), c);
        !status.ok()) {
      response.set_error(status.ToString());
    }
    break;
  }
  case subspace::RpcServerRequest::kClose: {
    logger_.Log(toolbelt::LogLevel::kDebug, "Handling Close request: %s",
                request.close().DebugString().c_str());
    if (auto status = HandleClose(request.client_id(), request.close(),
                                  response.mutable_close(), c);
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
      auto status = response_publisher_->Wait(interrupt_pipe_.ReadFd(), c);
      if (!status.ok()) {
        return status.status();
      }
      if (*status == interrupt_pipe_.ReadFd().Fd()) {
        return absl::OkStatus();
      }
    }

    // We got a buffer, fill it in and send it.
    if (!response.SerializeToArray(*buffer, length)) {
      return absl::InternalError("Failed to serialize RpcServerResponse");
    }
    auto result = response_publisher_->PublishMessage(length);
    if (!result.ok()) {
      return result.status();
    }
    logger_.Log(toolbelt::LogLevel::kDebug, "Published RPC response: %s",
                response.DebugString().c_str());
    return absl::OkStatus();
  }
}

absl::Status RpcServer::HandleOpen(uint64_t client_id,
                                   const subspace::RpcOpenRequest &request,
                                   subspace::RpcOpenResponse *response,
                                   co::Coroutine *c) {
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Handling Open request from client %" PRId64 ", %s", client_id,
              request.DebugString().c_str());

  auto s = CreateSession(client_id);
  if (!s.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to create session: %s", s.status().ToString()));
  }
  auto session = *s;
  response->set_session_id(session->session_id);

  for (auto &[name, method] : session->methods) {
    auto *m = response->add_methods();
    m->set_name(method->method->name);
    auto *req = m->mutable_request_channel();
    req->set_name(absl::StrFormat("%s/%d/%d", method->method->request_channel,
                                  client_id, session->session_id));
    req->set_type(method->method->request_type);
    req->set_slot_size(method->method->slot_size);
    req->set_num_slots(method->method->num_slots);

    auto *res = m->mutable_response_channel();
    res->set_name(absl::StrFormat("%s/%d/%d", method->method->response_channel,
                                  client_id, session->session_id));
    res->set_type(method->method->response_type);
  }
  return absl::OkStatus();
}

absl::Status RpcServer::HandleClose(uint64_t client_id,
                                    const subspace::RpcCloseRequest &request,
                                    subspace::RpcCloseResponse *response,
                                    co::Coroutine *c) {
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Handling Close request from client %" PRId64 ", %s", client_id,
              request.DebugString().c_str());
  auto it = sessions_.find(request.session_id());
  if (it == sessions_.end()) {
    return absl::NotFoundError("Session not found");
  }
  auto session = it->second;
  // Clean up session resources here.
  sessions_.erase(session->session_id);
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<RpcServer::Session>> RpcServer::CreateSession(uint64_t client_id) {
  auto session = std::make_shared<Session>();
  session->session_id = ++session_id_;
  session->client_id = client_id;
  for (auto &[name, method] : methods_) {
    auto method_instance = std::make_shared<MethodInstance>();
    method_instance->method = method;

    absl::StatusOr<subspace::Subscriber> sub = client_->CreateSubscriber(
        absl::StrFormat("%s/%d/%d", method->request_channel, session->client_id, session->session_id),
        {.reliable = true, .type = method->request_type});
    if (!sub.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                          "Failed to create subscriber for method %s: %s",
                          method->name.c_str(),
                          sub.status().ToString().c_str());
      return sub.status();
    }
    method_instance->request_subscriber =
        std::make_shared<subspace::Subscriber>(std::move(*sub));

    absl::StatusOr<subspace::Publisher> pub = client_->CreatePublisher(
        absl::StrFormat("%s/%d/%d", method->response_channel, session->client_id, session->session_id),
        {.slot_size = method->slot_size,
         .num_slots = method->num_slots,
         .reliable = true,
         .type = method->response_type});
    if (!pub.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                          "Failed to create publisher for method %s: %s",
                          method->name.c_str(),
                          pub.status().ToString().c_str());
      return pub.status();
    }
    method_instance->response_publisher =
        std::make_shared<subspace::Publisher>(std::move(*pub));

    session->methods.insert({method->name, method_instance});

    AddCoroutine(std::make_unique<co::Coroutine>(
        *scheduler_, [server = shared_from_this(), session,
                      method_instance](co::Coroutine *c) {
          SessionMethodCoroutine(std::move(server), session, method_instance,
                                 c);
        }));
  }
  sessions_[session->session_id] = session;
  logger_.Log(toolbelt::LogLevel::kDebug, "Created session: %d",
              session->session_id);
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
  while (server->running_) {
    auto s =
        method->request_subscriber->Wait(server->interrupt_pipe_.ReadFd(), c);
    if (!s.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for request: %s",
                          s.status().ToString().c_str());
      return;
    }
    if (*s == server->interrupt_pipe_.ReadFd().Fd()) {
      break;
    }
    subspace::RpcRequest request;
    bool request_ok = false;
    std::shared_ptr<MethodInstance> method_instance;
    for (;;) {
      auto m = method->request_subscriber->ReadMessage();
      if (!m.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error reading message for method %s: %s",
                            method->method->name.c_str(),
                            m.status().ToString().c_str());
        break;
      }
      if (m->length == 0) {
        // No message, continue waiting.
        server->logger_.Log(toolbelt::LogLevel::kDebug,
                            "No message received for method %s",
                            method->method->name.c_str());
        break;
      }
      if (!request.ParseFromArray(m->buffer, m->length)) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error parsing request for method %s: %s",
                            method->method->name.c_str(),
                            m.status().ToString().c_str());
        continue;
      }
      if (request.session_id() == session->session_id) {
        auto it = session->methods.find(request.method());
        if (it == session->methods.end()) {
          server->logger_.Log(toolbelt::LogLevel::kError,
                              "Unknown method: %s",
                              request.method().c_str());
          continue;
        }
        method_instance = it->second;
        request_ok = true;
        break;
      }
    }
    if (!request_ok) {
      continue;
    }
    subspace::RpcResponse response;
    response.set_session_id(session->session_id);
    response.set_request_id(request.request_id());
    response.set_client_id(request.client_id());
    auto *result = response.mutable_result();
    server->logger_.Log(toolbelt::LogLevel::kDebug,
                        "Calling method %s",
                        method_instance->method->name.c_str());
    absl::Status method_status =
        method_instance->method->callback(request.arguments(), result, c);
    if (!method_status.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error executing method %s: %s",
                            method_instance->method->name.c_str(),
                            method_status.ToString().c_str());
      response.set_error(absl::StrFormat("Error executing method %s: %s",
                                         method_instance->method->name,
                                         method_status.ToString()));
    }

    uint64_t length = response.ByteSizeLong();
    absl::StatusOr<void *> buffer;
    for (;;) {
      buffer = method_instance->response_publisher->GetMessageBuffer(
          int32_t(length));
      if (!buffer.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error getting buffer for method %s: %s",
                            method_instance->method->name.c_str(),
                            buffer.status().ToString().c_str());
        response.set_error(absl::StrFormat(
            "Error getting buffer for method %s: %s",
            method_instance->method->name, buffer.status().ToString()));
        return;
      }
      if (*buffer != nullptr) {
        break;
      }
      if (!server->interrupt_pipe_.ReadFd().Valid()) {
        return;
      }
      // Buffer is not ready, wait and try again.
      auto status = method_instance->response_publisher->Wait(
          server->interrupt_pipe_.ReadFd(), c);
      if (!status.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error waiting for buffer: %s",
                            status.status().ToString().c_str());
        return;
      }
      if (*status == server->interrupt_pipe_.ReadFd().Fd()) {
        return;
      }
    }
    // We got a buffer, fill it in and send it.
    if (!response.SerializeToArray(*buffer, length)) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error serializing response for method %s",
                          method_instance->method->name.c_str());
      break;
    }
    server->logger_.Log(toolbelt::LogLevel::kDebug,
                        "Publishing response for method %s: %s",
                        method_instance->method->name.c_str(),
                        response.DebugString().c_str());
    auto pub_result =
        method_instance->response_publisher->PublishMessage(length);
    if (!pub_result.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error publishing response for method %s: %s",
                          method_instance->method->name.c_str(),
                          pub_result.status().ToString().c_str());
    }
    server->logger_.Log(toolbelt::LogLevel::kDebug,
                        "Published response for method %s",
                        method_instance->method->name.c_str());
  }
}

} // namespace subspace

