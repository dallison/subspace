// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "rpc/server/rpc_server.h"
#include "proto/subspace.pb.h"
#include <inttypes.h>
#include <stdio.h>

namespace subspace {

using Method = internal::Method;
using Session = internal::Session;
using MethodInstance = internal::MethodInstance;
using AnyStreamWriter = internal::AnyStreamWriter;

RpcServer::RpcServer(std::string service_name,
                     std::string subspace_server_socket)
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
    const std::string &method, std::string_view request_type,
    std::string_view response_type,
    std::function<absl::Status(const google::protobuf::Any &,
                               google::protobuf::Any *, co::Coroutine *)>
        callback,
    MethodOptions &&options) {
  if (methods_.find(method) != methods_.end()) {
    return absl::AlreadyExistsError("Method already registered: " + method);
  }

  methods_[method] = std::make_shared<Method>(
      this, method, std::string{request_type}, std::string{response_type}, options.slot_size,
      options.num_slots, std::move(callback),
      options.id == -1 ? ++next_method_id_ : options.id);
  return absl::OkStatus();
}

// Register void method.
absl::Status RpcServer::RegisterMethod(
    const std::string &method, std::string_view request_type,
    std::function<absl::Status(const google::protobuf::Any &, co::Coroutine *)>
        callback,
    MethodOptions &&options) {
  if (methods_.find(method) != methods_.end()) {
    return absl::AlreadyExistsError("Method already registered: " + method);
  }
  methods_[method] = std::make_shared<Method>(
      this, method, std::string{request_type}, "subspace.VoidMessage", options.slot_size,
      options.num_slots,
      [callback](const google::protobuf::Any &req, google::protobuf::Any *res,
                 co::Coroutine *c) {
        auto status = callback(req, c);
        if (!status.ok()) {
          return status;
        }
        // The response for this void method is a VoidMessage packed
        // into a google.protobuf.Any.
        res->PackFrom(VoidMessage());
        return absl::OkStatus();
      },
      options.id == -1 ? ++next_method_id_ : options.id);
  return absl::OkStatus();
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<absl::Status(const std::vector<char> &, std::vector<char> *,
                               co::Coroutine *)>
        callback,
    MethodOptions &&options) {
  return RegisterMethod<RawMessage, RawMessage>(
      method,
      [callback = std::move(callback)](const RawMessage &req, RawMessage *res,
                                       co::Coroutine *c) -> absl::Status {
        std::vector<char> request(req.data().begin(), req.data().end());
        std::vector<char> response;
        auto status = callback(request, &response, c);
        if (!status.ok()) {
          return status;
        }
        res->set_data(response.data(), response.size());
        return absl::OkStatus();
      },
      std::move(options));
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<absl::Status(const absl::Span<const char> &,
                               std::vector<char> *, co::Coroutine *)>
        callback,
    MethodOptions &&options) {
  return RegisterMethod<RawMessage, RawMessage>(
      method,
      [callback = std::move(callback)](const RawMessage &req, RawMessage *res,
                                       co::Coroutine *c) -> absl::Status {
        const absl::Span<const char> request(req.data().data(),
                                             req.data().size());
        std::vector<char> response;
        auto status = callback(request, &response, c);
        if (!status.ok()) {
          return status;
        }
        // This is still a copy.
        res->set_data(response.data(), response.size());
        return absl::OkStatus();
      },
      std::move(options));
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method, std::string_view request_type,
    std::string_view response_type,
    std::function<absl::Status(const google::protobuf::Any &, AnyStreamWriter &,
                               co::Coroutine *)>
        callback,
    MethodOptions &&options) {
  if (methods_.find(method) != methods_.end()) {
    return absl::AlreadyExistsError("Method already registered: " + method);
  }

  methods_[method] = std::make_shared<Method>(
      this, method, std::string{request_type}, std::string{response_type}, options.slot_size,
      options.num_slots, std::move(callback),
      options.id == -1 ? ++next_method_id_ : options.id);
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
                     .type = "subspace.RpcServerRequest",
                     .max_active_messages = 1});
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

  for (auto &[id, method] : session->methods) {
    auto *m = response->add_methods();
    m->set_name(method->method->name);
    m->set_id(id);
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

    if (method->method->IsStreaming()) {
      m->set_cancel_channel(absl::StrFormat("%s/%d/%d",
                                            method->method->cancel_channel,
                                            client_id, session->session_id));
    }
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
  logger_.Log(toolbelt::LogLevel::kDebug, "Closed session: %d",
              session->session_id);
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<Session>>
RpcServer::CreateSession(uint64_t client_id) {
  auto session = std::make_shared<Session>();
  session->session_id = ++next_session_id_;
  session->client_id = client_id;
  for (auto &[name, method] : methods_) {
    auto method_instance = std::make_shared<MethodInstance>();
    method_instance->method = method;

    absl::StatusOr<subspace::Subscriber> sub = client_->CreateSubscriber(
        absl::StrFormat("%s/%d/%d", method->request_channel, session->client_id,
                        session->session_id),
        {.reliable = true, .type = method->request_type});
    if (!sub.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to create subscriber for method %s: %s",
                  method->name.c_str(), sub.status().ToString().c_str());
      return sub.status();
    }
    method_instance->request_subscriber =
        std::make_shared<subspace::Subscriber>(std::move(*sub));

    absl::StatusOr<subspace::Publisher> pub = client_->CreatePublisher(
        absl::StrFormat("%s/%d/%d", method->response_channel,
                        session->client_id, session->session_id),
        {.slot_size = method->slot_size,
         .num_slots = method->num_slots,
         .reliable = true,
         .type = method->response_type});
    if (!pub.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to create publisher for method %s: %s",
                  method->name.c_str(), pub.status().ToString().c_str());
      return pub.status();
    }
    method_instance->response_publisher =
        std::make_shared<subspace::Publisher>(std::move(*pub));

    // For a streaming method we create a cancel channel subscriber.
    if (method->IsStreaming()) {
      absl::StatusOr<subspace::Subscriber> cancel_sub =
          client_->CreateSubscriber(
              absl::StrFormat("%s/%d/%d", method->cancel_channel,
                              session->client_id, session->session_id),
              {.reliable = true, .type = "subspace.RpcCancelRequest"});
      if (!cancel_sub.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to create cancel subscriber for method %s: %s",
                    method->name.c_str(),
                    cancel_sub.status().ToString().c_str());
        return cancel_sub.status();
      }
      method_instance->cancel_subscriber =
          std::make_shared<subspace::Subscriber>(std::move(*cancel_sub));
    }

    session->methods.insert({method->id, method_instance});

    AddCoroutine(std::make_unique<co::Coroutine>(
        *scheduler_,
        [server = shared_from_this(), session,
         method_instance](co::Coroutine *c) {
          if (method_instance->method->IsStreaming()) {
            SessionStreamingMethodCoroutine(std::move(server), session,
                                            method_instance, c);
          } else {
            SessionMethodCoroutine(std::move(server), session, method_instance,
                                   c);
          }
        },
        absl::StrFormat("Session %d Method %s", session->session_id,
                        method->name.c_str())));
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

void RpcServer::SessionMethodCoroutine(
    std::shared_ptr<RpcServer> server, std::shared_ptr<Session> session,
    std::shared_ptr<MethodInstance> method_instance, co::Coroutine *c) {
  while (server->running_) {
    auto s = method_instance->request_subscriber->Wait(
        server->interrupt_pipe_.ReadFd(), c);
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
    for (;;) {
      auto m = method_instance->request_subscriber->ReadMessage();
      if (!m.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error reading message for method %s: %s",
                            method_instance->method->name.c_str(),
                            m.status().ToString().c_str());
        break;
      }
      if (m->length == 0) {
        // No message, continue waiting.
        break;
      }
      if (!request.ParseFromArray(m->buffer, m->length)) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error parsing request for method %s: %s",
                            method_instance->method->name.c_str(),
                            m.status().ToString().c_str());
        continue;
      }
      if (request.session_id() == session->session_id) {
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
    server->logger_.Log(toolbelt::LogLevel::kDebug, "Calling method %s",
                        method_instance->method->name.c_str());
    absl::Status method_status =
        method_instance->method->callback(request.argument(), result, c);
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
    server->logger_.Log(
        toolbelt::LogLevel::kDebug, "Publishing response for method %s: %s",
        method_instance->method->name.c_str(), response.DebugString().c_str());
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

void RpcServer::SessionStreamingMethodCoroutine(
    std::shared_ptr<RpcServer> server, std::shared_ptr<Session> session,
    std::shared_ptr<MethodInstance> method_instance, co::Coroutine *c) {
  while (server->running_) {
    auto s = method_instance->request_subscriber->Wait(
        server->interrupt_pipe_.ReadFd(), c);
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
    for (;;) {
      auto m = method_instance->request_subscriber->ReadMessage();
      if (!m.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error reading message for method %s: %s",
                            method_instance->method->name.c_str(),
                            m.status().ToString().c_str());
        break;
      }
      if (m->length == 0) {
        // No message, continue waiting.
        break;
      }
      if (!request.ParseFromArray(m->buffer, m->length)) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error parsing request for method %s: %s",
                            method_instance->method->name.c_str(),
                            m.status().ToString().c_str());
        continue;
      }
      if (request.session_id() == session->session_id) {
        request_ok = true;
        break;
      }
    }
    if (!request_ok) {
      continue;
    }

    server->logger_.Log(toolbelt::LogLevel::kDebug, "Calling method %s",
                        method_instance->method->name.c_str());

    AnyStreamWriter writer(server, session, method_instance, request);

    // Start a coroutine to read the cancellation channel and cancel the
    // StreamWriter if a cancellation request is received.
    server->AddCoroutine(std::make_unique<co::Coroutine>(
        *server->scheduler_, [server, session, method_instance, &writer,
                              &request](co::Coroutine *c) {
          toolbelt::FileDescriptor interrupt(
              dup(server->interrupt_pipe_.ReadFd().Fd()));
          while (!writer.IsCancelled()) {
            auto s = method_instance->cancel_subscriber->Wait(interrupt, c);
            if (!s.ok()) {
              server->logger_.Log(toolbelt::LogLevel::kError,
                                  "Error waiting for cancel: %s",
                                  s.status().ToString().c_str());
              return;
            }
            if (*s == interrupt.Fd()) {
              break;
            }
            bool request_ok = false;
            while (!request_ok) {
              auto msg = method_instance->cancel_subscriber->ReadMessage();
              if (!msg.ok()) {
                server->logger_.Log(toolbelt::LogLevel::kError,
                                    "Error reading cancel message: %s",
                                    msg.status().ToString().c_str());
                continue;
              }
              if (msg->length == 0) {
                // No message, continue waiting.
                break;
              }
              RpcCancelRequest cancel;
              if (!cancel.ParseFromArray(msg->buffer, msg->length)) {
                server->logger_.Log(toolbelt::LogLevel::kError,
                                    "Error parsing cancel message: %s",
                                    msg.status().ToString().c_str());
                continue;
              }
              if (cancel.session_id() == session->session_id &&
                  cancel.request_id() == request.request_id()) {
                request_ok = true;
                break;
              }
            }
            if (request_ok) {
              writer.Cancel();
            }
          }
        }));

    // Call the method and pass it StreamWriter that will be called to send back
    // a response to the client.
    absl::Status method_status =
        method_instance->method->stream_callback(request.argument(), writer, c);
    // If the method fails, we need to send an error response.
    if (!method_status.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error executing method %s: %s",
                          method_instance->method->name.c_str(),
                          method_status.ToString().c_str());
      SendRpcError(server, session, method_instance, request,
                   absl::StrFormat("Error executing method %s: %s",
                                   method_instance->method->name,
                                   method_status.ToString()),
                   c);
    }
  }
}

void RpcServer::SendRpcError(std::shared_ptr<RpcServer> server,
                             std::shared_ptr<Session> session,
                             std::shared_ptr<MethodInstance> method_instance,
                             const RpcRequest &request,
                             const std::string &error, co::Coroutine *c) {
  subspace::RpcResponse response;
  response.set_session_id(session->session_id);
  response.set_request_id(request.request_id());
  response.set_client_id(request.client_id());
  response.set_error(error);

  uint64_t length = response.ByteSizeLong();
  absl::StatusOr<void *> buffer;
  for (;;) {
    buffer =
        method_instance->response_publisher->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error getting buffer for error in method %s: %s",
                          method_instance->method->name.c_str(),
                          buffer.status().ToString().c_str());
      response.set_error(absl::StrFormat(
          "Error getting buffer for error in method %s: %s",
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
    return;
  }

  auto pub_result = method_instance->response_publisher->PublishMessage(length);
  if (!pub_result.ok()) {
    server->logger_.Log(toolbelt::LogLevel::kError,
                        "Error publishing error response for method %s: %s",
                        method_instance->method->name.c_str(),
                        pub_result.status().ToString().c_str());
  }
}

void RpcServer::SendStreamRpcResponse(
    std::shared_ptr<RpcServer> server, std::shared_ptr<Session> session,
    std::shared_ptr<MethodInstance> method_instance, const RpcRequest &request,
    std::unique_ptr<google::protobuf::Any> result, bool is_last,
    bool is_cancelled, co::Coroutine *c) {
  // Send response back to the client each time this is called
  subspace::RpcResponse response;
  response.set_session_id(session->session_id);
  response.set_request_id(request.request_id());
  response.set_client_id(request.client_id());
  if (result != nullptr) {
    response.set_allocated_result(result.release());
  }
  response.set_is_last(is_last);
  response.set_is_cancelled(is_cancelled);

  uint64_t length = response.ByteSizeLong();
  absl::StatusOr<void *> buffer;
  for (;;) {
    buffer =
        method_instance->response_publisher->GetMessageBuffer(int32_t(length));
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
    return;
  }
  server->logger_.Log(
      toolbelt::LogLevel::kDebug, "Publishing response for method %s: %s",
      method_instance->method->name.c_str(), response.DebugString().c_str());
  auto pub_result = method_instance->response_publisher->PublishMessage(length);
  if (!pub_result.ok()) {
    server->logger_.Log(toolbelt::LogLevel::kError,
                        "Error publishing response for method %s: %s",
                        method_instance->method->name.c_str(),
                        pub_result.status().ToString().c_str());
  }
  server->logger_.Log(toolbelt::LogLevel::kDebug,
                      "Published response for ˝method %s",
                      method_instance->method->name.c_str());
}

namespace internal {
bool AnyStreamWriter::Write(std::unique_ptr<google::protobuf::Any> res,
                            co::Coroutine *c) {
  if (IsCancelled()) {
    return false;
  }
  RpcServer::SendStreamRpcResponse(server, session, method_instance, request,
                                   std::move(res), false, IsCancelled(), c);
  return true;
}

void AnyStreamWriter::Finish(co::Coroutine *c) {
  RpcServer::SendStreamRpcResponse(server, session, method_instance, request,
                                   nullptr, true, IsCancelled(), c);
}

void Method::MakeChannelNames(RpcServer *server) {
  request_channel =
      absl::StrFormat("/rpc/%s/%s/request", server->Name(), this->name);
  response_channel =
      absl::StrFormat("/rpc/%s/%s/response", server->Name(), this->name);
  if (IsStreaming()) {
    cancel_channel =
        absl::StrFormat("/rpc/%s/%s/cancel", server->Name(), this->name);
  }
}
} // namespace internal
} // namespace subspace
