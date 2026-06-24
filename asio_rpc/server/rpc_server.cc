// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asio_rpc/server/rpc_server.h"
#include "asio_rpc/common/async_wait.h"
#include "proto/subspace.pb.h"
#include <inttypes.h>
#include <stdio.h>

namespace subspace::asio_rpc {

using Method = internal::Method;
using Session = internal::Session;
using MethodInstance = internal::MethodInstance;
using AnyStreamWriter = internal::AnyStreamWriter;

RpcServer::RpcServer(std::string service_name,
                     std::string subspace_server_socket)
    : name_(std::move(service_name)),
      subspace_server_socket_(std::move(subspace_server_socket)),
      io_context_(nullptr), logger_("asio_rpcserver") {
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
  char buf = 1;
  (void)::write(interrupt_pipe_.WriteFd().Fd(), &buf, 1);
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method, std::string_view request_type,
    std::string_view response_type,
    std::function<absl::Status(const google::protobuf::Any &,
                               google::protobuf::Any *,
                               boost::asio::yield_context)>
        callback,
    MethodOptions &&options) {
  return RegisterMethodAsync(
      method,
      [method, callback = std::move(callback)](
          const google::protobuf::Any &req, boost::asio::yield_context yield,
          std::function<void(std::unique_ptr<google::protobuf::Any>)> reply,
          std::function<void(std::string)> error_reply) {
        auto res = std::make_unique<google::protobuf::Any>();
        auto status = callback(req, res.get(), yield);
        if (!status.ok()) {
          error_reply(absl::StrFormat("Error executing method %s: %s", method,
                                      status.ToString()));
          return;
        }
        reply(std::move(res));
      },
      std::move(options), request_type, response_type);
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method, std::string_view request_type,
    std::function<absl::Status(const google::protobuf::Any &,
                               boost::asio::yield_context)>
        callback,
    MethodOptions &&options) {
  return RegisterMethodAsync(
      method,
      [method, callback = std::move(callback)](
          const google::protobuf::Any &req, boost::asio::yield_context yield,
          std::function<void(std::unique_ptr<google::protobuf::Any>)> reply,
          std::function<void(std::string)> error_reply) {
        auto status = callback(req, yield);
        if (!status.ok()) {
          error_reply(absl::StrFormat("Error executing method %s: %s", method,
                                      status.ToString()));
          return;
        }
        auto res = std::make_unique<google::protobuf::Any>();
        res->PackFrom(VoidMessage());
        reply(std::move(res));
      },
      std::move(options), request_type, "subspace.VoidMessage");
}

absl::Status RpcServer::RegisterMethodAsync(
    const std::string &method,
    std::function<void(
        const google::protobuf::Any &, boost::asio::yield_context,
        std::function<void(std::unique_ptr<google::protobuf::Any>)>,
        std::function<void(std::string)>)>
        callback,
    MethodOptions &&options, std::string_view request_type,
    std::string_view response_type) {
  if (methods_.find(method) != methods_.end()) {
    return absl::AlreadyExistsError("Method already registered: " + method);
  }

  methods_[method] = std::make_shared<Method>(
      this, method, std::string{request_type}, std::string{response_type},
      options.slot_size, options.num_slots, std::move(callback),
      options.id == -1 ? ++next_method_id_ : options.id);
  return absl::OkStatus();
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<absl::Status(const std::vector<char> &, std::vector<char> *,
                               boost::asio::yield_context)>
        callback,
    MethodOptions &&options) {
  return RegisterMethod<RawMessage, RawMessage>(
      method,
      [callback = std::move(callback)](const RawMessage &req, RawMessage *res,
                                       boost::asio::yield_context yield)
          -> absl::Status {
        std::vector<char> request(req.data().begin(), req.data().end());
        std::vector<char> response;
        auto status = callback(request, &response, yield);
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
                               std::vector<char> *,
                               boost::asio::yield_context)>
        callback,
    MethodOptions &&options) {
  return RegisterMethod<RawMessage, RawMessage>(
      method,
      [callback = std::move(callback)](const RawMessage &req, RawMessage *res,
                                       boost::asio::yield_context yield)
          -> absl::Status {
        const absl::Span<const char> request(req.data().data(),
                                             req.data().size());
        std::vector<char> response;
        auto status = callback(request, &response, yield);
        if (!status.ok()) {
          return status;
        }
        res->set_data(response.data(), response.size());
        return absl::OkStatus();
      },
      std::move(options));
}

absl::Status RpcServer::RegisterMethod(
    const std::string &method, std::string_view request_type,
    std::string_view response_type,
    std::function<absl::Status(const google::protobuf::Any &,
                               AnyStreamWriter &,
                               boost::asio::yield_context)>
        callback,
    MethodOptions &&options) {
  if (methods_.find(method) != methods_.end()) {
    return absl::AlreadyExistsError("Method already registered: " + method);
  }

  methods_[method] = std::make_shared<Method>(
      this, method, std::string{request_type}, std::string{response_type},
      options.slot_size, options.num_slots, std::move(callback),
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

absl::Status RpcServer::Run(boost::asio::io_context *ioc) {
  if (ioc != nullptr) {
    io_context_ = ioc;
  } else {
    io_context_ = &local_io_context_;
  }

  absl::Status status = CreateChannels();
  if (!status.ok()) {
    return status;
  }

  running_ = true;
  boost::asio::spawn(
      *io_context_,
      [server = shared_from_this()](boost::asio::yield_context yield) {
        ListenerCoroutine(server, yield);
      },
      boost::asio::detached);

  if (ioc == nullptr) {
    io_context_->run();
  }

  return absl::OkStatus();
}

void RpcServer::ListenerCoroutine(std::shared_ptr<RpcServer> server,
                                  boost::asio::yield_context yield) {
  while (server->running_) {
    int sub_fd = server->request_receiver_->GetPollFd().fd;
    int interrupt_fd = server->interrupt_pipe_.ReadFd().Fd();

    auto fd = async_wait_either(*server->io_context_, sub_fd, interrupt_fd,
                                yield);
    if (!fd.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for request: %s",
                          fd.status().ToString().c_str());
      continue;
    }
    if (*fd == interrupt_fd) {
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
              server->HandleIncomingRpcServerRequest(std::move(*msg), yield);
          !status.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error handling RPC request: %s",
                            status.ToString().c_str());
      }
    }
  }
}

absl::Status
RpcServer::HandleIncomingRpcServerRequest(subspace::Message msg,
                                          boost::asio::yield_context yield) {
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
                                 response.mutable_open(), yield);
        !status.ok()) {
      response.set_error(status.ToString());
    }
    break;
  }
  case subspace::RpcServerRequest::kClose: {
    if (auto status = HandleClose(request.client_id(), request.close(),
                                  response.mutable_close(), yield);
        !status.ok()) {
      response.set_error(status.ToString());
    }
    break;
  }
  default:
    response.set_error("Unknown request type");
    break;
  }

  return PublishRpcServerResponse(response, yield);
}

absl::Status RpcServer::PublishRpcServerResponse(
    const subspace::RpcServerResponse &response,
    boost::asio::yield_context yield) {
  uint64_t length = response.ByteSizeLong();
  for (;;) {
    absl::StatusOr<void *> buffer =
        response_publisher_->GetMessageBuffer(length);
    if (!buffer.ok()) {
      return buffer.status();
    }
    if (*buffer == nullptr) {
      int pub_fd = response_publisher_->GetPollFd().fd;
      int interrupt_fd = interrupt_pipe_.ReadFd().Fd();
      auto status =
          async_wait_either(*io_context_, pub_fd, interrupt_fd, yield);
      if (!status.ok()) {
        return status.status();
      }
      if (*status == interrupt_fd) {
        return absl::OkStatus();
      }
      continue;
    }

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

absl::Status
RpcServer::HandleOpen(uint64_t client_id,
                      const subspace::RpcOpenRequest &request,
                      subspace::RpcOpenResponse *response,
                      [[maybe_unused]] boost::asio::yield_context yield) {
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Handling Open request from client %" PRId64 ", %s", client_id,
              request.DebugString().c_str());

  auto s = CreateSession(client_id, yield);
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

absl::Status RpcServer::HandleClose(
    uint64_t client_id, const subspace::RpcCloseRequest &request,
    [[maybe_unused]] subspace::RpcCloseResponse *response,
    [[maybe_unused]] boost::asio::yield_context yield) {
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Handling Close request from client %" PRId64 ", %s", client_id,
              request.DebugString().c_str());
  auto it = sessions_.find(request.session_id());
  if (it == sessions_.end()) {
    return absl::NotFoundError("Session not found");
  }
  auto session = it->second;
  sessions_.erase(session->session_id);
  logger_.Log(toolbelt::LogLevel::kDebug, "Closed session: %d",
              session->session_id);
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<Session>>
RpcServer::CreateSession(uint64_t client_id,
                         [[maybe_unused]] boost::asio::yield_context yield) {
  auto session = std::make_shared<Session>();
  session->session_id = ++next_session_id_;
  session->client_id = client_id;
  for (auto &[name, method] : methods_) {
    auto method_instance = std::make_shared<MethodInstance>();
    method_instance->method = method;

    absl::StatusOr<subspace::Subscriber> sub = client_->CreateSubscriber(
        absl::StrFormat("%s/%d/%d", method->request_channel,
                        session->client_id, session->session_id),
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

    if (method->IsStreaming()) {
      boost::asio::spawn(
          *io_context_,
          [server = shared_from_this(), session,
           method_instance](boost::asio::yield_context yield) {
            SessionStreamingMethodCoroutine(std::move(server), session,
                                            method_instance, yield);
          },
          boost::asio::detached);
    } else {
      // Non-streaming methods use a two-coroutine pipeline: a request coroutine
      // that invokes the (possibly async) handler and a response coroutine that
      // publishes completed replies.  They communicate via a SharedPtrPipe of
      // ReplyItems.
      auto pipe = SharedPtrPipe<internal::ReplyItem>::Create();
      if (!pipe.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to create reply queue for method %s: %s",
                    method->name.c_str(), pipe.status().ToString().c_str());
        return pipe.status();
      }
      method_instance->reply_queue =
          std::make_shared<internal::ReplyQueue>(std::move(*pipe));

      boost::asio::spawn(
          *io_context_,
          [server = shared_from_this(), session,
           method_instance](boost::asio::yield_context yield) {
            SessionRequestCoroutine(std::move(server), session, method_instance,
                                    yield);
          },
          boost::asio::detached);
      boost::asio::spawn(
          *io_context_,
          [server = shared_from_this(), session,
           method_instance](boost::asio::yield_context yield) {
            SessionResponseCoroutine(std::move(server), session,
                                     method_instance, yield);
          },
          boost::asio::detached);
    }
  }
  sessions_[session->session_id] = session;
  logger_.Log(toolbelt::LogLevel::kDebug, "Created session: %d",
              session->session_id);
  return session;
}

absl::Status RpcServer::DestroySession(int session_id) {
  sessions_.erase(session_id);
  return absl::OkStatus();
}

void RpcServer::SessionRequestCoroutine(
    std::shared_ptr<RpcServer> server, std::shared_ptr<Session> session,
    std::shared_ptr<MethodInstance> method_instance,
    boost::asio::yield_context yield) {
  while (server->running_) {
    int sub_fd = method_instance->request_subscriber->GetPollFd().fd;
    int interrupt_fd = server->interrupt_pipe_.ReadFd().Fd();

    auto s =
        async_wait_either(*server->io_context_, sub_fd, interrupt_fd, yield);
    if (!s.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for request: %s",
                          s.status().ToString().c_str());
      return;
    }
    if (*s == interrupt_fd) {
      break;
    }
    // Drain all requests currently available for this method.  Requests for
    // other sessions can share the same channel, so only this session's
    // messages are dispatched to the handler below.
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
        break;
      }
      subspace::RpcRequest request;
      if (!request.ParseFromArray(m->buffer, m->length)) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error parsing request for method %s",
                            method_instance->method->name.c_str());
        continue;
      }
      if (request.session_id() != session->session_id) {
        continue;
      }

      // Give the handler reply handles tied to this request.  The handler runs
      // on this request coroutine (yield), so the completed result is pushed
      // into the reply queue from here and the response coroutine routes it
      // back to the client that issued this request.  Writes pass yield so
      // that, if the reply pipe is momentarily full, the write suspends this
      // coroutine (letting the response coroutine drain it) instead of blocking
      // the io_context thread.  No locking is needed: the server runs on a
      // single io_context thread.  Reply functions must only be invoked from
      // this coroutine.
      auto queue = method_instance->reply_queue;
      auto reply_fn =
          [queue, yield, session_id = session->session_id,
           request_id = request.request_id(), client_id = request.client_id()](
              std::unique_ptr<google::protobuf::Any> response) mutable {
            auto item = std::make_shared<internal::ReplyItem>();
            item->session_id = session_id;
            item->request_id = request_id;
            item->client_id = client_id;
            item->response = std::move(response);
            (void)queue->pipe.Write(std::move(item), yield);
          };
      auto error_fn =
          [queue, yield, session_id = session->session_id,
           request_id = request.request_id(),
           client_id = request.client_id()](std::string error_msg) mutable {
            auto item = std::make_shared<internal::ReplyItem>();
            item->session_id = session_id;
            item->request_id = request_id;
            item->client_id = client_id;
            item->error_message = std::move(error_msg);
            (void)queue->pipe.Write(std::move(item), yield);
          };

      server->logger_.Log(toolbelt::LogLevel::kDebug, "Calling method %s",
                          method_instance->method->name.c_str());
      method_instance->method->async_callback(
          request.argument(), yield, std::move(reply_fn), std::move(error_fn));
    }
  }
}

void RpcServer::SessionResponseCoroutine(
    std::shared_ptr<RpcServer> server,
    [[maybe_unused]] std::shared_ptr<Session> session,
    std::shared_ptr<MethodInstance> method_instance,
    boost::asio::yield_context yield) {
  auto &queue = *method_instance->reply_queue;
  // dup the interrupt fd so this coroutine can wait on it concurrently with the
  // request coroutine without two waiters racing on the same descriptor.
  int interrupt_fd = ::dup(server->interrupt_pipe_.ReadFd().Fd());
  toolbelt::FileDescriptor interrupt(interrupt_fd);

  while (server->running_) {
    auto fd = async_wait_either(*server->io_context_, queue.pipe.ReadFd(),
                                interrupt.Fd(), yield);
    if (!fd.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for queued response: %s",
                          fd.status().ToString().c_str());
      return;
    }
    if (*fd == interrupt.Fd()) {
      break;
    }

    auto item_or = queue.pipe.Read(yield);
    if (!item_or.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error reading queued response for method %s: %s",
                          method_instance->method->name.c_str(),
                          item_or.status().ToString().c_str());
      continue;
    }
    auto item = std::move(*item_or);

    // Convert the completed handler result into the protocol response expected
    // by the waiting client.
    subspace::RpcResponse response;
    response.set_session_id(item->session_id);
    response.set_request_id(item->request_id);
    response.set_client_id(item->client_id);
    if (!item->error_message.empty()) {
      response.set_error(item->error_message);
    } else if (item->response != nullptr) {
      response.mutable_result()->CopyFrom(*item->response);
    } else {
      response.set_error("handler produced a null response");
    }

    uint64_t length = response.ByteSizeLong();
    absl::StatusOr<void *> buffer;
    bool got_buffer = false;
    for (;;) {
      buffer = method_instance->response_publisher->GetMessageBuffer(
          int32_t(length));
      if (!buffer.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error getting buffer for method %s: %s",
                            method_instance->method->name.c_str(),
                            buffer.status().ToString().c_str());
        break;
      }
      if (*buffer != nullptr) {
        got_buffer = true;
        break;
      }
      if (!server->interrupt_pipe_.ReadFd().Valid()) {
        return;
      }
      // The response channel is temporarily full; wait for the client to free a
      // slot or for shutdown.
      int pub_fd = method_instance->response_publisher->GetPollFd().fd;
      auto status = async_wait_either(*server->io_context_, pub_fd,
                                      interrupt.Fd(), yield);
      if (!status.ok()) {
        server->logger_.Log(toolbelt::LogLevel::kError,
                            "Error waiting for buffer: %s",
                            status.status().ToString().c_str());
        return;
      }
      if (*status == interrupt.Fd()) {
        return;
      }
    }
    if (!got_buffer) {
      continue;
    }
    if (!response.SerializeToArray(*buffer, length)) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error serializing response for method %s",
                          method_instance->method->name.c_str());
      continue;
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
    std::shared_ptr<MethodInstance> method_instance,
    boost::asio::yield_context yield) {
  while (server->running_) {
    int sub_fd = method_instance->request_subscriber->GetPollFd().fd;
    int interrupt_fd = server->interrupt_pipe_.ReadFd().Fd();

    auto s =
        async_wait_either(*server->io_context_, sub_fd, interrupt_fd, yield);
    if (!s.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for request: %s",
                          s.status().ToString().c_str());
      return;
    }
    if (*s == interrupt_fd) {
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
    auto stream_state = writer.State();
    auto stop_pipe = toolbelt::Pipe::Create();
    if (!stop_pipe.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Failed to create stream cancel stop pipe: %s",
                          stop_pipe.status().ToString().c_str());
      SendRpcError(server, session, method_instance, request,
                   "Failed to create stream cancel watcher", yield);
      continue;
    }
    auto cancel_stop_pipe =
        std::make_shared<toolbelt::Pipe>(std::move(*stop_pipe));

    // Spawn a coroutine to read the cancellation channel.
    boost::asio::spawn(
        *server->io_context_,
        [server, session, method_instance, stream_state,
         cancel_stop_pipe](boost::asio::yield_context yield) {
          while (!stream_state->ShouldStopWatcher() &&
                 !stream_state->IsCancelled()) {
            int cancel_fd =
                method_instance->cancel_subscriber->GetPollFd().fd;
            int stop_fd = cancel_stop_pipe->ReadFd().Fd();
            auto s =
                async_wait_either(*server->io_context_, cancel_fd, stop_fd,
                                  yield);
            if (!s.ok()) {
              server->logger_.Log(toolbelt::LogLevel::kError,
                                  "Error waiting for cancel: %s",
                                  s.status().ToString().c_str());
              return;
            }
            if (*s == stop_fd || stream_state->ShouldStopWatcher()) {
              break;
            }
            bool cancel_ok = false;
            while (!cancel_ok) {
              auto msg = method_instance->cancel_subscriber->ReadMessage();
              if (!msg.ok()) {
                server->logger_.Log(toolbelt::LogLevel::kError,
                                    "Error reading cancel message: %s",
                                    msg.status().ToString().c_str());
                continue;
              }
              if (msg->length == 0) {
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
                  cancel.request_id() == stream_state->request.request_id()) {
                cancel_ok = true;
                break;
              }
            }
            if (cancel_ok) {
              stream_state->Cancel();
            }
          }
        },
        boost::asio::detached);

    absl::Status method_status =
        method_instance->method->stream_callback(request.argument(), writer,
                                                 yield);
    stream_state->StopWatcher();
    char stop_byte = 1;
    (void)::write(cancel_stop_pipe->WriteFd().Fd(), &stop_byte, 1);
    if (!method_status.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error executing method %s: %s",
                          method_instance->method->name.c_str(),
                          method_status.ToString().c_str());
      SendRpcError(server, session, method_instance, request,
                   absl::StrFormat("Error executing method %s: %s",
                                   method_instance->method->name,
                                   method_status.ToString()),
                   yield);
    }
  }
}

void RpcServer::SendRpcError(std::shared_ptr<RpcServer> server,
                             std::shared_ptr<Session> session,
                             std::shared_ptr<MethodInstance> method_instance,
                             const RpcRequest &request,
                             const std::string &error,
                             boost::asio::yield_context yield) {
  subspace::RpcResponse response;
  response.set_session_id(session->session_id);
  response.set_request_id(request.request_id());
  response.set_client_id(request.client_id());
  response.set_error(error);

  uint64_t length = response.ByteSizeLong();
  absl::StatusOr<void *> buffer;
  int interrupt_fd = server->interrupt_pipe_.ReadFd().Fd();
  for (;;) {
    buffer =
        method_instance->response_publisher->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error getting buffer for error in method %s: %s",
                          method_instance->method->name.c_str(),
                          buffer.status().ToString().c_str());
      return;
    }
    if (*buffer != nullptr) {
      break;
    }
    if (!server->interrupt_pipe_.ReadFd().Valid()) {
      return;
    }
    int pub_fd = method_instance->response_publisher->GetPollFd().fd;
    auto status =
        async_wait_either(*server->io_context_, pub_fd, interrupt_fd, yield);
    if (!status.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for buffer: %s",
                          status.status().ToString().c_str());
      return;
    }
    if (*status == interrupt_fd) {
      return;
    }
  }
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
    bool is_cancelled, boost::asio::yield_context yield) {
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
  int interrupt_fd = server->interrupt_pipe_.ReadFd().Fd();
  for (;;) {
    buffer =
        method_instance->response_publisher->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error getting buffer for method %s: %s",
                          method_instance->method->name.c_str(),
                          buffer.status().ToString().c_str());
      return;
    }
    if (*buffer != nullptr) {
      break;
    }
    if (!server->interrupt_pipe_.ReadFd().Valid()) {
      return;
    }
    int pub_fd = method_instance->response_publisher->GetPollFd().fd;
    auto status =
        async_wait_either(*server->io_context_, pub_fd, interrupt_fd, yield);
    if (!status.ok()) {
      server->logger_.Log(toolbelt::LogLevel::kError,
                          "Error waiting for buffer: %s",
                          status.status().ToString().c_str());
      return;
    }
    if (*status == interrupt_fd) {
      return;
    }
  }
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
                      "Published response for method %s",
                      method_instance->method->name.c_str());
}

namespace internal {
bool AnyStreamWriter::Write(std::unique_ptr<google::protobuf::Any> res,
                            boost::asio::yield_context yield) {
  if (IsCancelled()) {
    return false;
  }
  RpcServer::SendStreamRpcResponse(server, session, method_instance,
                                   state->request, std::move(res), false,
                                   IsCancelled(), yield);
  return true;
}

void AnyStreamWriter::Finish(boost::asio::yield_context yield) {
  RpcServer::SendStreamRpcResponse(server, session, method_instance,
                                   state->request, nullptr, true,
                                   IsCancelled(), yield);
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
} // namespace subspace::asio_rpc
