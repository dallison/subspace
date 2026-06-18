// Copyright 2023-2026 David Allison
// co20 RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once
#include "absl/container/flat_hash_map.h"
#include "absl/types/span.h"
#include "client/client.h"
#include "co/coroutine_cpp20.h"
#include "co20_rpc/common/shared_ptr_pipe.h"
#include "google/protobuf/any.pb.h"
#include "rpc/common/rpc_common.h"
#include "toolbelt/logging.h"
#include "toolbelt/pipe.h"

namespace subspace::co20_rpc {

class RpcServer;

namespace internal {

struct Session;
struct MethodInstance;

struct AnyStreamWriter {
  AnyStreamWriter(std::shared_ptr<RpcServer> server,
                  std::shared_ptr<Session> session,
                  std::shared_ptr<MethodInstance> method_instance,
                  const RpcRequest &request)
      : server(std::move(server)), session(std::move(session)),
        method_instance(std::move(method_instance)), request(request) {}

  co20::ValueTask<bool> Write(std::unique_ptr<google::protobuf::Any> res);
  co20::ValueTask<void> Finish();

  void Cancel() { is_cancelled = true; }

  bool IsCancelled() const { return is_cancelled; }

  std::shared_ptr<RpcServer> server;
  std::shared_ptr<Session> session;
  std::shared_ptr<MethodInstance> method_instance;
  const RpcRequest &request;
  bool is_cancelled = false;
};

// An item pushed by either the reply function or the error function of an
// async method handler.  Exactly one of response or error_message is
// meaningful: a non-empty error_message produces an error response, otherwise
// `response` owns the result Any.  The session/request/client ids are copied
// from the originating RpcRequest so the response coroutine can build the
// RpcResponse without keeping the original request alive.
struct ReplyItem {
  std::unique_ptr<google::protobuf::Any> response;
  int32_t session_id;
  int32_t request_id;
  uint64_t client_id;
  std::string error_message;
};

// Completed replies for a single non-streaming method instance are pushed into
// this queue by the request coroutine (via the handler's reply/error callbacks)
// and drained by the response coroutine.  The whole RPC server runs on a single
// scheduler thread, so no locking is needed.
struct ReplyQueue {
  explicit ReplyQueue(SharedPtrPipe<ReplyItem> pipe) : pipe(std::move(pipe)) {}

  SharedPtrPipe<ReplyItem> pipe;
};

struct Method {
  // Async handler: receives the request Any, a reply function for success and
  // an error function for failure.  The handler is itself a coroutine
  // (co20::ValueTask) so it may co_await work before calling exactly one of
  // reply/error_reply.
  Method(RpcServer *server, std::string name, std::string request_type,
         std::string response_type, int32_t slot_size, int32_t num_slots,
         std::function<co20::ValueTask<void>(
             const google::protobuf::Any &,
             std::function<void(std::unique_ptr<google::protobuf::Any>)>,
             std::function<void(std::string)>)>
             callback,
         int id)
      : name(std::move(name)), request_type(std::move(request_type)),
        response_type(std::move(response_type)), slot_size(slot_size),
        num_slots(num_slots), async_callback(std::move(callback)), id(id) {
    MakeChannelNames(server);
  }

  Method(RpcServer *server, std::string name, std::string request_type,
         std::string response_type, int32_t slot_size, int32_t num_slots,
         std::function<co20::ValueTask<absl::Status>(
             const google::protobuf::Any &, internal::AnyStreamWriter &)>
             callback,
         int id)
      : name(std::move(name)), request_type(std::move(request_type)),
        response_type(std::move(response_type)), slot_size(slot_size),
        num_slots(num_slots), stream_callback(std::move(callback)), id(id) {
    MakeChannelNames(server);
  }

  void MakeChannelNames(RpcServer *server);

  bool IsStreaming() const { return stream_callback != nullptr; }

  std::string name;
  std::string request_type;
  std::string response_type;
  int32_t slot_size;
  int32_t num_slots;
  // Async handler for a normal, non-streaming method.  See the constructor
  // above for the calling convention.
  std::function<co20::ValueTask<void>(
      const google::protobuf::Any &,
      std::function<void(std::unique_ptr<google::protobuf::Any>)>,
      std::function<void(std::string)>)>
      async_callback;
  std::function<co20::ValueTask<absl::Status>(const google::protobuf::Any &,
                                              internal::AnyStreamWriter &)>
      stream_callback;
  std::string request_channel;
  std::string response_channel;
  std::string cancel_channel;
  int id;
};

struct MethodInstance {
  std::shared_ptr<Method> method;
  std::shared_ptr<subspace::Subscriber> request_subscriber;
  std::shared_ptr<subspace::Publisher> response_publisher;
  std::shared_ptr<subspace::Subscriber> cancel_subscriber;
  // Non-null for non-streaming methods: completed responses are pushed here by
  // the request coroutine and drained by the response coroutine.
  std::shared_ptr<ReplyQueue> reply_queue;
};

struct Session {
  int session_id;
  uint64_t client_id;
  absl::flat_hash_map<int, std::shared_ptr<MethodInstance>> methods;
};

} // namespace internal

template <typename Response> struct StreamWriter {
  co20::ValueTask<bool> Write(const Response &res) {
    auto any = std::make_unique<google::protobuf::Any>();
    any->PackFrom(res);
    co_return co_await writer->Write(std::move(any));
  }

  co20::ValueTask<void> Finish() { co_await writer->Finish(); }

  void Cancel() { writer->Cancel(); }

  bool IsCancelled() const { return writer->IsCancelled(); }

  void SetWriter(internal::AnyStreamWriter *writer) { this->writer = writer; }
  internal::AnyStreamWriter *writer;
};

class RpcServer : public std::enable_shared_from_this<RpcServer> {
public:
  RpcServer(std::string service_name,
            std::string subspace_server_socket = "/tmp/subspace");

  ~RpcServer() = default;

  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }

  void SetStartingSessionId(int session_id) { next_session_id_ = session_id; }

  // Run the server. If a scheduler is provided, it will be used and this
  // function will not block. Otherwise an internal scheduler is created and
  // this function blocks until Stop() is called.
  absl::Status Run(co20::Scheduler *scheduler = nullptr);

  void Stop();

  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<co20::ValueTask<absl::Status>(const Request &, Response *)>
          callback,
      MethodOptions &&options = {});

  template <typename Request>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<co20::ValueTask<absl::Status>(const Request &)> callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method,
      std::function<co20::ValueTask<absl::Status>(const std::vector<char> &,
                                                  std::vector<char> *)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method,
      std::function<co20::ValueTask<absl::Status>(
          const absl::Span<const char> &, std::vector<char> *)>
          callback,
      MethodOptions &&options = {});

  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<co20::ValueTask<absl::Status>(const Request &,
                                                  StreamWriter<Response> &)>
          callback,
      MethodOptions &&options = {});

  // Register an async handler.  The callback is a coroutine that receives the
  // request Any plus a reply function for success and an error function for
  // failure; it may co_await work before calling exactly one of them.  The
  // reply/error functions must be invoked from the request coroutine; they push
  // the completed result into the method's reply queue, which the response
  // coroutine drains and publishes.  request_type and response_type are the
  // protobuf type names advertised to clients.
  absl::Status RegisterMethodAsync(
      const std::string &method,
      std::function<co20::ValueTask<void>(
          const google::protobuf::Any &,
          std::function<void(std::unique_ptr<google::protobuf::Any>)>,
          std::function<void(std::string)>)>
          callback,
      MethodOptions &&options = {},
      std::string_view request_type = "subspace.RawMessage",
      std::string_view response_type = "subspace.RawMessage");

  absl::Status UnregisterMethod(const std::string &method) {
    auto it = methods_.find(method);
    if (it == methods_.end()) {
      return absl::NotFoundError("Method not found: " + method);
    }
    methods_.erase(it);
    return absl::OkStatus();
  }

  const std::string &Name() const { return name_; }

  absl::Status RegisterMethod(
      const std::string &method, std::string_view request_type,
      std::string_view response_type,
      std::function<co20::ValueTask<absl::Status>(
          const google::protobuf::Any &, google::protobuf::Any *)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method, std::string_view request_type,
      std::function<co20::ValueTask<absl::Status>(
          const google::protobuf::Any &)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method, std::string_view request_type,
      std::string_view response_type,
      std::function<co20::ValueTask<absl::Status>(
          const google::protobuf::Any &, internal::AnyStreamWriter &)>
          callback,
      MethodOptions &&options = {});

  co20::Scheduler *Scheduler() { return scheduler_; }

private:
  friend struct internal::AnyStreamWriter;

  absl::Status CreateChannels();

  static co20::ValueTask<void>
  ListenerCoroutine(std::shared_ptr<RpcServer> server, co20::Coroutine &co);

  co20::ValueTask<absl::Status>
  HandleIncomingRpcServerRequest(subspace::Message msg);

  co20::ValueTask<absl::Status>
  HandleOpen(uint64_t client_id, const subspace::RpcOpenRequest &request,
             subspace::RpcOpenResponse *response);

  co20::ValueTask<absl::Status>
  HandleClose(uint64_t client_id, const subspace::RpcCloseRequest &request,
              subspace::RpcCloseResponse *response);

  co20::ValueTask<absl::Status>
  PublishRpcServerResponse(const subspace::RpcServerResponse &response);

  absl::StatusOr<std::shared_ptr<internal::Session>>
  CreateSession(uint64_t client_id);

  absl::Status DestroySession(int session_id);

  // Request coroutine for non-streaming methods: reads incoming requests and
  // invokes the method's async_callback (itself a coroutine).  The handler
  // replies via the reply/error callbacks, which enqueue into the reply queue.
  static co20::ValueTask<void> SessionRequestCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      co20::Coroutine &co);

  // Response coroutine for non-streaming methods: drains the reply queue and
  // publishes responses via the Subspace response publisher.
  static co20::ValueTask<void> SessionResponseCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      co20::Coroutine &co);

  static co20::ValueTask<void> SessionStreamingMethodCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      co20::Coroutine &co);

  static co20::ValueTask<void> SendStreamRpcResponse(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      const RpcRequest &request, std::unique_ptr<google::protobuf::Any> result,
      bool is_last, bool is_cancelled);

  static co20::ValueTask<void>
  SendRpcError(std::shared_ptr<RpcServer> server,
               std::shared_ptr<internal::Session> session,
               std::shared_ptr<internal::MethodInstance> method_instance,
               const RpcRequest &request, const std::string &error);

  std::string name_;
  std::string subspace_server_socket_;
  co20::Scheduler local_scheduler_;
  co20::Scheduler *scheduler_;
  std::shared_ptr<Client> client_;
  absl::flat_hash_map<std::string, std::shared_ptr<internal::Method>> methods_;
  toolbelt::Logger logger_;
  toolbelt::Pipe interrupt_pipe_;

  std::shared_ptr<subspace::Subscriber> request_receiver_;
  std::shared_ptr<subspace::Publisher> response_publisher_;
  bool running_ = false;
  int32_t next_session_id_ = 0;
  int next_method_id_ = 0;
  absl::flat_hash_map<int32_t, std::shared_ptr<internal::Session>> sessions_;
};

template <typename Request, typename Response>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<co20::ValueTask<absl::Status>(const Request &, Response *)>
        callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();
  auto response_descriptor = Response::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(), response_descriptor->full_name(),
      [method, callback = std::move(callback), request_descriptor](
          const google::protobuf::Any &req,
          google::protobuf::Any *res) -> co20::ValueTask<absl::Status> {
        if (!req.Is<Request>()) {
          co_return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name(), req.type_url()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          co_return absl::InvalidArgumentError("Failed to unpack request");
        }
        Response response;
        auto status = co_await callback(request, &response);
        if (!status.ok()) {
          co_return status;
        }
        res->PackFrom(response);
        co_return absl::OkStatus();
      },
      std::move(options));
}

template <typename Request>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<co20::ValueTask<absl::Status>(const Request &)> callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(),
      [method, callback = std::move(callback),
       request_descriptor](const google::protobuf::Any &req)
          -> co20::ValueTask<absl::Status> {
        if (!req.Is<Request>()) {
          co_return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name(), req.type_url()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          co_return absl::InvalidArgumentError("Failed to unpack request");
        }
        co_return co_await callback(request);
      },
      std::move(options));
}

template <typename Request, typename Response>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<co20::ValueTask<absl::Status>(const Request &,
                                                StreamWriter<Response> &)>
        callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();
  auto response_descriptor = Response::descriptor();

  StreamWriter<Response> typed_writer;
  return RegisterMethod(
      method, request_descriptor->full_name(), response_descriptor->full_name(),
      [method, callback = std::move(callback), request_descriptor,
       typed_writer](const google::protobuf::Any &req,
                     internal::AnyStreamWriter &writer) mutable
      -> co20::ValueTask<absl::Status> {
        if (!req.Is<Request>()) {
          co_return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name(), req.type_url()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          co_return absl::InvalidArgumentError("Failed to unpack request");
        }
        typed_writer.SetWriter(&writer);
        auto status = co_await callback(request, typed_writer);
        if (!status.ok()) {
          co_return status;
        }
        co_return absl::OkStatus();
      },
      std::move(options));
}
} // namespace subspace::co20_rpc
