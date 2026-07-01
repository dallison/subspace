// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once
#include "absl/container/flat_hash_map.h"
#include "absl/types/span.h"
#include "client/client.h"
#include "co/coroutine.h"
#include "google/protobuf/any.pb.h"
#include "rpc/common/rpc_common.h"
#include "toolbelt/logging.h"
#include "toolbelt/pipe.h"

#include <atomic>

namespace subspace {

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

  // Returns true if the write worked, false if the request was cancelled.
  bool Write(std::unique_ptr<google::protobuf::Any> res, co::Coroutine *c);
  void Finish(co::Coroutine *c);

  void Cancel() { is_cancelled.store(true); }

  bool IsCancelled() const { return is_cancelled.load(); }

  std::shared_ptr<RpcServer> server;
  std::shared_ptr<Session> session;
  std::shared_ptr<MethodInstance> method_instance;
  RpcRequest request;
  std::atomic<bool> is_cancelled{false};
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
// this queue by the request coroutine (or by a later async completion from
// another coroutine) and drained by the response coroutine.  The whole RPC
// server runs on a single coroutine scheduler thread, so no locking is needed:
// only one coroutine runs at a time and a small SharedPtrPipe write does not
// yield, so writes never interleave.
struct ReplyQueue {
  explicit ReplyQueue(toolbelt::SharedPtrPipe<ReplyItem> pipe)
      : pipe(std::move(pipe)) {}

  toolbelt::SharedPtrPipe<ReplyItem> pipe;
};

struct Method {
  // Async handler: receives the request Any, a coroutine pointer (usable for
  // inline async work), a reply function for success and an error function for
  // failure.  Exactly one of reply/error_reply should be called.  They may be
  // called immediately, or kept and invoked later from any thread.
  Method(RpcServer *server, std::string name, std::string request_type,
         std::string response_type, int32_t slot_size, int32_t num_slots,
         std::function<void(
             const google::protobuf::Any &, co::Coroutine *,
             std::function<void(std::unique_ptr<google::protobuf::Any>)>,
             std::function<void(std::string)>)>
             callback,
         int id)
      : name(std::move(name)), request_type(std::move(request_type)),
        response_type(std::move(response_type)), slot_size(slot_size),
        num_slots(num_slots), async_callback(std::move(callback)), id(id) {
    MakeChannelNames(server);
  }
  // Streaming method constructor.
  Method(
      RpcServer *server, std::string name, std::string request_type,
      std::string response_type, int32_t slot_size, int32_t num_slots,
      std::function<absl::Status(const google::protobuf::Any &,
                                 internal::AnyStreamWriter &, co::Coroutine *)>
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
  std::function<void(const google::protobuf::Any &, co::Coroutine *,
                     std::function<void(std::unique_ptr<google::protobuf::Any>)>,
                     std::function<void(std::string)>)>
      async_callback;
  // Callback for a streaming method that produces multiple responses.
  std::function<absl::Status(const google::protobuf::Any &,
                             internal::AnyStreamWriter &, co::Coroutine *)>
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
  // Returns true if the write worked, false if the request was cancelled.
  bool Write(const Response &res, co::Coroutine *c) {
    auto any = std::make_unique<google::protobuf::Any>();
    any->PackFrom(res);
    return writer->Write(std::move(any), c);
  }

  void Finish(co::Coroutine *c) { writer->Finish(c); }

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

  // Run the server.  If you pass a coroutine scheduler this will use it
  // and the function will not block.  If you don't pass a scheduler
  // the server will use its own internal scheduler and this function will
  // block until the server is stopped (the Stop call).
  //
  // If you are using this in threaded application (most common probably),
  // just omit the scheduler argument and the thread will run the server.
  //
  // There is no use of threads inside the server itself.
  absl::Status Run(co::CoroutineScheduler *scheduler = nullptr);

  // Stop the server running.  This will signal a stop and will return
  // immediately.
  void Stop();

  // Register a method that takes a request and produces a response.  The
  // default channel parameters (slot size and number of slots) will be used.

  // Register a method that takes a request and produces a response.  You
  // specify the slot size and number of slots for the channels.
  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, Response *, co::Coroutine *)>
          callback,
      MethodOptions &&options = {});

  // Type void method with slot parameters.
  template <typename Request>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, co::Coroutine *)> callback,
      MethodOptions &&options = {});

  // Method that takes a raw message and returns a raw message.
  // NOTE: this will make a copy of the request into a vector<char>.  For a more
  // efficient version that doesn't copy, use the version below that takes a
  // span.
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const std::vector<char> &, std::vector<char> *,
                                 co::Coroutine *)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const absl::Span<const char> &,
                                 std::vector<char> *, co::Coroutine *)>
          callback,
      MethodOptions &&options = {});

  // Server streaming methods.  These take in a single request and produce
  // multiple responses.
  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, StreamWriter<Response> &,
                                 co::Coroutine *)>
          callback,
      MethodOptions &&options = {});

  // Register an async handler.  The callback receives the request Any, a
  // coroutine pointer (usable for inline async work), a reply function for
  // success and an error function for failure.  Exactly one of reply/error
  // should be called.  Unlike the synchronous RegisterMethod overloads, the
  // callback does not have to produce its result before returning: it may keep
  // the reply/error functions and invoke one of them later, possibly from
  // another thread.  request_type and response_type are the protobuf type
  // names advertised to clients; by default async methods exchange RawMessage
  // payloads.
  absl::Status RegisterMethodAsync(
      const std::string &method,
      std::function<void(
          const google::protobuf::Any &, co::Coroutine *,
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

  // These methods are non-templated and take google::protobuf::Any arguments
  // and responses.  They are notionally private but we need to access them
  // for the server test unit test.

  // Method with a request and response.
  absl::Status RegisterMethod(
      const std::string &method, std::string_view request_type,
      std::string_view response_type,
      std::function<absl::Status(const google::protobuf::Any &,
                                 google::protobuf::Any *, co::Coroutine *)>
          callback,
      MethodOptions &&options = {});

  absl::Status
  RegisterMethod(const std::string &method, std::string_view request_type,
                 std::function<absl::Status(const google::protobuf::Any &,
                                            co::Coroutine *)>
                     callback,
                 MethodOptions &&options = {});

  // Streaming method.
  absl::Status RegisterMethod(
      const std::string &method, std::string_view request_type,
      std::string_view response_type,
      std::function<absl::Status(const google::protobuf::Any &,
                                 internal::AnyStreamWriter &, co::Coroutine *)>
          callback,
      MethodOptions &&options = {});

  // For debugging we might need to get hold of the scheduler.
  co::CoroutineScheduler *Scheduler() { return scheduler_; }

private:
  friend struct internal::AnyStreamWriter;

  absl::Status CreateChannels();
  void AddCoroutine(std::unique_ptr<co::Coroutine> coroutine) {
    coroutines_.insert(std::move(coroutine));
  }
  static void ListenerCoroutine(std::shared_ptr<RpcServer> server,
                                co::Coroutine *c);

  absl::Status HandleIncomingRpcServerRequest(subspace::Message msg,
                                              co::Coroutine *c);
  absl::Status HandleOpen(uint64_t client_id,
                          const subspace::RpcOpenRequest &request,
                          subspace::RpcOpenResponse *response,
                          co::Coroutine *c);
  absl::Status HandleClose(uint64_t client_id,
                           const subspace::RpcCloseRequest &request,
                           subspace::RpcCloseResponse *response,
                           co::Coroutine *c);
  absl::Status
  PublishRpcServerResponse(const subspace::RpcServerResponse &response,
                           co::Coroutine *c);

  absl::StatusOr<std::shared_ptr<internal::Session>>
  CreateSession(uint64_t client_id);
  absl::Status DestroySession(int session_id);

  // Request coroutine for non-streaming methods: reads incoming requests and
  // invokes the method's async_callback.  The callback may reply immediately or
  // keep the reply function and complete later, possibly from another thread.
  static void SessionRequestCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      co::Coroutine *c);

  // Response coroutine for non-streaming methods: drains the reply queue and
  // publishes responses via the Subspace response publisher.
  static void SessionResponseCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      co::Coroutine *c);

  static void SessionStreamingMethodCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      co::Coroutine *c);

  static void SendStreamRpcResponse(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      const RpcRequest &request, std::unique_ptr<google::protobuf::Any> result,
      bool is_last, bool is_cancelled, co::Coroutine *c);
  static void
  SendRpcError(std::shared_ptr<RpcServer> server,
               std::shared_ptr<internal::Session> session,
               std::shared_ptr<internal::MethodInstance> method_instance,
               const RpcRequest &request, const std::string &error,
               co::Coroutine *c);

  std::string name_;
  std::string subspace_server_socket_;
  co::CoroutineScheduler local_scheduler_;
  co::CoroutineScheduler *scheduler_;
  std::shared_ptr<Client> client_;
  absl::flat_hash_map<std::string, std::shared_ptr<internal::Method>> methods_;
  toolbelt::Logger logger_;
  toolbelt::Pipe interrupt_pipe_;

  std::shared_ptr<subspace::Subscriber> request_receiver_;
  std::shared_ptr<subspace::Publisher> response_publisher_;
  absl::flat_hash_set<std::unique_ptr<co::Coroutine>> coroutines_;
  bool running_ = false;
  int32_t next_session_id_ = 0; // Next session ID.
  int next_method_id_ = 0;      // Next method ID.
  absl::flat_hash_map<int32_t, std::shared_ptr<internal::Session>> sessions_;
};

template <typename Request, typename Response>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<absl::Status(const Request &, Response *, co::Coroutine *)>
        callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();
  auto response_descriptor = Response::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(), response_descriptor->full_name(),
      [method, callback = std::move(callback), request_descriptor](
          const google::protobuf::Any &req, google::protobuf::Any *res,
          co::Coroutine *c) -> absl::Status {
        if (!req.Is<Request>()) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name(), req.type_url()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          return absl::InvalidArgumentError("Failed to unpack request");
        }
        Response response;
        auto status = callback(request, &response, c);
        if (!status.ok()) {
          return status;
        }
        res->PackFrom(response);
        return absl::OkStatus();
      },
      std::move(options));
}

template <typename Request>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<absl::Status(const Request &, co::Coroutine *)> callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(),
      [method, callback = std::move(callback), request_descriptor](
          const google::protobuf::Any &req, co::Coroutine *c) -> absl::Status {
        if (!req.Is<Request>()) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name(), req.type_url()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          return absl::InvalidArgumentError("Failed to unpack request");
        }
        return callback(request, c);
      },
      std::move(options));
}

template <typename Request, typename Response>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<absl::Status(const Request &, StreamWriter<Response> &,
                               co::Coroutine *)>
        callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();
  auto response_descriptor = Response::descriptor();

  StreamWriter<Response> typed_writer;
  return RegisterMethod(
      method, request_descriptor->full_name(), response_descriptor->full_name(),
      [method, callback = std::move(callback), request_descriptor,
       typed_writer](const google::protobuf::Any &req,
                     internal::AnyStreamWriter &writer,
                     co::Coroutine *c) mutable -> absl::Status {
        if (!req.Is<Request>()) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name(), req.type_url()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          return absl::InvalidArgumentError("Failed to unpack request");
        }
        typed_writer.SetWriter(&writer);
        auto status = callback(request, typed_writer, c);
        if (!status.ok()) {
          return status;
        }
        return absl::OkStatus();
      },
      std::move(options));
}
} // namespace subspace