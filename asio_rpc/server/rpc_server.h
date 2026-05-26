// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once
#include "absl/container/flat_hash_map.h"
#include "absl/types/span.h"
#include "client/client.h"
#include "google/protobuf/any.pb.h"
#include "rpc/common/rpc_common.h"
#include "toolbelt/logging.h"
#include "toolbelt/pipe.h"

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

namespace subspace::asio_rpc {

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

  bool Write(std::unique_ptr<google::protobuf::Any> res,
             boost::asio::yield_context yield);
  void Finish(boost::asio::yield_context yield);

  void Cancel() { is_cancelled = true; }

  bool IsCancelled() const { return is_cancelled; }

  std::shared_ptr<RpcServer> server;
  std::shared_ptr<Session> session;
  std::shared_ptr<MethodInstance> method_instance;
  const RpcRequest &request;
  bool is_cancelled = false;
};

struct Method {
  Method(RpcServer *server, std::string name, std::string request_type,
         std::string response_type, int32_t slot_size, int32_t num_slots,
         std::function<absl::Status(const google::protobuf::Any &,
                                    google::protobuf::Any *,
                                    boost::asio::yield_context)>
             callback,
         int id)
      : name(std::move(name)), request_type(std::move(request_type)),
        response_type(std::move(response_type)), slot_size(slot_size),
        num_slots(num_slots), callback(std::move(callback)), id(id) {
    MakeChannelNames(server);
  }

  Method(
      RpcServer *server, std::string name, std::string request_type,
      std::string response_type, int32_t slot_size, int32_t num_slots,
      std::function<absl::Status(const google::protobuf::Any &,
                                 internal::AnyStreamWriter &,
                                 boost::asio::yield_context)>
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
  std::function<absl::Status(const google::protobuf::Any &,
                             google::protobuf::Any *,
                             boost::asio::yield_context)>
      callback;
  std::function<absl::Status(const google::protobuf::Any &,
                             internal::AnyStreamWriter &,
                             boost::asio::yield_context)>
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
};

struct Session {
  int session_id;
  uint64_t client_id;
  absl::flat_hash_map<int, std::shared_ptr<MethodInstance>> methods;
};

} // namespace internal

template <typename Response> struct StreamWriter {
  bool Write(const Response &res, boost::asio::yield_context yield) {
    auto any = std::make_unique<google::protobuf::Any>();
    any->PackFrom(res);
    return writer->Write(std::move(any), yield);
  }

  void Finish(boost::asio::yield_context yield) { writer->Finish(yield); }

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

  // Run the server.  If you pass an io_context this will use it
  // and the function will not block.  If you don't pass one
  // the server will use its own internal io_context and this function will
  // block until the server is stopped.
  absl::Status Run(boost::asio::io_context *ioc = nullptr);

  void Stop();

  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, Response *,
                                 boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

  template <typename Request>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const std::vector<char> &, std::vector<char> *,
                                 boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const absl::Span<const char> &,
                                 std::vector<char> *,
                                 boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, StreamWriter<Response> &,
                                 boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

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
      std::function<absl::Status(const google::protobuf::Any &,
                                 google::protobuf::Any *,
                                 boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method, std::string_view request_type,
      std::function<absl::Status(const google::protobuf::Any &,
                                 boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

  absl::Status RegisterMethod(
      const std::string &method, std::string_view request_type,
      std::string_view response_type,
      std::function<absl::Status(const google::protobuf::Any &,
                                 internal::AnyStreamWriter &,
                                 boost::asio::yield_context)>
          callback,
      MethodOptions &&options = {});

  boost::asio::io_context *IoContext() { return io_context_; }

private:
  friend struct internal::AnyStreamWriter;

  absl::Status CreateChannels();

  static void ListenerCoroutine(std::shared_ptr<RpcServer> server,
                                boost::asio::yield_context yield);

  absl::Status
  HandleIncomingRpcServerRequest(subspace::Message msg,
                                 boost::asio::yield_context yield);
  absl::Status HandleOpen(uint64_t client_id,
                          const subspace::RpcOpenRequest &request,
                          subspace::RpcOpenResponse *response,
                          boost::asio::yield_context yield);
  absl::Status HandleClose(uint64_t client_id,
                           const subspace::RpcCloseRequest &request,
                           subspace::RpcCloseResponse *response,
                           boost::asio::yield_context yield);
  absl::Status
  PublishRpcServerResponse(const subspace::RpcServerResponse &response,
                           boost::asio::yield_context yield);

  absl::StatusOr<std::shared_ptr<internal::Session>>
  CreateSession(uint64_t client_id, boost::asio::yield_context yield);
  absl::Status DestroySession(int session_id);

  static void SessionMethodCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      boost::asio::yield_context yield);

  static void SessionStreamingMethodCoroutine(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      boost::asio::yield_context yield);

  static void SendStreamRpcResponse(
      std::shared_ptr<RpcServer> server,
      std::shared_ptr<internal::Session> session,
      std::shared_ptr<internal::MethodInstance> method_instance,
      const RpcRequest &request, std::unique_ptr<google::protobuf::Any> result,
      bool is_last, bool is_cancelled, boost::asio::yield_context yield);

  static void
  SendRpcError(std::shared_ptr<RpcServer> server,
               std::shared_ptr<internal::Session> session,
               std::shared_ptr<internal::MethodInstance> method_instance,
               const RpcRequest &request, const std::string &error,
               boost::asio::yield_context yield);

  std::string name_;
  std::string subspace_server_socket_;
  boost::asio::io_context local_io_context_;
  boost::asio::io_context *io_context_;
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
    std::function<absl::Status(const Request &, Response *,
                               boost::asio::yield_context)>
        callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();
  auto response_descriptor = Response::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(), response_descriptor->full_name(),
      [method, callback = std::move(callback), request_descriptor](
          const google::protobuf::Any &req, google::protobuf::Any *res,
          boost::asio::yield_context yield) -> absl::Status {
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
        auto status = callback(request, &response, yield);
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
    std::function<absl::Status(const Request &, boost::asio::yield_context)>
        callback,
    MethodOptions &&options) {
  auto request_descriptor = Request::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(),
      [method, callback = std::move(callback),
       request_descriptor](const google::protobuf::Any &req,
                           boost::asio::yield_context yield) -> absl::Status {
        if (!req.Is<Request>()) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name(), req.type_url()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          return absl::InvalidArgumentError("Failed to unpack request");
        }
        return callback(request, yield);
      },
      std::move(options));
}

template <typename Request, typename Response>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method,
    std::function<absl::Status(const Request &, StreamWriter<Response> &,
                               boost::asio::yield_context)>
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
                     boost::asio::yield_context yield) mutable -> absl::Status {
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
        auto status = callback(request, typed_writer, yield);
        if (!status.ok()) {
          return status;
        }
        return absl::OkStatus();
      },
      std::move(options));
}
} // namespace subspace::asio_rpc
