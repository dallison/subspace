#pragma once
#include "absl/container/flat_hash_map.h"
#include "absl/types/span.h"
#include "client/client.h"
#include "coroutine.h"
#include "google/protobuf/any.pb.h"
#include "toolbelt/logging.h"
#include "toolbelt/pipe.h"

// This is only for testing.  The ServerTest class accesses the private
// RegisterMethod functions that we don't want exposed to the public API.
class ServerTest;

namespace subspace {

// Slot parameters for requests and responses for the open/close requests.
constexpr int32_t kRpcRequestSlotSize = 128;
constexpr int32_t kRpcResponseSlotSize = 128;
constexpr int32_t kRpcRequestNumSlots = 100;
constexpr int32_t kRpcResponseNumSlots = 100;

// Default slot parameters for method invocation requests.  The slot size
// can be expanded if the request is bigger.
constexpr int32_t kDefaultMethodSlotSize = 256;
constexpr int32_t kDefaultMethodNumSlots = 100;

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
  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, Response *, co::Coroutine *)>
          callback,
      int id = -1) {
    return RegisterMethod<Request, Response>(method, kDefaultMethodSlotSize,
                                             kDefaultMethodNumSlots,
                                             std::move(callback), id);
  }

  // Register a method that takes a request and produces a response.  You
  // specify the slot size and number of slots for the channels.
  template <typename Request, typename Response>
  absl::Status RegisterMethod(
      const std::string &method, int32_t slot_size, int32_t num_slots,
      std::function<absl::Status(const Request &, Response *, co::Coroutine *)>
          callback,
      int id = -1);

  // Typed void methods.
  template <typename Request>
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const Request &, co::Coroutine *)> callback,
      int id = -1) {
    return RegisterMethod<Request>(method, kDefaultMethodSlotSize,
                                   kDefaultMethodNumSlots, std::move(callback),
                                   id);
  }

  // Type void method with slot parameters.
  template <typename Request>
  absl::Status RegisterMethod(
      const std::string &method, int32_t slot_size, int32_t num_slots,
      std::function<absl::Status(const Request &, co::Coroutine *)> callback,
      int id = -1);

  // Method that takes a raw message and returns a raw message.
  // NOTE: this will make a copy of the request into a vector<char>.  For a more
  // efficient version that doesn't copy, use the version below that takes a
  // span.
  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const std::vector<char> &, std::vector<char> *,
                                 co::Coroutine *)>
          callback,
      int id = -1) {
    return RegisterMethod(method, kDefaultMethodNumSlots,
                          kDefaultMethodNumSlots, std::move(callback), id);
  }

  absl::Status RegisterMethod(
      const std::string &method, int slot_size, int num_slots,
      std::function<absl::Status(const std::vector<char> &, std::vector<char> *,
                                 co::Coroutine *)>
          callback,
      int id = -1);

  absl::Status RegisterMethod(
      const std::string &method,
      std::function<absl::Status(const absl::Span<const char> &,
                                 std::vector<char> *, co::Coroutine *)>
          callback,
      int id = -1) {
    return RegisterMethod(method, kDefaultMethodNumSlots,
                          kDefaultMethodNumSlots, std::move(callback), id);
  }

  absl::Status RegisterMethod(
      const std::string &method, int32_t slot_size, int32_t num_slots,
      std::function<absl::Status(const absl::Span<const char> &,
                                 std::vector<char> *, co::Coroutine *)>
          callback,
      int id = -1);

  // Server streaming methods.  These take in a single request and produce
  // multiple responses.  The responses are produced by calling a function and
  // providing a unique_ptr to the data to send.  This callback function takes
  // ownership of the data.  If the callback returns an error this will
  // terminate the stream and the error will be sent back to the client.
  template <typename Request, typename Response>
  absl::Status
  RegisterMethod(const std::string &method, int32_t slot_size,
                 int32_t num_slots,
                 std::function<absl::Status(
                     const Request &,
                     std::function<absl::Status(std::unique_ptr<Response>)>,
                     co::Coroutine *)>
                     callback,
                 int id = -1);

  absl::Status UnregisterMethod(const std::string &method) {
    auto it = methods_.find(method);
    if (it == methods_.end()) {
      return absl::NotFoundError("Method not found: " + method);
    }
    methods_.erase(it);
    return absl::OkStatus();
  }

  const std::string &Name() const { return name_; }

private:
  friend class ::ServerTest;

  struct Method {
    Method(RpcServer *server, std::string name, std::string request_type,
           std::string response_type, int32_t slot_size, int32_t num_slots,
           std::function<absl::Status(const google::protobuf::Any &,
                                      google::protobuf::Any *, co::Coroutine *)>
               callback,
           int id)
        : name(std::move(name)), request_type(std::move(request_type)),
          response_type(std::move(response_type)), slot_size(slot_size),
          num_slots(num_slots), callback(std::move(callback)), id(id) {
      MakeChannelNames(server);
    }
    // Streaming method constructor.
    Method(
        RpcServer *server, std::string name, std::string request_type,
        std::string response_type, int32_t slot_size, int32_t num_slots,
        std::function<absl::Status(
            const google::protobuf::Any &,
            std::function<absl::Status(std::unique_ptr<google::protobuf::Any>)>,
            co::Coroutine *)>
            callback,
        int id)
        : name(std::move(name)), request_type(std::move(request_type)),
          response_type(std::move(response_type)), slot_size(slot_size),
          num_slots(num_slots), stream_callback(std::move(callback)), id(id) {
      MakeChannelNames(server);
    }

    void MakeChannelNames(RpcServer *server) {
      request_channel =
          absl::StrFormat("/rpc/%s/%s/request", server->Name(), this->name);
      response_channel =
          absl::StrFormat("/rpc/%s/%s/response", server->Name(), this->name);
      if (IsStreaming()) {
        cancel_channel = absl::StrFormat("/rpc/%s/%s/cancel", server->Name(),
                                        this->name);
      }
    }

    bool IsStreaming() const { return stream_callback != nullptr; }

    std::string name;
    std::string request_type;
    std::string response_type;
    int32_t slot_size;
    int32_t num_slots;
    // Callback for normal, non-streaming method that is called and returns a single result.
    std::function<absl::Status(const google::protobuf::Any &,
                               google::protobuf::Any *, co::Coroutine *)>
        callback;
    // Callback for a streaming method that produces multiple responses.
    std::function<absl::Status(
        const google::protobuf::Any &,
        std::function<absl::Status(std::unique_ptr<google::protobuf::Any>)>,
        co::Coroutine *)>
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

  // These methods are non-templated and take google::protobuf::Any arguments
  // and responses.

  // Method with a request and response.
  absl::Status RegisterMethod(
      const std::string &method, const std::string &request_type,
      const std::string &response_type,
      std::function<absl::Status(const google::protobuf::Any &,
                                 google::protobuf::Any *, co::Coroutine *)>
          callback,
      int id = -1) {
    return RegisterMethod(method, request_type, response_type,
                          kDefaultMethodSlotSize, kDefaultMethodNumSlots,
                          std::move(callback), id);
  }

  absl::Status RegisterMethod(
      const std::string &method, const std::string &request_type,
      const std::string &response_type, int32_t slot_size, int32_t num_slots,
      std::function<absl::Status(const google::protobuf::Any &,
                                 google::protobuf::Any *, co::Coroutine *)>
          callback,
      int id = -1);

  absl::Status
  RegisterMethod(const std::string &method, const std::string &request_type,
                 std::function<absl::Status(const google::protobuf::Any &,
                                            co::Coroutine *)>
                     callback,
                 int id = -1) {
    return RegisterMethod(method, request_type, kDefaultMethodSlotSize,
                          kDefaultMethodNumSlots, std::move(callback), id);
  }

  absl::Status
  RegisterMethod(const std::string &method, const std::string &request_type,
                 int32_t slot_size, int32_t num_slots,
                 std::function<absl::Status(const google::protobuf::Any &,
                                            co::Coroutine *)>
                     callback,
                 int id = -1);

  // Streaming method.
  absl::Status RegisterMethod(
      const std::string &method, const std::string &request_type,
      const std::string &response_type, int32_t slot_size, int32_t num_slots,
      std::function<
          absl::Status(const google::protobuf::Any &,
                       std::function<absl::Status(std::unique_ptr<google::protobuf::Any>)>,
                       co::Coroutine *)>
          callback,
      int id = -1);

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

  absl::StatusOr<std::shared_ptr<Session>> CreateSession(uint64_t client_id);
  absl::Status DestroySession(int session_id);

  static void SessionMethodCoroutine(
      std::shared_ptr<RpcServer> server, std::shared_ptr<Session> session,
      std::shared_ptr<MethodInstance> method_instance, co::Coroutine *c);

  static void SessionStreamingMethodCoroutine(
      std::shared_ptr<RpcServer> server, std::shared_ptr<Session> session,
      std::shared_ptr<MethodInstance> method_instance, co::Coroutine *c);
  std::string name_;
  std::string subspace_server_socket_;
  co::CoroutineScheduler local_scheduler_;
  co::CoroutineScheduler *scheduler_;
  std::shared_ptr<Client> client_;
  absl::flat_hash_map<std::string, std::shared_ptr<Method>> methods_;
  toolbelt::Logger logger_;
  toolbelt::Pipe interrupt_pipe_;

  std::shared_ptr<subspace::Subscriber> request_receiver_;
  std::shared_ptr<subspace::Publisher> response_publisher_;
  absl::flat_hash_set<std::unique_ptr<co::Coroutine>> coroutines_;
  bool running_ = false;
  int32_t next_session_id_ = 0; // Next session ID.
  int next_method_id_ = 0;      // Next method ID.
  absl::flat_hash_map<int32_t, std::shared_ptr<Session>> sessions_;
};

template <typename Request, typename Response>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method, int32_t slot_size, int32_t num_slots,
    std::function<absl::Status(const Request &, Response *, co::Coroutine *)>
        callback,
    int id) {
  auto request_descriptor = Request::descriptor();
  auto response_descriptor = Response::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(), response_descriptor->full_name(),
      kDefaultMethodSlotSize, kDefaultMethodNumSlots,
      [method, callback = std::move(callback), request_descriptor](
          const google::protobuf::Any &req, google::protobuf::Any *res,
          co::Coroutine *c) -> absl::Status {
        if (!req.Is<Request>()) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name().c_str(), req.type_url().c_str()));
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
      id);
}

template <typename Request>
inline absl::Status RpcServer::RegisterMethod(
    const std::string &method, int32_t slot_size, int32_t num_slots,
    std::function<absl::Status(const Request &, co::Coroutine *)> callback,
    int id) {
  auto request_descriptor = Request::descriptor();

  return RegisterMethod(
      method, request_descriptor->full_name(), kDefaultMethodSlotSize,
      kDefaultMethodNumSlots,
      [method, callback = std::move(callback), request_descriptor](
          const google::protobuf::Any &req, co::Coroutine *c) -> absl::Status {
        if (!req.Is<Request>()) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "Invalid argment type for %s: need %s got %s", method,
              request_descriptor->full_name().c_str(), req.type_url().c_str()));
        }
        Request request;
        if (!req.UnpackTo(&request)) {
          return absl::InvalidArgumentError("Failed to unpack request");
        }
        return callback(request, c);
      },
      id);
}

} // namespace subspace