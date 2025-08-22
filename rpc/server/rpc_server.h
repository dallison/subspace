#pragma once
#include "absl/container/flat_hash_map.h"
#include "client/client.h"
#include "coroutine.h"
#include "google/protobuf/any.pb.h"
#include "toolbelt/logging.h"
#include "toolbelt/pipe.h"

namespace subspace {

constexpr int32_t kRpcRequestSlotSize = 16 * 1024;
constexpr int32_t kRpcResponseSlotSize = 16 * 1024;
constexpr int32_t kRpcRequestNumSlots = 16;
constexpr int32_t kRpcResponseNumSlots = 16;

constexpr int32_t kDefaultMethodSlotSize = 16 * 1024;
constexpr int32_t kDefaultMethodNumSlots = 64;

class RpcServer : public std::enable_shared_from_this<RpcServer> {
public:
  RpcServer(std::string name,
            std::string subspace_server_socket = "/tmp/subspace");

  ~RpcServer() {
    std::cerr << "destructing RpcServer: " << name_ << std::endl;
  }

  void SetLogLevel(const std::string &level) { logger_.SetLogLevel(level); }

  void SetStartingSessionId(int session_id) {
    session_id_ = session_id;
  }

  absl::Status Run(co::CoroutineScheduler *scheduler = nullptr);

  void Stop();

  // Method with a request and response.
  absl::Status
  RegisterMethod(const std::string &method, const std::string &request_type,
                 const std::string &response_type,
                 std::function<absl::Status(const google::protobuf::Any &,
                                            google::protobuf::Any *)>
                     callback) {
    return RegisterMethod(method, request_type, response_type,
                          kDefaultMethodSlotSize, kDefaultMethodNumSlots,
                          std::move(callback));
  }

  absl::Status
  RegisterMethod(const std::string &method, const std::string &request_type,
                 const std::string &response_type, int32_t slot_size,
                 int32_t num_slots,
                 std::function<absl::Status(const google::protobuf::Any &,
                                            google::protobuf::Any *)>
                     callback);

                       absl::Status
  RegisterMethod(const std::string &method, const std::string &request_type,
                 absl::Status (*callback)(const google::protobuf::Any &)) {
    return RegisterMethod(method, request_type, kDefaultMethodSlotSize,
                          kDefaultMethodNumSlots, callback);
  }

  absl::Status
  RegisterMethod(const std::string &method, const std::string &request_type,
                 int32_t slot_size, int32_t num_slots,
                 absl::Status (*callback)(const google::protobuf::Any &));


  template <typename Request, typename Response>
  absl::Status RegisterMethod(const std::string &method, const Request &request, Response* response) {
    auto request_descriptor = Request::descriptor();
    auto response_descriptor = Response::descriptor();

    return RegisterMethod(method, request_descriptor->full_name(), response_descriptor->full_name(),
                          kDefaultMethodSlotSize, kDefaultMethodNumSlots,
                          [request, response](const google::protobuf::Any &req,
                                              google::protobuf::Any *res) {
                            if (!req.UnpackTo(&request)) {
                              return absl::InvalidArgumentError("Failed to unpack request");
                            }
                            auto status = callback(request, response);
                            if (!status.ok()) {
                              return status;
                            }
                            res->PackFrom(*response);
                            return absl::OkStatus();
                          });
  }

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
  struct Method {
    Method(RpcServer *server, std::string name, std::string request_type,
           std::string response_type, int32_t slot_size, int32_t num_slots,
           std::function<absl::Status(const google::protobuf::Any &,
                                      google::protobuf::Any *)>
               callback)
        : name(std::move(name)), request_type(std::move(request_type)),
          response_type(std::move(response_type)), slot_size(slot_size),
          num_slots(num_slots), callback(std::move(callback)) {
      request_channel =
          absl::StrFormat("/rpc/%s/%s/request", server->Name(), this->name);
      response_channel =
          absl::StrFormat("/rpc/%s/%s/response", server->Name(), this->name);
    }
    std::string name;
    std::string request_type;
    std::string response_type;
    int32_t slot_size;
    int32_t num_slots;
    std::function<absl::Status(const google::protobuf::Any &,
                               google::protobuf::Any *)>
        callback;
    std::string request_channel;
    std::string response_channel;
  };

  struct MethodInstance {
    std::shared_ptr<Method> method;
    std::shared_ptr<subspace::Subscriber> request_subscriber;
    std::shared_ptr<subspace::Publisher> response_publisher;
  };

  struct Session {
    int session_id;
    uint64_t client_id;
    absl::flat_hash_map<std::string, std::shared_ptr<MethodInstance>> methods;
  };

  absl::Status CreateChannels();
  void AddCoroutine(std::unique_ptr<co::Coroutine> coroutine) {
    coroutines_.insert(std::move(coroutine));
  }
  static void ListenerCoroutine(std::shared_ptr<RpcServer> server, co::Coroutine *c);

  absl::Status HandleIncomingRpcServerRequest(subspace::Message msg,
                                              co::Coroutine *c);
  absl::Status HandleOpen(uint64_t client_id, const subspace::RpcOpenRequest &request,
                          subspace::RpcOpenResponse *response,
                          co::Coroutine *c);
  absl::Status HandleClose(uint64_t client_id, const subspace::RpcCloseRequest &request,
                           subspace::RpcCloseResponse *response,
                           co::Coroutine *c);
  absl::Status
  PublishRpcServerResponse(const subspace::RpcServerResponse &response,
                           co::Coroutine *c);

  absl::StatusOr<std::shared_ptr<Session>> CreateSession(uint64_t client_id);
  absl::Status DestroySession(int session_id);

  static void SessionMethodCoroutine(std::shared_ptr<RpcServer> server,
                              std::shared_ptr<Session> session,
                              std::shared_ptr<MethodInstance> method,
                              co::Coroutine *c);

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
  int32_t session_id_ = 0; // Next session ID.
  absl::flat_hash_map<int32_t, std::shared_ptr<Session>> sessions_;
};

} // namespace subspace