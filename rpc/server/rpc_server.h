#pragma once
#include "absl/container/flat_hash_map.h"
#include "client/client.h"
#include "coroutine.h"
#include "google/protobuf/any.pb.h"

namespace subspace {

constexpr int32_t kRpcRequestSlotSize = 16 * 1024;
constexpr int32_t kRpcResponseSlotSize = 16 * 1024;
constexpr int32_t kRpcRequestNumSlots = 16;
constexpr int32_t kRpcResponseNumSlots = 16;

class RpcServer : public std::enable_shared_from_this<RpcServer> {
public:
  RpcServer(std::string name, std::string subspace_server_socket = "/tmp/subspace");

  absl::Status Run();

  void Stop();

  absl::Status
  RegisterMethod(const std::string &method, const std::string &request_type,
                 const std::string &response_type, int32_t slot_size,
                 int32_t num_slots,
                 std::function<absl::Status(const google::protobuf::Any &,
                                            google::protobuf::Any *)>
                     callback) {
    if (methods_.find(method) != methods_.end()) {
      return absl::AlreadyExistsError("Method already registered: " + method);
    }
    methods_[method] =
        std::make_shared<Method>(this, method, request_type, response_type,
                                 slot_size, num_slots, std::move(callback));
    return absl::OkStatus();
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
    int id;
    absl::flat_hash_map<std::string, std::shared_ptr<MethodInstance>> methods;
  };

  absl::Status CreateChannels();
  void AddCoroutine(std::unique_ptr<co::Coroutine> coroutine) {
    coroutines_.insert(std::move(coroutine));
  }
  void ListenerCoroutine(co::Coroutine *c);

  absl::Status HandleIncomingRpcServerRequest(subspace::Message msg,
                                              co::Coroutine *c);
  absl::Status HandleOpen(const subspace::RpcOpenRequest &request,
                          subspace::RpcOpenResponse *response,
                          co::Coroutine *c);
  absl::Status HandleClose(const subspace::RpcCloseRequest &request,
                           subspace::RpcCloseResponse *response,
                           co::Coroutine *c);
  absl::Status
  PublishRpcServerResponse(const subspace::RpcServerResponse &response,
                           co::Coroutine *c);

  absl::StatusOr<std::shared_ptr<Session>> CreateSession();
  absl::Status DestroySession(int session_id);

  void SessionMethodCoroutine(std::shared_ptr<RpcServer> server,
                              std::shared_ptr<Session> session,
                              std::shared_ptr<MethodInstance> method,
                              co::Coroutine *c);

  std::string name_;
  std::string subspace_server_socket_;
  co::CoroutineScheduler scheduler_;
  std::shared_ptr<Client> client_;
  absl::flat_hash_map<std::string, std::shared_ptr<Method>> methods_;

  std::shared_ptr<subspace::Subscriber> request_receiver_;
  std::shared_ptr<subspace::Publisher> response_publisher_;
  absl::flat_hash_set<std::unique_ptr<co::Coroutine>> coroutines_;
  bool running_ = false;
  int32_t session_id_ = 0; // Next session ID.
  absl::flat_hash_map<int32_t, std::shared_ptr<Session>> sessions_;
};

} // namespace subspace