// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status_matchers.h"
#include "client/client.h"
#include "coroutine.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include "rpc/server/rpc_server.h"
#include "server/server.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/resource.h>
#include <thread>

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");

void SignalHandler(int sig) { printf("Signal %d", sig); }

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;
using InetAddress = toolbelt::InetAddress;

#define VAR(a) a##__COUNTER__
#define EVAL_AND_ASSERT_OK(expr) EVAL_AND_ASSERT_OK2(VAR(r_), expr)

#define EVAL_AND_ASSERT_OK2(result, expr)                                      \
  ({                                                                           \
    auto result = (expr);                                                      \
    if (!result.ok()) {                                                        \
      std::cerr << result.status() << std::endl;                               \
    }                                                                          \
    ASSERT_OK(result);                                                         \
    std::move(*result);                                                        \
  })

#define ASSERT_OK(e) ASSERT_THAT(e, ::absl_testing::IsOk())

class ServerTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    printf("Starting Subspace server\n");
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    // The server will write to this pipe to notify us when it
    // has started and stopped.  This end of the pipe is blocking.
    (void)pipe(server_pipe_);

    server_ =
        std::make_unique<subspace::Server>(scheduler_, socket_, "", 0, 0,
                                           /*local=*/true, server_pipe_[1]);

    // Start server running in a thread.
    server_thread_ = std::thread([]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });

    // Wait for server to tell us that it's running.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  static void TearDownTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    printf("Stopping Subspace server\n");
    server_->Stop();

    // Wait for server to tell us that it's stopped.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
  void TearDown() override {}

  static void InitClient(subspace::Client &client) {
    ASSERT_OK(client.Init(Socket()));
  }

  static const std::string &Socket() { return socket_; }

  static subspace::Server *Server() { return server_.get(); }

private:
  static co::CoroutineScheduler scheduler_;
  static std::string socket_;
  static int server_pipe_[2];
  static std::unique_ptr<subspace::Server> server_;
  static std::thread server_thread_;
};

co::CoroutineScheduler ServerTest::scheduler_;
std::string ServerTest::socket_ = "/tmp/subspace";
int ServerTest::server_pipe_[2];
std::unique_ptr<subspace::Server> ServerTest::server_;
std::thread ServerTest::server_thread_;

static int interrupt_pipe[2];

static std::shared_ptr<subspace::RpcServer> BuildServer() {
  auto server =
      std::make_shared<subspace::RpcServer>("test", ServerTest::Socket());

  auto s = server->RegisterMethod(
      "TestMethod", "subspace.TestRequest", "subspace.TestResponse", 256, 10,
      [](const google::protobuf::Any &req,
         google::protobuf::Any *res) -> absl::Status {
        std::cerr << "TestMethod called with request: " << req.DebugString()
                  << std::endl;
        rpc::TestResponse r;
        r.set_message("Hello from TestMethod");
        res->PackFrom(r);
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());
  return server;
}

static void Open(std::shared_ptr<subspace::RpcServer> server,
                 subspace::RpcServerResponse &response) {
  subspace::RpcServerRequest req;
  req.mutable_open();

  subspace::Client client;
  ServerTest::InitClient(client);

  auto pub = client.CreatePublisher(
      "/rpc/test/request", 1024, 10,
      {.reliable = true, .type = "subspace.RpcServerRequest"});
  ASSERT_OK(pub);
  auto sub = client.CreateSubscriber(
      "/rpc/test/response",
      {.reliable = true, .type = "subspace.RpcServerResponse"});
  ASSERT_OK(sub);

  auto buffer = pub->GetMessageBuffer(256);
  ASSERT_OK(buffer);
  ASSERT_TRUE(req.SerializeToArray(*buffer, 1024));
  ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

  subspace::Message msg;
  for (;;) {
    ASSERT_OK(sub->Wait());
    auto m = sub->ReadMessage();
    ASSERT_OK(m);
    if (m->length > 0) {
      msg = std::move(*m);
      break;
    }
  }

  ASSERT_TRUE(response.ParseFromArray(msg.buffer, msg.length));
}

static const subspace::RpcOpenResponse::Method *
FindMethod(const subspace::RpcOpenResponse &response, const std::string &name) {
  for (auto &method : response.methods()) {
    if (method.name() == name) {
      return &method;
    }
  }
  return nullptr;
}

TEST_F(ServerTest, Open) {
  auto server = BuildServer();
  std::thread server_thread([&]() {
    absl::Status status = server->Run();
    if (!status.ok()) {
      fprintf(stderr, "Error running RPC server: %s\n",
              status.ToString().c_str());
      exit(1);
    }
  });

  server_thread.detach();

  subspace::RpcServerResponse response;
  Open(server, response);
  std::cerr << "Received response: " << response.DebugString() << std::endl;

  std::cerr << "stopping server\n";
  server->Stop();
}

TEST_F(ServerTest, OpenAndCallNoResponseRead) {
  auto server = BuildServer();
  std::thread server_thread([&]() {
    absl::Status status = server->Run();
    if (!status.ok()) {
      fprintf(stderr, "Error running RPC server: %s\n",
              status.ToString().c_str());
      exit(1);
    }
  });

  server_thread.detach();

  subspace::RpcServerResponse response;
  Open(server, response);
  std::cerr << "Received response: " << response.DebugString() << std::endl;

  subspace::RpcRequest req;
  req.set_session_id(response.open().session_id());
  req.set_request_id(1234);
  req.set_method("TestMethod");
  auto *args = req.mutable_arguments();

  rpc::TestRequest test_request;
  test_request.set_message("Hello, world!");
  args->PackFrom(test_request);

  subspace::Client client;
  InitClient(client);

  // Find the TestRequest method in the response.
  auto *method = FindMethod(response.open(), "TestMethod");
  ASSERT_NE(method, nullptr);

  auto pub = client.CreatePublisher(
      method->request_channel().name(), method->request_channel().slot_size(),
      method->request_channel().num_slots(),
      {.reliable = true, .type = method->request_channel().type()});
  ASSERT_OK(pub);

  std::cerr << "publishing " << req.DebugString();
  std::cerr << "request channel: " << method->request_channel().name() << std::endl;
  // This is a reliable publisher but we are guaranteed to be able to send.
  auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
  ASSERT_OK(buffer);
  ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
  ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

  sleep(1);

  std::cerr << "stopping server\n";
  server->Stop();
}

TEST_F(ServerTest, OpenAndCall) {
  auto server = BuildServer();
  std::thread server_thread([&]() {
    absl::Status status = server->Run();
    if (!status.ok()) {
      fprintf(stderr, "Error running RPC server: %s\n",
              status.ToString().c_str());
      exit(1);
    }
  });

  server_thread.detach();

  subspace::RpcServerResponse response;
  Open(server, response);
  std::cerr << "Received response: " << response.DebugString() << std::endl;

  subspace::RpcRequest req;
  req.set_session_id(response.open().session_id());
  req.set_request_id(1234);
  req.set_method("TestMethod");
  auto *args = req.mutable_arguments();

  rpc::TestRequest test_request;
  test_request.set_message("Hello, world!");
  args->PackFrom(test_request);

  subspace::Client client;
  InitClient(client);

  // Find the TestRequest method in the response.
  auto *method = FindMethod(response.open(), "TestMethod");
  ASSERT_NE(method, nullptr);

  auto pub = client.CreatePublisher(
      method->request_channel().name(), method->request_channel().slot_size(),
      method->request_channel().num_slots(),
      {.reliable = true, .type = method->request_channel().type()});
  ASSERT_OK(pub);

  auto sub = client.CreateSubscriber(
      method->response_channel().name(),
      {.reliable = true, .type = method->response_channel().type()});
  ASSERT_OK(sub);

  std::cerr << "publishing " << req.DebugString();
  // This is a reliable publisher but we are guaranteed to be able to send.
  auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
  ASSERT_OK(buffer);
  ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
  ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

  subspace::Message msg;
  for (;;) {
    ASSERT_OK(sub->Wait());
    auto m = sub->ReadMessage();
    ASSERT_OK(m);
    if (m->length > 0) {
      msg = std::move(*m);
      break;
    }
  }
  subspace::RpcResponse rpc_response;
  rpc_response.ParseFromArray(msg.buffer, msg.length);
  std::cerr << "Received RPC response: " << rpc_response.DebugString() << std::endl;

  rpc::TestResponse response_message;
  rpc_response.result().UnpackTo(&response_message);
  std::cerr << "Received RPC result: " << response_message.DebugString() << std::endl;

  std::cerr << "stopping server\n";
  server->Stop();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
