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
#include "rpc/client/rpc_client.h"
#include "rpc/proto/rpc_test.pb.h"
#include "rpc/server/rpc_server.h"
#include "server/server.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <chrono>
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/resource.h>
#include <thread>

using namespace std::chrono_literals;
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

class ClientTest : public ::testing::Test {
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

co::CoroutineScheduler ClientTest::scheduler_;
std::string ClientTest::socket_ = "/tmp/subspace";
int ClientTest::server_pipe_[2];
std::unique_ptr<subspace::Server> ClientTest::server_;
std::thread ClientTest::server_thread_;

static std::shared_ptr<subspace::RpcServer> BuildServer() {
  auto server =
      std::make_shared<subspace::RpcServer>("test", ClientTest::Socket());

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

  // Void method.
  s = server->RegisterMethod(
      "VoidMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req) -> absl::Status {
        std::cerr << "VoidMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::OkStatus();
      });

  // Error method.
  s = server->RegisterMethod("ErrorMethod", "subspace.TestRequest",
                             "subspace.TestResponse",
                             [](const google::protobuf::Any &req,
                                google::protobuf::Any *res) -> absl::Status {
                               std::cerr << "ErrorMethod called with request: "
                                         << req.DebugString() << std::endl;
                               return absl::InternalError("Error occurred");
                             });
  EXPECT_TRUE(s.ok());

  // void error method.
  s = server->RegisterMethod(
      "VoidErrorMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req) -> absl::Status {
        std::cerr << "VoidErrorMethod called with request: "
                  << req.DebugString() << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());
  return server;
}

TEST_F(ClientTest, Basic) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    subspace::RpcClient client("test", 1, ClientTest::Socket());
    ASSERT_OK(client.Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    google::protobuf::Any any;
    any.PackFrom(test_request);

    absl::StatusOr<google::protobuf::Any> resp =
        client.InvokeMethod("TestMethod", any, c);
    ASSERT_OK(resp);

    rpc::TestResponse response;
    (*resp).UnpackTo(&response);
    EXPECT_EQ(response.message(), "Hello from TestMethod");
    ASSERT_OK(client.Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, BasicTimeout) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    subspace::RpcClient client("test", 2, ClientTest::Socket());
    client.SetLogLevel("debug");
    ASSERT_OK(client.Open(10s, c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    google::protobuf::Any any;
    any.PackFrom(test_request);

    absl::StatusOr<google::protobuf::Any> resp =
        client.InvokeMethod("TestMethod", any, c);
    ASSERT_OK(resp);

    rpc::TestResponse response;
    (*resp).UnpackTo(&response);
    EXPECT_EQ(response.message(), "Hello from TestMethod");
    ASSERT_OK(client.Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, Typed) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    subspace::RpcClient client("test", 3, ClientTest::Socket());
    ASSERT_OK(client.Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    absl::StatusOr<rpc::TestResponse> resp =
        client.Call<rpc::TestRequest, rpc::TestResponse>("TestMethod",
                                                         test_request, c);
    ASSERT_OK(resp);

    EXPECT_EQ(resp->message(), "Hello from TestMethod");
    ASSERT_OK(client.Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, TypedVoid) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    subspace::RpcClient client("test", 3, ClientTest::Socket());
    ASSERT_OK(client.Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    absl::Status resp =
        client.Call<rpc::TestRequest>("VoidMethod", test_request, c);
    ASSERT_OK(resp);

    ASSERT_OK(client.Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, TypedError) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    subspace::RpcClient client("test", 3, ClientTest::Socket());
    client.SetLogLevel("debug");
    ASSERT_OK(client.Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    absl::StatusOr<rpc::TestResponse> resp =
        client.Call<rpc::TestRequest, rpc::TestResponse>("ErrorMethod",
                                                         test_request, c);
    ASSERT_FALSE(resp.ok());
    std::cerr << "Received error: " << resp.status().ToString() << std::endl;
    ASSERT_EQ("INTERNAL: Failed to invoke method: INTERNAL: Error executing "
              "method ErrorMethod: INTERNAL: Error occurred",
              resp.status().ToString());

    ASSERT_OK(client.Close(c));
    server->Stop();
  });

  scheduler.Run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
