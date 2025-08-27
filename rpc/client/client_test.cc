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
  auto server = std::make_shared<subspace::RpcServer>("TestService",
                                                      ClientTest::Socket());
  server->SetLogLevel("info");

  auto s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "TestMethod",
      [](const auto &req, auto *res, co::Coroutine *) -> absl::Status {
        std::cerr << "TestMethod called with request: " << req.DebugString()
                  << std::endl;
        res->set_message("Hello from TestMethod");
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  // Performance
  s = server->RegisterMethod<rpc::PerfRequest, rpc::PerfResponse>(
      "PerfMethod",
      [](const auto &req, auto *res, co::Coroutine *) -> absl::Status {
        res->set_client_send_time(req.send_time());
        res->set_server_send_time(toolbelt::Now());
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  // Void method.
  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidMethod", [](const auto &req, auto *) -> absl::Status {
        std::cerr << "VoidMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::OkStatus();
      });

  // Error method.
  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "ErrorMethod",
      [](const auto &req, auto *res, co::Coroutine *) -> absl::Status {
        std::cerr << "ErrorMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  // void error method.
  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidErrorMethod", [](const auto &req, co::Coroutine *) -> absl::Status {
        std::cerr << "VoidErrorMethod called with request: "
                  << req.DebugString() << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "TimeoutMethod",
      [](const auto &req, auto *res, co::Coroutine *c) -> absl::Status {
        std::cerr << "TimeoutMethod called with request: " << req.DebugString()
                  << std::endl;
        c->Sleep(2);
        res->set_message("Hello from TimeoutMethod");
        std::cerr << "TimeoutMethod completed" << std::endl;
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  // Stream method.
  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "StreamMethod",
      [](const auto &req, subspace::StreamWriter<rpc::TestResponse> &writer,
         co::Coroutine *c) -> absl::Status {
        std::cerr << "StreamMethod called with request: " << req.DebugString()
                  << std::endl;

        constexpr int kNumResults = 200;
        for (int i = 0; i < kNumResults; i++) {
          if (writer.IsCancelled()) {
            std::cerr << "StreamMethod cancelled after " << i << " responses"
                      << std::endl;
            writer.Finish(c);
            return absl::OkStatus();
          }
          rpc::TestResponse r;
          r.set_message("Hello from StreamMethod part " + std::to_string(i));
          writer.Write(r, c);
          c->Millisleep(req.stream_period());
        }

        writer.Finish(c);
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  return server;
}

TEST_F(ClientTest, Typed) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 1,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    absl::StatusOr<rpc::TestResponse> resp =
        client->Call<rpc::TestRequest, rpc::TestResponse>("TestMethod",
                                                          test_request, c);
    std::cerr << resp.status().ToString() << std::endl;
    ASSERT_OK(resp);

    EXPECT_EQ(resp->message(), "Hello from TestMethod");
    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, TypedMethodId) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 2,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    auto TestMethodId = client->FindMethod("TestMethod");
    ASSERT_OK(TestMethodId);

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    absl::StatusOr<rpc::TestResponse> resp =
        client->Call<rpc::TestRequest, rpc::TestResponse>(*TestMethodId,
                                                          test_request, c);
    ASSERT_OK(resp);

    EXPECT_EQ(resp->message(), "Hello from TestMethod");
    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, TypedVoid) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 3,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    absl::Status resp =
        client->Call<rpc::TestRequest>("VoidMethod", test_request, c);
    ASSERT_OK(resp);

    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, TypedError) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 4,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    absl::StatusOr<rpc::TestResponse> resp =
        client->Call<rpc::TestRequest, rpc::TestResponse>("ErrorMethod",
                                                          test_request, c);
    ASSERT_FALSE(resp.ok());
    std::cerr << "Received error: " << resp.status().ToString() << std::endl;
    ASSERT_EQ("INTERNAL: Failed to invoke method: INTERNAL: Error executing "
              "method ErrorMethod: INTERNAL: Error occurred",
              resp.status().ToString());

    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, TypedTimeout) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 5,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    // Invoke with 1 second timeout.  The method will take 2 seconds to respond.
    absl::StatusOr<rpc::TestResponse> resp =
        client->Call<rpc::TestRequest, rpc::TestResponse>("TimeoutMethod",
                                                          test_request, 1s, c);
    ASSERT_FALSE(resp.ok());
    std::cerr << "Received error: " << resp.status().ToString() << std::endl;
    ASSERT_EQ("INTERNAL: Failed to invoke method: INTERNAL: Error waiting for "
              "response: INTERNAL: Timeout waiting for subscriber",
              resp.status().ToString());
    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, Performance) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 6,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    auto PerfMethodId = client->FindMethod("PerfMethod");
    ASSERT_OK(PerfMethodId);

    uint64_t total_rtt = 0;
    uint64_t total_server_time = 0;

    int num_iterations = 10000;
    for (int i = 0; i < num_iterations; i++) {
      rpc::PerfRequest request;
      request.set_send_time(toolbelt::Now());

      absl::StatusOr<rpc::PerfResponse> resp =
          client->Call<rpc::PerfRequest, rpc::PerfResponse>(*PerfMethodId,
                                                            request, c);
      ASSERT_OK(resp);

      uint64_t rtt = toolbelt::Now() - request.send_time();
      int64_t server_time = resp->server_send_time() - resp->client_send_time();
      total_rtt += rtt;
      total_server_time += server_time;
    }
    std::cerr << "RTT: " << (total_rtt / num_iterations)
              << " ns, server time: " << (total_server_time / num_iterations)
              << " ns" << std::endl;

    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, TypedStream) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 7,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");

    class MyResponseReceiver
        : public subspace::ResponseReceiver<rpc::TestResponse> {
    public:
      void OnResponse(const rpc::TestResponse &response
       ) override {
        std::cerr << "Received response: " << response.message() << std::endl;
        count++;
      }

      void OnError(const absl::Status &status) override {
        std::cerr << "Received error: " << status.ToString() << std::endl;
      }

      void OnCancel() override { std::cerr << "Stream cancelled" << std::endl; }

      void OnFinish() override { std::cerr << "Stream finished" << std::endl; }

      int GetCount() const { return count; }
      int count = 0;
    };

    MyResponseReceiver receiver;
    absl::Status status = client->Call<rpc::TestRequest, rpc::TestResponse>(
        "StreamMethod", test_request, receiver, c);
    std::cerr << status.ToString() << std::endl;
    ASSERT_OK(status);

    ASSERT_EQ(200, receiver.GetCount());
    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ClientTest, CancelStream) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto client = std::make_shared<subspace::RpcClient>("TestService", 7,
                                                        ClientTest::Socket());
    ASSERT_OK(client->Open(c));

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");
    test_request.set_stream_period(100);

    class MyResponseReceiver
        : public subspace::ResponseReceiver<rpc::TestResponse> {
    public:
      void OnResponse(const rpc::TestResponse &response) override {
        std::cerr << "Received response: " << response.message() << std::endl;
        count++;
        if (count == 5) {
          std::cerr << "Cancelling stream after 5 responses" << std::endl;
          if (auto status = Cancel(); !status.ok()) {
            std::cerr << "Error cancelling stream: " << status.ToString()
                      << std::endl;
          }
        }
      }

      void OnError(const absl::Status &status) override {
        std::cerr << "Received error: " << status.ToString() << std::endl;
      }

      void OnCancel() override { std::cerr << "Stream cancelled" << std::endl; }

      void OnFinish() override { std::cerr << "Stream finished" << std::endl; }

      int GetCount() const { return count; }

      int count = 0;
    };

    MyResponseReceiver receiver;
    absl::Status status = client->Call<rpc::TestRequest, rpc::TestResponse>(
        "StreamMethod", test_request, receiver, c);
    std::cerr << status.ToString() << std::endl;
    ASSERT_OK(status);

    // Depending on timing we may get a one more reponse after the cancel.
    ASSERT_LE(receiver.GetCount(), 6);

    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
