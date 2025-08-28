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
#include "rpc/proto/rpc_test.subspace.rpc_client.h"
#include "rpc/proto/rpc_test.subspace.rpc_server.h"
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

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");

void SignalHandler(int sig) { printf("Signal %d", sig); }

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;
using InetAddress = toolbelt::InetAddress;

using namespace std::chrono_literals;

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

class RpcTest : public ::testing::Test {
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

co::CoroutineScheduler RpcTest::scheduler_;
std::string RpcTest::socket_ = "/tmp/subspace";
int RpcTest::server_pipe_[2];
std::unique_ptr<subspace::Server> RpcTest::server_;
std::thread RpcTest::server_thread_;


class MyServer : public rpc::TestServiceServer {
public:
  MyServer(const std::string &socket) : rpc::TestServiceServer(socket) {}

  absl::StatusOr<rpc::TestResponse> TestMethod(const rpc::TestRequest &request,
                                               co::Coroutine *c) override {
    std::cerr << "TestMethod called with request: " << request.DebugString()
              << std::endl;
    rpc::TestResponse response;
    response.set_message("Hello from TestMethod");
    return response;
  }

  absl::StatusOr<rpc::PerfResponse> PerfMethod(const rpc::PerfRequest &request,
                                               co::Coroutine *c) override {
    rpc::PerfResponse res;
    res.set_client_send_time(request.send_time());
    res.set_server_send_time(toolbelt::Now());
    return res;
  }

  absl::Status StreamMethod(const rpc::TestRequest &request,
                            subspace::StreamWriter<rpc::TestResponse> &writer,
                            co::Coroutine *c) override {
    std::cerr << "StreamMethod called with request: " << request.DebugString()
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
      c->Millisleep(request.stream_period());
    }

    writer.Finish(c);
    return absl::OkStatus();
  }

  absl::StatusOr<rpc::TestResponse> ErrorMethod(const rpc::TestRequest &request,
                                                co::Coroutine *c) override {
    return absl::InternalError("Error occurred");
  }

  absl::StatusOr<rpc::TestResponse>
  TimeoutMethod(const rpc::TestRequest &request, co::Coroutine *c) override {
    c->Sleep(2);
    rpc::TestResponse response;
    response.set_message("Hello from TestMethod");
    return response;
  }
};

static MyServer* server_instance = nullptr;
static void SigQuit(int sig) {
  if (server_instance != nullptr) {
    std::cerr << "Signal " << sig << " received, dumping coroutines"
              << std::endl;
    server_instance->DumpCoroutines();
  }
  signal(SIGQUIT, SIG_DFL);
  raise(SIGQUIT);
}

static std::shared_ptr<MyServer> BuildServer(const std::string& log_level="info") {
  auto server = std::make_shared<MyServer>(RpcTest::Socket());
  server->SetLogLevel(log_level);
  auto s = server->RegisterMethods();
  EXPECT_TRUE(s.ok());
  server_instance = server.get();
  signal(SIGQUIT, SigQuit);
  return server;
}

TEST_F(RpcTest, Basic) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto cl = rpc::TestServiceClient::Create(getpid(), RpcTest::Socket(), c);
    ASSERT_OK(cl);
    auto client = *cl;
    client->SetLogLevel("info");

    rpc::TestRequest req;
    req.set_message("this is a test");
    absl::StatusOr<rpc::TestResponse> r = client->TestMethod(req, c);
    std::cerr << r.status().ToString() << std::endl;
    ASSERT_OK(r);

    EXPECT_EQ(r->message(), "Hello from TestMethod");
    ASSERT_OK(client->Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(RpcTest, Stream) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    rpc::TestServiceClient client(getpid(), RpcTest::Socket());
    client.SetLogLevel("info");

    ASSERT_OK(client.Open(c));

    class MyResponseReceiver
        : public subspace::ResponseReceiver<rpc::TestResponse> {
    public:
      void OnResponse(const rpc::TestResponse &response) override {
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
    rpc::TestRequest req;
    req.set_message("this is a test");
    absl::Status status = client.StreamMethod(req, receiver, c);
    absl::StatusOr<rpc::TestResponse> r = client.TestMethod(req, c);
    std::cerr << r.status().ToString() << std::endl;
    ASSERT_OK(r);

    ASSERT_EQ(200, receiver.GetCount());
    ASSERT_OK(client.Close(c));
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(RpcTest, Error) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto cl = rpc::TestServiceClient::Create(getpid(), RpcTest::Socket(), c);
    ASSERT_OK(cl);
    auto client = *cl;
    client->SetLogLevel("info");

    rpc::TestRequest req;
    req.set_message("this is a test");
    absl::StatusOr<rpc::TestResponse> r = client->ErrorMethod(req, c);
    std::cerr << r.status().ToString() << std::endl;
    ASSERT_FALSE(r.ok());
    EXPECT_TRUE(
        absl::StrContains(r.status().ToString(), "INTERNAL: Error occurred"));
    ASSERT_OK(client->Close(2s, c));
    std::cerr << "Stopping server\n";
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(RpcTest, Timeout) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    auto cl = rpc::TestServiceClient::Create(getpid(), RpcTest::Socket(), c);
    ASSERT_OK(cl);
    auto client = *cl;
    client->SetLogLevel("info");

    rpc::TestRequest req;
    req.set_message("this is a test");
    absl::StatusOr<rpc::TestResponse> r = client->TimeoutMethod(req, 1s, c);
    std::cerr << r.status().ToString() << std::endl;
    ASSERT_FALSE(r.ok());
    EXPECT_TRUE(
        absl::StrContains(r.status().ToString(), "INTERNAL: Error waiting for response: INTERNAL: Timeout"));
    ASSERT_OK(client->Close(2s, c));
    server->Stop();
  });

  scheduler.Run();
}


int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
