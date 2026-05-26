// Copyright 2023-2026 David Allison
// co20 RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "absl/strings/match.h"
#include "client/test_fixture.h"
#include "co/coroutine_cpp20.h"
#include "co20_rpc/proto/rpc_test_co20_rpc/rpc/proto/rpc_test.subspace.rpc_client.h"
#include "co20_rpc/proto/rpc_test_co20_rpc/rpc/proto/rpc_test.subspace.rpc_server.h"
#include "rpc/proto/rpc_test.pb.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

using namespace std::chrono_literals;

// ASSERT macros use 'return' which is illegal inside coroutines.
// These CO_ variants use co_return instead.
#define CO_ASSERT_OK(e)                                                        \
  do {                                                                         \
    const auto &_co_s = (e);                                                   \
    EXPECT_TRUE(_co_s.ok()) << "Expected OK: " #e;                            \
    if (!_co_s.ok())                                                           \
      co_return;                                                               \
  } while (0)

#define CO_ASSERT_TRUE(cond)                                                   \
  do {                                                                         \
    EXPECT_TRUE(cond);                                                         \
    if (!(cond))                                                               \
      co_return;                                                               \
  } while (0)

#define CO_ASSERT_FALSE(cond)                                                  \
  do {                                                                         \
    EXPECT_FALSE(cond);                                                        \
    if ((cond))                                                                \
      co_return;                                                               \
  } while (0)

#define CO_ASSERT_EQ(a, b)                                                     \
  do {                                                                         \
    EXPECT_EQ(a, b);                                                           \
    if ((a) != (b))                                                            \
      co_return;                                                               \
  } while (0)

class Co20RpcTest : public SubspaceTestBase {};

class MyServer : public rpc::TestServiceServer {
public:
  MyServer(const std::string &socket) : rpc::TestServiceServer(socket) {}

  co20::ValueTask<absl::Status>
  TestMethod(const rpc::TestRequest &,
             rpc::TestResponse *response) override {
    response->set_message("Hello from TestMethod");
    co_return absl::OkStatus();
  }

  co20::ValueTask<absl::Status>
  PerfMethod(const rpc::PerfRequest &request,
             rpc::PerfResponse *response) override {
    response->set_client_send_time(request.send_time());
    response->set_server_send_time(toolbelt::Now());
    co_return absl::OkStatus();
  }

  co20::ValueTask<absl::Status>
  StreamMethod(const rpc::TestRequest &request,
               subspace::co20_rpc::StreamWriter<rpc::TestResponse> &writer)
      override {
    constexpr int kNumResults = 200;
    for (int i = 0; i < kNumResults; i++) {
      if (writer.IsCancelled()) {
        co_await writer.Finish();
        co_return absl::OkStatus();
      }
      rpc::TestResponse r;
      r.set_message("Hello from StreamMethod part " + std::to_string(i));
      co_await writer.Write(r);
      co_await co20::Sleep(
          std::chrono::milliseconds(request.stream_period()));
    }
    co_await writer.Finish();
    co_return absl::OkStatus();
  }

  co20::ValueTask<absl::Status>
  ErrorMethod(const rpc::TestRequest &,
              rpc::TestResponse *) override {
    co_return absl::InternalError("Error occurred");
  }

  co20::ValueTask<absl::Status>
  TimeoutMethod(const rpc::TestRequest &,
                rpc::TestResponse *response) override {
    co_await co20::Sleep(std::chrono::seconds(2));
    response->set_message("Hello from TimeoutMethod");
    co_return absl::OkStatus();
  }
};

static std::shared_ptr<MyServer> BuildServer() {
  auto server = std::make_shared<MyServer>(Co20RpcTest::Socket());
  server->SetLogLevel("info");
  auto s = server->RegisterMethods();
  EXPECT_TRUE(s.ok());
  return server;
}

TEST_F(Co20RpcTest, Basic) {
  co20::Scheduler scheduler;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&]() -> co20::Task {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), Co20RpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        absl::StatusOr<rpc::TestResponse> r;
        auto s = co_await client->TestMethod(req, &r);
        CO_ASSERT_OK(s);
        EXPECT_EQ(r->message(), "Hello from TestMethod");

        auto cs = co_await client->Close();
        EXPECT_TRUE(cs.ok()) << cs;
        server->Stop();
        co_return;
      },
      "test");

  scheduler.Run();
}

TEST_F(Co20RpcTest, Stream) {
  co20::Scheduler scheduler;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&]() -> co20::Task {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), Co20RpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        class MyResponseReceiver
            : public subspace::co20_rpc::ResponseReceiver<rpc::TestResponse> {
        public:
          void OnResponse(rpc::TestResponse &&) override { count++; }
          void OnError(const absl::Status &) override {}
          void OnCancel() override {}
          void OnFinish() override {}
          int count = 0;
        };

        MyResponseReceiver receiver;
        rpc::TestRequest req;
        req.set_message("this is a test");
        auto status = co_await client->StreamMethod(req, receiver);
        CO_ASSERT_OK(status);
        CO_ASSERT_EQ(200, receiver.count);

        auto cs = co_await client->Close();
        EXPECT_TRUE(cs.ok()) << cs;
        server->Stop();
        co_return;
      },
      "test");

  scheduler.Run();
}

TEST_F(Co20RpcTest, Error) {
  co20::Scheduler scheduler;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&]() -> co20::Task {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), Co20RpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        absl::StatusOr<rpc::TestResponse> r;
        auto s = co_await client->ErrorMethod(req, &r);
        CO_ASSERT_FALSE(s.ok());
        EXPECT_TRUE(absl::StrContains(s.ToString(),
                                      "INTERNAL: Error occurred"));

        auto cs = co_await client->Close(2s);
        EXPECT_TRUE(cs.ok()) << cs;
        server->Stop();
        co_return;
      },
      "test");

  scheduler.Run();
}

TEST_F(Co20RpcTest, Timeout) {
  co20::Scheduler scheduler;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&]() -> co20::Task {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), Co20RpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        absl::StatusOr<rpc::TestResponse> r;
        auto s = co_await client->TimeoutMethod(req, &r, 1s);
        CO_ASSERT_FALSE(s.ok());
        EXPECT_TRUE(absl::StrContains(s.ToString(),
                                      "Error waiting for response"))
            << "Actual: " << s.ToString();

        auto cs = co_await client->Close(2s);
        EXPECT_TRUE(cs.ok()) << cs;
        server->Stop();
        scheduler.Stop();
        co_return;
      },
      "test");

  scheduler.Run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  return RUN_ALL_TESTS();
}
