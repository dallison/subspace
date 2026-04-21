// Copyright 2023-2026 David Allison
// co20 RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "absl/strings/match.h"
#include "co/coroutine_cpp20.h"
#include "co20_rpc/client/rpc_client.h"
#include "co20_rpc/server/rpc_server.h"
#include "client/test_fixture.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

using namespace std::chrono_literals;

#define CO_EXPECT_OK(e)                                                        \
  do {                                                                         \
    const auto &_co_s = (e);                                                   \
    EXPECT_TRUE(_co_s.ok()) << "Expected OK: " #e;                            \
    if (!_co_s.ok())                                                           \
      co_return;                                                               \
  } while (0)

#define CO_EXPECT_TRUE(cond)                                                   \
  do {                                                                         \
    EXPECT_TRUE(cond);                                                         \
    if (!(cond))                                                               \
      co_return;                                                               \
  } while (0)

#define CO_EXPECT_FALSE(cond)                                                  \
  do {                                                                         \
    EXPECT_FALSE(cond);                                                        \
    if ((cond))                                                                \
      co_return;                                                               \
  } while (0)

class Co20ClientTest : public SubspaceTestBase {};

static std::shared_ptr<subspace::co20_rpc::RpcServer> BuildServer() {
  auto server = std::make_shared<subspace::co20_rpc::RpcServer>(
      "Co20ClientTestService", Co20ClientTest::Socket());
  server->SetLogLevel("info");

  auto s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "TestMethod",
      [](const auto &,
         auto *res) -> co20::ValueTask<absl::Status> {
        res->set_message("Hello from TestMethod");
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::PerfRequest, rpc::PerfResponse>(
      "PerfMethod",
      [](const auto &req,
         auto *res) -> co20::ValueTask<absl::Status> {
        res->set_client_send_time(req.send_time());
        res->set_server_send_time(toolbelt::Now());
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidMethod",
      [](const auto &) -> co20::ValueTask<absl::Status> {
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "ErrorMethod",
      [](const auto &,
         auto *) -> co20::ValueTask<absl::Status> {
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidErrorMethod",
      [](const auto &) -> co20::ValueTask<absl::Status> {
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "StreamMethod",
      [](const auto &req,
         subspace::co20_rpc::StreamWriter<rpc::TestResponse> &writer)
          -> co20::ValueTask<absl::Status> {
        constexpr int kNumResults = 200;
        for (int i = 0; i < kNumResults; i++) {
          if (writer.IsCancelled()) {
            co_await writer.Finish();
            co_return absl::OkStatus();
          }
          rpc::TestResponse r;
          r.set_message("Hello from StreamMethod part " + std::to_string(i));
          co_await writer.Write(r);
          if (req.stream_period() > 0) {
            co_await co20::Sleep(
                std::chrono::milliseconds(req.stream_period()));
          }
        }

        co_await writer.Finish();
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  return server;
}

TEST_F(Co20ClientTest, Typed) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        auto client = std::make_shared<subspace::co20_rpc::RpcClient>(
            "Co20ClientTestService", 1, Co20ClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp;
        auto call_s = co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
            "TestMethod", test_request, &resp);
        CO_EXPECT_OK(call_s);
        CO_EXPECT_OK(resp);

        EXPECT_EQ(resp->message(), "Hello from TestMethod");
        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
        co_return;
      },
      "test_typed");

  scheduler.Run();
}

TEST_F(Co20ClientTest, TypedMethodId) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        auto client = std::make_shared<subspace::co20_rpc::RpcClient>(
            "Co20ClientTestService", 2, Co20ClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        auto TestMethodId = client->FindMethod("TestMethod");
        CO_EXPECT_OK(TestMethodId);

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp;
        auto call_s = co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
            *TestMethodId, test_request, &resp);
        CO_EXPECT_OK(call_s);
        CO_EXPECT_OK(resp);

        EXPECT_EQ(resp->message(), "Hello from TestMethod");
        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
        co_return;
      },
      "test_typed_method_id");

  scheduler.Run();
}

TEST_F(Co20ClientTest, TypedVoid) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        auto client = std::make_shared<subspace::co20_rpc::RpcClient>(
            "Co20ClientTestService", 3, Co20ClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        CO_EXPECT_OK(
            co_await client->Call<rpc::TestRequest>("VoidMethod", test_request));

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
        co_return;
      },
      "test_typed_void");

  scheduler.Run();
}

TEST_F(Co20ClientTest, TypedError) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        auto client = std::make_shared<subspace::co20_rpc::RpcClient>(
            "Co20ClientTestService", 4, Co20ClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp;
        auto call_s = co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
            "ErrorMethod", test_request, &resp);
        CO_EXPECT_FALSE(call_s.ok());
        EXPECT_TRUE(absl::StrContains(call_s.ToString(),
                                      "INTERNAL: Error occurred"));

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
        co_return;
      },
      "test_typed_error");

  scheduler.Run();
}

TEST_F(Co20ClientTest, Performance) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        auto client = std::make_shared<subspace::co20_rpc::RpcClient>(
            "Co20ClientTestService", 5, Co20ClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        auto PerfMethodId = client->FindMethod("PerfMethod");
        CO_EXPECT_OK(PerfMethodId);

        uint64_t total_rtt = 0;
        uint64_t total_server_time = 0;

        int num_iterations = 10000;
        for (int i = 0; i < num_iterations; i++) {
          rpc::PerfRequest request;
          request.set_send_time(toolbelt::Now());

          absl::StatusOr<rpc::PerfResponse> resp;
          auto call_s =
              co_await client->Call<rpc::PerfRequest, rpc::PerfResponse>(
                  *PerfMethodId, request, &resp);
          CO_EXPECT_OK(call_s);
          CO_EXPECT_OK(resp);

          uint64_t rtt = toolbelt::Now() - request.send_time();
          int64_t server_time =
              resp->server_send_time() - resp->client_send_time();
          total_rtt += rtt;
          total_server_time += server_time;
        }
        std::cerr << "RTT: " << (total_rtt / num_iterations)
                  << " ns, server time: "
                  << (total_server_time / num_iterations) << " ns" << std::endl;

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
        co_return;
      },
      "test_performance");

  scheduler.Run();
}

TEST_F(Co20ClientTest, TypedStream) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        auto client = std::make_shared<subspace::co20_rpc::RpcClient>(
            "Co20ClientTestService", 6, Co20ClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        class MyResponseReceiver
            : public subspace::co20_rpc::ResponseReceiver<rpc::TestResponse> {
        public:
          void OnResponse(rpc::TestResponse &&) override {
            count++;
          }

          void OnError(const absl::Status &status) override {
            std::cerr << "Received error: " << status.ToString() << std::endl;
          }

          void OnCancel() override {}

          void OnFinish() override {}

          int count = 0;
        };

        MyResponseReceiver receiver;
        absl::Status status =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                "StreamMethod", test_request, receiver);
        CO_EXPECT_OK(status);

        EXPECT_EQ(200, receiver.count);
        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
        co_return;
      },
      "test_typed_stream");

  scheduler.Run();
}

TEST_F(Co20ClientTest, CancelStream) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        auto client = std::make_shared<subspace::co20_rpc::RpcClient>(
            "Co20ClientTestService", 7, Co20ClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");
        test_request.set_stream_period(100);

        class MyResponseReceiver
            : public subspace::co20_rpc::ResponseReceiver<rpc::TestResponse> {
        public:
          void OnResponse(rpc::TestResponse &&) override {
            count++;
            if (count == 5) {
              if (auto status = CancelBlocking(); !status.ok()) {
                std::cerr << "Error cancelling: " << status.ToString()
                          << std::endl;
              }
            }
          }

          void OnError(const absl::Status &status) override {
            std::cerr << "Received error: " << status.ToString() << std::endl;
          }

          void OnCancel() override {}

          void OnFinish() override {}

          int count = 0;
        };

        MyResponseReceiver receiver;
        absl::Status status =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                "StreamMethod", test_request, receiver);
        CO_EXPECT_OK(status);

        EXPECT_LE(receiver.count, 6);

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
        co_return;
      },
      "test_cancel_stream");

  scheduler.Run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
