// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "absl/strings/match.h"
#include "coro_rpc/client/rpc_client.h"
#include "coro_rpc/server/rpc_server.h"
#include "client/test_fixture.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>

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

class CoroClientTest : public SubspaceTestBase {};

static std::shared_ptr<subspace::coro_rpc::RpcServer> BuildServer() {
  auto server = std::make_shared<subspace::coro_rpc::RpcServer>(
      "CoroClientTestService", CoroClientTest::Socket());
  server->SetLogLevel("info");

  auto s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "TestMethod",
      [](const auto &req,
         auto *res) -> boost::asio::awaitable<absl::Status> {
        std::cerr << "TestMethod called with request: " << req.DebugString()
                  << std::endl;
        res->set_message("Hello from TestMethod");
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::PerfRequest, rpc::PerfResponse>(
      "PerfMethod",
      [](const auto &req,
         auto *res) -> boost::asio::awaitable<absl::Status> {
        res->set_client_send_time(req.send_time());
        res->set_server_send_time(toolbelt::Now());
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidMethod",
      [](const auto &req) -> boost::asio::awaitable<absl::Status> {
        std::cerr << "VoidMethod called with request: " << req.DebugString()
                  << std::endl;
        co_return absl::OkStatus();
      });

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "ErrorMethod",
      [](const auto &req,
         auto *) -> boost::asio::awaitable<absl::Status> {
        std::cerr << "ErrorMethod called with request: " << req.DebugString()
                  << std::endl;
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidErrorMethod",
      [](const auto &req) -> boost::asio::awaitable<absl::Status> {
        std::cerr << "VoidErrorMethod called with request: "
                  << req.DebugString() << std::endl;
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "TimeoutMethod",
      [](const auto &req,
         auto *res) -> boost::asio::awaitable<absl::Status> {
        std::cerr << "TimeoutMethod called with request: " << req.DebugString()
                  << std::endl;
        auto executor = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer(executor, std::chrono::seconds(2));
        co_await timer.async_wait(boost::asio::use_awaitable);
        res->set_message("Hello from TimeoutMethod");
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "StreamMethod",
      [](const auto &req,
         subspace::coro_rpc::StreamWriter<rpc::TestResponse> &writer)
          -> boost::asio::awaitable<absl::Status> {
        std::cerr << "StreamMethod called with request: " << req.DebugString()
                  << std::endl;
        auto executor = co_await boost::asio::this_coro::executor;

        constexpr int kNumResults = 200;
        for (int i = 0; i < kNumResults; i++) {
          if (writer.IsCancelled()) {
            std::cerr << "StreamMethod cancelled after " << i << " responses"
                      << std::endl;
            co_await writer.Finish();
            co_return absl::OkStatus();
          }
          rpc::TestResponse r;
          r.set_message("Hello from StreamMethod part " + std::to_string(i));
          co_await writer.Write(r);
          boost::asio::steady_timer timer(
              executor, std::chrono::milliseconds(req.stream_period()));
          co_await timer.async_wait(boost::asio::use_awaitable);
        }

        co_await writer.Finish();
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  return server;
}

TEST_F(CoroClientTest, Typed) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 1, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                "TestMethod", test_request);
        std::cerr << resp.status().ToString() << std::endl;
        CO_EXPECT_OK(resp);

        EXPECT_EQ(resp->message(), "Hello from TestMethod");
        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroClientTest, TypedMethodId) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 2, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        auto TestMethodId = client->FindMethod("TestMethod");
        CO_EXPECT_OK(TestMethodId);

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                *TestMethodId, test_request);
        CO_EXPECT_OK(resp);

        EXPECT_EQ(resp->message(), "Hello from TestMethod");
        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroClientTest, TypedVoid) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 3, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::Status resp =
            co_await client->Call<rpc::TestRequest>("VoidMethod", test_request);
        CO_EXPECT_OK(resp);

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroClientTest, TypedError) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 4, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                "ErrorMethod", test_request);
        CO_EXPECT_FALSE(resp.ok());
        std::cerr << "Received error: " << resp.status().ToString()
                  << std::endl;
        EXPECT_TRUE(absl::StrContains(resp.status().ToString(),
                                      "INTERNAL: Error occurred"));

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroClientTest, TypedTimeout) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 5, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                "TimeoutMethod", test_request, 1s);
        CO_EXPECT_FALSE(resp.ok());
        std::cerr << "Received error: " << resp.status().ToString()
                  << std::endl;
        EXPECT_TRUE(
            absl::StrContains(resp.status().ToString(), "Timeout"));

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroClientTest, Performance) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 6, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        auto PerfMethodId = client->FindMethod("PerfMethod");
        CO_EXPECT_OK(PerfMethodId);

        uint64_t total_rtt = 0;
        uint64_t total_server_time = 0;

        int num_iterations = 10000;
        for (int i = 0; i < num_iterations; i++) {
          rpc::PerfRequest request;
          request.set_send_time(toolbelt::Now());

          absl::StatusOr<rpc::PerfResponse> resp =
              co_await client->Call<rpc::PerfRequest, rpc::PerfResponse>(
                  *PerfMethodId, request);
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
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroClientTest, TypedStream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 7, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        class MyResponseReceiver
            : public subspace::coro_rpc::ResponseReceiver<rpc::TestResponse> {
        public:
          void OnResponse(rpc::TestResponse &&response) override {
            std::cerr << "Received response: " << response.message()
                      << std::endl;
            count++;
          }

          void OnError(const absl::Status &status) override {
            std::cerr << "Received error: " << status.ToString() << std::endl;
          }

          void OnCancel() override {
            std::cerr << "Stream cancelled" << std::endl;
          }

          void OnFinish() override {
            std::cerr << "Stream finished" << std::endl;
          }

          int GetCount() const { return count; }
          int count = 0;
        };

        MyResponseReceiver receiver;
        absl::Status status =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                "StreamMethod", test_request, receiver);
        std::cerr << status.ToString() << std::endl;
        CO_EXPECT_OK(status);

        EXPECT_EQ(200, receiver.GetCount());
        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroClientTest, CancelStream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto client = std::make_shared<subspace::coro_rpc::RpcClient>(
            "CoroClientTestService", 8, CoroClientTest::Socket());
        CO_EXPECT_OK(co_await client->Open());

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");
        test_request.set_stream_period(100);

        class MyResponseReceiver
            : public subspace::coro_rpc::ResponseReceiver<rpc::TestResponse> {
        public:
          void OnResponse(rpc::TestResponse &&response) override {
            std::cerr << "Received response: " << response.message()
                      << std::endl;
            count++;
            if (count == 5) {
              std::cerr << "Cancelling stream after 5 responses" << std::endl;
              if (auto status = CancelBlocking(); !status.ok()) {
                std::cerr << "Error cancelling stream: " << status.ToString()
                          << std::endl;
              }
            }
          }

          void OnError(const absl::Status &status) override {
            std::cerr << "Received error: " << status.ToString() << std::endl;
          }

          void OnCancel() override {
            std::cerr << "Stream cancelled" << std::endl;
          }

          void OnFinish() override {
            std::cerr << "Stream finished" << std::endl;
          }

          int GetCount() const { return count; }

          int count = 0;
        };

        MyResponseReceiver receiver;
        absl::Status status =
            co_await client->Call<rpc::TestRequest, rpc::TestResponse>(
                "StreamMethod", test_request, receiver);
        std::cerr << status.ToString() << std::endl;
        CO_EXPECT_OK(status);

        EXPECT_LE(receiver.GetCount(), 6);

        CO_EXPECT_OK(co_await client->Close());
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
