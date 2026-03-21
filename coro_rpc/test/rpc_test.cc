// Copyright 2023-2026 David Allison
// Coro RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "absl/strings/match.h"
#include "client/test_fixture.h"
#include "coro_rpc/proto/rpc_test_coro_rpc/rpc/proto/rpc_test.subspace.rpc_client.h"
#include "coro_rpc/proto/rpc_test_coro_rpc/rpc/proto/rpc_test.subspace.rpc_server.h"
#include "rpc/proto/rpc_test.pb.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>

using namespace std::chrono_literals;

#define CO_ASSERT_OK(e)                                                        \
  do {                                                                         \
    const auto &_co_s = (e);                                                   \
    EXPECT_TRUE(_co_s.ok()) << "Expected OK: " #e;                            \
    if (!_co_s.ok())                                                           \
      co_return;                                                               \
  } while (0)

#define CO_ASSERT_FALSE(cond)                                                  \
  do {                                                                         \
    EXPECT_FALSE(cond);                                                        \
    if ((cond))                                                                \
      co_return;                                                               \
  } while (0)

class CoroRpcTest : public SubspaceTestBase {};

class MyServer : public rpc::TestServiceServer {
public:
  MyServer(const std::string &socket) : rpc::TestServiceServer(socket) {}

  boost::asio::awaitable<absl::StatusOr<rpc::TestResponse>>
  TestMethod(const rpc::TestRequest &) override {
    rpc::TestResponse response;
    response.set_message("Hello from TestMethod");
    co_return response;
  }

  boost::asio::awaitable<absl::StatusOr<rpc::PerfResponse>>
  PerfMethod(const rpc::PerfRequest &request) override {
    rpc::PerfResponse res;
    res.set_client_send_time(request.send_time());
    res.set_server_send_time(toolbelt::Now());
    co_return res;
  }

  boost::asio::awaitable<absl::Status>
  StreamMethod(const rpc::TestRequest &request,
               subspace::coro_rpc::StreamWriter<rpc::TestResponse> &writer)
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
      boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor,
                                      std::chrono::milliseconds(
                                          request.stream_period()));
      co_await timer.async_wait(boost::asio::use_awaitable);
    }
    co_await writer.Finish();
    co_return absl::OkStatus();
  }

  boost::asio::awaitable<absl::StatusOr<rpc::TestResponse>>
  ErrorMethod(const rpc::TestRequest &) override {
    co_return absl::InternalError("Error occurred");
  }

  boost::asio::awaitable<absl::StatusOr<rpc::TestResponse>>
  TimeoutMethod(const rpc::TestRequest &) override {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor,
                                    std::chrono::seconds(2));
    co_await timer.async_wait(boost::asio::use_awaitable);
    rpc::TestResponse response;
    response.set_message("Hello from TimeoutMethod");
    co_return response;
  }
};

static std::shared_ptr<MyServer> BuildServer() {
  auto server = std::make_shared<MyServer>(CoroRpcTest::Socket());
  server->SetLogLevel("info");
  auto s = server->RegisterMethods();
  EXPECT_TRUE(s.ok());
  return server;
}

TEST_F(CoroRpcTest, Basic) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), CoroRpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        auto r = co_await client->TestMethod(req);
        CO_ASSERT_OK(r);
        EXPECT_EQ(r->message(), "Hello from TestMethod");

        auto s = co_await client->Close();
        CO_ASSERT_OK(s);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroRpcTest, Stream) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), CoroRpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        class MyResponseReceiver
            : public subspace::coro_rpc::ResponseReceiver<rpc::TestResponse> {
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
        EXPECT_EQ(200, receiver.count);

        auto s = co_await client->Close();
        CO_ASSERT_OK(s);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroRpcTest, Error) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), CoroRpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        auto r = co_await client->ErrorMethod(req);
        CO_ASSERT_FALSE(r.ok());
        EXPECT_TRUE(absl::StrContains(r.status().ToString(),
                                      "INTERNAL: Error occurred"));

        auto s = co_await client->Close(2s);
        CO_ASSERT_OK(s);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroRpcTest, Timeout) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto cl = co_await rpc::TestServiceClient::Create(
            getpid(), CoroRpcTest::Socket());
        CO_ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        auto r = co_await client->TimeoutMethod(req, 1s);
        CO_ASSERT_FALSE(r.ok());
        EXPECT_TRUE(absl::StrContains(r.status().ToString(),
                                      "Error waiting for response"))
            << "Actual: " << r.status().ToString();

        auto s = co_await client->Close(2s);
        CO_ASSERT_OK(s);
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
