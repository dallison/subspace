// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "absl/strings/match.h"
#include "asio_rpc/client/rpc_client.h"
#include "asio_rpc/server/rpc_server.h"
#include "client/test_fixture.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

using namespace std::chrono_literals;

class AsioClientTest : public SubspaceTestBase {};

static std::shared_ptr<subspace::asio_rpc::RpcServer> BuildServer() {
  auto server = std::make_shared<subspace::asio_rpc::RpcServer>(
      "AsioClientTestService", AsioClientTest::Socket());
  server->SetLogLevel("info");

  auto s =
      server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
          "TestMethod",
          [](const auto &req, auto *res,
             boost::asio::yield_context) -> absl::Status {
            std::cerr << "TestMethod called with request: " << req.DebugString()
                      << std::endl;
            res->set_message("Hello from TestMethod");
            return absl::OkStatus();
          });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::PerfRequest, rpc::PerfResponse>(
      "PerfMethod",
      [](const auto &req, auto *res,
         boost::asio::yield_context) -> absl::Status {
        res->set_client_send_time(req.send_time());
        res->set_server_send_time(toolbelt::Now());
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidMethod",
      [](const auto &req, boost::asio::yield_context) -> absl::Status {
        std::cerr << "VoidMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::OkStatus();
      });

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "ErrorMethod",
      [](const auto &req, auto *,
         boost::asio::yield_context) -> absl::Status {
        std::cerr << "ErrorMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest>(
      "VoidErrorMethod",
      [](const auto &req, boost::asio::yield_context) -> absl::Status {
        std::cerr << "VoidErrorMethod called with request: "
                  << req.DebugString() << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "TimeoutMethod",
      [](const auto &req, auto *res,
         boost::asio::yield_context yield) -> absl::Status {
        std::cerr << "TimeoutMethod called with request: " << req.DebugString()
                  << std::endl;
        boost::asio::steady_timer timer(
            static_cast<boost::asio::io_context &>(
                yield.get_executor().context()),
            std::chrono::seconds(2));
        timer.async_wait(yield);
        res->set_message("Hello from TimeoutMethod");
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod<rpc::TestRequest, rpc::TestResponse>(
      "StreamMethod",
      [](const auto &req,
         subspace::asio_rpc::StreamWriter<rpc::TestResponse> &writer,
         boost::asio::yield_context yield) -> absl::Status {
        std::cerr << "StreamMethod called with request: " << req.DebugString()
                  << std::endl;

        constexpr int kNumResults = 200;
        for (int i = 0; i < kNumResults; i++) {
          if (writer.IsCancelled()) {
            std::cerr << "StreamMethod cancelled after " << i << " responses"
                      << std::endl;
            writer.Finish(yield);
            return absl::OkStatus();
          }
          rpc::TestResponse r;
          r.set_message("Hello from StreamMethod part " + std::to_string(i));
          writer.Write(r, yield);
          boost::asio::steady_timer timer(
              static_cast<boost::asio::io_context &>(
                  yield.get_executor().context()),
              std::chrono::milliseconds(req.stream_period()));
          timer.async_wait(yield);
        }

        writer.Finish(yield);
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  return server;
}

TEST_F(AsioClientTest, Typed) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 1, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            client->Call<rpc::TestRequest, rpc::TestResponse>("TestMethod",
                                                              test_request,
                                                              yield);
        std::cerr << resp.status().ToString() << std::endl;
        ASSERT_OK(resp);

        EXPECT_EQ(resp->message(), "Hello from TestMethod");
        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioClientTest, TypedMethodId) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 2, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

        auto TestMethodId = client->FindMethod("TestMethod");
        ASSERT_OK(TestMethodId);

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            client->Call<rpc::TestRequest, rpc::TestResponse>(*TestMethodId,
                                                              test_request,
                                                              yield);
        ASSERT_OK(resp);

        EXPECT_EQ(resp->message(), "Hello from TestMethod");
        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioClientTest, TypedVoid) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 3, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::Status resp =
            client->Call<rpc::TestRequest>("VoidMethod", test_request, yield);
        ASSERT_OK(resp);

        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioClientTest, TypedError) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 4, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            client->Call<rpc::TestRequest, rpc::TestResponse>("ErrorMethod",
                                                              test_request,
                                                              yield);
        ASSERT_FALSE(resp.ok());
        std::cerr << "Received error: " << resp.status().ToString()
                  << std::endl;
        ASSERT_TRUE(absl::StrContains(resp.status().ToString(),
                                      "INTERNAL: Error occurred"));

        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioClientTest, TypedTimeout) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 5, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        absl::StatusOr<rpc::TestResponse> resp =
            client->Call<rpc::TestRequest, rpc::TestResponse>("TimeoutMethod",
                                                              test_request, 1s,
                                                              yield);
        ASSERT_FALSE(resp.ok());
        std::cerr << "Received error: " << resp.status().ToString()
                  << std::endl;
        ASSERT_TRUE(
            absl::StrContains(resp.status().ToString(), "Timeout"));

        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioClientTest, Performance) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 6, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

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
                                                                request, yield);
          ASSERT_OK(resp);

          uint64_t rtt = toolbelt::Now() - request.send_time();
          int64_t server_time =
              resp->server_send_time() - resp->client_send_time();
          total_rtt += rtt;
          total_server_time += server_time;
        }
        std::cerr << "RTT: " << (total_rtt / num_iterations)
                  << " ns, server time: "
                  << (total_server_time / num_iterations) << " ns" << std::endl;

        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioClientTest, TypedStream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 7, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");

        class MyResponseReceiver
            : public subspace::asio_rpc::ResponseReceiver<rpc::TestResponse> {
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
            client->Call<rpc::TestRequest, rpc::TestResponse>(
                "StreamMethod", test_request, receiver, yield);
        std::cerr << status.ToString() << std::endl;
        ASSERT_OK(status);

        ASSERT_EQ(200, receiver.GetCount());
        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioClientTest, CancelStream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto client = std::make_shared<subspace::asio_rpc::RpcClient>(
            "AsioClientTestService", 8, AsioClientTest::Socket());
        ASSERT_OK(client->Open(yield));

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");
        test_request.set_stream_period(100);

        class MyResponseReceiver
            : public subspace::asio_rpc::ResponseReceiver<rpc::TestResponse> {
        public:
          void OnResponse(rpc::TestResponse &&response) override {
            std::cerr << "Received response: " << response.message()
                      << std::endl;
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
            client->Call<rpc::TestRequest, rpc::TestResponse>(
                "StreamMethod", test_request, receiver, yield);
        std::cerr << status.ToString() << std::endl;
        ASSERT_OK(status);

        ASSERT_LE(receiver.GetCount(), 6);

        ASSERT_OK(client->Close(yield));
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
