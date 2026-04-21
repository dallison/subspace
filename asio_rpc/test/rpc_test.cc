// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "absl/strings/match.h"
#include "asio_rpc/proto/rpc_test_asio_rpc/rpc/proto/rpc_test.subspace.rpc_client.h"
#include "asio_rpc/proto/rpc_test_asio_rpc/rpc/proto/rpc_test.subspace.rpc_server.h"
#include "client/test_fixture.h"
#include "rpc/proto/rpc_test.pb.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

using namespace std::chrono_literals;

class AsioRpcTest : public SubspaceTestBase {};

class MyServer : public rpc::TestServiceServer {
public:
  MyServer(const std::string &socket) : rpc::TestServiceServer(socket) {}

  absl::StatusOr<rpc::TestResponse>
  TestMethod(const rpc::TestRequest & /*request*/,
             boost::asio::yield_context) override {
    rpc::TestResponse response;
    response.set_message("Hello from TestMethod");
    return response;
  }

  absl::StatusOr<rpc::PerfResponse>
  PerfMethod(const rpc::PerfRequest &request,
             boost::asio::yield_context) override {
    rpc::PerfResponse res;
    res.set_client_send_time(request.send_time());
    res.set_server_send_time(toolbelt::Now());
    return res;
  }

  absl::Status
  StreamMethod(const rpc::TestRequest &request,
               subspace::asio_rpc::StreamWriter<rpc::TestResponse> &writer,
               boost::asio::yield_context yield) override {
    constexpr int kNumResults = 200;
    for (int i = 0; i < kNumResults; i++) {
      if (writer.IsCancelled()) {
        writer.Finish(yield);
        return absl::OkStatus();
      }
      rpc::TestResponse r;
      r.set_message("Hello from StreamMethod part " + std::to_string(i));
      writer.Write(r, yield);
      boost::asio::steady_timer timer(
          static_cast<boost::asio::io_context &>(
              yield.get_executor().context()),
          std::chrono::milliseconds(request.stream_period()));
      timer.async_wait(yield);
    }
    writer.Finish(yield);
    return absl::OkStatus();
  }

  absl::StatusOr<rpc::TestResponse>
  ErrorMethod(const rpc::TestRequest &,
              boost::asio::yield_context) override {
    return absl::InternalError("Error occurred");
  }

  absl::StatusOr<rpc::TestResponse>
  TimeoutMethod(const rpc::TestRequest &,
                boost::asio::yield_context yield) override {
    boost::asio::steady_timer timer(
        static_cast<boost::asio::io_context &>(
            yield.get_executor().context()),
        std::chrono::seconds(2));
    timer.async_wait(yield);
    rpc::TestResponse response;
    response.set_message("Hello from TimeoutMethod");
    return response;
  }
};

static std::shared_ptr<MyServer> BuildServer() {
  auto server = std::make_shared<MyServer>(AsioRpcTest::Socket());
  server->SetLogLevel("info");
  auto s = server->RegisterMethods();
  EXPECT_TRUE(s.ok());
  return server;
}

TEST_F(AsioRpcTest, Basic) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto cl =
            rpc::TestServiceClient::Create(getpid(), AsioRpcTest::Socket(),
                                           yield);
        ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        auto r = client->TestMethod(req, yield);
        ASSERT_OK(r);
        EXPECT_EQ(r->message(), "Hello from TestMethod");

        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioRpcTest, Stream) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto cl =
            rpc::TestServiceClient::Create(getpid(), AsioRpcTest::Socket(),
                                           yield);
        ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        class MyResponseReceiver
            : public subspace::asio_rpc::ResponseReceiver<rpc::TestResponse> {
        public:
          void OnResponse(rpc::TestResponse && /*response*/) override {
            count++;
          }
          void OnError(const absl::Status &) override {}
          void OnCancel() override {}
          void OnFinish() override {}
          int count = 0;
        };

        MyResponseReceiver receiver;
        rpc::TestRequest req;
        req.set_message("this is a test");
        absl::Status status = client->StreamMethod(req, receiver, yield);
        ASSERT_OK(status);
        ASSERT_EQ(200, receiver.count);

        ASSERT_OK(client->Close(yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioRpcTest, Error) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto cl =
            rpc::TestServiceClient::Create(getpid(), AsioRpcTest::Socket(),
                                           yield);
        ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        auto r = client->ErrorMethod(req, yield);
        ASSERT_FALSE(r.ok());
        EXPECT_TRUE(absl::StrContains(r.status().ToString(),
                                      "INTERNAL: Error occurred"));

        ASSERT_OK(client->Close(2s, yield));
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioRpcTest, Timeout) {
  boost::asio::io_context ioc;
  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto cl =
            rpc::TestServiceClient::Create(getpid(), AsioRpcTest::Socket(),
                                           yield);
        ASSERT_OK(cl);
        auto client = *cl;
        client->SetLogLevel("info");

        rpc::TestRequest req;
        req.set_message("this is a test");
        auto r = client->TimeoutMethod(req, 1s, yield);
        ASSERT_FALSE(r.ok());
        EXPECT_TRUE(absl::StrContains(r.status().ToString(),
                                      "Error waiting for response"))
            << "Actual: " << r.status().ToString();

        ASSERT_OK(client->Close(2s, yield));
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
