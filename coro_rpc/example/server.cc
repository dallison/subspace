// Copyright 2023-2026 David Allison
// Coro RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "rpc/proto/rpc_test.pb.h"
#include "coro_rpc/proto/rpc_test_coro_rpc/rpc/proto/rpc_test.subspace.rpc_server.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <inttypes.h>
#include <memory>
#include <signal.h>

#include <boost/asio.hpp>

ABSL_FLAG(std::string, subspace_socket, "/tmp/subspace",
          "Subspace server socket name");
ABSL_FLAG(std::string, log_level, "info",
          "Log level (debug, info, warn, error)");

class TestServer : public rpc::TestServiceServer {
public:
  TestServer(const std::string &socket) : rpc::TestServiceServer(socket) {}

  boost::asio::awaitable<absl::StatusOr<rpc::TestResponse>>
  TestMethod(const rpc::TestRequest &request) override {
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
  ErrorMethod(const rpc::TestRequest &request) override {
    co_return absl::InternalError("Error occurred");
  }

  boost::asio::awaitable<absl::StatusOr<rpc::TestResponse>>
  TimeoutMethod(const rpc::TestRequest &request) override {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor,
                                    std::chrono::seconds(2));
    co_await timer.async_wait(boost::asio::use_awaitable);
    rpc::TestResponse response;
    response.set_message("Hello from TimeoutMethod");
    co_return response;
  }
};

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);

  TestServer server(absl::GetFlag(FLAGS_subspace_socket));
  server.SetLogLevel(absl::GetFlag(FLAGS_log_level));
  auto reg_status = server.RegisterMethods();
  if (!reg_status.ok()) {
    fprintf(stderr, "Error registering methods: %s\n",
            reg_status.ToString().c_str());
    return 1;
  }

  boost::asio::io_context ioc;
  fprintf(stderr, "Starting server...\n");
  auto status = server.Run(&ioc);
  if (!status.ok()) {
    fprintf(stderr, "Error starting server: %s\n", status.ToString().c_str());
    return 1;
  }
  ioc.run();
}
