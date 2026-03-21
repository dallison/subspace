// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "rpc/proto/rpc_test.pb.h"
#include "asio_rpc/proto/rpc_test_asio_rpc/rpc/proto/rpc_test.subspace.rpc_server.h"
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

  absl::StatusOr<rpc::TestResponse>
  TestMethod(const rpc::TestRequest &request,
             boost::asio::yield_context yield) override {
    rpc::TestResponse response;
    response.set_message("Hello from TestMethod");
    return response;
  }

  absl::StatusOr<rpc::PerfResponse>
  PerfMethod(const rpc::PerfRequest &request,
             boost::asio::yield_context yield) override {
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
  ErrorMethod(const rpc::TestRequest &request,
              boost::asio::yield_context yield) override {
    return absl::InternalError("Error occurred");
  }

  absl::StatusOr<rpc::TestResponse>
  TimeoutMethod(const rpc::TestRequest &request,
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
