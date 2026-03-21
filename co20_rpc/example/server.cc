// Copyright 2023-2026 David Allison
// co20 RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "co/coroutine_cpp20.h"
#include "rpc/proto/rpc_test.pb.h"
#include "co20_rpc/proto/rpc_test_co20_rpc/rpc/proto/rpc_test.subspace.rpc_server.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <inttypes.h>
#include <memory>
#include <signal.h>

ABSL_FLAG(std::string, subspace_socket, "/tmp/subspace",
          "Subspace server socket name");
ABSL_FLAG(std::string, log_level, "info",
          "Log level (debug, info, warn, error)");

class TestServer : public rpc::TestServiceServer {
public:
  TestServer(const std::string &socket) : rpc::TestServiceServer(socket) {}

  co20::ValueTask<absl::Status>
  TestMethod(const rpc::TestRequest &request,
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
      co_await co20::Sleep(std::chrono::milliseconds(request.stream_period()));
    }

    co_await writer.Finish();
    co_return absl::OkStatus();
  }

  co20::ValueTask<absl::Status>
  ErrorMethod(const rpc::TestRequest &request,
              rpc::TestResponse *response) override {
    co_return absl::InternalError("Error occurred");
  }

  co20::ValueTask<absl::Status>
  TimeoutMethod(const rpc::TestRequest &request,
                rpc::TestResponse *response) override {
    co_await co20::Sleep(std::chrono::seconds(2));
    response->set_message("Hello from TimeoutMethod");
    co_return absl::OkStatus();
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

  co20::Scheduler scheduler;
  fprintf(stderr, "Starting server...\n");
  auto status = server.Run(&scheduler);
  if (!status.ok()) {
    fprintf(stderr, "Error starting server: %s\n", status.ToString().c_str());
    return 1;
  }
  scheduler.Run();
}
