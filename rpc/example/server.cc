// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "rpc/proto/rpc_test.pb.h"
#include "rpc/proto/rpc_test.subspace.rpc_server.h"
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
  absl::StatusOr<rpc::TestResponse> TestMethod(const rpc::TestRequest &request,
                                               co::Coroutine *c) override {
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
  fprintf(stderr, "Starting server...\n");
  auto status = server.Run();
  if (!status.ok()) {
    fprintf(stderr, "Error starting server: %s\n", status.ToString().c_str());
    return 1;
  }
}