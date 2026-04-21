// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "rpc/proto/rpc_test.pb.h"
#include "asio_rpc/proto/rpc_test_asio_rpc/rpc/proto/rpc_test.subspace.rpc_client.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <inttypes.h>
#include <memory>
#include <signal.h>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

ABSL_FLAG(std::string, subspace_socket, "/tmp/subspace",
          "Subspace server socket name");
ABSL_FLAG(std::string, method, "test", "Method to call");
ABSL_FLAG(std::string, log_level, "info",
          "Log level (debug, info, warn, error)");
ABSL_FLAG(int, perf_iterations, 1000,
          "Number of iterations for PerfMethod");

static absl::Status
InvokeTestMethod(std::shared_ptr<rpc::TestServiceClient> client,
                 boost::asio::yield_context yield) {
  rpc::TestRequest req;
  req.set_message("this is a test");
  auto r = client->TestMethod(req, yield);
  if (!r.ok()) {
    return r.status();
  }
  std::cout << "Response: " << r->DebugString() << std::endl;
  return absl::OkStatus();
}

static absl::Status
InvokePerfMethod(std::shared_ptr<rpc::TestServiceClient> client,
                 boost::asio::yield_context yield) {
  int num_iterations = absl::GetFlag(FLAGS_perf_iterations);
  uint64_t total_rtt = 0;
  uint64_t total_server_time = 0;
  for (int i = 0; i < num_iterations; i++) {
    rpc::PerfRequest req;
    req.set_send_time(toolbelt::Now());
    auto r = client->PerfMethod(req, yield);
    if (!r.ok()) {
      return r.status();
    }
    uint64_t rtt = toolbelt::Now() - req.send_time();
    uint64_t server_time = r->server_send_time() - r->client_send_time();
    total_rtt += rtt;
    total_server_time += server_time;
  }
  uint64_t rtt = total_rtt / num_iterations;
  uint64_t server_time = total_server_time / num_iterations;
  std::cout << "RTT: " << rtt << " ns, Server time: " << server_time << " ns"
            << std::endl;
  return absl::OkStatus();
}

static absl::Status
InvokeStreamMethod(std::shared_ptr<rpc::TestServiceClient> client,
                   boost::asio::yield_context yield) {
  rpc::TestRequest req;
  req.set_message("this is a test");
  req.set_stream_period(10);

  class MyResponseReceiver
      : public subspace::asio_rpc::ResponseReceiver<rpc::TestResponse> {
  public:
    void OnResponse(rpc::TestResponse &&response) override {
      std::cerr << "Received response: " << response.message() << std::endl;
      count++;
    }
    void OnError(const absl::Status &status) override {
      std::cerr << "Received error: " << status.ToString() << std::endl;
    }
    void OnCancel() override { std::cerr << "Stream cancelled" << std::endl; }
    void OnFinish() override { std::cerr << "Stream finished" << std::endl; }
    int GetCount() const { return count; }
    int count = 0;
  };

  MyResponseReceiver receiver;
  absl::Status status = client->StreamMethod(req, receiver, yield);
  if (!status.ok()) {
    return status;
  }

  std::cout << "Received " << receiver.GetCount() << " responses" << std::endl;
  return absl::OkStatus();
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  std::string socket = absl::GetFlag(FLAGS_subspace_socket);
  std::string method = absl::GetFlag(FLAGS_method);
  std::string log_level = absl::GetFlag(FLAGS_log_level);

  boost::asio::io_context ioc;
  int exit_code = 0;

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        auto cl = rpc::TestServiceClient::Create(getpid(), socket, yield);
        if (!cl.ok()) {
          fprintf(stderr, "Error creating client: %s\n",
                  cl.status().ToString().c_str());
          exit_code = 1;
          return;
        }
        auto client = *cl;
        client->SetLogLevel(log_level);

        absl::Status status;
        if (method == "test") {
          status = InvokeTestMethod(client, yield);
        } else if (method == "stream") {
          status = InvokeStreamMethod(client, yield);
        } else if (method == "perf") {
          status = InvokePerfMethod(client, yield);
        } else {
          fprintf(stderr, "Unknown method: %s\n", method.c_str());
          exit_code = 1;
          return;
        }

        if (!status.ok()) {
          fprintf(stderr, "Error invoking %s: %s\n", method.c_str(),
                  status.ToString().c_str());
          exit_code = 1;
        }

        auto close_status = client->Close(yield);
        if (!close_status.ok()) {
          fprintf(stderr, "Error closing client: %s\n",
                  close_status.ToString().c_str());
          exit_code = 1;
        }
      },
      boost::asio::detached);

  ioc.run();
  return exit_code;
}
