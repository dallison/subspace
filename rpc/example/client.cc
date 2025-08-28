// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "rpc/proto/rpc_test.pb.h"
#include "rpc/proto/rpc_test.subspace.rpc_client.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <inttypes.h>
#include <memory>
#include <signal.h>

ABSL_FLAG(std::string, subspace_socket, "/tmp/subspace",
          "Subspace server socket name");
ABSL_FLAG(std::string, method, "TestMethod", "Method to call");
ABSL_FLAG(std::string, log_level, "info",
          "Log level (debug, info, warn, error)");
ABSL_FLAG(int, perf_iterations, 1000,
          "Number of iterations for PerfMethod");
static absl::Status
InvokeTestMethod(std::shared_ptr<rpc::TestServiceClient> client) {
  rpc::TestRequest req;
  req.set_message("this is a test");
  auto r = client->TestMethod(req);
  if (!r.ok()) {
    return r.status();
  }
  std::cout << "Response: " << r->DebugString() << std::endl;
  return absl::OkStatus();
}

static absl::Status
InvokePerfMethod(std::shared_ptr<rpc::TestServiceClient> client) {
  int num_iterations = absl::GetFlag(FLAGS_perf_iterations);
  uint64_t total_rtt = 0;
  uint64_t total_server_time = 0;
  for (int i = 0; i < num_iterations; i++) {
    rpc::PerfRequest req;
    req.set_send_time(toolbelt::Now());
    auto r = client->PerfMethod(req);
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
InvokeStreamMethod(std::shared_ptr<rpc::TestServiceClient> client) {
  rpc::TestRequest req;
  req.set_message("this is a test");
  req.set_stream_period(10); // 10ms between responses

  class MyResponseReceiver
      : public subspace::ResponseReceiver<rpc::TestResponse> {
  public:
    void OnResponse(const rpc::TestResponse &response) override {
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
  absl::Status status = client->StreamMethod(req, receiver);
  if (!status.ok()) {
    return status;
  }

  std::cout << "Received " << receiver.GetCount() << " responses" << std::endl;
  return absl::OkStatus();
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  std::string socket = absl::GetFlag(FLAGS_subspace_socket);
  auto cl = rpc::TestServiceClient::Create(getpid(), socket);
  if (!cl.ok()) {
    fprintf(stderr, "Error creating client: %s\n",
            cl.status().ToString().c_str());
    return 1;
  }
  auto client = *cl;
  client->SetLogLevel(absl::GetFlag(FLAGS_log_level));
  std::string method = absl::GetFlag(FLAGS_method);
  if (method == "test") {
    auto status = InvokeTestMethod(client);
    if (!status.ok()) {
      fprintf(stderr, "Error invoking TestMethod: %s\n",
              status.ToString().c_str());
      return 1;
    }
  } else if (method == "stream") {
    auto status = InvokeStreamMethod(client);
    if (!status.ok()) {
      fprintf(stderr, "Error invoking StreamMethod: %s\n",
              status.ToString().c_str());
      return 1;
    }
  } else if (method == "perf") {
    auto status = InvokePerfMethod(client);
    if (!status.ok()) {
      fprintf(stderr, "Error invoking PerfMethod: %s\n",
              status.ToString().c_str());
      return 1;
    }
  } else {
    fprintf(stderr, "Unknown method: %s\n", method.c_str());
    return 1;
  }
  auto status = client->Close();
  if (!status.ok()) {
    fprintf(stderr, "Error closing client: %s\n", status.ToString().c_str());
    return 1;
  }
  return 0;
}