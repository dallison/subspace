// Copyright 2023-2026 David Allison
// Coro RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "rpc/proto/rpc_test.pb.h"
#include "coro_rpc/proto/rpc_test_coro_rpc/rpc/proto/rpc_test.subspace.rpc_client.h"
#include "toolbelt/clock.h"
#include <chrono>
#include <inttypes.h>
#include <memory>
#include <signal.h>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>

ABSL_FLAG(std::string, subspace_socket, "/tmp/subspace",
          "Subspace server socket name");
ABSL_FLAG(std::string, method, "test", "Method to call");
ABSL_FLAG(std::string, log_level, "info",
          "Log level (debug, info, warn, error)");
ABSL_FLAG(int, perf_iterations, 1000,
          "Number of iterations for PerfMethod");

static boost::asio::awaitable<absl::Status>
InvokeTestMethod(std::shared_ptr<rpc::TestServiceClient> client) {
  rpc::TestRequest req;
  req.set_message("this is a test");
  auto r = co_await client->TestMethod(req);
  if (!r.ok()) {
    co_return r.status();
  }
  std::cout << "Response: " << r->DebugString() << std::endl;
  co_return absl::OkStatus();
}

static boost::asio::awaitable<absl::Status>
InvokePerfMethod(std::shared_ptr<rpc::TestServiceClient> client) {
  int num_iterations = absl::GetFlag(FLAGS_perf_iterations);
  uint64_t total_rtt = 0;
  uint64_t total_server_time = 0;
  for (int i = 0; i < num_iterations; i++) {
    rpc::PerfRequest req;
    req.set_send_time(toolbelt::Now());
    auto r = co_await client->PerfMethod(req);
    if (!r.ok()) {
      co_return r.status();
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
  co_return absl::OkStatus();
}

static boost::asio::awaitable<absl::Status>
InvokeStreamMethod(std::shared_ptr<rpc::TestServiceClient> client) {
  rpc::TestRequest req;
  req.set_message("this is a test");
  req.set_stream_period(10);

  class MyResponseReceiver
      : public subspace::coro_rpc::ResponseReceiver<rpc::TestResponse> {
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
  absl::Status status = co_await client->StreamMethod(req, receiver);
  if (!status.ok()) {
    co_return status;
  }

  std::cout << "Received " << receiver.GetCount() << " responses" << std::endl;
  co_return absl::OkStatus();
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  std::string socket = absl::GetFlag(FLAGS_subspace_socket);
  std::string method = absl::GetFlag(FLAGS_method);
  std::string log_level = absl::GetFlag(FLAGS_log_level);

  boost::asio::io_context ioc;
  int exit_code = 0;

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        auto cl = co_await rpc::TestServiceClient::Create(getpid(), socket);
        if (!cl.ok()) {
          fprintf(stderr, "Error creating client: %s\n",
                  cl.status().ToString().c_str());
          exit_code = 1;
          co_return;
        }
        auto client = *cl;
        client->SetLogLevel(log_level);

        absl::Status status;
        if (method == "test") {
          status = co_await InvokeTestMethod(client);
        } else if (method == "stream") {
          status = co_await InvokeStreamMethod(client);
        } else if (method == "perf") {
          status = co_await InvokePerfMethod(client);
        } else {
          fprintf(stderr, "Unknown method: %s\n", method.c_str());
          exit_code = 1;
          co_return;
        }

        if (!status.ok()) {
          fprintf(stderr, "Error invoking %s: %s\n", method.c_str(),
                  status.ToString().c_str());
          exit_code = 1;
        }

        auto close_status = co_await client->Close();
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
