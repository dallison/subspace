// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef SUBSPACE_CLIENT_TEST_FIXTURE_H
#define SUBSPACE_CLIENT_TEST_FIXTURE_H

#include "absl/status/status_matchers.h"
#include "client/client.h"
#include "co/coroutine.h"
#include "server/server.h"
#include <cstdio>
#include <gtest/gtest.h>
#include <memory>
#include <signal.h>
#include <thread>
#include <unistd.h>

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;

#define VAR(a) a##__COUNTER__
#define EVAL_AND_ASSERT_OK(expr) EVAL_AND_ASSERT_OK2(VAR(r_), expr)

#define EVAL_AND_ASSERT_OK2(result, expr)                                      \
  ({                                                                           \
    auto result = (expr);                                                      \
    if (!result.ok()) {                                                        \
      std::cerr << result.status() << std::endl;                               \
    }                                                                          \
    ASSERT_OK(result);                                                         \
    std::move(*result);                                                        \
  })

#define ASSERT_OK(e) ASSERT_THAT(e, ::absl_testing::IsOk())

// Base test fixture that starts a single local Subspace server for the
// duration of the test suite.  Concrete fixtures inherit from this and
// automatically get server lifecycle management.
class SubspaceTestBase : public ::testing::Test {
public:
  static void SetUpTestSuite() {
    printf("Starting Subspace server\n");
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    (void)pipe(server_pipe_);

    server_ = std::make_unique<subspace::Server>(scheduler_, socket_, "", 0, 0,
                                                 /*local=*/true,
                                                 server_pipe_[1]);

    server_thread_ = std::thread([]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  static void TearDownTestSuite() {
    printf("Stopping Subspace server\n");
    server_->Stop();

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
    server_->CleanupAfterSession();
    (void)remove(socket_.c_str());
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }

  void InitClient(subspace::Client &client) {
    client.SetThreadSafe(true);
    ASSERT_OK(client.Init(Socket()));
  }

  static co::CoroutineScheduler &Scheduler() { return scheduler_; }
  static const std::string &Socket() { return socket_; }
  static subspace::Server *Server() { return server_.get(); }

private:
  inline static co::CoroutineScheduler scheduler_;
  inline static std::string socket_;
  inline static int server_pipe_[2];
  inline static std::unique_ptr<subspace::Server> server_;
  inline static std::thread server_thread_;
};

#endif // SUBSPACE_CLIENT_TEST_FIXTURE_H
