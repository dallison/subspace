// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status_matchers.h"
#include "client/client.h"
#include "coroutine.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include "rpc/server/rpc_server.h"
#include "server/server.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/pipe.h"
#include <gtest/gtest.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/resource.h>
#include <thread>

ABSL_FLAG(bool, start_server, true, "Start the subspace server");
ABSL_FLAG(std::string, server, "", "Path to server executable");

void SignalHandler(int sig) { printf("Signal %d", sig); }

using Publisher = subspace::Publisher;
using Subscriber = subspace::Subscriber;
using Message = subspace::Message;
using InetAddress = toolbelt::InetAddress;

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

class ServerTest : public ::testing::Test {
public:
  // We run one server for the duration of the whole test suite.
  static void SetUpTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    printf("Starting Subspace server\n");
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    // The server will write to this pipe to notify us when it
    // has started and stopped.  This end of the pipe is blocking.
    (void)pipe(server_pipe_);

    server_ =
        std::make_unique<subspace::Server>(scheduler_, socket_, "", 0, 0,
                                           /*local=*/true, server_pipe_[1]);

    // Start server running in a thread.
    server_thread_ = std::thread([]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });

    // Wait for server to tell us that it's running.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  static void TearDownTestSuite() {
    if (!absl::GetFlag(FLAGS_start_server)) {
      return;
    }
    printf("Stopping Subspace server\n");
    server_->Stop();

    // Wait for server to tell us that it's stopped.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
  }

  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
  void TearDown() override {}

  static void InitClient(subspace::Client &client) {
    ASSERT_OK(client.Init(Socket()));
  }

  static const std::string &Socket() { return socket_; }

  static subspace::Server *Server() { return server_.get(); }

private:
  static co::CoroutineScheduler scheduler_;
  static std::string socket_;
  static int server_pipe_[2];
  static std::unique_ptr<subspace::Server> server_;
  static std::thread server_thread_;
};

co::CoroutineScheduler ServerTest::scheduler_;
std::string ServerTest::socket_ = "/tmp/subspace";
int ServerTest::server_pipe_[2];
std::unique_ptr<subspace::Server> ServerTest::server_;
std::thread ServerTest::server_thread_;

static int next_session = 1;
static std::shared_ptr<subspace::RpcServer> BuildServer() {
  auto server = std::make_shared<subspace::RpcServer>("TestService",
                                                      ServerTest::Socket());

  server->SetLogLevel("debug");
  server->SetStartingSessionId(next_session++);

  auto s = server->RegisterMethod(
      "TestMethod", "subspace.TestRequest", "subspace.TestResponse", 256, 10,
      [](const google::protobuf::Any &req, google::protobuf::Any *res,
         co::Coroutine *) -> absl::Status {
        std::cerr << "TestMethod called with request: " << req.DebugString()
                  << std::endl;
        rpc::TestResponse r;
        r.set_message("Hello from TestMethod");
        res->PackFrom(r);
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  // Void method.
  s = server->RegisterMethod(
      "VoidMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req, co::Coroutine *) -> absl::Status {
        std::cerr << "VoidMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::OkStatus();
      });

  // Error method.
  s = server->RegisterMethod(
      "ErrorMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req, google::protobuf::Any *res,
         co::Coroutine *) -> absl::Status {
        std::cerr << "ErrorMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  // void error method.
  s = server->RegisterMethod(
      "VoidErrorMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req, co::Coroutine *) -> absl::Status {
        std::cerr << "VoidErrorMethod called with request: "
                  << req.DebugString() << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  // Stream method.
  s = server->RegisterMethod(
      "StreamMethod", "subspace.TestRequest", "subspace.TestResponse", 256, 10,
      [](const google::protobuf::Any &req,
         subspace::internal::AnyStreamWriter &writer,
         co::Coroutine *c) -> absl::Status {
        rpc::TestRequest test;
        if (!req.UnpackTo(&test)) {
          return absl::InvalidArgumentError("Failed to unpack request");
        }
        std::cerr << "StreamMethod called with request: " << req.DebugString()
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
          auto res = std::make_unique<google::protobuf::Any>();
          res->PackFrom(r);
          writer.Write(std::move(res), c);
          c->Millisleep(test.stream_period());
        }

        writer.Finish(c);
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  return server;
}

struct ServerContext {
  std::shared_ptr<subspace::Client> client;
  std::shared_ptr<subspace::Publisher> pub;
  std::shared_ptr<subspace::Subscriber> sub;
  uint64_t client_id;
  int session_id;
};

static uint64_t next_client_id = 1;
static int next_request_id = 1;

static void Open(std::shared_ptr<subspace::RpcServer> server,
                 subspace::RpcServerResponse &response, ServerContext &ctx,
                 co::Coroutine *c) {
  subspace::RpcServerRequest req;
  ctx.client_id = next_client_id++;
  req.set_client_id(ctx.client_id);

  int request_id = next_request_id++;
  req.set_request_id(request_id);
  req.mutable_open();

  subspace::Client client;
  ServerTest::InitClient(client);
  ctx.client = std::make_shared<subspace::Client>(std::move(client));

  auto pub = ctx.client->CreatePublisher(
      "/rpc/TestService/request", 1024, 10,
      {.reliable = true, .type = "subspace.RpcServerRequest"});
  ASSERT_OK(pub);

  ctx.pub = std::make_shared<subspace::Publisher>(std::move(*pub));
  auto sub = ctx.client->CreateSubscriber(
      "/rpc/TestService/response",
      {.reliable = true, .type = "subspace.RpcServerResponse"});
  ASSERT_OK(sub);

  ctx.sub = std::make_shared<subspace::Subscriber>(std::move(*sub));

  auto buffer = ctx.pub->GetMessageBuffer(1024);
  ASSERT_OK(buffer);
  ASSERT_NE(nullptr, *buffer);
  ASSERT_TRUE(req.SerializeToArray(*buffer, 1024));
  ASSERT_OK(ctx.pub->PublishMessage(int(req.ByteSizeLong())));

  subspace::Message msg;
  for (;;) {
    ASSERT_OK(ctx.sub->Wait(c));
    for (;;) {
      auto m = ctx.sub->ReadMessage();
      ASSERT_OK(m);
      if (m->length > 0) {
        msg = std::move(*m);
        ASSERT_TRUE(response.ParseFromArray(msg.buffer, msg.length));
        if (response.client_id() == ctx.client_id &&
            response.request_id() == request_id && response.has_open()) {
          ctx.session_id = response.open().session_id();
          return;
        }
      }
    }
  }
}

static void Close(std::shared_ptr<subspace::RpcServer> server,
                  ServerContext &ctx, co::Coroutine *c) {
  subspace::RpcServerRequest req;
  req.set_client_id(ctx.client_id);

  int request_id = next_request_id++;
  req.set_request_id(request_id);
  req.mutable_close()->set_session_id(ctx.session_id);

  auto buffer = ctx.pub->GetMessageBuffer(256);
  ASSERT_OK(buffer);
  ASSERT_TRUE(req.SerializeToArray(*buffer, 1024));
  ASSERT_OK(ctx.pub->PublishMessage(int(req.ByteSizeLong())));

  subspace::Message msg;
  for (;;) {
    std::cerr << "waiting for close\n";
    ASSERT_OK(ctx.sub->Wait(c));
    for (;;) {
      auto m = ctx.sub->ReadMessage();
      ASSERT_OK(m);
      if (m->length > 0) {
        msg = std::move(*m);
        subspace::RpcServerResponse response;
        ASSERT_TRUE(response.ParseFromArray(msg.buffer, msg.length));
        if (response.client_id() == ctx.client_id &&
            response.request_id() == request_id && response.has_close()) {
          std::cerr << "Received close response: " << response.DebugString()
                    << std::endl;
          return;
        }
      }
    }
  }
}

static const subspace::RpcOpenResponse::Method *
FindMethod(const subspace::RpcOpenResponse &response, const std::string &name) {
  for (auto &method : response.methods()) {
    if (method.name() == name) {
      return &method;
    }
  }
  return nullptr;
}

TEST_F(ServerTest, Open) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    ServerContext ctx;
    subspace::RpcServerResponse response;
    Open(server, response, ctx, c);
    std::cerr << "Received response: " << response.DebugString() << std::endl;

    Close(server, ctx, c);

    server->Stop();
  });

  scheduler.Run();
  std::cerr << "Server stopped" << std::endl;
  std::cerr << "Server use count: " << server.use_count() << std::endl;
}

TEST_F(ServerTest, OpenAndCallNoResponseRead) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    ServerContext ctx;
    subspace::RpcServerResponse response;
    Open(server, response, ctx, c);
    std::cerr << "Received response: " << response.DebugString() << std::endl;

    // Find the TestRequest method in the response.
    auto *method = FindMethod(response.open(), "TestMethod");
    ASSERT_NE(method, nullptr);

    subspace::RpcRequest req;
    req.set_client_id(ctx.client_id);
    req.set_session_id(response.open().session_id());
    int request_id = next_request_id++;
    req.set_request_id(request_id);
    req.set_method(method->id());
    auto *arg = req.mutable_argument();

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");
    arg->PackFrom(test_request);

    subspace::Client client;
    InitClient(client);

    std::cerr << "creating pub " << method->request_channel().name()
              << std::endl;
    auto pub = client.CreatePublisher(
        method->request_channel().name(), method->request_channel().slot_size(),
        method->request_channel().num_slots(),
        {.reliable = true, .type = method->request_channel().type()});
    ASSERT_OK(pub);

    std::cerr << "publishing " << req.DebugString();
    std::cerr << "request channel: " << method->request_channel().name()
              << std::endl;
    // This is a reliable publisher but we are guaranteed to be able to send.
    auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
    ASSERT_OK(buffer);
    ASSERT_NE(nullptr, *buffer);
    ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
    ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));
    Close(server, ctx, c);
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ServerTest, OpenAndCall) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    ServerContext ctx;
    subspace::RpcServerResponse response;
    Open(server, response, ctx, c);
    std::cerr << "Received response: " << response.DebugString() << std::endl;

    // Find the TestRequest method in the response.
    auto *method = FindMethod(response.open(), "TestMethod");
    ASSERT_NE(method, nullptr);

    subspace::RpcRequest req;
    req.set_client_id(ctx.client_id);
    req.set_session_id(response.open().session_id());
    int request_id = next_request_id++;
    req.set_request_id(request_id);
    req.set_method(method->id());
    auto *arg = req.mutable_argument();

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");
    arg->PackFrom(test_request);

    subspace::Client client;
    InitClient(client);

    auto pub = client.CreatePublisher(
        method->request_channel().name(), method->request_channel().slot_size(),
        method->request_channel().num_slots(),
        {.reliable = true, .type = method->request_channel().type()});
    ASSERT_OK(pub);

    std::cerr << "subscribing to " << method->response_channel().name()
              << std::endl;
    auto sub = client.CreateSubscriber(
        method->response_channel().name(),
        {.reliable = true, .type = method->response_channel().type()});
    ASSERT_OK(sub);

    std::cerr << "publishing " << req.DebugString();
    // This is a reliable publisher but we are guaranteed to be able to send.
    auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
    ASSERT_OK(buffer);
    ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
    ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

    subspace::Message msg;
    bool done = false;
    while (!done) {
      ASSERT_OK(sub->Wait(c));
      for (;;) {
        auto m = sub->ReadMessage();
        ASSERT_OK(m);
        std::cerr << "message length: " << m->length << std::endl;
        if (m->length > 0) {
          msg = std::move(*m);
          subspace::RpcResponse rpc_response;
          ASSERT_TRUE(rpc_response.ParseFromArray(msg.buffer, msg.length));
          std::cerr << "received message " << rpc_response.DebugString()
                    << std::endl;
          if (rpc_response.client_id() == ctx.client_id &&
              rpc_response.request_id() == request_id) {
            std::cerr << "Received RPC response: " << rpc_response.DebugString()
                      << std::endl;

            rpc::TestResponse response_message;
            rpc_response.result().UnpackTo(&response_message);
            std::cerr << "Received RPC result: "
                      << response_message.DebugString() << std::endl;
            done = true;
            break;
          }
          continue;
        }
        break;
      }
    }

    Close(server, ctx, c);

    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ServerTest, CallVoidMethod) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    ServerContext ctx;
    subspace::RpcServerResponse response;
    Open(server, response, ctx, c);
    std::cerr << "Received response: " << response.DebugString() << std::endl;

    // Find the TestRequest method in the response.
    auto *method = FindMethod(response.open(), "VoidMethod");
    ASSERT_NE(method, nullptr);

    subspace::RpcRequest req;
    req.set_client_id(ctx.client_id);
    req.set_session_id(response.open().session_id());
    int request_id = next_request_id++;
    req.set_request_id(request_id);
    req.set_method(method->id());
    auto *arg = req.mutable_argument();

    rpc::TestRequest test_request;
    test_request.set_message("Hello, void world!");
    arg->PackFrom(test_request);

    subspace::Client client;
    InitClient(client);

    auto pub = client.CreatePublisher(
        method->request_channel().name(), method->request_channel().slot_size(),
        method->request_channel().num_slots(),
        {.reliable = true, .type = method->request_channel().type()});
    ASSERT_OK(pub);

    auto sub = client.CreateSubscriber(
        method->response_channel().name(),
        {.reliable = true, .type = method->response_channel().type()});
    ASSERT_OK(sub);

    std::cerr << "publishing " << req.DebugString();
    std::cerr << "request channel: " << method->request_channel().name()
              << std::endl;
    // This is a reliable publisher but we are guaranteed to be able to send.
    auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
    ASSERT_OK(buffer);
    ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
    ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));
    bool done = false;
    subspace::Message msg;

    while (!done) {
      ASSERT_OK(sub->Wait(c));
      for (;;) {
        auto m = sub->ReadMessage();
        ASSERT_OK(m);
        if (m->length > 0) {
          msg = std::move(*m);
          subspace::RpcResponse rpc_response;
          ASSERT_TRUE(rpc_response.ParseFromArray(msg.buffer, msg.length));
          std::cerr << "received message " << rpc_response.DebugString() << " "
                    << rpc_response.client_id() << " " << ctx.client_id
                    << std::endl;
          if (rpc_response.client_id() == ctx.client_id &&
              rpc_response.request_id() == request_id) {
            std::cerr << "Received RPC response: " << rpc_response.DebugString()
                      << std::endl;

            rpc::TestResponse response_message;
            rpc_response.result().UnpackTo(&response_message);
            std::cerr << "Received RPC result: "
                      << response_message.DebugString() << std::endl;
            done = true;
            break;
          }
          continue;
        }
        break;
      }
    }
    Close(server, ctx, c);
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ServerTest, CallError) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    ServerContext ctx;
    subspace::RpcServerResponse response;
    Open(server, response, ctx, c);
    std::cerr << "Received response: " << response.DebugString() << std::endl;

    // Find the TestRequest method in the response.
    auto *method = FindMethod(response.open(), "ErrorMethod");
    ASSERT_NE(method, nullptr);

    subspace::RpcRequest req;
    req.set_client_id(ctx.client_id);
    req.set_session_id(response.open().session_id());
    int request_id = next_request_id++;
    req.set_request_id(request_id);
    req.set_method(method->id());
    auto *arg = req.mutable_argument();

    rpc::TestRequest test_request;
    test_request.set_message("Hello, error world!");
    arg->PackFrom(test_request);

    subspace::Client client;
    InitClient(client);

    auto pub = client.CreatePublisher(
        method->request_channel().name(), method->request_channel().slot_size(),
        method->request_channel().num_slots(),
        {.reliable = true, .type = method->request_channel().type()});
    ASSERT_OK(pub);

    auto sub = client.CreateSubscriber(
        method->response_channel().name(),
        {.reliable = true, .type = method->response_channel().type()});
    ASSERT_OK(sub);

    std::cerr << "publishing " << req.DebugString();
    std::cerr << "request channel: " << method->request_channel().name()
              << std::endl;
    // This is a reliable publisher but we are guaranteed to be able to send.
    auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
    ASSERT_OK(buffer);
    ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
    ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

    subspace::Message msg;

    bool done = false;
    while (!done) {
      ASSERT_OK(sub->Wait(c));
      for (;;) {
        auto m = sub->ReadMessage();
        ASSERT_OK(m);
        if (m->length > 0) {
          msg = std::move(*m);
          subspace::RpcResponse rpc_response;
          ASSERT_TRUE(rpc_response.ParseFromArray(msg.buffer, msg.length));
          if (rpc_response.client_id() == ctx.client_id &&
              rpc_response.request_id() == request_id) {
            std::cerr << "Received RPC response: " << rpc_response.DebugString()
                      << std::endl;

            rpc::TestResponse response_message;
            rpc_response.result().UnpackTo(&response_message);
            std::cerr << "Received RPC result: "
                      << response_message.DebugString() << std::endl;
            EXPECT_EQ(
                "Error executing method ErrorMethod: INTERNAL: Error occurred",
                rpc_response.error());
            done = true;
            break;
          }
          continue;
        }
        break;
      }
    }
    Close(server, ctx, c);
    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ServerTest, Stream) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    ServerContext ctx;
    subspace::RpcServerResponse response;
    Open(server, response, ctx, c);
    std::cerr << "Received response: " << response.DebugString() << std::endl;

    // Find the StreamMethod in the response.
    auto *method = FindMethod(response.open(), "StreamMethod");
    ASSERT_NE(method, nullptr);

    subspace::RpcRequest req;
    req.set_client_id(ctx.client_id);
    req.set_session_id(response.open().session_id());
    int request_id = next_request_id++;
    req.set_request_id(request_id);
    req.set_method(method->id());
    auto *arg = req.mutable_argument();

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");
    arg->PackFrom(test_request);

    subspace::Client client;
    InitClient(client);

    auto pub = client.CreatePublisher(
        method->request_channel().name(), method->request_channel().slot_size(),
        method->request_channel().num_slots(),
        {.reliable = true, .type = method->request_channel().type()});
    ASSERT_OK(pub);

    std::cerr << "subscribing to " << method->response_channel().name()
              << std::endl;
    auto sub = client.CreateSubscriber(
        method->response_channel().name(),
        {.reliable = true, .type = method->response_channel().type()});
    ASSERT_OK(sub);

    std::cerr << "publishing " << req.DebugString();
    // This is a reliable publisher but we are guaranteed to be able to send.
    auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
    ASSERT_OK(buffer);
    ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
    ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

    bool done = false;
    int num_results = 0;
    while (!done) {
      ASSERT_OK(sub->Wait(c));
      for (;;) {
        auto m = sub->ReadMessage();
        ASSERT_OK(m);
        std::cerr << "message length: " << m->length << std::endl;
        if (m->length > 0) {
          subspace::Message msg = std::move(*m);
          subspace::RpcResponse rpc_response;
          ASSERT_TRUE(rpc_response.ParseFromArray(msg.buffer, msg.length));
          std::cerr << "received message " << rpc_response.DebugString()
                    << std::endl;
          if (rpc_response.client_id() == ctx.client_id &&
              rpc_response.request_id() == request_id) {
            std::cerr << "Received RPC response: " << rpc_response.DebugString()
                      << std::endl;

            rpc::TestResponse response_message;
            rpc_response.result().UnpackTo(&response_message);
            std::cerr << "Received RPC result: "
                      << response_message.DebugString() << std::endl;
            num_results++;
            if (rpc_response.is_last()) {
              done = true;
              break;
            }
          }
          continue;
        }
        break;
      }
    }

    ASSERT_EQ(201, num_results);
    Close(server, ctx, c);

    server->Stop();
  });

  scheduler.Run();
}

TEST_F(ServerTest, CancelStream) {
  co::CoroutineScheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  co::Coroutine test(scheduler, [&](co::Coroutine *c) {
    ServerContext ctx;
    subspace::RpcServerResponse response;
    Open(server, response, ctx, c);
    std::cerr << "Received response: " << response.DebugString() << std::endl;

    // Find the StreamMethod in the response.
    auto *method = FindMethod(response.open(), "StreamMethod");
    ASSERT_NE(method, nullptr);

    subspace::RpcRequest req;
    req.set_client_id(ctx.client_id);
    req.set_session_id(response.open().session_id());
    int request_id = next_request_id++;
    req.set_request_id(request_id);
    req.set_method(method->id());
    auto *arg = req.mutable_argument();

    rpc::TestRequest test_request;
    test_request.set_message("Hello, world!");
    test_request.set_stream_period(100);
    arg->PackFrom(test_request);

    subspace::Client client;
    InitClient(client);

    auto pub = client.CreatePublisher(
        method->request_channel().name(), method->request_channel().slot_size(),
        method->request_channel().num_slots(),
        {.reliable = true, .type = method->request_channel().type()});
    ASSERT_OK(pub);

    auto cancel_pub = client.CreatePublisher(method->cancel_channel(), 64, 10,
                                             {.reliable = true});
    ASSERT_OK(cancel_pub);

    std::cerr << "subscribing to " << method->response_channel().name()
              << std::endl;
    auto sub = client.CreateSubscriber(
        method->response_channel().name(),
        {.reliable = true, .type = method->response_channel().type()});
    ASSERT_OK(sub);

    std::cerr << "publishing " << req.DebugString();
    // This is a reliable publisher but we are guaranteed to be able to send.
    auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
    ASSERT_OK(buffer);
    ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
    ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

    bool done = false;
    int num_results = 0;
    while (!done) {
      ASSERT_OK(sub->Wait(c));
      for (;;) {
        auto m = sub->ReadMessage();
        ASSERT_OK(m);
        std::cerr << "message length: " << m->length << std::endl;
        if (m->length > 0) {
          subspace::Message msg = std::move(*m);
          subspace::RpcResponse rpc_response;
          ASSERT_TRUE(rpc_response.ParseFromArray(msg.buffer, msg.length));
          std::cerr << "received message " << rpc_response.DebugString()
                    << std::endl;
          if (rpc_response.client_id() == ctx.client_id &&
              rpc_response.request_id() == request_id) {
            std::cerr << "Received RPC response: " << rpc_response.DebugString()
                      << std::endl;

            rpc::TestResponse response_message;
            rpc_response.result().UnpackTo(&response_message);
            std::cerr << "Received RPC result: "
                      << response_message.DebugString() << std::endl;
            num_results++;
            if (num_results == 5) {
              // Send a cancel message
              auto buffer = cancel_pub->GetMessageBuffer(64);
              ASSERT_OK(buffer);
              subspace::RpcCancelRequest cancel;
              cancel.set_client_id(ctx.client_id);
              cancel.set_request_id(request_id);
              cancel.set_session_id(response.open().session_id());
              ASSERT_TRUE(
                  cancel.SerializeToArray(*buffer, cancel.ByteSizeLong()));
              ASSERT_OK(cancel_pub->PublishMessage(int(cancel.ByteSizeLong())));
            }
            if (rpc_response.is_cancelled()) {
              done = true;
              break;
            }
          }
          continue;
        }
        break;
      }
    }

    ASSERT_EQ(6, num_results);
    Close(server, ctx, c);

    server->Stop();
  });

  scheduler.Run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
