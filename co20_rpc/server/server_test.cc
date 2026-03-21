// Copyright 2023-2026 David Allison
// co20 RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "co/coroutine_cpp20.h"
#include "co20_rpc/server/rpc_server.h"
#include "client/test_fixture.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include <gtest/gtest.h>
#include <memory>

#define CO_EXPECT_OK(e)                                                        \
  do {                                                                         \
    const auto &_co_s = (e);                                                   \
    EXPECT_TRUE(_co_s.ok()) << "Expected OK: " #e;                            \
    if (!_co_s.ok())                                                           \
      co_return;                                                               \
  } while (0)

#define CO_EXPECT_NE(a, b)                                                     \
  do {                                                                         \
    EXPECT_NE(a, b);                                                           \
    if ((a) == (b))                                                            \
      co_return;                                                               \
  } while (0)

#define CO_EXPECT_TRUE(cond)                                                   \
  do {                                                                         \
    EXPECT_TRUE(cond);                                                         \
    if (!(cond))                                                               \
      co_return;                                                               \
  } while (0)

class Co20ServerTest : public SubspaceTestBase {};

static int next_session = 1;
static std::shared_ptr<subspace::co20_rpc::RpcServer> BuildServer() {
  auto server = std::make_shared<subspace::co20_rpc::RpcServer>(
      "Co20TestService", Co20ServerTest::Socket());

  server->SetLogLevel("debug");
  server->SetStartingSessionId(next_session++);

  auto s = server->RegisterMethod(
      "TestMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req,
         google::protobuf::Any *res) -> co20::ValueTask<absl::Status> {
        rpc::TestResponse r;
        r.set_message("Hello from TestMethod");
        res->PackFrom(r);
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "VoidMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &)
          -> co20::ValueTask<absl::Status> {
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "ErrorMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &,
         google::protobuf::Any *) -> co20::ValueTask<absl::Status> {
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "VoidErrorMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &)
          -> co20::ValueTask<absl::Status> {
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "StreamMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req,
         subspace::co20_rpc::internal::AnyStreamWriter &writer)
          -> co20::ValueTask<absl::Status> {
        rpc::TestRequest test;
        if (!req.UnpackTo(&test)) {
          co_return absl::InvalidArgumentError("Failed to unpack request");
        }
        constexpr int kNumResults = 200;
        for (int i = 0; i < kNumResults; i++) {
          if (writer.IsCancelled()) {
            co_await writer.Finish();
            co_return absl::OkStatus();
          }
          rpc::TestResponse r;
          r.set_message("Hello from StreamMethod part " + std::to_string(i));
          auto res = std::make_unique<google::protobuf::Any>();
          res->PackFrom(r);
          co_await writer.Write(std::move(res));
          if (test.stream_period() > 0) {
            co_await co20::Sleep(
                std::chrono::milliseconds(test.stream_period()));
          }
        }

        co_await writer.Finish();
        co_return absl::OkStatus();
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

static co20::ValueTask<void>
Open([[maybe_unused]] std::shared_ptr<subspace::co20_rpc::RpcServer> server,
     subspace::RpcServerResponse &response, ServerContext &ctx) {
  subspace::RpcServerRequest req;
  ctx.client_id = next_client_id++;
  req.set_client_id(ctx.client_id);

  int request_id = next_request_id++;
  req.set_request_id(request_id);
  req.mutable_open();

  subspace::Client client;
  client.SetThreadSafe(true);
  CO_EXPECT_OK(client.Init(Co20ServerTest::Socket()));
  ctx.client = std::make_shared<subspace::Client>(std::move(client));

  auto pub = ctx.client->CreatePublisher(
      "/rpc/Co20TestService/request", 1024, 10,
      {.reliable = true, .type = "subspace.RpcServerRequest"});
  CO_EXPECT_OK(pub);

  ctx.pub = std::make_shared<subspace::Publisher>(std::move(*pub));
  auto sub = ctx.client->CreateSubscriber(
      "/rpc/Co20TestService/response",
      {.reliable = true, .type = "subspace.RpcServerResponse"});
  CO_EXPECT_OK(sub);

  ctx.sub = std::make_shared<subspace::Subscriber>(std::move(*sub));

  auto buffer = ctx.pub->GetMessageBuffer(1024);
  CO_EXPECT_OK(buffer);
  CO_EXPECT_NE(nullptr, *buffer);
  CO_EXPECT_TRUE(req.SerializeToArray(*buffer, 1024));
  CO_EXPECT_OK(ctx.pub->PublishMessage(int(req.ByteSizeLong())));

  subspace::Message msg;
  for (;;) {
    int sub_fd = ctx.sub->GetPollFd().fd;
    co_await co20::Wait(sub_fd, POLLIN);
    for (;;) {
      auto m = ctx.sub->ReadMessage();
      CO_EXPECT_OK(m);
      if (m->length > 0) {
        msg = std::move(*m);
        CO_EXPECT_TRUE(response.ParseFromArray(msg.buffer, msg.length));
        if (response.client_id() == ctx.client_id &&
            response.request_id() == request_id && response.has_open()) {
          ctx.session_id = response.open().session_id();
          co_return;
        }
        continue;
      }
      break;
    }
  }
}

static co20::ValueTask<void>
Close([[maybe_unused]] std::shared_ptr<subspace::co20_rpc::RpcServer> server,
      ServerContext &ctx) {
  subspace::RpcServerRequest req;
  req.set_client_id(ctx.client_id);

  int request_id = next_request_id++;
  req.set_request_id(request_id);
  req.mutable_close()->set_session_id(ctx.session_id);

  auto buffer = ctx.pub->GetMessageBuffer(256);
  CO_EXPECT_OK(buffer);
  CO_EXPECT_TRUE(req.SerializeToArray(*buffer, 1024));
  CO_EXPECT_OK(ctx.pub->PublishMessage(int(req.ByteSizeLong())));

  subspace::Message msg;
  for (;;) {
    int sub_fd = ctx.sub->GetPollFd().fd;
    co_await co20::Wait(sub_fd, POLLIN);
    for (;;) {
      auto m = ctx.sub->ReadMessage();
      CO_EXPECT_OK(m);
      if (m->length > 0) {
        msg = std::move(*m);
        subspace::RpcServerResponse response;
        CO_EXPECT_TRUE(response.ParseFromArray(msg.buffer, msg.length));
        if (response.client_id() == ctx.client_id &&
            response.request_id() == request_id && response.has_close()) {
          co_return;
        }
        continue;
      }
      break;
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

TEST_F(Co20ServerTest, Open) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        co_await Open(server, response, ctx);

        co_await Close(server, ctx);
        server->Stop();
        co_return;
      },
      "test_open");

  scheduler.Run();
}

TEST_F(Co20ServerTest, OpenAndCall) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        co_await Open(server, response, ctx);

        auto *method = FindMethod(response.open(), "TestMethod");
        CO_EXPECT_NE(method, nullptr);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        CO_EXPECT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        CO_EXPECT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        CO_EXPECT_OK(buffer);
        CO_EXPECT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        CO_EXPECT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          co_await co20::Wait(sub_fd, POLLIN);
          for (;;) {
            auto m = sub->ReadMessage();
            CO_EXPECT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              CO_EXPECT_TRUE(
                  rpc_response.ParseFromArray(msg.buffer, msg.length));
              if (rpc_response.client_id() == ctx.client_id &&
                  rpc_response.request_id() == request_id) {
                rpc::TestResponse response_message;
                rpc_response.result().UnpackTo(&response_message);
                EXPECT_EQ("Hello from TestMethod", response_message.message());
                done = true;
                break;
              }
              continue;
            }
            break;
          }
        }

        co_await Close(server, ctx);
        server->Stop();
        co_return;
      },
      "test_open_and_call");

  scheduler.Run();
}

TEST_F(Co20ServerTest, CallVoidMethod) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        co_await Open(server, response, ctx);

        auto *method = FindMethod(response.open(), "VoidMethod");
        CO_EXPECT_NE(method, nullptr);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        CO_EXPECT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        CO_EXPECT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        CO_EXPECT_OK(buffer);
        CO_EXPECT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        CO_EXPECT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          co_await co20::Wait(sub_fd, POLLIN);
          for (;;) {
            auto m = sub->ReadMessage();
            CO_EXPECT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              CO_EXPECT_TRUE(
                  rpc_response.ParseFromArray(msg.buffer, msg.length));
              if (rpc_response.client_id() == ctx.client_id &&
                  rpc_response.request_id() == request_id) {
                done = true;
                break;
              }
              continue;
            }
            break;
          }
        }
        co_await Close(server, ctx);
        server->Stop();
        co_return;
      },
      "test_call_void");

  scheduler.Run();
}

TEST_F(Co20ServerTest, CallError) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        co_await Open(server, response, ctx);

        auto *method = FindMethod(response.open(), "ErrorMethod");
        CO_EXPECT_NE(method, nullptr);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        CO_EXPECT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        CO_EXPECT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        CO_EXPECT_OK(buffer);
        CO_EXPECT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        CO_EXPECT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          co_await co20::Wait(sub_fd, POLLIN);
          for (;;) {
            auto m = sub->ReadMessage();
            CO_EXPECT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              CO_EXPECT_TRUE(
                  rpc_response.ParseFromArray(msg.buffer, msg.length));
              if (rpc_response.client_id() == ctx.client_id &&
                  rpc_response.request_id() == request_id) {
                EXPECT_EQ(
                    "Error executing method ErrorMethod: INTERNAL: Error "
                    "occurred",
                    rpc_response.error());
                done = true;
                break;
              }
              continue;
            }
            break;
          }
        }
        co_await Close(server, ctx);
        server->Stop();
        co_return;
      },
      "test_call_error");

  scheduler.Run();
}

TEST_F(Co20ServerTest, Stream) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        co_await Open(server, response, ctx);

        auto *method = FindMethod(response.open(), "StreamMethod");
        CO_EXPECT_NE(method, nullptr);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        CO_EXPECT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        CO_EXPECT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        CO_EXPECT_OK(buffer);
        CO_EXPECT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        CO_EXPECT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        int num_results = 0;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          co_await co20::Wait(sub_fd, POLLIN);
          for (;;) {
            auto m = sub->ReadMessage();
            CO_EXPECT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              CO_EXPECT_TRUE(
                  rpc_response.ParseFromArray(msg.buffer, msg.length));
              if (rpc_response.client_id() == ctx.client_id &&
                  rpc_response.request_id() == request_id) {
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

        EXPECT_EQ(201, num_results);
        co_await Close(server, ctx);
        server->Stop();
        co_return;
      },
      "test_stream");

  scheduler.Run();
}

TEST_F(Co20ServerTest, CancelStream) {
  co20::Scheduler scheduler;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&scheduler));

  scheduler.Spawn(
      [&](co20::Coroutine &) -> co20::Task {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        co_await Open(server, response, ctx);

        auto *method = FindMethod(response.open(), "StreamMethod");
        CO_EXPECT_NE(method, nullptr);

        subspace::RpcRequest req;
        req.set_client_id(ctx.client_id);
        req.set_session_id(response.open().session_id());
        int request_id = next_request_id++;
        req.set_request_id(request_id);
        req.set_method(method->id());
        auto *arg = req.mutable_argument();

        rpc::TestRequest test_request;
        test_request.set_message("Hello, world!");
        test_request.set_stream_period(500);
        arg->PackFrom(test_request);

        subspace::Client client;
        InitClient(client);

        auto pub = client.CreatePublisher(
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        CO_EXPECT_OK(pub);

        auto cancel_pub = client.CreatePublisher(
            method->cancel_channel(), 64, 10, {.reliable = true});
        CO_EXPECT_OK(cancel_pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        CO_EXPECT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        CO_EXPECT_OK(buffer);
        CO_EXPECT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        CO_EXPECT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        int num_results = 0;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          co_await co20::Wait(sub_fd, POLLIN);
          for (;;) {
            auto m = sub->ReadMessage();
            CO_EXPECT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              CO_EXPECT_TRUE(
                  rpc_response.ParseFromArray(msg.buffer, msg.length));
              if (rpc_response.client_id() == ctx.client_id &&
                  rpc_response.request_id() == request_id) {
                num_results++;
                if (num_results == 5) {
                  auto cbuf = cancel_pub->GetMessageBuffer(64);
                  CO_EXPECT_OK(cbuf);
                  subspace::RpcCancelRequest cancel;
                  cancel.set_client_id(ctx.client_id);
                  cancel.set_request_id(request_id);
                  cancel.set_session_id(response.open().session_id());
                  CO_EXPECT_TRUE(
                      cancel.SerializeToArray(*cbuf, cancel.ByteSizeLong()));
                  CO_EXPECT_OK(
                      cancel_pub->PublishMessage(int(cancel.ByteSizeLong())));
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

        // At least 6 (5 before cancel + cancel ack); a few extra may arrive
        // due to scheduling latency between the cancel send and processing.
        EXPECT_GE(num_results, 6);
        co_await Close(server, ctx);
        server->Stop();
        co_return;
      },
      "test_cancel_stream");

  scheduler.Run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
