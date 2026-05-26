// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "coro_rpc/common/async_wait.h"
#include "coro_rpc/server/rpc_server.h"
#include "client/test_fixture.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include <gtest/gtest.h>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>

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

class CoroServerTest : public SubspaceTestBase {};

static int next_session = 1;
static std::shared_ptr<subspace::coro_rpc::RpcServer> BuildServer() {
  auto server = std::make_shared<subspace::coro_rpc::RpcServer>(
      "CoroTestService", CoroServerTest::Socket());

  server->SetLogLevel("debug");
  server->SetStartingSessionId(next_session++);

  auto s = server->RegisterMethod(
      "TestMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req,
         google::protobuf::Any *res) -> boost::asio::awaitable<absl::Status> {
        std::cerr << "TestMethod called with request: " << req.DebugString()
                  << std::endl;
        rpc::TestResponse r;
        r.set_message("Hello from TestMethod");
        res->PackFrom(r);
        co_return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "VoidMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req)
          -> boost::asio::awaitable<absl::Status> {
        std::cerr << "VoidMethod called with request: " << req.DebugString()
                  << std::endl;
        co_return absl::OkStatus();
      });

  s = server->RegisterMethod(
      "ErrorMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req,
         google::protobuf::Any *) -> boost::asio::awaitable<absl::Status> {
        std::cerr << "ErrorMethod called with request: " << req.DebugString()
                  << std::endl;
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "VoidErrorMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req)
          -> boost::asio::awaitable<absl::Status> {
        std::cerr << "VoidErrorMethod called with request: "
                  << req.DebugString() << std::endl;
        co_return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "StreamMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req,
         subspace::coro_rpc::internal::AnyStreamWriter &writer)
          -> boost::asio::awaitable<absl::Status> {
        rpc::TestRequest test;
        if (!req.UnpackTo(&test)) {
          co_return absl::InvalidArgumentError("Failed to unpack request");
        }
        std::cerr << "StreamMethod called with request: " << req.DebugString()
                  << std::endl;
        auto executor = co_await boost::asio::this_coro::executor;
        constexpr int kNumResults = 200;
        for (int i = 0; i < kNumResults; i++) {
          if (writer.IsCancelled()) {
            std::cerr << "StreamMethod cancelled after " << i << " responses"
                      << std::endl;
            co_await writer.Finish();
            co_return absl::OkStatus();
          }
          rpc::TestResponse r;
          r.set_message("Hello from StreamMethod part " + std::to_string(i));
          auto res = std::make_unique<google::protobuf::Any>();
          res->PackFrom(r);
          co_await writer.Write(std::move(res));
          boost::asio::steady_timer timer(executor,
                                          std::chrono::milliseconds(
                                              test.stream_period()));
          co_await timer.async_wait(boost::asio::use_awaitable);
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

static boost::asio::awaitable<void>
Open([[maybe_unused]] std::shared_ptr<subspace::coro_rpc::RpcServer> server,
     subspace::RpcServerResponse &response, ServerContext &ctx) {
  subspace::RpcServerRequest req;
  ctx.client_id = next_client_id++;
  req.set_client_id(ctx.client_id);

  int request_id = next_request_id++;
  req.set_request_id(request_id);
  req.mutable_open();

  subspace::Client client;
  client.SetThreadSafe(true);
  CO_EXPECT_OK(client.Init(CoroServerTest::Socket()));
  ctx.client = std::make_shared<subspace::Client>(std::move(client));

  auto pub = ctx.client->CreatePublisher(
      "/rpc/CoroTestService/request", 1024, 10,
      {.reliable = true, .type = "subspace.RpcServerRequest"});
  CO_EXPECT_OK(pub);

  ctx.pub = std::make_shared<subspace::Publisher>(std::move(*pub));
  auto sub = ctx.client->CreateSubscriber(
      "/rpc/CoroTestService/response",
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
    CO_EXPECT_OK(co_await subspace::coro_rpc::async_wait_readable(sub_fd));
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

static boost::asio::awaitable<void>
Close([[maybe_unused]] std::shared_ptr<subspace::coro_rpc::RpcServer> server,
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
    CO_EXPECT_OK(co_await subspace::coro_rpc::async_wait_readable(sub_fd));
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

TEST_F(CoroServerTest, Open) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        co_await Open(server, response, ctx);
        std::cerr << "Received response: " << response.DebugString()
                  << std::endl;

        co_await Close(server, ctx);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroServerTest, OpenAndCall) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
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
          CO_EXPECT_OK(
              co_await subspace::coro_rpc::async_wait_readable(sub_fd));
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
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroServerTest, CallVoidMethod) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
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
          CO_EXPECT_OK(
              co_await subspace::coro_rpc::async_wait_readable(sub_fd));
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
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroServerTest, CallError) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
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
          CO_EXPECT_OK(
              co_await subspace::coro_rpc::async_wait_readable(sub_fd));
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
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroServerTest, Stream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
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
          CO_EXPECT_OK(
              co_await subspace::coro_rpc::async_wait_readable(sub_fd));
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
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(CoroServerTest, CancelStream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::co_spawn(
      ioc,
      [&]() -> boost::asio::awaitable<void> {
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
        test_request.set_stream_period(100);
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
          CO_EXPECT_OK(
              co_await subspace::coro_rpc::async_wait_readable(sub_fd));
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
                  auto buffer = cancel_pub->GetMessageBuffer(64);
                  CO_EXPECT_OK(buffer);
                  subspace::RpcCancelRequest cancel;
                  cancel.set_client_id(ctx.client_id);
                  cancel.set_request_id(request_id);
                  cancel.set_session_id(response.open().session_id());
                  CO_EXPECT_TRUE(
                      cancel.SerializeToArray(*buffer, cancel.ByteSizeLong()));
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

        EXPECT_EQ(6, num_results);
        co_await Close(server, ctx);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  return RUN_ALL_TESTS();
}
