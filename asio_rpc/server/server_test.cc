// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/flags/parse.h"
#include "asio_rpc/common/async_wait.h"
#include "asio_rpc/server/rpc_server.h"
#include "client/test_fixture.h"
#include "google/protobuf/any.pb.h"
#include "rpc/proto/rpc_test.pb.h"
#include <gtest/gtest.h>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

class AsioServerTest : public SubspaceTestBase {};

static int next_session = 1;
static std::shared_ptr<subspace::asio_rpc::RpcServer> BuildServer() {
  auto server = std::make_shared<subspace::asio_rpc::RpcServer>(
      "AsioTestService", AsioServerTest::Socket());

  server->SetLogLevel("debug");
  server->SetStartingSessionId(next_session++);

  auto s = server->RegisterMethod(
      "TestMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req, google::protobuf::Any *res,
         boost::asio::yield_context) -> absl::Status {
        std::cerr << "TestMethod called with request: " << req.DebugString()
                  << std::endl;
        rpc::TestResponse r;
        r.set_message("Hello from TestMethod");
        res->PackFrom(r);
        return absl::OkStatus();
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "VoidMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req,
         boost::asio::yield_context) -> absl::Status {
        std::cerr << "VoidMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::OkStatus();
      });

  s = server->RegisterMethod(
      "ErrorMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req, google::protobuf::Any *,
         boost::asio::yield_context) -> absl::Status {
        std::cerr << "ErrorMethod called with request: " << req.DebugString()
                  << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "VoidErrorMethod", "subspace.TestRequest",
      [](const google::protobuf::Any &req,
         boost::asio::yield_context) -> absl::Status {
        std::cerr << "VoidErrorMethod called with request: "
                  << req.DebugString() << std::endl;
        return absl::InternalError("Error occurred");
      });
  EXPECT_TRUE(s.ok());

  s = server->RegisterMethod(
      "StreamMethod", "subspace.TestRequest", "subspace.TestResponse",
      [](const google::protobuf::Any &req,
         subspace::asio_rpc::internal::AnyStreamWriter &writer,
         boost::asio::yield_context yield) -> absl::Status {
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
            writer.Finish(yield);
            return absl::OkStatus();
          }
          rpc::TestResponse r;
          r.set_message("Hello from StreamMethod part " + std::to_string(i));
          auto res = std::make_unique<google::protobuf::Any>();
          res->PackFrom(r);
          writer.Write(std::move(res), yield);
          boost::asio::steady_timer timer(
              static_cast<boost::asio::io_context &>(
                  yield.get_executor().context()),
              std::chrono::milliseconds(test.stream_period()));
          timer.async_wait(yield);
        }

        writer.Finish(yield);
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

static void Open(std::shared_ptr<subspace::asio_rpc::RpcServer> /*server*/,
                 subspace::RpcServerResponse &response, ServerContext &ctx,
                 boost::asio::yield_context yield) {
  subspace::RpcServerRequest req;
  ctx.client_id = next_client_id++;
  req.set_client_id(ctx.client_id);

  int request_id = next_request_id++;
  req.set_request_id(request_id);
  req.mutable_open();

  subspace::Client client;
  client.SetThreadSafe(true);
  ASSERT_OK(client.Init(AsioServerTest::Socket()));
  ctx.client = std::make_shared<subspace::Client>(std::move(client));

  auto pub = ctx.client->CreatePublisher(
      "/rpc/AsioTestService/request", 1024, 10,
      {.reliable = true, .type = "subspace.RpcServerRequest"});
  ASSERT_OK(pub);

  ctx.pub = std::make_shared<subspace::Publisher>(std::move(*pub));
  auto sub = ctx.client->CreateSubscriber(
      "/rpc/AsioTestService/response",
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
    int sub_fd = ctx.sub->GetPollFd().fd;
    ASSERT_OK(subspace::asio_rpc::async_wait_readable(sub_fd, yield));
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
        continue;
      }
      break;
    }
  }
}

static void Close(std::shared_ptr<subspace::asio_rpc::RpcServer> /*server*/,
                  ServerContext &ctx, boost::asio::yield_context yield) {
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
    int sub_fd = ctx.sub->GetPollFd().fd;
    ASSERT_OK(subspace::asio_rpc::async_wait_readable(sub_fd, yield));
    for (;;) {
      auto m = ctx.sub->ReadMessage();
      ASSERT_OK(m);
      if (m->length > 0) {
        msg = std::move(*m);
        subspace::RpcServerResponse response;
        ASSERT_TRUE(response.ParseFromArray(msg.buffer, msg.length));
        if (response.client_id() == ctx.client_id &&
            response.request_id() == request_id && response.has_close()) {
          return;
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

TEST_F(AsioServerTest, Open) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        Open(server, response, ctx, yield);
        std::cerr << "Received response: " << response.DebugString()
                  << std::endl;

        Close(server, ctx, yield);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioServerTest, OpenAndCall) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        Open(server, response, ctx, yield);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        ASSERT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        ASSERT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        ASSERT_OK(buffer);
        ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          ASSERT_OK(
              subspace::asio_rpc::async_wait_readable(sub_fd, yield));
          for (;;) {
            auto m = sub->ReadMessage();
            ASSERT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              ASSERT_TRUE(
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

        Close(server, ctx, yield);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioServerTest, CallVoidMethod) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        Open(server, response, ctx, yield);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        ASSERT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        ASSERT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        ASSERT_OK(buffer);
        ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          ASSERT_OK(
              subspace::asio_rpc::async_wait_readable(sub_fd, yield));
          for (;;) {
            auto m = sub->ReadMessage();
            ASSERT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              ASSERT_TRUE(
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
        Close(server, ctx, yield);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioServerTest, CallError) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        Open(server, response, ctx, yield);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        ASSERT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        ASSERT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        ASSERT_OK(buffer);
        ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          ASSERT_OK(
              subspace::asio_rpc::async_wait_readable(sub_fd, yield));
          for (;;) {
            auto m = sub->ReadMessage();
            ASSERT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              ASSERT_TRUE(
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
        Close(server, ctx, yield);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioServerTest, Stream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        Open(server, response, ctx, yield);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        ASSERT_OK(pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        ASSERT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        ASSERT_OK(buffer);
        ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        int num_results = 0;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          ASSERT_OK(
              subspace::asio_rpc::async_wait_readable(sub_fd, yield));
          for (;;) {
            auto m = sub->ReadMessage();
            ASSERT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              ASSERT_TRUE(
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

        ASSERT_EQ(201, num_results);
        Close(server, ctx, yield);
        server->Stop();
      },
      boost::asio::detached);

  ioc.run();
}

TEST_F(AsioServerTest, CancelStream) {
  boost::asio::io_context ioc;

  auto server = BuildServer();
  ASSERT_OK(server->Run(&ioc));

  boost::asio::spawn(
      ioc,
      [&](boost::asio::yield_context yield) {
        ServerContext ctx;
        subspace::RpcServerResponse response;
        Open(server, response, ctx, yield);

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
            method->request_channel().name(),
            method->request_channel().slot_size(),
            method->request_channel().num_slots(),
            {.reliable = true, .type = method->request_channel().type()});
        ASSERT_OK(pub);

        auto cancel_pub = client.CreatePublisher(
            method->cancel_channel(), 64, 10, {.reliable = true});
        ASSERT_OK(cancel_pub);

        auto sub = client.CreateSubscriber(
            method->response_channel().name(),
            {.reliable = true, .type = method->response_channel().type()});
        ASSERT_OK(sub);

        auto buffer = pub->GetMessageBuffer(int32_t(req.ByteSizeLong()));
        ASSERT_OK(buffer);
        ASSERT_TRUE(req.SerializeToArray(*buffer, req.ByteSizeLong()));
        ASSERT_OK(pub->PublishMessage(int(req.ByteSizeLong())));

        bool done = false;
        int num_results = 0;
        while (!done) {
          int sub_fd = sub->GetPollFd().fd;
          ASSERT_OK(
              subspace::asio_rpc::async_wait_readable(sub_fd, yield));
          for (;;) {
            auto m = sub->ReadMessage();
            ASSERT_OK(m);
            if (m->length > 0) {
              subspace::Message msg = std::move(*m);
              subspace::RpcResponse rpc_response;
              ASSERT_TRUE(
                  rpc_response.ParseFromArray(msg.buffer, msg.length));
              if (rpc_response.client_id() == ctx.client_id &&
                  rpc_response.request_id() == request_id) {
                num_results++;
                if (num_results == 5) {
                  auto buffer = cancel_pub->GetMessageBuffer(64);
                  ASSERT_OK(buffer);
                  subspace::RpcCancelRequest cancel;
                  cancel.set_client_id(ctx.client_id);
                  cancel.set_request_id(request_id);
                  cancel.set_session_id(response.open().session_id());
                  ASSERT_TRUE(
                      cancel.SerializeToArray(*buffer, cancel.ByteSizeLong()));
                  ASSERT_OK(
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

        ASSERT_EQ(6, num_results);
        Close(server, ctx, yield);
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
