// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/async/byte_stream.h"
#include "common/async/runtime.h"
#include "common/async/socket.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <vector>

#if SUBSPACE_ENABLE_MTLS
#include "common/async/mtls_test_certs.h"
#endif

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
#include <boost/asio/io_context.hpp>
#endif

namespace subspace::async {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
struct RuntimeFixture {
  boost::asio::io_context ioc;
  AsyncRuntime runtime{ioc};
  void Run() { ioc.run(); }
};
#else
struct RuntimeFixture {
  co::CoroutineScheduler scheduler;
  AsyncRuntime runtime{scheduler};
  void Run() { scheduler.Run(); }
};
#endif

TEST(ByteStream, CleartextBytesArriveIntact) {
  RuntimeFixture fx;
  std::string received;
  absl::Status status = absl::OkStatus();

  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<StreamSocket> listener =
        ListenStream(InetAddress("127.0.0.1", 0));
    if (!listener.ok()) {
      status = listener.status();
      return;
    }
    const int port = listener->BoundAddress().Port();
    fx.runtime.Spawn([&, port](Context client_ctx) {
      absl::StatusOr<StreamSocket> connected = ConnectStream(
          InetAddress("127.0.0.1", port), std::chrono::seconds(2), client_ctx);
      if (!connected.ok()) {
        status = connected.status();
        return;
      }
      ByteStream client(std::move(*connected));
      const std::string payload(100000, 'q');
      if (absl::Status s = client.SendAll(payload.data(), payload.size(),
                                          client_ctx);
          !s.ok()) {
        status = s;
      }
    });
    absl::StatusOr<StreamSocket> accepted = listener->Accept(ctx);
    if (!accepted.ok()) {
      status = accepted.status();
      return;
    }
    ByteStream server(std::move(*accepted));
    if (absl::Status s = server.SetNonBlocking(); !s.ok()) {
      status = s;
      return;
    }
    received.resize(100000);
    status = server.ReadFully(received.data(), received.size(), ctx);
  });

  fx.Run();
  ASSERT_THAT(status, IsOk());
  EXPECT_EQ(received, std::string(100000, 'q'));
}

#if SUBSPACE_ENABLE_MTLS

TEST(ByteStream, MutualTlsExchangesBothWays) {
  absl::StatusOr<std::unique_ptr<TestCerts>> certs = MintTestCerts();
  ASSERT_THAT(certs, IsOk());
  absl::StatusOr<std::shared_ptr<MtlsContext>> alpha =
      MtlsContext::Load((*certs)->alpha_cert, (*certs)->alpha_key,
                        (*certs)->ca);
  absl::StatusOr<std::shared_ptr<MtlsContext>> beta = MtlsContext::Load(
      (*certs)->beta_cert, (*certs)->beta_key, (*certs)->ca);
  ASSERT_THAT(alpha, IsOk());
  ASSERT_THAT(beta, IsOk());

  RuntimeFixture fx;
  std::string from_client;
  std::string from_server;
  absl::Status status = absl::OkStatus();

  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<StreamSocket> listener =
        ListenStream(InetAddress("127.0.0.1", 0));
    if (!listener.ok()) {
      status = listener.status();
      return;
    }
    const int port = listener->BoundAddress().Port();
    fx.runtime.Spawn([&, port](Context client_ctx) {
      absl::StatusOr<StreamSocket> connected = ConnectStream(
          InetAddress("127.0.0.1", port), std::chrono::seconds(2), client_ctx);
      if (!connected.ok()) {
        status = connected.status();
        return;
      }
      ByteStream client(std::move(*connected));
      if (absl::Status s = client.HandshakeTls(*alpha, /*server_role=*/false,
                                               client_ctx,
                                               std::chrono::seconds(2));
          !s.ok()) {
        status = s;
        return;
      }
      char storage[64] = {};
      const std::string payload = "client-to-server";
      memcpy(storage + sizeof(int32_t), payload.data(), payload.size());
      if (absl::StatusOr<ssize_t> n = client.SendMessage(
              storage + sizeof(int32_t), payload.size(), client_ctx);
          !n.ok()) {
        status = n.status();
        return;
      }
      char reply[32];
      if (absl::Status s =
              client.ReadFully(reply, 12, client_ctx); !s.ok()) {
        status = s;
        return;
      }
      from_server.assign(reply, 12);
    });

    absl::StatusOr<StreamSocket> accepted = listener->Accept(ctx);
    if (!accepted.ok()) {
      status = accepted.status();
      return;
    }
    ByteStream server(std::move(*accepted));
    if (absl::Status s = server.HandshakeTls(*beta, /*server_role=*/true, ctx,
                                             std::chrono::seconds(2));
        !s.ok()) {
      status = s;
      return;
    }
    char buffer[64];
    absl::StatusOr<ssize_t> n = server.ReceiveMessage(buffer, sizeof(buffer), ctx);
    if (!n.ok()) {
      status = n.status();
      return;
    }
    from_client.assign(buffer, static_cast<size_t>(*n));
    status = server.SendAll("server-reply", 12, ctx);
  });

  fx.Run();
  ASSERT_THAT(status, IsOk());
  EXPECT_EQ(from_client, "client-to-server");
  EXPECT_EQ(from_server, "server-reply");
}

TEST(ByteStream, RejectsACertificateFromAnotherCa) {
  absl::StatusOr<std::unique_ptr<TestCerts>> certs = MintTestCerts();
  ASSERT_THAT(certs, IsOk());
  absl::StatusOr<std::shared_ptr<MtlsContext>> trusted = MtlsContext::Load(
      (*certs)->alpha_cert, (*certs)->alpha_key, (*certs)->ca);
  absl::StatusOr<std::shared_ptr<MtlsContext>> stranger = MtlsContext::Load(
      (*certs)->other_cert, (*certs)->other_key, (*certs)->other_ca);
  ASSERT_THAT(trusted, IsOk());
  ASSERT_THAT(stranger, IsOk());

  RuntimeFixture fx;
  absl::Status client_status = absl::OkStatus();
  absl::Status server_status = absl::OkStatus();

  fx.runtime.Spawn([&](Context ctx) {
    absl::StatusOr<StreamSocket> listener =
        ListenStream(InetAddress("127.0.0.1", 0));
    if (!listener.ok()) {
      server_status = listener.status();
      return;
    }
    const int port = listener->BoundAddress().Port();
    fx.runtime.Spawn([&, port](Context client_ctx) {
      absl::StatusOr<StreamSocket> connected = ConnectStream(
          InetAddress("127.0.0.1", port), std::chrono::seconds(2), client_ctx);
      if (!connected.ok()) {
        client_status = connected.status();
        return;
      }
      ByteStream client(std::move(*connected));
      client_status = client.HandshakeTls(*stranger, /*server_role=*/false,
                                          client_ctx, std::chrono::seconds(2));
    });
    absl::StatusOr<StreamSocket> accepted = listener->Accept(ctx);
    if (!accepted.ok()) {
      server_status = accepted.status();
      return;
    }
    ByteStream server(std::move(*accepted));
    server_status = server.HandshakeTls(*trusted, /*server_role=*/true, ctx,
                                        std::chrono::seconds(2));
  });

  fx.Run();
  EXPECT_FALSE(client_status.ok());
  EXPECT_FALSE(server_status.ok());
}

#else

TEST(Mtls, LoadIsRejectedWhenTheBuildLeavesItOut) {
  absl::StatusOr<std::shared_ptr<MtlsContext>> loaded =
      MtlsContext::Load("cert.pem", "key.pem", "ca.pem");
  EXPECT_THAT(loaded, StatusIs(absl::StatusCode::kUnimplemented));
}

#endif

} // namespace
} // namespace subspace::async
