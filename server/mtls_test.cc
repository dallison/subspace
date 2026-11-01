// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/status/status_matchers.h"
#include "client/client.h"
#include "common/async/runtime.h"
#include "server/server.h"
#include "server/tunnel_test_util.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <signal.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <gtest/gtest.h>

#if SUBSPACE_ENABLE_MTLS
#include "common/async/mtls_test_certs.h"
#endif

namespace subspace {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using namespace std::chrono_literals;
using tunnel_test::Publish;
using tunnel_test::Read;
using tunnel_test::TunnelServers;
using tunnel_test::WaitForRoute;

#if defined(__ANDROID__)
constexpr const char *kTmp = "/data/local/tmp";
#else
constexpr const char *kTmp = "/tmp";
#endif

std::string TempSocket() {
  std::string path = std::string(kTmp) + "/subspace_mtlsXXXXXX";
  int fd = mkstemp(path.data());
  if (fd >= 0) {
    ::close(fd);
  }
  return path;
}

#if !SUBSPACE_ENABLE_MTLS

TEST(Mtls, CredentialsAreRejectedWhenTheBuildLeavesItOut) {
  signal(SIGPIPE, SIG_IGN);
  async::RuntimeEngine engine;
  Server server(engine, TempSocket(), "", 0, 0, /*local=*/true, -1);
  server.SetMtlsCredentials("cert.pem", "key.pem", "ca.pem");
  EXPECT_THAT(server.Run(), StatusIs(absl::StatusCode::kUnimplemented));
}

#else

TEST(MtlsTunnel, MessagesCrossBothWays) {
  signal(SIGPIPE, SIG_IGN);
  absl::StatusOr<std::unique_ptr<async::TestCerts>> certs =
      async::MintTestCerts();
  ASSERT_THAT(certs, IsOk());
  const async::TestCerts &files = **certs;
  const char *certs_for_server[2] = {files.alpha_cert.c_str(),
                                     files.beta_cert.c_str()};
  const char *keys_for_server[2] = {files.alpha_key.c_str(),
                                    files.beta_key.c_str()};

  TunnelServers servers;
  servers.Start(nullptr, [&](int i, Server &server) {
    server.SetMtlsCredentials(certs_for_server[i], keys_for_server[i],
                              files.ca);
  });
  Client clients[2];
  ASSERT_THAT(clients[0].Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(clients[1].Init(servers.Socket(1)), IsOk());

  absl::StatusOr<Publisher> pub_a = clients[0].CreatePublisher(
      "/mtls/a", PublisherOptions().SetSlotSize(256).SetNumSlots(8));
  absl::StatusOr<Publisher> pub_b = clients[1].CreatePublisher(
      "/mtls/b", PublisherOptions().SetSlotSize(256).SetNumSlots(8));
  ASSERT_THAT(pub_a, IsOk());
  ASSERT_THAT(pub_b, IsOk());
  absl::StatusOr<Subscriber> sub_a = clients[1].CreateSubscriber("/mtls/a");
  absl::StatusOr<Subscriber> sub_b = clients[0].CreateSubscriber("/mtls/b");
  ASSERT_THAT(sub_a, IsOk());
  ASSERT_THAT(sub_b, IsOk());
  ASSERT_THAT(WaitForRoute(*pub_a, *sub_a), IsOk());
  ASSERT_THAT(WaitForRoute(*pub_b, *sub_b), IsOk());

  ASSERT_THAT(Publish(*pub_a, "alpha-to-beta"), IsOk());
  ASSERT_THAT(Publish(*pub_b, "beta-to-alpha"), IsOk());
  absl::StatusOr<std::string> a = Read(*sub_a);
  absl::StatusOr<std::string> b = Read(*sub_b);
  ASSERT_THAT(a, IsOk());
  ASSERT_THAT(b, IsOk());
  EXPECT_EQ(*a, "alpha-to-beta");
  EXPECT_EQ(*b, "beta-to-alpha");
}

TEST(MtlsTunnel, ADifferentCaCarriesNothing) {
  signal(SIGPIPE, SIG_IGN);
  absl::StatusOr<std::unique_ptr<async::TestCerts>> certs =
      async::MintTestCerts();
  ASSERT_THAT(certs, IsOk());
  const async::TestCerts &files = **certs;

  TunnelServers servers;
  servers.Start(nullptr, [&](int i, Server &server) {
    if (i == 0) {
      server.SetMtlsCredentials(files.alpha_cert, files.alpha_key, files.ca);
    } else {
      server.SetMtlsCredentials(files.other_cert, files.other_key,
                                files.other_ca);
    }
  });
  Client clients[2];
  ASSERT_THAT(clients[0].Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(clients[1].Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = clients[0].CreatePublisher(
      "/mtls/rejected", PublisherOptions().SetSlotSize(64).SetNumSlots(4));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub =
      clients[1].CreateSubscriber("/mtls/rejected");
  ASSERT_THAT(sub, IsOk());

  auto deadline = std::chrono::steady_clock::now() + 3s;
  bool arrived = false;
  while (!arrived && std::chrono::steady_clock::now() < deadline) {
    ASSERT_THAT(Publish(*pub, "nope"), IsOk());
    arrived = Read(*sub, 50ms).ok();
  }
  EXPECT_FALSE(arrived);
}

// Two servers that find each other through native discovery.  `use_mtls` is
// per server.
class BridgePair {
public:
  ~BridgePair() { Stop(); }

  absl::Status Start(const async::TestCerts &certs, bool mtls0, bool mtls1) {
    signal(SIGPIPE, SIG_IGN);
    std::array<toolbelt::UDPSocket, 2> reserved;
    std::array<int, 2> ports{};
    for (int i = 0; i < 2; i++) {
      if (absl::Status s =
              reserved[i].Bind(toolbelt::InetAddress::AnyAddress(0));
          !s.ok()) {
        return s;
      }
      ports[i] = reserved[i].BoundAddress().Port();
    }
    const bool mtls[2] = {mtls0, mtls1};
    const char *cert[2] = {certs.alpha_cert.c_str(), certs.beta_cert.c_str()};
    const char *key[2] = {certs.alpha_key.c_str(), certs.beta_key.c_str()};
    for (int i = 0; i < 2; i++) {
      socket_[i] = TempSocket();
      if (pipe(pipe_[i]) != 0) {
        return absl::InternalError("pipe failed");
      }
      reserved[i].Close();
      server_[i] = std::make_unique<Server>(
          engine_[i], socket_[i], "", ports[i], ports[1 - i],
          /*local=*/false, pipe_[i][1]);
      if (mtls[i]) {
        server_[i]->SetMtlsCredentials(cert[i], key[i], certs.ca);
      }
      server_thread_[i] = std::thread([this, i]() {
        absl::Status s = server_[i]->Run();
        if (!s.ok()) {
          fprintf(stderr, "Error running Subspace server: %s\n",
                  s.ToString().c_str());
          _exit(1);
        }
      });
      char buf[8];
      if (::read(pipe_[i][0], buf, sizeof(buf)) != 8) {
        return absl::InternalError("server did not start");
      }
    }
    running_ = true;
    return absl::OkStatus();
  }

  void Stop() {
    if (!running_) {
      return;
    }
    for (int i = 0; i < 2; i++) {
      server_[i]->Stop();
    }
    for (int i = 0; i < 2; i++) {
      char buf[8];
      (void)::read(pipe_[i][0], buf, sizeof(buf));
      server_thread_[i].join();
      server_[i]->CleanupAfterSession();
      unlink(socket_[i].c_str());
      ::close(pipe_[i][0]);
      ::close(pipe_[i][1]);
    }
    running_ = false;
  }

  const std::string &Socket(int i) const { return socket_[i]; }

private:
  async::RuntimeEngine engine_[2];
  std::unique_ptr<Server> server_[2];
  std::thread server_thread_[2];
  std::string socket_[2];
  int pipe_[2][2]{};
  bool running_ = false;
};

TEST(MtlsBridge, AMessageCrosses) {
  absl::StatusOr<std::unique_ptr<async::TestCerts>> certs =
      async::MintTestCerts();
  ASSERT_THAT(certs, IsOk());
  BridgePair servers;
  ASSERT_THAT(servers.Start(**certs, true, true), IsOk());
  Client clients[2];
  ASSERT_THAT(clients[0].Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(clients[1].Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = clients[0].CreatePublisher(
      "/mtls/bridge", PublisherOptions().SetSlotSize(128).SetNumSlots(8));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = clients[1].CreateSubscriber("/mtls/bridge");
  ASSERT_THAT(sub, IsOk());

  auto deadline = std::chrono::steady_clock::now() + 10s;
  bool routed = false;
  while (!routed && std::chrono::steady_clock::now() < deadline) {
    ASSERT_THAT(Publish(*pub, "probe"), IsOk());
    routed = Read(*sub, 100ms).ok();
  }
  ASSERT_TRUE(routed) << "the bridge never carried a message";
  while (Read(*sub, 200ms).ok()) {
  }
  ASSERT_THAT(Publish(*pub, "bridged"), IsOk());
  absl::StatusOr<std::string> received = Read(*sub);
  ASSERT_THAT(received, IsOk());
  EXPECT_EQ(*received, "bridged");
}

TEST(MtlsBridge, ACleartextPeerIsRejected) {
  absl::StatusOr<std::unique_ptr<async::TestCerts>> certs =
      async::MintTestCerts();
  ASSERT_THAT(certs, IsOk());
  BridgePair servers;
  ASSERT_THAT(servers.Start(**certs, true, false), IsOk());
  Client clients[2];
  ASSERT_THAT(clients[0].Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(clients[1].Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = clients[0].CreatePublisher(
      "/mtls/clear", PublisherOptions().SetSlotSize(64).SetNumSlots(4));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = clients[1].CreateSubscriber("/mtls/clear");
  ASSERT_THAT(sub, IsOk());

  auto deadline = std::chrono::steady_clock::now() + 3s;
  bool arrived = false;
  while (!arrived && std::chrono::steady_clock::now() < deadline) {
    ASSERT_THAT(Publish(*pub, "nope"), IsOk());
    arrived = Read(*sub, 50ms).ok();
  }
  EXPECT_FALSE(arrived);
}

TEST(Mtls, MissingFilesFailBeforeTheServerListens) {
  signal(SIGPIPE, SIG_IGN);
  async::RuntimeEngine engine;
  const std::string socket = TempSocket();
  Server server(engine, socket, "", 0, 0, /*local=*/true, -1);
  server.SetMtlsCredentials("/no/such/cert.pem", "/no/such/key.pem",
                            "/no/such/ca.pem");
  absl::Status status = server.Run();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(std::string(status.message()).find("mTLS"), std::string::npos);
  unlink(socket.c_str());
}

#endif

} // namespace
} // namespace subspace
