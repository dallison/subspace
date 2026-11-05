// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "absl/status/status_matchers.h"
#include "client/checksum.h"
#include "client/client.h"
#include "common/async/runtime.h"
#include "server/server.h"
#include "server/tunnel.h"
#include "server/tunnel_test_util.h"
#include "toolbelt/sockets.h"
#include <array>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <memory>
#include <signal.h>
#include <string>
#include <sys/file.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace subspace {
namespace {

using ::absl_testing::IsOk;
using namespace std::chrono_literals;
using namespace tunnel_test;

absl::Status OkStartup(Server &, const std::string &, PluginContext *) {
  return absl::OkStatus();
}
void NoopReady(Server &, PluginContext *) {}
void NoopShutdown(PluginContext *) {}
void NoopChannel(Server &, const std::string &, PluginContext *) {}
void NoopUser(Server &, const std::string &, int, PluginContext *) {}
absl::StatusOr<bool> NoFree(Server &, const ClientBufferHandleMetadata &,
                            PluginContext *) {
  return false;
}

std::byte Flipped(std::byte b) {
  return std::byte{
      static_cast<unsigned char>(std::to_integer<unsigned char>(b) ^ 0xff)};
}

std::string Flipped(std::string data) {
  for (char &c : data) {
    c = static_cast<char>(static_cast<unsigned char>(c) ^ 0xff);
  }
  return data;
}

// Replaces a payload that starts with 'M' with its flipped bytes.
absl::StatusOr<PluginPayload> FlipOnSend(Server &, const std::string &,
                                         PluginPayload payload,
                                         PluginContext *) {
  if (payload.bytes.empty() || payload.bytes.front() != std::byte{'M'}) {
    payload.modified = false;
    return payload;
  }
  auto storage = std::make_shared<std::vector<std::byte>>(payload.bytes.begin(),
                                                          payload.bytes.end());
  for (std::byte &b : *storage) {
    b = Flipped(b);
  }
  PluginPayload out;
  out.bytes = absl::MakeConstSpan(*storage);
  out.keepalive = storage;
  out.modified = true;
  return out;
}

// Flips a payload back when its first byte is the flipped 'M'.
PluginReceiveResult UnflipOnReceive(Server &, const std::string &,
                                    std::byte *data, size_t length,
                                    size_t, int32_t, PluginContext *) {
  PluginReceiveResult result;
  result.length = static_cast<ssize_t>(length);
  if (length == 0 || data[0] != Flipped(std::byte{'M'})) {
    return result;
  }
  for (size_t i = 0; i < length; i++) {
    data[i] = Flipped(data[i]);
  }
  result.modified = true;
  return result;
}

// Drops a payload that starts with 'D'.
PluginReceiveResult DropOnReceive(Server &, const std::string &,
                                  std::byte *data, size_t length, size_t,
                                  int32_t, PluginContext *) {
  PluginReceiveResult result;
  result.length = length > 0 && data[0] == std::byte{'D'}
                      ? ssize_t{0}
                      : static_cast<ssize_t>(length);
  return result;
}

std::unique_ptr<PluginInterface> MakePlugin(bool send, bool unflip, bool drop) {
  PluginInterfaceFunctions functions = {
      .onStartup = OkStartup,
      .onReady = NoopReady,
      .onShutdown = NoopShutdown,
      .onNewChannel = NoopChannel,
      .onRemoveChannel = NoopChannel,
      .onNewPublisher = NoopUser,
      .onRemovePublisher = NoopUser,
      .onNewSubscriber = NoopUser,
      .onRemoveSubscriber = NoopUser,
      .onFreeClientBuffer = NoFree,
      .onSend = send ? FlipOnSend : nullptr,
      .onReceive = drop ? DropOnReceive : (unflip ? UnflipOnReceive : nullptr),
  };
  return std::make_unique<PluginInterface>(
      functions, std::make_unique<PluginContext>("plugin data"));
}

void WriteSum(const std::array<absl::Span<const uint8_t>, 3> &data,
              absl::Span<std::byte> checksum) {
  uint64_t sum = 0x9e3779b97f4a7c15ULL;
  for (const absl::Span<const uint8_t> &span : data) {
    for (uint8_t byte : span) {
      sum = sum * 131 + byte;
    }
  }
  memcpy(checksum.data(), &sum, checksum.size());
}

struct Received {
  std::string data;
  bool checksum_error = false;
};

absl::StatusOr<Received> ReadChecked(Subscriber &sub,
                                     std::chrono::milliseconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    absl::StatusOr<Message> msg = sub.ReadMessage();
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length > 0) {
      return Received{
          std::string(static_cast<const char *>(msg->buffer), msg->length),
          msg->checksum_error};
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return absl::DeadlineExceededError("no message arrived");
    }
    std::this_thread::sleep_for(2ms);
  }
}

PublisherOptions ChecksumOptions(int32_t checksum_size) {
  return PublisherOptions()
      .SetChecksum(true)
      .SetChecksumSize(checksum_size)
      .SetSlotSize(256)
      .SetNumSlots(8);
}

SubscriberOptions ChecksumSubscriber(int32_t) {
  return SubscriberOptions().SetChecksum(true).SetPassChecksumErrors(true);
}

class PluginBridgeTest : public ::testing::Test {
protected:
  void SetUp() override { signal(SIGPIPE, SIG_IGN); }
};

TEST_F(PluginBridgeTest, TunnelRecomputesChecksumWhenEgressChangesPayload) {
  TunnelServers servers;
  servers.Start(nullptr, [](int i, Server &server) {
    if (i == 0) {
      ASSERT_THAT(server.LoadBuiltinPlugin("flip", MakePlugin(true, false,
                                                              false)),
                  IsOk());
    }
  });
  Client publisher_client;
  Client subscriber_client;
  ASSERT_THAT(publisher_client.Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(subscriber_client.Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = publisher_client.CreatePublisher(
      "/plugin/flip", ChecksumOptions(4));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = subscriber_client.CreateSubscriber(
      "/plugin/flip", ChecksumSubscriber(4));
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  ASSERT_THAT(Publish(*pub, "Mhello"), IsOk());
  absl::StatusOr<Received> got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, Flipped("Mhello"));
  EXPECT_FALSE(got->checksum_error);

  ASSERT_THAT(Publish(*pub, "plain"), IsOk());
  got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, "plain");
  EXPECT_FALSE(got->checksum_error);
}

TEST_F(PluginBridgeTest, TunnelRecomputesChecksumWhenIngressChangesPayload) {
  TunnelServers servers;
  servers.Start(nullptr, [](int i, Server &server) {
    ASSERT_THAT(
        server.LoadBuiltinPlugin("flip", MakePlugin(i == 0, i == 1, false)),
        IsOk());
  });
  Client publisher_client;
  Client subscriber_client;
  ASSERT_THAT(publisher_client.Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(subscriber_client.Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = publisher_client.CreatePublisher(
      "/plugin/unflip", ChecksumOptions(4));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = subscriber_client.CreateSubscriber(
      "/plugin/unflip", ChecksumSubscriber(4));
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  ASSERT_THAT(Publish(*pub, "Mhello"), IsOk());
  absl::StatusOr<Received> got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, "Mhello");
  EXPECT_FALSE(got->checksum_error);
}

TEST_F(PluginBridgeTest, TunnelDropsIngressMessage) {
  TunnelServers servers;
  servers.Start(nullptr, [](int i, Server &server) {
    if (i == 1) {
      ASSERT_THAT(
          server.LoadBuiltinPlugin("drop", MakePlugin(false, false, true)),
          IsOk());
    }
  });
  Client publisher_client;
  Client subscriber_client;
  ASSERT_THAT(publisher_client.Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(subscriber_client.Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = publisher_client.CreatePublisher(
      "/plugin/drop", ChecksumOptions(4));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = subscriber_client.CreateSubscriber(
      "/plugin/drop", ChecksumSubscriber(4));
  ASSERT_THAT(sub, IsOk());
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  ASSERT_THAT(Publish(*pub, "Dnope"), IsOk());
  ASSERT_THAT(Publish(*pub, "kept"), IsOk());
  absl::StatusOr<Received> got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, "kept");
  EXPECT_FALSE(got->checksum_error);
}

TEST_F(PluginBridgeTest, CustomChecksumIsRecomputedOnlyWhenPayloadChanges) {
  TunnelServers servers;
  servers.Start(nullptr, [](int i, Server &server) {
    server.SetChecksumSize(8);
    if (i == 0) {
      server.SetChecksumCallback(WriteSum);
      ASSERT_THAT(server.LoadBuiltinPlugin("flip", MakePlugin(true, false,
                                                              false)),
                  IsOk());
    }
  });
  Client publisher_client;
  Client subscriber_client;
  ASSERT_THAT(publisher_client.Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(subscriber_client.Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = publisher_client.CreatePublisher(
      "/plugin/sum", ChecksumOptions(8));
  ASSERT_THAT(pub, IsOk());
  pub->SetChecksumCallback(WriteSum);
  absl::StatusOr<Subscriber> sub = subscriber_client.CreateSubscriber(
      "/plugin/sum", ChecksumSubscriber(8));
  ASSERT_THAT(sub, IsOk());
  sub->SetChecksumCallback(WriteSum);
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  ASSERT_THAT(Publish(*pub, "plain"), IsOk());
  absl::StatusOr<Received> got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, "plain");
  EXPECT_FALSE(got->checksum_error);

  ASSERT_THAT(Publish(*pub, "Mhello"), IsOk());
  got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, Flipped("Mhello"));
  EXPECT_FALSE(got->checksum_error);
}

TEST_F(PluginBridgeTest, ModifiedMessageIsNotSentWithoutAChecksumFunction) {
  TunnelServers servers;
  servers.Start(nullptr, [](int i, Server &server) {
    server.SetChecksumSize(8);
    if (i == 0) {
      ASSERT_THAT(server.LoadBuiltinPlugin("flip", MakePlugin(true, false,
                                                              false)),
                  IsOk());
    }
  });
  Client publisher_client;
  Client subscriber_client;
  ASSERT_THAT(publisher_client.Init(servers.Socket(0)), IsOk());
  ASSERT_THAT(subscriber_client.Init(servers.Socket(1)), IsOk());
  absl::StatusOr<Publisher> pub = publisher_client.CreatePublisher(
      "/plugin/nocheck", ChecksumOptions(8));
  ASSERT_THAT(pub, IsOk());
  pub->SetChecksumCallback(WriteSum);
  absl::StatusOr<Subscriber> sub = subscriber_client.CreateSubscriber(
      "/plugin/nocheck", ChecksumSubscriber(8));
  ASSERT_THAT(sub, IsOk());
  sub->SetChecksumCallback(WriteSum);
  ASSERT_THAT(WaitForRoute(*pub, *sub), IsOk());

  const uint64_t sent = servers.Stats(0).messages_sent;
  ASSERT_THAT(Publish(*pub, "Mhello"), IsOk());
  EXPECT_FALSE(ReadChecked(*sub, 1s).ok());
  EXPECT_EQ(servers.Stats(0).messages_sent, sent);

  ASSERT_THAT(Publish(*pub, "plain"), IsOk());
  absl::StatusOr<Received> got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, "plain");
  EXPECT_FALSE(got->checksum_error);
}

// Native discovery broadcasts, so servers that run it start while holding
// the bridge tests' port lock.
class NativeDiscoveryPorts {
public:
  ~NativeDiscoveryPorts() { Unlock(); }

  void Lock() {
    lock_fd_ = ::open(TUNNEL_TEST_TMP "/subspace_bridge_test_port.lock",
                      O_CREAT | O_RDWR, 0666);
    ASSERT_NE(-1, lock_fd_);
    ASSERT_EQ(0, ::flock(lock_fd_, LOCK_EX));
    for (int i = 0; i < 2; i++) {
      ASSERT_THAT(reserved_[i].Bind(toolbelt::InetAddress::AnyAddress(0)),
                  IsOk());
      ports_[i] = reserved_[i].BoundAddress().Port();
    }
  }

  int Take(int i) {
    reserved_[i].Close();
    return ports_[i];
  }
  int Port(int i) const { return ports_[i]; }

  void Unlock() {
    if (lock_fd_ < 0) {
      return;
    }
    (void)::flock(lock_fd_, LOCK_UN);
    ::close(lock_fd_);
    lock_fd_ = -1;
  }

private:
  int lock_fd_ = -1;
  std::array<toolbelt::UDPSocket, 2> reserved_;
  int ports_[2] = {0, 0};
};

class DiscoveryServer {
public:
  ~DiscoveryServer() { Stop(); }

  void Start(int discovery_port, int peer_port,
             const std::function<void(Server &)> &prepare) {
    char socket_template[] = TUNNEL_TEST_TMP "/subspace_pluginXXXXXX"; // NOLINT
    ::close(mkstemp(&socket_template[0]));
    socket_ = &socket_template[0];
    ASSERT_EQ(0, pipe(pipe_));
    server_ = std::make_unique<Server>(engine_, socket_, "", discovery_port,
                                       peer_port, /*local=*/false, pipe_[1]);
    prepare(*server_);
    Server *server = server_.get();
    thread_ = std::thread([server]() {
      absl::Status status = server->Run(/*num_asio_threads=*/4);
      if (!status.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                status.ToString().c_str());
        _exit(1);
      }
    });
    char buf[8];
    (void)::read(pipe_[0], buf, sizeof(buf));
  }

  void Stop() {
    if (server_ == nullptr) {
      return;
    }
    server_->Stop();
    char buf[8];
    (void)::read(pipe_[0], buf, sizeof(buf));
    thread_.join();
    server_->CleanupAfterSession();
    server_.reset();
    (void)remove(socket_.c_str());
    ::close(pipe_[0]);
    ::close(pipe_[1]);
  }

  const std::string &Socket() const { return socket_; }

private:
  async::RuntimeEngine engine_;
  std::string socket_;
  int pipe_[2] = {-1, -1};
  std::unique_ptr<Server> server_;
  std::thread thread_;
};

TEST_F(PluginBridgeTest, NativeBridgeRecomputesChecksumWhenEgressChangesPayload) {
  NativeDiscoveryPorts ports;
  ASSERT_NO_FATAL_FAILURE(ports.Lock());
  DiscoveryServer publisher_server;
  DiscoveryServer subscriber_server;
  ASSERT_NO_FATAL_FAILURE(publisher_server.Start(
      ports.Take(0), ports.Port(1), [](Server &server) {
        ASSERT_THAT(server.LoadBuiltinPlugin("flip", MakePlugin(true, false,
                                                                false)),
                    IsOk());
      }));
  ASSERT_NO_FATAL_FAILURE(
      subscriber_server.Start(ports.Take(1), ports.Port(0), [](Server &) {}));
  ports.Unlock();

  Client publisher_client;
  Client subscriber_client;
  ASSERT_THAT(publisher_client.Init(publisher_server.Socket()), IsOk());
  ASSERT_THAT(subscriber_client.Init(subscriber_server.Socket()), IsOk());
  absl::StatusOr<Publisher> pub = publisher_client.CreatePublisher(
      "/plugin/bridge", ChecksumOptions(4));
  ASSERT_THAT(pub, IsOk());
  absl::StatusOr<Subscriber> sub = subscriber_client.CreateSubscriber(
      "/plugin/bridge", ChecksumSubscriber(4));
  ASSERT_THAT(sub, IsOk());

  const auto deadline = std::chrono::steady_clock::now() + 15s;
  absl::StatusOr<Received> got = absl::DeadlineExceededError("no bridge");
  while (std::chrono::steady_clock::now() < deadline) {
    ASSERT_THAT(Publish(*pub, "Mhello"), IsOk());
    got = ReadChecked(*sub, 100ms);
    if (got.ok()) {
      break;
    }
  }
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, Flipped("Mhello"));
  EXPECT_FALSE(got->checksum_error);

  ASSERT_THAT(Publish(*pub, "plain"), IsOk());
  got = ReadChecked(*sub);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(got->data, "plain");
  EXPECT_FALSE(got->checksum_error);
}

} // namespace
} // namespace subspace
