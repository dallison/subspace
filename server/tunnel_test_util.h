// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "client/client.h"
#include "common/async/runtime.h"
#include "proto/tunnel_config.pb.h"
#include "server/server.h"
#include "server/tunnel.h"
#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#if defined(__ANDROID__)
#define TUNNEL_TEST_TMP "/data/local/tmp"
#else
#define TUNNEL_TEST_TMP "/tmp"
#endif

namespace subspace::tunnel_test {

using namespace std::chrono_literals;

inline constexpr const char *kDeviceNames[2] = {"alpha", "beta"};

// Finds `N` different free loopback TCP ports, or -1 for any it can't find.
// The sockets stay open until all the ports are found, so a port that's just
// been released can't come back twice.
template <size_t N> std::array<int, N> FreePorts() {
  std::array<int, N> fds;
  std::array<int, N> ports;
  for (size_t i = 0; i < N; i++) {
    fds[i] = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(addr);
    ports[i] = -1;
    if (::bind(fds[i], reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) ==
            0 &&
        ::getsockname(fds[i], reinterpret_cast<sockaddr *>(&addr), &len) ==
            0) {
      ports[i] = ntohs(addr.sin_port);
    }
  }
  for (int fd : fds) {
    ::close(fd);
  }
  return ports;
}

// Two servers that know each other only through their tunnels.
class TunnelServers {
public:
  ~TunnelServers() { Stop(); }

  // `configure` can change server `i`'s configuration before it starts.
  void Start(const std::function<void(int i, TunnelConfig &)> &configure =
                 nullptr) {
    const std::array<int, 2> ports = FreePorts<2>();
    ASSERT_GT(ports[0], 0);
    ASSERT_GT(ports[1], 0);
    for (int i = 0; i < 2; i++) {
      char socket_template[] = TUNNEL_TEST_TMP "/subspace_tunnelXXXXXX"; // NOLINT
      ::close(mkstemp(&socket_template[0]));
      socket_[i] = &socket_template[0];
      ASSERT_EQ(0, pipe(server_pipe_[i]));

      TunnelConfig config;
      config.set_device_name(kDeviceNames[i]);
      config.set_listen_address("127.0.0.1");
      config.set_listen_port(ports[i]);
      TunnelPeer *peer = config.add_peers();
      peer->set_name(kDeviceNames[1 - i]);
      peer->set_tcp_addr("127.0.0.1");
      peer->set_tcp_port(ports[1 - i]);
      if (configure) {
        configure(i, config);
      }

      // Local servers run no native discovery, so only the tunnel connects
      // them.
      server_[i] = std::make_unique<Server>(engine_[i], socket_[i], "", 0, 0,
                                            /*local=*/true, server_pipe_[i][1]);
      server_[i]->SetTunnelConfig(config);
      server_thread_[i] = std::thread([this, i]() {
        // The ingress and egress connections run on their own strands, so
        // several Asio threads exercise them concurrently.  The co backend
        // ignores the thread count.
        absl::Status s = server_[i]->Run(/*num_asio_threads=*/4);
        if (!s.ok()) {
          // exit() can hang while other threads are running.
          fprintf(stderr, "Error running Subspace server: %s\n",
                  s.ToString().c_str());
          _exit(1);
        }
      });
      char buf[8];
      (void)::read(server_pipe_[i][0], buf, sizeof(buf));
    }
    running_ = true;
  }

  void Stop() {
    if (!running_) {
      return;
    }
    char buf[8];
    for (int i = 0; i < 2; i++) {
      server_[i]->Stop();
      (void)::read(server_pipe_[i][0], buf, sizeof(buf));
      server_thread_[i].join();
      server_[i]->CleanupAfterSession();
      (void)remove(socket_[i].c_str());
      ::close(server_pipe_[i][0]);
      ::close(server_pipe_[i][1]);
    }
    running_ = false;
  }

  const std::string &Socket(int i) const { return socket_[i]; }
  const TunnelManager::Stats &Stats(int i) const {
    return server_[i]->GetTunnelManager()->GetStats();
  }

private:
  async::RuntimeEngine engine_[2];
  std::string socket_[2];
  int server_pipe_[2][2];
  std::unique_ptr<Server> server_[2];
  std::thread server_thread_[2];
  bool running_ = false;
};

inline std::string Pattern(size_t size, int seed) {
  std::string data(size, '\0');
  for (size_t i = 0; i < size; i++) {
    data[i] = static_cast<char>((i * 31 + seed) % 251);
  }
  return data;
}

inline absl::Status Publish(Publisher &pub, const std::string &data,
                            const std::string &metadata = "") {
  absl::StatusOr<void *> buffer;
  for (;;) {
    buffer = pub.GetMessageBuffer(static_cast<SlotSizeType>(data.size()));
    if (!buffer.ok() || *buffer != nullptr) {
      break;
    }
    // A reliable publisher waits for a free slot.
    if (absl::Status s = pub.Wait(5s); !s.ok()) {
      return s;
    }
  }
  if (!buffer.ok()) {
    return buffer.status();
  }
  memcpy(*buffer, data.data(), data.size());
  if (!metadata.empty()) {
    absl::Span<std::byte> area = pub.GetMetadata();
    if (area.size() < metadata.size()) {
      pub.CancelPublish();
      return absl::InvalidArgumentError("metadata doesn't fit");
    }
    memcpy(area.data(), metadata.data(), metadata.size());
  }
  return pub.PublishMessage(static_cast<int64_t>(data.size())).status();
}

// Reads the next message's payload, waiting up to `timeout` for it.
inline absl::StatusOr<std::string>
Read(Subscriber &sub, std::chrono::milliseconds timeout = 5s) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    absl::StatusOr<Message> msg = sub.ReadMessage();
    if (!msg.ok()) {
      return msg.status();
    }
    if (msg->length > 0) {
      return std::string(static_cast<const char *>(msg->buffer), msg->length);
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return absl::DeadlineExceededError("no message arrived");
    }
    std::this_thread::sleep_for(2ms);
  }
}

// What WaitForRoute publishes.
inline constexpr char kProbe[] = "probe";

// Publishes probes until one arrives, which shows the tunnel has a route for
// the channel, then discards any probes still on their way.
inline absl::Status WaitForRoute(Publisher &pub, Subscriber &sub) {
  auto deadline = std::chrono::steady_clock::now() + 15s;
  bool routed = false;
  while (!routed && std::chrono::steady_clock::now() < deadline) {
    if (absl::Status s = Publish(pub, kProbe); !s.ok()) {
      return s;
    }
    routed = Read(sub, 50ms).ok();
  }
  if (!routed) {
    return absl::DeadlineExceededError("the tunnel never carried a message");
  }
  while (Read(sub, 200ms).ok()) {
  }
  return absl::OkStatus();
}

} // namespace subspace::tunnel_test
