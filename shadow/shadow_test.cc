// Copyright 2023-2026 David Allison
// Shadow support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "shadow/shadow.h"
#include "server/server.h"
#include "client/client.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"
#include <chrono>
#include <csignal>
#include <cstring>
#include <poll.h>
#include <thread>
#include <unistd.h>

namespace {

using ::absl_testing::IsOk;

constexpr const char *kServerSocket = "/tmp/subspace_shadow_test";
constexpr const char *kShadowSocket = "/tmp/subspace_shadow_test_shadow";

class ShadowTest : public ::testing::Test {
public:
  static void SetUpTestSuite() {
    signal(SIGPIPE, SIG_IGN);

    (void)pipe(shadow_pipe_);

    shadow_ = std::make_unique<subspace::Shadow>(shadow_scheduler_,
                                                  kShadowSocket);
    shadow_->SetLogLevel("verbose");
    shadow_->SetNotifyFd(shadow_pipe_[1]);

    shadow_thread_ = std::thread([]() {
      absl::Status s = shadow_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Shadow error: %s\n", s.ToString().c_str());
      }
    });

    // Wait for shadow to bind its socket (it sends a notification byte).
    char c;
    (void)::read(shadow_pipe_[0], &c, 1);

    (void)pipe(server_pipe_);
    server_ = std::make_unique<subspace::Server>(
        server_scheduler_, kServerSocket, "", 0, 0, /*local=*/true,
        server_pipe_[1], 1, true);
    server_->SetShadowSocket(kShadowSocket);

    server_thread_ = std::thread([]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Server error: %s\n", s.ToString().c_str());
      }
    });

    // Wait for server to be ready.
    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  static void TearDownTestSuite() {
    server_->Stop(/*force=*/true);
    server_thread_.join();
    server_->CleanupAfterSession();
    (void)remove(kServerSocket);

    shadow_->Stop();
    shadow_thread_.join();

    close(shadow_pipe_[0]);
    close(shadow_pipe_[1]);
  }

  void InitClient(subspace::Client &client) {
    client.SetThreadSafe(true);
    ASSERT_THAT(client.Init(kServerSocket), IsOk());
  }

  // Block until the given condition is true on the shadow's state.  Each time
  // the shadow finishes processing an event it writes a byte to the pipe,
  // so we poll()/read() to wait efficiently instead of sleeping.
  static bool WaitForShadowState(std::function<bool()> condition,
                                 int timeout_ms = 5000) {
    if (condition()) {
      return true;
    }
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    for (;;) {
      auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        return condition();
      }
      int remaining_ms = static_cast<int>(
          std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
              .count());

      struct pollfd pfd;
      pfd.fd = shadow_pipe_[0];
      pfd.events = POLLIN;
      pfd.revents = 0;
      int ret = ::poll(&pfd, 1, remaining_ms);
      if (ret > 0 && (pfd.revents & POLLIN)) {
        char drain[64];
        (void)::read(shadow_pipe_[0], drain, sizeof(drain));
      }
      if (condition()) {
        return true;
      }
    }
  }

  static subspace::Shadow *GetShadow() { return shadow_.get(); }

private:
  static co::CoroutineScheduler server_scheduler_;
  static co::CoroutineScheduler shadow_scheduler_;
  static int server_pipe_[2];
  static int shadow_pipe_[2];
  static std::unique_ptr<subspace::Server> server_;
  static std::thread server_thread_;
  static std::unique_ptr<subspace::Shadow> shadow_;
  static std::thread shadow_thread_;
};

co::CoroutineScheduler ShadowTest::server_scheduler_;
co::CoroutineScheduler ShadowTest::shadow_scheduler_;
int ShadowTest::server_pipe_[2];
int ShadowTest::shadow_pipe_[2];
std::unique_ptr<subspace::Server> ShadowTest::server_;
std::thread ShadowTest::server_thread_;
std::unique_ptr<subspace::Shadow> ShadowTest::shadow_;
std::thread ShadowTest::shadow_thread_;

TEST_F(ShadowTest, ShadowReceivesInit) {
  ASSERT_TRUE(WaitForShadowState(
      []() { return GetShadow()->GetSessionId() != 0; }));
  EXPECT_NE(GetShadow()->GetSessionId(), 0u);
  EXPECT_TRUE(GetShadow()->GetScbFd().Valid());
}

TEST_F(ShadowTest, ShadowReceivesCreateChannel) {
  subspace::Client client;
  InitClient(client);

  auto pub = client.CreatePublisher("shadow_test_chan1", 256, 4);
  ASSERT_THAT(pub, IsOk());

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->WithChannels([](auto &channels) {
      auto it = channels.find("shadow_test_chan1");
      return it != channels.end() && it->second.ccb_fd.Valid();
    });
  }));

  GetShadow()->WithChannels([](auto &channels) {
    auto it = channels.find("shadow_test_chan1");
    ASSERT_NE(it, channels.end());
    EXPECT_EQ(it->second.slot_size, 256);
    EXPECT_EQ(it->second.num_slots, 4);
    EXPECT_TRUE(it->second.ccb_fd.Valid());
    EXPECT_TRUE(it->second.bcb_fd.Valid());
  });
}

TEST_F(ShadowTest, ShadowReceivesAddPublisher) {
  subspace::Client client;
  InitClient(client);

  auto pub = client.CreatePublisher("shadow_test_chan2", 128, 2);
  ASSERT_THAT(pub, IsOk());

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->WithChannels([](auto &channels) {
      auto it = channels.find("shadow_test_chan2");
      return it != channels.end() && it->second.publishers.size() == 1;
    });
  }));

  GetShadow()->WithChannels([](auto &channels) {
    auto it = channels.find("shadow_test_chan2");
    ASSERT_NE(it, channels.end());
    EXPECT_EQ(it->second.publishers.size(), 1u);

    auto &pub_entry = it->second.publishers.begin()->second;
    EXPECT_TRUE(pub_entry.poll_fd.Valid());
    EXPECT_TRUE(pub_entry.trigger_fd.Valid());
  });
}

TEST_F(ShadowTest, ShadowReceivesAddSubscriber) {
  subspace::Client client;
  InitClient(client);

  auto pub = client.CreatePublisher("shadow_test_chan3", 128, 4);
  ASSERT_THAT(pub, IsOk());
  auto sub = client.CreateSubscriber("shadow_test_chan3");
  ASSERT_THAT(sub, IsOk());

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->WithChannels([](auto &channels) {
      auto it = channels.find("shadow_test_chan3");
      return it != channels.end() && it->second.subscribers.size() == 1;
    });
  }));

  GetShadow()->WithChannels([](auto &channels) {
    auto it = channels.find("shadow_test_chan3");
    ASSERT_NE(it, channels.end());
    EXPECT_EQ(it->second.publishers.size(), 1u);
    EXPECT_EQ(it->second.subscribers.size(), 1u);

    auto &sub_entry = it->second.subscribers.begin()->second;
    EXPECT_TRUE(sub_entry.trigger_fd.Valid());
    EXPECT_TRUE(sub_entry.poll_fd.Valid());
  });
}

TEST_F(ShadowTest, ShadowReceivesRemovePublisher) {
  subspace::Client client;
  InitClient(client);

  {
    auto pub = client.CreatePublisher("shadow_test_chan4", 128, 2);
    ASSERT_THAT(pub, IsOk());

    ASSERT_TRUE(WaitForShadowState([]() {
      return GetShadow()->WithChannels([](auto &channels) {
        auto it = channels.find("shadow_test_chan4");
        return it != channels.end() && it->second.publishers.size() == 1;
      });
    }));
  }

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->WithChannels([](auto &channels) {
      return channels.count("shadow_test_chan4") == 0;
    });
  }));
}

TEST_F(ShadowTest, ShadowReceivesRemoveSubscriber) {
  subspace::Client client;
  InitClient(client);

  auto pub = client.CreatePublisher("shadow_test_chan5", 128, 4);
  ASSERT_THAT(pub, IsOk());
  {
    auto sub = client.CreateSubscriber("shadow_test_chan5");
    ASSERT_THAT(sub, IsOk());

    ASSERT_TRUE(WaitForShadowState([]() {
      return GetShadow()->WithChannels([](auto &channels) {
        auto it = channels.find("shadow_test_chan5");
        return it != channels.end() && it->second.subscribers.size() == 1;
      });
    }));
  }

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->WithChannels([](auto &channels) {
      auto it = channels.find("shadow_test_chan5");
      return it != channels.end() && it->second.subscribers.empty();
    });
  }));
}

TEST_F(ShadowTest, ShadowReceivesRemoveChannel) {
  subspace::Client client;
  InitClient(client);

  {
    auto pub = client.CreatePublisher("shadow_test_chan6", 128, 2);
    ASSERT_THAT(pub, IsOk());

    ASSERT_TRUE(WaitForShadowState([]() {
      return GetShadow()->WithChannels([](auto &channels) {
        return channels.count("shadow_test_chan6") > 0;
      });
    }));
  }

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->WithChannels([](auto &channels) {
      return channels.count("shadow_test_chan6") == 0;
    });
  }));
}

TEST_F(ShadowTest, ServerWithoutShadowSocketWorks) {
  co::CoroutineScheduler sched;
  subspace::Server server(sched, "/tmp/subspace_noshadow", "", 0, 0, true, -1,
                          1, false, false);
  EXPECT_EQ(server.GetPrimaryShadowReplicator(), nullptr);
  EXPECT_EQ(server.GetSecondaryShadowReplicator(), nullptr);

  server.SetShadowSocket("");
  EXPECT_EQ(server.GetPrimaryShadowReplicator(), nullptr);

  server.SetShadowSocket("/tmp/some_shadow");
  EXPECT_NE(server.GetPrimaryShadowReplicator(), nullptr);
  EXPECT_EQ(server.GetSecondaryShadowReplicator(), nullptr);

  server.SetShadowSockets("/tmp/primary", "/tmp/secondary");
  EXPECT_NE(server.GetPrimaryShadowReplicator(), nullptr);
  EXPECT_NE(server.GetSecondaryShadowReplicator(), nullptr);
}

constexpr const char *kRecoveryServerSocket = "/tmp/subspace_recovery_test";
constexpr const char *kRecoveryShadowSocket = "/tmp/subspace_recovery_shadow";

class ShadowRecoveryTest : public ::testing::Test {
protected:
  void StartShadow() {
    (void)pipe(shadow_pipe_);
    shadow_ = std::make_unique<subspace::Shadow>(shadow_scheduler_,
                                                  kRecoveryShadowSocket);
    shadow_->SetLogLevel("verbose");
    shadow_->SetNotifyFd(shadow_pipe_[1]);

    shadow_thread_ = std::thread([this]() {
      absl::Status s = shadow_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Shadow error: %s\n", s.ToString().c_str());
      }
    });

    char c;
    (void)::read(shadow_pipe_[0], &c, 1);
  }

  void StartServer() {
    server_scheduler_ = std::make_unique<co::CoroutineScheduler>();
    (void)pipe(server_pipe_);
    server_ = std::make_unique<subspace::Server>(
        *server_scheduler_, kRecoveryServerSocket, "", 0, 0, true,
        server_pipe_[1], 1, true, false);
    server_->SetShadowSocket(kRecoveryShadowSocket);

    server_thread_ = std::thread([this]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Server error: %s\n", s.ToString().c_str());
      }
    });

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  void StopServer() {
    server_->Stop(true);
    server_thread_.join();
    close(server_pipe_[0]);
    close(server_pipe_[1]);
    (void)remove(kRecoveryServerSocket);
    server_.reset();
    server_scheduler_.reset();
  }

  void StopShadow() {
    shadow_->Stop();
    shadow_thread_.join();
    close(shadow_pipe_[0]);
    close(shadow_pipe_[1]);
    shadow_.reset();
  }

  bool WaitForShadowState(std::function<bool()> condition,
                           int timeout_ms = 5000) {
    if (condition()) {
      return true;
    }
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    for (;;) {
      auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        return condition();
      }
      int remaining_ms = static_cast<int>(
          std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
              .count());
      struct pollfd pfd;
      pfd.fd = shadow_pipe_[0];
      pfd.events = POLLIN;
      pfd.revents = 0;
      int ret = ::poll(&pfd, 1, remaining_ms);
      if (ret > 0 && (pfd.revents & POLLIN)) {
        char drain[64];
        (void)::read(shadow_pipe_[0], drain, sizeof(drain));
      }
      if (condition()) {
        return true;
      }
    }
  }

  co::CoroutineScheduler shadow_scheduler_;
  std::unique_ptr<co::CoroutineScheduler> server_scheduler_;
  int server_pipe_[2] = {-1, -1};
  int shadow_pipe_[2] = {-1, -1};
  std::unique_ptr<subspace::Server> server_;
  std::thread server_thread_;
  std::unique_ptr<subspace::Shadow> shadow_;
  std::thread shadow_thread_;
};

TEST_F(ShadowRecoveryTest, ServerRecoversStateFromShadow) {
  signal(SIGPIPE, SIG_IGN);

  StartShadow();
  StartServer();

  // Create a publisher and subscriber via a client.  Keep the client
  // alive until we stop the server so the pub/sub remain registered.
  subspace::Client client;
  client.SetThreadSafe(true);
  ASSERT_THAT(client.Init(kRecoveryServerSocket), IsOk());

  auto pub = client.CreatePublisher("recovery_chan", 256, 4);
  ASSERT_THAT(pub, IsOk());

  auto sub = client.CreateSubscriber("recovery_chan");
  ASSERT_THAT(sub, IsOk());

  // Wait for shadow to receive the full state.
  ASSERT_TRUE(WaitForShadowState([this]() {
    return shadow_->WithChannels([](auto &channels) {
      auto it = channels.find("recovery_chan");
      return it != channels.end() && it->second.publishers.size() == 1 &&
             it->second.subscribers.size() == 1;
    });
  }));

  // Record the shadow state before stopping the server.
  uint64_t old_session_id = shadow_->GetSessionId();
  ASSERT_NE(old_session_id, 0u);

  int old_channel_id = shadow_->WithChannels([](auto &channels) {
    auto it = channels.find("recovery_chan");
    EXPECT_NE(it, channels.end());
    return it->second.channel_id;
  });

  // Simulate a crash: disconnect the shadow replicators first so that
  // cleanup events from Stop() don't reach the shadow.
  server_->ForEachShadow([](const std::unique_ptr<subspace::ShadowReplicator> &s) { s->Close(); });
  StopServer();

  // Start a new server that connects to the same shadow.
  StartServer();

  // The new server should have recovered the channel with its publisher
  // and subscriber.
  auto &recovered_channels = server_->GetChannels();
  ASSERT_EQ(recovered_channels.count("recovery_chan"), 1u);

  auto *channel = recovered_channels.at("recovery_chan").get();
  EXPECT_EQ(channel->GetChannelId(), old_channel_id);
  EXPECT_EQ(channel->NumSlots(), 4);

  int num_pubs = 0, num_subs = 0, num_bridge_pubs = 0, num_bridge_subs = 0;
  int num_tunnel_pubs = 0, num_tunnel_subs = 0;
  channel->CountUsers(num_pubs, num_subs, num_bridge_pubs, num_bridge_subs,
                      num_tunnel_pubs, num_tunnel_subs);
  EXPECT_EQ(num_pubs, 1);
  EXPECT_EQ(num_subs, 1);

  // The new server restores the old session_id for shared memory consistency.
  EXPECT_EQ(server_->GetSessionId(), old_session_id);

  // Wait for the shadow to receive the new init from the second server.
  ASSERT_TRUE(WaitForShadowState([this]() {
    return shadow_->GetSessionId() == server_->GetSessionId();
  }));

  // Clean up.
  StopServer();
  StopShadow();
}

TEST_F(ShadowRecoveryTest, ServerFunctionalAfterRecovery) {
  signal(SIGPIPE, SIG_IGN);

  StartShadow();
  StartServer();

  // Phase 1: Pre-crash -- create a channel with a publisher and subscriber,
  // then publish a message into it.
  subspace::Client pre_client;
  pre_client.SetThreadSafe(true);
  ASSERT_THAT(pre_client.Init(kRecoveryServerSocket), IsOk());

  auto pre_pub = pre_client.CreatePublisher("persist_chan", 256, 16);
  ASSERT_THAT(pre_pub, IsOk());
  {
    absl::StatusOr<void *> buf = pre_pub->GetMessageBuffer();
    ASSERT_THAT(buf, IsOk());
    memcpy(*buf, "before_crash", 12);
    ASSERT_THAT(pre_pub->PublishMessage(12), IsOk());
  }

  auto pre_sub = pre_client.CreateSubscriber("persist_chan");
  ASSERT_THAT(pre_sub, IsOk());

  ASSERT_TRUE(WaitForShadowState([this]() {
    return shadow_->WithChannels([](auto &ch) {
      auto it = ch.find("persist_chan");
      return it != ch.end() && it->second.publishers.size() == 1 &&
             it->second.subscribers.size() == 1;
    });
  }));

  // Simulate crash.
  server_->ForEachShadow([](const std::unique_ptr<subspace::ShadowReplicator> &s) { s->Close(); });
  StopServer();

  // Phase 2: Recovered server -- verify we can create new channels, publish
  // and subscribe on them, and that recovered channels still exist.
  StartServer();

  // Verify the old channel was recovered.
  ASSERT_EQ(server_->GetChannels().count("persist_chan"), 1u);

  // Connect a fresh client to the recovered server.
  subspace::Client post_client;
  post_client.SetThreadSafe(true);
  ASSERT_THAT(post_client.Init(kRecoveryServerSocket), IsOk());

  // (a) Create a brand-new channel, publish, and read.  Use a scope so
  //     the pub/sub are destroyed afterward, allowing the channel to be
  //     removed from the shadow.
  {
    auto new_pub = post_client.CreatePublisher("new_chan", 128, 4);
    ASSERT_THAT(new_pub, IsOk());
    {
      absl::StatusOr<void *> buf = new_pub->GetMessageBuffer();
      ASSERT_THAT(buf, IsOk());
      memcpy(*buf, "hello_post", 10);
      ASSERT_THAT(new_pub->PublishMessage(10), IsOk());
    }

    auto new_sub = post_client.CreateSubscriber("new_chan");
    ASSERT_THAT(new_sub, IsOk());
    {
      absl::StatusOr<subspace::Message> msg = new_sub->ReadMessage();
      ASSERT_THAT(msg, IsOk());
      EXPECT_EQ(msg->length, 10u);
      EXPECT_EQ(memcmp(msg->buffer, "hello_post", 10), 0);
    }

  }

  // (b) Create a publisher on the recovered channel and send/receive.
  {
    auto recover_pub = post_client.CreatePublisher("persist_chan", 256, 16);
    ASSERT_THAT(recover_pub, IsOk());
    {
      absl::StatusOr<void *> buf = recover_pub->GetMessageBuffer();
      ASSERT_THAT(buf, IsOk());
      memcpy(*buf, "after_crash", 11);
      ASSERT_THAT(recover_pub->PublishMessage(11), IsOk());
    }
    auto recover_sub = post_client.CreateSubscriber("persist_chan");
    ASSERT_THAT(recover_sub, IsOk());
    {
      absl::StatusOr<subspace::Message> msg =
          recover_sub->ReadMessage(subspace::ReadMode::kReadNewest);
      ASSERT_THAT(msg, IsOk());
      EXPECT_EQ(msg->length, 11u);
      EXPECT_EQ(memcmp(msg->buffer, "after_crash", 11), 0);
    }
  }

  // Clean up.
  StopServer();
  StopShadow();
}

TEST_F(ShadowRecoveryTest, ClientReconnectsAfterServerRestart) {
  signal(SIGPIPE, SIG_IGN);

  StartShadow();
  StartServer();

  // Phase 1: Create a publisher and subscriber, publish and read a message.
  subspace::Client client;
  client.SetThreadSafe(true);
  ASSERT_THAT(client.Init(kRecoveryServerSocket), IsOk());

  auto pub = client.CreatePublisher("reconnect_chan", 256, 16);
  ASSERT_THAT(pub, IsOk());

  auto sub = client.CreateSubscriber("reconnect_chan");
  ASSERT_THAT(sub, IsOk());

  {
    absl::StatusOr<void *> buf = pub->GetMessageBuffer();
    ASSERT_THAT(buf, IsOk());
    memcpy(*buf, "pre_crash", 9);
    ASSERT_THAT(pub->PublishMessage(9), IsOk());
  }
  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_THAT(msg, IsOk());
    EXPECT_EQ(msg->length, 9u);
    EXPECT_EQ(memcmp(msg->buffer, "pre_crash", 9), 0);
  }

  // Wait for shadow to have the full state.
  ASSERT_TRUE(WaitForShadowState([this]() {
    return shadow_->WithChannels([](auto &ch) {
      auto it = ch.find("reconnect_chan");
      return it != ch.end() && it->second.publishers.size() == 1 &&
             it->second.subscribers.size() == 1;
    });
  }));

  // Simulate crash: disconnect the shadow replicators so cleanup events
  // from Stop() don't reach the shadow, then stop the server.
  server_->ForEachShadow([](const std::unique_ptr<subspace::ShadowReplicator> &s) { s->Close(); });
  StopServer();

  // Phase 2: Restart the server (it recovers state from the shadow).
  StartServer();

  // The original client's socket is now dead.  The next RPC (creating a
  // new publisher) triggers automatic reconnection which reclaims the
  // original publisher and subscriber on "reconnect_chan".
  auto new_pub = client.CreatePublisher("reconnect_new_chan", 128, 4);
  ASSERT_THAT(new_pub, IsOk());

  auto new_sub = client.CreateSubscriber("reconnect_new_chan");
  ASSERT_THAT(new_sub, IsOk());

  // Verify the new channel works.
  {
    absl::StatusOr<void *> buf = new_pub->GetMessageBuffer();
    ASSERT_THAT(buf, IsOk());
    memcpy(*buf, "new_msg", 7);
    ASSERT_THAT(new_pub->PublishMessage(7), IsOk());
  }
  {
    absl::StatusOr<subspace::Message> msg = new_sub->ReadMessage();
    ASSERT_THAT(msg, IsOk());
    EXPECT_EQ(msg->length, 7u);
    EXPECT_EQ(memcmp(msg->buffer, "new_msg", 7), 0);
  }

  // Verify the original publisher and subscriber still work after
  // reclaim.  The IDs in the CCB should be the same as before.
  {
    absl::StatusOr<void *> buf = pub->GetMessageBuffer();
    ASSERT_THAT(buf, IsOk());
    memcpy(*buf, "post_crash", 10);
    ASSERT_THAT(pub->PublishMessage(10), IsOk());
  }
  {
    absl::StatusOr<subspace::Message> msg =
        sub->ReadMessage(subspace::ReadMode::kReadNewest);
    ASSERT_THAT(msg, IsOk());
    EXPECT_EQ(msg->length, 10u);
    EXPECT_EQ(memcmp(msg->buffer, "post_crash", 10), 0);
  }

  // Clean up.
  StopServer();
  StopShadow();
}

// ---------------------------------------------------------------------------
// Dual shadow tests
// ---------------------------------------------------------------------------

constexpr const char *kDualServerSocket = "/tmp/subspace_dual_shadow_test";
constexpr const char *kDualPrimaryShadowSocket =
    "/tmp/subspace_dual_shadow_primary";
constexpr const char *kDualSecondaryShadowSocket =
    "/tmp/subspace_dual_shadow_secondary";

class DualShadowRecoveryTest : public ::testing::Test {
protected:
  void StartShadow(std::unique_ptr<subspace::Shadow> &shadow,
                   co::CoroutineScheduler &sched, std::thread &thr,
                   int pipe_fds[2], const char *socket_path) {
    (void)pipe(pipe_fds);
    shadow = std::make_unique<subspace::Shadow>(sched, socket_path);
    shadow->SetLogLevel("verbose");
    shadow->SetNotifyFd(pipe_fds[1]);
    thr = std::thread([&shadow]() {
      absl::Status s = shadow->Run();
      if (!s.ok()) {
        fprintf(stderr, "Shadow error: %s\n", s.ToString().c_str());
      }
    });
    char c;
    (void)::read(pipe_fds[0], &c, 1);
  }

  void StopShadow(std::unique_ptr<subspace::Shadow> &shadow, std::thread &thr,
                  int pipe_fds[2]) {
    shadow->Stop();
    thr.join();
    close(pipe_fds[0]);
    close(pipe_fds[1]);
    pipe_fds[0] = pipe_fds[1] = -1;
    shadow.reset();
  }

  void StartServer(const std::string &primary_shadow = "",
                   const std::string &secondary_shadow = "") {
    server_scheduler_ = std::make_unique<co::CoroutineScheduler>();
    (void)pipe(server_pipe_);
    server_ = std::make_unique<subspace::Server>(
        *server_scheduler_, kDualServerSocket, "", 0, 0, true,
        server_pipe_[1], 1, true, false);
    server_->SetShadowSockets(primary_shadow, secondary_shadow);

    server_thread_ = std::thread([this]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Server error: %s\n", s.ToString().c_str());
      }
    });

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  void StopServer() {
    server_->Stop(true);
    server_thread_.join();
    close(server_pipe_[0]);
    close(server_pipe_[1]);
    (void)remove(kDualServerSocket);
    server_.reset();
    server_scheduler_.reset();
  }

  bool WaitForShadowState(int pipe_fd, std::function<bool()> condition,
                           int timeout_ms = 5000) {
    if (condition()) {
      return true;
    }
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    for (;;) {
      auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        return condition();
      }
      int remaining_ms = static_cast<int>(
          std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
              .count());
      struct pollfd pfd;
      pfd.fd = pipe_fd;
      pfd.events = POLLIN;
      pfd.revents = 0;
      int ret = ::poll(&pfd, 1, remaining_ms);
      if (ret > 0 && (pfd.revents & POLLIN)) {
        char drain[64];
        (void)::read(pipe_fd, drain, sizeof(drain));
      }
      if (condition()) {
        return true;
      }
    }
  }

  std::unique_ptr<co::CoroutineScheduler> server_scheduler_;
  int server_pipe_[2] = {-1, -1};
  std::unique_ptr<subspace::Server> server_;
  std::thread server_thread_;

  co::CoroutineScheduler primary_shadow_sched_;
  co::CoroutineScheduler secondary_shadow_sched_;
  int primary_shadow_pipe_[2] = {-1, -1};
  int secondary_shadow_pipe_[2] = {-1, -1};
  std::unique_ptr<subspace::Shadow> primary_shadow_;
  std::unique_ptr<subspace::Shadow> secondary_shadow_;
  std::thread primary_shadow_thread_;
  std::thread secondary_shadow_thread_;
};

TEST_F(DualShadowRecoveryTest, RecoverFromPrimaryWhenBothHealthy) {
  signal(SIGPIPE, SIG_IGN);

  StartShadow(primary_shadow_, primary_shadow_sched_, primary_shadow_thread_,
              primary_shadow_pipe_, kDualPrimaryShadowSocket);
  StartShadow(secondary_shadow_, secondary_shadow_sched_,
              secondary_shadow_thread_, secondary_shadow_pipe_,
              kDualSecondaryShadowSocket);
  StartServer(kDualPrimaryShadowSocket, kDualSecondaryShadowSocket);

  subspace::Client client;
  client.SetThreadSafe(true);
  ASSERT_THAT(client.Init(kDualServerSocket), IsOk());

  auto pub = client.CreatePublisher("dual_chan", 256, 4);
  ASSERT_THAT(pub, IsOk());
  auto sub = client.CreateSubscriber("dual_chan");
  ASSERT_THAT(sub, IsOk());

  // Wait for both shadows to receive the state.
  ASSERT_TRUE(WaitForShadowState(primary_shadow_pipe_[0], [this]() {
    return primary_shadow_->WithChannels([](auto &ch) {
      auto it = ch.find("dual_chan");
      return it != ch.end() && it->second.publishers.size() == 1 &&
             it->second.subscribers.size() == 1;
    });
  }));
  ASSERT_TRUE(WaitForShadowState(secondary_shadow_pipe_[0], [this]() {
    return secondary_shadow_->WithChannels([](auto &ch) {
      auto it = ch.find("dual_chan");
      return it != ch.end() && it->second.publishers.size() == 1 &&
             it->second.subscribers.size() == 1;
    });
  }));

  uint64_t primary_session = primary_shadow_->GetSessionId();
  ASSERT_NE(primary_session, 0u);

  // Simulate crash.
  server_->ForEachShadow([](const std::unique_ptr<subspace::ShadowReplicator> &s) { s->Close(); });
  StopServer();

  // Restart server -- both shadows are healthy, so primary should be used.
  StartServer(kDualPrimaryShadowSocket, kDualSecondaryShadowSocket);

  auto &channels = server_->GetChannels();
  ASSERT_EQ(channels.count("dual_chan"), 1u);
  auto *channel = channels.at("dual_chan").get();
  int np = 0, ns = 0, nbp = 0, nbs = 0;
  int ntp = 0, nts = 0;
  channel->CountUsers(np, ns, nbp, nbs, ntp, nts);
  EXPECT_EQ(np, 1);
  EXPECT_EQ(ns, 1);

  // Both shadows should now have the new session.
  ASSERT_TRUE(WaitForShadowState(primary_shadow_pipe_[0], [this]() {
    return primary_shadow_->GetSessionId() == server_->GetSessionId();
  }));
  ASSERT_TRUE(WaitForShadowState(secondary_shadow_pipe_[0], [this]() {
    return secondary_shadow_->GetSessionId() == server_->GetSessionId();
  }));

  StopServer();
  StopShadow(primary_shadow_, primary_shadow_thread_, primary_shadow_pipe_);
  StopShadow(secondary_shadow_, secondary_shadow_thread_,
             secondary_shadow_pipe_);
}

TEST_F(DualShadowRecoveryTest, RecoverFromSecondaryWhenPrimaryDown) {
  signal(SIGPIPE, SIG_IGN);

  StartShadow(primary_shadow_, primary_shadow_sched_, primary_shadow_thread_,
              primary_shadow_pipe_, kDualPrimaryShadowSocket);
  StartShadow(secondary_shadow_, secondary_shadow_sched_,
              secondary_shadow_thread_, secondary_shadow_pipe_,
              kDualSecondaryShadowSocket);
  StartServer(kDualPrimaryShadowSocket, kDualSecondaryShadowSocket);

  subspace::Client client;
  client.SetThreadSafe(true);
  ASSERT_THAT(client.Init(kDualServerSocket), IsOk());

  auto pub = client.CreatePublisher("dual_fallback_chan", 256, 4);
  ASSERT_THAT(pub, IsOk());

  // Wait for both shadows to have the channel.
  ASSERT_TRUE(WaitForShadowState(primary_shadow_pipe_[0], [this]() {
    return primary_shadow_->WithChannels([](auto &ch) {
      return ch.count("dual_fallback_chan") > 0;
    });
  }));
  ASSERT_TRUE(WaitForShadowState(secondary_shadow_pipe_[0], [this]() {
    return secondary_shadow_->WithChannels([](auto &ch) {
      return ch.count("dual_fallback_chan") > 0;
    });
  }));

  // Simulate crash.
  server_->ForEachShadow([](const std::unique_ptr<subspace::ShadowReplicator> &s) { s->Close(); });
  StopServer();

  // Stop the primary shadow before restarting the server.
  StopShadow(primary_shadow_, primary_shadow_thread_, primary_shadow_pipe_);
  (void)remove(kDualPrimaryShadowSocket);

  // Restart server -- primary is down, so secondary should be used.
  StartServer(kDualPrimaryShadowSocket, kDualSecondaryShadowSocket);

  auto &channels = server_->GetChannels();
  ASSERT_EQ(channels.count("dual_fallback_chan"), 1u);

  // Only secondary shadow should have the new session.
  ASSERT_TRUE(WaitForShadowState(secondary_shadow_pipe_[0], [this]() {
    return secondary_shadow_->GetSessionId() == server_->GetSessionId();
  }));

  StopServer();
  StopShadow(secondary_shadow_, secondary_shadow_thread_,
             secondary_shadow_pipe_);
}

TEST_F(DualShadowRecoveryTest, FreshStartWhenBothShadowsEmpty) {
  signal(SIGPIPE, SIG_IGN);

  // Start fresh shadows (no prior state).
  StartShadow(primary_shadow_, primary_shadow_sched_, primary_shadow_thread_,
              primary_shadow_pipe_, kDualPrimaryShadowSocket);
  StartShadow(secondary_shadow_, secondary_shadow_sched_,
              secondary_shadow_thread_, secondary_shadow_pipe_,
              kDualSecondaryShadowSocket);

  // Start the server -- neither shadow has any state to recover.
  StartServer(kDualPrimaryShadowSocket, kDualSecondaryShadowSocket);

  // Server should have no channels (fresh start).
  EXPECT_TRUE(server_->GetChannels().empty());

  // Create a channel to verify server is functional.
  subspace::Client client;
  client.SetThreadSafe(true);
  ASSERT_THAT(client.Init(kDualServerSocket), IsOk());

  auto pub = client.CreatePublisher("fresh_chan", 128, 4);
  ASSERT_THAT(pub, IsOk());
  {
    absl::StatusOr<void *> buf = pub->GetMessageBuffer();
    ASSERT_THAT(buf, IsOk());
    memcpy(*buf, "fresh", 5);
    ASSERT_THAT(pub->PublishMessage(5), IsOk());
  }

  auto sub = client.CreateSubscriber("fresh_chan");
  ASSERT_THAT(sub, IsOk());
  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_THAT(msg, IsOk());
    EXPECT_EQ(msg->length, 5u);
    EXPECT_EQ(memcmp(msg->buffer, "fresh", 5), 0);
  }

  // Both shadows should have received the new state.
  ASSERT_TRUE(WaitForShadowState(primary_shadow_pipe_[0], [this]() {
    return primary_shadow_->WithChannels([](auto &ch) {
      return ch.count("fresh_chan") > 0;
    });
  }));
  ASSERT_TRUE(WaitForShadowState(secondary_shadow_pipe_[0], [this]() {
    return secondary_shadow_->WithChannels([](auto &ch) {
      return ch.count("fresh_chan") > 0;
    });
  }));

  StopServer();
  StopShadow(primary_shadow_, primary_shadow_thread_, primary_shadow_pipe_);
  StopShadow(secondary_shadow_, secondary_shadow_thread_,
             secondary_shadow_pipe_);
}

// ---------------------------------------------------------------------------
// Bridge + shadow recovery test
// ---------------------------------------------------------------------------

constexpr const char *kBridgeServer0Socket = "/tmp/subspace_bridge_shadow_s0";
constexpr const char *kBridgeServer1Socket = "/tmp/subspace_bridge_shadow_s1";
constexpr const char *kBridgeShadowSocket = "/tmp/subspace_bridge_shadow_shd";
constexpr int kBridgeDiscPort0 = 6530;
constexpr int kBridgeDiscPort1 = 6531;

static void WaitForSubscribedMessage(toolbelt::FileDescriptor &bridge_pipe,
                                     const std::string &channel_name,
                                     int timeout_ms = 10000) {
  struct pollfd pfd;
  pfd.fd = bridge_pipe.Fd();
  pfd.events = POLLIN;
  pfd.revents = 0;
  int ret = ::poll(&pfd, 1, timeout_ms);
  ASSERT_GT(ret, 0) << "Timed out waiting for Subscribed message on pipe";

  char buffer[4096];
  int32_t length;
  absl::StatusOr<ssize_t> n = bridge_pipe.Read(&length, sizeof(length));
  ASSERT_THAT(n, IsOk());
  ASSERT_EQ(sizeof(int32_t), static_cast<size_t>(*n));
  length = ntohl(length);
  n = bridge_pipe.Read(buffer, length);
  ASSERT_THAT(n, IsOk());

  subspace::Subscribed subscribed;
  ASSERT_TRUE(subscribed.ParseFromArray(buffer, *n));
  ASSERT_EQ(subscribed.channel_name(), channel_name);
}

class BridgeShadowRecoveryTest : public ::testing::Test {
protected:
  void StartShadow() {
    (void)pipe(shadow_pipe_);
    shadow_ = std::make_unique<subspace::Shadow>(shadow_scheduler_,
                                                  kBridgeShadowSocket);
    shadow_->SetLogLevel("verbose");
    shadow_->SetNotifyFd(shadow_pipe_[1]);
    shadow_thread_ = std::thread([this]() {
      absl::Status s = shadow_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Shadow error: %s\n", s.ToString().c_str());
      }
    });
    char c;
    (void)::read(shadow_pipe_[0], &c, 1);
  }

  void StopShadow() {
    shadow_->Stop();
    shadow_thread_.join();
    close(shadow_pipe_[0]);
    close(shadow_pipe_[1]);
    shadow_.reset();
  }

  void StartServer0() {
    server0_scheduler_ = std::make_unique<co::CoroutineScheduler>();
    (void)pipe(server0_pipe_);
    server0_ = std::make_unique<subspace::Server>(
        *server0_scheduler_, kBridgeServer0Socket, "", kBridgeDiscPort0,
        kBridgeDiscPort1, /*local=*/false, server0_pipe_[1]);
    server0_->SetLogLevel("debug");
    server0_->SetShadowSocket(kBridgeShadowSocket);
    auto bp = server0_->CreateBridgeNotificationPipe();
    ASSERT_THAT(bp, IsOk());
    server0_bridge_pipe_ = *bp;

    server0_thread_ = std::thread([this]() {
      absl::Status s = server0_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Server0 error: %s\n", s.ToString().c_str());
      }
    });
    char buf[8];
    (void)::read(server0_pipe_[0], buf, 8);
  }

  void StopServer0() {
    server0_->Stop(true);
    server0_thread_.join();
    close(server0_pipe_[0]);
    close(server0_pipe_[1]);
    (void)remove(kBridgeServer0Socket);
    server0_.reset();
    server0_scheduler_.reset();
    server0_bridge_pipe_.Close();
  }

  void StartServer1() {
    (void)pipe(server1_pipe_);
    server1_ = std::make_unique<subspace::Server>(
        server1_scheduler_, kBridgeServer1Socket, "", kBridgeDiscPort1,
        kBridgeDiscPort0, /*local=*/false, server1_pipe_[1]);
    server1_->SetLogLevel("debug");
    auto bp = server1_->CreateBridgeNotificationPipe();
    ASSERT_THAT(bp, IsOk());
    server1_bridge_pipe_ = *bp;

    server1_thread_ = std::thread([this]() {
      absl::Status s = server1_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Server1 error: %s\n", s.ToString().c_str());
      }
    });
    char buf[8];
    (void)::read(server1_pipe_[0], buf, 8);
  }

  void StopServer1() {
    server1_->Stop(true);
    server1_thread_.join();
    close(server1_pipe_[0]);
    close(server1_pipe_[1]);
    (void)remove(kBridgeServer1Socket);
    server1_.reset();
    server1_bridge_pipe_.Close();
  }

  bool WaitForShadowState(std::function<bool()> condition,
                           int timeout_ms = 5000) {
    if (condition()) {
      return true;
    }
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    for (;;) {
      auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        return condition();
      }
      int remaining_ms = static_cast<int>(
          std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
              .count());
      struct pollfd pfd;
      pfd.fd = shadow_pipe_[0];
      pfd.events = POLLIN;
      pfd.revents = 0;
      int ret = ::poll(&pfd, 1, remaining_ms);
      if (ret > 0 && (pfd.revents & POLLIN)) {
        char drain[64];
        (void)::read(shadow_pipe_[0], drain, sizeof(drain));
      }
      if (condition()) {
        return true;
      }
    }
  }

  co::CoroutineScheduler shadow_scheduler_;
  co::CoroutineScheduler server1_scheduler_;
  std::unique_ptr<co::CoroutineScheduler> server0_scheduler_;

  int shadow_pipe_[2] = {-1, -1};
  int server0_pipe_[2] = {-1, -1};
  int server1_pipe_[2] = {-1, -1};

  std::unique_ptr<subspace::Shadow> shadow_;
  std::unique_ptr<subspace::Server> server0_;
  std::unique_ptr<subspace::Server> server1_;

  std::thread shadow_thread_;
  std::thread server0_thread_;
  std::thread server1_thread_;

  toolbelt::FileDescriptor server0_bridge_pipe_;
  toolbelt::FileDescriptor server1_bridge_pipe_;
};

TEST_F(BridgeShadowRecoveryTest, BridgeRecoversAfterServerRestart) {
  signal(SIGPIPE, SIG_IGN);

  StartShadow();
  StartServer0();
  StartServer1();

  // Client 0 on server 0: create a non-local publisher.
  subspace::Client client0;
  client0.SetThreadSafe(true);
  ASSERT_THAT(client0.Init(kBridgeServer0Socket), IsOk());

  auto pub = client0.CreatePublisher(
      "/bridge_recovery_chan",
      {.slot_size = 256, .num_slots = 10, .local = false});
  ASSERT_THAT(pub, IsOk());

  // Client 1 on server 1: create a subscriber.
  subspace::Client client1;
  client1.SetThreadSafe(true);
  ASSERT_THAT(client1.Init(kBridgeServer1Socket), IsOk());

  auto sub = client1.CreateSubscriber("/bridge_recovery_chan",
                                      {.max_active_messages = 2});
  ASSERT_THAT(sub, IsOk());

  // Wait for bridge to be established.  Server 1's BridgeReceiverCoroutine
  // writes to server1_bridge_pipe_ when it receives the Subscribed message.
  WaitForSubscribedMessage(server1_bridge_pipe_, "/bridge_recovery_chan");

  // Verify bridge works: publish and read a message.
  {
    absl::StatusOr<void *> buf = pub->GetMessageBuffer();
    ASSERT_THAT(buf, IsOk());
    memcpy(*buf, "pre_crash", 9);
    ASSERT_THAT(pub->PublishMessage(9), IsOk());
  }
  {
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_THAT(msg, IsOk());
    EXPECT_EQ(msg->length, 9u);
    EXPECT_EQ(memcmp(msg->buffer, "pre_crash", 9), 0);
  }

  // Wait for shadow to replicate the channel state.
  ASSERT_TRUE(WaitForShadowState([this]() {
    return shadow_->WithChannels([](auto &ch) {
      auto it = ch.find("/bridge_recovery_chan");
      return it != ch.end() && it->second.publishers.size() >= 1;
    });
  }));

  // Simulate crash on server 0.  SimulateCrash() prevents the
  // destructor from cleaning up shared memory files.
  server0_->ForEachShadow(
      [](const std::unique_ptr<subspace::ShadowReplicator> &s) { s->Close(); });
  server0_->SimulateCrash();
  StopServer0();

  // Restart server 0 -- it recovers from shadow and immediately advertises.
  StartServer0();

  // Wait for bridge to re-establish.  The restarted server 0 sends an
  // immediate Advertise, server 1 receives it, cleans up the stale
  // bridged entry (via BridgeReceiverCoroutine cleanup), and sets up
  // a new bridge.
  WaitForSubscribedMessage(server1_bridge_pipe_, "/bridge_recovery_chan");

  // Publish a message on server 0 and verify it arrives on server 1
  // through the recovered bridge.
  {
    absl::StatusOr<void *> buf = pub->GetMessageBuffer();
    ASSERT_THAT(buf, IsOk());
    memcpy(*buf, "post_crash", 10);
    ASSERT_THAT(pub->PublishMessage(10), IsOk());
  }

  // Wait for the message to arrive via the bridge.  The subscriber may be
  // triggered by bridge setup (publisher being added) before the actual
  // message arrives, so loop until we read the expected content.
  bool received = false;
  while (!received) {
    ASSERT_THAT(sub->Wait(std::chrono::seconds(10)), IsOk());
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_THAT(msg, IsOk());
    if (msg->length == 10u &&
        memcmp(msg->buffer, "post_crash", 10) == 0) {
      received = true;
    }
  }

  StopServer0();
  StopServer1();
  StopShadow();
}

} // namespace
