// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "shadow/shadow.h"
#include "server/server.h"
#include "subspace/client/client.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"
#include <chrono>
#include <csignal>
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
    auto &channels = GetShadow()->GetChannels();
    auto it = channels.find("shadow_test_chan1");
    return it != channels.end() && it->second.ccb_fd.Valid();
  }));

  auto &channels = GetShadow()->GetChannels();
  auto it = channels.find("shadow_test_chan1");
  ASSERT_NE(it, channels.end());
  EXPECT_EQ(it->second.slot_size, 256);
  EXPECT_EQ(it->second.num_slots, 4);
  EXPECT_TRUE(it->second.ccb_fd.Valid());
  EXPECT_TRUE(it->second.bcb_fd.Valid());
}

TEST_F(ShadowTest, ShadowReceivesAddPublisher) {
  subspace::Client client;
  InitClient(client);

  auto pub = client.CreatePublisher("shadow_test_chan2", 128, 2);
  ASSERT_THAT(pub, IsOk());

  ASSERT_TRUE(WaitForShadowState([]() {
    auto &channels = GetShadow()->GetChannels();
    auto it = channels.find("shadow_test_chan2");
    return it != channels.end() && it->second.publishers.size() == 1;
  }));

  auto &channels = GetShadow()->GetChannels();
  auto it = channels.find("shadow_test_chan2");
  ASSERT_NE(it, channels.end());
  EXPECT_EQ(it->second.publishers.size(), 1u);

  auto &pub_entry = it->second.publishers.begin()->second;
  EXPECT_TRUE(pub_entry.poll_fd.Valid());
  EXPECT_TRUE(pub_entry.trigger_fd.Valid());
}

TEST_F(ShadowTest, ShadowReceivesAddSubscriber) {
  subspace::Client client;
  InitClient(client);

  auto pub = client.CreatePublisher("shadow_test_chan3", 128, 4);
  ASSERT_THAT(pub, IsOk());
  auto sub = client.CreateSubscriber("shadow_test_chan3");
  ASSERT_THAT(sub, IsOk());

  ASSERT_TRUE(WaitForShadowState([]() {
    auto &channels = GetShadow()->GetChannels();
    auto it = channels.find("shadow_test_chan3");
    return it != channels.end() && it->second.subscribers.size() == 1;
  }));

  auto &channels = GetShadow()->GetChannels();
  auto it = channels.find("shadow_test_chan3");
  ASSERT_NE(it, channels.end());
  EXPECT_EQ(it->second.publishers.size(), 1u);
  EXPECT_EQ(it->second.subscribers.size(), 1u);

  auto &sub_entry = it->second.subscribers.begin()->second;
  EXPECT_TRUE(sub_entry.trigger_fd.Valid());
  EXPECT_TRUE(sub_entry.poll_fd.Valid());
}

TEST_F(ShadowTest, ShadowReceivesRemovePublisher) {
  subspace::Client client;
  InitClient(client);

  {
    auto pub = client.CreatePublisher("shadow_test_chan4", 128, 2);
    ASSERT_THAT(pub, IsOk());

    ASSERT_TRUE(WaitForShadowState([]() {
      auto &channels = GetShadow()->GetChannels();
      auto it = channels.find("shadow_test_chan4");
      return it != channels.end() && it->second.publishers.size() == 1;
    }));
  }

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->GetChannels().count("shadow_test_chan4") == 0;
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
      auto &channels = GetShadow()->GetChannels();
      auto it = channels.find("shadow_test_chan5");
      return it != channels.end() && it->second.subscribers.size() == 1;
    }));
  }

  ASSERT_TRUE(WaitForShadowState([]() {
    auto &channels = GetShadow()->GetChannels();
    auto it = channels.find("shadow_test_chan5");
    return it != channels.end() && it->second.subscribers.empty();
  }));
}

TEST_F(ShadowTest, ShadowReceivesRemoveChannel) {
  subspace::Client client;
  InitClient(client);

  {
    auto pub = client.CreatePublisher("shadow_test_chan6", 128, 2);
    ASSERT_THAT(pub, IsOk());

    ASSERT_TRUE(WaitForShadowState([]() {
      return GetShadow()->GetChannels().count("shadow_test_chan6") > 0;
    }));
  }

  ASSERT_TRUE(WaitForShadowState([]() {
    return GetShadow()->GetChannels().count("shadow_test_chan6") == 0;
  }));
}

TEST_F(ShadowTest, ServerWithoutShadowSocketWorks) {
  co::CoroutineScheduler sched;
  subspace::Server server(sched, "/tmp/subspace_noshadow", "", 0, 0, true, -1,
                          1, false, false);
  EXPECT_EQ(server.GetShadowReplicator(), nullptr);

  server.SetShadowSocket("");
  EXPECT_EQ(server.GetShadowReplicator(), nullptr);

  server.SetShadowSocket("/tmp/some_shadow");
  EXPECT_NE(server.GetShadowReplicator(), nullptr);
}

} // namespace
