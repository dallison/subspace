#include "dashboard/engine.h"

#include "client/test_fixture.h"
#include "gtest/gtest.h"

#include <chrono>
#include <string_view>

namespace subspace::dashboard {
namespace {

class TestApplication : public retro::Cpp20Application {
public:
  absl::Status Init() override { return absl::OkStatus(); }
};

class DashboardEngineTest : public ::SubspaceTestBase {};

TEST_F(DashboardEngineTest, PublishesContinuouslyWithoutRateInput) {
  DashboardEngine engine(
      {.socket = Socket(), .channel = "/dashboard-engine-initial-rate-test"});
  ASSERT_TRUE(engine.Init().ok());

  TestApplication app;
  engine.StartTasks(app);
  app.Spawn(
      [&engine](co20::Coroutine &c) -> co20::Task {
        co_await c.Sleep(std::chrono::milliseconds(1250));
        engine.RequestStop();
        co_return;
      },
      "test-controller");
  app.Cpp20Scheduler().Run();

  const auto snapshot = engine.Snapshot();
  EXPECT_GE(snapshot.published_messages, 2);
  EXPECT_GE(snapshot.received_messages, 2);
  EXPECT_TRUE(snapshot.error.empty());
}

TEST_F(DashboardEngineTest, PublishesAndReceivesAtSelectedRate) {
  DashboardEngine engine(
      {.socket = Socket(), .channel = "/dashboard-engine-test"});
  ASSERT_TRUE(engine.Init().ok());

  TestApplication app;
  engine.StartTasks(app);
  app.Spawn(
      [&engine](co20::Coroutine &c) -> co20::Task {
        EXPECT_TRUE(engine.IncreaseRate()); // 10 Hz.
        EXPECT_TRUE(engine.IncreaseRate()); // 100 Hz.
        co_await c.Sleep(std::chrono::milliseconds(500));
        engine.RequestStop();
        co_return;
      },
      "test-controller");
  app.Cpp20Scheduler().Run();

  const auto snapshot = engine.Snapshot();
  EXPECT_GE(snapshot.published_messages, 20);
  EXPECT_GE(snapshot.received_messages, 20);
  EXPECT_TRUE(snapshot.error.empty());
}

TEST_F(DashboardEngineTest, HighestRateDoesNotStarveShutdown) {
  DashboardEngine engine(
      {.socket = Socket(), .channel = "/dashboard-engine-max-rate-test"});
  ASSERT_TRUE(engine.Init().ok());

  TestApplication app;
  engine.StartTasks(app);
  app.Spawn(
      [&engine](co20::Coroutine &c) -> co20::Task {
        while (engine.Rate().setting.hz < 10'000'000.0) {
          EXPECT_TRUE(engine.IncreaseRate());
        }
        co_await c.Sleep(std::chrono::milliseconds(100));
        engine.RequestStop();
        co_return;
      },
      "test-controller");
  app.Cpp20Scheduler().Run();

  const auto snapshot = engine.Snapshot();
  EXPECT_GE(snapshot.published_messages, 100);
  EXPECT_GE(snapshot.received_messages, 100);
  EXPECT_TRUE(engine.IsStopping());
  EXPECT_TRUE(snapshot.error.empty());
}

TEST_F(DashboardEngineTest, SamplesHighRateThroughputInScaledRange) {
  DashboardEngine engine(
      {.socket = Socket(), .channel = "/dashboard-engine-scaled-rate-test"});
  ASSERT_TRUE(engine.Init().ok());

  TestApplication app;
  engine.StartTasks(app);
  app.Spawn(
      [&engine](co20::Coroutine &c) -> co20::Task {
        while (engine.Rate().setting.hz < 1'000'000.0) {
          EXPECT_TRUE(engine.IncreaseRate());
        }
        co_await c.Sleep(std::chrono::milliseconds(500));
        engine.RequestStop();
        co_return;
      },
      "test-controller");
  app.Cpp20Scheduler().Run();

  const auto snapshot = engine.Snapshot();
  const auto &throughput = snapshot.throughput_hz;
  ASSERT_TRUE(throughput.valid);
  EXPECT_GE(throughput.current, 1'000.0);
  EXPECT_NE(ScaleThroughput(throughput.current).unit, "Hz");
  EXPECT_TRUE(snapshot.error.empty());
}

TEST_F(DashboardEngineTest, SwitchesSlotsAndUnregistersOldSubscriber) {
  constexpr std::string_view kBase = "/dashboard-engine-slot-switch-test";
  DashboardEngine engine({.socket = Socket(), .channel = std::string(kBase)});
  ASSERT_TRUE(engine.Init().ok());

  TestApplication app;
  engine.StartTasks(app);
  app.Spawn(
      [&engine](co20::Coroutine &c) -> co20::Task {
        EXPECT_TRUE(engine.IncreaseRate()); // 10 Hz.
        EXPECT_TRUE(engine.IncreaseRate()); // 100 Hz.
        co_await c.Sleep(std::chrono::milliseconds(200));
        EXPECT_TRUE(engine.IncreaseSlots()); // 64 slots.
        co_await c.Sleep(std::chrono::milliseconds(600));
        engine.RequestStop();
        co_return;
      },
      "test-controller");
  app.Cpp20Scheduler().Run();

  const auto snapshot = engine.Snapshot();
  EXPECT_EQ(snapshot.requested_slots, 64);
  EXPECT_EQ(snapshot.active_slots, 64);
  EXPECT_EQ(snapshot.active_channel,
            ChannelNameForSlots(kBase, snapshot.active_slots));
  EXPECT_GT(snapshot.received_messages, 0);
  EXPECT_TRUE(snapshot.error.empty());

  subspace::Client observer;
  ASSERT_TRUE(observer.Init(Socket()).ok());
  auto old_info = observer.GetChannelInfo(ChannelNameForSlots(kBase, 32));
  if (old_info.ok()) {
    EXPECT_EQ(old_info->num_subscribers, 0);
  }
  auto active_info = observer.GetChannelInfo(ChannelNameForSlots(kBase, 64));
  ASSERT_TRUE(active_info.ok());
  EXPECT_EQ(active_info->num_subscribers, 1);
}

TEST_F(DashboardEngineTest, CoalescesRapidSlotChanges) {
  constexpr std::string_view kBase = "/dashboard-engine-rapid-slot-test";
  DashboardEngine engine({.socket = Socket(), .channel = std::string(kBase)});
  ASSERT_TRUE(engine.Init().ok());

  TestApplication app;
  engine.StartTasks(app);
  app.Spawn(
      [&engine](co20::Coroutine &c) -> co20::Task {
        EXPECT_TRUE(engine.IncreaseRate());
        EXPECT_TRUE(engine.IncreaseSlots()); // 64
        EXPECT_TRUE(engine.IncreaseSlots()); // 128
        EXPECT_TRUE(engine.IncreaseSlots()); // 256
        EXPECT_TRUE(engine.DecreaseSlots()); // 128
        co_await c.Sleep(std::chrono::milliseconds(500));
        engine.RequestStop();
        co_return;
      },
      "test-controller");
  app.Cpp20Scheduler().Run();

  const auto snapshot = engine.Snapshot();
  EXPECT_EQ(snapshot.requested_slots, 128);
  EXPECT_EQ(snapshot.active_slots, 128);
  EXPECT_EQ(snapshot.active_channel, ChannelNameForSlots(kBase, 128));
  EXPECT_GT(snapshot.received_messages, 0);
  EXPECT_TRUE(snapshot.error.empty());
}

TEST_F(DashboardEngineTest, StopsWhileChannelSwitchIsPending) {
  DashboardEngine engine(
      {.socket = Socket(), .channel = "/dashboard-engine-stop-switch-test"});
  ASSERT_TRUE(engine.Init().ok());

  TestApplication app;
  engine.StartTasks(app);
  app.Spawn(
      [&engine](co20::Coroutine &) -> co20::Task {
        EXPECT_TRUE(engine.IncreaseSlots());
        engine.RequestStop();
        co_return;
      },
      "test-controller");
  app.Cpp20Scheduler().Run();

  EXPECT_TRUE(engine.IsStopping());
  EXPECT_TRUE(engine.Snapshot().error.empty());
}

} // namespace
} // namespace subspace::dashboard
