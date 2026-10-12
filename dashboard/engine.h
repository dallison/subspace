#pragma once

#include "client/client.h"
#include "co/coroutine_cpp20.h"
#include "dashboard/metrics.h"
#include "retro/app.h"

#include "absl/status/status.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace subspace::dashboard {

class DashboardEngine {
public:
  struct Config {
    std::string socket = "/tmp/subspace";
    std::string channel = "/dashboard";
  };

  struct ModeSnapshot {
    ModeController::Setting setting;
    size_t index;
  };

  struct RateSnapshot {
    RateController::Setting setting;
    size_t index;
  };

  struct SlotSnapshot {
    int32_t count;
    size_t index;
  };

  struct QueueSnapshot {
    int32_t size;
    size_t index;
  };

  explicit DashboardEngine(Config config);
  ~DashboardEngine();

  DashboardEngine(const DashboardEngine &) = delete;
  DashboardEngine &operator=(const DashboardEngine &) = delete;

  absl::Status Init();
  void StartTasks(retro::Cpp20Application &app);
  void RequestStop();

  bool IncreaseMode();
  bool DecreaseMode();
  bool IncreaseRate();
  bool DecreaseRate();
  bool IncreaseSlots();
  bool DecreaseSlots();
  bool IncreaseSubscriberQueue();
  bool DecreaseSubscriberQueue();
  void RecordWarning(std::string warning);
  bool IsStopping() const {
    return stopping_.load(std::memory_order_acquire);
  }

  const Config &GetConfig() const { return config_; }
  ModeSnapshot Mode() const;
  RateSnapshot Rate() const;
  SlotSnapshot Slots() const;
  QueueSnapshot SubscriberQueue() const;
  DashboardSnapshot Snapshot() const;

private:
  struct ChannelProfile {
    std::string name;
    int32_t slots;
    int32_t queue_size;
    uint64_t generation;
  };

  void PublisherLoop();
  void SubscriberLoop();
  co20::Task SamplerTask(co20::Coroutine &c);

  void NotifyRateChanged();
  void NotifyModeChanged();
  void NotifyChannelChanged();
  ChannelProfile CurrentChannel() const;
  void ResetMeasurements();
  absl::StatusOr<subspace::Publisher>
  CreatePublisher(const ChannelProfile &profile);
  absl::StatusOr<subspace::Subscriber>
  CreateSubscriber(const ChannelProfile &profile);
  void ReportError(const absl::Status &status);
  void JoinWorkers();
  void RecordPublished(uint64_t count);
  void RecordDropped(uint64_t count);
  void RecordReceived(uint64_t latency_ns, uint64_t dropped);
  void RecordReceivedBatch(const std::vector<uint64_t> &latencies);
  void SampleMetrics();
  static absl::Status CreateNonBlockingPipe(int pipe_fds[2]);
  static void Wake(int write_fd);
  static void Drain(int read_fd);
  static void ClosePipe(int pipe_fds[2]);

  Config config_;
  mutable std::mutex rate_mutex_;
  mutable std::mutex metrics_mutex_;
  std::condition_variable rate_changed_;
  ModeController mode_;
  RateController rate_;
  SlotController slots_;
  SubscriberQueueController queues_;
  ChannelProfile published_channel_;
  ChannelProfile active_channel_;
  RollingMetrics metrics_;
  uint64_t dropped_since_warning_ = 0;
  std::shared_ptr<subspace::Client> publisher_client_;
  std::shared_ptr<subspace::Client> subscriber_client_;
  std::optional<subspace::Publisher> publisher_;
  std::optional<subspace::Subscriber> subscriber_;
  std::atomic<bool> stopping_ = false;
  std::atomic<PerformanceMode> performance_mode_ =
      PerformanceMode::kThroughput;
  // True while the subscriber thread is draining messages it has already been
  // woken for. The publisher skips per-message trigger writes in that window.
  std::atomic<bool> subscriber_busy_ = false;
  bool tasks_started_ = false;
  std::thread publisher_thread_;
  std::thread subscriber_thread_;

  int publisher_control_[2] = {-1, -1};
  int subscriber_stop_[2] = {-1, -1};
  int sampler_stop_[2] = {-1, -1};
};

} // namespace subspace::dashboard
