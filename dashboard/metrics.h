#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace subspace::dashboard {

struct StatisticalSummary {
  bool valid = false;
  double current = 0.0;
  double min = 0.0;
  double mean = 0.0;
  double p50 = 0.0;
  double p99 = 0.0;
  double max = 0.0;
};

struct DashboardSnapshot {
  StatisticalSummary throughput_hz;
  StatisticalSummary latency_ns;
  uint64_t published_messages = 0;
  uint64_t received_messages = 0;
  uint64_t dropped_messages = 0;
  double drop_rate_percent = 0.0;
  int32_t requested_slots = 32;
  int32_t active_slots = 32;
  std::string active_channel;
  std::string target_rate;
  std::string error;
  std::deque<std::string> warnings;
};

struct ScaledValue {
  double value = 0.0;
  std::string unit;
};

class RateController {
public:
  struct Setting {
    double hz;
    const char *label;
  };

  RateController();

  bool Increase();
  bool Decrease();
  const Setting &Current() const;
  uint64_t Generation() const { return generation_; }
  size_t Index() const { return index_; }

  static constexpr size_t kInitialIndex = 1;
  static const std::array<Setting, 9> &Settings();

private:
  size_t index_ = kInitialIndex;
  uint64_t generation_ = 0;
};

class SlotController {
public:
  SlotController() = default;

  bool Increase();
  bool Decrease();
  int32_t Current() const;
  size_t Index() const { return index_; }
  uint64_t Generation() const { return generation_; }

  static constexpr size_t kInitialIndex = 2;
  static const std::array<int32_t, 8> &Settings();

private:
  size_t index_ = kInitialIndex;
  uint64_t generation_ = 0;
};

class RollingMetrics {
public:
  static constexpr size_t kHistoryBuckets = 10;
  static constexpr size_t kReservoirSize = 2048;
  static constexpr size_t kWarningHistorySize = 100;

  void RecordPublished(uint64_t count = 1);
  void RecordReceived(uint64_t latency_ns);
  void RecordDropped(uint64_t count);
  void SetTargetRate(std::string target_rate);
  void SetError(std::string error);
  void RecordWarning(std::string warning);
  void ResetStatistics();
  void ResetForChannel(std::string channel, int32_t slots);
  void Sample(uint64_t now_ns);

  const DashboardSnapshot &Snapshot() const { return snapshot_; }

private:
  struct LatencyInterval {
    uint64_t count = 0;
    long double sum = 0.0;
    uint64_t min = 0;
    uint64_t max = 0;
    std::vector<uint64_t> reservoir;
  };

  struct Bucket {
    double throughput_hz = 0.0;
    uint64_t received = 0;
    uint64_t dropped = 0;
    LatencyInterval latency;
  };

  void UpdateSnapshot();
  void AddReservoirSample(uint64_t latency_ns);
  uint64_t NextRandom();

  uint64_t last_sample_ns_ = 0;
  uint64_t received_since_sample_ = 0;
  uint64_t dropped_since_sample_ = 0;
  std::optional<uint64_t> latest_latency_ns_;
  LatencyInterval current_latency_;
  std::deque<Bucket> history_;
  DashboardSnapshot snapshot_;
  uint64_t random_state_ = 0x9e3779b97f4a7c15ULL;
};

ScaledValue ScaleThroughput(double hz);
ScaledValue ScaleThroughputToUnit(double hz, const std::string &unit);
ScaledValue ScaleLatency(double nanoseconds);
std::string FormatGaugeNumber(double value);
std::string FormatCompact(double value);
double DropRatePercent(uint64_t received, uint64_t dropped);
std::string ChannelNameForSlots(std::string_view base, int32_t slots);

} // namespace subspace::dashboard
