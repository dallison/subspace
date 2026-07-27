#include "dashboard/metrics.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <numeric>
#include <sstream>

namespace subspace::dashboard {
namespace {

template <typename T>
double Percentile(std::vector<T> values, double percentile) {
  if (values.empty()) {
    return 0.0;
  }
  const size_t index = std::min(
      values.size() - 1,
      static_cast<size_t>(std::ceil(values.size() * percentile)) - 1);
  std::nth_element(values.begin(), values.begin() + index, values.end());
  return static_cast<double>(values[index]);
}

} // namespace

const std::array<RateController::Setting, 9> &RateController::Settings() {
  static constexpr std::array<Setting, 9> settings = {{
      {0.0, "STOPPED"},
      {1.0, "1 Hz"},
      {10.0, "10 Hz"},
      {100.0, "100 Hz"},
      {1'000.0, "1 kHz"},
      {10'000.0, "10 kHz"},
      {100'000.0, "100 kHz"},
      {1'000'000.0, "1 MHz"},
      {10'000'000.0, "10 MHz"},
  }};
  return settings;
}

RateController::RateController() = default;

bool RateController::Increase() {
  if (index_ + 1 >= Settings().size()) {
    return false;
  }
  ++index_;
  ++generation_;
  return true;
}

bool RateController::Decrease() {
  if (index_ == 0) {
    return false;
  }
  --index_;
  ++generation_;
  return true;
}

const RateController::Setting &RateController::Current() const {
  return Settings()[index_];
}

const std::array<int32_t, 8> &SlotController::Settings() {
  static constexpr std::array<int32_t, 8> settings = {
      8, 16, 32, 64, 128, 256, 512, 1024};
  return settings;
}

bool SlotController::Increase() {
  if (index_ + 1 >= Settings().size()) {
    return false;
  }
  ++index_;
  ++generation_;
  return true;
}

bool SlotController::Decrease() {
  if (index_ == 0) {
    return false;
  }
  --index_;
  ++generation_;
  return true;
}

int32_t SlotController::Current() const { return Settings()[index_]; }

void RollingMetrics::RecordPublished(uint64_t count) {
  snapshot_.published_messages += count;
}

void RollingMetrics::RecordReceived(uint64_t latency_ns) {
  ++snapshot_.received_messages;
  ++received_since_sample_;
  latest_latency_ns_ = latency_ns;

  auto &interval = current_latency_;
  if (interval.count == 0) {
    interval.min = latency_ns;
    interval.max = latency_ns;
  } else {
    interval.min = std::min(interval.min, latency_ns);
    interval.max = std::max(interval.max, latency_ns);
  }
  ++interval.count;
  interval.sum += latency_ns;
  AddReservoirSample(latency_ns);
}

void RollingMetrics::RecordDropped(uint64_t count) {
  snapshot_.dropped_messages += count;
  dropped_since_sample_ += count;
}

void RollingMetrics::SetTargetRate(std::string target_rate) {
  snapshot_.target_rate = std::move(target_rate);
}

void RollingMetrics::SetError(std::string error) {
  snapshot_.error = std::move(error);
}

void RollingMetrics::RecordWarning(std::string warning) {
  snapshot_.warnings.push_back(std::move(warning));
  if (snapshot_.warnings.size() > kWarningHistorySize) {
    snapshot_.warnings.pop_front();
  }
}

void RollingMetrics::ResetStatistics() {
  const int32_t requested_slots = snapshot_.requested_slots;
  const int32_t active_slots = snapshot_.active_slots;
  std::string active_channel = std::move(snapshot_.active_channel);
  std::string target_rate = std::move(snapshot_.target_rate);
  std::string error = std::move(snapshot_.error);
  std::deque<std::string> warnings = std::move(snapshot_.warnings);

  snapshot_ = {};
  snapshot_.requested_slots = requested_slots;
  snapshot_.active_slots = active_slots;
  snapshot_.active_channel = std::move(active_channel);
  snapshot_.target_rate = std::move(target_rate);
  snapshot_.error = std::move(error);
  snapshot_.warnings = std::move(warnings);
  last_sample_ns_ = 0;
  received_since_sample_ = 0;
  dropped_since_sample_ = 0;
  latest_latency_ns_.reset();
  current_latency_ = {};
  history_.clear();
}

void RollingMetrics::ResetForChannel(std::string channel, int32_t slots) {
  ResetStatistics();
  snapshot_.requested_slots = slots;
  snapshot_.active_slots = slots;
  snapshot_.active_channel = std::move(channel);
}

void RollingMetrics::Sample(uint64_t now_ns) {
  if (last_sample_ns_ == 0) {
    last_sample_ns_ = now_ns;
    UpdateSnapshot();
    return;
  }

  const uint64_t elapsed_ns = now_ns - last_sample_ns_;
  if (elapsed_ns == 0) {
    return;
  }

  Bucket bucket;
  bucket.throughput_hz = static_cast<double>(received_since_sample_) *
                         1'000'000'000.0 / static_cast<double>(elapsed_ns);
  bucket.received = received_since_sample_;
  bucket.dropped = dropped_since_sample_;
  bucket.latency = std::move(current_latency_);
  history_.push_back(std::move(bucket));
  if (history_.size() > kHistoryBuckets) {
    history_.pop_front();
  }

  current_latency_ = {};
  received_since_sample_ = 0;
  dropped_since_sample_ = 0;
  last_sample_ns_ = now_ns;
  UpdateSnapshot();
}

void RollingMetrics::UpdateSnapshot() {
  if (!history_.empty()) {
    std::vector<double> rates;
    rates.reserve(history_.size());
    uint64_t received = 0;
    uint64_t dropped = 0;
    for (const auto &bucket : history_) {
      rates.push_back(bucket.throughput_hz);
      received += bucket.received;
      dropped += bucket.dropped;
    }
    auto &summary = snapshot_.throughput_hz;
    summary.valid = true;
    const auto [min_it, max_it] =
        std::minmax_element(rates.begin(), rates.end());
    summary.min = *min_it;
    summary.max = *max_it;
    summary.mean =
        std::accumulate(rates.begin(), rates.end(), 0.0) / rates.size();
    // "Current" is the rolling one-second rate rather than the latest 100 ms
    // bucket. At low rates, the latest bucket is usually zero and makes
    // continuous publishing look input-driven.
    summary.current = summary.mean;
    summary.p50 = Percentile(rates, 0.50);
    summary.p99 = Percentile(rates, 0.99);
    snapshot_.drop_rate_percent = DropRatePercent(received, dropped);
  } else {
    snapshot_.drop_rate_percent = 0.0;
  }

  uint64_t total_count = 0;
  long double total_sum = 0.0;
  uint64_t min_latency = std::numeric_limits<uint64_t>::max();
  uint64_t max_latency = 0;
  std::vector<uint64_t> samples;
  samples.reserve(history_.size() * kReservoirSize);
  for (const auto &bucket : history_) {
    if (bucket.latency.count == 0) {
      continue;
    }
    total_count += bucket.latency.count;
    total_sum += bucket.latency.sum;
    min_latency = std::min(min_latency, bucket.latency.min);
    max_latency = std::max(max_latency, bucket.latency.max);
    samples.insert(samples.end(), bucket.latency.reservoir.begin(),
                   bucket.latency.reservoir.end());
  }

  auto &latency = snapshot_.latency_ns;
  latency.valid = total_count != 0;
  if (latency.valid) {
    latency.current =
        static_cast<double>(latest_latency_ns_.value_or(max_latency));
    latency.min = static_cast<double>(min_latency);
    latency.max = static_cast<double>(max_latency);
    latency.mean = static_cast<double>(total_sum / total_count);
    latency.p50 = Percentile(samples, 0.50);
    latency.p99 = Percentile(samples, 0.99);
  } else {
    latency = {};
  }
}

void RollingMetrics::AddReservoirSample(uint64_t latency_ns) {
  auto &reservoir = current_latency_.reservoir;
  if (reservoir.size() < kReservoirSize) {
    reservoir.push_back(latency_ns);
    return;
  }
  const uint64_t index = NextRandom() % current_latency_.count;
  if (index < reservoir.size()) {
    reservoir[index] = latency_ns;
  }
}

uint64_t RollingMetrics::NextRandom() {
  uint64_t x = random_state_;
  x ^= x >> 12;
  x ^= x << 25;
  x ^= x >> 27;
  random_state_ = x;
  return x * 0x2545f4914f6cdd1dULL;
}

ScaledValue ScaleThroughput(double hz) {
  if (hz >= 1'000'000.0) {
    return {.value = hz / 1'000'000.0, .unit = "MHz"};
  }
  if (hz >= 1'000.0) {
    return {.value = hz / 1'000.0, .unit = "kHz"};
  }
  return {.value = hz, .unit = "Hz"};
}

ScaledValue ScaleThroughputToUnit(double hz, const std::string &unit) {
  if (unit == "MHz") {
    return {.value = hz / 1'000'000.0, .unit = unit};
  }
  if (unit == "kHz") {
    return {.value = hz / 1'000.0, .unit = unit};
  }
  return {.value = hz, .unit = "Hz"};
}

ScaledValue ScaleLatency(double nanoseconds) {
  if (nanoseconds >= 1'000'000.0) {
    return {.value = nanoseconds / 1'000'000.0, .unit = "ms"};
  }
  if (nanoseconds >= 1'000.0) {
    return {.value = nanoseconds / 1'000.0, .unit = "µs"};
  }
  return {.value = nanoseconds, .unit = "ns"};
}

std::string FormatGaugeNumber(double value) {
  std::ostringstream stream;
  stream << std::fixed;
  if (value >= 100.0) {
    stream << std::setprecision(0);
  } else if (value >= 10.0) {
    stream << std::setprecision(1);
  } else {
    stream << std::setprecision(2);
  }
  stream << std::min(value, 999.0);
  return stream.str();
}

std::string FormatCompact(double value) {
  std::ostringstream stream;
  if (std::abs(value) >= 1000.0 ||
      (std::abs(value) > 0.0 && std::abs(value) < 0.01)) {
    stream << std::scientific << std::setprecision(2) << value;
  } else {
    stream << std::fixed << std::setprecision(2) << value;
  }
  return stream.str();
}

double DropRatePercent(uint64_t received, uint64_t dropped) {
  const long double total =
      static_cast<long double>(received) + static_cast<long double>(dropped);
  if (total == 0.0) {
    return 0.0;
  }
  return static_cast<double>(100.0L * dropped / total);
}

std::string ChannelNameForSlots(std::string_view base, int32_t slots) {
  return std::string(base) + "-" + std::to_string(slots) + "-slots";
}

} // namespace subspace::dashboard
