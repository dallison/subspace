#include "dashboard/metrics.h"
#include "dashboard/ui.h"

#include "gtest/gtest.h"

#include <limits>
#include <string>

namespace subspace::dashboard {
namespace {

TEST(RateControllerTest, ClampsAtBothEnds) {
  RateController rate;
  EXPECT_EQ(rate.Current().hz, 1.0);

  while (rate.Decrease()) {
  }
  EXPECT_EQ(rate.Current().hz, 0.0);
  EXPECT_FALSE(rate.Decrease());

  while (rate.Increase()) {
  }
  EXPECT_EQ(rate.Current().hz, 10'000'000.0);
  EXPECT_EQ(std::string(rate.Current().label), "10 MHz");
  EXPECT_FALSE(rate.Increase());
}

TEST(SlotControllerTest, UsesPresetsAndClampsAtBothEnds) {
  SlotController slots;
  EXPECT_EQ(slots.Current(), 32);
  EXPECT_EQ(slots.Generation(), 0);

  while (slots.Decrease()) {
  }
  EXPECT_EQ(slots.Current(), 8);
  EXPECT_FALSE(slots.Decrease());

  while (slots.Increase()) {
  }
  EXPECT_EQ(slots.Current(), 1024);
  EXPECT_FALSE(slots.Increase());
  EXPECT_EQ(SlotController::Settings().size(), 8);
}

TEST(ChannelNameTest, AppendsSlotCountToBaseName) {
  EXPECT_EQ(ChannelNameForSlots("/dashboard", 32), "/dashboard-32-slots");
  EXPECT_EQ(ChannelNameForSlots("/custom", 1024), "/custom-1024-slots");
}

TEST(DisplayScalingTest, UsesExpectedBoundaries) {
  EXPECT_EQ(ScaleThroughput(999.0).unit, "Hz");
  EXPECT_EQ(ScaleThroughput(1'000.0).unit, "kHz");
  EXPECT_EQ(ScaleThroughput(1'000'000.0).unit, "MHz");
  EXPECT_EQ(ScaleThroughputToUnit(50'000.0, "MHz").unit, "MHz");
  EXPECT_DOUBLE_EQ(ScaleThroughputToUnit(50'000.0, "MHz").value, 0.05);
  EXPECT_EQ(ScaleLatency(999.0).unit, "ns");
  EXPECT_EQ(ScaleLatency(1'000.0).unit, "µs");
  EXPECT_EQ(ScaleLatency(1'000'000.0).unit, "ms");
}

TEST(DropRateTest, ReportsDroppedShareOfAccountedMessages) {
  EXPECT_DOUBLE_EQ(DropRatePercent(0, 0), 0.0);
  EXPECT_DOUBLE_EQ(DropRatePercent(50, 50), 50.0);
  EXPECT_DOUBLE_EQ(DropRatePercent(75, 25), 25.0);
  EXPECT_DOUBLE_EQ(DropRatePercent(std::numeric_limits<uint64_t>::max(),
                                  std::numeric_limits<uint64_t>::max()),
                   50.0);
}

TEST(BigFontTest, EverySupportedGlyphIsEightByEight) {
  for (char character : std::string("0123456789.")) {
    const auto &glyph = GlyphFor(character);
    EXPECT_EQ(glyph.size(), 8);
    for (std::string_view row : glyph) {
      EXPECT_EQ(row.size(), 8) << "glyph " << character;
    }
  }
}

TEST(RollingMetricsTest, ComputesCountsAndWindowStatistics) {
  RollingMetrics metrics;
  metrics.SetTargetRate("100 Hz");
  metrics.Sample(1'000'000'000ULL);
  for (uint64_t latency : {100ULL, 200ULL, 300ULL, 400ULL, 500ULL}) {
    metrics.RecordPublished();
    metrics.RecordReceived(latency);
  }
  metrics.RecordDropped(2);
  metrics.Sample(1'100'000'000ULL);

  const auto &snapshot = metrics.Snapshot();
  EXPECT_EQ(snapshot.published_messages, 5);
  EXPECT_EQ(snapshot.received_messages, 5);
  EXPECT_EQ(snapshot.dropped_messages, 2);
  EXPECT_NEAR(snapshot.drop_rate_percent, 100.0 * 2.0 / 7.0, 0.001);
  EXPECT_EQ(snapshot.target_rate, "100 Hz");
  ASSERT_TRUE(snapshot.throughput_hz.valid);
  EXPECT_DOUBLE_EQ(snapshot.throughput_hz.current, 50.0);
  EXPECT_DOUBLE_EQ(snapshot.throughput_hz.p50, 50.0);
  ASSERT_TRUE(snapshot.latency_ns.valid);
  EXPECT_DOUBLE_EQ(snapshot.latency_ns.current, 500.0);
  EXPECT_DOUBLE_EQ(snapshot.latency_ns.min, 100.0);
  EXPECT_DOUBLE_EQ(snapshot.latency_ns.mean, 300.0);
  EXPECT_DOUBLE_EQ(snapshot.latency_ns.p50, 300.0);
  EXPECT_DOUBLE_EQ(snapshot.latency_ns.p99, 500.0);
  EXPECT_DOUBLE_EQ(snapshot.latency_ns.max, 500.0);
}

TEST(RollingMetricsTest, AgesLatencyOutAndReportsIdleThroughput) {
  RollingMetrics metrics;
  metrics.Sample(1'000'000'000ULL);
  metrics.RecordReceived(250);
  metrics.Sample(1'100'000'000ULL);
  for (size_t i = 0; i < RollingMetrics::kHistoryBuckets; ++i) {
    metrics.Sample(1'200'000'000ULL + i * 100'000'000ULL);
  }

  const auto &snapshot = metrics.Snapshot();
  EXPECT_TRUE(snapshot.throughput_hz.valid);
  EXPECT_DOUBLE_EQ(snapshot.throughput_hz.current, 0.0);
  EXPECT_FALSE(snapshot.latency_ns.valid);
}

TEST(RollingMetricsTest, DropRateUsesRollingOneSecondWindow) {
  RollingMetrics metrics;
  metrics.Sample(1'000'000'000ULL);
  for (size_t i = 0; i < 50; ++i) {
    metrics.RecordReceived(100);
  }
  metrics.RecordDropped(50);
  metrics.Sample(1'100'000'000ULL);
  EXPECT_DOUBLE_EQ(metrics.Snapshot().drop_rate_percent, 50.0);

  for (size_t i = 0; i < 50; ++i) {
    metrics.RecordReceived(100);
  }
  metrics.Sample(1'200'000'000ULL);
  EXPECT_NEAR(metrics.Snapshot().drop_rate_percent, 100.0 / 3.0, 0.001);

  for (size_t i = 0; i < RollingMetrics::kHistoryBuckets - 1; ++i) {
    metrics.Sample(1'300'000'000ULL + i * 100'000'000ULL);
  }
  EXPECT_DOUBLE_EQ(metrics.Snapshot().drop_rate_percent, 0.0);
}

TEST(RollingMetricsTest, ShowsRollingRateBetweenLowFrequencyMessages) {
  RollingMetrics metrics;
  metrics.Sample(1'000'000'000ULL);
  metrics.RecordReceived(250);
  metrics.Sample(1'100'000'000ULL);
  for (size_t i = 0; i < RollingMetrics::kHistoryBuckets - 1; ++i) {
    metrics.Sample(1'200'000'000ULL + i * 100'000'000ULL);
  }

  const auto &throughput = metrics.Snapshot().throughput_hz;
  ASSERT_TRUE(throughput.valid);
  EXPECT_NEAR(throughput.current, 1.0, 0.01);
}

TEST(RollingMetricsTest, KeepsReservoirBounded) {
  RollingMetrics metrics;
  metrics.Sample(1'000'000'000ULL);
  for (uint64_t i = 1; i <= RollingMetrics::kReservoirSize * 4; ++i) {
    metrics.RecordReceived(i);
  }
  metrics.Sample(1'100'000'000ULL);

  const auto &latency = metrics.Snapshot().latency_ns;
  EXPECT_TRUE(latency.valid);
  EXPECT_EQ(latency.min, 1.0);
  EXPECT_EQ(latency.max,
            static_cast<double>(RollingMetrics::kReservoirSize * 4));
  EXPECT_GT(latency.p99, latency.mean);
  EXPECT_LE(latency.p99, latency.max);
}

TEST(RollingMetricsTest, KeepsMostRecentWarnings) {
  RollingMetrics metrics;
  for (size_t i = 0; i < RollingMetrics::kWarningHistorySize + 2; ++i) {
    metrics.RecordWarning("warning-" + std::to_string(i));
  }

  const auto &warnings = metrics.Snapshot().warnings;
  ASSERT_EQ(RollingMetrics::kWarningHistorySize, warnings.size());
  EXPECT_EQ("warning-2", warnings.front());
  EXPECT_EQ("warning-" +
                std::to_string(RollingMetrics::kWarningHistorySize + 1),
            warnings.back());
}

TEST(RollingMetricsTest, StatisticsResetKeepsConfigurationAndWarnings) {
  RollingMetrics metrics;
  metrics.ResetForChannel("/dashboard-32-slots", 32);
  metrics.SetTargetRate("1 MHz");
  metrics.RecordWarning("retained warning");
  metrics.Sample(1'000'000'000ULL);
  metrics.RecordPublished(10);
  metrics.RecordReceived(100);
  metrics.RecordDropped(1);
  metrics.Sample(1'100'000'000ULL);

  metrics.ResetStatistics();

  const auto &snapshot = metrics.Snapshot();
  EXPECT_EQ(snapshot.published_messages, 0);
  EXPECT_EQ(snapshot.received_messages, 0);
  EXPECT_EQ(snapshot.dropped_messages, 0);
  EXPECT_DOUBLE_EQ(snapshot.drop_rate_percent, 0.0);
  EXPECT_FALSE(snapshot.throughput_hz.valid);
  EXPECT_FALSE(snapshot.latency_ns.valid);
  EXPECT_EQ(snapshot.active_channel, "/dashboard-32-slots");
  EXPECT_EQ(snapshot.active_slots, 32);
  EXPECT_EQ(snapshot.target_rate, "1 MHz");
  ASSERT_EQ(snapshot.warnings.size(), 1);
  EXPECT_EQ(snapshot.warnings.front(), "retained warning");
}

TEST(RollingMetricsTest, ChannelResetClearsMeasurementsButKeepsControls) {
  RollingMetrics metrics;
  metrics.SetTargetRate("100 kHz");
  metrics.RecordPublished(7);
  metrics.RecordReceived(123);
  metrics.RecordDropped(2);
  metrics.RecordWarning("retained warning");
  metrics.Sample(1'000'000'000ULL);

  metrics.ResetForChannel("/dashboard-64-slots", 64);

  const auto &snapshot = metrics.Snapshot();
  EXPECT_EQ(snapshot.published_messages, 0);
  EXPECT_EQ(snapshot.received_messages, 0);
  EXPECT_EQ(snapshot.dropped_messages, 0);
  EXPECT_DOUBLE_EQ(snapshot.drop_rate_percent, 0.0);
  EXPECT_FALSE(snapshot.throughput_hz.valid);
  EXPECT_FALSE(snapshot.latency_ns.valid);
  EXPECT_EQ(snapshot.target_rate, "100 kHz");
  EXPECT_EQ(snapshot.requested_slots, 64);
  EXPECT_EQ(snapshot.active_slots, 64);
  EXPECT_EQ(snapshot.active_channel, "/dashboard-64-slots");
  ASSERT_EQ(snapshot.warnings.size(), 1);
  EXPECT_EQ(snapshot.warnings.front(), "retained warning");
}

} // namespace
} // namespace subspace::dashboard
