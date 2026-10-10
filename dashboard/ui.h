#pragma once

#include "co/coroutine_cpp20.h"
#include "dashboard/engine.h"
#include "retro/app.h"
#include "retro/table.h"
#include "retro/window.h"

#include "absl/status/status.h"

#include <array>
#include <memory>
#include <string>
#include <string_view>

namespace subspace::dashboard {

using Glyph = std::array<std::string_view, 8>;

enum class AcceleratorControl { kRate, kSlots, kGaugeStatistic };
enum class GaugeStatistic { kCurrent, kMean, kP50 };

const Glyph &GlyphFor(char character);

class GaugeWindow : public retro::Window {
public:
  enum class Kind { kThroughput, kLatency };

  GaugeWindow(retro::Screen *screen, retro::WindowOptions options, Kind kind);
  void DrawValue(double value, std::string_view statistic,
                 const std::string &preferred_unit = "");

private:
  Kind kind_;
};

class StatsWindow : public retro::Window {
public:
  StatsWindow(retro::Screen *screen, retro::WindowOptions options);
  void DrawSnapshot(const DashboardSnapshot &snapshot,
                    const DashboardEngine::Config &config);

private:
  retro::Table table_;
};

class AcceleratorWindow : public retro::Window {
public:
  AcceleratorWindow(retro::Screen *screen, retro::WindowOptions options);
  void DrawSetting(const RateController::Setting &setting, size_t rate_index,
                   size_t rate_count, int32_t slots, size_t slot_index,
                   size_t slot_count, GaugeStatistic gauge_statistic,
                   AcceleratorControl selected,
                   const std::string &error);
};

class WarningWindow : public retro::Window {
public:
  WarningWindow(retro::Screen *screen, retro::WindowOptions options);
  void DrawWarnings(const std::deque<std::string> &warnings);
};

class DashboardApplication : public retro::Cpp20Application {
public:
  explicit DashboardApplication(DashboardEngine &engine);
  ~DashboardApplication() override;

  absl::Status Init() override;
  void Run() override;

private:
  co20::Task InputTask(co20::Coroutine &c);
  co20::Task RefreshTask(co20::Coroutine &c);
  co20::Task WarningCaptureTask(co20::Coroutine &c);
  absl::Status CaptureStandardError();
  void RestoreStandardError();
  void DrainCapturedWarnings(bool flush_partial);
  void DrawAll();

  DashboardEngine &engine_;
  std::unique_ptr<GaugeWindow> throughput_window_;
  std::unique_ptr<GaugeWindow> latency_window_;
  std::unique_ptr<StatsWindow> stats_window_;
  std::unique_ptr<AcceleratorWindow> accelerator_window_;
  std::unique_ptr<WarningWindow> warning_window_;
  AcceleratorControl selected_control_ = AcceleratorControl::kRate;
  GaugeStatistic gauge_statistic_ = GaugeStatistic::kCurrent;
  int saved_stderr_ = -1;
  int stderr_pipe_[2] = {-1, -1};
  std::string pending_warning_;
};

} // namespace subspace::dashboard
