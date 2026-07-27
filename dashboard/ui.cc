#include "dashboard/ui.h"

#include "dashboard/metrics.h"
#include "retro/screen.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace subspace::dashboard {
namespace {

std::string SanitizeWarning(std::string_view text) {
  std::string sanitized;
  sanitized.reserve(text.size());
  for (size_t i = 0; i < text.size(); ++i) {
    const unsigned char character =
        static_cast<unsigned char>(text[i]);
    if (character == '\x1b' && i + 1 < text.size() &&
        text[i + 1] == '[') {
      i += 2;
      while (i < text.size()) {
        const unsigned char sequence_character =
            static_cast<unsigned char>(text[i]);
        if (sequence_character >= 0x40 && sequence_character <= 0x7e) {
          break;
        }
        ++i;
      }
      continue;
    }
    if (character == '\t') {
      sanitized.push_back(' ');
    } else if (character >= 0x20 && character != 0x7f) {
      sanitized.push_back(static_cast<char>(character));
    }
  }
  return sanitized;
}

constexpr Glyph kBlank = {
    "        ", "        ", "        ", "        ",
    "        ", "        ", "        ", "        ",
};
constexpr Glyph kZero = {
    " ****** ", "**    **", "**   ***", "**  * **",
    "***   **", "**    **", "**    **", " ****** ",
};
constexpr Glyph kOne = {
    "   **   ", "  ***   ", " ****   ", "   **   ",
    "   **   ", "   **   ", "   **   ", " ****** ",
};
constexpr Glyph kTwo = {
    " ****** ", "**    **", "      **", "     ** ",
    "   ***  ", "  **    ", "**      ", "********",
};
constexpr Glyph kThree = {
    " ****** ", "**    **", "      **", "   **** ",
    "      **", "      **", "**    **", " ****** ",
};
constexpr Glyph kFour = {
    "    *** ", "   **** ", "  ** ** ", " **  ** ",
    "**   ** ", "********", "     ** ", "     ** ",
};
constexpr Glyph kFive = {
    "********", "**      ", "**      ", "******* ",
    "      **", "      **", "**    **", " ****** ",
};
constexpr Glyph kSix = {
    "  ***** ", " **     ", "**      ", "******* ",
    "**    **", "**    **", "**    **", " ****** ",
};
constexpr Glyph kSeven = {
    "********", "      **", "     ** ", "    **  ",
    "   **   ", "  **    ", " **     ", " **     ",
};
constexpr Glyph kEight = {
    " ****** ", "**    **", "**    **", " ****** ",
    "**    **", "**    **", "**    **", " ****** ",
};
constexpr Glyph kNine = {
    " ****** ", "**    **", "**    **", " *******",
    "      **", "      **", "     ** ", " *****  ",
};
constexpr Glyph kDot = {
    "        ", "        ", "        ", "        ",
    "        ", "        ", "   **   ", "   **   ",
};

std::string CountString(uint64_t value) { return std::to_string(value); }

constexpr std::array<std::string_view, 3> kGaugeStatisticLabels = {
    "CURRENT", "MEAN", "P50"};

size_t GaugeStatisticIndex(GaugeStatistic statistic) {
  return static_cast<size_t>(statistic);
}

double GaugeValue(const StatisticalSummary &summary,
                  GaugeStatistic statistic) {
  switch (statistic) {
  case GaugeStatistic::kCurrent:
    return summary.current;
  case GaugeStatistic::kMean:
    return summary.mean;
  case GaugeStatistic::kP50:
    return summary.p50;
  }
  return 0.0;
}

std::vector<std::string> SummaryRow(const std::string &name,
                                    const StatisticalSummary &summary,
                                    double divisor, const std::string &unit) {
  if (!summary.valid) {
    return {name, "-", "-", "-", "-", "-", "-", unit};
  }
  return {name,
          FormatCompact(summary.current / divisor),
          FormatCompact(summary.min / divisor),
          FormatCompact(summary.mean / divisor),
          FormatCompact(summary.p50 / divisor),
          FormatCompact(summary.p99 / divisor),
          FormatCompact(summary.max / divisor),
          unit};
}

double SummaryMagnitude(const StatisticalSummary &summary) {
  if (!summary.valid) {
    return 0.0;
  }
  return std::max(
      {summary.current, summary.min, summary.mean, summary.p50, summary.p99,
       summary.max});
}

std::vector<std::string>
ThroughputSummaryRow(const StatisticalSummary &summary) {
  const ScaledValue scaled = ScaleThroughput(SummaryMagnitude(summary));
  double divisor = 1.0;
  if (scaled.unit == "kHz") {
    divisor = 1'000.0;
  } else if (scaled.unit == "MHz") {
    divisor = 1'000'000.0;
  }
  return SummaryRow("Throughput", summary, divisor, scaled.unit);
}

std::vector<std::string> LatencySummaryRow(const StatisticalSummary &summary) {
  const ScaledValue scaled = ScaleLatency(SummaryMagnitude(summary));
  double divisor = 1.0;
  if (scaled.unit == "µs") {
    divisor = 1'000.0;
  } else if (scaled.unit == "ms") {
    divisor = 1'000'000.0;
  }
  return SummaryRow("Latency", summary, divisor, scaled.unit);
}

} // namespace

const Glyph &GlyphFor(char character) {
  switch (character) {
  case '0':
    return kZero;
  case '1':
    return kOne;
  case '2':
    return kTwo;
  case '3':
    return kThree;
  case '4':
    return kFour;
  case '5':
    return kFive;
  case '6':
    return kSix;
  case '7':
    return kSeven;
  case '8':
    return kEight;
  case '9':
    return kNine;
  case '.':
    return kDot;
  default:
    return kBlank;
  }
}

GaugeWindow::GaugeWindow(retro::Screen *screen, retro::WindowOptions options,
                         Kind kind)
    : Window(screen, std::move(options)), kind_(kind) {}

void GaugeWindow::DrawValue(double value, std::string_view statistic,
                            const std::string &preferred_unit) {
  ScaledValue scaled =
      kind_ == Kind::kThroughput ? ScaleThroughput(value) : ScaleLatency(value);
  if (kind_ == Kind::kThroughput && !preferred_unit.empty()) {
    scaled = ScaleThroughputToUnit(value, preferred_unit);
  }
  const std::string text = FormatGaugeNumber(scaled.value);
  const int rendered_width =
      static_cast<int>(text.size() * 8 + (text.empty() ? 0 : text.size() - 1));
  const int start_col = std::max(1, (Width() - rendered_width) / 2);
  const int number_color = kind_ == Kind::kThroughput ? retro::kColorPairGreen
                                                      : retro::kColorPairYellow;
  const int unit_color = kind_ == Kind::kThroughput ? retro::kColorPairCyan
                                                    : retro::kColorPairMagenta;

  Window::Draw(false);
  PrintInMiddle(1, std::string(statistic), unit_color);
  for (int row = 0; row < 8; ++row) {
    int col = start_col;
    for (char character : text) {
      PrintAt(row + 2, col, std::string(GlyphFor(character)[row]),
              number_color);
      col += 9;
    }
  }
  PrintInMiddle(Height() - 2, scaled.unit, unit_color);
  Refresh();
}

StatsWindow::StatsWindow(retro::Screen *screen, retro::WindowOptions options)
    : Window(screen, std::move(options)),
      table_(this,
             {"Metric", "Current", "Min", "Mean", "P50", "P99", "Max",
              "Unit"},
             -1) {}

void StatsWindow::DrawSnapshot(const DashboardSnapshot &snapshot,
                               const DashboardEngine::Config &config) {
  Window::Draw(false);
  table_.Clear();
  table_.AddRow(
      {"Channel", snapshot.active_channel, "", "", "", "", "", ""});
  table_.AddRow({"Socket", config.socket, "", "", "", "", "", ""});
  table_.AddRow({"Slots", CountString(snapshot.active_slots), "", "", "", "",
                 "", "slots"});
  table_.AddRow({"Slot size", "8", "", "", "", "", "", "bytes"});
  table_.AddRow({"Target", snapshot.target_rate, "", "", "", "", "", ""});
  table_.AddRow({"Published", CountString(snapshot.published_messages), "", "",
                 "", "", "", "messages"});
  table_.AddRow({"Received", CountString(snapshot.received_messages), "", "",
                 "", "", "", "messages"});
  table_.AddRow({"Dropped", CountString(snapshot.dropped_messages), "", "", "",
                 "", "", "messages"});
  table_.AddRow({"Drop rate", FormatCompact(snapshot.drop_rate_percent), "", "",
                 "", "", "", "%"});
  table_.AddRow(ThroughputSummaryRow(snapshot.throughput_hz));
  table_.AddRow(LatencySummaryRow(snapshot.latency_ns));
  table_.Draw();
}

AcceleratorWindow::AcceleratorWindow(retro::Screen *screen,
                                     retro::WindowOptions options)
    : Window(screen, std::move(options)) {}

void AcceleratorWindow::DrawSetting(const RateController::Setting &setting,
                                    size_t rate_index, size_t rate_count,
                                    int32_t slots, size_t slot_index,
                                    size_t slot_count,
                                    GaugeStatistic gauge_statistic,
                                    AcceleratorControl selected,
                                    const std::string &error) {
  Window::Draw(false);
  PrintInMiddle(1, "PUBLISH RATE",
                selected == AcceleratorControl::kRate
                    ? retro::kColorPairCyan
                    : retro::kColorPairNormal);
  PrintInMiddle(2, setting.label, retro::kColorPairGreen);
  PrintInMiddle(3, "CHANNEL SLOTS",
                selected == AcceleratorControl::kSlots
                    ? retro::kColorPairCyan
                    : retro::kColorPairNormal);
  PrintInMiddle(4, CountString(slots), retro::kColorPairGreen);
  PrintInMiddle(5, "GAUGE STATISTIC",
                selected == AcceleratorControl::kGaugeStatistic
                    ? retro::kColorPairCyan
                    : retro::kColorPairNormal);
  PrintInMiddle(
      6, std::string(kGaugeStatisticLabels[GaugeStatisticIndex(
             gauge_statistic)]),
      retro::kColorPairGreen);

  const int bar_width = std::max(4, Width() - 6);
  size_t selected_index = rate_index;
  size_t selected_count = rate_count;
  if (selected == AcceleratorControl::kSlots) {
    selected_index = slot_index;
    selected_count = slot_count;
  } else if (selected == AcceleratorControl::kGaugeStatistic) {
    selected_index = GaugeStatisticIndex(gauge_statistic);
    selected_count = kGaugeStatisticLabels.size();
  }
  const int filled =
      selected_count <= 1
          ? 0
          : static_cast<int>((bar_width - 2) * selected_index /
                             (selected_count - 1));
  std::string bar = "[";
  bar.append(filled, '*');
  bar.append(bar_width - 2 - filled, ' ');
  bar += "]";
  PrintInMiddle(7, bar, retro::kColorPairYellow);
  PrintInMiddle(8, "LEFT / RIGHT  select");
  PrintInMiddle(9, "UP / DOWN     adjust");
  PrintInMiddle(10, "q             quit");
  if (!error.empty()) {
    std::string banner = "ERROR: " + error;
    const size_t max_width = static_cast<size_t>(std::max(1, Width() - 4));
    if (banner.size() > max_width) {
      banner.resize(max_width);
    }
    PrintInMiddle(Height() - 2, banner, retro::kColorPairRed);
  } else {
    PrintInMiddle(Height() - 2, "Subspace dashboard", retro::kColorPairMagenta);
  }
  Refresh();
}

WarningWindow::WarningWindow(retro::Screen *screen,
                             retro::WindowOptions options)
    : Window(screen, std::move(options)) {}

void WarningWindow::DrawWarnings(
    const std::deque<std::string> &warnings) {
  Window::Draw(false);
  const size_t visible_rows =
      static_cast<size_t>(std::max(0, Height() - 2));
  const size_t start =
      warnings.size() > visible_rows ? warnings.size() - visible_rows : 0;
  const size_t max_width =
      static_cast<size_t>(std::max(1, Width() - 4));
  int row = 1;
  for (size_t i = start; i < warnings.size(); ++i, ++row) {
    std::string warning = warnings[i];
    if (warning.size() > max_width) {
      warning.resize(max_width);
    }
    const int color = warning.rfind("ERROR:", 0) == 0
                          ? retro::kColorPairRed
                          : retro::kColorPairYellow;
    PrintAt(row, 2, warning, color);
  }
  Refresh();
}

DashboardApplication::DashboardApplication(DashboardEngine &engine)
    : Cpp20Application(32, 112), engine_(engine) {}

DashboardApplication::~DashboardApplication() { RestoreStandardError(); }

void DashboardApplication::Run() {
  retro::Cpp20Application::Run();
  RestoreStandardError();
}

absl::Status DashboardApplication::Init() {
  const int width = screen_.Width();
  const int height = screen_.Height();
  constexpr int top_height = 12;
  constexpr int warning_height = 6;
  const int body_height = height - top_height - warning_height;
  const int left_width = width / 2;
  const int stats_width = width * 3 / 4;

  throughput_window_ =
      std::make_unique<GaugeWindow>(&screen_,
                                    retro::WindowOptions{.title = "THROUGHPUT",
                                                         .nlines = top_height,
                                                         .ncols = left_width,
                                                         .y = 0,
                                                         .x = 0},
                                    GaugeWindow::Kind::kThroughput);
  latency_window_ = std::make_unique<GaugeWindow>(
      &screen_,
      retro::WindowOptions{.title = "LATENCY",
                           .nlines = top_height,
                           .ncols = width - left_width,
                           .y = 0,
                           .x = left_width},
      GaugeWindow::Kind::kLatency);
  stats_window_ = std::make_unique<StatsWindow>(
      &screen_, retro::WindowOptions{.title = "STATISTICS",
                                     .nlines = body_height,
                                     .ncols = stats_width,
                                     .y = top_height,
                                     .x = 0});
  accelerator_window_ = std::make_unique<AcceleratorWindow>(
      &screen_, retro::WindowOptions{.title = "ACCELERATOR",
                                     .nlines = body_height,
                                     .ncols = width - stats_width,
                                     .y = top_height,
                                     .x = stats_width});
  warning_window_ = std::make_unique<WarningWindow>(
      &screen_, retro::WindowOptions{.title = "WARNINGS",
                                     .nlines = warning_height,
                                     .ncols = width,
                                     .y = top_height + body_height,
                                     .x = 0});

  if (auto status = CaptureStandardError(); !status.ok()) {
    return status;
  }
  Spawn(
      [this](co20::Coroutine &c) -> co20::Task {
        return WarningCaptureTask(c);
      },
      "dashboard-warning-capture");
  engine_.StartTasks(*this);
  Spawn([this](co20::Coroutine &c) -> co20::Task { return InputTask(c); },
        "dashboard-input");
  Spawn([this](co20::Coroutine &c) -> co20::Task { return RefreshTask(c); },
        "dashboard-refresh");
  return absl::OkStatus();
}

absl::Status DashboardApplication::CaptureStandardError() {
  if (pipe(stderr_pipe_) != 0) {
    return absl::ErrnoToStatus(errno, "failed to create warning capture pipe");
  }
  for (int fd : stderr_pipe_) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      const int saved_errno = errno;
      close(stderr_pipe_[0]);
      close(stderr_pipe_[1]);
      stderr_pipe_[0] = -1;
      stderr_pipe_[1] = -1;
      return absl::ErrnoToStatus(saved_errno,
                                 "failed to configure warning capture pipe");
    }
  }

  saved_stderr_ = dup(STDERR_FILENO);
  if (saved_stderr_ == -1 ||
      dup2(stderr_pipe_[1], STDERR_FILENO) == -1) {
    const int saved_errno = errno;
    RestoreStandardError();
    return absl::ErrnoToStatus(saved_errno, "failed to capture standard error");
  }
  close(stderr_pipe_[1]);
  stderr_pipe_[1] = -1;
  return absl::OkStatus();
}

void DashboardApplication::RestoreStandardError() {
  if (saved_stderr_ != -1) {
    std::cerr.flush();
    std::fflush(stderr);
    (void)dup2(saved_stderr_, STDERR_FILENO);
    close(saved_stderr_);
    saved_stderr_ = -1;
    clearerr(stderr);
    std::cerr.clear();
  }
  DrainCapturedWarnings(/*flush_partial=*/true);
  if (stderr_pipe_[0] != -1) {
    close(stderr_pipe_[0]);
    stderr_pipe_[0] = -1;
  }
  if (stderr_pipe_[1] != -1) {
    close(stderr_pipe_[1]);
    stderr_pipe_[1] = -1;
  }
}

void DashboardApplication::DrainCapturedWarnings(bool flush_partial) {
  if (stderr_pipe_[0] == -1) {
    return;
  }

  char buffer[1024];
  ssize_t count;
  while ((count = read(stderr_pipe_[0], buffer, sizeof(buffer))) > 0) {
    pending_warning_.append(buffer, static_cast<size_t>(count));
  }

  for (;;) {
    const size_t newline = pending_warning_.find_first_of("\r\n");
    if (newline == std::string::npos) {
      break;
    }
    if (newline != 0) {
      std::string warning =
          SanitizeWarning(std::string_view(pending_warning_).substr(0, newline));
      if (!warning.empty()) {
        engine_.RecordWarning(std::move(warning));
      }
    }
    const size_t next = pending_warning_.find_first_not_of("\r\n", newline);
    pending_warning_.erase(
        0, next == std::string::npos ? pending_warning_.size() : next);
  }
  if (pending_warning_.size() > 4096 ||
      (flush_partial && !pending_warning_.empty())) {
    std::string warning = SanitizeWarning(pending_warning_);
    if (!warning.empty()) {
      engine_.RecordWarning(std::move(warning));
    }
    pending_warning_.clear();
  }
}

co20::Task DashboardApplication::WarningCaptureTask(co20::Coroutine &c) {
  while (!engine_.IsStopping()) {
    const int ready =
        co_await c.Wait(stderr_pipe_[0], POLLIN, 100'000'000ULL);
    if (ready == stderr_pipe_[0]) {
      DrainCapturedWarnings(/*flush_partial=*/false);
    }
  }
  DrainCapturedWarnings(/*flush_partial=*/true);
  co_return;
}

co20::Task DashboardApplication::InputTask(co20::Coroutine &c) {
  while (!engine_.IsStopping()) {
    const int ready = co_await c.Wait(STDIN_FILENO, POLLIN, 100'000'000ULL);
    if (ready == STDIN_FILENO) {
      const int character = getch();
      switch (character) {
      case KEY_LEFT: {
        const size_t index = static_cast<size_t>(selected_control_);
        selected_control_ =
            static_cast<AcceleratorControl>((index + 2) % 3);
        break;
      }
      case KEY_RIGHT: {
        const size_t index = static_cast<size_t>(selected_control_);
        selected_control_ =
            static_cast<AcceleratorControl>((index + 1) % 3);
        break;
      }
      case KEY_UP: {
        bool changed = false;
        if (selected_control_ == AcceleratorControl::kRate) {
          changed = engine_.IncreaseRate();
        } else if (selected_control_ == AcceleratorControl::kSlots) {
          changed = engine_.IncreaseSlots();
        } else {
          const size_t index = GaugeStatisticIndex(gauge_statistic_);
          if (index + 1 < kGaugeStatisticLabels.size()) {
            gauge_statistic_ = static_cast<GaugeStatistic>(index + 1);
            changed = true;
          }
        }
        if (!changed) {
          beep();
        }
        break;
      }
      case KEY_DOWN: {
        bool changed = false;
        if (selected_control_ == AcceleratorControl::kRate) {
          changed = engine_.DecreaseRate();
        } else if (selected_control_ == AcceleratorControl::kSlots) {
          changed = engine_.DecreaseSlots();
        } else {
          const size_t index = GaugeStatisticIndex(gauge_statistic_);
          if (index != 0) {
            gauge_statistic_ = static_cast<GaugeStatistic>(index - 1);
            changed = true;
          }
        }
        if (!changed) {
          beep();
        }
        break;
      }
      case 'q':
      case 'Q':
        engine_.RequestStop();
        break;
      default:
        break;
      }
    }
  }
  co_return;
}

co20::Task DashboardApplication::RefreshTask(co20::Coroutine &c) {
  while (!engine_.IsStopping()) {
    DrawAll();
    co_await c.Sleep(std::chrono::milliseconds(100));
  }
  co_return;
}

void DashboardApplication::DrawAll() {
  const auto snapshot = engine_.Snapshot();
  const auto &settings = RateController::Settings();
  const auto rate = engine_.Rate();
  const auto &slot_settings = SlotController::Settings();
  const auto slots = engine_.Slots();
  const auto &setting = rate.setting;
  const std::string throughput_unit = ScaleThroughput(setting.hz).unit;
  const std::string_view statistic =
      kGaugeStatisticLabels[GaugeStatisticIndex(gauge_statistic_)];
  throughput_window_->DrawValue(
      snapshot.throughput_hz.valid
          ? GaugeValue(snapshot.throughput_hz, gauge_statistic_)
          : 0.0,
      statistic, throughput_unit);
  latency_window_->DrawValue(
      snapshot.latency_ns.valid
          ? GaugeValue(snapshot.latency_ns, gauge_statistic_)
          : 0.0,
      statistic);
  stats_window_->DrawSnapshot(snapshot, engine_.GetConfig());
  accelerator_window_->DrawSetting(
      setting, rate.index, settings.size(), slots.count, slots.index,
      slot_settings.size(), gauge_statistic_, selected_control_,
      snapshot.error);
  warning_window_->DrawWarnings(snapshot.warnings);
}

} // namespace subspace::dashboard
