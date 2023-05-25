// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/logging.h"
#include "absl/strings/str_format.h"
#include "common/clock.h"
#include <cstdio>
#include <inttypes.h>

namespace subspace {

static const char *LogLevelAsString(LogLevel level) {
  switch (level) {
  case LogLevel::kVerboseDebug:
    return "V";
  case LogLevel::kDebug:
    return "D";
  case LogLevel::kInfo:
    return "I";
  case LogLevel::kWarning:
    return "W";
  case LogLevel::kError:
    return "E";
  case LogLevel::kFatal:
    return "F";
  }
  return "U";
}

Logger::ForegroundColor Logger::ColorForLogLevel(LogLevel level) {
  switch (level) {
  case LogLevel::kVerboseDebug:
    return kBlue;
  case LogLevel::kDebug:
    return kGreen;
  case LogLevel::kInfo:
    return kNormal;
  case LogLevel::kWarning:
    return kMagenta;
  case LogLevel::kError:
    return kRed;
  case LogLevel::kFatal:
    return kRed;
  }
  return kCyan;
}

std::string Logger::ColorString(ForegroundColor color) {
  if (!in_color_) {
    return "";
  }
  return absl::StrFormat("\033[%d;1m", color);
}

std::string Logger::NormalString() {
  if (!in_color_) {
    return "";
  }
  return absl::StrFormat("\033[39;0m");
}

void Logger::Log(LogLevel level, const char *fmt, ...) {
  if (level < min_level_) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  VLog(level, fmt, ap);
  va_end(ap);
}

void Logger::VLog(LogLevel level, const char *fmt, va_list ap) {
  if (level < min_level_) {
    return;
  }
  size_t n = vsnprintf(buffer_, sizeof(buffer_), fmt, ap);

  // Strip final \n if present.  Refactoring from printf can leave
  // this in place.
  if (buffer_[n-1] == '\n') {
    buffer_[n-1] = '\0';
  }

  struct timespec now_ts;
  clock_gettime(CLOCK_REALTIME, &now_ts);
  uint64_t now_ns = now_ts.tv_sec * 1000000000LL + now_ts.tv_nsec;

  char timebuf[64];
  struct tm tm;
  n = strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S",
                      localtime_r(&now_ts.tv_sec, &tm));
  snprintf(timebuf + n, sizeof(timebuf) - n, ".%09" PRIu64, now_ns % 1000000000);

  ForegroundColor color = ColorForLogLevel(level);
  fprintf(output_stream_, "%s%s: %s: %s%s\n", ColorString(color).c_str(), timebuf,
          LogLevelAsString(level), buffer_, NormalString().c_str());
  if (level == LogLevel::kFatal) {
    abort();
  }
}
} // namespace subspace
