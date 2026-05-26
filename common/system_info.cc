// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/system_info.h"

#include <unistd.h>

namespace subspace {

uint64_t PageSize() {
  static const uint64_t page_size = [] {
    long value = sysconf(_SC_PAGESIZE);
    if (value <= 0) {
      value = getpagesize();
    }
    if (value <= 0) {
      value = 4096;
    }
    return static_cast<uint64_t>(value);
  }();
  return page_size;
}

uint64_t PageAlignedSize(uint64_t size) {
  uint64_t page_size = PageSize();
  return (size + page_size - 1) & ~(page_size - 1);
}

} // namespace subspace
