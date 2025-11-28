// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/types/span.h"
#include <array>
#include <cstddef>
#include <cstdint>

// Undefine this if you don't want to use hardware CRC32 instructions
#define HARDWARE_CRC 1

namespace subspace {

extern "C" {
uint32_t SubspaceCRC32(uint32_t crc, const uint8_t *data, size_t length);
}

template <size_t N>
uint32_t
CalculateChecksum(const std::array<absl::Span<const uint8_t>, N> &data) {
  uint32_t crc = 0xFFFFFFFF;
  for (size_t i = 0; i < N; i++) {
    crc = SubspaceCRC32(crc, data[i].data(), data[i].size());
  }
  return ~crc;
}

template <size_t N>
bool VerifyChecksum(const std::array<absl::Span<const uint8_t>, N> &data,
                    uint32_t checksum) {
  return CalculateChecksum(data) == checksum;
}

} // namespace subspace
