// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/types/span.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>

// Undefine this if you don't want to use hardware CRC32 instructions
#define SUBSPACE_HARDWARE_CRC 1

namespace subspace {

extern "C" {
uint32_t SubspaceCRC32(uint32_t crc, const uint8_t *data, size_t length);
}

// The callback receives the data to be checksummed and a writable region
// where the checksum should be stored.  The default CRC32 implementation
// writes 4 bytes; custom callbacks may use the full region.
using ChecksumCallback = std::function<void(
    const std::array<absl::Span<const uint8_t>, 2> &data,
    absl::Span<std::byte> checksum)>;

template <size_t N>
void CalculateCRC32Checksum(const std::array<absl::Span<const uint8_t>, N> &data,
                       absl::Span<std::byte> checksum) {
  uint32_t crc = 0xFFFFFFFF;
  for (size_t i = 0; i < N; i++) {
    crc = SubspaceCRC32(crc, data[i].data(), data[i].size());
  }
  *reinterpret_cast<uint32_t *>(checksum.data()) = ~crc;
}

template <size_t N>
bool VerifyCRC32Checksum(const std::array<absl::Span<const uint8_t>, N> &data,
                    absl::Span<const std::byte> checksum) {
  uint32_t crc = 0xFFFFFFFF;
  for (size_t i = 0; i < N; i++) {
    crc = SubspaceCRC32(crc, data[i].data(), data[i].size());
  }
  return *reinterpret_cast<const uint32_t *>(checksum.data()) == ~crc;
}

} // namespace subspace
