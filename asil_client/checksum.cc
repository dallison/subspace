// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asil_client/checksum.h"

#include <array>

namespace subspace {
namespace asil {

namespace {

#if defined(__x86_64__) && defined(__SSE4_2__)
constexpr uint32_t kPolynomial = 0x82F63B78; // CRC32C.
#else
constexpr uint32_t kPolynomial = 0xEDB88320; // IEEE 802.3.
#endif

constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> table = {};
  for (uint32_t i = 0; i < 256; i++) {
    uint32_t crc = i;
    for (int bit = 0; bit < 8; bit++) {
      crc = (crc & 1) != 0 ? (crc >> 1) ^ kPolynomial : crc >> 1;
    }
    table[i] = crc;
  }
  return table;
}

constexpr std::array<uint32_t, 256> kTable = MakeTable();

} // namespace

uint32_t Crc32(uint32_t crc, const uint8_t *data, size_t length) {
  for (size_t i = 0; i < length; i++) {
    crc = (crc >> 8) ^ kTable[(crc ^ data[i]) & 0xFF];
  }
  return crc;
}

} // namespace asil
} // namespace subspace
