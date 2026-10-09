// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <cstddef>
#include <cstdint>

namespace subspace {
namespace asil {

// The CRC32 that the standard client puts in message prefixes.  x86-64 builds
// with SSE4.2 use the CRC32C polynomial; all other builds use the IEEE 802.3
// polynomial.  Pass 0xFFFFFFFF as the first crc and invert the final result.
uint32_t Crc32(uint32_t crc, const uint8_t *data, size_t length);

} // namespace asil
} // namespace subspace
