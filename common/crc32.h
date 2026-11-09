// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <cstddef>
#include <cstdint>

// Undefine this if you don't want to use hardware CRC32 instructions.
#ifndef SUBSPACE_HARDWARE_CRC
#define SUBSPACE_HARDWARE_CRC 1
#endif

namespace subspace {

// CRC32 of a published message, using the IEEE 802.3 polynomial on every
// platform (the same as the Rust client). The ARM CRC32 instructions are IEEE;
// x86 uses a lookup table because its SSE4.2 CRC32 instruction is CRC-32C.
// Pass 0xFFFFFFFF as the first crc and invert the final result.
extern "C" {
uint32_t SubspaceCRC32(uint32_t crc, const uint8_t *data, size_t length);
}

} // namespace subspace
