// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/types/span.h"
#include <cstddef>
#include <cstdint>
#include <vector>

// Undefine this if you don't want to use hardware CRC32 instructions
#define HARDWARE_CRC 1


namespace subspace {
uint32_t CalculateChecksum(const std::vector<absl::Span<const uint8_t>> &data);
bool VerifyChecksum(const std::vector<absl::Span<const uint8_t>> &data,
                    uint32_t checksum);
} // namespace subspace
