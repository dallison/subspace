// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <cstdint>
#include <cstddef>
#include "absl/types/span.h"
#include <vector>
namespace subspace {
    uint32_t CalculateChecksum(const std::vector<absl::Span<const uint8_t>>& data);
    bool VerifyChecksum(const std::vector<absl::Span<const uint8_t>>& data, uint32_t checksum);
}

