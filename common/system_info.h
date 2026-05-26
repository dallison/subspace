// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <cstdint>

namespace subspace {

uint64_t PageSize();
uint64_t PageAlignedSize(uint64_t size);

} // namespace subspace
