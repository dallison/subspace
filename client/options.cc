// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "client/options.h"

#include <atomic>

namespace subspace {
namespace {

std::atomic<bool> default_use_split_buffers{false};

} // namespace

void SetDefaultUseSplitBuffers(bool use_split_buffers) {
  default_use_split_buffers.store(use_split_buffers, std::memory_order_relaxed);
}

bool DefaultUseSplitBuffers() {
  return default_use_split_buffers.load(std::memory_order_relaxed);
}

} // namespace subspace
