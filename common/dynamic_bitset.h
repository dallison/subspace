// Copyright 2023-2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.
//
// A heap-backed bitset.  This is not part of the shared-memory layout.

#pragma once

#include "common/atomic_bitset.h"

#include <vector>

namespace subspace {

class DynamicBitSet {
public:
  DynamicBitSet() : num_bits_(0), bits_(0) {}
  DynamicBitSet(size_t num_bits)
      : num_bits_(num_bits), bits_(BitsToWords(num_bits), 0) {}

  void Resize(size_t num_bits) {
    num_bits_ = num_bits;
    bits_.resize(BitsToWords(num_bits), 0);
  }

  void Set(size_t bit) {
    size_t word = bit / 64;
    size_t offset = bit % 64;
    bits_[word] |= 1ULL << offset;
  }

  void Clear(size_t bit) {
    size_t word = bit / 64;
    size_t offset = bit % 64;
    bits_[word] &= ~(1ULL << offset);
  }

  bool IsSet(size_t bit) const {
    size_t word = bit / 64;
    size_t offset = bit % 64;
    return bits_[word] & (1ULL << offset);
  }

  void ClearAll() {
    for (size_t i = 0; i < BitsToWords(num_bits_); i++) {
      bits_[i] = 0;
    }
  }

  bool IsEmpty() const {
    for (size_t i = 0; i < BitsToWords(num_bits_); i++) {
      if (bits_[i] != 0) {
        return false;
      }
    }
    return true;
  }

private:
  size_t num_bits_;
  std::vector<unsigned long long> bits_;
};

} // namespace subspace
