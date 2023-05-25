// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_BITSET_H
#define __COMMON_BITSET_H

#include <array>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
namespace subspace {

// This is a fixed size bitset.
template <int Size> class BitSet {
public:
  BitSet() { Init(); }

  void Init() {
    bits_.fill({}); // Zero out all bits.
  }
  // Allocate the first available bit.
  absl::StatusOr<int> Allocate(const std::string &type);

  // Are all bits clear?
  bool IsEmpty() const;

  // Set bit b.
  void Set(int b);

  // Clear bit b.
  void Clear(int b);

  // Is bit b set.
  bool IsSet(int b) const;

private:
  static constexpr size_t kBitsPerWord = sizeof(long long) * 8;
  static constexpr size_t kNumWords = Size / kBitsPerWord;

  // Note the use of explicit long long type here because
  // we use ffsll to look for the set bits and that is
  // explicit in its use of long long.
  std::array<long long, kNumWords> bits_;
};

template <int Size>
inline absl::StatusOr<int> BitSet<Size>::Allocate(const std::string &type) {
  for (int i = 0; i < kNumWords; i++) {
    int bit = ffsll(~bits_[i]);
    if (bit != 0) {
      bits_[i] |= (1LL << (bit - 1));
      return i * kBitsPerWord + (bit - 1);
    }
  }
  return absl::InternalError(
      absl::StrFormat("No capacity for another %s", type));
}

template <int Size> inline void BitSet<Size>::Clear(int b) {
  int word = b / kBitsPerWord;
  int bit = b % kBitsPerWord;
  bits_[word] &= ~(1 << bit);
}

template <int Size> inline void BitSet<Size>::Set(int b) {
  int word = b / kBitsPerWord;
  int bit = b % kBitsPerWord;
  bits_[word] |= (1 << bit);
}

template <int Size> inline bool BitSet<Size>::IsEmpty() const {
  for (int i = 0; i < kNumWords; i++) {
    if (bits_[i] != 0) {
      return false;
    }
  }
  return true;
}

template <int Size> inline bool BitSet<Size>::IsSet(int b) const {
  int word = b / kBitsPerWord;
  int bit = b % kBitsPerWord;
  return (bits_[word] & (1 << bit)) != 0;
}

} // namespace subspace

#endif // __COMMON_BITSET_H
