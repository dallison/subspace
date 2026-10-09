// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "asil_client/shm_layout.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace subspace {
namespace asil {

// A view of an AtomicBitSet in shared memory.  The memory orders match
// common/atomic_bitset.h.  The bit count stored in shared memory is ignored;
// the view uses the count the server reported for the channel.
class BitsetView {
public:
  BitsetView() = default;
  BitsetView(void *bitset, int num_bits)
      : words_(reinterpret_cast<std::atomic<uint64_t> *>(
            static_cast<char *>(bitset) + sizeof(size_t))),
        num_bits_(num_bits) {}

  void Set(int bit) {
    words_[bit / 64].fetch_or(Mask(bit), std::memory_order_relaxed);
  }

  void Clear(int bit) {
    words_[bit / 64].fetch_and(~Mask(bit), std::memory_order_relaxed);
  }

  bool SetWasClear(int bit) {
    const uint64_t old =
        words_[bit / 64].fetch_or(Mask(bit), std::memory_order_release);
    return (old & Mask(bit)) == 0;
  }

  bool ClearWasSet(int bit) {
    const uint64_t old =
        words_[bit / 64].fetch_and(~Mask(bit), std::memory_order_acquire);
    return (old & Mask(bit)) != 0;
  }

  bool IsSet(int bit) const {
    return (words_[bit / 64].load(std::memory_order_relaxed) & Mask(bit)) != 0;
  }

  bool IsSetSeqCst(int bit) const {
    return (words_[bit / 64].load(std::memory_order_seq_cst) & Mask(bit)) != 0;
  }

  bool IsEmpty() const {
    for (int i = 0; i < NumWords(); i++) {
      if (words_[i].load(std::memory_order_relaxed) != 0) {
        return false;
      }
    }
    return true;
  }

  int FindFirstSet() const {
    for (int i = 0; i < NumWords(); i++) {
      const uint64_t word = words_[i].load(std::memory_order_relaxed);
      if (word != 0) {
        const int bit = i * 64 + __builtin_ctzll(word);
        return bit < num_bits_ ? bit : -1;
      }
    }
    return -1;
  }

  // Calls fn(bit) for each set bit, loading each word once.
  template <typename Fn>
  void Traverse(Fn &&fn,
                std::memory_order order = std::memory_order_relaxed) const {
    for (int i = 0; i < NumWords(); i++) {
      uint64_t word = words_[i].load(order);
      while (word != 0) {
        const int bit = i * 64 + __builtin_ctzll(word);
        if (bit >= num_bits_) {
          return;
        }
        fn(bit);
        word &= word - 1;
      }
    }
  }

private:
  static uint64_t Mask(int bit) { return uint64_t{1} << (bit % 64); }
  int NumWords() const { return static_cast<int>(shm::BitsToWords(num_bits_)); }

  std::atomic<uint64_t> *words_ = nullptr;
  int num_bits_ = 0;
};

} // namespace asil
} // namespace subspace
