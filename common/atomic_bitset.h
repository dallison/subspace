#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <iostream>

namespace subspace {

inline constexpr size_t BitsToWords(size_t bits) { return bits / 65 + 1; }

template <size_t SizeInBits> class AtomicBitSet {
public:
  AtomicBitSet() { Init(); }

  // In SizeInBits == 0, we can store the bits outside the object and this
  // constructor allows us to specify how many there are.
  AtomicBitSet(size_t num_bits) { Init(num_bits); }

  void Init() {
    for (size_t i = 0; i < BitsToWords(num_bits_); i++) {
      bits_[i].store(0, std::memory_order_relaxed);
    }
 }

  void Init(size_t num_bits) {
    num_bits_ = num_bits;
    for (size_t i = 0; i < BitsToWords(num_bits_); i++) {
      bits_[i].store(0, std::memory_order_relaxed);
    }
  }
  void Set(size_t bit) {
    size_t word = bit / 64;
    size_t offset = bit % 64;
    bits_[word].fetch_or(1ULL << offset, std::memory_order_relaxed);
  }

  void Clear(size_t bit) {
    size_t word = bit / 64;
    size_t offset = bit % 64;
    bits_[word].fetch_and(~(1ULL << offset), std::memory_order_relaxed);
  }

  bool IsSet(size_t bit) const {
    size_t word = bit / 64;
    size_t offset = bit % 64;
    return bits_[word].load(std::memory_order_relaxed) & (1ULL << offset);
  }

  void ClearAll() {
    for (size_t i = 0; i < BitsToWords(num_bits_); i++) {
      bits_[i].store(0, std::memory_order_relaxed);
    }
  }

  bool IsEmpty() const {
    for (size_t i = 0; i < BitsToWords(num_bits_); i++) {
      if (bits_[i].load(std::memory_order_relaxed) != 0) {
        return false;
      }
    }
    return true;
  }

  void Traverse(std::function<void(size_t)> func) const {
    for (size_t i = 0; i < BitsToWords(num_bits_); i++) {
      int shift = 0;
      int bit = i * 64;
      while (bit < num_bits_ && shift < 64) {
        uint64_t word = bits_[i].load(std::memory_order_relaxed) >> shift;
        size_t n = ffsll(word);
        if (n == 0) {
          break;
        }
        bit += n;
        func(bit - 1);
        shift += n;
      }
    }
  }

private:
  // If SizeInBits is 0, then kNumWords is 0 (allows bits to be stored outside
  // the object).
  size_t num_bits_ = SizeInBits;
  std::atomic<uint64_t> bits_[SizeInBits == 0 ? 0 : BitsToWords(SizeInBits)];
};

inline size_t SizeofAtomicBitSet(size_t size_in_bits) {
  return sizeof(std::atomic<uint64_t>) * BitsToWords(size_in_bits);
}

// An atomic bitset with its bits not stored in the object.
using InPlaceAtomicBitset = AtomicBitSet<0>;

} // namespace subspace
