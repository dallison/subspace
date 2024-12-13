#pragma once

#include "absl/container/flat_hash_set.h"
#include <vector>

namespace subspace {

template <typename T, int N>
class FastRingBuffer {
public:
  FastRingBuffer() = default;

  void Insert(T value) {
    if (set_.contains(value)) {
      return;
    }
    if (set_.size() == N) {
      set_.erase(buffer_[head_]);
      head_ = (head_ + 1) % N;
    }
    buffer_[tail_] = value;
    set_.insert(value);
    tail_ = (tail_ + 1) % N;
  }

  bool Contains(T value) const { return set_.contains(value); }

  size_t Size() const { return set_.size(); }

  void Traverse(std::function<void(T)> func) const {
    for (int i = head_; i != tail_; i = (i + 1) % N) {
      func(buffer_[i]);
    }
  }
  
private:
  std::array<T, N> buffer_;
  int head_ = 0;
  int tail_ = 0;
  absl::flat_hash_set<T> set_;
};

}