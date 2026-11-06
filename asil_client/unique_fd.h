// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <unistd.h>

namespace subspace {
namespace asil {

// An owned file descriptor, closed on destruction.
class UniqueFd {
public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}
  ~UniqueFd() { Reset(); }

  UniqueFd(const UniqueFd &) = delete;
  UniqueFd &operator=(const UniqueFd &) = delete;

  UniqueFd(UniqueFd &&other) noexcept : fd_(other.Release()) {}
  UniqueFd &operator=(UniqueFd &&other) noexcept {
    if (this != &other) {
      Reset(other.Release());
    }
    return *this;
  }

  int Get() const { return fd_; }
  bool Valid() const { return fd_ >= 0; }

  int Release() {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }

  void Reset(int fd = -1) {
    if (fd_ >= 0) {
      (void)::close(fd_);
    }
    fd_ = fd;
  }

private:
  int fd_ = -1;
};

// A fixed-capacity list of owned file descriptors.
template <int kCapacity> class FdList {
public:
  static constexpr int Capacity() { return kCapacity; }

  // Takes ownership of fd.  Returns false, closing fd, if the list is full.
  bool Add(UniqueFd fd) {
    if (count_ >= kCapacity) {
      return false;
    }
    fds_[count_++] = static_cast<UniqueFd &&>(fd);
    return true;
  }

  void Clear() {
    for (int i = 0; i < count_; i++) {
      fds_[i].Reset();
    }
    count_ = 0;
  }

  int Size() const { return count_; }
  int Get(int i) const { return fds_[i].Get(); }

private:
  UniqueFd fds_[kCapacity];
  int count_ = 0;
};

} // namespace asil
} // namespace subspace
