// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_MUTEX_H
#define __COMMON_MUTEX_H

#include <pthread.h>

namespace subspace {

// Mutex RAII class
class toolbelt::MutexLock {
public:
  toolbelt::MutexLock(pthread_mutex_t *mutex) : mutex_(mutex) {
    int e = pthread_mutex_lock(mutex_);
    if (e == EOWNERDEAD) {
      // We hit the tiny race in glibc.  There's not
      // much we can do.  The memory could be in any
      // state.  The only safe thing to do is abort.
      abort();
    }
  }
  ~toolbelt::MutexLock() { pthread_mutex_unlock(mutex_); }

private:
  pthread_mutex_t *mutex_;
};

// ReadWrite lock RAII class.
class RWLock {
public:
  // Pass read=true to lock for reading.  There may be multiple readers
  // at once, but only one writer.
  RWLock(pthread_rwlock_t *lock, bool read) : lock_(lock) {
    if (read) {
      ReadLock();
    } else {
      WriteLock();
    }
  }

  ~RWLock() { Unlock(); }

  void ReadLock() { pthread_rwlock_rdlock(lock_); }

  void WriteLock() { pthread_rwlock_wrlock(lock_); }

  void Unlock() { pthread_rwlock_unlock(lock_); }

private:
  pthread_rwlock_t *lock_;
};
} // namespace subspace

#endif //  __COMMON_MUTEX_H
