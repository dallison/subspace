// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_TASK_H
#define _COMMON_ASYNC_TASK_H

// Lets a function that waits be written once for every backend.
//
// Under the co and Asio backends a coroutine has its own stack, so a function
// that waits is an ordinary function.  Under co20 a coroutine can only suspend
// at a co_await in its own body, so every function between a coroutine and a
// wait has to be a coroutine itself, returning an awaitable task.  A function
// written as
//
//   async::Task<absl::Status> Foo(async::Context ctx) {
//     absl::Status s = ASYNC_AWAIT(async::WaitReadable(ctx, fd));
//     ASYNC_RETURN s;
//   }
//
// is a plain function returning absl::Status under co and Asio, and a C++20
// coroutine returning co20::ValueTask<absl::Status> under co20.  Under co20 a
// Task<T> function must contain at least one ASYNC_AWAIT or ASYNC_RETURN, and
// must use ASYNC_RETURN rather than return.
//
// A Task is lazy: it starts when it is awaited.  Arguments are only safe to
// pass by reference when the task is awaited in the same full expression that
// creates it, which ASYNC_AWAIT(Foo(...)) always does.

#include "common/async/context.h"

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20

#include <cstdio>
#include <cstdlib>
#include <type_traits>
#include <utility>

namespace subspace::async {

template <typename T> using Task = co20::ValueTask<T>;

namespace detail {

// Runs `task` inline inside await_suspend when it finishes without parking on
// the scheduler, returning `false` so the caller resumes in place instead of
// via symmetric transfer.  GCC at -O0 and under AddressSanitizer does not
// tail-call symmetric transfers (GCC bug 100897), so a coroutine loop whose
// awaits finish without parking would otherwise grow the C stack on every
// iteration and overflow.  When `task` does park on the scheduler, `done()` is
// false, `continuation_` is wired to `caller`, and returning `true` unwinds
// the C stack back to `Scheduler::Run`.
template <typename T> struct [[nodiscard]] FastValueTaskAwaiter {
  co20::ValueTask<T> task_;

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> caller) noexcept {
    task_.handle_.resume();
    if (task_.handle_.done()) {
      return false;
    }
    task_.handle_.promise().continuation_ = caller;
    return true;
  }

  T await_resume() { return task_.await_resume(); }
};

template <typename T> struct [[nodiscard]] FastValueTaskRefAwaiter {
  co20::ValueTask<T> &task_;

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> caller) noexcept {
    task_.handle_.resume();
    if (task_.handle_.done()) {
      return false;
    }
    task_.handle_.promise().continuation_ = caller;
    return true;
  }

  T await_resume() { return task_.await_resume(); }
};

template <typename A> A FastAwait(A &&a) { return std::forward<A>(a); }

template <typename T>
FastValueTaskAwaiter<T> FastAwait(co20::ValueTask<T> &&task) {
  return FastValueTaskAwaiter<T>{std::move(task)};
}

template <typename T>
FastValueTaskRefAwaiter<T> FastAwait(co20::ValueTask<T> &task) {
  return FastValueTaskRefAwaiter<T>{task};
}

}  // namespace detail

// Variadic so an expression with an unparenthesised comma, such as a call to
// a template with several arguments, works.
#define ASYNC_AWAIT(...)                                                       \
  (co_await ::subspace::async::detail::FastAwait(__VA_ARGS__))
#define ASYNC_RETURN co_return
// Runs a task to completion from code that isn't a coroutine, blocking the
// thread instead of suspending.  See SyncWait.
#define ASYNC_BLOCK(...) (::subspace::async::SyncWait(__VA_ARGS__))

// Runs `task` to completion from code that is not a coroutine.  Every wait in
// the task must use a Context that doesn't suspend, so the task never
// suspends and is finished when this returns.
template <typename T> T SyncWait(Task<T> task) {
  // Each co_await of a ValueTask points co20::self's handle at the awaiting
  // frame.  These frames belong to this call, not to co20::self, so hide it.
  co20::Coroutine *self = std::exchange(co20::self, nullptr);
  task.handle_.resume();
  co20::self = self;
  if (!task.handle_.done()) {
    std::fprintf(stderr, "async::SyncWait: task suspended; its Context must "
                         "not hold a coroutine\n");
    std::abort();
  }
  auto &promise = task.handle_.promise();
  if (promise.exception_) {
    std::rethrow_exception(promise.exception_);
  }
  if constexpr (!std::is_void_v<T>) {
    return std::move(*promise.value_);
  }
}

}  // namespace subspace::async

#else

namespace subspace::async {

template <typename T> using Task = T;

#define ASYNC_AWAIT(...) (__VA_ARGS__)
#define ASYNC_RETURN return
#define ASYNC_BLOCK(...) (__VA_ARGS__)

template <typename T> T SyncWait(T value) { return value; }

}  // namespace subspace::async

#endif

#endif  // _COMMON_ASYNC_TASK_H
