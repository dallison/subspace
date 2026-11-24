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

#define ASYNC_AWAIT(expr) (co_await (expr))
#define ASYNC_RETURN co_return

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
  if constexpr (!std::is_void_v<T>) {
    return std::move(task.handle_.promise().value_);
  }
}

}  // namespace subspace::async

#else

namespace subspace::async {

template <typename T> using Task = T;

#define ASYNC_AWAIT(expr) (expr)
#define ASYNC_RETURN return

template <typename T> T SyncWait(T value) { return value; }

}  // namespace subspace::async

#endif

#endif  // _COMMON_ASYNC_TASK_H
