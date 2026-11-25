// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_CONTEXT_H
#define _COMMON_ASYNC_CONTEXT_H

// Build-time selection of the coroutine/networking backend used by the
// Subspace server (and the client paths it relies on).  This mirrors the
// SUBSPACE_SHMEM_MODE idiom in common/channel.h: a single macro selects one of
// three fully-compiled-in implementations.
//
//   SUBSPACE_CORO_BACKEND_CO   - the existing `co` coroutine library and the
//                                cpp_toolbelt sockets (the default).
//   SUBSPACE_CORO_BACKEND_ASIO - Boost.Asio stackful coroutines
//                                (yield_context) with an externally-provided
//                                io_context, and Asio TCP/UDP/vsock sockets.
//   SUBSPACE_CORO_BACKEND_CO20 - C++20 stackless coroutines (co20::Scheduler).
//                                Functions that suspend return async::Task<T>
//                                and are awaited (see common/async/task.h).
//
// Everything the server/client coroutines touch is expressed in terms of the
// `subspace::async` abstraction (Context, AsyncRuntime, WaitReadable/Sleep and
// the StreamSocket/UdpSocket facade).  The backends are selected here.
//
// The build (Bazel or CMake) always defines SUBSPACE_CORO_BACKEND.  The
// language mode can differ between translation units (co20_rpc compiles as
// C++20 against a C++17 client library), so the backend must come from the
// build rather than from __cplusplus.  The fallback below is only for builds
// that don't define it.

#define SUBSPACE_CORO_BACKEND_CO 1
#define SUBSPACE_CORO_BACKEND_ASIO 2
#define SUBSPACE_CORO_BACKEND_CO20 3

#ifndef SUBSPACE_CORO_BACKEND
#if __cplusplus >= 202002L && defined(__cpp_impl_coroutine)
#define SUBSPACE_CORO_BACKEND SUBSPACE_CORO_BACKEND_CO20
#else
#define SUBSPACE_CORO_BACKEND SUBSPACE_CORO_BACKEND_CO
#endif
#endif

#if SUBSPACE_CORO_BACKEND != SUBSPACE_CORO_BACKEND_CO &&                        \
    SUBSPACE_CORO_BACKEND != SUBSPACE_CORO_BACKEND_ASIO &&                      \
    SUBSPACE_CORO_BACKEND != SUBSPACE_CORO_BACKEND_CO20
#error "SUBSPACE_CORO_BACKEND must be SUBSPACE_CORO_BACKEND_CO, SUBSPACE_CORO_BACKEND_ASIO or SUBSPACE_CORO_BACKEND_CO20"
#endif

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/spawn.hpp>

namespace subspace::async {

// In the Asio backend a coroutine is driven by an explicit yield_context value
// that is threaded down the call chain (there is no thread-local "current
// coroutine").  Functions take a Context by value.
using Context = boost::asio::yield_context;

// A placeholder Context for code paths that never actually suspend (e.g. a
// blocking client whose socket fds are left blocking, so the EAGAIN -> yield
// path is never reached).  The returned value MUST NOT be used to drive an
// asynchronous operation - it has no associated coroutine.  This mirrors the
// co backend where a null co::Coroutine* means "block, don't yield".
inline Context NullContext() {
  return boost::asio::yield_context(nullptr, boost::asio::any_io_executor());
}

}  // namespace subspace::async

#elif SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20

#if __cplusplus < 202002L
#error "SUBSPACE_CORO_BACKEND_CO20 requires C++20 (-std=c++20)"
#endif

#include "co/coroutine_cpp20.h"
#include "toolbelt/async_waiter.h"
#include <cstddef>

#if !CO20_HAVE_COROUTINES
#error "SUBSPACE_CORO_BACKEND_CO20 requires compiler support for C++20 coroutines"
#endif

namespace subspace::async {

// Says how code driven by this Context waits for an fd or a timer.  With a
// co20 coroutine the wait suspends it, so the function doing the wait must be
// awaited.  Otherwise the wait blocks the calling thread: through the
// AsyncWaiter if there is one (a stackful co::Coroutine yields to its own
// scheduler there), or with poll().  A Context that doesn't suspend is what
// SyncWait (task.h) needs.
class Context {
public:
  constexpr Context() = default;
  constexpr Context(std::nullptr_t) {}
  constexpr Context(co20::Coroutine *coroutine) : coroutine_(coroutine) {}
  constexpr Context(const toolbelt::AsyncWaiter *waiter) : waiter_(waiter) {}

  co20::Coroutine *Coroutine() const { return coroutine_; }
  const toolbelt::AsyncWaiter *Waiter() const { return waiter_; }
  bool Suspends() const { return coroutine_ != nullptr; }

private:
  co20::Coroutine *coroutine_ = nullptr;
  const toolbelt::AsyncWaiter *waiter_ = nullptr;
};

// A Context that blocks with poll().
inline Context NullContext() { return Context(); }

}  // namespace subspace::async

#else  // SUBSPACE_CORO_BACKEND_CO

#include "co/coroutine.h"

namespace subspace::async {

// In the co backend the "context" is a pointer to the currently running
// coroutine.  This is exactly the type that the cpp_toolbelt socket methods
// accept (const co::Coroutine *), so the co socket facade can be a thin alias
// over toolbelt and call sites pass the Context straight through.
using Context = const co::Coroutine *;

// A null coroutine pointer means "block, don't yield" in the co backend.
inline Context NullContext() { return nullptr; }

}  // namespace subspace::async

#endif

namespace subspace::async {

// True when compiled against the Asio backend.  Usable in `if constexpr` and
// ordinary runtime checks so call sites can avoid macro soup where convenient.
inline constexpr bool kIsAsioBackend =
    SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO;

// True when compiled against the C++20 stackless coroutine backend.
inline constexpr bool kIsCo20Backend =
    SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20;

}  // namespace subspace::async

// The backend changes the layout of Server, Client and the socket types, so
// objects built for different backends must not be linked together.  Every
// translation unit that includes this header references a symbol named for
// its backend, and the async library defines only the one it was built for,
// so a mismatch fails at link time.
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
#define SUBSPACE_CORO_BACKEND_LINK_SYMBOL subspace_coro_backend_asio
#elif SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_CO20
#define SUBSPACE_CORO_BACKEND_LINK_SYMBOL subspace_coro_backend_co20
#else
#define SUBSPACE_CORO_BACKEND_LINK_SYMBOL subspace_coro_backend_co
#endif

extern "C" const int SUBSPACE_CORO_BACKEND_LINK_SYMBOL;
[[maybe_unused]] __attribute__((used)) static const int
    *const subspace_coro_backend_check = &SUBSPACE_CORO_BACKEND_LINK_SYMBOL;

#endif  // _COMMON_ASYNC_CONTEXT_H
