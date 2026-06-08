// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_CONTEXT_H
#define _COMMON_ASYNC_CONTEXT_H

// Build-time selection of the coroutine/networking backend used by the
// Subspace server (and the client paths it relies on).  This mirrors the
// SUBSPACE_SHMEM_MODE idiom in common/channel.h: a single macro selects one of
// two fully-compiled-in implementations.
//
//   SUBSPACE_CORO_BACKEND_CO   - the existing `co` coroutine library and the
//                                cpp_toolbelt sockets (the default).
//   SUBSPACE_CORO_BACKEND_ASIO - Boost.Asio stackful coroutines
//                                (yield_context) with an externally-provided
//                                io_context, and Asio TCP/UDP/vsock sockets.
//
// Everything the server/client coroutines touch is expressed in terms of the
// `subspace::async` abstraction (Context, AsyncRuntime, WaitReadable/Sleep and
// the StreamSocket/UdpSocket facade).  The two backends are selected here.

#define SUBSPACE_CORO_BACKEND_CO 1
#define SUBSPACE_CORO_BACKEND_ASIO 2

#ifndef SUBSPACE_CORO_BACKEND
#define SUBSPACE_CORO_BACKEND SUBSPACE_CORO_BACKEND_CO
#endif

#if SUBSPACE_CORO_BACKEND != SUBSPACE_CORO_BACKEND_CO &&                        \
    SUBSPACE_CORO_BACKEND != SUBSPACE_CORO_BACKEND_ASIO
#error "SUBSPACE_CORO_BACKEND must be SUBSPACE_CORO_BACKEND_CO or SUBSPACE_CORO_BACKEND_ASIO"
#endif

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO

#include <boost/asio/spawn.hpp>

namespace subspace::async {

// In the Asio backend a coroutine is driven by an explicit yield_context value
// that is threaded down the call chain (there is no thread-local "current
// coroutine").  Functions take a Context by value.
using Context = boost::asio::yield_context;

}  // namespace subspace::async

#else  // SUBSPACE_CORO_BACKEND_CO

#include "co/coroutine.h"

namespace subspace::async {

// In the co backend the "context" is a pointer to the currently running
// coroutine.  This is exactly the type that the cpp_toolbelt socket methods
// accept (const co::Coroutine *), so the co socket facade can be a thin alias
// over toolbelt and call sites pass the Context straight through.
using Context = const co::Coroutine *;

}  // namespace subspace::async

#endif

namespace subspace::async {

// True when compiled against the Asio backend.  Usable in `if constexpr` and
// ordinary runtime checks so call sites can avoid macro soup where convenient.
inline constexpr bool kIsAsioBackend =
    SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO;

}  // namespace subspace::async

#endif  // _COMMON_ASYNC_CONTEXT_H
