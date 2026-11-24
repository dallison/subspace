# C++20 Coroutines (co20) Backend

Subspace can run its server and client internals on C++20 coroutines
(`co20`) instead of the stackful `co` library. This document describes what
changes under `co20`, how to build it, and the rules callers must follow.

The default build still uses `co`. `co20` is selected at build time and the
wire protocol, shared memory layout, and blocking client API are unchanged.

## Building

### Bazel

```bash
bazelisk test --config=cpp20 //client/... //server/...
```

`--config=cpp20` sets `-std=c++20` and `--//:coro_backend=co20`. The config
setting `//:coro_backend_co20` is true under this backend.

### CMake

```bash
cmake -S . -B build -DCMAKE_CXX_STANDARD=20 -DSUBSPACE_CORO_BACKEND=co20
```

`SUBSPACE_CORO_BACKEND` accepts `auto` (the default, which picks co20 for C++20
and later and `co` otherwise), `co`, `co20`, or `asio`. Requesting `co20` with
a standard older than C++20 is an error.

## What Changes Under co20

Under `co20` the server's internal coroutines are `async::Task<T>` values
(`common/async/task.h`) instead of stackful `co` coroutines. Code that needs to
wait on a reply writes `ASYNC_AWAIT(...)` and returns through `ASYNC_RETURN`.
The macros expand to nothing on the `co` and `asio` backends:

| Macro | co20 | co / asio |
|-------|------|-----------|
| `ASYNC_AWAIT(expr)` | `(co_await FastAwait(expr))` | `(expr)` |
| `ASYNC_RETURN ...` | `co_return ...` | `return ...` |
| `ASYNC_BLOCK(expr)` | `SyncWait(expr)` | `(expr)` |

`FastAwait` resumes `ValueTask`s directly and returns `false` from
`await_suspend` whenever the callee finishes without parking on the scheduler,
keeping C stack depth bounded even when the compiler does not tail-call-optimize
coroutine symmetric transfer (for example GCC at `-O0` or under ASan).

A stackless coroutine only suspends at a `co_await` in its own body. Every
function between a coroutine and the point where it waits must therefore also
return `Task<T>`. This is why the reload and buffer chain in the client is
converted as a whole rather than one function at a time.

## Client API: the `Async` Variants

Each client method that can wait on the server has a blocking form and an
`...Async` form that returns `async::Task<T>`:

```cpp
// Blocking caller, outside any coroutine:
absl::StatusOr<Publisher> pub = client.CreatePublisher("chan", options);

// Caller inside a coroutine (for example, a server-side in-process client):
absl::StatusOr<Publisher> pub =
    co_await client.CreatePublisherAsync("chan", options);
```

The naming scheme is `<SyncName>Async`. The blocking method is a thin wrapper
over the Async method:

```cpp
absl::StatusOr<Message> ReadMessageInternal(ReadMode mode, bool pass_activation, bool clear_trigger) {
  return ASYNC_BLOCK(client_->ReadMessageInternal(impl_.get(), mode, pass_activation, clear_trigger));
}
```

`SyncWait(task)` runs a task to completion from non-coroutine code. It hides the
current coroutine while it runs and aborts if the task suspends.

## The In-Process Rule

**Code that runs on the server's scheduler thread must use the Async API.**

The server runs every coroutine on one scheduler thread. A blocking client call
from a server coroutine waits for a reply that only that same thread can
deliver, so it deadlocks. `SERVER_CLIENT_CALL((blocking), async...)` in
`server/server.h` picks the right form per backend. For example, the bridge
transmitter reads through `ReadMessageInternalAsync` under co20.

Code that runs on a separate thread or process (ordinary applications, the C
and Rust clients, and tests that run the client from `main`) keeps using the
blocking API.

## Reload Design

Buffers can be added or removed while a client is running. When a client
receives a buffer it does not yet have mapped, it asks the server for the
buffer, which is a round trip. Under co20 that round trip is a `Task`:

- `Channel::CheckReload()` is `async::Task<void>` and loops over
  `reload_callback_`, which is `std::function<async::Task<bool>(Channel *)>`.
  In the default build `Task<bool>` is plain `bool`.
- `ClientChannel`'s buffer functions (`AttachBuffers`, `AttachSplitBuffers`,
  `ValidateSlotBuffer`, `GetRegisteredClientBuffer`, `UnmapForReload`, and so
  on) are Tasks. The publisher and subscriber slot search that calls them
  (`FindFreeSlotUnreliable`, `NextSlot`, `LastSlot`, and similar) is also a Task.
- `Channel::Buffer()` does not reload under co20. It breaks out of its loop
  instead, so callers must `ASYNC_AWAIT(CheckReload())` first.
  `ValidateSlotBuffer` does this.
- `ClientChannel::Unmap()` is virtual and stays synchronous through
  `ASYNC_BLOCK(UnmapAsync())`.

Under co20 a blocking sleep inside the reload path is replaced by
`async::Sleep(async::Context(co20::self), ...)`, so waiting yields the
scheduler instead of blocking it.

## Known Limitations

- **Destroying a Publisher or Subscriber inside a coroutine.** The destructors
  call `ASYNC_BLOCK(RemovePublisher/RemoveSubscriber)` and the sync `Unmap`. A
  destructor that has to unregister a user-provided split buffer makes a round
  trip, which blocks when it runs inside a coroutine. Call `CloseAsync()` first
  in that case. `CloseAsync()` does not unmap the buffers itself, because a
  live `Message` may still reference them.
- **Coroutine frames on hot paths.** A co20 coroutine allocates its frame on
  each call. Code that asserts zero allocations on a hot path does not hold
  under co20. `//client:alloc_test` is gated for this reason and runs under the
  default backend.
- **Client thread-safe mode.** Thread-safe mode is not compatible with the
  Async API.
- **Destruction order.** `co20` `Scheduler::~Scheduler` destroys suspended
  frames. Stop the server gracefully (non-force `Stop`) so that no coroutine is
  left suspended when the scheduler is destroyed.

## Gated Targets

These targets depend on the `co` stackful API, or on the zero-allocation
property above. They are marked `target_compatible_with` the incompatible
platform under `--config=cpp20`, so `bazelisk test --config=cpp20 //...`
skips them:

- `//client:alloc_test`
- `//c_client:client_test`
- `//rpc/client:client_test`
- `//rpc/server:server_test`
- `//rpc/test:rpc_test`
- `//shadow:shadow_test`

`co20_rpc/` is the existing co20 RPC library. The `rpc/` tree is co-only and
is not part of the co20 build.

## Testing

```bash
# Default backend:
bazelisk test //...

# co20 backend:
bazelisk test --config=cpp20 //...

# Sanitizers under co20 (use Linux; macOS ASan is unreliable):
bazelisk test --config=cpp20 --config=asan  <targets>
bazelisk test --config=cpp20 --config=tsan  <targets>
```

Run the bridge and tunnel suites under co20 when you change the reload chain.
They exercise the in-process Async paths end to end.
