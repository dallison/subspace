# C++20 Coroutines (`co20`) — Linux Verification & Debug Handoff

This document is a handoff guide for continuing verification and debugging of
the C++20 stackless coroutine (`co20`) backend on a native Linux host.

For the general architecture and API rules of the `co20` backend, read
`docs/cpp20-coroutines.md` first.

---

## 1. Current State on Branch `co20`

- **Branch**: `co20` (`origin/co20`)
- **Workspace rule**: Always use `bazelisk` (not `bazel`).
- **What is implemented**:
  - `common/async/` (`task.h`, `wait.h`, `wait.cc`, `socket.*`, `byte_stream.*`,
    `runtime.h`) supports `SUBSPACE_CORO_BACKEND_CO20` (`--config=cpp20`).
  - `ClientImpl` server round-trips, channel reload callbacks
    (`Channel::CheckReload`), split-buffer registration/lookup/unmapping, and
    `PublisherImpl`/`SubscriberImpl` slot searches return `async::Task<T>`
    (`co20::ValueTask<T>`) via `ASYNC_AWAIT` / `ASYNC_RETURN`.
  - Public `Client`, `Publisher`, and `Subscriber` blocking APIs remain
    unchanged via `ASYNC_BLOCK` (`SyncWait`), and `<SyncName>Async` methods
    (including `CloseAsync`) suspend on a `co20::Scheduler`.
  - `Server`, `ClientHandler`, `TunnelManager`, FFI, Python server bindings,
    and plugins run on `async::RuntimeEngine` / `async::Task`, using
    `SERVER_CLIENT_CALL` and `SERVER_CLIENT_CLOSE` for in-process clients on
    the server's scheduler thread.
  - Gated targets (`target_compatible_with` skipped under `//:coro_backend_co20`):
    `//client:alloc_test`, `//c_client:client_test`, `//rpc/client:client_test`,
    `//rpc/server:server_test`, `//rpc/test:rpc_test`, and `//shadow:shadow_test`.

### Already Verified Before Handoff
- **macOS full default (`co`) suite**: `bazelisk test //...` — **passed**.
- **macOS full `co20` suite**: `bazelisk test --config=cpp20 //...` — **passed**
  (all 31 compatible targets pass; 12 incompatible targets skipped).
- **Linux (`ubuntu:24.04` GCC in Docker) targeted runs**:
  `//client:client_test`, `//client:bridge_test`, `//server:server_test`,
  `//server:tunnel_test`, `//server:tunnel_fake_peer_test`, and
  `//server:tunnel_stress_test` passed under `--config=cpp20` and
  `--config=cpp20 --config=asan`.

---

## 2. Prioritized Linux Test Matrix to Run

Run these in order on the Linux host and investigate/fix any failures:

### Step 1: Full `co20` Bazel Suite (matches `.github/workflows/ci.yml`)
```bash
bazelisk test --config=cpp20 --test_output=errors //...
```

### Step 2: Full Default (`co`) Regression Suite
```bash
bazelisk test --test_output=errors //...
```

### Step 3: AddressSanitizer under `co20`
Catches coroutine frame use-after-free across `co_await`, dangling lambda
captures, and stack overflows:
```bash
bazelisk test --config=cpp20 --config=asan --test_output=errors \
  //common/... //client/... //server/... //co20_rpc/... //asio_rpc/... //coro_rpc/...
```
Also stress-run the multi-coroutine bridge and tunnel tests without cache:
```bash
bazelisk test --config=cpp20 --config=asan --nocache_test_results --runs_per_test=5 --test_output=errors \
  //client:bridge_test //server:tunnel_test //server:tunnel_fake_peer_test //server:tunnel_stress_test
```

### Step 4: ThreadSanitizer under `co20`
Catches races between blocking client threads (`SyncWait`), shared memory
atomics, and the server scheduler thread (specify C++ targets directly so Rust
targets do not fail linking TSan):
```bash
bazelisk test --config=cpp20 --config=tsan --test_output=errors \
  //common/... //client:client_test //client:bridge_test \
  //server:server_test //server:tunnel_test //server:tunnel_fake_peer_test \
  //server:tunnel_stress_test //co20_rpc/...
```

### Step 5: Linux Anonymous `memfd` Shared Memory (`--config=linux_memfd`)
**Important**: Default Linux builds use `/dev/shm` (`SUBSPACE_SHMEM_MODE_LINUX`).
`--config=linux_memfd` switches to `SUBSPACE_SHMEM_MODE_MEMFD` (the same path
Android uses), which compiles additional `ASYNC_AWAIT` code paths in
`client/client_channel.cc` (`CreateSplitBufferSet`, `OpenSplitBufferSet`,
`GetRegisteredClientBuffer`, `PublisherImpl::CreateOrAttachBuffers`) that were
not compiled in `/dev/shm` or macOS POSIX builds:
```bash
bazelisk test --config=cpp20 --config=linux_memfd --test_output=errors \
  //client:client_test //client:bridge_test //server:server_test //server:tunnel_test
```

### Step 6: Mutual TLS (`--config=mtls`) & 64-bit Slot Sizes (`--config=slot_size_64`)
Exercises the `co20` OpenSSL paths in `common/async/byte_stream.cc` and widened
API signatures in `client/client.h`:
```bash
bazelisk test --config=cpp20 --config=mtls --test_output=errors \
  //client:bridge_test //server:tunnel_test //server:tunnel_stress_test

bazelisk test --config=cpp20 --config=slot_size_64 --test_output=errors \
  //client:client_test //server:server_test
```

### Step 7: Clang (`--config=clang`) vs GCC
If both `g++` and `clang++` are installed on the Linux machine, run at least
`//client:client_test`, `//client:bridge_test`, `//server:tunnel_stress_test`,
and `//co20_rpc/test:rpc_test` under `--config=cpp20 --config=clang` as well.
Clang and GCC lower coroutine frames and symmetric transfer differently.

### Step 8: Native CMake C++20 Build
```bash
cmake -S . -B build-co20 -DCMAKE_CXX_STANDARD=20 -DSUBSPACE_CORO_BACKEND=co20
cmake --build build-co20 --target subspace_server client_test server_test co20_rpc_test -j
ctest --test-dir build-co20 --output-on-failure -R "^(client_test|server_test|co20_rpc.*)$"
```
> **Known CMake gap to check/fix if needed**: In Bazel, `c_client_test`,
> `shadow_test`, `rpc_client_test`, `rpc_server_test`, and `rpc_test` are gated
> off under `//:coro_backend_co20` because those test `.cc` files construct
> `subspace::Server` with a `co::CoroutineScheduler` directly. In CMake
> (`c_client/CMakeLists.txt`, `shadow/CMakeLists.txt`, `rpc/CMakeLists.txt`),
> those test executables are not yet wrapped in
> `if(NOT SUBSPACE_CORO_BACKEND_RESOLVED STREQUAL "co20")`, so a bare
> `cmake --build build-co20` (all targets) will try to compile them against a
> `co20` `libserver`. Keep `subspace_rpc_plugin` in `rpc/CMakeLists.txt`
> enabled if you gate the `rpc_*_test` targets, because `co20_rpc` uses that
> host plugin to generate its stubs.

---

## 3. Linux Bugs Already Found & Fixed (Read Before Debugging Similar Symptoms)

### Finding A: GCC `-O0` / ASan C-Stack Overflow on Symmetric Transfer
- **Symptom**: `SIGSEGV` on a guard page or `AddressSanitizer: stack-overflow`
  deep inside `ValueTaskAwaiter::await_suspend` / `actor(frame*)` chains during
  long non-parking loops (originally seen in `//server:tunnel_stress_test`).
- **Root cause**: At `-O0` (`fastbuild`) and under AddressSanitizer, GCC does
  not tail-call-optimize C++20 coroutine symmetric transfer
  (`std::coroutine_handle<> await_suspend(...)`, GCC bug 100897). Every raw
  `co_await` of a `co20::ValueTask<T>` that finishes without parking on the
  scheduler pushes C stack frames that only unwind when a coroutine finally
  suspends back to `co20::Scheduler::Run`.
- **How it is fixed**: `ASYNC_AWAIT(expr)` in `common/async/task.h` wraps
  `ValueTask`s in `detail::FastAwait(...)` (`FastValueTaskAwaiter`). Its
  `bool await_suspend(caller)` resumes the child inline; if `handle_.done()` is
  `true`, `continuation_` was never set, `resume()` returns normally, and
  `await_suspend` returns `false` so the caller continues with \(O(1)\) C stack
  depth. Only when the child actually parks on `co20::Scheduler` (`!done()`)
  does it wire `continuation_ = caller` and return `true`.
- **Watch out for**: Any code that uses **raw `co_await`** (instead of
  `ASYNC_AWAIT`) on a `ValueTask` inside a tight non-parking loop can still hit
  this under GCC `-O0` / ASan. Notably, `co20_rpc/` uses raw `co_await` on
  `co20::ValueTask`; if a `co20_rpc` test overflows the stack under ASan on
  Linux, apply `FastAwait` or `ASYNC_AWAIT` there too.

### Finding B: Server Shutdown Reconnect Deadlock on Linux UDS Backlog
- **Symptom**: `TunnelStressTest.ManyChannelsFlowBothWaysAtOnce` hung for 300s
  during `Server::Stop()` teardown on Linux (2/3 runs before fix).
- **Root cause**: Both `ClientHandler` coroutines and in-process client
  coroutines (`TunnelManager`, `ChannelDirectoryCoroutine`, `SendChannelStats`)
  share `Server::shutdown_trigger_fd_` as their `interrupt_fd`. Idle
  `ClientHandler`s have older `LastTick()` timestamps in `co20::Scheduler`, so
  they wake first and close the server end of the UDS sockets. When in-process
  client coroutines wake next and run `SERVER_CLIENT_CLOSE` (`CloseAsync`),
  `send()` fails synchronously with `EPIPE` (`InternalError`, not
  `CancelledError`). `ClientImpl::ShouldReconnect` previously saw a
  non-cancelled error and called synchronous `UnixSocket::Connect` against
  `listen_socket` (`listen(..., 5)`), whose backlog was no longer being drained.
  On Linux, the 6th blocking `connect()` on a full UDS listen backlog blocks in
  the kernel forever, wedging the scheduler thread.
- **How it is fixed**:
  1. `ClientImpl::ShouldReconnect` (`client/client.cc`) checks
     `co->IsAborted()` and does a non-blocking `poll(..., 0)` on
     `co->GetInterruptFd()` under `co20`, returning `false` when shutdown is
     signaled.
  2. `Server::ListenerCoroutine` (`server/server.cc`) yields via
     `co_await ctx.Coroutine()->Yield()` instead of `async::Sleep(ctx, 5ms)`
     while waiting for child coroutines to drain (since `interrupt_fd` is
     already readable during drain, `Sleep` woke immediately via `epoll` churn).
- **Watch out for**: `toolbelt::UnixSocket::Connect` in `ClientImpl::Init` /
  `Reconnect` is synchronous. Never create/connect more than 5 in-process
  clients concurrently before the server's `ListenerCoroutine` gets a turn to
  `accept()` them on the scheduler.

### Finding C: Blocking Client Calls or Unclosed Destructors on the Scheduler Thread
- **Rule**: `ASYNC_BLOCK(expr)` calls `SyncWait`, which sets
  `co20::CoroutineScope(nullptr)` while running `expr`.
  - If `expr` hits a socket wait (`SocketContext()`), `co20::self` is `nullptr`,
    so `common/async/socket.cc` falls back to blocking `poll()`. If the server
    lives on the **same** scheduler thread, blocking `poll()` deadlocks the
    thread forever.
  - If `expr` hits an unconditional coroutine suspension (`!task.done()`),
    `SyncWait` calls `std::abort()`.
- **Destructor trap**: `~Publisher()` and `~Subscriber()` call
  `ASYNC_BLOCK(client_->RemovePublisher(...))` unless `CloseAsync()` was
  awaited first (`closed_ == true`). Every `Publisher` / `Subscriber` owned by
  a server coroutine **must** be closed via `SERVER_CLIENT_CLOSE(x)` on **all**
  exit paths (normal exit, error return, shutdown).

---

## 4. Debugging Playbook on Linux

### 1. Diagnosing a Hang / 300s Test Timeout
Run the test binary directly (or attach to the Bazel test process before timeout)
and dump all thread backtraces:
```bash
# Run directly with GDB batch on hang, or attach to running PID:
gdb -p <PID> -batch -ex "info threads" -ex "thread apply all bt"
```
Check what the `Server` thread (`co20::Scheduler::Run`) is doing:
- **Stuck in `poll()` inside `subspace::async::SyncWait`**:
  Look up the stack above `SyncWait` — a server-side coroutine called a
  synchronous `Client`/`Publisher`/`Subscriber` method (or ran `~Publisher()` /
  `~Subscriber()` without `SERVER_CLIENT_CLOSE`). Convert that call site to its
  `*Async` / `SERVER_CLIENT_CALL` / `SERVER_CLIENT_CLOSE` equivalent.
- **Stuck in `connect()` inside `toolbelt::UnixSocket::Connect`**:
  UDS listen backlog (5) is full while the listener coroutine is either stopped
  or starved on the same scheduler thread.
- **Stuck in `epoll_wait()` inside `co20::Scheduler::Run` during teardown**:
  A server coroutine has not exited during `Server::Stop()`'s drain loop
  (`while (!coroutines_.empty())` in `server/server.cc`). Print the names of
  live coroutines in `coroutines_` (each `co20::Coroutine` has `Name()`) to see
  which coroutine is ignoring `shutdown_trigger_fd_` or waiting without an
  `interrupt_fd`.

### 2. Diagnosing a `SIGSEGV` or Stack Overflow
Re-run the failing test with `--config=cpp20 --config=asan --test_output=errors`:
- **`AddressSanitizer: stack-overflow`**:
  Inspect the repeating 2–6 frame cycle in the ASan trace. One of the calls in
  that cycle is using raw `co_await` instead of `ASYNC_AWAIT` (`FastAwait`), or
  calling a synchronous recursion in `ReadMessageInternal` (e.g. skipping
  activation messages).
- **`AddressSanitizer: heap-use-after-free` on `resume()` or promise access**:
  Check for:
  1. **Coroutine lambda capture lifetime**: In C++20, a lambda coroutine
     `auto task = [x]() -> async::Task<void> { ... }();` stores `x` inside the
     **lambda closure object**, NOT inside the coroutine frame! If the closure
     temporary is destroyed before `task` finishes (e.g. `Spawn`ed onto
     `co20::Scheduler` or stored and `co_await`ed later), any capture (`[this]`,
     `[&]`, `[=]`) is a `heap-use-after-free` / `stack-use-after-return` after
     the first suspension point. Pass values as **function parameters** to a
     named coroutine/static helper (as `Server::SpawnCoroutine` does in
     `server/server.h`), or ensure the `std::function` / closure outlives the
     awaited `Task` within the same full-expression.
  2. **`this` or reference invalidation across `ASYNC_AWAIT`**: Check that
     containers (`channels_`, `client_handlers_`, `egress_channels_`) are not
     mutated/erased by another coroutine while a raw pointer/iterator is held
     across an `ASYNC_AWAIT`.

### 3. Diagnosing `--config=linux_memfd` Failures
If a test fails only with `--config=cpp20 --config=linux_memfd`:
- Focus on `client/client_channel.cc` (`#if SUBSPACE_SHMEM_MODE == SUBSPACE_SHMEM_MODE_MEMFD`
  blocks) and `client/publisher.cc` (`CreateOrAttachBuffers` memfd registration
  and rollback).
- Check `GetRegisteredClientBuffer` retry timing (100 attempts x 1ms via
  `async::Sleep(async::Context(co20::self), 1ms)`): under heavy ASan/TSan load
  on many-core machines, 100ms total retry budget might be tight if the server
  scheduler is busy.
- Check fd ownership (`toolbelt::FileDescriptor`) across `ASYNC_AWAIT` in
  `RegisterClientBuffer` / `GetClientBuffers`.
