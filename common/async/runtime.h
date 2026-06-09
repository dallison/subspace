// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_RUNTIME_H
#define _COMMON_ASYNC_RUNTIME_H

#include "common/async/context.h"
#include <functional>
#include <string>
#include <vector>

#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <list>
#include <memory>
#include <thread>
#endif

namespace subspace::async {

// The execution engine an embedder provides to a Server (and to AsyncRuntime).
// Under the co backend this is a co::CoroutineScheduler; under Asio it is a
// boost::asio::io_context.  Exposed so callers (and tests) can declare an
// engine variable in a backend-agnostic way and pass it to the Server.
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
using RuntimeEngine = boost::asio::io_context;
#else
using RuntimeEngine = co::CoroutineScheduler;
#endif

// Options that mirror the subset of co::CoroutineOptions the server uses when
// spawning coroutines.  Under Asio `interrupt_fd` and `name` are advisory (Asio
// uses cancellation rather than an interrupt fd), but they are kept so the same
// call sites compile under both backends.
struct SpawnOptions {
  std::string name;
  int interrupt_fd = -1;
  // Asio only: when false, the coroutine is tracked for the shutdown drain
  // count but is NOT force-cancelled by StopGracefully().  Client handlers use
  // this so that, during shutdown, they stay alive to service the orderly
  // disconnect of the server's in-process clients (statistics / channel
  // directory) instead of being killed out from under them (which would make
  // those clients auto-reconnect and recreate channels).  They exit on their
  // own when their peer closes the connection.  Ignored on the co backend.
  bool cancellable = true;
};

// AsyncRuntime wraps the backend's execution engine.  Under the co backend it
// holds a reference to a co::CoroutineScheduler (provided externally, exactly
// like the old Server constructor).  Under the Asio backend it holds a
// reference to a boost::asio::io_context (also provided externally, like
// asio_rpc's RpcServer::Run(io_context*)).
class AsyncRuntime {
public:
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  explicit AsyncRuntime(boost::asio::io_context &ioc)
      : ioc_(ioc), client_strand_(boost::asio::make_strand(ioc)) {}

  boost::asio::io_context &io_context() { return ioc_; }

  // Spawn a coroutine running `fn(ctx)` directly on the io_context.  In
  // multi-threaded mode (Run(n>1)) such a coroutine may run on any of the
  // io_context threads and several may run concurrently.
  void Spawn(std::function<void(Context)> fn, SpawnOptions opts = {}) {
    SpawnTracked(ioc_.get_executor(), std::move(fn), opts.cancellable,
                 std::move(opts.name));
  }

  // Spawn a coroutine on the client strand.  All coroutines spawned this way
  // are serialized with respect to one another (Asio guarantees a strand runs
  // at most one handler at a time), even when the io_context is run on multiple
  // threads.  This is how the single-threaded invariant of the client handlers
  // (and the listener that creates them) is preserved without any mutexes.
  void SpawnOnStrand(std::function<void(Context)> fn, SpawnOptions opts = {}) {
    SpawnTracked(client_strand_, std::move(fn), opts.cancellable,
                 std::move(opts.name));
  }

  // Run `fn` on the client strand.  This lets code running on the parallel
  // io_context (e.g. the bridge transmitter/receiver coroutines) mutate
  // strand-confined server state - the channels_ / bridged_publishers_ hash
  // maps - without a mutex: the work is serialized with the client handlers
  // and the discovery coroutines, which also run on the strand.  It is
  // fire-and-forget, so `fn` must capture everything it needs by value.
  void RunOnStrand(std::function<void()> fn) {
    boost::asio::post(client_strand_, std::move(fn));
  }

  // Spawn a coroutine on a fresh, private strand.  Different coroutines spawned
  // this way can run in parallel on different io_context threads (so bridge
  // load still spreads across cores), but each coroutine's own async operations
  // are serialized within its strand.  That serialization is required for the
  // WaitFdReady/WaitReadable handshake (see common/async): the descriptor
  // completion is bound to the coroutine's executor, so it must be a strand or
  // a multi-threaded reactor could deliver the readiness on another thread
  // before the coroutine registers its notifier wait, hanging it forever.
  void SpawnOnNewStrand(std::function<void(Context)> fn, SpawnOptions opts = {}) {
    SpawnTracked(boost::asio::make_strand(ioc_), std::move(fn),
                 opts.cancellable, std::move(opts.name));
  }

  // Run the io_context on `num_threads` threads (the calling thread plus
  // num_threads-1 helpers).  Blocks until the io_context runs out of work or is
  // stopped, exactly like the single-threaded ioc_.run().
  void Run(int num_threads = 1) {
    if (num_threads <= 1) {
      ioc_.run();
      return;
    }
    std::vector<std::thread> threads;
    threads.reserve(num_threads - 1);
    for (int i = 0; i < num_threads - 1; i++) {
      threads.emplace_back([this]() { ioc_.run(); });
    }
    ioc_.run();
    for (auto &t : threads) {
      t.join();
    }
  }

  // Forcibly stop the io_context, abandoning any suspended coroutines.  Prefer
  // StopGracefully() for a clean shutdown.
  void Stop() { ioc_.stop(); }

  // Gracefully stop: cancel every tracked coroutine so its pending async
  // operation completes with operation_aborted.  Each coroutine then unwinds
  // its `while(!shutting_down_)` loop and returns while the owning objects are
  // still alive, so no coroutine touches destroyed state (unlike a forced
  // Stop()).  Once all coroutines have returned the io_context has no work left
  // and Run() returns on its own.  Safe to call from another thread: the work
  // is posted onto the strand and runs on an io_context thread.  This is the
  // Asio analogue of the co backend's per-coroutine interrupt fd.
  void StopGracefully() {
    boost::asio::post(client_strand_, [this]() {
      stopping_ = true;
      EmitCancellations();
    });
  }

  // No-op under Asio; provided for source compatibility.
  void EnableAborts(bool) {}

  // Returns one (empty) string per live coroutine.  Only the size is
  // meaningful: it is the number of coroutines currently tracked for
  // cancellation, used by the listener to decide when it is the last coroutine
  // standing during shutdown.  MUST be called from a coroutine running on the
  // client strand (the listener is), since cancel_signals_ is strand-confined.
  std::vector<std::string> AllCoroutineStrings() const {
    std::vector<std::string> names;
    names.reserve(cancel_signals_.size());
    for (const auto &entry : cancel_signals_) {
      names.push_back(entry.name);
    }
    return names;
  }

private:
  // Emit cancellation to every live cancellable coroutine, then re-schedule
  // ourselves while any cancellable coroutine is still alive.  This MUST run on
  // the client strand.  Re-emitting is essential: boost::asio cancellation
  // signals are not sticky - emit() only cancels the operation a coroutine is
  // suspended on at that instant.  A coroutine that is between async operations
  // (e.g. publishing, or processing a message) when we first emit would
  // otherwise miss the cancellation entirely and then block forever in its next
  // wait.  Re-emitting on a short timer keeps catching each coroutine the next
  // time it suspends, playing the role the persistently-readable interrupt fd
  // plays on the co backend.  The loop self-terminates once no cancellable
  // coroutine remains (the non-cancellable client handlers exit on their own
  // when their peers disconnect, and the listener stops once it is the last
  // coroutine standing).
  void EmitCancellations() {
    bool any_cancellable = false;
    for (auto &entry : cancel_signals_) {
      if (!entry.cancellable) {
        continue;
      }
      any_cancellable = true;
      // Post the emit onto the coroutine's OWN executor rather than emitting it
      // here on client_strand_: a boost::asio::cancellation_signal is not
      // thread-safe, and a tracked coroutine may run on a different strand
      // (SpawnOnNewStrand) or, in multi-threaded mode, a different thread, so
      // emitting from client_strand_ would race the coroutine's manipulation of
      // its own slot.  The signal is held by shared_ptr so it stays valid even
      // if the coroutine finishes and its registry entry is erased before the
      // posted emit runs (emit on a finished operation is a harmless no-op).
      //
      // The wait primitives (common/async) deliberately do NOT let this signal
      // cancel their underlying timer/descriptor operation directly; instead
      // each wait installs a cancellation handler that wakes its sentinel timer
      // through a single guarded cancel().  That makes re-emitting safe: emit
      // only ever pokes that idempotent waker, never asio's per-operation timer
      // cancellation (which would corrupt the timer queue when it races the
      // wait's own natural completion - a use-after-free crash during shutdown).
      auto signal = entry.signal;
      boost::asio::post(entry.executor, [signal]() {
        signal->emit(boost::asio::cancellation_type::all);
      });
    }
    if (!any_cancellable) {
      return;
    }
    // Re-emit on a short timer: a cancellation handler is only armed while a
    // coroutine is actually suspended in a wait, so a coroutine that was between
    // waits when we emitted is caught the next time it suspends.
    reemit_timer_ = std::make_unique<boost::asio::steady_timer>(
        client_strand_, std::chrono::milliseconds(2));
    reemit_timer_->async_wait(boost::asio::bind_executor(
        client_strand_, [this](const boost::system::error_code &ec) {
          if (!ec) {
            EmitCancellations();
          }
        }));
  }

  // Register a per-coroutine cancellation_signal (on the strand, so the
  // registry needs no mutex even with multiple io_context threads), spawn the
  // coroutine bound to that signal's slot, and deregister on completion.  If a
  // graceful stop is already in progress the new coroutine is cancelled at
  // once so it does not keep the io_context alive.
  template <typename Executor>
  void SpawnTracked(const Executor &ex, std::function<void(Context)> fn,
                    bool cancellable, std::string name) {
    boost::asio::post(client_strand_, [this, ex, cancellable,
                                       name = std::move(name),
                                       fn = std::move(fn)]() mutable {
      auto it = cancel_signals_.emplace(cancel_signals_.end());
      it->signal = std::make_shared<boost::asio::cancellation_signal>();
      it->executor = ex;
      it->cancellable = cancellable;
      it->name = std::move(name);
      boost::asio::spawn(
          ex, [fn = std::move(fn)](Context yield) { fn(yield); },
          boost::asio::bind_cancellation_slot(
              it->signal->slot(),
              // Run the completion handler on the strand and erase the entry
              // immediately (no deferred post).  This closes the window in
              // which the re-emit timer could otherwise emit a cancellation on
              // a coroutine that has already finished but whose entry has not
              // yet been removed - the source of a use-after-free during
              // graceful shutdown.
              boost::asio::bind_executor(
                  client_strand_, [this, it](std::exception_ptr) {
                    cancel_signals_.erase(it);
                  })));
      // If a graceful stop is already underway, make sure the re-emit loop is
      // running so this freshly-spawned cancellable coroutine is cancelled too
      // (and does not keep the io_context alive).  EmitCancellations is
      // inflight-gated, so calling it here cannot double-cancel anything.
      if (stopping_ && cancellable) {
        EmitCancellations();
      }
    });
  }

  // One entry per live coroutine.  Only ever touched on client_strand_.
  // std::list keeps iterators stable so each coroutine can erase its own entry
  // on completion.
  struct CoroutineEntry {
    // Held by shared_ptr so a cancellation emit posted to the coroutine's
    // executor can keep the signal alive past the erase of this entry.
    std::shared_ptr<boost::asio::cancellation_signal> signal;
    // The coroutine's own executor (its strand, or the io_context executor).
    // Cancellation must be emitted here, never on client_strand_, because
    // cancellation_signal is not thread-safe.
    boost::asio::any_io_executor executor;
    bool cancellable = true;
    std::string name;
  };

  boost::asio::io_context &ioc_;
  boost::asio::strand<boost::asio::io_context::executor_type> client_strand_;
  std::list<CoroutineEntry> cancel_signals_;
  bool stopping_ = false;
  // Drives the periodic re-emit of cancellations during graceful shutdown.
  std::unique_ptr<boost::asio::steady_timer> reemit_timer_;

#else  // SUBSPACE_CORO_BACKEND_CO
  explicit AsyncRuntime(co::CoroutineScheduler &scheduler)
      : scheduler_(scheduler) {}

  co::CoroutineScheduler &scheduler() { return scheduler_; }

  void Spawn(std::function<void(Context)> fn, SpawnOptions opts = {}) {
    scheduler_.Spawn([fn = std::move(fn)](co::Coroutine *c) { fn(c); },
                     {.name = std::move(opts.name),
                      .interrupt_fd = opts.interrupt_fd});
  }

  // The co backend is single-threaded, so there is no strand: spawning "on the
  // strand" is identical to a normal spawn.
  void SpawnOnStrand(std::function<void(Context)> fn, SpawnOptions opts = {}) {
    Spawn(std::move(fn), std::move(opts));
  }

  // The co backend is single-threaded, so there is no concurrency to serialize
  // against: run `fn` inline.
  void RunOnStrand(std::function<void()> fn) { fn(); }

  // The co backend is single-threaded; a private strand is meaningless, so this
  // is just a normal spawn.
  void SpawnOnNewStrand(std::function<void(Context)> fn, SpawnOptions opts = {}) {
    Spawn(std::move(fn), std::move(opts));
  }

  // The co scheduler is single-threaded; num_threads is ignored.
  void Run(int /*num_threads*/ = 1) { scheduler_.Run(); }
  void Stop() { scheduler_.Stop(); }
  // The co backend drives graceful shutdown through per-coroutine interrupt fds
  // (see Server::Stop); there is no separate runtime-level graceful stop.
  void StopGracefully() { scheduler_.Stop(); }
  void EnableAborts(bool enabled) { scheduler_.EnableAborts(enabled); }
  std::vector<std::string> AllCoroutineStrings() const {
    return scheduler_.AllCoroutineStrings();
  }

private:
  co::CoroutineScheduler &scheduler_;
#endif
};

}  // namespace subspace::async

#endif  // _COMMON_ASYNC_RUNTIME_H
