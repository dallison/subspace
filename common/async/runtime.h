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
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#endif

namespace subspace::async {

// Options that mirror the subset of co::CoroutineOptions the server uses when
// spawning coroutines.  Under Asio `interrupt_fd` and `name` are advisory (Asio
// uses cancellation rather than an interrupt fd), but they are kept so the same
// call sites compile under both backends.
struct SpawnOptions {
  std::string name;
  int interrupt_fd = -1;
};

// AsyncRuntime wraps the backend's execution engine.  Under the co backend it
// holds a reference to a co::CoroutineScheduler (provided externally, exactly
// like the old Server constructor).  Under the Asio backend it holds a
// reference to a boost::asio::io_context (also provided externally, like
// asio_rpc's RpcServer::Run(io_context*)).
class AsyncRuntime {
public:
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  explicit AsyncRuntime(boost::asio::io_context &ioc) : ioc_(ioc) {}

  boost::asio::io_context &io_context() { return ioc_; }

  // Spawn a coroutine running `fn(ctx)`.  The Asio implementation detaches the
  // coroutine onto the io_context; the embedder owns running it.
  void Spawn(std::function<void(Context)> fn, SpawnOptions = {}) {
    boost::asio::spawn(
        ioc_, [fn = std::move(fn)](Context yield) { fn(yield); },
        boost::asio::detached);
  }

  // Run/Stop drive the io_context.  When the embedder owns the io_context it
  // can run it directly instead.
  void Run() { ioc_.run(); }
  void Stop() { ioc_.stop(); }

  // No-ops / empty results under Asio; provided for source compatibility.
  void EnableAborts(bool) {}
  std::vector<std::string> AllCoroutineStrings() const { return {}; }

private:
  boost::asio::io_context &ioc_;

#else  // SUBSPACE_CORO_BACKEND_CO
  explicit AsyncRuntime(co::CoroutineScheduler &scheduler)
      : scheduler_(scheduler) {}

  co::CoroutineScheduler &scheduler() { return scheduler_; }

  void Spawn(std::function<void(Context)> fn, SpawnOptions opts = {}) {
    scheduler_.Spawn([fn = std::move(fn)](co::Coroutine *c) { fn(c); },
                     {.name = std::move(opts.name),
                      .interrupt_fd = opts.interrupt_fd});
  }

  void Run() { scheduler_.Run(); }
  void Stop() { scheduler_.Stop(); }
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
