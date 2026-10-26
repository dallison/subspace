// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_COMPLETION_H_
#define CLIENT_ASIO_COMPLETION_H_

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>

#include <utility>

namespace subspace::asio_client_internal {

using Executor = boost::asio::any_io_executor;
using WorkGuard = boost::asio::executor_work_guard<Executor>;

inline WorkGuard MakeWorkGuard(const Executor &executor) {
  return boost::asio::make_work_guard(executor);
}

template <typename Handler, typename Result>
void Complete(const Executor &fallback_executor, Handler handler,
              Result result) {
  auto executor =
      boost::asio::get_associated_executor(handler, fallback_executor);
  boost::asio::post(executor, [handler = std::move(handler),
                               result = std::move(result)]() mutable {
    std::move(handler)(std::move(result));
  });
}

} // namespace subspace::asio_client_internal

#endif // CLIENT_ASIO_COMPLETION_H_
