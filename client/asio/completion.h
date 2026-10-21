// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_COMPLETION_H_
#define CLIENT_ASIO_COMPLETION_H_

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <utility>

namespace subspace::asio_client_internal {

using WorkGuard =
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

inline WorkGuard MakeWorkGuard(boost::asio::io_context &io) {
  return boost::asio::make_work_guard(io);
}

template <typename Handler, typename Result>
void Complete(boost::asio::io_context &io, Handler handler, Result result) {
  auto executor =
      boost::asio::get_associated_executor(handler, io.get_executor());
  boost::asio::post(executor, [handler = std::move(handler),
                               result = std::move(result)]() mutable {
    std::move(handler)(std::move(result));
  });
}

} // namespace subspace::asio_client_internal

#endif // CLIENT_ASIO_COMPLETION_H_
