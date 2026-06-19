// Copyright 2023-2026 David Allison
// Asio backend support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_WAIT_H
#define _COMMON_ASYNC_WAIT_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/async/context.h"
#include <chrono>

namespace subspace::async {

// FD-readiness and sleep primitives, expressed against the active backend's
// Context.  These replace the implicit co::self / co::Wait / co::Sleep calls so
// that the same source compiles under either backend.

// Wait until `fd` is readable.  A non-zero timeout returns
// DeadlineExceededError if the fd is not ready in time; a zero timeout waits
// forever.
absl::Status WaitReadable(Context ctx, int fd,
                          std::chrono::nanoseconds timeout =
                              std::chrono::nanoseconds(0));

// Wait until either `fd1` or `fd2` becomes readable.  Returns the raw fd that
// became ready.
absl::StatusOr<int> WaitEither(Context ctx, int fd1, int fd2);

// Suspend the current coroutine for `duration`.
void Sleep(Context ctx, std::chrono::nanoseconds duration);

// Convenience overload taking whole seconds.
inline void Sleep(Context ctx, int seconds) {
  Sleep(ctx, std::chrono::seconds(seconds));
}

}  // namespace subspace::async

#endif  // _COMMON_ASYNC_WAIT_H
