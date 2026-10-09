// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <cstdint>

namespace subspace {
namespace asil {

// Every ASIL client operation returns one of these.  The client throws no
// exceptions.
enum class Error : int32_t {
  kOk = 0,
  // An argument is out of range or a required pointer is null.
  kInvalidArgument,
  // The object hasn't been set up, or has been closed.
  kNotInitialized,
  // The object is already set up.
  kAlreadyInitialized,
  // Talking to the server failed.  The connection is unusable.
  kConnectionFailed,
  // The server sent something the client can't use.
  kProtocolError,
  // The server refused the request.  Client::LastServerError() says why.
  kServerRejected,
  // A shared memory system call failed.  errno is preserved.
  kSharedMemoryError,
  // The channel's shared memory doesn't have the expected layout.
  kLayoutMismatch,
  // The channel uses a feature that the ASIL client doesn't provide.
  kUnsupported,
  // No slot could be claimed.
  kNoSlot,
  // The message is bigger than the slot.
  kMessageTooLarge,
  // The message checksum is wrong.
  kChecksumMismatch,
  // The wait timed out.
  kTimeout,
  // A fixed capacity, such as the channel name length, was exceeded.
  kCapacityExceeded,
  // The subscriber already holds max_active_messages messages.
  kActiveMessageLimit,
  // The channel has no message buffers to map.  The server creates them for
  // a static channel; otherwise a publisher must open first.
  kNoBuffers,
  // The message is in a buffer that a standard publisher created after this
  // subscriber opened, by resizing the channel.  The message is skipped.
  kBufferNotMapped,
};

// Returns a static description of the error.
const char *ErrorString(Error error);

} // namespace asil
} // namespace subspace
