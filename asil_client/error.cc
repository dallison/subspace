// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "asil_client/error.h"

namespace subspace {
namespace asil {

const char *ErrorString(Error error) {
  switch (error) {
  case Error::kOk:
    return "ok";
  case Error::kInvalidArgument:
    return "invalid argument";
  case Error::kNotInitialized:
    return "not initialized";
  case Error::kAlreadyInitialized:
    return "already initialized";
  case Error::kConnectionFailed:
    return "connection to server failed";
  case Error::kProtocolError:
    return "protocol error";
  case Error::kServerRejected:
    return "server rejected request";
  case Error::kSharedMemoryError:
    return "shared memory error";
  case Error::kLayoutMismatch:
    return "shared memory layout mismatch";
  case Error::kUnsupported:
    return "unsupported channel feature";
  case Error::kNoSlot:
    return "no slot available";
  case Error::kMessageTooLarge:
    return "message too large";
  case Error::kChecksumMismatch:
    return "checksum mismatch";
  case Error::kTimeout:
    return "timeout";
  case Error::kCapacityExceeded:
    return "capacity exceeded";
  }
  return "unknown error";
}

} // namespace asil
} // namespace subspace
