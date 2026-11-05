// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _COMMON_ASYNC_MTLS_H
#define _COMMON_ASYNC_MTLS_H

#include "absl/status/statusor.h"
#include <memory>
#include <string>

// Build with --//:mtls (Bazel) or -DSUBSPACE_ENABLE_MTLS=ON (CMake) to turn
// this on.  A server built without it rejects credential paths at startup.
#ifndef SUBSPACE_ENABLE_MTLS
#define SUBSPACE_ENABLE_MTLS 0
#endif

namespace subspace::async {

// Trusts every certificate the configured CA signed.  There is no hostname
// check: bridge peers are discovered by address, and tunnel peers are named
// in the tunnel configuration, neither of which has to appear in the
// certificate.
class MtlsContext {
public:
  static absl::StatusOr<std::shared_ptr<MtlsContext>>
  Load(const std::string &cert_file, const std::string &key_file,
       const std::string &ca_file);

  ~MtlsContext();

  MtlsContext(const MtlsContext &) = delete;
  MtlsContext &operator=(const MtlsContext &) = delete;

private:
  struct State;
  MtlsContext();

  std::unique_ptr<State> state_;

  friend class ByteStream;
};

inline bool MtlsAvailable() {
#if SUBSPACE_ENABLE_MTLS
  return true;
#else
  return false;
#endif
}

} // namespace subspace::async

#endif // _COMMON_ASYNC_MTLS_H
