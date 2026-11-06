// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xSERVERSTATIC_CONFIG_H
#define _xSERVERSTATIC_CONFIG_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/subspace.pb.h"
#include <string>

namespace subspace {

// Prefix of the channels the server creates for itself.  A static channel
// config can't use it.
inline constexpr char kServerChannelPrefix[] = "/subspace/";

// Parses a StaticChannelConfig in protobuf text format and validates it.
absl::StatusOr<StaticChannelConfig>
ParseStaticChannelConfig(const std::string &text);

// Reads, parses and validates a StaticChannelConfig text format file.
absl::StatusOr<StaticChannelConfig>
ReadStaticChannelConfig(const std::string &filename);

absl::Status ValidateStaticChannelConfig(const StaticChannelConfig &config);

} // namespace subspace

#endif // _xSERVERSTATIC_CONFIG_H
