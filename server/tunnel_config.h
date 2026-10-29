// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/tunnel_config.pb.h"
#include <string>
#include <string_view>

namespace subspace {

inline constexpr int kDefaultTunnelDiscoveryRefreshSecs = 5;

// Parses a tunnel configuration in protobuf text format.  Unknown fields are
// errors.  Error messages name `source` and give the line and column.
absl::StatusOr<TunnelConfig> ParseTunnelConfig(std::string_view text,
                                               std::string_view source);

// Reads and parses the tunnel configuration file at `path`.
absl::StatusOr<TunnelConfig> LoadTunnelConfig(const std::string &path);

// The device name this server announces to tunnel peers: the configured
// name, else the machine name, else the host name.
std::string TunnelDeviceName(const TunnelConfig &config,
                             const std::string &machine_name,
                             const std::string &hostname);

// Checks the parts of the configuration that the parser can't.  Every
// problem found is reported in the returned status.
absl::Status ValidateTunnelConfig(const TunnelConfig &config,
                                  std::string_view device_name);

// Seconds between periodic discovery resends.  Zero disables them.
int TunnelDiscoveryRefreshSecs(const TunnelConfig &config);

} // namespace subspace
