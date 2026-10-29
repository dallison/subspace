// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/tunnel_config.h"

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/text_format.h"
#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>
#include <vector>

namespace subspace {

namespace {

class ErrorList : public google::protobuf::io::ErrorCollector {
public:
  explicit ErrorList(std::string_view source) : source_(source) {}

  void RecordError(int line, google::protobuf::io::ColumnNumber column,
                   absl::string_view message) override {
    errors_.push_back(absl::StrFormat("%s:%d:%d: %s", source_, line + 1,
                                      column + 1, message));
  }

  const std::vector<std::string> &Errors() const { return errors_; }

private:
  std::string source_;
  std::vector<std::string> errors_;
};

bool ValidPort(int32_t port) { return port > 0 && port <= 65535; }

} // namespace

absl::StatusOr<TunnelConfig> ParseTunnelConfig(std::string_view text,
                                               std::string_view source) {
  ErrorList errors(source);
  google::protobuf::TextFormat::Parser parser;
  parser.RecordErrorsTo(&errors);
  TunnelConfig config;
  if (!parser.ParseFromString(text, &config)) {
    if (errors.Errors().empty()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("%s: invalid tunnel configuration", source));
    }
    return absl::InvalidArgumentError(absl::StrJoin(errors.Errors(), "\n"));
  }
  return config;
}

absl::StatusOr<TunnelConfig> LoadTunnelConfig(const std::string &path) {
  std::ifstream in(path);
  if (!in) {
    return absl::NotFoundError(absl::StrFormat(
        "can't open tunnel configuration %s: %s", path, strerror(errno)));
  }
  std::stringstream text;
  text << in.rdbuf();
  if (in.bad()) {
    return absl::InternalError(
        absl::StrFormat("can't read tunnel configuration %s", path));
  }
  return ParseTunnelConfig(text.str(), path);
}

std::string TunnelDeviceName(const TunnelConfig &config,
                             const std::string &machine_name,
                             const std::string &hostname) {
  if (!config.device_name().empty()) {
    return config.device_name();
  }
  if (!machine_name.empty()) {
    return machine_name;
  }
  return hostname;
}

absl::Status ValidateTunnelConfig(const TunnelConfig &config,
                                  std::string_view device_name) {
  std::vector<std::string> errors;
  if (device_name.empty()) {
    errors.push_back("the device name is empty");
  }
  if (config.listen_port() == 0) {
    errors.push_back("listen_port is required");
  } else if (!ValidPort(config.listen_port())) {
    errors.push_back(
        absl::StrFormat("listen_port %d is not a valid TCP port",
                        config.listen_port()));
  }
  if (config.has_discovery_refresh_secs() &&
      config.discovery_refresh_secs() < 0) {
    errors.push_back(
        absl::StrFormat("discovery_refresh_secs %d is negative",
                        config.discovery_refresh_secs()));
  }

  absl::flat_hash_set<std::string> names;
  for (int i = 0; i < config.peers_size(); i++) {
    const TunnelPeer &peer = config.peers(i);
    std::string label =
        peer.name().empty() ? absl::StrFormat("peers[%d]", i)
                            : absl::StrFormat("peer %s", peer.name());
    if (peer.name().empty()) {
      errors.push_back(absl::StrFormat("%s has no name", label));
    } else if (peer.name() == device_name) {
      errors.push_back(absl::StrFormat(
          "%s has the same name as this device", label));
    } else if (!names.insert(peer.name()).second) {
      errors.push_back(absl::StrFormat("%s is listed more than once", label));
    }
    if (peer.tcp_addr().empty()) {
      errors.push_back(absl::StrFormat("%s has no tcp_addr", label));
    }
    if (!ValidPort(peer.tcp_port())) {
      errors.push_back(absl::StrFormat("%s has tcp_port %d, which is not a "
                                       "valid TCP port",
                                       label, peer.tcp_port()));
    }
  }

  if (errors.empty()) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "invalid tunnel configuration: %s", absl::StrJoin(errors, "; ")));
}

int TunnelDiscoveryRefreshSecs(const TunnelConfig &config) {
  if (!config.has_discovery_refresh_secs()) {
    return kDefaultTunnelDiscoveryRefreshSecs;
  }
  return config.discovery_refresh_secs();
}

} // namespace subspace
