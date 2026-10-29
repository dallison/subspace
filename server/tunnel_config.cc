// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/tunnel_config.h"

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "common/channel.h"
#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/text_format.h"
#include "server/tunnel_protocol.h"
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

// Checks what send and receive routes have in common and returns the route's
// name for further errors.
std::string
ValidateRoute(std::string_view field, std::string_view kind, int index,
              const std::string &topic,
              const google::protobuf::RepeatedPtrField<std::string> &peers,
              const absl::flat_hash_set<std::string> &peer_names,
              absl::flat_hash_set<std::string> &topics,
              std::vector<std::string> &errors) {
  std::string label = topic.empty()
                          ? absl::StrFormat("%s[%d]", field, index)
                          : absl::StrFormat("%s %s", kind, topic);
  if (topic.empty()) {
    errors.push_back(absl::StrFormat("%s has no topic", label));
  } else if (tunnel::IsInternalChannel(topic)) {
    errors.push_back(absl::StrFormat("%s is an internal channel", label));
  } else if (!topics.insert(topic).second) {
    errors.push_back(absl::StrFormat("%s is listed more than once", label));
  }
  if (peers.empty()) {
    errors.push_back(absl::StrFormat("%s names no peers", label));
  }
  absl::flat_hash_set<std::string> seen;
  for (const std::string &peer : peers) {
    if (!peer_names.contains(peer)) {
      errors.push_back(
          absl::StrFormat("%s names unknown peer \"%s\"", label, peer));
    } else if (!seen.insert(peer).second) {
      errors.push_back(
          absl::StrFormat("%s names peer %s more than once", label, peer));
    }
  }
  return label;
}

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

  absl::flat_hash_set<std::string> send_topics;
  for (int i = 0; i < config.send_routes_size(); i++) {
    const TunnelSendRoute &route = config.send_routes(i);
    ValidateRoute("send_routes", "send route", i, route.topic(), route.peers(),
                  names, send_topics, errors);
  }
  absl::flat_hash_set<std::string> receive_topics;
  for (int i = 0; i < config.receive_routes_size(); i++) {
    const TunnelReceiveRoute &route = config.receive_routes(i);
    std::string label =
        ValidateRoute("receive_routes", "receive route", i, route.topic(),
                      route.peers(), names, receive_topics, errors);
    if (!route.has_vchan_id()) {
      continue;
    }
    if (route.mux().empty()) {
      errors.push_back(absl::StrFormat("%s has a vchan_id but no mux", label));
    } else if (route.vchan_id() < 0 || route.vchan_id() >= kMaxVchanId) {
      errors.push_back(absl::StrFormat(
          "%s has vchan_id %d, which is not between 0 and %d", label,
          route.vchan_id(), kMaxVchanId - 1));
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
