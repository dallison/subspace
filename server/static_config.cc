// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/static_config.h"

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "common/channel.h"
#include "google/protobuf/text_format.h"
#include <fstream>
#include <sstream>

namespace subspace {

namespace {

absl::Status ConfigError(const std::string &message) {
  return absl::InvalidArgumentError("Static channel config: " + message);
}

absl::Status ValidateName(const std::string &name,
                          absl::flat_hash_set<std::string> &names) {
  if (name.empty()) {
    return ConfigError("every channel and multiplexer needs a name");
  }
  if (absl::StartsWith(name, kServerChannelPrefix)) {
    return ConfigError(absl::StrFormat(
        "%s uses the server's reserved prefix %s", name, kServerChannelPrefix));
  }
  if (!names.insert(name).second) {
    return ConfigError(absl::StrFormat("%s is configured more than once", name));
  }
  return absl::OkStatus();
}

absl::Status ValidateLayout(const std::string &name, int32_t slot_size,
                            int32_t num_slots, int32_t checksum_size,
                            int32_t metadata_size) {
  if (slot_size <= 0 || num_slots <= 0) {
    return ConfigError(absl::StrFormat(
        "%s needs a slot_size and num_slots greater than 0, not %d and %d",
        name, slot_size, num_slots));
  }
  if (checksum_size < 0 || checksum_size > kMaxChecksumSize) {
    return ConfigError(
        absl::StrFormat("%s has checksum_size %d; it must be from 0 to %d",
                        name, checksum_size, kMaxChecksumSize));
  }
  if (metadata_size < 0 || metadata_size > kMaxMetadataSize) {
    return ConfigError(
        absl::StrFormat("%s has metadata_size %d; it must be from 0 to %d",
                        name, metadata_size, kMaxMetadataSize));
  }
  return absl::OkStatus();
}

} // namespace

absl::Status ValidateStaticChannelConfig(const StaticChannelConfig &config) {
  absl::flat_hash_set<std::string> names;
  // Virtual channel ids in use on each multiplexer.
  absl::flat_hash_map<std::string, absl::flat_hash_set<int32_t>> mux_vchan_ids;

  for (const StaticMultiplexer &mux : config.multiplexers()) {
    if (absl::Status s = ValidateName(mux.name(), names); !s.ok()) {
      return s;
    }
    if (absl::Status s =
            ValidateLayout(mux.name(), mux.slot_size(), mux.num_slots(),
                           mux.checksum_size(), mux.metadata_size());
        !s.ok()) {
      return s;
    }
    mux_vchan_ids[mux.name()];
  }

  for (const StaticChannel &channel : config.channels()) {
    if (absl::Status s = ValidateName(channel.name(), names); !s.ok()) {
      return s;
    }
    if (channel.mux().empty()) {
      if (channel.vchan_id() != 0) {
        return ConfigError(absl::StrFormat(
            "%s has a vchan_id but no mux", channel.name()));
      }
      if (absl::Status s = ValidateLayout(
              channel.name(), channel.slot_size(), channel.num_slots(),
              channel.checksum_size(), channel.metadata_size());
          !s.ok()) {
        return s;
      }
      continue;
    }
    auto ids = mux_vchan_ids.find(channel.mux());
    if (ids == mux_vchan_ids.end()) {
      return ConfigError(
          absl::StrFormat("%s uses mux %s, which isn't a configured "
                          "multiplexer",
                          channel.name(), channel.mux()));
    }
    if (channel.slot_size() != 0 || channel.num_slots() != 0 ||
        !channel.type().empty() || channel.checksum_size() != 0 ||
        channel.metadata_size() != 0 || channel.use_split_buffers()) {
      return ConfigError(absl::StrFormat(
          "%s is on mux %s and takes its slot_size, num_slots, type, "
          "checksum_size, metadata_size and use_split_buffers from it; leave "
          "them unset",
          channel.name(), channel.mux()));
    }
    if (channel.vchan_id() < 0 || channel.vchan_id() >= kMaxVchanId) {
      return ConfigError(
          absl::StrFormat("%s has vchan_id %d; it must be from 0 to %d",
                          channel.name(), channel.vchan_id(), kMaxVchanId - 1));
    }
    if (!ids->second.insert(channel.vchan_id()).second) {
      return ConfigError(absl::StrFormat(
          "%s reuses vchan_id %d on mux %s", channel.name(),
          channel.vchan_id(), channel.mux()));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<StaticChannelConfig>
ParseStaticChannelConfig(const std::string &text) {
  StaticChannelConfig config;
  if (!google::protobuf::TextFormat::ParseFromString(text, &config)) {
    return ConfigError("invalid protobuf text format");
  }
  if (absl::Status s = ValidateStaticChannelConfig(config); !s.ok()) {
    return s;
  }
  return config;
}

absl::StatusOr<StaticChannelConfig>
ReadStaticChannelConfig(const std::string &filename) {
  std::ifstream in(filename);
  if (!in) {
    return absl::NotFoundError(absl::StrFormat(
        "Static channel config: can't open %s", filename));
  }
  std::stringstream text;
  text << in.rdbuf();
  absl::StatusOr<StaticChannelConfig> config =
      ParseStaticChannelConfig(text.str());
  if (!config.ok()) {
    return absl::Status(config.status().code(),
                        absl::StrFormat("%s: %s", filename,
                                        config.status().message()));
  }
  return config;
}

} // namespace subspace
