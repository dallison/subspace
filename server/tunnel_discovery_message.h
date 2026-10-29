// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// The discovery message exchanged by tunnel peers.  It is serialized the way
// ROS 1 serializes messages: a string is a 32-bit length followed by its
// bytes, an array is a 32-bit element count followed by its elements, an
// int32 is 4 bytes and a bool is 1 byte, all little-endian with no padding.
namespace subspace::tunnel {

struct DiscoveryTopicInfo {
  std::string topic;
  std::string datatype;
  // The remaining fields are only meaningful for published topics.  A
  // published topic that isn't virtual has an empty mux and a vchan_id of -1.
  std::string mux;
  int32_t vchan_id = 0;
  bool is_reliable = false;

  bool operator==(const DiscoveryTopicInfo &other) const {
    return topic == other.topic && datatype == other.datatype &&
           mux == other.mux && vchan_id == other.vchan_id &&
           is_reliable == other.is_reliable;
  }
};

struct DiscoveryMessage {
  std::string device_name;
  std::vector<DiscoveryTopicInfo> published_topics;
  std::vector<DiscoveryTopicInfo> subscribed_topics;
};

std::vector<std::byte> EncodeDiscoveryMessage(const DiscoveryMessage &msg);

// Fails if the data ends before the message does.  Bytes after the end of
// the message are ignored.
absl::StatusOr<DiscoveryMessage>
DecodeDiscoveryMessage(absl::Span<const std::byte> data);

} // namespace subspace::tunnel
