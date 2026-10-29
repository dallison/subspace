// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/tunnel_discovery_message.h"

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include <utility>

namespace subspace::tunnel {

namespace {

// The smallest encoding of a DiscoveryTopicInfo: three empty strings, an
// int32 and a bool.
constexpr size_t kMinTopicInfoSize = 4 + 4 + 4 + 4 + 1;

class Writer {
public:
  void U32(uint32_t value) {
    for (int i = 0; i < 4; i++) {
      out_.push_back(static_cast<std::byte>(value >> (8 * i)));
    }
  }
  void I32(int32_t value) { U32(static_cast<uint32_t>(value)); }
  void Bool(bool value) { out_.push_back(static_cast<std::byte>(value)); }
  void String(const std::string &s) {
    U32(static_cast<uint32_t>(s.size()));
    const auto *p = reinterpret_cast<const std::byte *>(s.data());
    out_.insert(out_.end(), p, p + s.size());
  }
  void Topics(const std::vector<DiscoveryTopicInfo> &topics) {
    U32(static_cast<uint32_t>(topics.size()));
    for (const DiscoveryTopicInfo &info : topics) {
      String(info.topic);
      String(info.datatype);
      String(info.mux);
      I32(info.vchan_id);
      Bool(info.is_reliable);
    }
  }
  std::vector<std::byte> Take() { return std::move(out_); }

private:
  std::vector<std::byte> out_;
};

class Reader {
public:
  explicit Reader(absl::Span<const std::byte> data) : data_(data) {}

  absl::StatusOr<uint32_t> U32(const char *what) {
    if (Remaining() < 4) {
      return Truncated(what);
    }
    uint32_t value = 0;
    for (int i = 0; i < 4; i++) {
      value |= static_cast<uint32_t>(data_[pos_ + i]) << (8 * i);
    }
    pos_ += 4;
    return value;
  }

  absl::StatusOr<int32_t> I32(const char *what) {
    absl::StatusOr<uint32_t> value = U32(what);
    if (!value.ok()) {
      return value.status();
    }
    return static_cast<int32_t>(*value);
  }

  absl::StatusOr<bool> Bool(const char *what) {
    if (Remaining() < 1) {
      return Truncated(what);
    }
    return data_[pos_++] != std::byte{0};
  }

  absl::Status String(const char *what, std::string *out) {
    absl::StatusOr<uint32_t> length = U32(what);
    if (!length.ok()) {
      return length.status();
    }
    if (*length > Remaining()) {
      return Truncated(what);
    }
    out->assign(reinterpret_cast<const char *>(data_.data() + pos_), *length);
    pos_ += *length;
    return absl::OkStatus();
  }

  absl::Status Topics(const char *what,
                      std::vector<DiscoveryTopicInfo> *topics) {
    absl::StatusOr<uint32_t> count = U32(what);
    if (!count.ok()) {
      return count.status();
    }
    // Checking the count against the bytes left stops a corrupt count from
    // reserving a huge vector.
    if (*count > Remaining() / kMinTopicInfoSize) {
      return Truncated(what);
    }
    topics->resize(*count);
    for (DiscoveryTopicInfo &info : *topics) {
      if (absl::Status s = String("topic", &info.topic); !s.ok()) {
        return s;
      }
      if (absl::Status s = String("datatype", &info.datatype); !s.ok()) {
        return s;
      }
      if (absl::Status s = String("mux", &info.mux); !s.ok()) {
        return s;
      }
      absl::StatusOr<int32_t> vchan_id = I32("vchan_id");
      if (!vchan_id.ok()) {
        return vchan_id.status();
      }
      info.vchan_id = *vchan_id;
      absl::StatusOr<bool> is_reliable = Bool("is_reliable");
      if (!is_reliable.ok()) {
        return is_reliable.status();
      }
      info.is_reliable = *is_reliable;
    }
    return absl::OkStatus();
  }

private:
  size_t Remaining() const { return data_.size() - pos_; }

  absl::Status Truncated(const char *what) const {
    return absl::InvalidArgumentError(absl::StrFormat(
        "discovery message truncated reading %s at offset %d of %d", what,
        pos_, data_.size()));
  }

  absl::Span<const std::byte> data_;
  size_t pos_ = 0;
};

} // namespace

std::vector<std::byte> EncodeDiscoveryMessage(const DiscoveryMessage &msg) {
  Writer writer;
  writer.String(msg.device_name);
  writer.Topics(msg.published_topics);
  writer.Topics(msg.subscribed_topics);
  return writer.Take();
}

absl::StatusOr<DiscoveryMessage>
DecodeDiscoveryMessage(absl::Span<const std::byte> data) {
  Reader reader(data);
  DiscoveryMessage msg;
  if (absl::Status s = reader.String("device_name", &msg.device_name);
      !s.ok()) {
    return s;
  }
  if (absl::Status s =
          reader.Topics("published_topics", &msg.published_topics);
      !s.ok()) {
    return s;
  }
  if (absl::Status s =
          reader.Topics("subscribed_topics", &msg.subscribed_topics);
      !s.ok()) {
    return s;
  }
  return msg;
}

} // namespace subspace::tunnel
