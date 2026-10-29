// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/tunnel_protocol.h"

#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include <cstddef>
#include <cstring>
#include <ctime>
#include <limits>

namespace subspace::tunnel {

namespace {

void PutLE(std::byte *out, uint64_t value, size_t size) {
  for (size_t i = 0; i < size; i++) {
    out[i] = static_cast<std::byte>(value >> (8 * i));
  }
}

uint64_t GetLE(const std::byte *in, size_t size) {
  uint64_t value = 0;
  for (size_t i = 0; i < size; i++) {
    value |= static_cast<uint64_t>(in[i]) << (8 * i);
  }
  return value;
}

} // namespace

absl::StatusOr<PacketHeader> MakePacketHeader(PacketType type,
                                              uint64_t body_size,
                                              uint64_t seq,
                                              uint64_t send_time_ns) {
  if (body_size > kMaxSendBodySize) {
    return absl::OutOfRangeError(absl::StrFormat(
        "tunnel packet body of %d bytes exceeds the limit of %d bytes",
        body_size, kMaxSendBodySize));
  }
  PacketHeader header;
  header.packet_length = static_cast<uint32_t>(body_size + kPacketHeaderSize);
  header.type = type;
  header.send_time_ns = send_time_ns;
  header.seq = seq;
  return header;
}

void EncodePacketHeader(const PacketHeader &header, std::byte *out) {
  PutLE(out, kPacketMagic, 4);
  PutLE(out + 4, header.packet_length, 4);
  PutLE(out + 8, static_cast<uint8_t>(header.type), 1);
  PutLE(out + 9, header.send_time_ns, 8);
  PutLE(out + 17, header.seq, 8);
}

absl::StatusOr<PacketHeader>
DecodePacketHeader(absl::Span<const std::byte> data) {
  if (data.size() < kPacketHeaderSize) {
    return absl::InvalidArgumentError(
        absl::StrFormat("tunnel packet header needs %d bytes, got %d",
                        kPacketHeaderSize, data.size()));
  }
  uint32_t magic = static_cast<uint32_t>(GetLE(data.data(), 4));
  if (magic != kPacketMagic) {
    return absl::InvalidArgumentError(
        absl::StrFormat("bad tunnel packet magic 0x%08x", magic));
  }
  PacketHeader header;
  header.packet_length = static_cast<uint32_t>(GetLE(data.data() + 4, 4));
  if (header.packet_length < kPacketHeaderSize) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "tunnel packet length %d is shorter than its header",
        header.packet_length));
  }
  uint8_t type = static_cast<uint8_t>(GetLE(data.data() + 8, 1));
  if (type < static_cast<uint8_t>(PacketType::kHandshake) ||
      type > static_cast<uint8_t>(PacketType::kOperation)) {
    return absl::InvalidArgumentError(
        absl::StrFormat("unknown tunnel packet type %d", type));
  }
  header.type = static_cast<PacketType>(type);
  header.send_time_ns = GetLE(data.data() + 9, 8);
  header.seq = GetLE(data.data() + 17, 8);
  return header;
}

uint64_t RealtimeNowNs() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
         static_cast<uint64_t>(ts.tv_nsec);
}

int32_t HandshakeChecksumSize(const SubspaceTransport &transport) {
  if (!transport.has_checksum_size() || transport.checksum_size() <= 0) {
    return 4;
  }
  return transport.checksum_size();
}

int32_t WirePrefixSize(const SubspaceTransport &transport) {
  return Channel::ComputePrefixSize(HandshakeChecksumSize(transport),
                                    transport.metadata_size());
}

absl::Status ValidateHandshake(const Handshake &handshake) {
  if (handshake.topic_name().empty()) {
    return absl::InvalidArgumentError("tunnel handshake has no topic name");
  }
  if (!handshake.has_subspace()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("tunnel handshake for %s has no Subspace transport",
                        handshake.topic_name()));
  }
  const SubspaceTransport &transport = handshake.subspace();
  if (transport.has_checksum_size() &&
      transport.checksum_size() > kMaxChecksumSize) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "tunnel handshake for %s has checksum_size %d; the maximum is %d",
        handshake.topic_name(), transport.checksum_size(), kMaxChecksumSize));
  }
  if (transport.metadata_size() < 0 ||
      transport.metadata_size() > kMaxMetadataSize) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "tunnel handshake for %s has metadata_size %d; it must be between 0 "
        "and %d",
        handshake.topic_name(), transport.metadata_size(), kMaxMetadataSize));
  }
  if (transport.slot_size() >
      static_cast<uint64_t>(std::numeric_limits<SlotSizeType>::max())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "tunnel handshake for %s has slot_size %d, larger than this build "
        "supports",
        handshake.topic_name(), transport.slot_size()));
  }
  if (transport.num_slots() >
      static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("tunnel handshake for %s has num_slots %d",
                        handshake.topic_name(), transport.num_slots()));
  }
  return absl::OkStatus();
}

bool IsInternalChannel(std::string_view name) {
  return absl::StartsWith(name, "/subspace/") ||
         absl::StrContains(name, "/subspace/channel_directory") ||
         absl::StrContains(name, "/framework/ipc_information") ||
         absl::StrContains(name, "/ipc/discovery/");
}

int64_t ClearUncheckedFlags(std::byte *prefix) {
  int64_t flags;
  std::byte *field = prefix + offsetof(MessagePrefix, flags);
  memcpy(&flags, field, sizeof(flags));
  const int64_t cleared = flags & ~kChecksummedMessageFlags;
  flags &= kChecksummedMessageFlags;
  memcpy(field, &flags, sizeof(flags));
  return cleared;
}

} // namespace subspace::tunnel
