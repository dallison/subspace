// Copyright 2026 General Motors LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "common/channel.h"
#include "proto/tunnel_handshake.pb.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

// Wire format for the tunnel-compatible bridge.  Every TCP connection
// carries one channel from one peer and is a sequence of packets, each a
// fixed header followed by a body.  The first packet is a handshake; after
// that each data packet holds a full message prefix area followed by the
// message payload.
namespace subspace::tunnel {

inline constexpr uint32_t kPacketMagic = 0xF0CACC1A;

// Packed little-endian: magic (4), packet length (4), type (1), send time in
// CLOCK_REALTIME nanoseconds (8), sequence number (8).  The packet length
// includes the header.
inline constexpr size_t kPacketHeaderSize = 25;

enum class PacketType : uint8_t {
  kHandshake = 1,
  kData = 2,
  kOperation = 3,
};

struct PacketHeader {
  uint32_t packet_length = 0;
  PacketType type = PacketType::kData;
  uint64_t send_time_ns = 0;
  // Data packets on a connection are numbered from 1.  Other packets use 0.
  uint64_t seq = 0;

  size_t BodySize() const { return packet_length - kPacketHeaderSize; }
};

// Largest body we send.  Peers track the remaining body length in a signed
// 32-bit integer, so anything larger breaks their framing.
inline constexpr uint64_t kMaxSendBodySize = INT32_MAX;

// Bodies of handshake and discovery packets are read into memory before
// they are parsed, so they are capped.  Data bodies are not.
inline constexpr size_t kMaxControlBodySize = 16 * 1024 * 1024;

// The channel a peer's discovery sender feeds.  Discovery messages arrive
// over an ordinary tunnel connection whose handshake names this channel, and
// each one is a serialized subspace.tunnel.Discovery.  Peers may also send
// an older encoding on /ipc/discovery/receive, which is an internal channel
// and so is ignored.
inline constexpr std::string_view kDiscoveryChannelName =
    "/ipc/discovery/proto/receive";

// Handshake values a peer's discovery receiver expects.
inline constexpr uint64_t kDiscoverySlotSize = 256;
inline constexpr uint32_t kDiscoveryNumSlots = 24;

// Used when a handshake leaves the slot size or slot count at zero.
inline constexpr uint64_t kDefaultSlotSize = 256;
inline constexpr uint32_t kDefaultNumSlots = 24;

// Peers retry failed and dropped connections at this interval.
inline constexpr int kReconnectIntervalSecs = 3;

// Builds a header for a body of `body_size` bytes.  Fails if the body is
// larger than kMaxSendBodySize.
absl::StatusOr<PacketHeader> MakePacketHeader(PacketType type,
                                              uint64_t body_size,
                                              uint64_t seq,
                                              uint64_t send_time_ns);

// `out` must hold kPacketHeaderSize bytes.
void EncodePacketHeader(const PacketHeader &header, std::byte *out);

// Checks the magic number, the packet type and that the length covers the
// header.
absl::StatusOr<PacketHeader>
DecodePacketHeader(absl::Span<const std::byte> data);

// The current CLOCK_REALTIME time in nanoseconds, for send_time_ns.
uint64_t RealtimeNowNs();

// Checks that a handshake describes a channel we can create: a topic name,
// a Subspace transport, and checksum and metadata sizes Subspace supports.
absl::Status ValidateHandshake(const Handshake &handshake);

// The checksum size a handshake describes.  An absent value means 4.
int32_t HandshakeChecksumSize(const SubspaceTransport &transport);

// Size of the prefix area that precedes the payload in every data packet.
int32_t WirePrefixSize(const SubspaceTransport &transport);

// True for channels the tunnel never advertises or carries: Subspace's own
// channels and the tunnel's discovery and IPC information channels.
bool IsInternalChannel(std::string_view name);

// Clears the flag bits outside kChecksummedMessageFlags in a message prefix
// and returns the ones that were set.  Peers number the other bits
// differently, so they mean nothing on the far side.  Subspace's checksum
// doesn't cover them, so this never invalidates it; a peer whose checksum
// covers every flag bit can't verify a message that had any of them set.  The
// prefix need not be aligned.
int64_t ClearUncheckedFlags(std::byte *prefix);

} // namespace subspace::tunnel
