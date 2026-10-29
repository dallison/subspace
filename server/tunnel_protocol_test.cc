// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/tunnel_discovery_message.h"
#include "server/tunnel_protocol.h"

#include "absl/status/status_matchers.h"
#include "client/checksum.h"
#include "common/channel.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <string>
#include <vector>

namespace subspace::tunnel {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

std::vector<std::byte> Bytes(std::initializer_list<int> values) {
  std::vector<std::byte> out;
  for (int v : values) {
    out.push_back(static_cast<std::byte>(v));
  }
  return out;
}

void Append(std::vector<std::byte> &out, std::string_view s) {
  for (char c : s) {
    out.push_back(static_cast<std::byte>(c));
  }
}

TEST(TunnelPacketHeaderTest, EncodesLittleEndianPackedLayout) {
  absl::StatusOr<PacketHeader> header = MakePacketHeader(
      PacketType::kData, /*body_size=*/100, /*seq=*/1, 0x0102030405060708);
  ASSERT_THAT(header, IsOk());
  std::array<std::byte, kPacketHeaderSize> wire;
  EncodePacketHeader(*header, wire.data());
  std::vector<std::byte> expected =
      Bytes({0x1A, 0xCC, 0xCA, 0xF0,                          // magic
             0x7D, 0x00, 0x00, 0x00,                          // length 125
             0x02,                                            // data
             0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // send time
             0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); // seq
  EXPECT_EQ(std::vector<std::byte>(wire.begin(), wire.end()), expected);

  absl::StatusOr<PacketHeader> decoded = DecodePacketHeader(wire);
  ASSERT_THAT(decoded, IsOk());
  EXPECT_EQ(decoded->packet_length, 125u);
  EXPECT_EQ(decoded->type, PacketType::kData);
  EXPECT_EQ(decoded->send_time_ns, 0x0102030405060708u);
  EXPECT_EQ(decoded->seq, 1u);
  EXPECT_EQ(decoded->BodySize(), 100u);
}

TEST(TunnelPacketHeaderTest, BodySizeLimit) {
  absl::StatusOr<PacketHeader> largest =
      MakePacketHeader(PacketType::kData, kMaxSendBodySize, 1, 0);
  ASSERT_THAT(largest, IsOk());
  EXPECT_EQ(largest->BodySize(), kMaxSendBodySize);
  EXPECT_THAT(MakePacketHeader(PacketType::kData, kMaxSendBodySize + 1, 1, 0),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(TunnelPacketHeaderTest, RejectsBadHeaders) {
  std::array<std::byte, kPacketHeaderSize> wire;
  PacketHeader header;
  header.packet_length = kPacketHeaderSize;
  header.type = PacketType::kHandshake;
  EncodePacketHeader(header, wire.data());
  ASSERT_THAT(DecodePacketHeader(wire), IsOk());

  EXPECT_FALSE(
      DecodePacketHeader(absl::MakeConstSpan(wire).first(kPacketHeaderSize - 1))
          .ok());

  std::array<std::byte, kPacketHeaderSize> bad_magic = wire;
  bad_magic[0] = std::byte{0x1B};
  EXPECT_FALSE(DecodePacketHeader(bad_magic).ok());

  std::array<std::byte, kPacketHeaderSize> short_length = wire;
  short_length[4] = static_cast<std::byte>(kPacketHeaderSize - 1);
  EXPECT_FALSE(DecodePacketHeader(short_length).ok());

  std::array<std::byte, kPacketHeaderSize> bad_type = wire;
  bad_type[8] = std::byte{0};
  EXPECT_FALSE(DecodePacketHeader(bad_type).ok());
  bad_type[8] = std::byte{4};
  EXPECT_FALSE(DecodePacketHeader(bad_type).ok());
}

TEST(TunnelHandshakeTest, UsesWireFieldNumbers) {
  Handshake handshake;
  handshake.set_topic_name("/a");
  handshake.set_channel_type("T");
  SubspaceTransport *transport = handshake.mutable_subspace();
  transport->set_slot_size(256);
  transport->set_num_slots(24);
  transport->set_is_reliable(true);
  transport->set_mux("m");
  transport->set_vchan_id(3);
  transport->set_checksum_size(4);
  transport->set_metadata_size(16);

  std::string serialized;
  ASSERT_TRUE(handshake.SerializeToString(&serialized));
  std::vector<std::byte> expected = Bytes({0x0A, 0x02});
  Append(expected, "/a");
  for (std::byte b : Bytes({0x12, 0x01})) {
    expected.push_back(b);
  }
  Append(expected, "T");
  for (std::byte b : Bytes({0x22, 0x10,              // subspace, 16 bytes
                            0x08, 0x80, 0x02,        // slot_size 256
                            0x10, 0x18,              // num_slots 24
                            0x18, 0x01,              // is_reliable
                            0x22, 0x01, 'm',         // mux
                            0x28, 0x03,              // vchan_id 3
                            0x30, 0x04,              // checksum_size 4
                            0x38, 0x10})) {          // metadata_size 16
    expected.push_back(b);
  }
  std::vector<std::byte> actual(
      reinterpret_cast<const std::byte *>(serialized.data()),
      reinterpret_cast<const std::byte *>(serialized.data()) +
          serialized.size());
  EXPECT_EQ(actual, expected);
}

TEST(TunnelHandshakeTest, AbsentChecksumSizeMeansFour) {
  Handshake handshake;
  handshake.set_topic_name("/a");
  handshake.mutable_subspace()->set_metadata_size(80);
  ASSERT_THAT(ValidateHandshake(handshake), IsOk());
  EXPECT_FALSE(handshake.subspace().has_checksum_size());
  EXPECT_EQ(HandshakeChecksumSize(handshake.subspace()), 4);
  // 48 bytes of prefix fields, a 4-byte checksum and 80 bytes of metadata,
  // rounded up to 64.
  EXPECT_EQ(WirePrefixSize(handshake.subspace()), 192);

  handshake.mutable_subspace()->set_checksum_size(32);
  EXPECT_EQ(HandshakeChecksumSize(handshake.subspace()), 32);
}

TEST(TunnelHandshakeTest, Validation) {
  Handshake handshake;
  EXPECT_FALSE(ValidateHandshake(handshake).ok());
  handshake.set_topic_name("/a");
  EXPECT_FALSE(ValidateHandshake(handshake).ok());
  handshake.mutable_subspace();
  EXPECT_THAT(ValidateHandshake(handshake), IsOk());

  Handshake bad = handshake;
  bad.mutable_subspace()->set_checksum_size(kMaxChecksumSize + 1);
  EXPECT_FALSE(ValidateHandshake(bad).ok());
  bad = handshake;
  bad.mutable_subspace()->set_metadata_size(-1);
  EXPECT_FALSE(ValidateHandshake(bad).ok());
  bad = handshake;
  bad.mutable_subspace()->set_metadata_size(kMaxMetadataSize + 1);
  EXPECT_FALSE(ValidateHandshake(bad).ok());
}

TEST(TunnelChannelTest, InternalChannels) {
  EXPECT_TRUE(IsInternalChannel("/subspace/ChannelDirectory"));
  EXPECT_TRUE(IsInternalChannel("/subspace/Statistics"));
  EXPECT_TRUE(IsInternalChannel("/ipc/discovery/receive"));
  EXPECT_TRUE(IsInternalChannel("/ipc/discovery/send"));
  EXPECT_TRUE(IsInternalChannel("/framework/ipc_information/host"));
  EXPECT_FALSE(IsInternalChannel("/camera/front"));
  EXPECT_FALSE(IsInternalChannel("/my/subspace"));
}

TEST(TunnelDiscoveryMessageTest, EncodesRosLayout) {
  DiscoveryMessage msg;
  msg.device_name = "dev";
  msg.published_topics.push_back({.topic = "/t",
                                  .datatype = "D",
                                  .mux = "",
                                  .vchan_id = -1,
                                  .is_reliable = true});
  msg.subscribed_topics.push_back({.topic = "/s", .datatype = ""});

  std::vector<std::byte> expected = Bytes({0x03, 0, 0, 0});
  Append(expected, "dev");
  for (std::byte b : Bytes({0x01, 0, 0, 0, 0x02, 0, 0, 0})) {
    expected.push_back(b);
  }
  Append(expected, "/t");
  for (std::byte b : Bytes({0x01, 0, 0, 0})) {
    expected.push_back(b);
  }
  Append(expected, "D");
  for (std::byte b : Bytes({0x00, 0, 0, 0,          // mux
                            0xFF, 0xFF, 0xFF, 0xFF, // vchan_id -1
                            0x01,                   // is_reliable
                            0x01, 0, 0, 0,          // one subscribed topic
                            0x02, 0, 0, 0})) {
    expected.push_back(b);
  }
  Append(expected, "/s");
  for (std::byte b : Bytes({0x00, 0, 0, 0,  // datatype
                            0x00, 0, 0, 0,  // mux
                            0x00, 0, 0, 0,  // vchan_id
                            0x00})) {       // is_reliable
    expected.push_back(b);
  }
  EXPECT_EQ(EncodeDiscoveryMessage(msg), expected);

  absl::StatusOr<DiscoveryMessage> decoded = DecodeDiscoveryMessage(expected);
  ASSERT_THAT(decoded, IsOk());
  EXPECT_EQ(decoded->device_name, "dev");
  EXPECT_EQ(decoded->published_topics, msg.published_topics);
  EXPECT_EQ(decoded->subscribed_topics, msg.subscribed_topics);
}

TEST(TunnelDiscoveryMessageTest, IgnoresTrailingBytes) {
  DiscoveryMessage msg;
  msg.device_name = "peer";
  std::vector<std::byte> wire = EncodeDiscoveryMessage(msg);
  wire.push_back(std::byte{0x55});
  absl::StatusOr<DiscoveryMessage> decoded = DecodeDiscoveryMessage(wire);
  ASSERT_THAT(decoded, IsOk());
  EXPECT_EQ(decoded->device_name, "peer");
  EXPECT_TRUE(decoded->published_topics.empty());
}

TEST(TunnelDiscoveryMessageTest, RejectsTruncatedMessages) {
  DiscoveryMessage msg;
  msg.device_name = "dev";
  msg.published_topics.push_back({.topic = "/t", .datatype = "D"});
  std::vector<std::byte> wire = EncodeDiscoveryMessage(msg);
  for (size_t length = 0; length < wire.size(); length++) {
    EXPECT_FALSE(
        DecodeDiscoveryMessage(absl::MakeConstSpan(wire).first(length)).ok())
        << "length " << length;
  }
}

TEST(TunnelDiscoveryMessageTest, RejectsHugeCounts) {
  // An empty device name followed by a topic count of 0xFFFFFFFF.
  std::vector<std::byte> wire =
      Bytes({0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0});
  EXPECT_FALSE(DecodeDiscoveryMessage(wire).ok());
  // A string length larger than the message.
  wire = Bytes({0xFF, 0xFF, 0xFF, 0x7F, 'a'});
  EXPECT_FALSE(DecodeDiscoveryMessage(wire).ok());
}

// A prefix area followed by a payload, laid out like a slot.
class PrefixTest : public ::testing::Test {
protected:
  static constexpr int32_t kMetadataSize = 16;

  void SetUp() override {
    memset(area_, 0, sizeof(area_));
    for (size_t i = 0; i < sizeof(payload_); i++) {
      payload_[i] = static_cast<char>(i * 7);
    }
    prefix()->slot_id = 3;
    prefix()->message_size = sizeof(payload_);
    prefix()->ordinal = 42;
    prefix()->timestamp = 1234;
    prefix()->vchan_id = -1;
    prefix()->checksum_size = 4;
    prefix()->metadata_size = kMetadataSize;
    char *metadata = reinterpret_cast<char *>(prefix()) +
                     offsetof(MessagePrefix, checksum) + 4;
    memcpy(metadata, "metadata-bytes!!", kMetadataSize);
  }

  MessagePrefix *prefix() { return reinterpret_cast<MessagePrefix *>(area_); }

  // Checksums the way a Subspace publisher does, with flags masked.
  void SubspaceChecksum() {
    prefix()->SetHasChecksum();
    auto data = GetMessageChecksumData(prefix(), payload_, sizeof(payload_), 4,
                                       kMetadataSize);
    CalculateCRC32Checksum(data.Spans(), GetChecksumSpan(prefix(), 4));
  }

  // Checksums with every flag bit included.
  void FullFlagsChecksum() {
    prefix()->SetHasChecksum();
    const auto *base = reinterpret_cast<const uint8_t *>(prefix());
    std::array<absl::Span<const uint8_t>, 3> spans = {
        absl::Span<const uint8_t>(base + offsetof(MessagePrefix, slot_id),
                                  offsetof(MessagePrefix, checksum) -
                                      offsetof(MessagePrefix, slot_id)),
        absl::Span<const uint8_t>(base + offsetof(MessagePrefix, checksum) + 4,
                                  kMetadataSize),
        absl::Span<const uint8_t>(reinterpret_cast<const uint8_t *>(payload_),
                                  sizeof(payload_))};
    CalculateCRC32Checksum(spans, GetChecksumSpan(prefix(), 4));
  }

  bool SubspaceVerifies() {
    auto data = GetMessageChecksumData(prefix(), payload_, sizeof(payload_), 4,
                                       kMetadataSize);
    return VerifyCRC32Checksum(data.Spans(), GetChecksumSpan(prefix(), 4));
  }

  IngressPrefixResult Translate(int32_t local_vchan_id,
                                int32_t checksum_size = 4) {
    return TranslateIngressPrefix(prefix(), payload_, sizeof(payload_),
                                  checksum_size, kMetadataSize,
                                  local_vchan_id);
  }

  alignas(64) char area_[128];
  char payload_[100];
};

TEST_F(PrefixTest, EgressKeepsOnlyChecksummedFlags) {
  SubspaceChecksum();
  prefix()->flags |= kMessageBridged | kMessageCrossMachine | 0x100;
  PrepareEgressPrefix(reinterpret_cast<std::byte *>(prefix()));
  EXPECT_EQ(prefix()->flags, kMessageHasChecksum);
  EXPECT_TRUE(SubspaceVerifies());
}

TEST_F(PrefixTest, IngressLeavesMatchingPrefixAlone) {
  SubspaceChecksum();
  MessagePrefix before = *prefix();
  EXPECT_EQ(Translate(-1), IngressPrefixResult::kUnchanged);
  EXPECT_EQ(memcmp(&before, prefix(), sizeof(before)), 0);
  EXPECT_TRUE(SubspaceVerifies());
}

TEST_F(PrefixTest, IngressRecomputesFullFlagsChecksum) {
  // A peer that checksums every flag bit, with its cross-machine bit set.
  prefix()->flags = 2;
  FullFlagsChecksum();
  EXPECT_FALSE(SubspaceVerifies());
  EXPECT_EQ(Translate(-1), IngressPrefixResult::kRewritten);
  EXPECT_EQ(prefix()->flags, kMessageHasChecksum);
  EXPECT_TRUE(SubspaceVerifies());
}

TEST_F(PrefixTest, IngressAcceptsMaskedChecksumWithExtraFlags) {
  SubspaceChecksum();
  prefix()->flags |= 0x40;
  EXPECT_EQ(Translate(-1), IngressPrefixResult::kRewritten);
  EXPECT_EQ(prefix()->flags, kMessageHasChecksum);
  EXPECT_TRUE(SubspaceVerifies());
}

TEST_F(PrefixTest, IngressNormalizesVirtualChannelId) {
  prefix()->vchan_id = 7;
  SubspaceChecksum();
  EXPECT_EQ(Translate(2), IngressPrefixResult::kRewritten);
  EXPECT_EQ(prefix()->vchan_id, 2);
  EXPECT_TRUE(SubspaceVerifies());
}

TEST_F(PrefixTest, IngressKeepsBadChecksumBad) {
  prefix()->vchan_id = 7;
  SubspaceChecksum();
  payload_[10] ^= 1;
  EXPECT_EQ(Translate(2), IngressPrefixResult::kBadChecksum);
  EXPECT_EQ(prefix()->vchan_id, 2);
  EXPECT_FALSE(SubspaceVerifies());
}

TEST_F(PrefixTest, IngressWithoutChecksum) {
  prefix()->flags = 2;
  prefix()->vchan_id = 7;
  EXPECT_EQ(Translate(-1), IngressPrefixResult::kRewritten);
  EXPECT_EQ(prefix()->flags, 0);
  EXPECT_EQ(prefix()->vchan_id, -1);
}

TEST_F(PrefixTest, IngressCustomChecksumIsNotRecomputed) {
  prefix()->vchan_id = 7;
  prefix()->SetHasChecksum();
  uint32_t checksum = prefix()->checksum;
  EXPECT_EQ(Translate(2, /*checksum_size=*/8),
            IngressPrefixResult::kChecksumNotRecomputed);
  EXPECT_EQ(prefix()->vchan_id, 2);
  EXPECT_EQ(prefix()->checksum, checksum);
}

} // namespace
} // namespace subspace::tunnel
