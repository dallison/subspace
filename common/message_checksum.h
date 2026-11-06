// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "common/crc32.h"
#include "common/shm_layout.h"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace subspace {

// The prefix fields a message checksum covers: slot_id up to the checksum,
// with flags limited to kChecksummedMessageFlags.  The bridge and the tunnel
// set other flags after the checksum is computed.
class ChecksummedPrefixFields {
public:
  explicit ChecksummedPrefixFields(const MessagePrefix *prefix) {
    std::memcpy(bytes_,
                reinterpret_cast<const uint8_t *>(prefix) +
                    offsetof(MessagePrefix, slot_id),
                sizeof(bytes_));
    int64_t flags;
    std::memcpy(&flags, bytes_ + kFlagsOffset, sizeof(flags));
    flags &= kChecksummedMessageFlags;
    std::memcpy(bytes_ + kFlagsOffset, &flags, sizeof(flags));
  }

  const uint8_t *Data() const { return bytes_; }
  static constexpr size_t Size() { return kSize; }

private:
  static constexpr size_t kFlagsOffset =
      offsetof(MessagePrefix, flags) - offsetof(MessagePrefix, slot_id);
  static constexpr size_t kSize =
      offsetof(MessagePrefix, checksum) - offsetof(MessagePrefix, slot_id);

  uint8_t bytes_[kSize];
};

// The user metadata, which follows the checksum storage in the prefix.
inline const uint8_t *ChecksummedMetadata(const MessagePrefix *prefix,
                                          int32_t checksum_size) {
  return reinterpret_cast<const uint8_t *>(prefix) +
         offsetof(MessagePrefix, checksum) + checksum_size;
}

// The default message checksum: CRC32 over the checksummed prefix fields, the
// metadata and the payload.
inline uint32_t MessageChecksum(const MessagePrefix *prefix,
                                const void *payload, size_t payload_size,
                                int32_t checksum_size,
                                int32_t metadata_size) {
  const ChecksummedPrefixFields fields(prefix);
  uint32_t crc = 0xFFFFFFFF;
  crc = SubspaceCRC32(crc, fields.Data(), fields.Size());
  crc = SubspaceCRC32(crc, ChecksummedMetadata(prefix, checksum_size),
                      static_cast<size_t>(metadata_size));
  crc = SubspaceCRC32(crc, static_cast<const uint8_t *>(payload),
                      payload_size);
  return ~crc;
}

inline void SetMessageChecksum(MessagePrefix *prefix, const void *payload,
                               size_t payload_size, int32_t checksum_size,
                               int32_t metadata_size) {
  const uint32_t checksum = MessageChecksum(prefix, payload, payload_size,
                                            checksum_size, metadata_size);
  std::memcpy(&prefix->checksum, &checksum, sizeof(checksum));
}

inline bool MessageChecksumMatches(const MessagePrefix *prefix,
                                   const void *payload, size_t payload_size,
                                   int32_t checksum_size,
                                   int32_t metadata_size) {
  uint32_t stored;
  std::memcpy(&stored, &prefix->checksum, sizeof(stored));
  return stored == MessageChecksum(prefix, payload, payload_size,
                                   checksum_size, metadata_size);
}

} // namespace subspace
