# Checksums and User Metadata

Subspace supports optional per-message checksums for integrity verification and
per-message user metadata attached to the message prefix.  Both features are
controlled through `PublisherOptions` and affect the layout of the prefix area
that precedes every message buffer in shared memory.

## Message Prefix Layout

Every message slot in shared memory begins with a **prefix area** followed by
the message buffer itself.  The prefix area always starts with the fixed-size
`MessagePrefix` struct (64 bytes), which contains:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | `padding` (reserved for bridge transport) |
| 4 | 4 | `slot_id` |
| 8 | 8 | `message_size` |
| 16 | 8 | `ordinal` |
| 24 | 8 | `timestamp` |
| 32 | 8 | `flags` |
| 40 | 4 | `vchan_id` |
| 44 | 2 | `checksum_size` |
| 46 | 2 | `metadata_size` |
| 48 | 4 | `checksum` (start of checksum storage) |
| 52 | 12 | padding to 64-byte boundary |

The `checksum` field at offset 48 marks the **start** of the checksum storage
area.  If the configured `checksum_size` exceeds the remaining space in the
first 64-byte chunk, the prefix area automatically extends into additional
64-byte chunks.  The same applies to user metadata, which follows immediately
after the checksum area.

### Prefix Size Calculation

The total prefix size in bytes is:

```
prefix_size = align_up(offsetof(MessagePrefix, checksum) + checksum_size + metadata_size, 64)
```

This is always a multiple of 64.  With the defaults (`checksum_size = 4`,
`metadata_size = 0`) the prefix fits in 64 bytes — just the `MessagePrefix`
itself.  The `Channel::ComputePrefixSize()` static method performs this
calculation.

**Examples:**

| `checksum_size` | `metadata_size` | Prefix Size |
|-----------------|-----------------|-------------|
| 4 (default) | 0 (default) | 64 bytes |
| 20 | 0 | 128 bytes |
| 4 | 12 | 64 bytes |
| 4 | 13 | 128 bytes |
| 32 | 64 | 192 bytes |

## Checksums

### Enabling Checksums

Checksums are opt-in.  Enable them on the publisher and subscriber:

```cpp
auto pub = client->CreatePublisher("channel",
    subspace::PublisherOptions()
        .SetSlotSize(1024)
        .SetNumSlots(10)
        .SetChecksum(true));

auto sub = client->CreateSubscriber("channel",
    subspace::SubscriberOptions()
        .SetChecksum(true));
```

When enabled, the publisher computes a checksum over the message prefix fields
(excluding the checksum itself) and the message body, then stores it in the
checksum area.  The subscriber recomputes the checksum and compares it against
the stored value.

### Default CRC32

By default, `checksum_size` is 4 and the built-in hardware-accelerated CRC32
is used:

- **`CalculateCRC32Checksum()`** — computes CRC32 and writes 4 bytes.
- **`VerifyCRC32Checksum()`** — recomputes CRC32 and compares against the
  stored 4 bytes.

These functions use ARM or x86 hardware CRC instructions when available
(`SUBSPACE_HARDWARE_CRC`), with a software fallback.

### Larger Checksums

To use a checksum algorithm that produces more than 4 bytes, increase
`checksum_size`:

```cpp
auto pub = client->CreatePublisher("channel",
    subspace::PublisherOptions()
        .SetSlotSize(1024)
        .SetNumSlots(10)
        .SetChecksum(true)
        .SetChecksumSize(20));   // 20 bytes for SHA-1, for example
```

All publishers on a channel must use the same `checksum_size`.  The server
validates consistency and rejects mismatched values.

### Custom Checksum Callbacks

The `ChecksumCallback` type lets you replace the built-in CRC32 with any
algorithm:

```cpp
using ChecksumCallback = std::function<void(
    const std::array<absl::Span<const uint8_t>, 3> &data,
    absl::Span<std::byte> checksum)>;
```

The three spans in `data` cover:

| Index | Contents |
|-------|----------|
| 0 | Prefix fields before the checksum (`slot_id` through `metadata_size`) |
| 1 | User metadata area (may be empty) |
| 2 | Message payload |

The `checksum` span is `ChecksumSize()` bytes and points directly into the
prefix's checksum storage.  The callback should write the computed checksum
there.

**Setting a callback:**

```cpp
auto my_checksum = [](const std::array<absl::Span<const uint8_t>, 3> &data,
                      absl::Span<std::byte> checksum) {
    // Compute digest over data[0], data[1], data[2]
    // Write result into checksum.data(), up to checksum.size() bytes
};

pub.SetChecksumCallback(my_checksum);
sub.SetChecksumCallback(my_checksum);
```

Both the publisher and subscriber must use the same callback.  Call
`ResetChecksumCallback()` to revert to the built-in CRC32.

### Checksum Verification Behavior

When a subscriber reads a message with a checksum error:

- **`pass_checksum_errors = false`** (default) — `ReadMessage()` returns an
  error status.
- **`pass_checksum_errors = true`** — the message is delivered but its
  `checksum_error` flag is set, allowing the application to decide how to
  handle it.

## User Metadata

### Overview

User metadata is arbitrary per-message data stored in the prefix area,
immediately after the checksum storage.  It travels through shared memory and
across bridges alongside the message, but is separate from the message payload.

Use cases include:
- Timestamps from different clock domains
- Sequence numbers from upstream sources
- Small fixed-size annotations that every message carries
- Routing or priority tags

### Enabling Metadata

Set `metadata_size` on the publisher to reserve space:

```cpp
auto pub = client->CreatePublisher("channel",
    subspace::PublisherOptions()
        .SetSlotSize(1024)
        .SetNumSlots(10)
        .SetMetadataSize(16));   // 16 bytes of user metadata
```

All publishers on a channel must use the same `metadata_size`.  The server
validates consistency.

### Writing Metadata (Publisher)

Call `GetMetadata()` between `GetMessageBuffer()` and `PublishMessage()`:

```cpp
auto buffer = pub.GetMessageBuffer(128).value();

// Write per-message metadata
auto meta = pub.GetMetadata();   // absl::Span<std::byte>, 16 bytes
MyMetadata m = { .source_id = 42, .priority = 1 };
memcpy(meta.data(), &m, sizeof(m));

// Write message payload into buffer...

pub.PublishMessage(payload_size);
```

`GetMetadata()` returns an `absl::Span<std::byte>` of `MetadataSize()` bytes.
It returns an empty span if `MetadataSize()` is 0 or no slot is currently held.

### Reading Metadata (Subscriber)

Call `GetMetadata()` after `ReadMessage()`:

```cpp
auto msg = sub.ReadMessage().value();
if (msg.length > 0) {
    auto meta = sub.GetMetadata();  // absl::Span<const std::byte>, 16 bytes
    const MyMetadata *m = reinterpret_cast<const MyMetadata *>(meta.data());
    // Use m->source_id, m->priority, etc.
}
```

The span is valid while the message is active — between `ReadMessage()` and
the next `ReadMessage()` or `ClearActiveMessage()` call.  It returns an empty
span if `MetadataSize()` is 0.

### Metadata and Checksums Together

When both are enabled, the checksum covers the metadata area (span index 1 in
the callback).  This means checksum verification will detect corruption in
metadata as well as in the message body.

```cpp
auto pub = client->CreatePublisher("channel",
    subspace::PublisherOptions()
        .SetSlotSize(1024)
        .SetNumSlots(10)
        .SetChecksum(true)
        .SetChecksumSize(4)
        .SetMetadataSize(32));
```

## Querying Sizes at Runtime

Both `Publisher` and `Subscriber` expose:

| Method | Returns |
|--------|---------|
| `PrefixSize()` | Total prefix area in bytes (multiple of 64) |
| `ChecksumSize()` | Bytes reserved for the checksum |
| `MetadataSize()` | Bytes of user metadata |

These reflect the values configured on the channel, which may have been set by
an earlier publisher.

## Bridge Behavior

Checksum size and metadata size are propagated across the bridge in the
`Subscribed` protobuf message.  When a bridge receiver creates a local
publisher on the receiving server, it uses the same `checksum_size` and
`metadata_size` as the originating channel.  The entire prefix area
(including checksum and metadata) is transmitted as part of the bridged message.

## Server-Side Validation

The server enforces the following rules:

- `checksum_size` must be at least 4 (clamped if lower).
- `metadata_size` must be at least 0 (clamped if negative).
- If a channel already has publishers, new publishers must use the same
  `checksum_size` and `metadata_size`.  Mismatched values are rejected with
  an error.
