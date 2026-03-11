# Subspace C++ Client Design Document

## 1. Overview

Subspace is a shared-memory inter-process communication (IPC) system. Publishers
write messages into shared memory slots and subscribers read them, with the
server acting as a coordination broker for channel creation, resource
management, and trigger distribution. Once shared memory is mapped, message
data never passes through the server — publishers and subscribers communicate
directly through shared memory using lock-free atomic operations.

### Key Properties

- **Zero-copy messaging.** Subscribers read directly from the publisher's
  shared memory buffer; no data is copied between processes.
- **Lock-free data path.** After initial setup, all slot ownership transfers
  use compare-and-swap (CAS) on a single 64-bit atomic; no mutexes are held
  during publish or read.
- **Multiple publishers and subscribers per channel.** A channel supports any
  number of concurrent publishers and subscribers.
- **Unreliable (default) and reliable modes.** Unreliable publishers may
  overwrite unread messages when slots are exhausted. Reliable publishers block
  until subscribers have released a slot.
- **Channel multiplexing.** Multiple virtual channels can share a single
  physical channel (shared memory region), each with its own ordinal sequence
  and activation state.
- **Checksums and metadata.** Messages may carry a configurable-size checksum
  and a user metadata area in the prefix, both participating in integrity
  verification.

---

## 2. Architecture

### 2.1 Components

```
┌─────────────┐      Unix socket       ┌──────────────┐
│   Client     │◄──────────────────────►│    Server     │
│  (Publisher)  │   protobuf + FDs      │  (Broker)     │
│  (Subscriber) │                       └──────────────┘
└──────┬───────┘                               │
       │  mmap                                 │ creates shared memory
       ▼                                       ▼
  ┌─────────────────────────────────────────────────┐
  │              Shared Memory                       │
  │  ┌─────┐  ┌─────┐  ┌───────────────────┐       │
  │  │ SCB │  │ CCB │  │ Buffers (per slot) │       │
  │  └─────┘  └─────┘  └───────────────────┘       │
  └─────────────────────────────────────────────────┘
```

- **Server:** Creates and manages shared memory files, allocates channel/subscriber/publisher IDs, validates options, and distributes file descriptors via `SCM_RIGHTS`.
- **Client:** Connects to the server over a Unix domain socket. After channel setup, all message passing occurs through shared memory without server involvement.
- **Shared Memory Regions:**
  - **SCB (System Control Block):** Global per-session; contains per-channel counters used to detect configuration changes.
  - **CCB (Channel Control Block):** Per-channel; contains slot metadata, bitsets, ordinal accumulators, subscriber tracking, and statistics.
  - **BCB (Buffer Control Block):** Per-channel; tracks buffer reference counts and sizes.
  - **Buffers:** Per-channel, one or more; hold the actual message prefixes and payloads.

### 2.2 Connection Protocol

1. The client connects to the server via a Unix domain socket (default `/tmp/subspace`).
2. The client sends an `Init` protobuf request containing the client name.
3. The server responds with a session ID and transmits the SCB file descriptor via `SCM_RIGHTS`.
4. The client maps the SCB into its address space.
5. All subsequent requests (CreatePublisher, CreateSubscriber, etc.) use the same socket with a 4-byte length prefix + serialized protobuf, followed by file descriptor transfer for shared memory regions.

### 2.3 Thread Safety

By default, the client is **not** thread-safe. Call `SetThreadSafe(true)` to
enable internal locking. In thread-safe mode:

- `GetMessageBuffer` / `GetMessageBufferSpan` acquires a lock that is held
  until `PublishMessage` or `CancelPublish` is called.
- All other operations acquire and release the lock within the call.

### 2.4 Coroutine Support

The client accepts an optional `co::Coroutine*` parameter. When provided, blocking operations (`Wait`, `ReadMessage` on placeholder channels) yield the coroutine instead of blocking the thread.

---

## 3. Shared Memory Layout

### 3.1 MessagePrefix (64 bytes, 64-byte aligned)

Every message slot begins with a `MessagePrefix` structure:

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | `padding` | `int32_t` | Reserved for socket framing (used by bridge). |
| 4 | `slot_id` | `int32_t` | Slot identifier. |
| 8 | `message_size` | `uint64_t` | Payload size in bytes. |
| 16 | `ordinal` | `uint64_t` | Monotonically increasing sequence number. |
| 24 | `timestamp` | `uint64_t` | Publish time (nanoseconds, `CLOCK_MONOTONIC`). |
| 32 | `flags` | `int64_t` | Bitfield: `kMessageActivate` (1), `kMessageBridged` (2), `kMessageHasChecksum` (4). |
| 40 | `vchan_id` | `int32_t` | Virtual channel ID (-1 = global). |
| 44 | `checksum_size` | `uint16_t` | Size of checksum area in bytes. |
| 46 | `metadata_size` | `uint16_t` | Size of user metadata area in bytes. |
| 48 | `checksum` | `uint32_t` | Start of checksum storage (extends beyond 4 bytes for larger checksums). |
| 52 | `padding3` | `char[12]` | Padding to 64 bytes. |

The prefix may extend beyond 64 bytes when `checksum_size + metadata_size > 16`. The total prefix size is:

```
PrefixSize = Aligned<64>(48 + checksum_size + metadata_size)
```

The extended prefix layout after offset 48:

```
[checksum storage: checksum_size bytes]
[metadata area: metadata_size bytes]
[alignment padding to next 64-byte boundary]
```

### 3.2 Buffer Layout

Each buffer is a single shared memory region. Within a buffer, slots are laid out contiguously:

```
┌──────────────────────────────────────────────────┐
│ Slot 0: [Prefix | Payload]                       │
│ Slot 1: [Prefix | Payload]                       │
│ ...                                              │
│ Slot N-1: [Prefix | Payload]                     │
└──────────────────────────────────────────────────┘
```

Each slot occupies `PrefixSize + Aligned<64>(SlotSize)` bytes. The buffer
address for slot `i` is:

```
buffer_base + (PrefixSize + Aligned<64>(SlotSize)) * i + PrefixSize
```

### 3.3 Slot Ref-Count Bitfield

Each `MessageSlot` contains a 64-bit atomic `refs` field that encodes multiple
pieces of state in a single word, enabling lock-free ownership transfer:

| Bits | Field | Description |
|------|-------|-------------|
| 0–9 | ref_count | Total subscriber reference count (max 1023). |
| 10–19 | reliable_ref_count | Reliable subscriber reference count. |
| 20–29 | retired_refs | Count of subscribers that have retired this slot. |
| 30–39 | vchan_id | Virtual channel ID (10 bits; 1023 encodes -1). |
| 40–62 | ordinal | Low 23 bits of the message ordinal. |
| 63 | pub_owned | Set when a publisher owns the slot for writing. |

### 3.4 Channel Control Block (CCB)

The CCB contains:

- **Channel name** (max 64 chars, for debugging).
- **Slot array** (`MessageSlot slots[]`, variable length).
- **OrdinalAccumulator** — per-virtual-channel atomic ordinal counters.
- **ActivationTracker** — bitset of activated virtual channels.
- **Subscriber tracking** — bitset of active subscribers, per-subscriber vchan_id array, subscriber counter per vchan.
- **Statistics** — `total_bytes`, `total_messages`, `max_message_size`, `total_drops` (atomics).
- **free_slots_exhausted** — atomic bool, optimization to skip scanning the free-slots bitset.

Following the slot array (with 64-byte alignment):

1. **RetiredSlots** bitset — one bit per slot; set when all subscribers have released the slot.
2. **FreeSlots** bitset — one bit per slot; set when the slot has never been used or has been returned to the free pool.
3. **AvailableSlots** — one bitset per subscriber (up to 1024); bit set when a new message is available for that subscriber.

---

## 4. Publisher

### 4.1 Creation

`Client::CreatePublisher(channel_name, slot_size, num_slots, options)`:

1. Sends a `CreatePublisher` RPC to the server.
2. The server creates the channel (if new) with the specified slot size and slot count, validates options (type matching, checksum/metadata size limits), and returns file descriptors for CCB, BCB, and buffer shared memory.
3. The client maps all shared memory regions.
4. For **unreliable** publishers: a free slot is immediately claimed via `FindFreeSlotUnreliable`. If `options.activate` is set, an activation message is published.
5. For **reliable** publishers: an activation message is published via `ActivateReliableChannel`.
6. Subscriber trigger file descriptors are collected so the publisher can notify subscribers on publish.

### 4.2 Publishing a Message

The publish flow is a two-phase protocol:

**Phase 1: Get buffer**

```cpp
void* buf = publisher.GetMessageBuffer(max_size);
// Write payload into buf.
// Optionally write metadata via publisher.GetMetadata().
```

- If `max_size` exceeds the current slot size, the channel is resized (see Section 8).
- For reliable publishers, returns `nullptr` when no slot is available.
- For unreliable publishers, always returns a buffer (may recycle a slot with unread messages).

**Phase 2: Publish**

```cpp
Message msg = publisher.PublishMessage(payload_size);
```

Internally, `ActivateSlotAndGetAnother`:

1. Assigns an ordinal from `OrdinalAccumulator::Next(vchan_id)`.
2. Writes the timestamp (`CLOCK_MONOTONIC`).
3. Fills the `MessagePrefix` fields: `message_size`, `ordinal`, `timestamp`, `vchan_id`, `checksum_size`, `metadata_size`, `flags`, `slot_id`.
4. If checksum is enabled, computes the checksum over three data regions:
   - Prefix fields from `slot_id` through `metadata_size` (bytes 4–48).
   - User metadata area.
   - Message payload.
5. **Releases the slot** with `slot->refs.store(BuildRefsBitField(...), memory_order_release)`, clearing the `pub_owned` bit. The `memory_order_release` ensures all prefix and payload writes are visible before the store.
6. Sets the corresponding bit in each subscriber's `AvailableSlots` bitset.
7. Triggers all subscribers by writing to their trigger file descriptors.
8. For unreliable publishers, immediately claims the next free slot for subsequent publishes.

### 4.3 GetMetadata

`Publisher::GetMetadata()` returns a writable `absl::Span<std::byte>` over the metadata area in the current slot's prefix. This span is valid between `GetMessageBuffer` and `PublishMessage`. If `metadata_size` is 0, the span is empty.

### 4.4 CancelPublish

`Publisher::CancelPublish()` releases the current slot and any held lock without publishing. Required in thread-safe mode if the caller decides not to publish after calling `GetMessageBuffer`.

### 4.5 Retirement Notifications

If `notify_retirement` is set, the publisher receives a pipe file descriptor. When a slot is fully retired (all subscribers have released it), the slot ID is written to this pipe. The publisher can poll `GetRetirementFd()` to learn which slots have been retired.

### 4.6 On-Send Callback

`Publisher::SetOnSendCallback(callback)` registers a callback invoked just before publish. The callback receives the buffer pointer and message size, and returns the (possibly modified) message size. Returning an error aborts the publish.

---

## 5. Subscriber

### 5.1 Creation

`Client::CreateSubscriber(channel_name, options)`:

1. Sends a `CreateSubscriber` RPC to the server.
2. If no publisher exists for the channel, a **placeholder** subscriber is created with `num_slots = 0`. The placeholder is automatically reloaded when a publisher appears (detected via SCB counter changes).
3. Otherwise, the client maps CCB, BCB, and buffer shared memory. The subscriber registers itself in the CCB's subscriber bitset and initializes its `AvailableSlots` bitset.
4. Trigger file descriptors are set up for poll-based notification.

### 5.2 Reading Messages

```cpp
Message msg = subscriber.ReadMessage(ReadMode::kReadNext);
```

**ReadMessage flow:**

1. If the subscriber is a placeholder, attempt to reload it by contacting the server.
2. If reliable publisher triggers need refreshing (detected via SCB counters), reload them.
3. Clear the subscriber's poll trigger.
4. **Slot selection:**
   - `kReadNext`: Scans `AvailableSlots` for this subscriber, collects all slots with non-zero ordinal that are not publisher-owned and match the vchan_id filter. Sorts by timestamp. Returns the first slot whose ordinal has not been seen.
   - `kReadNewest`: Same scan, but returns only the most recent slot.
5. **Claim the slot:** `AtomicIncRefCount(slot, +1)` increments the ref count via CAS. If the CAS fails (slot was recycled), retries from scratch.
6. **Dropped message detection:** Compares the new message's ordinal against the ordinal tracker. Gaps indicate dropped messages; the dropped-message callback is invoked with the count.
7. **Checksum verification:** If the prefix has the `kMessageHasChecksum` flag:
   - Computes the checksum over the same three data regions used by the publisher.
   - Compares against the stored checksum.
   - If mismatch and `pass_checksum_errors` is false: returns an error.
   - If mismatch and `pass_checksum_errors` is true: sets `msg.checksum_error = true` and delivers the message.
8. **Activation messages:** If the message is an activation and `pass_activation` is false, the message is silently consumed (ordinal remembered, ref decremented) and the next message is read recursively.
9. **Active message tracking:** The slot reference is stored in an `ActiveMessage` object. The `Message` returned to the user holds a `shared_ptr<ActiveMessage>`. When the `Message` (and all copies) is destroyed, `ActiveMessage::DecRef` decrements the ref count. When the ref count reaches zero, `RemoveActiveMessage` is called, which:
   - Decrements the slot's atomic ref count.
   - If the slot is fully retired (all subscribers released), sets the bit in `RetiredSlots`.
   - Writes the slot ID to retirement notification pipes.
   - Triggers reliable publishers that a slot may be available.

### 5.3 GetMetadata

`Subscriber::GetMetadata()` returns a read-only `absl::Span<const std::byte>` over the metadata area. Valid while the message is active (before the `Message` object is destroyed or the next `ReadMessage` call replaces it).

### 5.4 FindMessage

`Subscriber::FindMessage(timestamp)` performs a binary search over active slots to locate a message by timestamp. Returns an empty `Message` if not found. This allows random access to historical messages that are still in the slot ring.

### 5.5 Dropped Message Callback

```cpp
subscriber.RegisterDroppedMessageCallback([](Subscriber*, int64_t count) {
    // 'count' messages were dropped.
});
```

Invoked during `ReadMessage` when ordinal gaps are detected. The callback receives the subscriber pointer and the number of dropped messages.

### 5.6 Message Callback

```cpp
subscriber.RegisterMessageCallback([](Subscriber*, Message msg) {
    // Process message.
});
subscriber.ProcessAllMessages();
```

`ProcessAllMessages` reads all available messages and invokes the registered callback for each one.

### 5.7 GetAllMessages

```cpp
std::vector<Message> msgs = subscriber.GetAllMessages();
```

Returns all available messages as a vector. If the subscriber has reached its `max_active_messages` limit, it is untriggered to prevent further wakeups until messages are released.

### 5.8 On-Receive Callback

`Subscriber::SetOnReceiveCallback(callback)` registers a callback invoked when a message is read. The callback receives the buffer pointer and message size, and returns the (possibly modified) message size. This can be used for in-place transformation or validation.

---

## 6. Message Lifetime and Reference Counting

### 6.1 Message Object

The `Message` struct returned by `ReadMessage` contains:

| Field | Description |
|-------|-------------|
| `length` | Payload size in bytes. |
| `buffer` | Pointer to payload in shared memory (read-only for subscribers). |
| `ordinal` | Monotonic sequence number. |
| `timestamp` | Publish time (nanoseconds). |
| `vchan_id` | Virtual channel ID. |
| `is_activation` | Whether this is an activation message. |
| `slot_id` | Slot identifier. |
| `checksum_error` | Whether checksum verification failed. |
| `active_message` | `shared_ptr<ActiveMessage>` — holds the slot reference. |

### 6.2 Copy and Move Semantics

- **Copy:** Increments `ActiveMessage::refs`. The slot remains held until all copies are destroyed.
- **Move:** Transfers ownership without changing the ref count.
- **Destroy / Reset:** Decrements `ActiveMessage::refs`. When it reaches zero, the slot is released.

### 6.3 subspace::shared_ptr and subspace::weak_ptr

The typed `ReadMessage<T>()` overload returns a `subspace::shared_ptr<T>` which wraps a `Message` and provides `operator->()` and `operator*()` for typed access to the payload. `subspace::weak_ptr<T>` allows holding a non-owning reference that does not prevent slot release.

### 6.4 Active Message Limit

`max_active_messages` (default 1) limits how many slots the subscriber can hold simultaneously. Exceeding this limit causes `AddActiveMessage` to return false, and the subscriber may need to release older messages before reading new ones. When `keep_active_message` is true, the subscriber maintains a reference to the most recently read message until `ClearActiveMessage()` is called.

---

## 7. Checksums

### 7.1 Default (CRC32)

The default checksum is CRC32, stored in 4 bytes at the `checksum` field of the prefix.

### 7.2 Custom Checksums

Both publisher and subscriber can install a custom checksum callback:

```cpp
using ChecksumCallback = std::function<void(
    const std::array<absl::Span<const uint8_t>, 3>& data,
    absl::Span<std::byte> checksum)>;
```

The callback receives three spans of data to checksum:

1. Prefix fields from `slot_id` through `metadata_size` (offsets 4–48).
2. User metadata area (immediately after checksum storage).
3. Message payload.

The callback writes the computed checksum into the `checksum` span. The publisher and subscriber must use the same callback for checksums to match.

### 7.3 Checksum Size Limits

Both `checksum_size` and `metadata_size` are stored as `uint16_t` in the prefix. The server validates that neither exceeds `0xFFFF` (65535 bytes).

### 7.4 What Is and Is Not Checksummed

- **Checksummed:** `slot_id`, `message_size`, `ordinal`, `timestamp`, `flags`, `vchan_id`, `checksum_size`, `metadata_size`, user metadata, and the message payload.
- **Not checksummed:** The `padding` field (offset 0–3), the checksum storage itself, and any alignment padding between the end of metadata and the end of the prefix.

---

## 8. Channel Resize

When a publisher calls `GetMessageBuffer(max_size)` and `max_size` exceeds the current slot size:

1. If the channel is `fixed_size`, the operation fails.
2. If a resize callback is registered, it is invoked with (old_size, new_size). Returning an error prevents the resize.
3. A new shared memory buffer is created with the larger slot size.
4. The buffer count in the CCB is atomically incremented via CAS.
5. Existing subscribers detect the new buffer count (via SCB counter changes or direct CCB polling) and attach the new buffer.
6. Slots are assigned to the new (largest) buffer on their next use.
7. Old buffers are unmapped when their reference count drops to zero.

The new slot size is computed using a growth strategy:
- Slots ≤ 4 KB: 2× growth
- Slots ≤ 64 KB: 1.5× growth
- Slots ≤ 1 MB: 1.25× growth
- Larger: 1.125× growth

---

## 9. Reliable Mode

### 9.1 Publisher Behavior

A reliable publisher guarantees that no message is overwritten while any
subscriber still holds a reference to it.

- `FindFreeSlotReliable` only selects slots with zero ref count (both regular and reliable) and does not skip past slots with non-zero reliable ref counts.
- If no slot is available, `GetMessageBuffer` returns `nullptr` (or an empty span). The caller must use `Wait()` or poll the publisher's file descriptor and retry.
- Publishing requires at least one subscriber to exist; otherwise `GetMessageBuffer` returns `nullptr`.

### 9.2 Subscriber Behavior

- Reliable subscribers increment both the regular and reliable ref counts in the slot's `refs` field.
- The reliable ref count prevents the slot from being reused by a reliable publisher until all reliable subscribers have released it.

### 9.3 Backpressure

When a reliable subscriber releases a message (destroying the `Message` object), the subscriber triggers all reliable publishers via their trigger file descriptors. This wakes the publisher's `Wait()` or makes its poll fd readable.

---

## 10. Virtual Channels and Multiplexing

### 10.1 Virtual Channel IDs

Each publisher and subscriber can be assigned a `vchan_id` (virtual channel ID). Messages carry the publisher's `vchan_id` in the slot. Subscribers filter messages by vchan_id:

- `vchan_id == -1` on the subscriber means "receive all messages" (broadcast).
- `vchan_id == -1` on the publisher means "send to all subscribers".
- Otherwise, only subscribers with matching `vchan_id` (or -1) see the message.

### 10.2 Multiplexing (Mux)

A **mux** (multiplexer) allows multiple logical channels (virtual channels) to share a single physical channel's shared memory. This reduces the overhead of creating separate shared memory regions for many related channels.

- Set via `PublisherOptions::SetMux(mux_name)` and `SubscriberOptions::SetMux(mux_name)`.
- The server creates a `ChannelMultiplexer` for the mux. Each virtual channel within the mux shares the mux's CCB and buffers.
- Ordinals are tracked per virtual channel via `OrdinalAccumulator`.
- Subscriber triggers include mux-level triggers so mux subscribers receive notifications for all virtual channels.

---

## 11. Activation

### 11.1 Purpose

Activation messages signal that a channel (or virtual channel) is "ready."
This is primarily used with reliable subscribers to indicate when a publisher
has been set up and is ready to send.

### 11.2 Flow

1. **Reliable publisher creation:** Automatically sends an activation message via `ActivateReliableChannel`.
2. **Unreliable publisher with `activate` option:** Sends an activation message via `ActivateChannel`.
3. The activation message has the `kMessageActivate` flag set in the prefix and the `kMessageIsActivation` flag in the slot.

### 11.3 Subscriber Handling

- If `pass_activation` is false (default): Activation messages are silently consumed. The ordinal is remembered and the slot reference is released. `ReadMessage` automatically skips to the next message.
- If `pass_activation` is true: The activation message is delivered to the user with `msg.is_activation == true`.

---

## 12. Channel Type Matching

Publishers and subscribers can specify a `type` string. The server enforces:

- If a channel already has a type (set by the first publisher/subscriber with a non-empty type), all subsequent publishers and subscribers must either specify the same type or leave it empty.
- Mismatched types result in an error from `CreatePublisher` or `CreateSubscriber`.
- The type is opaque to Subspace; it is purely for user-level consistency checks.

---

## 13. Polling and Waiting

### 13.1 Subscriber Polling

Each subscriber has a trigger file descriptor (pipe or eventfd). Publishers write to this fd when a new message is published. Subscribers use `GetPollFd()` to get a `struct pollfd` for use with `poll()` or `epoll()`, or call `Wait()` to block until a message is available.

**Important:** After `Wait()` returns, the subscriber should read **all** available messages before waiting again. The trigger fd may not be re-armed until all messages are consumed.

### 13.2 Publisher Polling (Reliable Only)

Reliable publishers have a trigger fd that becomes readable when a subscriber releases a slot. `Publisher::Wait()` blocks on this fd. For unreliable publishers, the fd is -1 and `Wait()` returns an error.

### 13.3 Dual-fd Wait

Both `Publisher::Wait(fd, ...)` and `Subscriber::Wait(fd, ...)` accept an additional file descriptor. The call returns the fd that became ready, allowing the caller to multiplex between message availability and an external event (e.g., a shutdown signal).

---

## 14. Server Crash Behavior

### 14.1 No Automatic Reconnection

The client does **not** implement automatic reconnection. If the server
crashes or the Unix socket is closed:

1. The next RPC attempt (`SendRequestReceiveResponse`) fails.
2. The client closes the socket.
3. All subsequent operations that require server communication (`CreatePublisher`, `CreateSubscriber`, `ReloadSubscriber`, etc.) return an error.

### 14.2 Shared Memory Survives

After a server crash, existing shared memory mappings remain valid. Publishers
and subscribers that have already been created can continue to operate on
their mapped shared memory regions:

- **Publishers** can continue to publish messages (slot claiming, prefix writing, and subscriber notification all use shared memory and do not require the server).
- **Subscribers** can continue to read messages from slots that are already in shared memory.

However:

- **No new publishers or subscribers can be created** (requires server RPC).
- **Placeholder subscribers cannot be reloaded** (requires server RPC to discover publishers).
- **Channel resize fails** if it requires creating new shared memory (the `shm_open` call may succeed since it's an OS operation, but the server cannot track the new buffer).
- **Trigger fd distribution stops.** New subscribers created after reconnection to a new server will not have trigger fds for existing publishers, and vice versa.
- **SCB counter-based reload detection** may produce stale results since the server is no longer updating counters.

### 14.3 Cleanup

Shared memory files (in `/dev/shm/` on Linux) are not automatically cleaned up when the server crashes. They persist until:

- The server is restarted and calls cleanup.
- The files are manually deleted.
- The system is rebooted.

---

## 15. Race Conditions and Concurrency

### 15.1 Slot Ownership Transfer (Publisher → Subscribers)

**Critical path:**

1. Publisher writes all prefix fields and payload data.
2. Publisher stores the new `refs` value with `memory_order_release`:
   ```cpp
   slot->refs.store(BuildRefsBitField(ordinal, vchan_id, 0), memory_order_release);
   ```
3. Subscriber loads `refs` with `memory_order_relaxed` and checks that `pub_owned` is clear.

The `memory_order_release` on the publisher's store ensures that all payload
and prefix writes are visible to any thread that observes the new `refs` value.
However, the subscriber uses `memory_order_relaxed` for its load, which means:

- On x86/x86-64 (TSO architecture), this is safe because all stores are
  visible in program order to other cores.
- On weakly-ordered architectures (e.g., ARM), the subscriber may
  theoretically observe the cleared `pub_owned` bit before seeing the payload
  writes. In practice, the `compare_exchange_weak` in `AtomicIncRefCount`
  (also `memory_order_relaxed`) may need to be strengthened to
  `memory_order_acquire` for correctness on non-TSO platforms.

**Potential race:** If a subscriber reads a slot's `refs` and observes the new
ordinal but the prefix/payload data has not yet propagated (on non-x86), it
could read stale data. This is mitigated on x86 by TSO guarantees.

### 15.2 Multiple Publishers Claiming Slots

Multiple publishers race to claim free slots using CAS:

```cpp
slot->refs.compare_exchange_weak(expected, kPubOwned | owner, memory_order_relaxed);
```

If the CAS fails (another publisher claimed the slot first), the publisher
retries with a different slot. This is a standard lock-free pattern.
Starvation is possible under extreme contention but unlikely in practice.

### 15.3 Multiple Subscribers Claiming the Same Slot

Multiple subscribers increment the ref count on the same slot using CAS in
`AtomicIncRefCount`. The CAS loop retries until successful. Each subscriber
checks:

- `pub_owned` is clear.
- The ordinal in `refs` matches the expected ordinal.
- The `vchan_id` in `refs` matches.

If any check fails, the subscriber skips the slot.

### 15.4 Subscriber Release and Retirement

When a subscriber decrements the ref count to zero and the `retired_refs`
count equals `NumSubscribers(vchan_id)`, the slot is marked as retired. This
check is performed inside the CAS loop. Race condition: if two subscribers
release simultaneously, only one will see `refs == 0` and trigger retirement.
This is correct because the CAS serializes the updates.

### 15.5 Buffer Resize Race

When a publisher resizes the channel:

1. It creates a new shared memory buffer and updates `ccb_->num_buffers` via CAS.
2. Subscribers detect the new buffer count and call `AttachBuffers` to map it.
3. Between the CAS and the subscriber's attachment, a slot may reference the new buffer index but the subscriber hasn't mapped it yet.

This is handled by the **embargo** mechanism: if `ValidateSlotBuffer` fails
(slot references an unmapped buffer), the slot is added to `embargoed_slots_`
and skipped. The subscriber retries on the next read, by which time the buffer
should be mapped.

### 15.6 Ordinal Wrap

The ordinal stored in the `refs` bitfield is only 23 bits wide (0–8,388,607).
The full ordinal is a `uint64_t` stored in the `MessageSlot::ordinal` field.
The 23-bit value in `refs` is used as a CAS guard to detect slot reuse. If
more than 8 million messages are published to a channel without all subscribers
advancing, ordinal aliasing could theoretically allow a subscriber to mistake
a recycled slot for the expected one. In practice, this requires all slots to
be recycled multiple times while a subscriber is stalled.

### 15.7 Dropped Message Detection

Dropped messages are detected by ordinal gaps. The `OrdinalTracker` is a
per-virtual-channel ring buffer that records seen ordinals. When a new message's
ordinal is not contiguous with the last seen ordinal, the gap is reported as
dropped messages. This detection is **best-effort**: if messages wrap around
the ring buffer, old ordinals may be evicted and false drops could be reported.

### 15.8 Trigger Fd Races

The trigger fd (pipe/eventfd) is a level-triggered notification. The publisher
writes to it after publishing; the subscriber clears it before scanning for
messages. If a message is published between the clear and the scan, the
subscriber will find it during the scan. If a message is published after the
scan completes, the trigger fd will be readable and the next `Wait()` will
return immediately.

However, the subscriber's `ClearPollFd` (draining the trigger) and the scan
are not atomic. There is a window where:

1. Subscriber clears the trigger.
2. Publisher publishes and writes to the trigger.
3. Subscriber scans and finds the message.
4. Subscriber calls `Wait()` — the trigger is still set from step 2, so it returns immediately but there is no new message.

This is benign (a spurious wakeup) but means callers should always handle
`ReadMessage` returning an empty message after `Wait()`.

---

## 16. Error Handling Summary

### Publisher Errors

| Operation | Error Condition | Behavior |
|-----------|----------------|----------|
| `CreatePublisher` | Server error (type mismatch, size limits) | Returns error status |
| `GetMessageBuffer` (unreliable) | All slots exhausted | Returns error (fatal) |
| `GetMessageBuffer` (reliable) | No free slot | Returns `nullptr`; caller should wait |
| `GetMessageBuffer` (reliable) | No subscribers | Returns `nullptr` |
| `GetMessageBuffer` | max_size > slot_size, fixed_size | Returns error |
| `PublishMessage` | message_size == 0 | Returns error |
| `PublishMessage` | On-send callback fails | Returns error |
| `Wait` (unreliable) | N/A | Returns error (only valid for reliable) |
| `Wait` (reliable) | Timeout | Returns error |

### Subscriber Errors

| Operation | Error Condition | Behavior |
|-----------|----------------|----------|
| `CreateSubscriber` | Server error (type mismatch) | Returns error status |
| `CreateSubscriber` | max_active_messages < 1 | Returns error |
| `ReadMessage` | No message available | Returns `Message` with `length == 0` |
| `ReadMessage` | Checksum error, pass_checksum_errors=false | Returns error |
| `ReadMessage` | Checksum error, pass_checksum_errors=true | Returns message with `checksum_error == true` |
| `ReadMessage` | Placeholder, server disconnected | Returns error |
| `Wait` | Timeout | Returns error |
| `AddActiveMessage` | At max_active_messages | Returns false |

---

## 17. Bridge Support

Publishers and subscribers can be created in **bridge** mode for server-to-server message forwarding:

- **Bridge publisher:** Uses `PublishMessageWithPrefix` to forward messages that already have a complete prefix (preserving the original ordinal, timestamp, etc.).
- **Bridge subscriber:** Maps buffers as read-write (via `read_write` option) so the prefix's `padding` field can be used for socket framing.

Bridge subscribers call `ClearActiveMessage()` immediately after reading so that slots are retired without waiting for the user to drop the `Message` object.

---

## 18. Statistics and Counters

### Channel Counters (from SCB)

- `num_pub_updates` / `num_sub_updates`: Incremented when publishers/subscribers are added or removed. Used by subscribers to detect configuration changes and reload.
- `num_pubs` / `num_reliable_pubs` / `num_subs` / `num_reliable_subs`: Current counts.
- `num_resizes`: Number of channel resizes.

### Channel Statistics (from CCB)

- `total_bytes`: Total bytes published.
- `total_messages`: Total messages published.
- `max_message_size`: Largest message seen.
- `total_drops`: Total messages dropped by unreliable publishers.

### Virtual Memory Usage

`GetVirtualMemoryUsage()` returns the total virtual memory mapped for the channel (CCB + BCB + SCB + all buffers).

---

## 19. Configuration Limits

| Parameter | Maximum | Enforced By |
|-----------|---------|-------------|
| `checksum_size` | 65535 (0xFFFF) | Server |
| `metadata_size` | 65535 (0xFFFF) | Server |
| `num_slots` | 1024 (kMaxBuffers) | Server |
| `max_active_messages` | num_slots | Subscriber |
| `vchan_id` | 1023 | 10-bit field |
| Subscribers per channel | 1024 (kMaxSlotOwners) | CCB bitset |
| Channels per session | 1024 (kMaxChannels) | SCB |
| Channel name length | 64 (kMaxChannelName) | CCB |
