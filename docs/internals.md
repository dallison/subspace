# How Subspace Works Internally

This is the storage and delivery model: what a channel is, where a message
payload lives, what a slot is, how the slot ring reuses those slots, how
resize grows the payload area, and how publishers and subscribers move a
message. Deeper behavior lives in the documents linked from each section.

If you are coming from a system that calls a stream a topic and one sample a
frame, a Subspace **channel** is the topic and one published **message** is
the frame. The payload of that message is the frame body.

## Main features

Subspace is a shared-memory publish/subscribe IPC system. After setup, message
bytes never pass through the server.

- Multiple publishers and multiple subscribers share one channel.
- The data path is lock-free shared memory. The server only creates channels,
  hands out file descriptors, and coordinates triggers.
- Messages are untyped. The application brings its own serialization. An
  optional type string is checked for consistency and is otherwise opaque.
- A channel is unreliable by default. Reliable publishers and subscribers add
  backpressure so a message is not overwritten while a reliable subscriber
  still needs it. See [Reliable Messages](reliable-messages.md).
- A subscriber can take the next message or the newest one.
- Publishers wake subscribers by writing a trigger file descriptor.
- Payload and prefix can live in one buffer, or the payload can live in a
  separate allocation. See [Split Buffers](split-buffers.md).
- A publisher can own several unpublished slots at once and reclaim an exact
  retired slot. See [Publisher Buffer Leases](publisher-buffer-leases.md).
- The server enforces publisher and subscriber limits and unreliable-channel
  capacity.
- The server publishes channel telemetry for participants, drops, and resizes.
  See [Channel Telemetry](channel-telemetry.md).
- Servers discover each other with UDP and bridge channels over TCP. A shadow
  process lets the server restart without losing shared-memory state. See
  [Shadow Process](shadow-process.md).
- A `Message` is reference-counted. `shared_ptr` and `weak_ptr` hold or observe
  that reference.
- Virtual channels can share one physical channel's memory.
- The same shared-memory layout is used by the C++, C, and Rust clients.

The C++ layout and races are specified in
[Client Design](client_design.md). The setup path is summarized in
[Client Architecture](client-architecture.md).

## What a channel is

A channel is a named stream of messages. The first publisher creates it and
chooses the layout: slot size, number of slots, checksum and metadata sizes,
whether it is fixed size, whether it uses split buffers, and which multiplexer
it belongs to. Later publishers must be compatible with that layout. The
server rejects a mismatched type string.

One channel is three shared-memory regions, plus the global system control
block:

| Region | Role |
| --- | --- |
| System control block (SCB) | One per server session. Per-channel counters so clients notice publishers, subscribers, and resizes. |
| Channel control block (CCB) | Slot metadata, ordinals, subscriber membership, and the free, retired, and available bitsets. |
| Buffer control block (BCB) | Reference count and size of each message buffer. |
| Message buffers | The prefix and payload bytes. |

The server creates these regions and passes their file descriptors to each
client over the Unix socket. Each client `mmap`s them. Publishers map the
buffers read-write. Subscribers map them read-only. After that, publishing and
reading are loads and stores in that memory, plus a write to a trigger file
descriptor.

A subscriber created before any publisher is a placeholder. It has no slots
yet. When a publisher appears, the subscriber reloads and maps the real
channel.

Several logical streams can share one physical channel through a multiplexer.
Each virtual channel has its own ordinal sequence. They share the same slots
and the same buffers. A subscriber with virtual channel id `-1` receives every
virtual channel on that mux.

The number of channels in one session is fixed at build time (`kMaxChannels`,
1024 by default). See [Channel Limit](max-channels.md).

## Where the payload is stored

The payload is stored in a shared-memory message buffer, in the slot the
publisher claimed for that message. The application writes the bytes there.
Subscribers read those same bytes. Nothing copies the payload through the
server or through a kernel socket buffer.

In the normal layout, one buffer is a contiguous array of slot regions. Each
region is a prefix followed by the payload area:

```text
buffer:
  slot 0: [MessagePrefix | checksum | metadata | pad | payload]
  slot 1: [MessagePrefix | checksum | metadata | pad | payload]
  ...
  slot N-1: [MessagePrefix | checksum | metadata | pad | payload]
```

The prefix is at least 64 bytes and grows, still aligned to 64 bytes, when the
channel has a larger checksum or user metadata area. The payload area is the
channel's slot size, also aligned to 64 bytes. The address of slot `i`'s
payload is:

```text
buffer_base + i * (prefix_size + slot_size) + prefix_size
```

`MessagePrefix` records the payload length (`message_size`), the ordinal, the
`CLOCK_MONOTONIC` timestamp, flags, the virtual channel id, and the checksum
and metadata sizes. The checksum and user metadata sit in the prefix area,
before the payload. See [Checksums and User Metadata](checksums-and-metadata.md).

On a split-buffer channel the prefix stays in this buffer and the payload
bytes live in a separate allocation, one payload region per slot. The
subscriber still receives a pointer to those payload bytes. See
[Split Buffers](split-buffers.md).

## What a slot is

A slot is one storage cell for one current message.

The channel is created with a fixed `num_slots`. That count does not change
for the life of the channel. Slot ids run from `0` to `num_slots - 1`.

Each slot has two parts:

1. A `MessageSlot` in the channel control block. This is metadata: the atomic
   reference word, the full ordinal, the message size, the slot id, which
   buffer generation holds the bytes (`buffer_index`), the virtual channel id,
   which subscribers own it, the timestamp, and flags.
2. The byte region in a message buffer: prefix plus payload capacity.

One published message occupies one slot. The slot holds that message until a
publisher claims the slot again and overwrites it. While the message is still
in the slot, a subscriber can read it, including older messages that have not
yet been reused (`FindMessage` searches the slots that are still live). When
the slot is claimed for a new message, the previous payload is gone.

The 64-bit `refs` word is the lock-free ownership record. It packs the
subscriber reference count, the reliable reference count, the retired-subscriber
count, the virtual channel id, the low bits of the ordinal, and a bit that is
set while a publisher owns the slot for writing. Publishers and subscribers
transfer ownership with a compare-and-swap on this word. The publisher writes
the prefix and payload first, then stores `refs` with release ordering so a
subscriber that observes the new word also observes the bytes.

So: one slot, one stored message, payload included. The slot is not a pointer
to a frame kept somewhere else, except on a split-buffer channel, where the
payload bytes are in the matching payload allocation and the slot still names
that one message.

## Slots, buffers, and the ring

The message buffer stores the slot byte regions. The channel control block
stores the `MessageSlot` records that describe them. Together they are the
channel's ring: a fixed set of slots that publishers reuse.

There is no second container that holds slots which then hold frames. The
payload already lives in the slot. Publishing the next message after every
slot is in use means claiming an existing slot and writing a new prefix and
payload over the old one.

Reuse order is what makes it a ring:

- Unused slots start in the free set.
- After every current subscriber has released a message, its slot moves to the
  retired set and can be claimed again.
- With subscribers attached, an unreliable publisher prefers a recently retired
  slot so it keeps rewriting a small, cache-hot set.
- With nobody subscribed, publishing fills the free slots first and then
  reclaims the oldest message. The slots become a rolling window of recent
  messages for a subscriber that attaches later.
- If every retired and free slot is still referenced, an unreliable publisher
  reclaims the oldest slot whose reference count is zero. That overwrites a
  message some subscriber has not read. The subscriber later sees a gap in
  ordinals.

Two smaller rings store identifiers, not payloads:

- Each subscriber has an `InPlaceSlotQueue` in the channel control block. A
  publisher pushes a slot id and ordinal when it publishes. An unreliable
  subscriber pops that queue to find the next message. The per-subscriber
  available-slot bitset remains the fallback and is still authoritative for
  newest-message reads.
- Drop detection keeps a short per-virtual-channel history of ordinals the
  subscriber has already seen. A gap in that history is reported as dropped
  messages.

## How a publisher sends a message

`CreatePublisher` asks the server to create or attach the channel. The server
returns file descriptors for the control blocks, the buffers, and the
subscriber trigger fds. The client maps them.

The usual send is two calls:

```cpp
void *buf = *publisher.GetMessageBuffer(message_size);
std::memcpy(buf, data, message_size);
publisher.PublishMessage(message_size);
```

`GetMessageBuffer` claims a slot and sets the publisher-owned bit in `refs`.
The returned pointer is the payload area inside the shared-memory buffer. If
`message_size` is larger than the current slot size, the channel resizes
first, as described below. The application writes the payload, and optionally
the metadata span, directly into that memory.

`PublishMessage` then:

1. Takes the next ordinal for this virtual channel.
2. Fills the prefix: size, ordinal, timestamp, flags, virtual channel id,
   checksum size, metadata size, and slot id.
3. Computes the checksum over the prefix fields, the metadata, and the
   payload when checksums are enabled.
4. Stores `refs` with the publisher-owned bit clear. That publish is the
   moment the message becomes visible.
5. Sets the slot's bit in each matching subscriber's available-slot set and
   pushes the slot id onto that subscriber's queue.
6. Writes the subscriber trigger file descriptors so a waiting subscriber
   wakes up.

An unreliable publisher then immediately claims another slot, so the next
`GetMessageBuffer` is usually already holding a buffer.

A publisher that must hold several unpublished buffers uses
`AcquireBufferLease` instead of the implicit current slot. See
[Publisher Buffer Leases](publisher-buffer-leases.md).

## How a subscriber picks up a message

`CreateSubscriber` attaches to the same shared memory and receives a trigger
file descriptor. `Wait` or `poll` on that descriptor blocks until a publisher
has written it. After `Wait` returns, read every available message before
waiting again. `ReadMessage` can also return an empty message after a wakeup,
so the caller retries.

`ReadMessage` selects a slot and claims it:

- `kReadNext` pops the subscriber's slot-id queue. If that queue overflowed,
  the subscriber scans its available-slot bitset in ordinal order.
- `kReadNewest` takes the highest ordinal still visible in the bitset.

Claiming increments `refs` with a compare-and-swap that also checks the
ordinal and virtual channel id. If another publisher reused the slot first,
the compare-and-swap fails and the subscriber looks again.

The returned `Message` holds a pointer straight at the payload in shared
memory, plus the length, ordinal, timestamp, slot id, and virtual channel id.
The `Message` owns a reference. Copies share it. When the last `Message` is
destroyed, the reference is dropped. When every subscriber that had to see the
slot has released it, the slot is retired and its id can be written to a
publisher retirement pipe.

Activation messages mark a virtual channel ready. By default `ReadMessage`
consumes them internally and returns the next real message.

A subscriber created with `max_active_messages` can hold only that many
messages at once. Extra `Message` objects keep their slots alive until they
are destroyed, which is what makes backpressure work on a reliable channel.

## Reliable and unreliable

Reliability is a property of the publisher and of the subscriber, not a
separate kind of storage. Both kinds use the same slots and the same buffers.

| | Unreliable | Reliable |
| --- | --- | --- |
| Publisher | May reuse the oldest unreferenced slot and overwrite a message nobody has read. | Claims only a slot that reliable subscribers have finished with. `GetMessageBuffer` returns `nullptr` when none is available. |
| Subscriber | Sees ordinal gaps when it was too slow. The dropped-message callback reports the gap. | Holds a reference on its current message until it advances, clears it, or is destroyed, so the publisher cannot overwrite it. |
| Progress | The publisher keeps going. Slow subscribers lose messages. | The publisher waits. Throughput is limited by the slowest reliable subscriber. |

The guarantee holds between a reliable publisher and a reliable subscriber. An
unreliable subscriber does not make a reliable publisher wait. A reliable
subscriber can still miss messages from an unreliable publisher.

A reliable publisher does not block inside `GetMessageBuffer`. A `nullptr`
buffer means "retry after `Wait`". `Wait` wakes when a subscriber releases a
slot. Another publisher may take that slot first, so the retry still handles
`nullptr`.

The slot count is the amount of backlog a reliable publisher can build before
it stalls. More slots absorb a burst. They do not help a subscriber that is
always slower than the publisher.

On an unreliable channel the server also refuses a combination of publishers
and subscribers that would fill every slot by construction:

```text
sum(publisher.max_outstanding_slot_leases)
  + sum(subscriber.max_active_messages)
  <= num_slots - 1
```

See [Reliable Messages](reliable-messages.md).

## How resize works

Resize changes the payload capacity of each slot. It does not change
`num_slots`.

`GetMessageBuffer(max_size)` resizes when `max_size` is larger than the
current slot size. A channel created as fixed size refuses. A channel with
`max_slot_size` set refuses to grow past that cap. A registered resize
callback can also reject the growth. The requested size is rounded up to 64
bytes. The growth step depends on the current size: double up to 4 KB, 1.5×
up to 64 KB, 1.25× up to 1 MB, and 1.125× after that.

The resize allocates a new shared-memory buffer. That buffer has the same
number of slot regions, each with a larger payload area. The channel control
block's buffer count is incremented. Each `MessageSlot::buffer_index` says
which buffer generation currently holds that slot. The next time a publisher
claims the slot, it moves the slot onto the newest buffer and drops a
reference on the old one.

Subscribers notice the new buffer count and map it. A slot that already
points at a buffer the subscriber has not mapped yet is skipped and retried
on the next read. An old buffer is unmapped when its reference count reaches
zero, which is after every slot has moved off it and every subscriber has
dropped the messages that still lived there.

The slot-size field is 64-bit inside shared memory. The C and C++ APIs expose
it as 32-bit unless the build sets `SUBSPACE_64BIT_SLOT_SIZE`. See
[Slot Sizes](slot-sizes.md).
