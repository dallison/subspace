# Subspace Client Architecture

## Overview

Subspace is a **shared-memory pub/sub IPC system**. The key insight is that the server is only involved in setup — actual message transfer happens directly through shared memory with no server round-trips, which is what gives it sub-microsecond latency.

## Connection Setup

1. The client connects to the server via a **Unix Domain Socket** (default `/tmp/subspace`).
2. It sends an `Init` protobuf request.
3. The server responds with a session ID and a file descriptor for the **System Control Block (SCB)** — a shared memory region the client maps into its address space.

## Three Shared Memory Regions

Each channel uses three shared memory structures:

- **System Control Block (SCB)** — one per system, tracks channel-level counters for reload detection.
- **Channel Control Block (CCB)** — one per channel, contains slot metadata, bitsets for available/retired/free slots, ordinals, and stats. Publishers map it read-write; subscribers map it read-only.
- **Message Buffers** — the actual data. Stored in `/dev/shm/` (Linux) or POSIX shared memory. Publishers map read-write, subscribers read-only. Buffers can grow dynamically. Split-buffer channels keep prefixes in these buffers but place payload slots in separate payload allocations.

## Publishing Flow

1. **Create publisher** — client sends an RPC to the server, which allocates shared memory and returns file descriptors for the CCB, buffers, and trigger FDs.
2. **`GetMessageBuffer(size)`** — returns a pointer into a shared memory slot. If the message is larger than the current slot size, the channel auto-resizes.
3. **User writes data** directly into the buffer (zero-copy).
4. **`PublishMessage(length)`** — writes a `MessagePrefix` (size, ordinal, timestamp, optional checksum and user metadata in the prefix area), marks the slot as available, and triggers subscriber file descriptors to wake them up.

Publishers that need multiple in-flight producer buffers can use the explicit
lease path instead. `AcquireBufferLease()` transfers ownership of a slot to a
`PublisherBufferLease` without holding the thread-safe client mutex.
`PublishBufferLease()` publishes it, while `ReleaseBufferLease()` discards it.
`ReclaimBufferLease(slot_id)` reacquires the exact slot reported by a retirement
fd. Lease IDs prevent stale tokens from operating on reused slots. See
[Publisher Buffer Leases](publisher-buffer-leases.md).

## Subscribing Flow

1. **Create subscriber** — similar RPC to the server, gets back FDs for shared memory and a trigger FD.
2. **`ReadMessage()`** — scans the `available_slots` bitset in the CCB, finds the next unseen ordinal, atomically increments the slot's reference count, and returns a pointer directly into shared memory (zero-copy read).
3. **Message lifetime** — the returned `Message` object holds a reference count on the slot. The slot can't be reused by a publisher until all subscribers release it.

## Reliable vs. Unreliable

- **Unreliable implicit publishing** — publisher reserves its next slot and may overwrite old unread messages (subscribers detect dropped messages via ordinal gaps).
- **Unreliable explicit leases** — slots are acquired on demand. Acquisition can return an empty lease when all reusable slots are actively referenced or already leased.
- **Reliable** — publisher waits for a free slot, guaranteeing no message loss.

The server prevents configured clients from exhausting an unreliable channel:

```text
sum(publisher.max_outstanding_slot_leases)
  + sum(subscriber.max_active_messages)
  <= num_slots - 1
```

Capacity is aggregated across virtual channels sharing a mux. The server also
enforces `max_subscribers`, a channel-level subscriber limit established by the
first subscriber.

## Key Class Hierarchy

```
Client (public API, copyable)
  └── ClientImpl (core implementation)
        ├── Publisher → PublisherImpl → ClientChannel → Channel
        └── Subscriber → SubscriberImpl → ClientChannel → Channel
```

## Split Buffers

Split-buffer channels separate the `MessagePrefix` from the payload bytes. The
prefix remains in regular Subspace shared memory so ordinals, flags, checksums,
metadata, and slot state use the same layout. Payload slots live in separate
allocations that are either built-in shared-memory objects or application-owned
allocator handles mapped through callbacks.

Publishers opt in when creating the channel. Subscribers learn the channel mode
from the server and provide mapping callbacks only when custom allocator handles
are used. The server validates compatible options and replicates split-buffer
metadata to the shadow process for crash recovery. See [Split Buffers](split-buffers.md).

## C Wrapper (`c_client/`)

A C ABI layer over the C++ API using opaque `void*` pointers, plain structs, and
thread-local error strings. Functions like `subspace_create_publisher()`,
`subspace_publish_message()`, and `subspace_read_message()` mirror the C++ API
with C-style naming. Messages must be explicitly freed with
`subspace_free_message()`.

The C wrapper also exposes channel introspection, stats/counters, timeout and
fd-based waits, bulk reads, user metadata, checksum callbacks, split-buffer
callbacks and handle lookup, explicit publisher leases, transform callbacks,
and slot/prefix diagnostics.
See [C Client API](c-client.md).

## Other Notable Features

- **Virtual channels** — multiplex logical channels on one physical channel.
- **Coroutine support** — the client can yield when waiting for slots/messages (uses the `co` library).
- **Checksums** — optional message integrity verification using CRC32 by default, with support for arbitrary-sized checksums via callbacks.  The `checksum_size` and `metadata_size` publisher options control the prefix layout; user metadata can be attached to each message through `GetMetadata()`.  See [Checksums and User Metadata](checksums-and-metadata.md) for details.
- **Split buffers** — optional separate payload allocations for custom memory pools or handle-based mapping APIs. See [Split Buffers](split-buffers.md).
- **Publisher buffer leases** — own multiple unpublished slots, reclaim exact retired slots, and avoid holding the client mutex across an asynchronous producer pipeline. See [Publisher Buffer Leases](publisher-buffer-leases.md).
- **Subscriber limits** — `max_subscribers` gives channels a server-enforced subscriber cap.
- **Bridging** — forwards channels between servers over TCP for cross-machine communication.
- **Tunnel support** — the `for_tunnel` publisher/subscriber option marks messages with the `kMessageCrossMachine` flag in the `MessagePrefix`, allowing external tunnel processes to distinguish locally and remotely generated messages.

## Summary

After the initial setup handshake with the server, all message passing is just reads and writes to shared memory plus lightweight FD triggers for notification — no context switches through a broker.
