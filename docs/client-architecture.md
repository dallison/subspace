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
- **Message Buffers** — the actual data. Stored in `/dev/shm/` (Linux) or POSIX shared memory. Publishers map read-write, subscribers read-only. Buffers can grow dynamically.

## Publishing Flow

1. **Create publisher** — client sends an RPC to the server, which allocates shared memory and returns file descriptors for the CCB, buffers, and trigger FDs.
2. **`GetMessageBuffer(size)`** — returns a pointer into a shared memory slot. If the message is larger than the current slot size, the channel auto-resizes.
3. **User writes data** directly into the buffer (zero-copy).
4. **`PublishMessage(length)`** — writes a `MessagePrefix` (size, ordinal, timestamp, optional checksum), marks the slot as available, and triggers subscriber file descriptors to wake them up.

## Subscribing Flow

1. **Create subscriber** — similar RPC to the server, gets back FDs for shared memory and a trigger FD.
2. **`ReadMessage()`** — scans the `available_slots` bitset in the CCB, finds the next unseen ordinal, atomically increments the slot's reference count, and returns a pointer directly into shared memory (zero-copy read).
3. **Message lifetime** — the returned `Message` object holds a reference count on the slot. The slot can't be reused by a publisher until all subscribers release it.

## Reliable vs. Unreliable

- **Unreliable** — publisher always has a slot, may overwrite old unread messages (subscribers detect dropped messages via ordinal gaps).
- **Reliable** — publisher waits for a free slot, guaranteeing no message loss.

## Key Class Hierarchy

```
Client (public API, copyable)
  └── ClientImpl (core implementation)
        ├── Publisher → PublisherImpl → ClientChannel → Channel
        └── Subscriber → SubscriberImpl → ClientChannel → Channel
```

## C Wrapper (`c_client/`)

A thin C layer over the C++ API using opaque `void*` pointers and thread-local error strings. Functions like `subspace_create_publisher()`, `subspace_publish_message()`, and `subspace_read_message()` mirror the C++ API with C-style naming. Messages must be explicitly freed with `subspace_free_message()`.

## Other Notable Features

- **Virtual channels** — multiplex logical channels on one physical channel.
- **Coroutine support** — the client can yield when waiting for slots/messages (uses the `co` library).
- **Checksums** — optional message integrity verification.
- **Bridging** — forwards channels between servers over TCP for cross-machine communication.

## Summary

After the initial setup handshake with the server, all message passing is just reads and writes to shared memory plus lightweight FD triggers for notification — no context switches through a broker.
