# Subspace Rust Client

## Overview

The Subspace Rust client provides a native Rust API for the Subspace shared-memory
pub/sub IPC system. It communicates with the Subspace server over a Unix domain
socket for setup, then reads and writes messages directly through shared memory
with zero-copy semantics — the same architecture as the C++ client.

The client is thread-safe: `Client`, `Publisher`, and `Subscriber` use
`Arc<Mutex<...>>` internally and can be shared across threads.

## Getting Started

### Connecting to the Server

```rust
use subspace_client::{Client, PublisherOptions, SubscriberOptions, ReadMode};

let client = Client::new("/tmp/subspace", "my_app").unwrap();
```

The first argument is the server's Unix socket path. The second is a client
name used for identification in server logs.

### Publishing Messages

```rust
let opts = PublisherOptions::new()
    .set_slot_size(256)
    .set_num_slots(10);

let publisher = client.create_publisher("my_channel", &opts).unwrap();

// Get a buffer to write into (zero-copy).
let (buf_ptr, capacity) = publisher.get_message_buffer(256).unwrap().unwrap();

// Write data directly into shared memory.
let payload = b"hello world";
unsafe {
    std::ptr::copy_nonoverlapping(payload.as_ptr(), buf_ptr, payload.len());
}

// Publish — notifies all subscribers.
let msg = publisher.publish_message(payload.len() as i64).unwrap();
```

`get_message_buffer` returns `Ok(None)` when a reliable publisher has no free
slots. In that case, call `publisher.wait(timeout_ms)` to block until a slot
becomes available.

### Subscribing to Messages

```rust
let opts = SubscriberOptions::new();
let subscriber = client.create_subscriber("my_channel", &opts).unwrap();

// Wait for a message (with 5-second timeout).
subscriber.wait(Some(5000)).unwrap();

// Read the next message (zero-copy pointer into shared memory).
let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
if !msg.is_empty() {
    let data = unsafe { msg.as_slice() };
    println!("received {} bytes: {:?}", msg.length, data);
}
```

When no messages are available, `read_message` returns an empty message
(`msg.length == 0`). To drain all available messages:

```rust
loop {
    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    if msg.is_empty() {
        break;
    }
    // process msg...
}
```

### Read Modes

- **`ReadMode::ReadNext`** — returns the next unread message in order.
- **`ReadMode::ReadNewest`** — skips to the latest message, discarding older ones.

## Publisher Options

| Option | Default | Description |
|--------|---------|-------------|
| `slot_size` | 0 | Message buffer size in bytes (aligned to 64). |
| `num_slots` | 0 | Number of message slots in the channel. |
| `local` | false | Channel is local-only (not bridged). |
| `reliable` | false | Reliable delivery — no overwrites, publisher blocks when full. |
| `bridge` | false | Publisher is used for server-to-server bridging. |
| `for_tunnel` | false | Publisher is used by an external tunnel process. |
| `fixed_size` | false | Prevent dynamic slot resizing. |
| `channel_type` | "" | Type string; subscribers must match to connect. |
| `activate` | false | Send an activation message immediately. |
| `mux` | "" | Multiplexer name for virtual channels. |
| `vchan_id` | -1 | Virtual channel ID within a multiplexer. |
| `notify_retirement` | false | Enable retirement notifications via a pipe FD. |
| `checksum` | false | Compute CRC32 checksums on published messages. |
| `checksum_size` | 4 | Bytes reserved for checksums (default: 4 for CRC32). |
| `metadata_size` | 0 | Bytes reserved in each message prefix for user metadata. |

All options use a builder pattern:

```rust
let opts = PublisherOptions::new()
    .set_slot_size(1024)
    .set_num_slots(4)
    .set_reliable(true)
    .set_checksum(true);
```

## Subscriber Options

| Option | Default | Description |
|--------|---------|-------------|
| `reliable` | false | Reliable mode — slots are held until the subscriber releases them. |
| `bridge` | false | Subscriber is used for server-to-server bridging. |
| `for_tunnel` | false | Subscriber is used by an external tunnel process. |
| `channel_type` | "" | Must match the publisher's type string. |
| `max_active_messages` | 1 | Max messages a subscriber can hold simultaneously. |
| `log_dropped_messages` | true | Log warnings when messages are dropped. |
| `pass_activation` | false | Deliver channel activation messages to the subscriber. |
| `read_write` | false | Map shared memory as read-write (default is read-only). |
| `mux` | "" | Multiplexer name for virtual channels. |
| `vchan_id` | -1 | Virtual channel filter (-1 = receive all). |
| `checksum` | false | Verify checksums on received messages. |
| `pass_checksum_errors` | false | Deliver messages with bad checksums (with `checksum_error = true`) instead of returning an error. |
| `keep_active_message` | false | Keep active message reference semantics. |

## Message Lifetime

Each `Message` holds an `Arc<ActiveMessage>` that keeps the underlying shared
memory slot alive. The slot is released (and can be reused by the publisher)
only when all `Message` references are dropped.

Cloning a `Message` shares the same slot reference — the slot is freed when the
last clone is dropped. For reliable channels, this is how backpressure works:
the publisher cannot overwrite a slot until every subscriber has dropped its
message.

## Reliable vs. Unreliable Channels

**Unreliable channels** (default): the publisher always has a slot available and
may overwrite old unread messages. Subscribers detect gaps via ordinal numbers
and can register a dropped-message callback.

**Reliable channels** (`reliable = true`): the publisher waits for a free slot,
guaranteeing no message loss. `get_message_buffer` returns `Ok(None)` when all
slots are in use; call `publisher.wait()` to block until one is freed.

## Waiting and Polling

Both `Publisher` and `Subscriber` expose `wait(timeout_ms)` for blocking waits
and `get_poll_fd()` for integration with external event loops:

```rust
use std::os::unix::io::RawFd;

let fd: RawFd = subscriber.get_poll_fd();
// Use fd with poll(), epoll(), or any async reactor.
```

`wait(None)` blocks indefinitely; `wait(Some(ms))` blocks up to the given
number of milliseconds.

## Checksums

Enable publisher-side checksums to protect message integrity:

```rust
let pub_opts = PublisherOptions::new()
    .set_slot_size(256)
    .set_num_slots(10)
    .set_checksum(true);

let sub_opts = SubscriberOptions::new()
    .set_checksum(true);
```

The default algorithm is CRC32 over the prefix fields, metadata, and payload,
using 4 bytes of storage. The checksum size can be changed with
`set_checksum_size` to accommodate larger or different checksum algorithms:

```rust
let pub_opts = PublisherOptions::new()
    .set_slot_size(256)
    .set_num_slots(10)
    .set_checksum(true)
    .set_checksum_size(20); // e.g. for a larger hash
```

The subscriber automatically learns the checksum size from the server response,
so only the publisher needs to configure it.

Custom checksum algorithms can be provided via callbacks:

```rust
publisher.set_checksum_callback(|spans: &[&[u8]], cksum: &mut [u8]| {
    // Write your checksum into cksum.
});

subscriber.set_checksum_callback(|spans: &[&[u8]], cksum: &mut [u8]| {
    // Compute expected checksum for verification.
});
```

## Metadata

Reserve space in the message prefix for per-message user metadata:

```rust
let opts = PublisherOptions::new()
    .set_slot_size(256)
    .set_num_slots(10)
    .set_metadata_size(8);

let publisher = client.create_publisher("meta_chan", &opts).unwrap();
publisher.set_metadata(&[1, 2, 3, 4, 5, 6, 7, 8]);

// On the subscriber side:
let metadata = subscriber.get_metadata();
```

## Retirement Notifications

When `notify_retirement` is enabled, the publisher receives a notification (via
a pipe FD) each time a slot is retired — meaning all subscribers have released
it:

```rust
let opts = PublisherOptions::new()
    .set_slot_size(256)
    .set_num_slots(10)
    .set_notify_retirement(true);

let publisher = client.create_publisher("ret_chan", &opts).unwrap();
let retirement_fd = publisher.get_retirement_fd();
// Poll retirement_fd to learn when slots are freed.
```

## Callbacks

### Dropped Message Callback

Called when a subscriber detects gaps in the ordinal sequence (unreliable
channels only):

```rust
subscriber.register_dropped_message_callback(|num_dropped| {
    eprintln!("dropped {} messages", num_dropped);
});
```

### Message Callback

Called for each message received via `process_all_messages`:

```rust
subscriber.register_message_callback(|msg: Message| {
    println!("got message: ordinal={}", msg.ordinal);
});

subscriber.process_all_messages(ReadMode::ReadNext).unwrap();
```

### Resize Callback

Called when a publisher dynamically resizes its slot buffer:

```rust
publisher.register_resize_callback(|old_size, new_size| -> bool {
    println!("resize from {} to {}", old_size, new_size);
    true // return false to reject the resize
});
```

## Channel Information

Query channel state from the server:

```rust
let info = client.get_channel_info("my_channel").unwrap();
println!("publishers: {}, subscribers: {}", info.num_publishers, info.num_subscribers);
println!("slot_size: {}, num_slots: {}", info.slot_size, info.num_slots);

let stats = client.get_channel_stats("my_channel").unwrap();
println!("total messages: {}, total bytes: {}", stats.total_messages, stats.total_bytes);

let all = client.get_all_channel_info().unwrap();
for ch in &all {
    println!("{}: {} pubs, {} subs", ch.channel_name, ch.num_publishers, ch.num_subscribers);
}
```

## Error Handling

All fallible operations return `Result<T, SubspaceError>`. The error variants
are:

| Variant | Description |
|---------|-------------|
| `Internal` | Unexpected internal error. |
| `InvalidArgument` | Bad parameter (e.g., `max_active_messages < 1`). |
| `NotConnected` | Client is not connected to the server. |
| `Timeout` | A wait operation timed out. |
| `ChecksumError` | Message checksum verification failed. |
| `Io` | Underlying I/O error. |
| `ProtobufEncode` / `ProtobufDecode` | Protobuf serialization error. |
| `Nix` | Unix system call error. |
| `ServerError` | Error returned by the Subspace server. |

## Building

The Rust client is built with Bazel:

```bash
# Build the library
bazelisk build //rust_client:subspace_client_rust

# Run the tests
bazelisk test //rust_client:client_test

# Run the latency test (manual)
bazelisk test //rust_client:latency_test
```

The build uses `prost` for protobuf code generation (via a `build.rs` script)
and communicates with the server using the same protobuf wire format as the C++
client.

## Architecture

The Rust client follows the same shared-memory architecture as the C++ client:

1. **Connection**: Unix domain socket to the server for control-plane RPCs.
2. **Shared memory**: Three regions per channel — SCB, CCB, and message buffers
   — mapped into the client's address space via file descriptors received over
   the socket (`SCM_RIGHTS`).
3. **Zero-copy I/O**: Publishers write directly into shared memory buffers;
   subscribers read from the same buffers. No data is copied through the server.
4. **Notifications**: Eventfds (Linux) are used to wake subscribers when new
   messages are published and to wake reliable publishers when slots are freed.

This means the Rust client achieves the same sub-microsecond message latency as
the C++ client for the data path, with the server only involved during channel
setup and teardown.
