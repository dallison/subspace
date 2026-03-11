# Subspace Server Architecture

## Overview

The Subspace server is a **single-threaded, coroutine-based** process that manages shared-memory IPC channels. It handles client connections over Unix Domain Sockets and supports cross-machine communication via a UDP discovery and TCP bridging system.

The server is only involved in **setup and teardown** — once channels are established, message transfer happens directly through shared memory between clients with no server involvement.

## Server Startup and Main Loop

1. Creates a `co::CoroutineScheduler` (single-threaded event loop).
2. Instantiates the `Server` with configuration (socket path, discovery ports, interface, etc.).
3. Loads plugins if specified.
4. `Server::Run()`:
   - Creates a Unix Domain Socket listener at the configured path.
   - Allocates the System Control Block (SCB) in shared memory.
   - Sets up network discovery (if not in local-only mode).
   - Spawns coroutines:
     - **ListenerCoroutine** — accepts client connections.
     - **ChannelDirectoryCoroutine** — publishes channel directory updates.
     - **StatisticsCoroutine** — publishes channel statistics every 2 seconds.
     - **DiscoveryReceiverCoroutine** — receives UDP discovery messages.
     - **GratuitousAdvertiseCoroutine** — broadcasts channel advertisements every 5 seconds.
   - Runs the coroutine scheduler (blocks until shutdown).

## Client Connection Handling

1. **ListenerCoroutine** accepts connections on the Unix Domain Socket.
2. For each connection, a `ClientHandler` is created and stored.
3. A coroutine runs `ClientHandler::Run()`, which loops reading protobuf `Request` messages and dispatching them.
4. Responses (with file descriptors) are sent back via `SendFds()` using SCM_RIGHTS.

### Request Types

| Request | Description |
|---------|-------------|
| `Init` | Returns SCB file descriptor and session info |
| `CreatePublisher` | Creates/registers a publisher on a channel |
| `CreateSubscriber` | Creates/registers a subscriber on a channel |
| `RemovePublisher` / `RemoveSubscriber` | Removes users from a channel |
| `GetTriggers` | Returns trigger FDs for a channel |
| `GetChannelInfo` / `GetChannelStats` | Returns channel metadata |

## Channel Creation and Management

### Channel Types

- **Regular Channel** (`ServerChannel`) — standard shared-memory channel.
- **Multiplexer** (`ChannelMultiplexer`) — container for virtual channels that share storage.
- **Virtual Channel** (`VirtualChannel`) — a logical channel multiplexed on a parent mux, sharing the mux's CCB and buffers.

### Creation Flow

1. If a multiplexer is specified, find or create the mux, then create a `VirtualChannel` on it.
2. Otherwise, allocate a channel ID from a bitset, create a `ServerChannel`, and call `Allocate()` to create shared memory.
3. The channel is stored in the server's `channels_` map.

### Placeholder Channels

Subscribers can create channels before any publishers exist. These "placeholder" channels are remapped when the first publisher arrives, allowing subscriber-first initialization.

## Shared Memory Allocation

Each channel requires three shared memory regions, created via `shm_open()` (POSIX shared memory):

### System Control Block (SCB)

- One per server.
- Contains a `ChannelCounters` array (one entry per channel).
- Tracks publisher/subscriber counts and update numbers.
- Created once at server startup.

### Channel Control Block (CCB)

- One per channel.
- Contains: channel name, num_slots, ordinals, activation tracker.
- Variable-length: `MessageSlot` array + bitsets for retired/free/available slots.
- Size: `CcbSize(num_slots)` = base + slots + bitsets.

### Buffer Control Block (BCB)

- Tracks buffer references and sizes.
- Supports up to 1024 buffers per channel.

### Message Buffers

- Allocated on demand as channels grow.
- Named: `subspace_<session_id>.<channel_id>.<buffer_index>`.
- On Linux: stored in `/dev/shm/`.

### Cleanup

- On channel removal: shared memory is unlinked.
- On session end: all session files are removed.
- On server shutdown: all subspace files are cleaned up.

## Publisher and Subscriber Registration

### Adding a Publisher

1. Allocate a user ID from the channel's `user_ids_` bitset.
2. Create a `PublisherUser` with reliability/local/bridge flags.
3. Initialize a trigger FD for reliable publishers.
4. Update SCB counters.
5. Return the publisher ID and file descriptors (CCB, BCB, trigger, poll, retirement FDs).

### Adding a Subscriber

1. Allocate a user ID similarly.
2. Check capacity for unreliable channels: `slots_needed = num_pubs + num_subs + max_active_messages + 1`.
3. Create a `SubscriberUser` with a trigger FD.
4. Register the subscriber in the CCB.
5. Return file descriptors (CCB, BCB, trigger, all subscriber trigger FDs, retirement FDs).

## Bridge and Discovery System

Subspace supports cross-machine communication via UDP discovery and TCP data transfer.

### Discovery Protocol (UDP)

- **Query** — "Who publishes channel X?"
- **Advertise** — "I publish channel X."
- **Subscribe** — "I want to subscribe to channel X, connect to this address."

### Bridge Transmitter

Created when a remote subscriber requests a local channel:
1. Connects a TCP socket to the remote subscriber.
2. Sends a `Subscribed` message with channel details.
3. Creates a local subscriber to the channel.
4. Reads messages from the channel and forwards them over TCP.
5. Handles retirement notifications for reliable channels.

### Bridge Receiver

Created when the local server wants to subscribe to a remote channel:
1. Binds a TCP listener socket.
2. Sends a `Subscribe` discovery message.
3. Accepts a connection from the remote publisher.
4. Creates a local publisher.
5. Receives messages over TCP and publishes them locally.

### Gratuitous Advertise

Every 5 seconds, broadcasts an `Advertise` for all local channels so late-joining subscribers can discover them.

## Key Classes

```
Server
  ├── ClientHandler (one per client connection, runs in its own coroutine)
  ├── ServerChannel (one per channel)
  │     ├── PublisherUser / SubscriberUser (registered users)
  │     └── shared memory: SCB, CCB, BCB, buffers
  ├── ChannelMultiplexer (container for virtual channels)
  │     └── VirtualChannel (shares mux storage, own users)
  └── Plugin (loadable modules via dlopen)
```

## Design Decisions

- **Single-threaded with coroutines** — avoids locking and race conditions; cooperative multitasking via the `co` library. All blocking operations (accept, receive, send) yield to the scheduler.
- **File descriptor passing** — shared memory FDs are sent to clients via Unix socket SCM_RIGHTS messages, so clients map the same memory regions.
- **Discovery-based bridging** — UDP for lightweight discovery, TCP for reliable data transfer. Supports IPv4 and virtual addresses (VSOCK).
- **Capacity management** — unreliable channels check capacity before adding subscribers to prevent buffer exhaustion.
- **Retirement tracking** — for reliable channels, tracks message lifetimes across bridges to prevent premature slot reuse.
- **Plugin system** — loadable modules with callbacks (OnReady, OnNewChannel, OnNewPublisher, etc.) for extensibility.

## File Structure

```
server/
├── main.cc              # Entry point, command-line parsing
├── server.h/cc          # Main Server class
├── client_handler.h/cc  # Client connection handler
├── server_channel.h/cc  # Channel management
└── plugin.h             # Plugin interface
```
