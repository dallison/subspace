# Shadow Process: Crash Recovery for the Subspace Server

## Overview

The **shadow process** (`subspace_shadow`) is an optional companion to the Subspace server that maintains a real-time mirror of the server's channel database. If the server crashes and restarts, it reconnects to the shadow and recovers all channels, publishers, and subscribers — including their shared memory and event file descriptors — so that the shared memory regions remain valid and accessible to clients that reconnect.

The server supports up to **two shadow processes** — a **primary** and a **secondary**. During normal operation, state is replicated to both in parallel. On recovery, the server tries the primary first; if it's unavailable or has no valid state, it falls back to the secondary. If neither has valid data, the server starts fresh with an empty database.

Without any shadow, a server crash means all shared memory regions are orphaned and clients must re-establish channels from scratch. With one or two shadows, recovery is transparent: the new server instance picks up exactly where the old one left off (minus the client socket connections, which must be re-established).

## Architecture

```
 ┌──────────┐  ShadowEvent (protobuf + SCM_RIGHTS FDs)  ┌────────────────┐
 │           │ ─────────────────────────────────────────► │ Primary Shadow │
 │           │ ◄──── state dump on connect ────────────── │                │
 │  Server   │                                           └────────────────┘
 │           │  ShadowEvent (protobuf + SCM_RIGHTS FDs)  ┌──────────────────┐
 │           │ ─────────────────────────────────────────► │ Secondary Shadow │
 │           │ ◄──── state dump on connect ────────────── │                  │
 └──────────┘                                            └──────────────────┘
```

The server communicates with each shadow independently over a **Unix domain socket**. The server is the client (connects to the shadow). All messages use **protobuf** serialization. File descriptors for shared memory and event notification are transferred using **SCM_RIGHTS** ancillary messages.

Both shadows receive identical data in parallel. They are independent and do not communicate with each other.

All processes use `co::CoroutineScheduler` for single-threaded, non-blocking I/O.

## Running

### Start the shadow process(es)

```bash
# Primary shadow
subspace_shadow --socket /tmp/subspace_shadow --log_level info

# Optional secondary shadow (on a different socket)
subspace_shadow --socket /tmp/subspace_shadow2 --log_level info
```

Each shadow listens on the given Unix socket and waits for server connections.

### Start the server with shadow enabled

```bash
# Single shadow (primary only)
subspace_server --socket /tmp/subspace --shadow_socket /tmp/subspace_shadow

# Dual shadows (primary + secondary)
subspace_server --socket /tmp/subspace \
    --shadow_socket /tmp/subspace_shadow \
    --secondary_shadow_socket /tmp/subspace_shadow2
```

The `--shadow_socket` flag configures the primary shadow and `--secondary_shadow_socket` configures the secondary. Both are optional and can be used independently.

### Typical deployment

Start the shadow(s) first, then the server. If the server crashes, simply restart it with the same flags — it will automatically recover state from the primary shadow (or secondary if primary is unavailable).

```bash
# Terminal 1: primary shadow (long-lived, survives server crashes)
subspace_shadow --socket /tmp/subspace_shadow

# Terminal 2: secondary shadow (optional, provides redundancy)
subspace_shadow --socket /tmp/subspace_shadow2

# Terminal 3: server
subspace_server --socket /tmp/subspace \
    --shadow_socket /tmp/subspace_shadow \
    --secondary_shadow_socket /tmp/subspace_shadow2

# If the server crashes, just restart it:
subspace_server --socket /tmp/subspace \
    --shadow_socket /tmp/subspace_shadow \
    --secondary_shadow_socket /tmp/subspace_shadow2
# -> "Recovered 5 channels from primary shadow"
```

If a shadow is not running or unreachable when the server starts, the server logs a warning and continues. If neither shadow is reachable or has valid state, the server starts fresh.

## Protocol

All messages are wrapped in a `ShadowEvent` protobuf oneof. File descriptors accompany create events as a separate SCM_RIGHTS message immediately after the protobuf payload.

### Event types

| Event | Direction | FDs | Purpose |
|---|---|---|---|
| `ShadowStateDump` | Shadow -> Server | `[scb_fd]` if session_id != 0 | Header sent on new connection; contains the shadow's session ID and channel count |
| `ShadowCreateChannel` | Both | `[ccb_fd, bcb_fd]` | Channel metadata + shared memory FDs |
| `ShadowAddPublisher` | Both | `[poll_fd, trigger_fd, ...]` | Publisher metadata + notification FDs |
| `ShadowAddSubscriber` | Both | `[trigger_fd, poll_fd]` | Subscriber metadata + notification FDs |
| `ShadowStateDone` | Shadow -> Server | none | End-of-dump marker |
| `ShadowInit` | Server -> Shadow | `[scb_fd]` | New session; clears shadow state |
| `ShadowRemoveChannel` | Server -> Shadow | none | Channel removed |
| `ShadowRemovePublisher` | Server -> Shadow | none | Publisher removed |
| `ShadowRemoveSubscriber` | Server -> Shadow | none | Subscriber removed |

### Connection sequence

```
Server                                  Shadow
  │                                       │
  │─── connect() ────────────────────────►│
  │                                       │
  │◄── ShadowStateDump (session_id) ──────│  ← header + SCB fd (if state exists)
  │◄── ShadowCreateChannel + FDs ─────────│  ← for each channel
  │◄── ShadowAddPublisher + FDs ──────────│     ← for each publisher
  │◄── ShadowAddSubscriber + FDs ─────────│     ← for each subscriber
  │◄── ShadowStateDone ──────────────────-│  ← end marker
  │                                       │
  │    (server recovers if session_id≠0)  │
  │                                       │
  │─── ShadowInit (new session_id) ──────►│  ← resets shadow state
  │─── ShadowCreateChannel ... ──────────►│  ← re-replicates recovered channels
  │                                       │
  │    (normal event stream begins)       │
  │─── ShadowCreateChannel ─────────────►│
  │─── ShadowAddPublisher ──────────────►│
  │─── ShadowRemoveSubscriber ──────────►│
  │─── ...                                │
```

## How Recovery Works

### 1. Try primary shadow, then secondary

On startup, the server attempts recovery in strict priority order:

1. **Primary shadow**: Connect and call `ReceiveStateDump()`. If `session_id != 0` and the SCB FD is valid, recover from it.
2. **Secondary shadow** (only if primary failed or had no state): Connect and call `ReceiveStateDump()`. If valid, recover from it.
3. **Fresh start**: If neither shadow provided valid state, create a new empty SCB.

### 2. Shadow sends state dump

When the server connects, the shadow immediately sends its entire state before the server sends anything:

- A `ShadowStateDump` header with the previous session's ID and the SCB file descriptor.
- For each channel: a `ShadowCreateChannel` message with CCB and BCB file descriptors.
- For each publisher/subscriber on each channel: the corresponding Add message with trigger/poll FDs.
- A `ShadowStateDone` marker to signal the end.

If the shadow has no prior state (first startup), it sends `session_id=0` with no FDs.

### 3. Server reconstructs internal state

`Server::RecoverFromShadow()` iterates over the recovered channels:

- **Reserves the channel ID** in the server's ID bitmap (so new channels don't collide).
- **Constructs a `ServerChannel`** with the recovered metadata (name, slot size, num_slots, type, etc.).
- **Maps existing shared memory** via `ServerChannel::MapExisting()` — this maps the recovered CCB and BCB file descriptors into the server's address space without reinitializing them, preserving all message data.
- **Reconstructs publishers and subscribers** with `handler=nullptr` (orphaned users — they have no client connection yet). Their trigger FDs and retirement pipes are restored from the recovered file descriptors.
- **Inserts the channel** into the server's channel map.

### 4. Server re-replicates to both shadows

After recovery, the server connects to any shadow that wasn't tried during recovery (e.g. if it recovered from the primary, it still connects to the secondary). It then sends a `ShadowInit` with a new session ID to **each** connected shadow (which clears their old state), followed by all recovered channels and users. This ensures both shadows have identical, up-to-date state going forward.

### 5. Normal operation resumes

The server proceeds with its normal startup (spawning coroutines, accepting client connections). New client activity generates the usual `ShadowCreateChannel`, `ShadowAddPublisher`, etc. events, which are fanned out to both shadows.

## What Survives a Crash

| Survives | Does not survive |
|---|---|
| Shared memory regions (SCB, CCB, BCB) | Client TCP/socket connections |
| Channel metadata (name, slot config, type) | In-flight RPC state |
| Publisher/subscriber registration (IDs, flags) | Coroutine state |
| Message data already written to shared memory | Publisher/subscriber client handlers |
| Event notification FDs (trigger, poll, retirement) | |

After recovery, orphaned publishers and subscribers exist in the server's channel database with null client handlers. When clients reconnect, they re-register their existing publisher and subscriber IDs. The server reclaims the orphaned users by matching IDs, preserving the same publisher/subscriber IDs and file descriptors that are embedded in the CCB. The shared memory backing the channel is the same physical memory from before the crash.

## File Descriptor Lifecycle

The shadow process keeps all replicated file descriptors open for the lifetime of their associated objects. This is critical: Linux (and other Unix systems) only destroy shared memory mappings and eventfds when **all** file descriptors referencing them are closed. As long as the shadow holds a copy of each FD, the underlying kernel objects survive even if the server process crashes.

The FDs are transferred using `SCM_RIGHTS` ancillary messages on the Unix domain socket. The `toolbelt::FileDescriptor` wrapper uses reference-counted shared ownership, ensuring FDs aren't prematurely closed.

## Key Source Files

| File | Role |
|---|---|
| `shadow/shadow.h`, `shadow/shadow.cc` | Shadow process: event handling, state storage, state dump |
| `shadow/main.cc` | Shadow binary entry point |
| `server/shadow_replicator.h`, `server/shadow_replicator.cc` | Server side: sends events, receives state dumps |
| `server/server.cc` (`RecoverFromShadow`, `Run`) | Recovery logic in the server |
| `server/server_channel.h` (`MapExisting`) | Maps recovered shared memory FDs |
| `proto/subspace.proto` | Protobuf message definitions |
| `shadow/shadow_test.cc` | Integration tests including crash recovery |

## Dual Shadow Failover

When two shadows are configured, the recovery priority is always **primary first**. The server does not compare timestamps or state freshness between the two shadows — it simply trusts the primary if it has valid data.

If one shadow becomes unreachable during normal operation (its `SendEvent` fails and the connection closes), the other continues receiving events independently. There is no automatic reconnection to a failed shadow.

Both shadows are independently stateless from each other's perspective: they don't communicate or synchronize. After recovery, both are re-initialized with the same state via `ShadowInit` + re-replication.

## Limitations

- **No incremental recovery**: The shadow always sends its full state on each new connection. For a small number of channels this is fast; it could become a bottleneck with thousands of channels.
- **No automatic shadow reconnection**: If a shadow becomes unreachable during normal operation, the server does not attempt to reconnect. The other shadow (if configured) continues independently.
- **Shadow must outlive crashes**: If both shadows crash, their state is lost. The shadow is intentionally simple (no disk I/O, no complex logic) to minimize this risk. Running two shadows provides redundancy against this.
