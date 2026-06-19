# Asio Backend and vsock Bridging

Subspace's server (and the client paths it relies on) can be built against one
of two coroutine/networking backends, selected at **build time**:

| Backend | Coroutines | Sockets | Threading |
|---------|-----------|---------|-----------|
| `co` (default) | the `co` coroutine library | `cpp_toolbelt` sockets | single-threaded |
| `asio` | Boost.Asio stackful coroutines (`yield_context`) | Boost.Asio sockets (raw POSIX under the hood) | single- or multi-threaded |

Both backends are fully compiled-in alternatives behind a common
`subspace::async` abstraction (`common/async/`). The wire protocol, shared
memory layout, and client API are identical, so a `co` server and an `asio`
server interoperate, and existing client code does not change.

## Choosing a backend

The two backends are mutually exclusive and chosen by a single build flag that
sets the `SUBSPACE_CORO_BACKEND` macro (see `common/async/context.h`).

### Bazel

```bash
# Default (co) — no flag needed:
bazelisk build //server:subspace_server

# Asio:
bazelisk build //server:subspace_server --//:coro_backend=asio

# Tests under a given backend:
bazelisk test //client:bridge_test --//:coro_backend=asio
```

The flag is declared in the root `BUILD.bazel` as `//:coro_backend` with values
`co` and `asio`.

### CMake

```bash
cmake -DSUBSPACE_CORO_BACKEND=asio ...
```

When `asio` is selected, CMake fetches Boost (asio/coroutine/context) via
`FetchContent`; the default `co` build pulls in no Boost.

## Using the asio backend

### Client code is unchanged

The client public API still takes `const co::Coroutine *` where it always did,
so existing applications compile and run unmodified. Most clients are not
coroutine-driven at all; under the asio backend such a client simply runs in
**blocking mode** (its socket is left blocking, so it never needs a
`yield_context`). No code change is required to use an asio-backed client.

### Server: multi-threading

The asio server can run its `io_context` on several threads. This is the main
reason to choose the asio backend: bridge data-plane work (the per-channel
transmitter/receiver coroutines) can then run concurrently across threads.

```bash
subspace_server --num_asio_threads=4
```

- `--num_asio_threads` (asio builds only) sets how many threads run the
  `io_context`. Default `1` (inline on the calling thread, like the co backend).
- **No mutexes are used.** Client handlers, the Unix-domain-socket listener, and
  all discovery/control-plane coroutines that touch shared server state (the
  channel map, channel ids, discovery connections) are confined to a single
  Boost.Asio **strand**, so they stay serialized regardless of thread count.
- Only the bridge data-plane coroutines run directly on the `io_context`, so
  their socket I/O spreads across the thread pool.

Graceful shutdown on the asio backend uses Boost.Asio cancellation signals
(re-emitted on a short timer until every cancellable coroutine has exited),
which plays the role the per-coroutine interrupt fd plays on the co backend.

The `--num_asio_threads` flag does not exist on the co backend, which is always
single-threaded; passing a thread count to `Server::Run()` there is ignored.

## Enabling vsock bridges

By default, cross-machine bridges connect over **TCP**. On hosts where the two
servers are a VM host and guest (or two guests), you can instead carry the
per-channel bridge connections over **vsock** (`AF_VSOCK`). This is supported on
**both** backends.

> **Discovery still uses IP.** Only the per-channel bridge data connections (and
> the retirement-notification connections) move to vsock. The servers still find
> each other over UDP broadcast/unicast or `--tcp_discovery`. vsock is Linux-only.

### Command-line flags

```bash
# Enable vsock for bridges, auto-detecting this server's local CID:
subspace_server --vsock

# Or advertise a specific CID explicitly:
subspace_server --vsock --vsock_cid=3
```

- `--vsock` (bool, default `false`): use vsock instead of TCP for bridge and
  retirement connections.
- `--vsock_cid` (int, default `-1`): the local **context id (CID)** this server
  advertises to peers as the address they should connect to. `-1` auto-detects
  the local CID via `/dev/vsock`
  (`ioctl(IOCTL_VM_SOCKETS_GET_LOCAL_CID)`). Well-known CIDs:
  - `1` — local / loopback (`VMADDR_CID_LOCAL`)
  - `2` — host (`VMADDR_CID_HOST`)
  - `3+` — guest CIDs assigned by the hypervisor

Typically every server in the mesh is started with `--vsock`. A server on the
host advertises CID `2`; a guest advertises its hypervisor-assigned CID; both
are discovered automatically when `--vsock_cid` is left at `-1`.

### Programmatic API

When embedding the server, call:

```cpp
// advertise_cid is this server's own local CID (what peers dial to reach it).
server->SetVsockBridging(/*enable=*/true, /*advertise_cid=*/cid);
```

### How it works

The bridge connection is established as follows (unchanged in direction; only
the address family differs):

1. A server with a **subscriber** binds a bridge listener and sends a
   `Discovery.Subscribe` message (over IP discovery) telling the publisher side
   where to connect.
2. The server with the **publisher** connects back to that address and streams
   messages. Retirement notifications flow the other way.

To make this work across address families, the bridge endpoint address carried
in the discovery protocol is **self-describing**: `ChannelAddress` has a
`family` field (`FAMILY_INET` or `FAMILY_VSOCK`). The receiving server parses
the address according to that field rather than assuming its own local address
type, so the two servers do not have to be configured identically and older
peers (which never set the field, defaulting to `FAMILY_INET`) remain
compatible.

When `--vsock` is set:

- the bridge listener binds to `(VMADDR_CID_ANY, <ephemeral port>)`, accepting
  connections to any of the host's local CIDs;
- the advertised endpoint uses `--vsock_cid` (the real local CID) and
  `FAMILY_VSOCK`;
- the peer connects over `AF_VSOCK` to that CID and port.

### Testing

`client/bridge_test.cc` contains `VsockBridgeTest.BridgeOverVsockLoopback`, an
end-to-end test that bridges a channel between two in-process servers over vsock
using the loopback CID (`VMADDR_CID_LOCAL`). It is compiled only on Linux and
skips itself at runtime unless vsock loopback is usable, so it is a no-op
elsewhere. To run it on a Linux box (loading the loopback module if needed):

```bash
sudo modprobe vsock_loopback   # if not already loaded
bazelisk test //client:bridge_test --test_filter='VsockBridgeTest.*' --test_output=all
# or under the asio backend:
bazelisk test //client:bridge_test --//:coro_backend=asio --test_filter='VsockBridgeTest.*' --test_output=all
```

### Limitations

- vsock requires Linux with vsock support (e.g. the `vhost_vsock` /
  `vsock_loopback` modules). It is a no-op build elsewhere and `--vsock` will
  fail to auto-detect a CID off-Linux (pass `--vsock_cid` explicitly if you have
  a reason to).
- Discovery is never carried over vsock; the servers must still be able to run
  IP discovery (broadcast/unicast or `--tcp_discovery`).
