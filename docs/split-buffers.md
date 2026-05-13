# Split Buffers

Subspace supports split message buffers: prefixes live in regular shared memory
and payload slots live in separately allocated blocks. Publishers and
subscribers provide matching callbacks when payload slots come from a custom
allocator such as Qualcomm memory pools.

## Runtime Channel Options

Split buffers are opt-in per channel. Publishers request split payload buffers
with:

```cpp
auto opts = subspace::PublisherOptions()
    .SetSlotSize(4096)
    .SetNumSlots(16)
    .SetUseSplitBuffers(true)
    .SetSplitBufferCallbacks(qcomm_callbacks);
```

Subscribers do not request split buffers.  They learn the channel mode from the
server when they attach or reload, and can provide callbacks for allocators they
know how to map:

```cpp
auto opts = subspace::SubscriberOptions()
    .SetSplitBufferCallbacks(qcomm_callbacks);
```

If a channel already exists, the server validates that later publishers use
compatible split-buffer settings and publisher limits.

The C API exposes the same generic split-buffer settings through
`SubspacePublisherOptions` and `SubspaceSubscriberOptions`.

## How It Works

For split-buffer channels, the publisher still allocates message buffers
client-side.  Prefixes are regular shared memory.  Payload slots are either
regular split shared-memory objects or blocks returned by the publisher's
callbacks.  Subscribers read the generic split-buffer metadata and either open
the shared-memory object or map the allocator-specific handle via callbacks.

The publisher also sends fire-and-forget registration requests to the server for
client-owned buffers.  The server treats handles as opaque and offers them to
plugins through `OnFreeClientBuffer` when cleanup is needed. Message transfer
remains direct between clients; the server is not on the message data path.

## Testing

Generic split-buffer tests cover the default path. On Linux, split buffers use
`/dev/shm` when no custom callbacks are provided:

```bash
bazelisk test //client:client_test //server:server_test
```
