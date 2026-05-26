# Split Buffers

Split buffers let a channel keep message prefixes in normal Subspace shared
memory while placing payload slots in separately allocated payload buffers. The
message still travels directly between publisher and subscriber; the server is
only involved in setup, validation, and recovery metadata.

This mode is useful when payload memory must come from a specific allocator,
memory pool, DMA-capable region, or other handle-based mapping API. When no
custom allocator callbacks are supplied, Subspace uses generic shared-memory
payload objects for the split payload buffers.

## Normal vs. Split Layout

In the normal layout, each message slot contains both the prefix and payload in
one shared-memory buffer:

```text
slot N: [MessagePrefix][checksum][metadata][padding][payload bytes]
```

In the split-buffer layout, the prefix stays in the channel's regular message
buffer and the payload lives in a separate payload allocation:

```text
prefix slot N:  [MessagePrefix][checksum][metadata][padding]
payload slot N: [payload bytes in separate mapped allocation]
```

The `MessagePrefix` still carries the ordinal, timestamp, size, flags, checksum
size, and metadata size. Checksums and metadata behave the same as in the normal
layout: the checksum covers the prefix fields, metadata, and payload, but not the
checksum storage itself or alignment padding.

## Channel Rules

Split buffers are a channel-level setting:

- The first publisher that creates a channel chooses whether the channel uses
  split buffers.
- Later publishers must use compatible split-buffer settings.
- Subscribers do not request split-buffer mode. They learn it from the server
  when attaching or reloading channel state.
- The server validates the configured publisher limit for split-buffer channels.
- The server records split-buffer metadata in the shadow process so recovery can
  rebuild channel state after a server restart.

## Bridged Channels

Split-buffer behavior over a server bridge has two independent settings:

- `use_split_buffers` controls the local channel layout on the server where the
  publisher is created.
- `split_buffers_over_bridge` controls the mirror publisher created by a
  receiving bridge server.

When the source channel uses split buffers, bridge messages are sent as two
length-prefixed chunks: the `MessagePrefix` area first, then the payload buffer.
When the source channel does not use split buffers, bridge messages keep the
legacy single length-prefixed prefix-plus-payload chunk.

Set `split_buffers_over_bridge` when the receiving server should create its
bridge publisher with split payload buffers, even if the source channel itself
does not use split buffers locally. The two flags can be set independently so a
channel can use split buffers only on the source side, only on the receiving side,
on both sides, or on neither side.

## C++ API

Publishers enable split buffers through `PublisherOptions`:

```cpp
auto opts = subspace::PublisherOptions()
    .SetSlotSize(4096)
    .SetNumSlots(16)
    .SetUseSplitBuffers(true)
    .SetMaxPublishers(4);

auto pub = client.CreatePublisher("camera", opts);
```

To request split payload buffers for the remote mirror publisher created over a
bridge, set `SetSplitBuffersOverBridge(true)`:

```cpp
auto opts = subspace::PublisherOptions()
    .SetSlotSize(4096)
    .SetNumSlots(16)
    .SetSplitBuffersOverBridge(true);
```

For a custom allocator, provide split-buffer callbacks. The allocator returns an
opaque handle plus a mapped address for each payload slot. Subscribers use the
matching map/unmap callbacks to map those handles in their own process.

```cpp
auto callbacks = subspace::SplitBufferCallbacks{
    .allocate = [](const subspace::SplitBufferMetadata &metadata) {
      return AllocatePayload(metadata);
    },
    .map = [](const subspace::SplitBufferMetadata &metadata) {
      return MapPayload(metadata);
    },
    .unmap = [](const subspace::SplitBufferMetadata &metadata,
                const subspace::SplitBufferMapping &mapping) {
      return UnmapPayload(metadata, mapping);
    },
    .free = [](const subspace::SplitBufferMetadata &metadata,
               const subspace::SplitBufferMapping &mapping) {
      return FreePayload(metadata, mapping);
    },
};

auto pub_opts = subspace::PublisherOptions()
    .SetSlotSize(4096)
    .SetNumSlots(16)
    .SetUseSplitBuffers(true)
    .SetSplitBufferCallbacks(callbacks);

auto sub_opts = subspace::SubscriberOptions()
    .SetSplitBufferCallbacks(callbacks);
```

The exact callback type names and helper methods are defined in
`client/client.h`.

## C API

The C API exposes the same feature through `SubspacePublisherOptions` and
`SubspaceSubscriberOptions`:

```c
SubspacePublisherOptions pub_opts =
    subspace_publisher_options_default(4096, 16);
pub_opts.use_split_buffers = true;
pub_opts.max_publishers = 4;
pub_opts.split_buffers_over_bridge = true;

SubspacePublisher pub =
    subspace_create_publisher(client, "camera", pub_opts);
```

Custom allocators use `SubspaceSplitBufferCallbacks`:

```c
static bool allocate_payload(const SubspaceSplitBufferInfo *info,
                             SubspaceSplitBufferMapping *mapping,
                             void *user_data) {
  mapping->handle = allocate_external_handle(info->allocation_size);
  mapping->address = map_external_handle(mapping->handle, &mapping->size);
  mapping->private_data = NULL;
  return mapping->address != NULL;
}

static bool map_payload(const SubspaceSplitBufferInfo *info,
                        SubspaceSplitBufferMapping *mapping,
                        void *user_data) {
  mapping->address = map_external_handle(info->handle, &mapping->size);
  mapping->handle = info->handle;
  mapping->private_data = NULL;
  return mapping->address != NULL;
}

static bool release_payload(const SubspaceSplitBufferInfo *info,
                            const SubspaceSplitBufferMapping *mapping,
                            void *user_data) {
  unmap_external_handle(mapping->address, mapping->size);
  return true;
}

SubspaceSubscriberOptions sub_opts = subspace_subscriber_options_default();

pub_opts.use_split_buffers = true;
pub_opts.split_callbacks.allocate = allocate_payload;
pub_opts.split_callbacks.free = release_payload;
sub_opts.split_callbacks.map = map_payload;
sub_opts.split_callbacks.unmap = release_payload;
```

`SubspaceSplitBufferInfo` describes the requested buffer:

| Field | Meaning |
| --- | --- |
| `channel_name` | Channel that owns the buffer. |
| `session_id` | Server session id for the channel. |
| `buffer_index` | Buffer generation/index. |
| `slot_id` | Slot within the buffer. |
| `is_prefix` | Whether the mapping describes a prefix buffer. |
| `full_size` | Full logical buffer size. |
| `allocation_size` | Requested allocation size. |
| `handle` | Opaque allocator handle when mapping an existing buffer. |

`SubspaceSplitBufferMapping` is filled by callbacks:

| Field | Meaning |
| --- | --- |
| `handle` | Opaque allocator handle for the payload allocation. |
| `address` | Mapped process-local address. |
| `size` | Mapped size in bytes. |
| `private_data` | Callback-owned state passed back to release callbacks. |

The C API also exposes split-buffer inspection helpers:

| Function | Purpose |
| --- | --- |
| `subspace_publisher_uses_split_buffers` / `subspace_subscriber_uses_split_buffers` | Report whether the attached channel uses split buffers. |
| `subspace_get_publisher_addresses` / `subspace_get_subscriber_addresses` | Return mapped payload addresses. |
| `subspace_get_publisher_split_buffer_handles` / `subspace_get_subscriber_split_buffer_handles` | Return allocator handles for mapped payload slots. |
| `subspace_get_publisher_split_buffer_handle_from_address` / `subspace_get_subscriber_split_buffer_handle_from_address` | Look up a handle from a mapped payload address. |

See [C Client API](c-client.md) for the broader C API reference.

## Server and Shadow Behavior

The publisher registers client-owned split payload buffers with the server. The
server treats allocator handles as opaque values; it does not read or write
payload bytes and is not on the data path.

The server uses the metadata for:

- validating compatible channel options as publishers attach;
- informing subscribers how to map payload slots;
- exposing handles to cleanup hooks such as `OnFreeClientBuffer`;
- replicating split-buffer state to the shadow process for recovery.

During shadow recovery, the restarted server restores the channel's
split-buffer options, maximum publisher count, and registered client-buffer
metadata before clients reattach.

## Lifetime

For built-in shared-memory split buffers, Subspace owns creation, mapping, and
cleanup. For custom allocators:

- publisher allocate callbacks create payload allocations and return handles;
- subscriber map callbacks map handles into the subscriber process;
- unmap callbacks release process-local mappings;
- free callbacks release publisher-owned payload allocations;
- callback `user_data` is passed through unchanged and remains owned by the
  application.

Callbacks should treat handles as allocator-defined identifiers, not ordinary
pointers. If an application needs to retain returned handles or address arrays,
it should copy them before the channel resizes, reloads, or the owning handle is
destroyed.

## Testing

Generic split-buffer tests cover the built-in shared-memory path and recovery
behavior:

```bash
bazelisk test //client:client_test //c_client:client_test //shadow:shadow_test
```

On Linux, repeated-run stress testing is useful for catching shared-memory name
collisions and recovery races:

```bash
bazelisk test //common:split_buffer_test --runs_per_test=100
bazelisk test //shadow:shadow_test --runs_per_test=100
```
