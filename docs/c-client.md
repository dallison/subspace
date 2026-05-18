# C Client API

The C client in `c_client/subspace.h` is a C ABI wrapper around the C++ client.
It is intended for C applications and for language bindings that need stable
opaque handles, plain structs, and thread-local error strings instead of C++
types.

The API mirrors the C++ client closely: clients connect to a local Subspace
server, publishers create or attach to channels, subscribers read messages from
channels, and message transfer still happens directly through shared memory.

## Handles and Errors

The main handle types are opaque wrappers:

| Type | Purpose |
| --- | --- |
| `SubspaceClient` | Connection to a server over a Unix domain socket. |
| `SubspacePublisher` | Publisher attached to a channel. |
| `SubspaceSubscriber` | Subscriber attached to a channel. |
| `SubspaceMessage` | A received message reference. |
| `SubspaceMessageBuffer` | A writable publish buffer returned by a publisher. |

Most functions return `bool`, an integer status, or a handle whose inner pointer
is `NULL` on failure. When a call fails, use:

```c
if (subspace_has_error()) {
  fprintf(stderr, "subspace error: %s\n", subspace_get_last_error());
}
```

The error string is thread-local and owned by the library.

## Client Setup

Use the default server socket, a custom socket, or a custom socket plus client
name:

```c
SubspaceClient client = subspace_create_client();
SubspaceClient named = subspace_create_client_with_socket_and_name(
    "/tmp/subspace", "my_client");

if (named.client == NULL) {
  fprintf(stderr, "%s\n", subspace_get_last_error());
}
```

Additional client controls and queries include:

| Function | Purpose |
| --- | --- |
| `subspace_set_client_debug` | Enable or disable client debug logging. |
| `subspace_set_client_thread_safe` | Enable thread-safe client behavior where supported. |
| `subspace_channel_exists` | Check whether a channel exists. |
| `subspace_get_channel_counters` | Read per-channel update counters. |
| `subspace_get_channel_info` | Read one channel's metadata. |
| `subspace_get_all_channel_info` | Return metadata for all channels. |
| `subspace_get_channel_stats` | Read byte/message/max-size stats for one channel. |
| `subspace_get_all_channel_stats` | Return stats for all channels. |

The arrays returned by `subspace_get_all_channel_info` and
`subspace_get_all_channel_stats` are owned by the client wrapper and remain
valid until the next call on the same client or until the client is destroyed.

## Publisher Options

Start with `subspace_publisher_options_default(slot_size, num_slots)` and then
override fields:

```c
SubspacePublisherOptions opts =
    subspace_publisher_options_default(sizeof(MyMessage), 16);

opts.reliable = true;
opts.fixed_size = false;
opts.type.type = "MyMessage";
opts.type.type_length = strlen(opts.type.type);
opts.checksum = true;
opts.metadata_size = 32;

SubspacePublisher pub = subspace_create_publisher(client, "my_channel", opts);
```

Important publisher options:

| Field | Purpose |
| --- | --- |
| `slot_size`, `num_slots` | Initial payload slot size and fixed slot count. |
| `local` | Keep the channel local to this server. |
| `reliable` | Wait for free slots instead of overwriting unread messages. |
| `bridge`, `for_tunnel` | Mark bridge/tunnel publishers. |
| `fixed_size` | Reject oversized messages instead of resizing. |
| `type` | Opaque type string used to match publishers and subscribers. |
| `activate` | Publish an activation message when the publisher is created. |
| `mux`, `vchan_id` | Use a mux channel and optional virtual channel id. |
| `notify_retirement` | Expose a retirement fd for reliable-slot retirement notifications. |
| `checksum`, `checksum_size` | Enable checksums and reserve checksum bytes in the prefix. |
| `metadata_size` | Reserve per-message user metadata bytes in the prefix. |
| `prefer_retired_slots` | Prefer recently retired slots for unreliable publishers. |
| `use_split_buffers` | Store payloads in split payload buffers. |
| `split_buffers_over_bridge` | Ask receiving bridge servers to create their mirror publisher with split payload buffers. |
| `max_publishers` | Maximum publishers allowed on a split-buffer channel. |
| `split_callbacks` | Optional allocator callbacks for split payload buffers. |

## Publishing

Publishing is a two-step operation: get a writable buffer, then publish the
number of bytes written.

```c
SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, sizeof(MyMessage));
if (buffer.buffer == NULL) {
  subspace_wait_for_publisher(pub);
  buffer = subspace_get_message_buffer(pub, sizeof(MyMessage));
}

struct MyMessage *msg = (struct MyMessage *)buffer.buffer;
msg->value = 42;

SubspaceMessage status = subspace_publish_message(pub, sizeof(*msg));
if (status.length == 0) {
  fprintf(stderr, "publish failed: %s\n", subspace_get_last_error());
}
```

Related publisher APIs:

| Function | Purpose |
| --- | --- |
| `subspace_publish_message_with_prefix` | Publish using prefix fields already written by the caller. |
| `subspace_cancel_publish` | Release the current publish buffer without publishing. |
| `subspace_wait_for_publisher` | Wait indefinitely for a reliable publisher slot. |
| `subspace_wait_for_publisher_with_timeout` | Wait with a timeout. |
| `subspace_wait_for_publisher_with_fd` | Wait until the publisher or an interrupt fd triggers. |
| `subspace_wait_for_publisher_with_fd_and_timeout` | Wait with both an interrupt fd and timeout. |
| `subspace_get_publisher_poll_fd` | Return a `pollfd` for event-loop integration. |
| `subspace_get_publisher_fd` | Return the publisher trigger fd. |
| `subspace_get_publisher_retirement_fd` | Return the retirement notification fd. |

## Subscriber Options

Start with `subspace_subscriber_options_default()`:

```c
SubspaceSubscriberOptions opts = subspace_subscriber_options_default();
opts.reliable = true;
opts.max_active_messages = 8;
opts.type.type = "MyMessage";
opts.type.type_length = strlen(opts.type.type);
opts.checksum = true;

SubspaceSubscriber sub = subspace_create_subscriber(client, "my_channel", opts);
```

Important subscriber options:

| Field | Purpose |
| --- | --- |
| `reliable` | Participate in reliable slot lifetime tracking. |
| `bridge`, `for_tunnel` | Mark bridge/tunnel subscribers. |
| `type` | Required type string when type matching is used. |
| `max_active_messages` | Maximum simultaneously held `SubspaceMessage` values. |
| `pass_activation` | Deliver activation messages to the caller. |
| `log_dropped_messages` | Log detected drops to stderr. |
| `read_write` | Map payload buffers writable for this subscriber. |
| `mux`, `vchan_id` | Attach through a mux channel and optional virtual channel id. |
| `checksum` | Verify message checksums. |
| `pass_checksum_errors` | Deliver messages with bad checksums and set `checksum_error`. |
| `keep_active_message` | Keep the most recent active message referenced. |
| `split_callbacks` | Optional mapper callbacks for split payload buffers. |

Subscribers do not request split-buffer mode. They learn the channel mode from
the server and use `split_callbacks` only if the publisher used a custom split
payload allocator.

## Reading Messages

Read messages with the default "next" mode, an explicit read mode, or by
timestamp:

```c
SubspaceMessage msg = subspace_read_message(sub);
if (msg.length > 0) {
  const struct MyMessage *data = (const struct MyMessage *)msg.buffer;
  printf("ordinal=%lu value=%d\n", msg.ordinal, data->value);
  subspace_free_message(&msg);
}

SubspaceMessage newest =
    subspace_read_message_with_mode(sub, kSubspaceReadNewest);
subspace_free_message(&newest);
```

Every non-empty `SubspaceMessage` must be released with
`subspace_free_message`. To read batches, use:

```c
SubspaceMessage *messages = NULL;
size_t count = 0;
if (subspace_get_all_messages(sub, kSubspaceReadNext, &messages, &count)) {
  for (size_t i = 0; i < count; ++i) {
    /* process messages[i] */
  }
  subspace_free_messages(messages, count);
}
```

Related subscriber APIs:

| Function | Purpose |
| --- | --- |
| `subspace_find_message` | Find a message by timestamp. |
| `subspace_wait_for_subscriber` | Wait indefinitely for a message. |
| `subspace_wait_for_subscriber_with_timeout` | Wait with a timeout. |
| `subspace_wait_for_subscriber_with_fd` | Wait until the subscriber or interrupt fd triggers. |
| `subspace_wait_for_subscriber_with_fd_and_timeout` | Wait with both an interrupt fd and timeout. |
| `subspace_get_subscriber_poll_fd` | Return a `pollfd` for event loops. |
| `subspace_get_subscriber_fd` | Return the subscriber trigger fd. |
| `subspace_clear_active_message` | Clear the subscriber's active message reference. |
| `subspace_trigger_subscriber` / `subspace_untrigger_subscriber` | Manually trigger or clear the subscriber fd. |
| `subspace_trigger_reliable_publishers` | Wake reliable publishers blocked on this subscriber. |

## Metadata and Checksums

Publishers reserve metadata with `metadata_size`. The metadata area lives in the
message prefix, immediately after the checksum storage, and is included in
checksum verification when checksums are enabled.

```c
size_t metadata_size = 0;
void *metadata = subspace_get_publisher_metadata(pub, &metadata_size);
if (metadata != NULL && metadata_size >= sizeof(uint32_t)) {
  *(uint32_t *)metadata = 1234;
}

SubspaceMessage msg = subspace_read_message(sub);
const void *received_metadata =
    subspace_get_subscriber_metadata(sub, &metadata_size);
```

Useful prefix accessors:

| Function | Purpose |
| --- | --- |
| `subspace_get_publisher_metadata_size` / `subspace_get_subscriber_metadata_size` | Return configured metadata bytes. |
| `subspace_get_publisher_prefix_size` / `subspace_get_subscriber_prefix_size` | Return prefix size. |
| `subspace_get_publisher_checksum_size` / `subspace_get_subscriber_checksum_size` | Return checksum bytes. |

The default checksum implementation is CRC32. Register callbacks when a channel
uses a different algorithm or checksum size:

```c
static void checksum_callback(const SubspaceChecksumSpan *spans,
                              size_t span_count, uint8_t *checksum,
                              size_t checksum_size, void *user_data) {
  /* Compute over spans[0..span_count) and write checksum_size bytes. */
}

subspace_register_publisher_checksum_callback(pub, checksum_callback, NULL);
subspace_register_subscriber_checksum_callback(sub, checksum_callback, NULL);
```

The callback receives the same spans as the C++ API: prefix fields before the
checksum, user metadata, and payload bytes. Publisher and subscriber callbacks
must produce matching bytes.

## Callbacks

The C client exposes both message callbacks and lower-level transform callbacks.

```c
static void on_message(SubspaceSubscriber sub, SubspaceMessage msg) {
  /* The callback receives ownership of msg and must free it. */
  subspace_free_message(&msg);
}

subspace_register_subscriber_callback(sub, on_message);
subspace_process_all_messages(sub);
subspace_remove_subscriber_callback(sub);
```

Callback-related APIs:

| Function | Purpose |
| --- | --- |
| `subspace_register_subscriber_callback` | Register a message callback. |
| `subspace_process_all_messages` | Read all available messages and invoke the callback. |
| `subspace_process_all_messages_with_mode` | Same, with explicit read mode. |
| `subspace_invoke_subscriber_callback` | Invoke the registered callback with a caller-supplied message. |
| `subspace_register_dropped_message_callback` | Register dropped-message notification callback. |
| `subspace_register_publisher_on_send_callback` | Transform outgoing payloads before publish. |
| `subspace_register_subscriber_on_receive_callback` | Transform incoming payloads after read. |
| `subspace_register_resize_callback` | Register publisher resize callback. |

Each registration has a matching remove/unregister function.

## Split Buffers

Split buffers keep message prefixes in ordinary shared memory while storing
payload slots in separately allocated payload buffers. This is useful when
payload memory must come from an external allocator or memory pool.

For the default shared-memory split-buffer path:

```c
SubspacePublisherOptions opts =
    subspace_publisher_options_default(4096, 16);
opts.use_split_buffers = true;
opts.max_publishers = 4;

SubspacePublisher pub = subspace_create_publisher(client, "camera", opts);
```

For custom allocators, fill `SubspaceSplitBufferCallbacks`:

```c
static bool allocate_payload(const SubspaceSplitBufferInfo *info,
                             SubspaceSplitBufferMapping *mapping,
                             void *user_data) {
  mapping->handle = allocate_external_handle(info->allocation_size);
  mapping->address = map_external_handle(mapping->handle, &mapping->size);
  mapping->private_data = NULL;
  return mapping->address != NULL;
}

static bool release_payload(const SubspaceSplitBufferInfo *info,
                            const SubspaceSplitBufferMapping *mapping,
                            void *user_data) {
  unmap_external_handle(mapping->address, mapping->size);
  return true;
}

opts.use_split_buffers = true;
opts.split_callbacks.allocate = allocate_payload;
opts.split_callbacks.free = release_payload;
opts.split_callbacks.user_data = allocator_context;
```

Subscribers use `split_callbacks.map` and `split_callbacks.unmap` to map custom
handles. If callbacks are not supplied, subscribers use the built-in shared
memory split-buffer mapping.

Split-buffer inspection APIs:

| Function | Purpose |
| --- | --- |
| `subspace_publisher_uses_split_buffers` / `subspace_subscriber_uses_split_buffers` | Report whether the attached channel uses split buffers. |
| `subspace_get_publisher_addresses` / `subspace_get_subscriber_addresses` | Return mapped payload addresses, one per slot. |
| `subspace_get_publisher_split_buffer_handles` / `subspace_get_subscriber_split_buffer_handles` | Return allocator handles, one per slot. |
| `subspace_get_publisher_split_buffer_handle_from_address` / `subspace_get_subscriber_split_buffer_handle_from_address` | Look up the handle for one payload address. |

See [Split Buffers](split-buffers.md) for the shared-memory layout and server
behavior.

## Introspection and Diagnostics

The C client now exposes most of the C++ client inspection surface:

| Area | Functions |
| --- | --- |
| Publisher traits | `subspace_is_publisher_reliable`, `subspace_is_publisher_local`, `subspace_is_publisher_fixed_size`, `subspace_is_publisher_for_tunnel`, `subspace_publisher_uses_split_buffers` |
| Subscriber traits | `subspace_is_subscriber_placeholder`, `subspace_is_subscriber_reliable`, `subspace_is_subscriber_for_tunnel`, `subspace_subscriber_uses_split_buffers` |
| Names and routing | `subspace_get_publisher_name`, `subspace_get_subscriber_name`, `subspace_get_publisher_type`, `subspace_get_subscriber_type`, `subspace_get_publisher_mux`, `subspace_get_subscriber_mux`, virtual-channel accessors |
| Counts and stats | `subspace_get_publisher_num_subscribers`, `subspace_get_subscriber_num_subscribers`, channel counters, publisher stats counters |
| Memory use | `subspace_get_publisher_virtual_memory_usage`, `subspace_get_subscriber_virtual_memory_usage` |
| Slot inspection | `subspace_get_publisher_current_slot`, `subspace_get_subscriber_slot`, `subspace_get_publisher_prefix`, `subspace_get_subscriber_prefix`, `subspace_snapshot_message_slot`, `subspace_snapshot_message_prefix` |
| Debug dumps | `subspace_dump_publisher_slots`, `subspace_dump_subscriber_slots` |
| Shared memory names | `subspace_get_publisher_buffer_shared_memory_name` |

String and array results are owned by the wrapper object that returned them.
Treat them as borrowed data; copy them if they need to outlive the next related
API call or the handle itself.

## Cleanup

Remove handles when done:

```c
subspace_remove_subscriber(&sub);
subspace_remove_publisher(&pub);
subspace_remove_client(&client);
```

After removal, the handle's inner pointer is cleared. Messages must still be
freed before removing the subscriber that owns them.
