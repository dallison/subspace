# Publisher Buffer Leases

Publisher buffer leases let a C++, C, Python, or Rust publisher own several
unpublished channel slots at once. They are useful for asynchronous producers,
external memory pipelines, and applications that need to reclaim the exact slot
reported by retirement notifications.

The lease API is currently exposed by the C++, C, Python, and Rust clients. The
Java client continues to use the implicit single-buffer publish API.

## Implicit Buffers Versus Explicit Leases

The existing publish workflow remains unchanged:

1. `GetMessageBuffer()` obtains the publisher's implicit current slot.
2. The application writes the payload and optional metadata.
3. `PublishMessage()` publishes it. An unreliable publisher immediately
   reserves its next current slot.

The explicit workflow is:

1. `AcquireBufferLease()` obtains a `PublisherBufferLease`.
2. The application writes through `lease.buffer` and, optionally,
   `GetMetadata(lease)`.
3. It calls either `PublishBufferLease()` or `ReleaseBufferLease()`.

Explicit leases do not hold the thread-safe client's mutex for the lifetime of
the buffer. A publisher may own up to `max_outstanding_slot_leases` unpublished
leases at once. The default is `1`.

Choose one publish workflow for a publisher. After the explicit lease path
takes the implicit current slot, the legacy unreliable `GetMessageBuffer()`
path no longer has its usual preallocated current slot.

## C++ Example

```cpp
subspace::PublisherOptions options;
options.SetSlotSize(4096)
    .SetNumSlots(16)
    .SetMetadataSize(16)
    .SetMaxOutstandingSlotLeases(3)
    .SetNotifyRetirement(true)
    .SetNotifyRetirementOnForcedReuse(false);

auto pub_or = client.CreatePublisher("camera", options);
if (!pub_or.ok()) {
  return pub_or.status();
}
subspace::Publisher pub = *std::move(pub_or);

auto lease_or = pub.AcquireBufferLease();
if (!lease_or.ok()) {
  return lease_or.status();
}
subspace::PublisherBufferLease lease = *lease_or;
if (!lease) {
  // No slot is currently available. Retry after a retirement notification.
  return absl::UnavailableError("no publisher slot available");
}

std::memcpy(lease.buffer, payload, payload_size);
absl::Span<std::byte> metadata = pub.GetMetadata(lease);
// Fill metadata when configured.

auto message_or = pub.PublishBufferLease(lease, payload_size);
if (!message_or.ok()) {
  return message_or.status();
}
```

`PublisherBufferLease` contains:

| Field | Meaning |
| --- | --- |
| `buffer` | Writable payload address. |
| `buffer_size` | Capacity of that payload buffer. |
| `slot_id` | Channel slot owned by the lease. |
| `lease_id` | Generation token that rejects stale operations after reuse. |

Publishing or releasing invalidates the token. Do not publish, release, or
query metadata with an old lease. To discard an unpublished buffer, call:

```cpp
absl::Status status = pub.ReleaseBufferLease(lease);
```

## Python Example

Python lease buffers are writable staging `memoryview` objects. On publish, the
requested payload bytes are copied into the validated leased slot. This keeps
an already-exported view safe after publish or release: later writes affect
only its private staging allocation, not a published or reused shared-memory
slot. Acquiring or reclaiming returns `None` when the requested slot is
temporarily unavailable:

```python
options = subspace.PublisherOptions()
options.set_slot_size(4096)
options.set_num_slots(16)
options.set_metadata_size(16)
options.set_max_outstanding_slot_leases(3)
options.set_notify_retirement(True)
options.set_notify_retirement_on_forced_reuse(False)

pub = client.create_publisher("camera", options=options)
lease = pub.acquire_buffer_lease()
if lease is not None:
    lease.buffer[:len(payload)] = payload
    pub.set_metadata(lease, metadata)
    message = pub.publish_buffer_lease(lease, len(payload))
```

`publish_buffer_lease()` and `release_buffer_lease()` invalidate the Python
lease object after success. `lease.valid` becomes false and subsequent access
is rejected. After reading a retired `int32_t` slot ID from
`pub.get_retirement_fd()`, call `pub.reclaim_buffer_lease(slot_id)` to acquire
the same slot with a new `lease_id`.

## Rust Example

Rust exposes the payload as a raw shared-memory pointer and provides an unsafe
slice helper:

```rust
let options = PublisherOptions::new()
    .set_slot_size(4096)
    .set_num_slots(16)
    .set_metadata_size(16)
    .set_max_outstanding_slot_leases(3)
    .set_notify_retirement(true)
    .set_notify_retirement_on_forced_reuse(false);
let publisher = client.create_publisher("camera", &options)?;

if let Some(mut lease) = publisher.acquire_buffer_lease()? {
    unsafe {
        lease.as_mut_slice()[..payload.len()].copy_from_slice(payload);
    }
    publisher.set_lease_metadata(&lease, metadata)?;
    publisher.publish_buffer_lease(&lease, payload.len() as i64)?;
}
```

`acquire_buffer_lease()` and `reclaim_buffer_lease()` return `Ok(None)` for
temporary unavailability. `publish_buffer_lease()` and
`release_buffer_lease()` logically invalidate the token; stale copies are
rejected with `SubspaceError::FailedPrecondition`.

An empty lease is a temporary availability result, not an error. For example,
an unreliable publisher cannot take a slot that is actively referenced by a
subscriber, and a reliable publisher does not acquire a slot while it has no
subscribers.

## Retirement and Exact-Slot Reclamation

With `notify_retirement` enabled, `GetRetirementFd()` returns a pipe read end.
Each notification is an `int32_t` slot ID. A slot normally retires when every
subscriber has released it. A leased publication with no subscribers retires
immediately.

After reading a retired slot ID, reclaim that exact slot:

```cpp
int32_t slot_id;
ssize_t count =
    read(pub.GetRetirementFd().Fd(), &slot_id, sizeof(slot_id));
if (count == sizeof(slot_id)) {
  auto reclaimed_or = pub.ReclaimBufferLease(slot_id);
  if (reclaimed_or.ok() && *reclaimed_or) {
    subspace::PublisherBufferLease reclaimed = *reclaimed_or;
    // reclaimed.slot_id is slot_id and reclaimed.lease_id is a new token.
  }
}
```

For unreliable publishers,
`notify_retirement_on_forced_reuse` controls notifications generated when the
publisher overwrites an unread message:

- `true` (default) preserves legacy retirement notifications for forced reuse.
  Such a notification is informational; the slot may already have been reused,
  so exact reclamation can return an empty lease.
- `false` reports only subscriber-completed retirement, plus immediate
  retirement when there are no subscribers. Use this mode when notifications
  drive exact-slot reclamation or external-resource lifetime.

Virtual publishers do not support retirement notifications.

## Channel Capacity

For unreliable channels, the server reserves capacity from configured maxima,
not current usage:

```text
slots_needed =
    sum(publisher.max_outstanding_slot_leases)
  + sum(subscriber.max_active_messages)

slots_needed <= num_slots - 1
```

This guarantees that a publisher below its configured lease limit can acquire
another lease even when subscribers hold their maximum active messages. The
same accounting is aggregated across virtual channels sharing a multiplexer.
A publisher or subscriber registration is rejected if its configured budget
would exceed the channel capacity.

The default lease limit of `1` preserves the previous capacity behavior.

## Subscriber Limits

`SubscriberOptions::SetMaxSubscribers(n)`, Python
`SubscriberOptions.set_max_subscribers(n)`, Rust
`SubscriberOptions::set_max_subscribers(n)`, and the C
`SubspaceSubscriberOptions.max_subscribers` field set a server-enforced
subscriber count limit. `0` means no explicit limit. The first subscriber to a
channel establishes the value; later subscribers must request the same value.
For virtual channels, the limit applies to the shared multiplexer.

## C API

Configure the C publisher before creation:

```c
SubspacePublisherOptions options =
    subspace_publisher_options_default(4096, 16);
options.max_outstanding_slot_leases = 3;
options.notify_retirement = true;
options.notify_retirement_on_forced_reuse = false;
```

The matching lifecycle functions are:

| Function | Purpose |
| --- | --- |
| `subspace_acquire_publisher_buffer` | Acquire any available unpublished slot. |
| `subspace_reclaim_publisher_buffer` | Acquire a specific retired slot ID. |
| `subspace_get_publisher_buffer_metadata` | Return writable metadata for a valid lease. |
| `subspace_publish_publisher_buffer` | Publish a leased slot and invalidate the lease. |
| `subspace_release_publisher_buffer` | Discard a leased slot and invalidate the lease. |
| `subspace_get_publisher_retirement_fd` | Return the fd that emits retired `int32_t` slot IDs. |

An unavailable acquisition has `buffer == NULL` without setting the
thread-local error. A real failure also sets `subspace_get_last_error()`.
