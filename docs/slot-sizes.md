# 64-bit Slot Sizes

A channel's slot size is the number of bytes available for the payload of a
single message. Slot sizes are 64-bit values everywhere inside Subspace, but
the C and C++ client APIs expose them as 32-bit values by default. Building
with `SUBSPACE_64BIT_SLOT_SIZE` widens those APIs so that a channel can have
slots larger than 2GB.

## Why There Is an Option

The C and C++ APIs originally typed the slot size as `int32_t`. Widening them
unconditionally would change the signature of every function that accepts or
returns a slot size, which breaks source compatibility for existing callers and
silently changes the layout of `PublisherOptions`. The macro keeps the old API
as the default and makes the wider one opt-in.

## What Is Always 64-bit

The macro only selects the width of the client API types. Regardless of how you
build:

- The `slot_size` fields in the wire protocol are `int64`.
- The shared-memory control block, the buffer sizing arithmetic, and the slot
  offset calculations all use 64-bit values.
- The server tracks and allocates slot sizes as `int64_t`.
- `ChannelControlBlock::max_message_size` is a `uint64_t`.

This means a server and a client built with different settings still agree
about the channel. Only the numbers the API hands back to your code differ.

## The Default: 32-bit API

Without the macro, `subspace::SlotSizeType` is `int32_t` and the C
`SubspaceSlotSize` is `int32_t`.

The client rounds a requested slot size up to a 64-byte boundary before sending
it to the server, so the largest slot you can ask for is **2,147,483,584 bytes
(2GB - 64)**, not `INT32_MAX`. `INT32_MAX` itself fails, because rounding it up
overflows the field and the server rejects the negative value with
`num_slots and slot_size must be greater than 0`.

A channel that needs more than 2GB in total is still fine in this mode; use
more slots. The limit is per slot, not per channel.

## Enabling the 64-bit API

The macro changes struct layouts and function signatures, so it must be set
consistently for the library and for everything that includes its headers. Set
it globally rather than per-target.

### Bazel

```bash
bazel build --config=slot_size_64 //...
bazel test  --config=slot_size_64 //...
```

The config is defined in `.bazelrc` and simply adds
`--copt=-DSUBSPACE_64BIT_SLOT_SIZE`.

### CMake

```bash
cmake -DSUBSPACE_64BIT_SLOT_SIZE=ON ..
make -j$(nproc)
```

The option adds a global `add_compile_definitions(SUBSPACE_64BIT_SLOT_SIZE)`
and prints `Client API slot size: 64 bit` while configuring.

### Other Build Systems

Compile every translation unit, including your own, with
`-DSUBSPACE_64BIT_SLOT_SIZE`.

## API Changes When Enabled

Nothing is renamed. `subspace::SlotSizeType` changes from `int32_t` to
`int64_t`, the C `SubspaceSlotSize` does the same, and every declaration spelled
in terms of them widens with it.

In C++ those are:

```cpp
SlotSizeType PublisherOptions::slot_size;          // field
SlotSizeType PublisherOptions::SlotSize() const;
PublisherOptions &PublisherOptions::SetSlotSize(SlotSizeType size);

absl::StatusOr<Publisher> Client::CreatePublisher(
    const std::string &channel_name, SlotSizeType slot_size, int num_slots,
    const PublisherOptions &opts = PublisherOptions());

SlotSizeType Publisher::SlotSize() const;
SlotSizeType Subscriber::SlotSize() const;

absl::StatusOr<void *> Publisher::GetMessageBuffer(SlotSizeType max_size = -1,
                                                   bool lock = true);
absl::StatusOr<absl::Span<std::byte>>
Publisher::GetMessageBufferSpan(SlotSizeType max_size = -1, bool lock = true);

absl::Status Publisher::RegisterResizeCallback(
    std::function<absl::Status(Publisher *, SlotSizeType, SlotSizeType)> cb);
```

And in C:

```c
SubspaceSlotSize SubspacePublisherOptions.slot_size;  // field

SubspacePublisherOptions
subspace_publisher_options_default(SubspaceSlotSize slot_size, int num_slots);

SubspaceSlotSize subspace_get_publisher_slot_size(SubspacePublisher publisher);
SubspaceSlotSize
subspace_get_subscriber_slot_size(SubspaceSubscriber subscriber);
```

Code that already uses `int` or `int32_t` for slot sizes keeps compiling when
the macro is off, and keeps compiling with an implicit widening when it is on.
Code that stores the result of `Publisher::SlotSize()` in an `int32_t` will
narrow when the macro is on, so prefer `subspace::SlotSizeType` or `int64_t`.

### Unaffected

These already report 64-bit values in both builds, so they are never truncated:

- `ChannelInfo::slot_size` and `SubspaceChannelInfo::slot_size` (`uint64_t`),
  as returned by `Client::GetChannelInfo()` and
  `subspace_get_channel_info()`.
- `Message::SlotSize()` (`uint64_t`).
- `ChannelStats::max_message_size` and the `max_message_size` argument of
  `GetStatsCounters()` (`uint64_t`).
- `subspace_get_message_buffer()`, whose `max_size` argument is a `size_t`.

## Mixing Builds

Do not link code compiled with the macro against a library compiled without it,
or vice versa. `PublisherOptions` and `SubspacePublisherOptions` change size,
and the functions above change signature.

In C++ most mismatches fail to link, because `SetSlotSize(int)` and
`SetSlotSize(long)` mangle differently. C has no such protection, so a
mismatched C build will compile and link and then misbehave.

Separate *processes* may be built differently, because the wire protocol and
shared memory are 64-bit either way. A 32-bit-API client can attach to a
channel whose slots are larger than 2GB, but `Publisher::SlotSize()`,
`Subscriber::SlotSize()` and the resize callback will report a truncated value.
Use the macro on every process that needs to see large slots.

## Example

```cpp
// Build with --config=slot_size_64 or -DSUBSPACE_64BIT_SLOT_SIZE=ON.
constexpr int64_t kSlotSize = 3LL * 1024 * 1024 * 1024; // 3GB.

subspace::Client client;
if (absl::Status status = client.Init(); !status.ok()) {
  return status;
}

// Three slots: one for the publisher's lease, one for the subscriber's active
// message and one for the publisher to move on to.
absl::StatusOr<subspace::Publisher> pub = client.CreatePublisher(
    "big_frames",
    subspace::PublisherOptions().SetSlotSize(kSlotSize).SetNumSlots(3));
if (!pub.ok()) {
  return pub.status();
}

absl::StatusOr<void *> buffer = pub->GetMessageBuffer(kSlotSize);
if (!buffer.ok()) {
  return buffer.status();
}
std::memcpy(*buffer, frame_data, kSlotSize);
return pub->PublishMessage(kSlotSize).status();
```

## Resource Notes

Large slots are worth sizing deliberately.

The channel reserves `num_slots * (slot_size + prefix_size)` bytes of shared
memory, so a three-slot 3GB channel maps about 9GB of address space. The
backing objects are sparse, so only the pages actually written are committed,
but the nominal size still has to fit in the shared-memory filesystem. A
container with a small `/dev/shm` will fail to create the channel.

Prefer sizing the channel up front over letting it grow. Automatic growth
allocates a fresh shared-memory buffer at every expansion step, and above 1MB
the growth factor is only 1.03125, so growing into the gigabyte range creates
hundreds of buffers and maps many times the final size.

Bridging a large channel is expensive: the bridge allocates a heap buffer the
size of a full slot for each message it receives.

## Other Language Clients

The Python, Rust, and Java clients are unconditionally 64-bit and are not
affected by the macro.

The Rust client is a native reimplementation with its own `i64` slot size in
`PublisherOptions`, so it is genuinely independent of how the C++ library was
built. The Python and Java clients are bindings over the C++ client: their
signatures are 64-bit, but the range they can actually use follows the setting
the C++ library was built with. Note that a Java `ByteBuffer` has an `int`
capacity, so the JNI layer reports an error rather than returning an unusable
buffer for a slot larger than 2GB.

## Testing

`bazel test --config=slot_size_64 //...` runs the full suite against the
widened API, including `ClientTest.SlotSizeLargerThanInt32`, which creates a
2.01GB slot and verifies a full-slot message round trip. That test is compiled
out of the default build because the 32-bit API cannot express the size. CI
runs the `slot-size-64` job on Linux and macOS to cover it.
