# ASIL Client

The ASIL client in `asil_client/` is a Subspace client for safety-related
software.  It publishes and reads messages on the same channels as the
standard C++, C, Python and Rust clients, using the same shared memory
protocol, so an ASIL process and a standard process can share a channel in
either direction.

It works with any server.  A server running a
[static channel config](../README.md#static-channel-config), where every
channel exists before any client connects and keeps a fixed layout, gives a
safety case the most predictable setup.

## Design Rules

- **C++17, the standard library and POSIX.**  The core library has no other
  dependencies and is compiled with `-fno-exceptions -fno-rtti`.  The server
  connection adds [phaser](https://github.com/dallison/phaser) and is built
  the same way.
- **Errors are values.**  Every operation that can fail returns
  `subspace::asil::Error`.  `ErrorString()` describes a code, and
  `Client::LastServerError()` gives the server's reason for
  `Error::kServerRejected`.
- **No heap allocation.**  The client, publishers and subscribers allocate no
  memory, and neither does `PhaserServerConnection` while it talks to the
  server.
- **Resources are acquired when an object opens.**  A publisher or
  subscriber maps all of its channel's shared memory in `Open()`, and creates
  shared memory only when an ASIL publisher is the first on a channel that
  isn't static.  Publishing and reading make no `mmap`, `shm_open` or other
  shared memory calls.  After `Open()` the server is involved only when a
  publisher or subscriber refreshes its trigger file descriptors because
  another process joined or left the channel, and on `Close()`.
- **Fixed capacities.**  Channel names hold up to 255 characters and each
  trigger list holds up to 1024 file descriptors.  A request or response to
  the server holds up to 16 KB in wire format.  Exceeding any of these
  returns `Error::kCapacityExceeded`.  `max_active_messages` is at most 64.
  A subscriber keeps the newest ordinal of every virtual channel, which makes
  it about 8 KB, so keep subscribers in static storage or inside long-lived
  objects.
- **Single threaded objects.**  A `Client`, `Publisher` or `Subscriber` is used
  by one thread at a time.  The `ServerConnection` outlives the `Client`, and
  the `Client` outlives its publishers and subscribers.

## Scope

The ASIL client shares channels with the standard clients for:

- Unreliable and reliable publishers and subscribers.
- Virtual channels on a multiplexer, and subscribers to a whole multiplexer.
- Subscribers that hold several messages at once (`max_active_messages`).
- Subscriber queues: an ASIL publisher fills the queues of standard
  subscribers that use them.
- Split buffers, including slots that a standard publisher's custom
  allocator created.
- POSIX shared memory on macOS and QNX, `/dev/shm` on Linux, and the memfd
  backend (always used on Android, and selected on Linux with
  `SUBSPACE_LINUX_USE_MEMFD`).
- Checksums, user metadata, `kReadNext` and `kReadNewest`, dropped message
  counting and activation messages.

### Channel Buffers

A publisher or subscriber maps one of the channel's message buffers, the
newest, when it opens.  A server with a static channel config creates every
static channel's buffers when it starts, so ASIL publishers and subscribers
can open in any order.  On a server that creates channels on demand, a
channel has buffers once its first publisher opens.  Until then an ASIL
subscriber's `Open()` returns `Error::kNoBuffers`.  An ASIL publisher that is
first on such a channel creates its buffer.

ASIL publishers always use fixed-size slots, so a channel never resizes while
one is open.  A standard publisher can resize a channel that only it
publishes on, which gives the channel a new buffer.  An ASIL subscriber that
opened before the resize can't read the messages in the new buffer.
`ReadMessage` skips each of them and returns `Error::kBufferNotMapped`.  An
ASIL publisher or subscriber that opens after the resize uses the new buffer.

### Split Buffers

A channel with split buffers keeps the message prefixes in one shared memory
object and each slot's payload in its own.  The ASIL client maps all of them
in `Open()`, into an array of `SplitSlot` that the caller provides in
`split_slots`, with its length in `split_slot_capacity`.  The array needs at
least the channel's `num_slots` entries, or `Open()` returns
`Error::kCapacityExceeded`.  It must outlive the publisher or subscriber.
An ASIL publisher sets `use_split_buffers` and maps the channel's existing
buffers; it never creates split buffers.

A static channel or multiplexer has split buffers when its config sets
`use_split_buffers`, and the server creates them.  Publishers must then use
split buffers, and on any other static channel they must not.  On a server
that creates channels on demand, the first standard publisher creates them.

A standard publisher can create its slots with a custom allocator.  To map
those slots, set `split_allocator` to C functions that map and unmap a slot,
with a context pointer.  `map` gets the slot's `SplitBufferInfo`: the
allocator's handle, sizes and the descriptor the creator registered, if any.
`Open()` calls `map` for each slot, and `Close()` calls `unmap`.  Without
`split_allocator.map`, such a channel returns `Error::kUnsupported`.

```cpp
asil::Error MapSlot(void *context, const asil::SplitBufferInfo &info,
                    asil::SplitBufferMapping &mapping) {
  mapping.address = static_cast<Pool *>(context)->Find(info.handle);
  return mapping.address != nullptr ? asil::Error::kOk
                                    : asil::Error::kInvalidArgument;
}

asil::SplitSlot slots[16];
asil::SubscriberOptions options;
options.split_slots = slots;
options.split_slot_capacity = 16;
options.split_allocator.map = MapSlot;
options.split_allocator.context = &pool;
```

## Building

Bazel:

```
bazelisk build //asil_client:asil_client //asil_client:phaser_connection
bazelisk test //asil_client:all
```

| Target | Contents | Dependencies |
|---|---|---|
| `//asil_client:asil_client` | Client, publisher, subscriber, shared memory | C++17, POSIX |
| `//asil_client:phaser_connection` | The standard server handshake | `asil_client`, `//common:server_wire` |
| `//common:server_wire` | Server request and response encoding, shared with the standard client | `//proto:subspace_phaser` |
| `//proto:subspace_phaser` | Phaser messages generated from `subspace.proto` | phaser runtime, Abseil, cpp_toolbelt |

CMake builds the same libraries as `subspace_asil_client`,
`subspace_asil_phaser_connection` and `subspace_phaser`, with the tests
`asil_layout_test` and `asil_client_test`.  Phaser is fetched with
`FetchContent`, and its `protoc-gen-phaser` plugin is built for the host.  A
cross-compile needs a host plugin passed as `-DPHASER_PLUGIN_EXECUTABLE`; see
[the Android CMake build](android.md#building-with-cmake).

Soong builds `libsubspace_asil_client`, `libsubspace_asil_phaser_connection`
and `libsubspace_phaser`, using the phaser modules from
`external/phaser/Android.bp.example`.

The core library takes `SUBSPACE_MAX_CHANNELS` from the same build setting as
the server, because it sizes the shared system control block.

## The Server Connection

The client talks to the server through the abstract `ServerConnection` in
`asil_client/server_connection.h`.  It has one call for each request the
client makes: `Init`, `CreatePublisher`, `CreateSubscriber`, `GetTriggers`,
`RemovePublisher` and `RemoveSubscriber`, plus `RegisterBuffer` and
`GetBuffer`, which pass buffer file descriptors between processes through the
server.  `GetBuffer` fetches one buffer: a channel's single buffer for the
memfd backend, or the prefixes or one slot of a split buffer set, so that
each response stays small.

`PhaserServerConnection` implements it with the server's standard protocol:
length-prefixed messages in protobuf wire format on the server's Unix socket,
with file descriptors passed by `SCM_RIGHTS`.  The server receives the same
bytes it receives from the standard clients.  Requests and responses are
encoded by `ServerWire` in `common/server_wire.h`, which the standard C++
client uses too.  The standard client gives it growable buffers.  The ASIL
connection gives it fixed buffers inside the object: it builds each request
as a phaser message in a fixed buffer, serializes it to protobuf wire format,
and decodes the response into the same buffer, so it needs neither the
protobuf library nor the heap.  The buffers take about 60 KB, so keep the
connection in static storage or inside a long-lived object.  A system with
its own qualified transport can replace it with another implementation of
`ServerConnection`.

## Example

```cpp
#include "asil_client/client.h"
#include "asil_client/phaser_connection.h"

#include <cstring>

namespace asil = subspace::asil;

asil::PhaserServerConnection connection;

asil::Error Run() {
  if (asil::Error e = connection.Connect("/tmp/subspace");
      e != asil::Error::kOk) {
    return e;
  }
  asil::Client client;
  if (asil::Error e = client.Init(connection, "controller");
      e != asil::Error::kOk) {
    return e;
  }

  asil::PublisherOptions pub_options;
  pub_options.slot_size = 256;
  pub_options.num_slots = 8;
  pub_options.checksum = true;
  asil::Publisher publisher;
  if (asil::Error e =
          client.CreatePublisher("/vehicle/command", pub_options, publisher);
      e != asil::Error::kOk) {
    return e;
  }

  asil::SubscriberOptions sub_options;
  sub_options.checksum = true;
  asil::Subscriber subscriber;
  if (asil::Error e =
          client.CreateSubscriber("/vehicle/state", sub_options, subscriber);
      e != asil::Error::kOk) {
    return e;
  }

  void *buffer = publisher.Buffer();
  if (buffer == nullptr) {
    return asil::Error::kNoSlot;
  }
  std::memcpy(buffer, "go", 2);
  if (asil::Error e = publisher.Publish(2); e != asil::Error::kOk) {
    return e;
  }

  if (asil::Error e = subscriber.Wait(/*timeout_ms=*/100);
      e == asil::Error::kTimeout) {
    return asil::Error::kOk;
  } else if (e != asil::Error::kOk) {
    return e;
  }
  asil::Message msg;
  for (;;) {
    if (asil::Error e = subscriber.ReadMessage(msg); e != asil::Error::kOk) {
      return e;
    }
    if (msg.length == 0) {
      break;
    }
    // msg.data stays valid until the next ReadMessage or ReleaseMessage.
  }
  return asil::Error::kOk;
}
```

## Publishers

`PublisherOptions` gives the slot size, number of slots, checksum size,
metadata size and type.  On a static channel these must match the config.
`subscriber_queue_arena_size` must match the channel's other publishers.

`Buffer()` returns the payload of the slot the publisher holds, and
`Metadata()` returns its metadata area.  `Publish(size)` sends the message and
claims the next slot.  It fills in an optional `PublishedMessage` with the
message's ordinal and timestamp.  When `checksum` is set the publisher
computes the same CRC32 as the standard clients.  When `activate` is set the
publisher sends the channel's activation message if no publisher has sent one.

A publisher joining a channel whose slots a standard publisher has grown uses
the channel's newest buffer, so `SlotSize()` can be larger than the requested
size.

### Reliable Publishers

With `reliable` set, the publisher never overwrites a message that a
reliable subscriber hasn't read.  It sends an activation message when it
opens, which every reliable subscriber holds until it reads further.
`Buffer()` returns null while the channel has no subscribers, and while every
slot holds a message a reliable subscriber still needs.  `PollFd()` becomes
readable when a reliable subscriber releases a message, and
`Wait(timeout_ms)` polls it:

```cpp
void *buffer = publisher.Buffer();
while (buffer == nullptr) {
  if (asil::Error e = publisher.Wait(/*timeout_ms=*/100);
      e != asil::Error::kOk) {
    return e;
  }
  buffer = publisher.Buffer();
}
```

### Virtual Channels

Set `mux` to the multiplexer's name to publish on one of its virtual
channels.  The server assigns the virtual channel id unless `vchan_id` gives
one.  `VirtualChannelId()` returns it.

## Subscribers

`ReadMessage(msg, mode)` fills in a `Message` with the payload, length,
ordinal, timestamp, slot, virtual channel id, metadata and the number of
messages dropped since the previous one.  A length of 0 means there was
nothing to read.  `PollFd()` is readable when messages may be waiting, and
`Wait(timeout_ms)` polls it.

With `checksum` set, a message with a bad checksum returns
`Error::kChecksumMismatch`.  With `pass_checksum_errors` also set, it is
delivered with `checksum_error` set.  Activation messages are skipped unless
`pass_activation` is set.


### Holding Messages

By default a subscriber holds one message, and reading the next releases it.
With `max_active_messages` set to more than 1, every message read stays valid
until it is released.  `ReleaseMessage(msg)` releases one message and
`ReleaseMessage()` releases them all.  When the subscriber holds
`max_active_messages` messages, `ReadMessage` returns
`Error::kActiveMessageLimit`.

### Reliable Subscribers

With `reliable` set, reliable publishers wait for the subscriber to read each
message.  A reliable subscriber that holds one message keeps it until it has
read the next one, so a publisher can never take every slot.  Reading with
`kReadNewest` marks the skipped messages as read, which lets reliable
publishers reuse their slots.

### Virtual Channels and Multiplexers

Set `mux` to subscribe to a virtual channel.  Subscribing to the multiplexer
itself delivers the messages of all its virtual channels, with each message's
`vchan_id`.  Each virtual channel has its own ordinals, so dropped messages
are counted per virtual channel.

### Subscriber Queues

An ASIL subscriber reads the channel's record of unread messages directly,
so it works on channels with subscriber queues without using one.  ASIL
publishers add every message to the queues of the standard subscribers that
have them.

## Testing

`layout_test` compares every size, offset and constant in
`asil_client/shm_layout.h` with `common/channel.h`, checks that the checksum
matches the standard client's, and passes subscriber queue entries between
the ASIL client and the standard `InPlaceSlotQueue`.

`asil_client_test` exchanges messages between the ASIL client and the
standard client in both directions.  One server runs a static channel config,
for reliable channels, virtual channels, held messages, split buffers and
subscribers that open before any publisher.  A second server creates channels
on demand, for channels created by ASIL publishers, resized channels,
subscriber queues, custom split buffer allocators and subscribers that open
before a channel has buffers.  The
test also checks that setting up and closing a publisher and a subscriber
makes no heap allocations, and that a request too large for the wire buffer
returns `Error::kCapacityExceeded` and leaves the connection usable.
