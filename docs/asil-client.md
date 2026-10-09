# ASIL Client

The ASIL client in `asil_client/` is a Subspace client for safety-related
software.  It publishes and reads messages on the same channels as the
standard C++, C, Python and Rust clients, using the same shared memory
protocol, so an ASIL process and a standard process can share a channel in
either direction.

It is designed for a server running a [static channel config](../README.md#static-channel-config),
where every channel exists before any client connects and keeps a fixed
layout.

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
  server.  The server is involved when a client, publisher or subscriber is
  set up or closed, and when a publisher or subscriber refreshes its trigger
  file descriptors after another process joins or leaves the channel.
- **Fixed capacities.**  Channel names hold up to 255 characters and each
  trigger list holds up to 1024 file descriptors.  A request or response to
  the server holds up to 16 KB in wire format.  Exceeding any of these returns
  `Error::kCapacityExceeded`.
- **Single threaded objects.**  A `Client`, `Publisher` or `Subscriber` is used
  by one thread at a time.  The `ServerConnection` outlives the `Client`, and
  the `Client` outlives its publishers and subscribers.

## Scope

The ASIL client provides unreliable publishers and subscribers on
fixed-size, non-virtual channels:

- Each channel has one buffer, which is what fixed-size channels use.
- A subscriber holds at most one message.  Reading the next message releases
  the previous one.
- Checksums, user metadata, `kReadNext` and `kReadNewest`, dropped message
  counting and activation messages work with both clients.

Reliable publishers and subscribers, virtual channels, subscriber queues,
split buffers and the memfd backend (always used on Android, and selected on
Linux with `SUBSPACE_LINUX_USE_MEMFD`) return `Error::kUnsupported`.
Channels created by standard publishers that resize their slots also have
more than one buffer and are rejected the same way.

## Building

Bazel:

```
bazelisk build //asil_client:asil_client //asil_client:phaser_connection
bazelisk test //asil_client:all
```

| Target | Contents | Dependencies |
|---|---|---|
| `//asil_client:asil_client` | Client, publisher, subscriber, shared memory | C++17, POSIX |
| `//asil_client:phaser_connection` | The standard server handshake | `asil_client`, `//proto:subspace_phaser` |
| `//proto:subspace_phaser` | Phaser messages generated from `subspace.proto` | phaser runtime, Abseil, cpp_toolbelt |

CMake builds the same libraries as `subspace_asil_client`,
`subspace_asil_phaser_connection` and `subspace_phaser`, with the tests
`asil_layout_test` and `asil_client_test`.  Phaser is fetched with
`FetchContent`, and its `protoc-gen-phaser` plugin is built for the host.  A
cross-compile needs a host plugin passed as `-DPHASER_PLUGIN_EXECUTABLE`; see
[the Android CMake build](android.md#building-with-cmake).  Without one, the
cross-compile builds only the core library.

Soong builds `libsubspace_asil_client`, `libsubspace_asil_phaser_connection`
and `libsubspace_phaser`, using the phaser modules from
`external/phaser/Android.bp.example`.

The core library takes `SUBSPACE_MAX_CHANNELS` from the same build setting as
the server, because it sizes the shared system control block.

## The Server Connection

The client talks to the server through the abstract `ServerConnection` in
`asil_client/server_connection.h`.  It has one call for each request the
client makes: `Init`, `CreatePublisher`, `CreateSubscriber`, `GetTriggers`,
`RemovePublisher` and `RemoveSubscriber`.

`PhaserServerConnection` implements it with the server's standard protocol:
length-prefixed messages in protobuf wire format on the server's Unix socket,
with file descriptors passed by `SCM_RIGHTS`.  The server receives the same
bytes it receives from the standard clients.  The connection builds each
request as a phaser message in a fixed buffer inside the object, serializes
it to protobuf wire format, and decodes the response into the same buffer,
so it needs neither the protobuf library nor the heap.  The buffers take
about 60 KB, so keep the connection in static storage or inside a long-lived
object.  A system with its own qualified transport can replace it with
another implementation of `ServerConnection`.

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

`Buffer()` returns the payload of the slot the publisher holds, and
`Metadata()` returns its metadata area.  `Publish(size)` sends the message and
claims the next slot.  It fills in an optional `PublishedMessage` with the
message's ordinal and timestamp.  When `checksum` is set the publisher
computes the same CRC32 as the standard clients.  When `activate` is set the
publisher sends the channel's activation message if no publisher has sent one.

## Subscribers

`ReadMessage(msg, mode)` fills in a `Message` with the payload, length,
ordinal, timestamp, slot, metadata and the number of messages dropped since
the previous one.  A length of 0 means there was nothing to read.  `PollFd()`
is readable when messages may be waiting, and `Wait(timeout_ms)` polls it.

With `checksum` set, a message with a bad checksum returns
`Error::kChecksumMismatch`.  With `pass_checksum_errors` also set, it is
delivered with `checksum_error` set.  Activation messages are skipped unless
`pass_activation` is set.

## Testing

`layout_test` compares every size, offset and constant in
`asil_client/shm_layout.h` with `common/channel.h`, and checks that the
checksum matches the standard client's.  `asil_client_test` runs a server with
a static channel config and exchanges messages between the ASIL client and the
standard client in both directions.  It also checks that setting up and
closing a publisher and a subscriber makes no heap allocations, and that a
request too large for the wire buffer returns `Error::kCapacityExceeded` and
leaves the connection usable.  It is skipped on memfd builds.
