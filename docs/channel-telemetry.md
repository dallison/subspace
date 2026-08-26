# Channel Telemetry

Channel telemetry reports server-observed changes to an existing channel. A
client creates a subscriber for the channel it wants to monitor and sets the
subscriber's telemetry option. The server then publishes `subspace.Telemetry`
protobuf messages containing participant, drop, and resize changes.

The target channel must already exist. A telemetry request for an unknown
channel returns an error.

## C++ API

Create a telemetry subscriber by setting `SubscriberOptions::SetTelemetry`:

```cpp
auto subscriber_or = client->CreateSubscriber(
    "camera",
    subspace::SubscriberOptions().SetTelemetry(true));
if (!subscriber_or.ok()) {
  return subscriber_or.status();
}
subspace::Subscriber subscriber = std::move(*subscriber_or);
```

The server chooses the telemetry channel's type, `subspace.Telemetry`; a type
set in the subscriber options is ignored. `Subscriber::Name()` continues to
return the monitored channel's public name.

`ReadTelemetryMessage()` reads the next message and deserializes it:

```cpp
absl::Status status = subscriber.Wait();
if (!status.ok()) {
  return status;
}

auto telemetry_or = subscriber.ReadTelemetryMessage();
if (!telemetry_or.ok()) {
  return telemetry_or.status();
}

std::shared_ptr<subspace::Telemetry> telemetry = *telemetry_or;
if (telemetry == nullptr) {
  // No message is currently available.
  return absl::OkStatus();
}

for (const auto &publisher : telemetry->publishers()) {
  // publisher.name() identifies the client.
  // publisher.change() is NONE, ADDED, or REMOVED.
}
```

The method returns `absl::StatusOr<std::shared_ptr<Telemetry>>`:

- A non-null pointer contains the decoded protobuf.
- A null pointer means that no message is currently available.
- A non-OK status reports an underlying read error or malformed protobuf data.

Both `ReadMode::kReadNext` and `ReadMode::kReadNewest` are supported.

## Message Contents

The first telemetry message is a snapshot of the channel's current publishers
and subscribers. Snapshot participants have `Telemetry::NONE` as their change.

Later messages contain changes accumulated over a one-second batching period:

- `publishers`: publisher client names with `ADDED` or `REMOVED`.
- `subscribers`: subscriber client names with `ADDED` or `REMOVED`.
- `drops`: the number of messages dropped since the previous telemetry batch.
- `resizes`: each new channel slot size observed since the previous batch.

The server does not publish an empty batch. Consequently, after the initial
snapshot, no telemetry message is sent during periods with no changes.

## Lifecycle and Visibility

Telemetry resources are lazy and per monitored channel:

1. The first telemetry subscriber starts the channel's telemetry publisher and
   batching coroutine.
2. Additional telemetry subscribers share that publisher.
3. Removing the last telemetry subscriber stops the coroutine and removes the
   internal telemetry channel.

Internal telemetry channels are hidden from the channel directory, channel
information, and channel statistics. Their generated names are implementation
details and should not be used by clients.

Telemetry subscribers cannot be bridge, tunnel, or virtual-channel
subscribers. Telemetry observes the target channel on the connected server.

## Other Client Bindings

The C, Python, and Rust clients also provide decoded telemetry reads.

### C

`subspace_read_telemetry_message()` returns an owned `SubspaceTelemetry`.
`telemetry.telemetry == NULL` means no message is available. Free a non-empty
result with `subspace_free_telemetry()`:

```c
SubspaceSubscriberOptions options = subspace_subscriber_options_default();
options.telemetry = true;

SubspaceTelemetry telemetry = subspace_read_telemetry_message(subscriber);
if (telemetry.telemetry != NULL) {
  for (size_t i = 0; i < telemetry.num_publishers; ++i) {
    const SubspaceTelemetryParticipant *publisher = &telemetry.publishers[i];
    /* Use publisher->name and publisher->change. */
  }
  subspace_free_telemetry(&telemetry);
}
```

Use `subspace_read_telemetry_message_with_mode()` to select the next or newest
read mode.

### Python

`read_telemetry_message()` returns a `Telemetry` object or `None`:

```python
options = subspace.SubscriberOptions().set_telemetry(True)
subscriber = client.create_subscriber("camera", options=options)

telemetry = subscriber.read_telemetry_message()
if telemetry is not None:
    for publisher in telemetry.publishers:
        print(publisher.name, publisher.change)
```

The exposed entry types are `TelemetryPublisher`, `TelemetrySubscriber`,
`TelemetryDrop`, and `TelemetryResize`. Changes use the `TelemetryChange` enum.

### Rust

`read_telemetry_message()` returns `Result<Option<proto::Telemetry>>`:

```rust
let options = SubscriberOptions::new().set_telemetry(true);
let subscriber = client.create_subscriber("camera", &options)?;

if let Some(telemetry) =
    subscriber.read_telemetry_message(ReadMode::ReadNext)?
{
    for publisher in telemetry.publishers {
        println!("{} {}", publisher.name, publisher.change);
    }
}
```
