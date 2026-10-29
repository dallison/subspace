# Tunnel-Compatible Discovery and Bridging

The Subspace server can exchange channels with peers that speak the cruise
IPC tunnel and discovery protocols. This tunnel mode runs alongside native
Subspace discovery and bridging. The two never carry each other's traffic: a
message that arrives through the tunnel is not forwarded over a native bridge,
and a message that arrives over a native bridge is not sent into the tunnel.

The tunnel uses TCP over IPv4 only.

## How It Works

Each server listens on one TCP port, `listen_port`, for both discovery and
data. Every connection carries one channel in one direction, from the server
that sends it to the server that receives it. A connection starts with a
handshake packet that names the channel and describes how it is set up, and
then carries one data packet per message. The receiving server never sends
anything back.

Discovery is a channel too: `/ipc/discovery/proto/receive`. Each server opens
a discovery connection to each of its peers, and its messages on that
connection list the channels the server publishes and subscribes to. When a
peer says it subscribes to a channel that this server publishes, this server
opens a data connection to the peer for that channel. A static send route
opens the connection without waiting for discovery.

Here `lidar-box` publishes `/lidar/points` and `compute` subscribes to it:

```
lidar-box                                compute
publishes /lidar/points                  subscribes to /lidar/points

connects to compute:7400 --------------> discovery receiver
  Handshake  /ipc/discovery/proto/receive
  Data       Discovery: publishes /lidar/points

discovery receiver <-------------------- connects to lidar-box:7400
                                           Handshake  /ipc/discovery/proto/receive
                                           Data       Discovery: subscribes to /lidar/points

connects to compute:7400 --------------> publishes on /lidar/points
  Handshake  /lidar/points, slot size, slot count, ...
  Data       prefix + message
  Data       prefix + message
  ...
```

With discovery on, two peers keep two discovery connections open, one opened
by each. They also have one data connection for each channel that one of them
sends the other.

Inside each server, the tunnel is a client of that server. For each channel
it sends, it subscribes to the channel and forwards what it reads. For each
channel it receives, it publishes what arrives. See
[Channels the Tunnel Uses](#channels-the-tunnel-uses) for how those
publishers and subscribers are marked.

## Enabling the Tunnel

Two command line flags control the tunnel:

| Flag | Meaning |
|------|---------|
| `--tunnel` | Turn on tunnel discovery and bridging. |
| `--tunnel_config=<path>` | The tunnel configuration file, in protobuf text format. Required with `--tunnel`. |

```bash
subspace_server --tunnel --tunnel_config=/etc/subspace/tunnel.textproto
```

`--tunnel_config` without `--tunnel` is ignored with a warning.
`--local --tunnel` turns off native discovery and bridging and runs only the
tunnel.

The server exits at startup if it can't read or parse the configuration file,
if the configuration is invalid, or if it can't listen on the tunnel port.

A program that embeds the server calls `Server::SetTunnelConfig` before
`Server::Run`.

## Configuration

The file holds a `subspace.TunnelConfig` message, defined in
`proto/tunnel_config.proto`. This example uses every field:

```textproto
# This server's name in discovery messages.  Defaults to --machine, then the
# host name.
device_name: "lidar-box"

# Where peers connect for both discovery and data.
listen_address: "0.0.0.0"
listen_port: 7400

peers {
  name: "compute"        # Must match the peer's own device name.
  tcp_addr: "10.0.0.2"   # Host name or IPv4 address.
  tcp_port: 7400         # The peer's listen port.
}
peers {
  name: "logger"
  tcp_addr: "logger.local"
  tcp_port: 7400
  disable_discovery: true  # Only static routes run with this peer.
}

# Channels discovery never announces or sends.
discovery_denylist: "/lidar/raw_debug"

# Resend the full discovery message this often, in seconds.  Absent means
# 5; 0 sends only when something changes or a connection is made.
discovery_refresh_secs: 5

# True exchanges no discovery with any peer, leaving only static routes.
disable_discovery: false

# Send /lidar/points to logger whether or not it asks for it.
send_routes {
  topic: "/lidar/points"
  peers: "logger"
}

# Announce the subscription to /camera/front only to compute.  If the tunnel
# creates the channel here, it goes on virtual channel 3 of /camera/mux.
receive_routes {
  topic: "/camera/front"
  peers: "compute"
  mux: "/camera/mux"
  vchan_id: 3
}
```

| Field | Default | Notes |
|-------|---------|-------|
| `device_name` | `--machine`, else the host name | Must not be empty and must not match any peer's name. |
| `listen_address` | All interfaces | Must resolve to an IPv4 address. |
| `listen_port` | None | Required. 1 to 65535. |
| `peers` | None | The servers this one exchanges channels with. |
| `discovery_denylist` | Empty | Exact channel names, not prefixes or patterns. |
| `discovery_refresh_secs` | 5 | Must not be negative. 0 turns off the periodic resend. |
| `disable_discovery` | `false` | See [Turning Off Discovery](#turning-off-discovery). |
| `send_routes` | None | See [Static Routes](#static-routes). |
| `receive_routes` | None | See [Static Routes](#static-routes). |

Each entry in `peers` has these fields:

| Field | Default | Notes |
|-------|---------|-------|
| `name` | None | Required and unique. Must equal the peer's own device name, because discovery messages identify their sender by it. |
| `tcp_addr` | None | Required. A host name or IPv4 address. |
| `tcp_port` | None | Required. The peer's `listen_port`, 1 to 65535. |
| `disable_discovery` | `false` | Exchange no discovery with this peer. |

A peer whose address doesn't resolve at startup is logged and retried every
3 seconds. It doesn't stop the server.

### Validation

The server checks the whole configuration at startup and reports every
problem in one error, for example `invalid tunnel configuration: peer logger
has no tcp_addr; send route /lidar/points names unknown peer "loger"`. These
are the rules:

- The device name, after the defaults, must not be empty.
- `listen_port` is required and must be a valid TCP port.
- `discovery_refresh_secs` must not be negative.
- Each peer needs a name, a `tcp_addr` and a valid `tcp_port`. Names must be
  unique and must differ from this server's device name.
- Each route needs a topic and at least one peer. A topic can appear in only
  one send route and one receive route, and it can't be an
  [internal channel](#channels-the-tunnel-uses). Every peer a route names must
  be in `peers`, and only once.
- A receive route with a `vchan_id` needs a `mux`, and the ID must be between
  0 and 1022.

## Discovery

### The Discovery Connection

For each peer with discovery on, the server connects to the peer's
`tcp_addr` and `tcp_port` and sends a handshake for
`/ipc/discovery/proto/receive` with these values:

| Handshake field | Value |
|-----------------|-------|
| `topic_name` | `/ipc/discovery/proto/receive` |
| `channel_type` | Empty |
| `slot_size` | 256 |
| `num_slots` | 24 |
| `is_reliable` | `false` |
| `mux` | Empty |
| `vchan_id` | -1 |
| `checksum_size` | 4 |
| `metadata_size` | 0 |

If the connection fails or closes, the server tries again after 3 seconds,
or sooner if it has something new to announce. The connection attempt times
out after 3 seconds.

### Discovery Messages

Each discovery message is one data packet on the discovery connection (see
[Wire Format](#wire-format)). The packet body is a 64-byte message prefix
followed by a serialized `subspace.tunnel.Discovery`
(`proto/tunnel_discovery.proto`). This matches the cruise `DiscoveryProto`
message field for field. In the prefix, the server sets `message_size` to the
length of the serialized message, `ordinal` to the packet's sequence number,
`timestamp`, `vchan_id` to -1 and `checksum_size` to 4. Every other field,
including the checksum, is zero.

| Message | Field | Number | Meaning |
|---------|-------|--------|---------|
| `Discovery` | `device_name` | 1 | The sender's device name. Required. |
| | `published_topics` | 2 | Channels the sender publishes. |
| | `subscribed_topics` | 3 | Channels the sender subscribes to. |
| `DiscoveryTopic` | `topic` | 1 | The channel name. |
| | `datatype` | 2 | The channel's type. |
| | `mux` | 3 | Published topics only: the multiplexer of a virtual channel, else empty. |
| | `vchan_id` | 4 | Published topics only: the virtual channel ID, else -1. |
| | `is_reliable` | 5 | Published topics only: true if any local publisher is reliable. |

Each message lists everything the server announces to that peer, not just
what changed. Announcements are additive. A channel that disappears locally
stays known to the peer until the peer restarts.

### What Is Announced

A channel is announced as published if a program other than a bridge
publishes it, and as subscribed if a program other than a bridge subscribes
to it. The tunnel's own publishers and subscribers, and native bridges, don't
count. These channels are never announced:

- channels in `discovery_denylist`,
- [internal channels](#channels-the-tunnel-uses),
- multiplexer channels (virtual channels on them are announced under their
  own names), and
- local channels.

A subscription that has a receive route is announced only to the route's
peers. Publications are announced to every peer with discovery on.

### When Messages Are Sent

The server sends a discovery message to a peer:

- when the connection is made, or remade after a failure,
- when what it would announce changes, for example when a channel gets its
  first publisher or subscriber, or its first reliable publisher,
- when a peer newly announces a channel the server subscribes to, so the peer
  learns of the subscription right away, and
- every `discovery_refresh_secs`.

### Receiving Discovery

Discovery from peers arrives on connections that peers open to
`listen_port`. The server identifies the sender by the message's
`device_name`, not by the connection's address, so a peer behind NAT or a
port forward is still recognized.

- Messages from a device that isn't listed in `peers` are ignored, with a
  warning the first time each unknown device name appears.
- Messages from a peer with discovery off are ignored. See
  [Turning Off Discovery](#turning-off-discovery).
- Messages that don't decode, or that have no device name, are skipped with
  a warning, and the connection stays open.
- A message larger than 16 MiB closes the connection.

The server uses only the topic names from a peer's discovery. The type, size,
reliability and virtual channel of a channel come from the handshake of the
data connection that carries it.

When a peer announces that it subscribes to a channel the server publishes,
the server opens a data connection to the peer for it. When a peer announces
that it publishes a channel the server subscribes to, the server sends its
own discovery again so the peer connects sooner. Channels that are excluded
on this server are ignored in both cases, so the server never sends an
excluded channel because of discovery, even if a peer announces that it
subscribes to it.

### Other Discovery Formats

Only the protobuf format is supported. Cruise peers need a build that sends it,
with `discovery_protocol` set to `protobuf` or `both` (the default). The older
ROS-encoded discovery that such peers may also send on `/ipc/discovery/receive`
is read and discarded, as is anything a peer sends on another internal
channel. Those connections are kept open so the peer doesn't keep
reconnecting.

### Native Discovery

Native Subspace discovery (UDP `Query`, `Advertise` and `Subscribe` messages,
or the same messages over `--tcp_discovery`) keeps running alongside the
tunnel unless `--local` is set. It uses its own ports and connections and is
described in [Server Architecture](server-architecture.md#bridge-and-discovery-system).
Neither discovery system announces a channel only because the other one feeds
it.

## Static Routes

Static routes name channels and peers in the configuration, so data flows
whether or not discovery asks for it. They work with discovery on or off.

### Send Routes

A send route sends a channel to the peers it names. As soon as a program
other than a bridge publishes the channel on this server, the server opens a
data connection to each of those peers, whether or not they have announced
that they subscribe.

- The denylist doesn't apply to send routes. A send route can send a
  denylisted channel, but discovery still doesn't announce it.
- Messages that arrived over the tunnel or a native bridge are never sent, so
  a send route needs a publisher on this server.
- Local channels and multiplexer channels are never sent, even with a send
  route.

### Receive Routes

A receive route limits discovery for one channel and says how to create it:

- The server's subscription to the channel is announced only to the route's
  peers, so only they connect to send it. A receive route doesn't subscribe
  by itself: there is nothing to announce until a program on this server
  subscribes.
- If the channel doesn't exist on this server when a peer's data connection
  arrives, the tunnel creates it on the route's `mux` and `vchan_id`. Without
  a `vchan_id` the server picks one. Without a `mux` the channel follows the
  sender's handshake.
- If the channel already exists, its settings win. A program that subscribes
  before any data arrives creates the channel, so with discovery on, a
  route's `mux` and `vchan_id` have no effect. A program that subscribes
  later with the same `mux` gets the route's virtual channel.

Receive routes don't limit which peers may send. The server accepts a data
connection for any channel from any host.

### Example: Routes Without Discovery

Two servers with no discovery. `alpha` sends `/static/out` to `beta`, and
`beta` puts it on virtual channel 6 of `/static/mux`:

```textproto
# alpha
device_name: "alpha"
listen_port: 7400
peers { name: "beta" tcp_addr: "10.0.0.3" tcp_port: 7400 }
disable_discovery: true
send_routes { topic: "/static/out" peers: "beta" }
```

```textproto
# beta
device_name: "beta"
listen_port: 7400
peers { name: "alpha" tcp_addr: "10.0.0.2" tcp_port: 7400 }
disable_discovery: true
receive_routes {
  topic: "/static/out"
  peers: "alpha"
  mux: "/static/mux"
  vchan_id: 6
}
```

## Turning Off Discovery

Discovery can be turned off for every peer or for some of them. Data
connections and static routes are unaffected either way.

- `disable_discovery: true` at the top level: the server opens no discovery
  connections. Discovery connections from peers are read and discarded, and
  kept open so the peers don't keep reconnecting. Only send routes start
  data connections.
- `disable_discovery: true` on a peer: the server opens no discovery
  connection to that peer and ignores discovery from it, logging that once at
  info level. Discovery with the other peers is unaffected.

A receive route's peers are still used for discovery with the peers that have
it on, and its `mux` and `vchan_id` apply whether or not discovery is on.

## Data Connections

### Sending

The server opens a data connection when a peer subscribes to a channel that
the server publishes, or when a send route names the channel and peer. There
is one connection per channel and peer. It goes to the peer's `tcp_addr` and
`tcp_port`. If the connection fails, the server tries again every 3 seconds.
A connection stays in place until the server stops, even if the local
publisher goes away.

The handshake describes the channel as it is on the sending server. The
tunnel then sends every message published on this server, and skips messages
that arrived over the tunnel or a native bridge.

### Receiving

Connections from peers arrive on `listen_port`. The first packet names the
channel and describes how the sender's channel is set up. The server then
publishes the incoming messages on the local channel of that name.

- If the local channel doesn't exist, the server creates it with the sender's
  slot size, slot count, multiplexer and virtual channel ID, or with a
  receive route's multiplexer and virtual channel ID.
- If it exists, the local channel's multiplexer, virtual channel ID, fixed
  size and slot count limits win, so the tunnel never conflicts with local
  publishers.
- If the local channel's type is different, the tunnel publishes without a
  type.
- Reliability, checksum size and metadata size come from the sender. If the
  local channel's prefix size doesn't match the sender's, the server logs an
  error and closes the connection, and the sender tries again.

### Reliability

A reliable channel stays reliable across the tunnel. The sending server
subscribes reliably if any local publisher is reliable, so its publishers wait
when the tunnel falls behind. The receiving server publishes reliably and
waits for its own reliable subscribers. A send connection takes its
reliability from the channel when it starts. If a reliable publisher appears
later, discovery announces the channel as reliable, but connections already
running stay as they are.

### Checksums and Virtual Channels

The tunnel never changes or recomputes a checksum. A checksum verifies across
it whenever both ends compute it the same way, including with a custom checksum
callback, and cruise peers do once they're built with this version of
Subspace. The receiving server leaves every field the checksum covers as the
sender wrote it:

- A virtual channel's ID can differ between machines. The message is delivered
  on the receiving server's virtual channel, and its prefix keeps the sender's
  ID, the same way it keeps the sender's slot ID, ordinal and timestamp.
- Flags cross the tunnel as they are. The receiving server also marks the
  message bridged and cross-machine, and the checksum doesn't cover those
  flags.

User metadata in the message prefix crosses the tunnel unchanged.

### Message Sizes

Tunnel channels resize like any other Subspace channel, and there is no
maximum message size of their own. An incoming message larger than the current
slot size makes the channel grow, in either direction. The limits are:

- A message is dropped if it is larger than a fixed-size channel's slot size
  or the channel's maximum slot size. The receiving server logs a warning.
- The packet header has a 32-bit length, so a message whose packet would be
  larger than 2 GiB (`INT32_MAX` bytes) is not sent. The sending server logs a
  warning.

### Message Flags

Messages that came in over the tunnel are marked as bridged and cross-machine.
The server doesn't send these messages back out over the tunnel or over a
native bridge, which prevents echoes between peers.

## Wire Format

### Packets

Each connection carries a sequence of packets. A packet is a 25-byte header
followed by a body. The header is packed and little-endian:

| Offset | Size | Field | Meaning |
|--------|------|-------|---------|
| 0 | 4 | Magic | `0xF0CACC1A` |
| 4 | 4 | Packet length | Header and body together, so at least 25. |
| 8 | 1 | Type | 1 is a handshake, 2 is data and 3 is an operation. |
| 9 | 8 | Send time | `CLOCK_REALTIME` nanoseconds when the packet was sent. |
| 17 | 8 | Sequence number | Data packets are numbered from 1 on each connection. Other packets use 0. |

A packet with the wrong magic number, an unknown type, or a length shorter
than the header closes the connection. The receiving server doesn't use the send time or sequence
number. It never sends operation packets and skips any it receives. It also
skips packets that arrive before the handshake.

### Handshake

The handshake body is a serialized `subspace.tunnel.Handshake`
(`proto/tunnel_handshake.proto`). The field numbers are part of the wire
format.

| Message | Field | Number | Meaning |
|---------|-------|--------|---------|
| `Handshake` | `topic_name` | 1 | The channel the connection carries. Required. |
| | `channel_type` | 2 | The channel's type, as bytes. May be empty. |
| | `subspace` | 4 | A `SubspaceTransport`. Required. |
| `SubspaceTransport` | `slot_size` | 1 | The sender's slot size. 0 means 256. |
| | `num_slots` | 2 | The sender's slot count. 0 means 24. |
| | `is_reliable` | 3 | Whether the channel is reliable. |
| | `mux` | 4 | The sender's multiplexer, or empty. |
| | `vchan_id` | 5 | The sender's virtual channel ID, or -1. |
| | `checksum_size` | 6 | Bytes of checksum in the prefix. Absent, or 0 or less, means 4. At most 65535. |
| | `metadata_size` | 7 | Bytes of user metadata in the prefix. 0 to 65535. |

The server closes a connection whose handshake body is larger than 16 MiB,
doesn't parse, or breaks these limits. It also closes one with a slot size
this build can't hold, or a slot count above `INT32_MAX`. A second handshake
on the same connection closes it too.

The whole handshake must arrive within 5 seconds of the server accepting the
connection. If it doesn't, the server logs a warning and closes the
connection, even if the peer sent other packets in the meantime.

### Data Packets

A data packet's body is the message's prefix area followed by its payload.
The prefix area is the sender's Subspace message prefix, exactly as it is in
shared memory, including the checksum and user metadata. Its size is
`Aligned<64>(48 + checksum_size + metadata_size)` bytes, using the sizes from
the handshake, so it is 64 bytes with the defaults.
[Checksums and User Metadata](checksums-and-metadata.md#message-prefix-layout)
describes the layout. The prefix is in the sender's byte order, which is
little-endian on every platform Subspace supports.

The receiving server drops a data packet if:

- its body is shorter than the prefix area,
- the prefix's `message_size` doesn't equal the payload length,
- the payload is empty,
- the message is a reliable activation message, or
- the message is too large for the channel (see
  [Message Sizes](#message-sizes)).

## Channels the Tunnel Uses

| Channel | Use |
|---------|-----|
| `/ipc/discovery/proto/receive` | Protobuf discovery. Each server opens one connection to each peer that has discovery on. |
| `/ipc/discovery/receive` | Older ROS-encoded discovery that cruise peers may send. Read and discarded. |
| Internal channels | Never announced, sent or routed. Connections for them are read and discarded. |
| Every other channel | Carried when discovery or a static route asks for it. |

Internal channels are those whose names start with `/subspace/`, or contain
`/subspace/channel_directory`, `/framework/ipc_information` or
`/ipc/discovery/`. They include the server's own channel directory and
statistics channels, and both discovery channels.

On each server, the tunnel's publishers and subscribers are bridge users with
`for_tunnel` set. They connect as clients named `tunnel ingress <channel>`
and `tunnel egress <channel>`. `GetChannelInfo` counts them in
`num_tunnel_pubs` and `num_tunnel_subs`. Because they are bridge users,
neither tunnel nor native discovery treats them as publishers or subscribers.
A channel that only the tunnel feeds isn't announced to other peers.

## Platforms

The tunnel builds with Bazel, CMake and Soong on Linux, macOS, Android and
QNX. CI runs the tunnel tests on Linux and macOS with both coroutine
backends, and on the Android x86_64 emulator with Bazel (both backends) and
CMake. For QNX, `.github/scripts/qnx-deploy-client-test.sh` stages the tunnel
tests with `client_test` for a QNX VM.

### Between the Android Emulator and the Host

The emulator's network only lets the guest open connections, so the host
reaches the guest through `adb forward` and the guest reaches the host through
`adb reverse`. Both sides open connections to each other, so each needs its own
listen port:

```bash
adb reverse tcp:7400 tcp:7400   # guest -> host
adb forward tcp:7401 tcp:7401   # host -> guest
```

Host configuration:

```textproto
device_name: "host"
listen_port: 7400
peers { name: "emulator" tcp_addr: "127.0.0.1" tcp_port: 7401 }
```

Guest configuration:

```textproto
device_name: "emulator"
listen_port: 7401
peers { name: "host" tcp_addr: "127.0.0.1" tcp_port: 7400 }
```

## Logging

At startup the server logs its device name, listen address, and the number
of peers and routes, and whether discovery is off. It logs when connections
open and close, and when messages are dropped. When an outgoing connection
fails the same way on every 3-second retry, the failure is logged at info
level once and at debug level after that, until it changes. Discovery from an
unknown device, and discovery ignored from a peer with discovery off, are
each logged once.

## Limitations

- Only TCP over IPv4 is supported.
- Only protobuf discovery is supported. Peers that send only ROS-encoded
  discovery can't exchange channels with the server through discovery, but
  static routes don't depend on it.
- Discovery is additive. Channels are never withdrawn, and data connections
  stay open until the server stops.
- Connections are neither authenticated nor encrypted. Any host that can
  reach `listen_port` can publish on any channel that isn't internal, so
  restrict access to the port with a firewall.
