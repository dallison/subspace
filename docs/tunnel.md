# Tunnel-Compatible Discovery and Bridging

The Subspace server can exchange channels with peers that speak the cruise
IPC tunnel and discovery protocols. This tunnel mode runs alongside native
Subspace discovery and bridging. The two never carry each other's traffic: a
message that arrives through the tunnel is not forwarded over a native bridge,
and a message that arrives over a native bridge is not sent into the tunnel.

The tunnel uses TCP only.

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

## Configuration File

The file holds a `subspace.TunnelConfig` message, defined in
`proto/tunnel_config.proto`:

```textproto
# This server's name in discovery messages.  Defaults to --machine, then the
# host name.
device_name: "lidar-box"

# Where peers connect for both discovery and data.
listen_address: "0.0.0.0"
listen_port: 7400

peers {
  name: "compute"        # Must match the peer's own device name.
  tcp_addr: "10.0.0.2"   # Host name or IP address.
  tcp_port: 7400         # The peer's listen port.
}
peers {
  name: "logger"
  tcp_addr: "logger.local"
  tcp_port: 7400
}

# Channels never announced to peers or sent to them.
discovery_denylist: "/lidar/raw_debug"

# Resend the full discovery message this often, in seconds.  Absent means
# 5; 0 sends only when something changes or a connection is made.
discovery_refresh_secs: 5
```

| Field | Default | Notes |
|-------|---------|-------|
| `device_name` | `--machine`, else the host name | Must not be empty and must not match any peer's name. |
| `listen_address` | All interfaces | Must resolve to an IPv4 address. |
| `listen_port` | None | Required. 1 to 65535. |
| `peers` | None | Each peer needs a unique, non-empty `name`, a `tcp_addr` and a valid `tcp_port`. |
| `discovery_denylist` | Empty | Exact channel names, not prefixes or patterns. |
| `discovery_refresh_secs` | 5 | Must not be negative. |

A peer whose address doesn't resolve at startup is logged and retried every
3 seconds. It doesn't stop the server.

## Discovery

Each server keeps one discovery connection open to every configured peer,
using the cruise discovery topic `/ipc/discovery/receive`. Over it, the server
sends its device name and the channels it publishes and subscribes to. The
server sends the message:

- when the connection is made, or remade after a failure,
- when its set of channels changes,
- when a peer newly announces a channel the server subscribes to, so the peer
  learns of the subscription right away, and
- every `discovery_refresh_secs`.

Announcements are additive. A channel that disappears locally stays known to
the peer until the peer restarts.

A channel is announced only if a program other than a bridge publishes or
subscribes to it. The following channels are never announced:

- channels in `discovery_denylist`,
- internal channels: names starting with `/subspace/` or containing
  `/subspace/channel_directory`, `/framework/ipc_information` or
  `/ipc/discovery/`,
- multiplexer channels (virtual channels on them are announced under their
  own names), and
- local channels.

The server never sends an excluded channel, even if a peer announces that it
subscribes to it. The denylist applies only to discovery and sending; a peer
that connects to send a denylisted channel is accepted.

Discovery messages from a device that isn't listed in `peers` are ignored, with
a warning the first time each unknown device name appears.

## Data

When a peer subscribes to a channel that the server publishes, the server
opens a TCP connection to the peer for that channel and sends each message on
it. There is one connection per channel and peer. If the connection fails, the
server tries again every 3 seconds. A connection stays in place until the
server stops, even if the local publisher goes away.

Connections from peers arrive on `listen_port`. The first packet names the
channel and describes how the sender's channel is set up. The server then
publishes the incoming messages on the local channel of that name.

- If the local channel doesn't exist, the server creates it with the sender's
  slot size, slot count, multiplexer and virtual channel ID.
- If it exists, the local channel's multiplexer, virtual channel ID, fixed
  size and slot count limits win, so the tunnel never conflicts with local
  publishers.
- If the local channel's type is different, the tunnel publishes without a
  type.
- Reliability, checksum size and metadata size come from the sender.

Checksums and user metadata in the message prefix cross the tunnel unchanged.

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

## Logging

The server logs when the tunnel starts listening, when connections open and
close, and when messages are dropped. When an outgoing connection fails the
same way on every 3-second retry, the failure is logged at info level once
and at debug level after that, until it changes.

## Limitations

- Only TCP is supported.
- Routes come from discovery; there are no static routes.
- All peers share the same options; there are no per-peer settings.
