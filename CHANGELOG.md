# CHANGELOG.md

## Unreleased

### Static Channel Config
- `subspace_server --channel_config=FILE` reads a `StaticChannelConfig` in
  protobuf text format and creates its channels and multiplexers before the
  server accepts clients. Clients can then use only those channels and the
  server's own, the channels stay when they have no users, and the server
  defines their layout.
- Publishers on a static channel must set the new `server_defined` option
  (`PublisherOptions::SetServerDefined`, `server_defined` in the C, Python,
  Rust and ASIL clients) and can't set the slot size, number of slots, fixed
  size, checksum size, metadata size, subscriber queue arena size or split
  buffer setting. The server rejects `server_defined` on any other channel.
  `CreatePublisherResponse` now reports the channel's slot size, checksum
  size, metadata size, split buffer setting and fixed size, and a
  `server_defined` publisher uses them.
- `subspace_publisher_options_server_defined()` in the C client gives
  options for a static channel.
- The default `checksum_size` in the C++ and Rust `PublisherOptions` is now 0,
  which means 4 bytes as before, so a `server_defined` publisher can use the
  default options.
- A multiplexer with no publishers now reports its slot size.
- Publishers on different virtual channels of a multiplexer are checked
  against each other for fixed size and locality, as they share the
  multiplexer's buffers. Before, a second fixed-size publisher on another
  virtual channel was rejected, so a static multiplexer could only have
  publishers on one virtual channel.
- The server creates the message buffers of every static channel and
  multiplexer when it starts, so subscribers can map them before any
  publisher exists.
- `use_split_buffers` in a static channel or multiplexer config gives it
  split buffers, which the server creates.
- `GetClientBuffersRequest` can ask for one prefix or slot of a split buffer
  set with `filter_slot`, `is_prefix` and `slot_id`.

### ASIL Client
- `asil_client/` is a new C++17 client for safety-related software. It
  shares channels with the standard clients through the same shared memory
  protocol, using only the standard library and
  POSIX. It is built without exceptions or RTTI, returns an error code from
  every operation, and allocates no memory while publishing or reading.
  Checksums, metadata, `ReadNewest`, dropped message counts and activation
  messages work with the standard clients. The server handshake is behind a
  replaceable `ServerConnection` interface. `PhaserServerConnection`
  implements the standard protocol with phaser messages in fixed buffers,
  sent and received as the native Phaser payload, so the handshake makes no
  heap allocations either. See `docs/asil-client.md`.
- The ASIL client also supports reliable publishers and subscribers, virtual
  channels and multiplexer subscribers, subscribers holding up to 64
  messages, subscriber queues, split buffers (with C callbacks for slots from
  a custom allocator), and the memfd backend. Reading the newest message on
  a reliable subscriber marks the skipped messages as read.
- ASIL publishers and subscribers map all of a channel's shared memory when
  they open, and publishing and reading make no shared memory calls. A
  subscriber needs the channel to have buffers when it opens: always true of
  static channels, otherwise a publisher must have opened. Messages in a
  buffer that a standard publisher created by resizing after the subscriber
  opened return `kBufferNotMapped`.
- The standard C++ client builds its server requests and responses as
  phaser messages too, in growable buffers. `ServerWire` in
  `common/server_wire.h` encodes them for both C++ clients, with fixed or
  growable buffers. The socket carries the native Phaser payload. The Rust
  client sends and parses that same payload. Bridge, tunnel, discovery, the
  channel directory and statistics stay on protobuf.
- The ASIL client and the standard C++ client share the shared memory
  protocol code in `common/`: finding a slot, publishing, the checksum, and
  taking, releasing and skipping messages. Sharing it fixed these bugs in the
  standard client:
  - A reliable subscriber reading the newest message didn't mark the
    messages it skipped as seen, so a reliable publisher could no longer get
    a slot once the free slots ran out.
  - A subscriber ignoring an activation message kept its owner bit on the
    slot. If the process then died, the server dropped a reference from
    whatever message was in that slot later.
  - With retirement notification on, releasing a bridged message crashed if
    its slot on the other side of the bridge had a higher id than any slot
    in this channel.
- Bazel, CMake and Soong build phaser and generate `subspace_phaser` from
  `subspace.proto`. Android CMake cross-compiles need a host
  `protoc-gen-phaser` in `PHASER_PLUGIN_EXECUTABLE`.
- Abseil 20250814.1, protobuf 33.4, cpp_toolbelt 2.1.6 and phaser 3.1.1 in
  the Bazel build. CMake uses cpp_toolbelt 2.1.6.

### Bridge and Tunnel Plugins
- Server plugins can change messages as they cross a native bridge or the
  tunnel. `onSend` runs on the way out and `onReceive` on the way in. If a plugin
  changes the payload, the server writes a new checksum. `SetChecksumSize`
  and `SetChecksumCallback` tell it the checksum length and function, the
  same way a client does. The default is a 4-byte CRC32. An unchanged message
  keeps the checksum its publisher wrote. Rebuild existing plugins against
  this server; the plugin interface gained these two functions.

### Mutual TLS
- Bridge connections, including retirement sockets, and tunnel connections
  can require a certificate from both sides. The server uses BoringSSL
  (Bazel) or OpenSSL (CMake). Build it in with `--config=mtls` or
  `-DSUBSPACE_ENABLE_MTLS=ON`; a server built without it keeps the cleartext
  protocols. `--mtls_cert`, `--mtls_key`, and `--mtls_ca` turn it on at run
  time, and `Server::SetMtlsCredentials` does the same for an embedded
  server. See `docs/mtls.md`.

### Tunnel-Compatible Discovery and Bridging
- The server can exchange channels over TCP with peers that use the Cruise
  IPC tunnel and discovery protocols. `--tunnel` turns it on, and
  `--tunnel_config` names a configuration file in protobuf text format
  (`proto/tunnel_config.proto`) that lists the device name, listen port,
  peers, discovery denylist and refresh interval. Programs that embed the
  server call `Server::SetTunnelConfig`. See `docs/tunnel.md`.
- Tunnel discovery uses the Cruise protobuf format on
  `/ipc/discovery/proto/receive` (`proto/tunnel_discovery.proto`). ROS-encoded
  discovery is not supported; connections that carry it are ignored.
- Static routes carry channels without discovery. A `send_routes` entry sends
  a channel published on this server to the peers it names, whether or not
  they ask for it. A `receive_routes` entry announces a subscription only to
  the peers it names, and sets the multiplexer and virtual channel ID used if
  the tunnel creates the channel.
- `disable_discovery` turns tunnel discovery off for every peer, or, on a
  `peers` entry, for that peer. Data connections and static routes still run.
- `docs/tunnel.md` describes the tunnel's wire format, discovery protocol and
  channels.
- An incoming tunnel connection is closed if its handshake doesn't arrive
  within 5 seconds.
- The tunnel runs alongside native discovery and bridging, and the two never
  forward each other's messages. With `--local --tunnel` only the tunnel runs.
- Tunnel channels resize with their messages and have no maximum message
  size of their own. Messages too large for a fixed-size or capped channel
  are dropped on arrival, and messages whose packet would exceed 2 GiB are
  not sent. Both cases are logged.
- The tunnel never changes or recomputes a message's checksum, so custom
  checksum callbacks work across it. Flags cross unchanged, and a message
  whose virtual channel has a different ID on the receiving server keeps the
  sender's ID in its prefix.
- `Publisher::PublishMessageWithPrefix` takes an optional
  `use_vchan_id_from_prefix`. When it's false, the message goes to the
  publisher's own virtual channel and the prefix keeps the sender's
  `vchan_id`.
- CMake and Android builds now generate the tunnel protos alongside
  `subspace.proto`.
- `tunnel_stress_test` covers high message rates on reliable and unreliable
  channels, many channels flowing both ways at once, and a stream that carries
  on while each server in turn is killed and restarted. On an unreliable
  channel messages can be dropped, but what arrives is in order and ends with
  the last message sent. `tunnel_test` checks that two servers connected both
  natively and through the tunnel receive each message twice and that nothing
  loops.

### Asio Waits and Shutdown
- With the Boost.Asio backend, `async::WaitReadable`, `WaitWritable` and
  `WaitEither` no longer return before the fd is ready. Asio's reactor can
  deliver a readiness event left over from an earlier wait, so the waits now
  check with `poll()` and wait again if the fd isn't ready. Tunnel connections
  stalled on this, because a readable connection looks like the peer closing
  it.
- On Linux, a coroutine that waits on an fd another coroutine is already
  waiting on now gets an `InternalError` at once. It used to throw, which
  ended the coroutine without a trace. On macOS, asio's kqueue reactor
  accepts the second wait and never wakes the first, so two coroutines must
  never wait on the same fd at once.
- A cancelled wait, as at graceful shutdown, returns `CancelledError` instead
  of `InternalError`, and a client whose request is cancelled no longer
  reconnects. A server with many in-process clients, such as one per tunnel or
  bridge connection, could hang at shutdown, with clients blocked connecting
  to a server that had stopped accepting connections.

### Unreliable Subscribers That Fall Behind
- An unreliable publisher no longer loses the message it has just published.
  A subscriber sets a slot's retired bit just after releasing the slot, so
  a publisher can reuse the slot in between and the bit then marks the new
  message. A publisher that took the slot for that bit overwrote a message
  nobody had read, often the newest, and a subscriber waiting on its poll fd
  then waited for the next message. Publishers now take a slot for its
  retired bit only if the slot's refs show every subscriber has seen and
  released it.
- A subscriber that falls behind an unreliable publisher no longer reads
  messages out of order. Its snapshot of the slots could catch one that a
  publisher was reusing, with ordinal and timestamp zeroed. That entry sorted
  first, and since a claim with ordinal 0 matches any message, the
  subscriber took the message published there next ahead of older ones.
  Snapshots now keep a slot only while it holds the message its refs were
  committed with.
- `ReclaimBufferLease` makes the same check, so it returns an empty lease for
  a slot whose retired bit is stale. It also does so for a slot that retired
  before a subscriber joined, until the new subscriber has released it too.
- The Rust client has both fixes. Its subscriber already skipped older
  messages rather than reading them late, but its publisher had the same
  stale retired bits.

### Configurable Server Coroutine Stack
- Coroutines spawned by the server (`AsyncRuntime`) and by the shadow process
  use a build-time stack size. The default remains 64 KiB.
- A downstream Bazel module sets `subspace.coroutine_stack_size(bytes = N)`
  next to `bazel_dep`. Inside this repo, Bazel uses
  `--//:coroutine_stack_size=N`. CMake uses
  `-DSUBSPACE_COROUTINE_STACK_SIZE=N`. `N` must be a positive multiple of
  4096. This is process memory, not shared memory, so clients do not need the
  same value.
- AddressSanitizer builds still use at least 256 KiB. A configured size
  larger than that is kept.

### Late Subscribers Do Not Count Channel History As Drops
- A subscriber created on a channel with history no longer reports that
  history as dropped messages. A slot held by another subscriber is never
  reused, so a new subscriber's first message could be far older than the
  rest of the history, and the jump to the next message counted as drops.
- At creation, a subscriber remembers the newest ordinal already published on
  each virtual channel it reads. Ordinal gaps up to that point are history,
  not drops. A virtual channel subscriber remembers only the multiplexer and
  its own virtual channel. C++ and Rust behave the same.
- Drop detection now also runs on a subscriber's first read, so messages lost
  between creation and the first read are counted. A subscriber on a channel
  with nothing published still starts counting at its first message.
- `OrdinalAccumulator` has a read-only `PeekNext` (`peek_next` in Rust). The
  shared-memory layout is unchanged.

### Per-Interval Statistics
- On `/subspace/Statistics`, each channel's `total_messages`, `total_bytes`
  and `total_drops` now count the interval since the previous message
  instead of the channel's lifetime. `GetChannelStats` still returns
  lifetime totals.
- A channel removed and created again under the same name counts from its
  creation. A new virtual channel counts from its creation, not from the
  creation of its multiplexer. If a `Statistics` message fails to publish,
  the next message also counts its interval.
- `Statistics` carries the server's `machine_name`, set with
  `Server::SetMachineName`, and a `sequence` that goes up by one per
  published message. A gap in `sequence` means lost intervals.

### Larger Server Coroutine Stacks Under ASan
- With AddressSanitizer, server and shadow coroutines get at least 256 KiB
  of stack. On macOS, ASan's `strerror` interceptor alone uses about 56 KiB
  of stack, so an error path that formatted `errno` could run off the end of
  a 64 KiB stack and corrupt the heap. A build that sets a larger coroutine
  stack keeps that size. See `subspace.coroutine_stack_size`.

### Checksum Covers Only Publisher Flags
- The message checksum now covers only the `kMessageActivate` and
  `kMessageHasChecksum` bits of the prefix `flags`
  (`kChecksummedMessageFlags`). Other flags, such as `kMessageBridged`,
  `kMessageCrossMachine` or a bit a program sets itself, can be set after
  publishing, for example while forwarding a message over a bridge or tunnel,
  without invalidating the checksum. C++ and Rust compute the same value.
  Messages with only those two flags set have the same checksum as before.
- `GetMessageChecksumData` returns a `MessageChecksumData` holding a masked
  copy of the prefix fields. Pass `Spans()` to the checksum calculation. The
  Rust `get_message_checksum_data` takes a `PREFIX_FIELDS_LEN` scratch buffer.

### Stuck Subscriber Warning
- A subscriber now logs an error when a message has been waiting for 5
  seconds that it can't read because it already holds `max_active_messages`
  messages. The error is logged a second time after another grace period and
  then silenced until the subscriber reads a message, after which the next
  stuck period is reported again. The check runs when the subscriber tries
  to read. Two new subscriber options control it in C++, C, Python, and Rust:
  `warn_when_stuck` (default true) and `stuck_warning_grace_period` in
  seconds (default 5).
- A stuck subscriber callback (`RegisterStuckSubscriberCallback` in C++,
  `subspace_register_stuck_subscriber_callback` in C,
  `register_stuck_subscriber_callback` in Python and Rust) replaces the
  logged error. It receives the number of seconds the subscriber has been
  stuck and is called once per stuck period. The same two options control
  it.

### Configurable Channel Limit
- The maximum number of channels in one server session defaults to 1024 and
  can be raised at build time. A downstream Bazel module sets it next to
  `bazel_dep` with `subspace.max_channels(count = N)`. Inside this repo,
  Bazel uses `--//:max_channels=N`. CMake uses `-DSUBSPACE_MAX_CHANNELS=N`.
  A Cargo build of the Rust client reads `SUBSPACE_MAX_CHANNELS`. `N` must
  be a positive multiple of 64. The server and every client must be built
  with the same value, because it sizes the shared-memory system control
  block.

### Placeholder Subscriber Wakeups
- A placeholder subscriber now clears its trigger before checking for a new
  publisher in `ReadMessage` and `FindMessage`. Previously it cleared after
  the check, so a publisher that was created and published in between had its
  trigger discarded. The subscriber stayed a placeholder and never woke for
  that message.

### Subscriber Locality
- Added a `local` subscriber option (C++, C, Python, and Rust). A local
  subscriber makes its channel local, just as a local publisher does, so the
  server neither advertises nor bridges it. `ChannelInfo::is_local` and
  `ChannelStats::is_local` now report true when any publisher or subscriber
  is local. Publishers must still agree with each other on locality; local
  subscribers don't constrain them.
- Channel locality is now latched: once a local publisher or subscriber has
  joined a channel, it stays local after they leave, until the channel is
  removed. Previously a remaining non-local user could get the channel
  advertised, e.g. while a local publisher restarted. The latch is replicated
  to the shadow and restored on recovery.

### Publishers With Fewer Slots
- A publisher that joins an existing channel with fewer slots than the
  channel has now lays out the CCB with the channel's slot count, reported in
  the new `CreatePublisherResponse.num_slots`. Previously it used its own
  requested count, read subscriber queue offsets from the wrong place, and
  could lose messages or crash on its first publish.

### Prefix Layout Fixes
- A channel's checksum/metadata/prefix layout is now established before its
  shared memory is allocated, so shadow replicas receive the publisher's real
  layout instead of the `4`/`0` defaults. Previously a shadow permanently
  recorded the defaults, since the sizes are only replicated at channel
  creation.
- A publisher on a multiplexer now promotes a subscriber-created placeholder
  with its own prefix layout, including when the publisher uses a different
  virtual channel than the subscriber.
- Subscribers attaching to an existing placeholder multiplexer no longer tear
  down and recreate its shared memory underneath already-attached subscribers.
- A failed channel allocation now restores the channel to its placeholder
  state instead of leaving it claiming slots it never mapped.
- Shadow recovery now recomputes the prefix size from the recovered checksum
  and metadata sizes rather than leaving it at the default.

### ReadMessage trigger control
- Added `ClearTrigger` (`kClearTrigger` / `kNoClearTrigger`) so `ReadMessage`
  can leave the subscriber trigger fd unread. Exposed in C++, C, Python, and
  Rust.

### Publisher Buffer Leases
- Added explicit C++, C, Python, and Rust APIs to acquire multiple unpublished
  publisher slots, publish or release individual leases, and reject stale lease
  tokens.
- Added exact-slot reclamation from retirement notifications and per-lease
  metadata access.
- Added `max_outstanding_slot_leases` and
  `notify_retirement_on_forced_reuse` publisher options.
- Hardened lease concurrency and lifetime handling: C++ metadata lookup is
  synchronized, Python exported views cannot mutate reused slots, and Rust
  clone teardown and publisher locking are ownership- and unwind-safe.

### Channel Admission
- Added server-enforced `max_subscribers` limits, including virtual channels
  sharing a multiplexer.
- Channel capacity now reserves every publisher's maximum lease budget and
  every subscriber's maximum active-message budget.
- Lease budgets and subscriber limits survive shadow-server recovery.

### Split Buffers
- QNX and macOS split-buffer shared-memory object names now use the portable
  leading-slash form required by `shm_open`.

## Subspace Version 2.2.0

### Client API Improvements

#### Publisher Options
- **Retirement notification support**: Enhanced publisher options with retirement notification capabilities
  - `notify_retirement` option for publishers to receive slot retirement notifications
  - `GetRetirementFd()` method to obtain file descriptor for retirement notifications
  - Improved retirement tracking for GPU memory management and external resource cleanup

#### Thread Safety Features
- **Configurable thread safety**: New thread safety options for concurrent client usage
  - Optional mutex protection for all client operations
  - Automatic message buffer locking during publish operations
  - Configurable locking behavior for performance optimization
  - Thread-safe message retirement handling

#### Checksums and Prefix Extensions
- **Configurable checksum generation and checking**: Optionally allows a checksum to be stored in the message prefix and verified by subscribers.  A message with a failed checksum can either be dropped by the client or received with a failure notification.
- **Arbitrary-sized checksums**: The `checksum_size` option (default 4 for CRC32) controls how many bytes are reserved for the checksum starting at the `checksum` field of `MessagePrefix`. Custom checksum callbacks receive an `absl::Span<std::byte>` of `checksum_size` bytes to write their result.
- **User metadata in the prefix**: The `metadata_size` option reserves additional bytes immediately after the checksum area. Publishers write metadata via `GetMetadata()` and subscribers read it with the same method. The total prefix size is automatically aligned to 64-byte boundaries.
  
#### Portability
- **POSIX shared memory**: for non-Linux systems, uses POSIX shared memory with a shadow file in /tmp.

#### Bridge Support
- **Enhanced bridge publisher/subscriber options**: Improved bridge functionality in client options
  - Better bridge detection and configuration
  - Enhanced virtual channel support for bridged communications
  - Improved multiplexer channel handling across bridges

### New Features

#### Server Plugin Architecture
- **Dynamic plugin loading**: Added comprehensive plugin system for extending server functionality
  - Plugin interface with lifecycle hooks: `onStartup`, `onReady`, `onShutdown`
  - Event-driven callbacks for channel and user management: `onNewChannel`, `onRemoveChannel`, `onNewPublisher`, `onRemovePublisher`, `onNewSubscriber`, `onRemoveSubscriber`
  - Plugin context system with integrated logging support
  - Single-threaded coroutine-based execution model for plugins
  - Dynamic library loading support with C-style function interfaces

#### Client-Side Thread Safety
- **Optional thread safety support**: Added thread-safe client operations for multi-threaded applications
  - `SetThreadSafe(true)` method to enable mutex-protected client operations
  - Thread-safe publisher message buffer management with automatic locking
  - Configurable locking behavior with option to disable automatic locking for zero-copy operations
  - Mutual exclusion protection for concurrent client operations
  - Thread-safe retirement notification handling across multiple threads

#### Message Checksum Support
- **Hardware-accelerated checksums**: Integrated CRC32 checksum calculation for message integrity
  - Hardware CRC32 instruction support with fallback implementations
  - `CalculateCRC32Checksum()` and `VerifyCRC32Checksum()` functions for multi-span data validation
  - ARM assembly optimizations for CRC32 calculations
  - Configurable hardware acceleration via `SUBSPACE_HARDWARE_CRC` compilation flag
  - Template-based checksum calculation for arbitrary data spans
- **Configurable checksum and metadata sizes**: `checksum_size` and `metadata_size` publisher options control the prefix layout
  - Checksum callbacks receive an `absl::Span<std::byte>` of the configured size
  - `GetMetadata()` on publishers (writable) and subscribers (read-only) for per-message user data
  - `PrefixSize()`, `ChecksumSize()`, and `MetadataSize()` exposed on both `Publisher` and `Subscriber`
  - Sizes are validated on the server and propagated across the bridge

### Testing Infrastructure

#### Bridge Testing Framework
- **Comprehensive bridge retirement testing**: Added extensive test coverage for message retirement across bridges
  - `BasicRetirement` test: Validates basic retirement notification functionality
  - `MultipleRetirement` test: Tests retirement notifications for multiple messages with slot tracking
  - `MultipleRetirement2` test: Alternative retirement scenario testing with different slot allocation patterns
- **Multi-server bridge testing**: Enhanced test infrastructure supporting multiple Subspace servers
  - Two-server test setup with proper lifecycle management
  - Bridge notification pipes for inter-server communication testing
  - Retirement notification validation across server boundaries

#### Multi-threaded Testing
- **Concurrent operation validation**: Extensive multi-threaded testing infrastructure
  - Stress tests with multiple concurrent clients and channels
  - Latency testing under multi-threaded conditions
  - Thread safety validation for concurrent publisher/subscriber operations
  - Performance benchmarking for threaded vs non-threaded scenarios

#### Test Utilities
- **Improved test helpers**: Enhanced utility functions for bridge testing
  - `WaitForSubscribedMessage`: Robust waiting mechanism for bridge subscription notifications
  - Better signal handling for test debugging (`SigQuitHandler`)
  - Enhanced coroutine debugging capabilities for multi-server scenarios

### Server Infrastructure

#### Bridge Communication
- **Enhanced bridge transmitter functionality**: Improved the bridge transmitter coroutine system
  - Better retirement notification handling across bridges
  - Improved error handling for bridge connection failures
  - Enhanced retirement socket management

#### Retirement Notification System
- **Robust retirement tracking**: Improved retirement notification system for bridged channels
  - Better tracking of active messages across bridge connections
  - Enhanced retirement receiver coroutine functionality
  - Improved slot retirement correlation across server boundaries

#### Plugin Integration
- **Server extension points**: Comprehensive plugin integration throughout server lifecycle
  - Plugin initialization during server startup
  - Event notifications for all channel and user lifecycle events
  - Graceful plugin shutdown handling
  - Error handling and logging for plugin operations


### Documentation

#### Version 2 Features
- **Comprehensive feature documentation**: Enhanced documentation of Subspace Version 2 features
  - Lock-free shared memory implementation details
  - Message retirement notification system documentation
  - Multiplexed virtual channels usage patterns
  - C client API documentation
  - Thread safety features and guidelines
  - Server plugin development guide


### Bug Fixes

#### Bridge Test Improvements
- **Fixed bridge test debug output**: Removed excessive debug logging in `bridge_test.cc` that was cluttering test output
  - Removed verbose bridge notification length logging in `WaitForSubscribedMessage` function
  - Improved test output clarity for bridge functionality testing

#### Bridge Notification System
- **Enhanced bridge notification handling**: Improved the robustness of bridge notification processing
  - Better error handling for bridge notification pipe operations
  - Cleaner separation of bridge notification logic from debug output

### Infrastructure Improvements

#### Build System
- **Enhanced CMake support**: Improved CMake build configuration
  - Better dependency management for server components
  - Enhanced library linking for cross-platform builds
  - Improved target configuration for test executables
  - Plugin compilation support

#### Development Tools
- **Better debugging support**: Enhanced debugging capabilities for multi-server scenarios
  - Improved coroutine introspection tools
  - Better signal handling for development debugging
  - Enhanced test output formatting
  - Plugin debugging and logging infrastructure

### Performance Enhancements

#### Hardware Acceleration
- **Optimized checksum calculations**: Hardware-accelerated CRC32 for improved performance
  - ARM assembly optimizations for mobile and embedded platforms
  - Intel hardware CRC32 instruction utilization
  - Fallback implementations for compatibility

#### Thread Safety Optimization
- **Selective locking**: Configurable thread safety to minimize performance impact
  - Optional locking for zero-copy operations
  - Minimal mutex contention in multi-threaded scenarios
  - Atomic operations for retirement trigger management

### Breaking Changes
- `ChecksumCallback` signature changed: now receives `absl::Span<std::byte> checksum` (writable destination) instead of returning a `uint32_t`.
- `CalculateChecksum()` renamed to `CalculateCRC32Checksum()`.
- `VerifyChecksum()` renamed to `VerifyCRC32Checksum()`.

### Migration Notes
- No migration required for existing applications using the default 4-byte CRC32 checksum (the defaults are unchanged).
- Custom checksum callbacks must be updated to the new `void(data, absl::Span<std::byte> checksum)` signature.
- New `checksum_size` and `metadata_size` publisher options are opt-in; existing publishers default to `checksum_size = 4` and `metadata_size = 0`.
- New retirement notification features are opt-in via publisher options.
- Thread safety must be explicitly enabled via `SetThreadSafe(true)`.
- Server plugins require separate compilation and loading.
- Enhanced bridge testing infrastructure is available for developers working with multi-server deployments.

---

**Note**: This release focuses primarily on improving the stability and testability of the bridge communication system, while adding significant new capabilities for server extensibility, client thread safety, and message integrity validation. All new features maintain backward compatibility with existing Subspace applications.
