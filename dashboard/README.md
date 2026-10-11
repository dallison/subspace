# Subspace dashboard

The dashboard publishes timestamp messages to a slot-specific Subspace channel
and displays the measured receive rate, one-way latency, rolling statistics,
and drops.

## Build and run

The current development build uses the sibling `../retro` and `../co`
checkouts through Bzlmod local overrides. It supports Linux and macOS.

Start a Subspace server:

```sh
bazelisk run //server:subspace_server
```

In another terminal of at least 112 columns by 36 rows:

```sh
bazelisk run -c opt //dashboard:dashboard
```

Use the optimized build for throughput measurements; Bazel's default
fastbuild mode is substantially slower in the per-message hot path.

Optional flags:

```sh
bazelisk run -c opt //dashboard:dashboard -- \
  --socket=/tmp/subspace \
  --channel=/dashboard
```

## Controls

- Left/right arrows: select the operating-mode, publish-rate, channel-slot,
  subscriber-queue, or gauge-statistic control.
- Up/down arrows: adjust the selected control. Throughput mode batches
  notifications and drains every available message. Latency mode wakes the
  subscriber for every publication and reads the newest available message.
  Rates range from STOPPED through 10 MHz; slot presets are 8, 16, 32, 64, 128,
  256, 512, and 1024; subscriber queue presets are off, 8, 16, 32, 64, 128,
  256, 512, and 1024; gauge statistics are current, mean, and p50 and apply to
  both gauges.
- `q`: stop all tasks and quit.

The publisher and subscriber run on separate OS threads with independent
Subspace client connections. The Retro UI and metric sampler use the C++20
coroutine scheduler on the main thread. `--channel` is a base name: the active
channel appends its slot count and subscriber queue size, such as
`/dashboard-32-slots-0-queue`. Changing either value creates the new publisher
before moving the subscriber and unregistering its old subscription. A queue
size of off leaves subscriber queues disabled. Any other size reserves one
subscriber queue of that capacity in the publisher's subscriber queue arena
and requests that capacity from the subscriber. Each payload contains its
monotonic publish timestamp.
Dropped-message warnings are aggregated into the scrolling warning pane rather
than being written over the curses display.
Latency p99 uses a bounded uniform reservoir over the rolling one-second
window; counts, min, max, and mean use every received message.

