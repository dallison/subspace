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

In another terminal of at least 112 columns by 32 rows:

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

- Left/right arrows: select the publish-rate, channel-slot, or gauge-statistic
  control.
- Up/down arrows: adjust the selected control. Rates range from STOPPED through
  10 MHz; slot presets are 8, 16, 32, 64, 128, 256, 512, and 1024; gauge
  statistics are current, mean, and p50 and apply to both gauges.
- `q`: stop all tasks and quit.

The publisher and subscriber run on separate OS threads with independent
Subspace client connections. The Retro UI and metric sampler use the C++20
coroutine scheduler on the main thread. `--channel` is a base name: the active
channel appends its slot count, such as `/dashboard-32-slots`. Changing slots
creates the new publisher before moving the subscriber and unregistering its
old subscription. Each payload contains its monotonic publish timestamp.
Dropped-message warnings are aggregated into the scrolling warning pane rather
than being written over the curses display.
Latency p99 uses a bounded uniform reservoir over the rolling one-second
window; counts, min, max, and mean use every received message.

