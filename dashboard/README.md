# Subspace dashboard

The dashboard is a terminal UI for exercising a local Subspace server under
controlled load. It publishes fixed-size timestamp payloads on a dedicated
channel, receives them on a second client connection, and displays measured
throughput, one-way latency, rolling statistics, and drop counts. Use it to
compare performance modes, slot counts, and subscriber queue settings without
writing a custom benchmark harness.

The binary is built for desktop Linux and macOS only (see `BUILD.bazel`:
Android and QNX are marked incompatible).

## Prerequisites

1. A running Subspace server reachable on a Unix domain socket (default
   `/tmp/subspace`).
2. A terminal at least **112 columns by 36 rows** (hard-coded in
   `DashboardApplication`). Smaller windows clip the Retro layout.
3. For meaningful throughput numbers, run the dashboard with **`-c opt`**. The
   default fastbuild configuration is much slower on the per-message hot path.

Dependencies (`retro`, `coroutines`, the Subspace client) are pulled in through
Bzlmod in the repo root `MODULE.bazel`.

## Build and run

Start a Subspace server:

```sh
bazelisk run //server:subspace_server
```

In another terminal:

```sh
bazelisk run -c opt //dashboard:dashboard
```

Optional flags:

```sh
bazelisk run -c opt //dashboard:dashboard -- \
  --socket=/tmp/subspace \
  --channel=/dashboard
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--socket` | `/tmp/subspace` | Unix socket path for the Subspace server |
| `--channel` | `/dashboard` | Base channel name; see [Channel naming](#channel-naming) |

Press **`q`** to stop publisher and subscriber threads, tear down coroutine
tasks, and exit. If the engine recorded a fatal error, it is printed on stderr
after the UI closes.

## UI layout

The Retro application divides the screen into:

- **Throughput and latency gauges** — large numeric displays; the selected
  statistic (current, mean, or p50) applies to both gauges.
- **Stats table** — cumulative publish/receive/drop counts, active channel,
  requested vs active slot and queue sizes, and target rate.
- **Accelerator bar** — five controls top to bottom: publish rate, channel slots,
  subscriber queue, gauge statistic, then mode at the bottom. The highlighted
  control responds to up/down arrows.
- **Warning pane** — scrolling log of non-fatal events (channel switches,
  aggregated drop notices). Client library warnings captured from stderr are
  sanitized and appended here instead of corrupting the curses display.

## Controls

**Left/right arrows** cycle which accelerator control is selected (same order as
the bar, top to bottom):

1. **RATE** — publish rate preset
2. **SLOTS** — publisher slot count
3. **QUEUE** — subscriber queue capacity
4. **GAUGE** — which statistic the gauges show (current, mean, p50)
5. **MODE** — performance mode (throughput, latency, reliable)

**Up/down arrows** adjust the selected control.

**`q`** — request shutdown.

Changing mode, slots, or queue size resets rolling statistics and may switch
channels; see below.

### MODE presets

| Label | Publisher / subscriber | Notify behavior | Subscriber read path |
|-------|------------------------|-----------------|----------------------|
| **THROUGHPUT** | Standard unreliable transport | Batches wakeups; may defer `NotifySubscribers` while the subscriber is draining | Drains all available messages per wakeup (`ReadMessage` + batch reads) |
| **LATENCY** | Standard unreliable transport | Notifies on every publish | `ReadNewest` only; ordinals detect skipped messages as drops |
| **RELIABLE** | `SetReliable(true)` on both sides | Notifies on every publish (same as latency) | Same batch drain as throughput, but over reliable delivery |

Throughput mode optimizes for sustained message rate. Latency mode measures
time-to-newest-message and explicitly counts gaps in message ordinals as drops.
Reliable mode stress-tests the reliable pub/sub path while still draining in
batches like throughput.

### RATE presets

`STOPPED`, `1 Hz`, `10 Hz`, `100 Hz`, `1 kHz`, `10 kHz`, `100 kHz`, `1 MHz`,
`2 MHz`, `5 MHz`, `10 MHz`.

At **STOPPED** the publisher loop idles without sending. At high rates the
publisher spins instead of sleeping when the next deadline is less than ~100 µs
away, so the UI and subscriber threads still get CPU time.

Default starting rate is **1 Hz** (second preset).

### SLOTS presets

`8`, `16`, `32`, `64`, `128`, `256`, `512`, `1024` publisher slots.

Default is **32** (third preset). Publish batching for metrics uses
`max(1, slots - 1)` messages between throughput counter updates.

### QUEUE presets

`off` (`0`), then `8`, `16`, `32`, `64`, `128`, `256`, `512`, `1024`.

Default is **off** (subscriber queues disabled). Any non-zero value:

- Reserves subscriber-queue arena space on the publisher
  (`SubscriberQueueArenaSize`)
- Requests that queue capacity on the subscriber (`SetSubscriberQueueSize`)

### GAUGE statistic

**Current** — latest sample in the rolling window. **Mean** and **p50** —
aggregates over the same window. Latency **p99** appears in the stats table
(from a bounded reservoir; see [Metrics](#metrics)).

## Channel naming

`--channel` is a **base name**, not the full Subspace channel. The active
channel is:

```text
<base>-<slots>-slots-<queue>-queue[-reliable]
```

Examples with defaults `--channel=/dashboard`, 32 slots, queue off:

- Throughput or latency: `/dashboard-32-slots-0-queue`
- Reliable mode: `/dashboard-32-slots-0-queue-reliable`

Changing **slots**, **queue**, or **mode** bumps an internal generation counter
and moves traffic to a new channel name. The engine creates the new publisher
first, then retargets the subscriber and drops the old subscription. A warning
line is recorded when the subscriber switches.

Message type is fixed: `subspace.dashboard.Timestamp`, **8-byte** payload
(`uint64` monotonic publish time in nanoseconds from `toolbelt::Now()`).

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│  Main thread (Retro + co20 coroutines)                      │
│  - InputTask, RefreshTask, WarningCaptureTask               │
│  - SamplerTask (100 ms) → RollingMetrics                    │
└─────────────────────────────────────────────────────────────┘
         │                              ▲
         │ controls                     │ DashboardSnapshot
         ▼                              │
┌─────────────────┐            ┌─────────────────┐
│ Publisher thread│  Subspace  │Subscriber thread│
│ PublisherLoop   │──channel──▶│ SubscriberLoop  │
│ (own Client)    │            │ (own Client)    │
└─────────────────┘            └─────────────────┘
```

- **Two Subspace clients** — `"subspace-dashboard-publisher"` and
  `"subspace-dashboard-subscriber"` — so publish and receive paths do not share
  one connection’s lock contention.
- **Publisher thread** — rate-limited loop: allocate buffer, stamp time,
  `PublishMessage`, optional notify. Polls publisher FD and a control pipe for
  stop / rate / channel changes.
- **Subscriber thread** — mode-dependent read loop; coordinates with the
  publisher via `subscriber_busy_` so throughput mode avoids redundant trigger
  syscalls while a drain is in progress.
- **Sampler** — every 100 ms, computes throughput and latency summaries into
  `DashboardSnapshot` for the UI refresh task.

Fatal errors on either worker thread set `snapshot.error` and stop the process
after shutdown.

## Metrics

- **Throughput (Hz)** — messages received in the last completed 100 ms sample,
  scaled for display (Hz / kHz / MHz as appropriate).
- **Latency (ns)** — one-way delay `receive_time - publish_time` per message
  (clamped to zero if clocks disagree).
- **Rolling window** — ten 100 ms buckets (~1 s history) for gauge statistics.
- **Latency p99** — uniform reservoir (2048 samples) over the current bucket;
  min, max, and mean use **every** message in that bucket.
- **Drops** — in latency mode, inferred from ordinal gaps; otherwise from the
  subscriber dropped-message callback. Large drop bursts are summarized into the
  warning pane rather than spamming the UI.

Adjusting rate resets counters via `ResetStatistics`; changing channel resets
channel-specific fields via `ResetForChannel`.

## Tests

```sh
bazelisk test //dashboard/...
```

- `dashboard_test` — controllers, channel naming, metric formatting.
- `engine_test` — end-to-end publish/subscribe against the test server fixture,
  including mode and channel switches.

## Source map

| File | Role |
|------|------|
| `main.cc` | Flags, engine init, run application |
| `engine.cc` / `engine.h` | Publisher/subscriber threads, channel lifecycle |
| `metrics.cc` / `metrics.h` | Controllers, rolling metrics, formatting |
| `ui.cc` / `ui.h` | Retro windows, input, stderr capture |
| `BUILD.bazel` | `dashboard`, `dashboard_lib`, tests |
