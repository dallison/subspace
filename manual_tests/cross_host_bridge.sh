#!/usr/bin/env bash
# Copyright 2026 David Allison
# All Rights Reserved
# See LICENSE file for licensing information.
#
# Manual test for cross-host Subspace bridging using TCP unicast discovery.
#
# It runs two Subspace servers and bridges a channel in *both* directions:
#   - channel "g2h": published on server B, subscribed on server A
#   - channel "h2g": published on server A, subscribed on server B
#
# Two modes are supported:
#
#   native    Both servers run on this host over loopback.  This validates the
#             TCP discovery + bridge code path with no emulator involved.
#
#   emulator  Server A ("host") runs natively; server B ("guest") runs inside a
#             running Android emulator.  Because the emulator sits behind a NAT
#             where only the guest can dial out, we:
#               * use TCP discovery with the guest dialing the host,
#               * advertise 127.0.0.1 for every bridge listener, and
#               * wire the loopback bridge ports through adb:
#                   adb reverse tcp:6502 -> host discovery listener
#                   adb reverse tcp:7100 -> host bridge receiver (g2h data)
#                   adb forward tcp:7200 -> guest bridge receiver (h2g data)
#
# Usage:
#   manual_tests/cross_host_bridge.sh [native|emulator]
#
# Requirements for emulator mode: a booted emulator (`adb devices` shows one),
# the Android NDK configured for `--config=android_arm64`, and adb on PATH.

set -uo pipefail

MODE="${1:-native}"

# Discovery + bridge ports.  The host and guest use *different* bridge ports so
# the adb forward/reverse listeners never collide with a server's own listener.
DISC_PORT=6502
HOST_BRIDGE_PORT=7100   # host's bridge receiver (g2h)
GUEST_BRIDGE_PORT=7200  # guest/connector bridge receiver (h2g)

HOST_SOCKET=/tmp/subspace_bridge_host
PEER_SOCKET=/tmp/subspace_bridge_peer        # native mode only
GUEST_SOCKET=/data/local/tmp/subspace        # emulator mode only
ANDROID_TMP=/data/local/tmp

LOGDIR="$(mktemp -d /tmp/subspace_bridge.XXXXXX)"
PIDS=()
SUB_TIME=3        # seconds to let subscribers + bridge establish
PUB_TIME=8        # seconds of publishing

note()  { printf '\n=== %s ===\n' "$*"; }
fail()  { printf 'FAIL: %s\n' "$*" >&2; }

cleanup() {
  set +m
  for p in "${PIDS[@]:-}"; do
    kill "$p" 2>/dev/null
    wait "$p" 2>/dev/null
  done
  pkill -f 'manual_tests/pub' 2>/dev/null
  pkill -f 'manual_tests/sub' 2>/dev/null
  pkill -f "subspace_server --socket=${HOST_SOCKET}" 2>/dev/null
  pkill -f "subspace_server --socket=${PEER_SOCKET}" 2>/dev/null
  if [[ "$MODE" == emulator ]]; then
    adb shell "pkill subspace_server; pkill pub; pkill sub" 2>/dev/null
    adb reverse --remove "tcp:${DISC_PORT}" 2>/dev/null
    adb reverse --remove "tcp:${HOST_BRIDGE_PORT}" 2>/dev/null
    adb forward --remove "tcp:${GUEST_BRIDGE_PORT}" 2>/dev/null
  fi
}
trap cleanup EXIT

# Count "Message" lines a subscriber printed, and PASS/FAIL accordingly.
check_dir() {
  local label="$1" logfile="$2"
  local n
  n=$(grep -c 'Message' "$logfile" 2>/dev/null) || true
  n=${n:-0}
  if [[ "$n" -gt 0 ]]; then
    printf 'PASS: %s delivered %s messages\n' "$label" "$n"
    return 0
  fi
  fail "$label delivered no messages"
  printf '  --- %s ---\n' "$logfile"
  sed -n '1,5p' "$logfile" 2>/dev/null | sed 's/^/  /'
  return 1
}

run_native() {
  note "Building host binaries"
  bazelisk build //server:subspace_server //manual_tests:pub //manual_tests:sub \
    >"$LOGDIR/build.log" 2>&1 || { cat "$LOGDIR/build.log"; exit 1; }

  local SRV=bazel-bin/server/subspace_server
  local PUB=bazel-bin/manual_tests/pub
  local SUB=bazel-bin/manual_tests/sub

  rm -f "$HOST_SOCKET" "$PEER_SOCKET"

  note "Starting servers (TCP discovery over loopback)"
  # Server A: discovery listener.
  "$SRV" --socket="$HOST_SOCKET" --tcp_discovery --disc_port="$DISC_PORT" \
         --bridge_ports="$HOST_BRIDGE_PORT" --bridge_advertise_address=127.0.0.1 \
         --cleanup_filesystem=false --log_level=debug \
         >"$LOGDIR/serverA.log" 2>&1 &
  PIDS+=($!)
  # Server B: discovery connector (dials A).
  "$SRV" --socket="$PEER_SOCKET" --tcp_discovery --peer_address=127.0.0.1 \
         --peer_port="$DISC_PORT" --bridge_ports="$GUEST_BRIDGE_PORT" \
         --bridge_advertise_address=127.0.0.1 \
         --cleanup_filesystem=false --log_level=debug \
         >"$LOGDIR/serverB.log" 2>&1 &
  PIDS+=($!)
  sleep 2

  note "Starting subscribers"
  "$SUB" --socket="$HOST_SOCKET" --channel=g2h >"$LOGDIR/sub_g2h.log" 2>&1 &
  PIDS+=($!)
  "$SUB" --socket="$PEER_SOCKET" --channel=h2g >"$LOGDIR/sub_h2g.log" 2>&1 &
  PIDS+=($!)
  sleep "$SUB_TIME"

  note "Publishing in both directions for ${PUB_TIME}s"
  "$PUB" --socket="$PEER_SOCKET" --channel=g2h --local=false \
         --num_msgs=200 --frequency=25 </dev/null >"$LOGDIR/pub_g2h.log" 2>&1 &
  PIDS+=($!)
  "$PUB" --socket="$HOST_SOCKET" --channel=h2g --local=false \
         --num_msgs=200 --frequency=25 </dev/null >"$LOGDIR/pub_h2g.log" 2>&1 &
  PIDS+=($!)
  sleep "$PUB_TIME"

  local rc=0
  note "Results"
  check_dir "g2h (server B -> server A)" "$LOGDIR/sub_g2h.log" || rc=1
  check_dir "h2g (server A -> server B)" "$LOGDIR/sub_h2g.log" || rc=1
  return $rc
}

run_emulator() {
  if ! adb get-state >/dev/null 2>&1; then
    fail "no Android device/emulator detected (adb get-state). Start one first."
    exit 1
  fi

  note "Building host and Android (arm64) binaries"
  bazelisk build //server:subspace_server //manual_tests:pub //manual_tests:sub \
    >"$LOGDIR/build_host.log" 2>&1 || { cat "$LOGDIR/build_host.log"; exit 1; }
  bazelisk build --config=android_arm64 \
    //server:subspace_server //manual_tests:pub //manual_tests:sub \
    >"$LOGDIR/build_android.log" 2>&1 || { cat "$LOGDIR/build_android.log"; exit 1; }

  # Resolve binary paths via cquery because the bazel-bin symlink points at
  # whichever configuration was built last (here, the Android one).
  local SRV PUB SUB
  SRV=$(bazelisk cquery --output=files //server:subspace_server 2>/dev/null | tail -1)
  PUB=$(bazelisk cquery --output=files //manual_tests:pub 2>/dev/null | tail -1)
  SUB=$(bazelisk cquery --output=files //manual_tests:sub 2>/dev/null | tail -1)
  local AOUT APUB AS
  AOUT=$(bazelisk cquery --config=android_arm64 --output=files \
         //server:subspace_server 2>/dev/null | tail -1)
  APUB=$(bazelisk cquery --config=android_arm64 --output=files \
         //manual_tests:pub 2>/dev/null | tail -1)
  AS=$(bazelisk cquery --config=android_arm64 --output=files \
       //manual_tests:sub 2>/dev/null | tail -1)

  note "Pushing Android binaries to ${ANDROID_TMP}"
  adb shell "pkill subspace_server; pkill pub; pkill sub" 2>/dev/null
  adb push "$AOUT" "$ANDROID_TMP/subspace_server" >/dev/null
  adb push "$APUB" "$ANDROID_TMP/pub" >/dev/null
  adb push "$AS"   "$ANDROID_TMP/sub" >/dev/null
  adb shell "chmod 755 $ANDROID_TMP/subspace_server $ANDROID_TMP/pub $ANDROID_TMP/sub"

  note "Wiring adb port tunnels"
  # Discovery: guest dials 127.0.0.1:DISC_PORT -> host discovery listener.
  adb reverse "tcp:${DISC_PORT}" "tcp:${DISC_PORT}"
  # g2h data: guest transmitter dials 127.0.0.1:HOST_BRIDGE_PORT -> host receiver.
  adb reverse "tcp:${HOST_BRIDGE_PORT}" "tcp:${HOST_BRIDGE_PORT}"
  # h2g data: host transmitter dials 127.0.0.1:GUEST_BRIDGE_PORT -> guest receiver.
  adb forward "tcp:${GUEST_BRIDGE_PORT}" "tcp:${GUEST_BRIDGE_PORT}"

  rm -f "$HOST_SOCKET"

  note "Starting host server (discovery listener)"
  "$SRV" --socket="$HOST_SOCKET" --tcp_discovery --disc_port="$DISC_PORT" \
         --bridge_ports="$HOST_BRIDGE_PORT" --bridge_advertise_address=127.0.0.1 \
         --cleanup_filesystem=false --log_level=debug \
         >"$LOGDIR/serverA.log" 2>&1 &
  PIDS+=($!)

  note "Starting guest server in emulator (discovery connector)"
  adb shell "rm -f $GUEST_SOCKET" 2>/dev/null
  adb shell "cd $ANDROID_TMP && ./subspace_server --socket=$GUEST_SOCKET \
      --tcp_discovery --peer_address=127.0.0.1 --peer_port=$DISC_PORT \
      --bridge_ports=$GUEST_BRIDGE_PORT --bridge_advertise_address=127.0.0.1 \
      --cleanup_filesystem=false --log_level=debug" >"$LOGDIR/serverB.log" 2>&1 &
  PIDS+=($!)
  sleep 3

  note "Starting subscribers"
  # g2h: host subscribes.
  "$SUB" --socket="$HOST_SOCKET" --channel=g2h >"$LOGDIR/sub_g2h.log" 2>&1 &
  PIDS+=($!)
  # h2g: guest subscribes.
  adb shell "$ANDROID_TMP/sub --socket=$GUEST_SOCKET --channel=h2g" \
      >"$LOGDIR/sub_h2g.log" 2>&1 &
  PIDS+=($!)
  sleep "$SUB_TIME"

  note "Publishing in both directions for ${PUB_TIME}s"
  # g2h: guest publishes.
  adb shell "$ANDROID_TMP/pub --socket=$GUEST_SOCKET --channel=g2h --local=false \
      --num_msgs=200 --frequency=25 </dev/null" >"$LOGDIR/pub_g2h.log" 2>&1 &
  PIDS+=($!)
  # h2g: host publishes.
  "$PUB" --socket="$HOST_SOCKET" --channel=h2g --local=false \
         --num_msgs=200 --frequency=25 </dev/null >"$LOGDIR/pub_h2g.log" 2>&1 &
  PIDS+=($!)
  sleep "$PUB_TIME"

  local rc=0
  note "Results"
  check_dir "g2h (guest -> host)" "$LOGDIR/sub_g2h.log" || rc=1
  check_dir "h2g (host -> guest)" "$LOGDIR/sub_h2g.log" || rc=1
  return $rc
}

printf 'Cross-host bridge test (mode=%s, logs in %s)\n' "$MODE" "$LOGDIR"
case "$MODE" in
  native)   run_native;   RC=$? ;;
  emulator) run_emulator; RC=$? ;;
  *) echo "unknown mode: $MODE (use native|emulator)"; exit 2 ;;
esac

note "Done (logs in $LOGDIR)"
exit "$RC"
