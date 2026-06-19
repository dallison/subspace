#!/bin/bash
set -euo pipefail

# Runs the Boost.Asio-backend subspace test binaries on the Android emulator.
# The binaries are expected to have been cross-compiled beforehand with
# `--config=android_x86_64 --//:coro_backend=asio` so that bazel-bin/ points at
# the asio-configured outputs.  Only the backend-dependent targets are run here;
# the co-backend coverage lives in the regular `android` job.

# Wait for device to be fully available after emulator boot.
adb wait-for-device
adb root
sleep 5
adb wait-for-device
adb shell "while [[ -z \$(getprop sys.boot_completed) ]]; do sleep 1; done"

adb shell "mkdir -p /data/local/tmp"

# Collect shared libraries (dereference symlinks from bazel output).
rm -rf /tmp/android_libs
mkdir -p /tmp/android_libs
find bazel-bin/ -name "*.so" -path "*_solib*" -exec cp -L {} /tmp/android_libs/ \; 2>/dev/null || true
find bazel-bin/plugins/ -name "*.so" -exec cp -L {} /tmp/android_libs/ \; 2>/dev/null || true

if ls /tmp/android_libs/*.so 1>/dev/null 2>&1; then
  adb shell "rm -rf /data/local/tmp/android_libs"
  adb push /tmp/android_libs /data/local/tmp/android_libs
fi

LIB="LD_LIBRARY_PATH=/data/local/tmp/android_libs"

# Push test binaries (asio backend).
adb push bazel-bin/server/subspace_server /data/local/tmp/subspace_server
adb push bazel-bin/client/client_test /data/local/tmp/client_test
adb push bazel-bin/client/bridge_test /data/local/tmp/bridge_test
adb push bazel-bin/common/split_buffer_test /data/local/tmp/split_buffer_test
adb push bazel-bin/asio_rpc/test/rpc_test /data/local/tmp/asio_rpc_test
adb push bazel-bin/asio_rpc/server/server_test /data/local/tmp/asio_rpc_server_test
adb push bazel-bin/asio_rpc/client/client_test /data/local/tmp/asio_rpc_client_test

# Push plugin .so files to the relative path the tests load them from.  Remove
# the target directory first so a stale directory cannot shadow the file.
rm -rf /tmp/android_plugins
mkdir -p /tmp/android_plugins
cp -L bazel-bin/plugins/nop_plugin.so /tmp/android_plugins/
cp -L bazel-bin/plugins/split_buffer_free_test_plugin.so /tmp/android_plugins/
adb shell "rm -rf /data/local/tmp/plugins"
adb push /tmp/android_plugins /data/local/tmp/plugins

# Make binaries executable.
adb shell "chmod +x /data/local/tmp/*_test /data/local/tmp/subspace_server"

# Run a test binary on the emulator, retrying a few times to absorb transient
# flakiness.  The asio backend multiplexes work across real threads and is far
# more sensitive to scheduling latency than the cooperative co backend, and the
# GitHub-hosted x86_64 emulator (2 cores, swiftshader) is slow enough that
# timing-sensitive tests occasionally miss a deadline.  The same tests pass
# reliably on the real Linux/macOS asio runners; the retry keeps the emulator
# smoke lane stable without masking a genuine, reproducible regression.
run_with_retry() {
  local desc="$1"
  local cmd="$2"
  local attempts=3
  local n
  for n in $(seq 1 "$attempts"); do
    echo "=== ${desc} (attempt ${n}/${attempts}) ==="
    if adb shell "cd /data/local/tmp && ${LIB} ${cmd}"; then
      return 0
    fi
    echo "--- ${desc} attempt ${n} failed ---"
    sleep 3
  done
  echo "FAILED: ${desc} after ${attempts} attempts"
  return 1
}

run_with_retry "split_buffer_test (asio)" "./split_buffer_test"
run_with_retry "client_test (asio)" "./client_test"

# Exclude the multi-threaded bridge stress test on the emulator: it spawns 4
# asio io-context threads x 8 channels x 1000 messages, which is both
# ill-matched to a 2-core emulator and the heaviest source of timing flakiness
# here.  Its multi-core thread-safety coverage runs on the real Linux/macOS
# asio runners (the asio-backend jobs), which exercise the full bridge_test.
run_with_retry "bridge_test (asio)" "./bridge_test --gtest_filter=-BridgeStressTest.*"

run_with_retry "asio_rpc_test" "./asio_rpc_test"
run_with_retry "asio_rpc_server_test" "./asio_rpc_server_test"
run_with_retry "asio_rpc_client_test" "./asio_rpc_client_test"

echo "=== All Android asio tests passed ==="
