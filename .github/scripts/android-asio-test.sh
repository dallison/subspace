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

echo "=== split_buffer_test (asio) ==="
adb shell "cd /data/local/tmp && $LIB ./split_buffer_test"

echo "=== client_test (asio) ==="
adb shell "cd /data/local/tmp && $LIB ./client_test"

echo "=== bridge_test (asio) ==="
adb shell "cd /data/local/tmp && $LIB ./bridge_test"

echo "=== asio_rpc tests ==="
adb shell "cd /data/local/tmp && $LIB ./asio_rpc_test"
adb shell "cd /data/local/tmp && $LIB ./asio_rpc_server_test"
adb shell "cd /data/local/tmp && $LIB ./asio_rpc_client_test"

echo "=== All Android asio tests passed ==="
