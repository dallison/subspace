#!/bin/bash
set -euo pipefail

# Wait for device to be fully available after emulator boot
adb wait-for-device
adb root
sleep 5
adb wait-for-device
adb shell "while [[ -z \$(getprop sys.boot_completed) ]]; do sleep 1; done"

adb shell "mkdir -p /data/local/tmp"

# Collect shared libraries (dereference symlinks from bazel output)
mkdir -p /tmp/android_libs
find bazel-bin/ -name "*.so" -path "*_solib*" -exec cp -L {} /tmp/android_libs/ \; 2>/dev/null || true
find bazel-bin/plugins/ -name "*.so" -exec cp -L {} /tmp/android_libs/ \; 2>/dev/null || true

if ls /tmp/android_libs/*.so 1>/dev/null 2>&1; then
  adb push /tmp/android_libs /data/local/tmp/android_libs
fi

LIB="LD_LIBRARY_PATH=/data/local/tmp/android_libs"

# Push test binaries
adb push bazel-bin/server/subspace_server /data/local/tmp/subspace_server
adb push bazel-bin/client/client_test /data/local/tmp/client_test
adb push bazel-bin/client/bridge_test /data/local/tmp/bridge_test
adb push bazel-bin/common/split_buffer_test /data/local/tmp/split_buffer_test
adb push bazel-bin/coro_rpc/test/rpc_test /data/local/tmp/coro_rpc_test
adb push bazel-bin/coro_rpc/server/server_test /data/local/tmp/coro_rpc_server_test
adb push bazel-bin/coro_rpc/client/client_test /data/local/tmp/coro_rpc_client_test
adb push bazel-bin/co20_rpc/test/rpc_test /data/local/tmp/co20_rpc_test
adb push bazel-bin/co20_rpc/server/server_test /data/local/tmp/co20_rpc_server_test
adb push bazel-bin/co20_rpc/client/client_test /data/local/tmp/co20_rpc_client_test
adb push bazel-bin/asio_rpc/test/rpc_test /data/local/tmp/asio_rpc_test
adb push bazel-bin/asio_rpc/server/server_test /data/local/tmp/asio_rpc_server_test
adb push bazel-bin/asio_rpc/client/client_test /data/local/tmp/asio_rpc_client_test

# Push plugin .so files to relative path expected by tests
adb shell "mkdir -p /data/local/tmp/plugins"
find bazel-bin/plugins/ -name "*.so" -exec adb push {} /data/local/tmp/plugins/ \; 2>/dev/null || true

# Make binaries executable
adb shell "chmod +x /data/local/tmp/*_test /data/local/tmp/subspace_server"

echo "=== split_buffer_test ==="
adb shell "cd /data/local/tmp && $LIB ./split_buffer_test"

echo "=== client_test ==="
adb shell "cd /data/local/tmp && $LIB ./client_test"

echo "=== bridge_test ==="
adb shell "cd /data/local/tmp && $LIB ./bridge_test"

echo "=== coro_rpc tests ==="
adb shell "cd /data/local/tmp && $LIB ./coro_rpc_test"
adb shell "cd /data/local/tmp && $LIB ./coro_rpc_server_test"
adb shell "cd /data/local/tmp && $LIB ./coro_rpc_client_test"

echo "=== co20_rpc tests ==="
adb shell "cd /data/local/tmp && $LIB ./co20_rpc_test"
adb shell "cd /data/local/tmp && $LIB ./co20_rpc_server_test"
adb shell "cd /data/local/tmp && $LIB ./co20_rpc_client_test"

echo "=== asio_rpc tests ==="
adb shell "cd /data/local/tmp && $LIB ./asio_rpc_test"
adb shell "cd /data/local/tmp && $LIB ./asio_rpc_server_test"
adb shell "cd /data/local/tmp && $LIB ./asio_rpc_client_test"

echo "=== All Android tests passed ==="
