#!/bin/bash
set -euo pipefail

BUILD_DIR="build/android"

# Wait for device to be fully available after emulator boot
adb wait-for-device
adb root
sleep 5
adb wait-for-device
adb shell "while [[ -z \$(getprop sys.boot_completed) ]]; do sleep 1; done"

# Create shared memory directory
adb shell "mkdir -p /dev/subspace && chmod 777 /dev/subspace"
adb shell "mkdir -p /data/local/tmp"

# Push libc++_shared.so from NDK (needed since we built with ANDROID_STL=c++_shared)
NDK_LIBCXX=$(find "$ANDROID_NDK_HOME" -name "libc++_shared.so" -path "*x86_64*" | head -1)
if [ -n "$NDK_LIBCXX" ]; then
  adb push "$NDK_LIBCXX" /data/local/tmp/libc++_shared.so
fi

LIB="LD_LIBRARY_PATH=/data/local/tmp:/data/local/tmp/plugins"

# Push test binaries
adb push "$BUILD_DIR/server/subspace_server" /data/local/tmp/subspace_server
adb push "$BUILD_DIR/client/client_test" /data/local/tmp/client_test
adb push "$BUILD_DIR/common/split_buffer_test" /data/local/tmp/split_buffer_test
adb push "$BUILD_DIR/common/common_test" /data/local/tmp/common_test
adb push "$BUILD_DIR/c_client/c_client_test" /data/local/tmp/c_client_test
adb push "$BUILD_DIR/shadow/shadow_test" /data/local/tmp/shadow_test

# Push plugin .so files
adb shell "mkdir -p /data/local/tmp/plugins"
adb push "$BUILD_DIR/plugins/nop_plugin.so" /data/local/tmp/plugins/
adb push "$BUILD_DIR/plugins/split_buffer_free_test_plugin.so" /data/local/tmp/plugins/

# Make binaries executable
adb shell "chmod +x /data/local/tmp/*_test /data/local/tmp/subspace_server"

echo "=== common_test ==="
adb shell "cd /data/local/tmp && $LIB ./common_test"

echo "=== split_buffer_test ==="
adb shell "cd /data/local/tmp && $LIB ./split_buffer_test"

echo "=== client_test ==="
adb shell "cd /data/local/tmp && $LIB ./client_test"

echo "=== c_client_test ==="
adb shell "cd /data/local/tmp && $LIB ./c_client_test"

echo "=== shadow_test ==="
adb shell "cd /data/local/tmp && $LIB ./shadow_test"

echo "=== All CMake Android tests passed ==="
