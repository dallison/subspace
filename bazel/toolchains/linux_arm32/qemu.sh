#!/usr/bin/env bash
# Run an armhf binary on the host. Ubuntu 24.04 names the emulator
# qemu-arm-static; Ubuntu 26.04 names it qemu-arm.
set -euo pipefail
export QEMU_LD_PREFIX="${QEMU_LD_PREFIX:-/usr/arm-linux-gnueabihf}"
if command -v qemu-arm-static >/dev/null 2>&1; then
  exec qemu-arm-static "$@"
elif command -v qemu-arm >/dev/null 2>&1; then
  exec qemu-arm "$@"
fi
echo "qemu-arm-static or qemu-arm is required to run armhf binaries" >&2
exit 1
