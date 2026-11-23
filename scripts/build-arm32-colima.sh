#!/usr/bin/env bash
# Cross-build Subspace for 32-bit ARM (armhf) inside Colima.
#
# Colima only hosts aarch64 and x86_64 VMs, and Bazel does not ship a 32-bit
# ARM binary, so this uses the aarch64 Colima VM to run an Ubuntu container
# that cross-compiles with CMake. A host protoc is built first so it matches
# the protobuf version fetched for the target.
#
# Usage, from the repo root:
#   scripts/build-arm32-colima.sh
#   scripts/build-arm32-colima.sh --test
#
# Outputs land in build/host-protoc (host tools) and build/armhf (armhf).

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

run_tests=0
if [[ "${1:-}" == "--test" ]]; then
  run_tests=1
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--test]" >&2
  exit 2
fi

if ! colima status >/dev/null 2>&1; then
  echo "Starting Colima (aarch64)..."
  colima start --profile default --arch aarch64 --cpu 6 --memory 12 --disk 80 --vm-type vz
fi

docker build -t subspace-arm32-builder -f docker/arm32/Dockerfile docker/arm32

docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -v "$root:/src" \
  -w /src \
  subspace-arm32-builder \
  bash -eo pipefail -c "
    set -euo pipefail
    # Protobuf's CMake build installs the compiler as a symlink named protoc
    # pointing at protoc-<version>. Follow that symlink rather than requiring
    # a regular file of exactly that name.
    protoc=build/host-protoc/_deps/protobuf-build/protoc
    if [[ ! -e \"\$protoc\" ]]; then
      cmake -S . -B build/host-protoc \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DSUBSPACE_ASIO_RPC=OFF \
        -DSUBSPACE_CO20_RPC=OFF
      cmake --build build/host-protoc --target protoc --parallel
    fi
    if [[ ! -e \"\$protoc\" ]]; then
      echo 'host protoc was not produced' >&2
      exit 1
    fi
    echo \"Using host protoc: \$protoc\"
    \"\$protoc\" --version
    cmake -S . -B build/armhf \
      -G Ninja \
      -DCMAKE_TOOLCHAIN_FILE=/src/cmake/toolchains/linux-armhf.cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DPROTOC_EXECUTABLE=/src/\$protoc \
      -DSUBSPACE_ASIO_RPC=OFF
    cmake --build build/armhf --parallel
    readelf -h build/armhf/server/subspace_server
    if [[ $run_tests -eq 1 ]]; then
      # client_test raises its own soft NOFILE limit (client/test_fixture.h).
      ctest --test-dir build/armhf --output-on-failure --timeout 300
    fi
  "
