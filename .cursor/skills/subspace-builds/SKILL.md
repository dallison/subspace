---
name: subspace-builds
description: Build and test Subspace across supported platforms and build systems. Use when the user asks how to build, test, cross-compile, run CI-equivalent checks, or work with Bazel, CMake, Android, Soong/Blueprint, AOSP, QNX, Linux, or macOS builds in this repository.
---

# Subspace Builds

## General Rules

- User-facing examples should use `bazel`; use `bazelisk` as a drop-in
  replacement if `bazel` is not available.
- Prefer focused targets while iterating; use `bazel test //...` only when
  broad verification is needed.
- Check `.bazelrc`, `.github/workflows/ci.yml`, and `docs/android.md` before changing platform build behavior.
- Do not check in generated protobuf files.

## Native Host Builds

Linux:

```bash
CC=clang bazel build //...
bazel test //...

cmake -S . -B build/cmake-Debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/cmake-Debug --parallel
ctest --test-dir build/cmake-Debug --output-on-failure --timeout 300
```

macOS Apple Silicon:

```bash
bazel build //... --config=macos_arm64
bazel test //... --config=macos_arm64
```

macOS Intel:

```bash
bazel build //... --config=macos_x86_64
bazel test //... --config=macos_x86_64
```

## Linux ARM32 (armhf)

Cross-compile from an x86_64 Linux host. Requires `g++-arm-linux-gnueabihf`.
`qemu-user-static` runs the resulting binaries on the host (`--config=linux_arm32`
sets `QEMU_LD_PREFIX=/usr/arm-linux-gnueabihf`).

```bash
bazel build --config=linux_arm32 //server:subspace_server //client:subspace_client
bazel test --config=linux_arm32 //common:common_test //client:client_test
```

CMake needs a host `protoc` first. The toolchain file is
`cmake/toolchains/linux-armhf.cmake`. CI runs that cross-compile as the
`cmake (armhf)` job; `scripts/build-arm32-colima.sh` is the local Colima
equivalent.

```bash
cmake -S . -B build/host-protoc -DCMAKE_BUILD_TYPE=Release
cmake --build build/host-protoc --target protoc --parallel

cmake -S . -B build/armhf \
  -DCMAKE_TOOLCHAIN_FILE=cmake/toolchains/linux-armhf.cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DPROTOC_EXECUTABLE="$PWD/build/host-protoc/_deps/protobuf-build/protoc"
cmake --build build/armhf --parallel
```

The Rust client is not part of this cross compile. Its standard library
target is not registered for `armv7-unknown-linux-gnueabihf`.

## Android With Bazel

Requires `ANDROID_NDK_HOME`.

```bash
bazel build \
  //server:subspace_server \
  //client:client_test \
  //c_client:client_test \
  //plugins:nop_plugin.so \
  //plugins:split_buffer_free_test_plugin.so \
  //android/jni:libsubspace_jni.so \
  //android/java:subspace-java \
  //android/java:subspace-java-test \
  --config=android_arm64
```

Use `--config=android_x86_64` for x86_64 emulators and GitHub CI. The emulator
deployment and test flow is in `.github/scripts/android-test.sh`.

## Android With CMake

Requires `ANDROID_NDK_HOME`. Build a host `protoc` first and pass it into the
Android cross-compile so protobuf versions match:

```bash
cmake -S . -B build/host-protoc -DCMAKE_BUILD_TYPE=Release
cmake --build build/host-protoc --target protoc --parallel

cmake -S . -B build/android \
  -DCMAKE_TOOLCHAIN_FILE=$ANDROID_NDK_HOME/build/cmake/android.toolchain.cmake \
  -DANDROID_ABI=arm64-v8a \
  -DANDROID_PLATFORM=android-28 \
  -DANDROID_STL=c++_shared \
  -DCMAKE_BUILD_TYPE=Release \
  -DPROTOC_EXECUTABLE="$PWD/build/host-protoc/_deps/protobuf-build/protoc"

cmake --build build/android --parallel
```

Use `-DANDROID_ABI=x86_64` for x86_64 emulators and GitHub CI. The emulator
deployment and test flow is in `.github/scripts/cmake-android-test.sh`.

## Android With Soong/Blueprint

Soong requires a full AOSP checkout; GitHub-hosted runners do not provide one.
Use a local AOSP tree or a self-hosted runner with AOSP already synced.

Place this repository at `external/subspace`, ensure `external/coroutines` and
`external/cpp_toolbelt` are present, and copy their repository-provided
`Android.bp.example` files to `Android.bp`.

Add to the product:

```make
PRODUCT_SOONG_NAMESPACES += external/subspace
PRODUCT_PACKAGES += \
    subspace_server \
    libsubspace_client \
    libsubspace_jni \
    subspace-java \
    subspace_java_client_test
```

Build from AOSP root:

```bash
source build/envsetup.sh
lunch <product>-userdebug

m external.subspace-subspace_server-soong \
  external.subspace-libsubspace_client-soong \
  external.subspace-libsubspace_jni-soong \
  external.subspace-subspace-java-soong \
  external.subspace-subspace_java_client_test-soong
```

Run the device-side Java integration test:

```bash
adb shell subspace_java_client_test
```

## QNX With Bazel

Requires the QNX SDP and `QNX_SDP_PATH`.

```bash
bazel build //... --config=qnx_aarch64
bazel build //... --config=qnx_x86_64
```

If the default path in `.bazelrc` is wrong, set `QNX_SDP_PATH` in the
environment or user-local `user.bazelrc`.

## References

- Android details: `docs/android.md`
- General Bazel/CMake docs: `README.md`
- CI build matrix: `.github/workflows/ci.yml`
- 32-bit ARM (armhf): Bazel `--config=linux_arm32`, or the `cmake (armhf)` job with `cmake/toolchains/linux-armhf.cmake` and `qemu-user`. Local CMake equivalent: `scripts/build-arm32-colima.sh`.
- Test runner rule: `.cursor/rules/test-runner.mdc`
