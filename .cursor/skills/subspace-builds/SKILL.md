---
name: subspace-builds
description: Build and test Subspace across supported platforms and build systems. Use when the user asks how to build, test, cross-compile, run CI-equivalent checks, or work with Bazel, CMake, Android, Soong/Blueprint, AOSP, QNX, Linux, or macOS builds in this repository.
---

# Subspace Builds

## General Rules

- Use `bazelisk`, not `bazel`, for repository Bazel commands.
- Prefer focused targets while iterating; use `bazelisk test //...` only when broad verification is needed.
- Check `.bazelrc`, `.github/workflows/ci.yml`, and `docs/android.md` before changing platform build behavior.
- Do not check in generated protobuf files.

## Native Host Builds

Linux:

```bash
CC=clang bazelisk build //...
bazelisk test //...

cmake -S . -B build/cmake-Debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/cmake-Debug --parallel
ctest --test-dir build/cmake-Debug --output-on-failure
```

macOS Apple Silicon:

```bash
bazelisk build //... --config=macos_arm64
bazelisk test //... --config=macos_arm64
```

macOS Intel:

```bash
bazelisk build //... --config=macos_x86_64
bazelisk test //... --config=macos_x86_64
```

## Android With Bazel

Requires `ANDROID_NDK_HOME`.

```bash
bazelisk build \
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
bazelisk build //... --config=qnx_aarch64
bazelisk build //... --config=qnx_x86_64
```

If the default path in `.bazelrc` is wrong, set `QNX_SDP_PATH` in the
environment or user-local `user.bazelrc`.

## References

- Android details: `docs/android.md`
- General Bazel/CMake docs: `README.md`
- CI build matrix: `.github/workflows/ci.yml`
- Test runner rule: `.cursor/rules/test-runner.mdc`
