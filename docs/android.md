# Running Subspace on Android

This guide covers cross-compiling and running subspace on an Android device or
emulator from a macOS (Apple Silicon) host.

## Prerequisites

| Tool | Install |
|------|---------|
| OpenJDK 17 | `brew install openjdk@17` |
| Android CLI tools | `brew install --cask android-commandlinetools` |
| NDK 27 | via `sdkmanager` (see below) |
| ARM64 system image | via `sdkmanager` |

Install SDK components (accepts licenses automatically):

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export ANDROID_HOME="/opt/homebrew/share/android-commandlinetools"

yes | sdkmanager --sdk_root="$ANDROID_HOME" \
    "platform-tools" \
    "platforms;android-34" \
    "ndk;27.0.12077973" \
    "system-images;android-34;google_apis;arm64-v8a" \
    "emulator"
```

## Shell Environment (~/.zshrc)

```bash
export ANDROID_HOME="/opt/homebrew/share/android-commandlinetools"
export ANDROID_NDK_HOME="$ANDROID_HOME/ndk/27.0.12077973"
export PATH="/opt/homebrew/opt/openjdk@17/bin:$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$PATH"
```

## Emulator Setup

Create and boot an ARM64 AVD:

```bash
avdmanager create avd -n subspace_test \
    -k "system-images;android-34;google_apis;arm64-v8a" \
    --device "pixel_6"

emulator -avd subspace_test -no-window -no-audio -gpu swiftshader_indirect &
adb wait-for-device
```

## Cross-Compiling

Build the server and tests for Android ARM64:

```bash
bazelisk build //server:subspace_server //client:client_test \
    --config=android_arm64
```

The `android_arm64` config in `.bazelrc` sets:
- `--platforms=//platform/android:android_arm64`
- `--cpu=aarch64` (prevents legacy macOS config_settings from matching)
- `--linkopt=-lc++_static --linkopt=-lc++abi` (NDK C++ stdlib)
- `--action_env=ANDROID_NDK_HOME`

## Device Setup

### Enable root access

The emulator with `google_apis` images supports `adb root`:

```bash
adb root
```

### Socket path

The default server socket on Android is `/data/local/tmp/subspace` (defined by
`kDefaultServerSocket` in `client/client.h`). This path is writable without
root.

## Deploying Binaries

Bazel produces shared libraries as symlinks in `bazel-bin/_solib_arm64-v8a/`.
You must dereference them before pushing to the device:

```bash
# Dereference shared library symlinks
rm -rf /tmp/android_libs
mkdir -p /tmp/android_libs
cp -L bazel-bin/_solib_arm64-v8a/*.so /tmp/android_libs/

# Push libraries and binaries
adb push /tmp/android_libs/ /data/local/tmp/android_libs/
adb push bazel-bin/server/subspace_server /data/local/tmp/
adb push bazel-bin/client/client_test /data/local/tmp/
adb shell "chmod 755 /data/local/tmp/subspace_server /data/local/tmp/client_test"

# Push plugins
adb shell "mkdir -p /data/local/tmp/plugins"
adb push bazel-bin/plugins/nop_plugin.so /data/local/tmp/plugins/
adb push bazel-bin/plugins/split_buffer_free_test_plugin.so /data/local/tmp/plugins/
```

## Running

### Start the server

```bash
adb shell "cd /data/local/tmp && ./subspace_server &"
```

The server uses the default socket `/data/local/tmp/subspace` on Android. Shared
memory is fd-backed and does not require a device-visible directory.

### Run tests

```bash
adb shell "cd /data/local/tmp && LD_LIBRARY_PATH=/data/local/tmp/android_libs ./client_test"
```

`LD_LIBRARY_PATH` is required because the test binary links against shared
libraries that live in the `android_libs/` directory.

To run a specific test:

```bash
adb shell "cd /data/local/tmp && LD_LIBRARY_PATH=/data/local/tmp/android_libs \
    ./client_test --gtest_filter='ClientTest.Init'"
```

## Android-Specific Implementation Notes

### Shared Memory

Android lacks POSIX shared memory (`shm_open`/`shm_unlink`). Subspace uses
`SUBSPACE_SHMEM_MODE_ANDROID` (defined in `common/channel.h`) which:

- Creates anonymous `memfd_create()` regions for the SCB, CCB, BCB, and
  client-owned message buffers.
- Sizes them with `ftruncate()` and maps them with `mmap()`.
- Passes file descriptors between processes via Unix domain sockets
  (`SCM_RIGHTS`).
- Keeps client-side buffer allocation and resize by registering publisher-owned
  buffer FDs with the server, which brokers them to subscribers.

### Split Buffers

Built-in split buffers also use anonymous FDs. The publisher registers the
prefix and slot FDs with the server; subscribers fetch those descriptors when
attaching to a new buffer generation. Custom split-buffer callbacks continue to
use the callback-provided handles.

### Linker Namespaces

Android enforces linker namespace restrictions. Shared libraries must be in a
directory referenced by `LD_LIBRARY_PATH` or in the same directory as the
executable. The `android_libs/` approach works for `/data/local/tmp/` binaries.

## CMake Cross-Compilation

Subspace can be cross-compiled for Android using CMake with the NDK toolchain:

```bash
export ANDROID_NDK_HOME=/path/to/ndk

cmake -S . -B build/android \
  -DCMAKE_TOOLCHAIN_FILE=$ANDROID_NDK_HOME/build/cmake/android.toolchain.cmake \
  -DANDROID_ABI=arm64-v8a \
  -DANDROID_PLATFORM=android-28 \
  -DANDROID_STL=c++_shared \
  -DCMAKE_BUILD_TYPE=Release

cmake --build build/android --parallel
```

CMake cross-compiles generate protobuf sources in the build tree. Ensure the
host-native `protoc` matches the protobuf version fetched by CMake, or pass a
matching compiler explicitly with `-DPROTOC_EXECUTABLE=/path/to/protoc`. The CI
build does this by building the native `protoc` target first and passing that
binary into the Android configure step.

## AOSP / Soong (Blueprint) Build

Subspace provides `Android.bp` files for building as part of an AOSP source
tree using the Soong build system. This is the recommended approach for
integrating subspace into an Android platform image.

### Directory Layout

Place the subspace source tree in your AOSP checkout (e.g.,
`external/subspace/`). The Blueprint files define these modules:

| Module | Type | Description |
|--------|------|-------------|
| `libsubspace_common` | static lib | Core channel, shared memory, syscall shim |
| `libsubspace_client` | shared lib | Client API (publisher/subscriber) |
| `libsubspace_server` | static lib | Server implementation |
| `subspace_server` | binary | Standalone server daemon |
| `libsubspace_proto` | static lib | Protobuf message definitions |
| `libsubspace_jni` | shared lib | JNI bindings for Java clients |
| `subspace-java` | java lib | Java client wrapper |

### External Dependencies

Subspace requires two external libraries that must also be present in the AOSP
tree:

1. **coroutines** (`external/coroutines/`) — https://github.com/dallison/coroutines
2. **cpp_toolbelt** (`external/cpp_toolbelt/`) — https://github.com/dallison/cpp_toolbelt

Example Blueprint files for both are provided in
`external/coroutines/Android.bp.example` and
`external/cpp_toolbelt/Android.bp.example` within this repository. Copy these to
`Android.bp` in the respective source trees in your AOSP checkout.

### AOSP Dependencies

The following modules must be available in the AOSP tree (they are part of
standard AOSP):

- `libprotobuf-cpp-full` — Protocol Buffers runtime. `subspace.proto` uses
  `google.protobuf.Any`, which is not available from the lite runtime.
- `liblog` — Android logging
- `libdl` — Dynamic linker
- `libabsl` — Abseil runtime and headers
- `jni_headers` — JNI headers (for the JNI module)

### Building

Add Subspace to a product makefile:

```make
PRODUCT_SOONG_NAMESPACES += external/subspace
PRODUCT_PACKAGES += subspace_server libsubspace_client libsubspace_jni subspace-java
```

Then build from your AOSP root:

```bash
m external.subspace-subspace_server-soong \
  external.subspace-libsubspace_client-soong \
  external.subspace-libsubspace_jni-soong \
  external.subspace-subspace-java-soong
```

### Integration Notes

- The `subspace_defaults` module in the root `Android.bp` sets C++17 mode,
  warning flags, exceptions/RTTI, and the `-DSUBSPACE_ANDROID` preprocessor
  define.
- All modules use `stl: "c++_shared"` and `min_sdk_version: "28"`.
- The server binary can be included in the system partition via
  `PRODUCT_PACKAGES += subspace_server` in your device makefile.
- The JNI library and Java wrapper can be included in apps via the standard
  AOSP module dependency mechanism.

