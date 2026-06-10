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

## Building With Bazel

Bazel is the simplest way to cross-compile Subspace Android artifacts from this
repository because it fetches its own C++ dependencies and uses the NDK
toolchain configured in `.bazelrc`.

Build the server, native tests, JNI library, and Java client test for Android
ARM64:

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

The `android_arm64` config in `.bazelrc` sets:
- `--platforms=//platform/android:android_arm64`
- `--cpu=aarch64` (prevents legacy macOS config_settings from matching)
- `--linkopt=-lc++_static --linkopt=-lc++abi` (NDK C++ stdlib)
- `--action_env=ANDROID_NDK_HOME`

For an x86_64 emulator or CI runner, use `--config=android_x86_64` instead.

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

## Deploying Bazel Binaries

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

## Running Bazel-built Tests

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
`SUBSPACE_SHMEM_MODE_MEMFD` (defined in `common/channel.h`) which:

- Creates anonymous `memfd_create()` regions for the SCB, CCB, BCB, and
  client-owned message buffers.
- Sizes them with `ftruncate()` and maps them with `mmap()`.
- Passes file descriptors between processes via Unix domain sockets
  (`SCM_RIGHTS`).
- Keeps client-side buffer allocation and resize by registering publisher-owned
  buffer FDs with the server, which brokers them to subscribers.

`SUBSPACE_SHMEM_MODE_MEMFD` names the mechanism, not the platform: Android always
uses it, but it can also be selected on Linux (independently of Android) by
building with `--config=linux_memfd` (Bazel) or `-DSUBSPACE_LINUX_USE_MEMFD=ON`
(CMake). The default Linux backend remains named `/dev/shm` objects
(`SUBSPACE_SHMEM_MODE_LINUX`); macOS/QNX (`SUBSPACE_SHMEM_MODE_POSIX`) are
unaffected. Note that memfd mode routes buffer creation/resize through the
server (anonymous FDs must be passed via `SCM_RIGHTS`), whereas the named
`/dev/shm` backend maps buffers directly by name without server round-trips.

### Split Buffers

Built-in split buffers also use anonymous FDs. The publisher registers the
prefix and slot FDs with the server; subscribers fetch those descriptors when
attaching to a new buffer generation. Custom split-buffer callbacks continue to
use the callback-provided handles.

### Linker Namespaces

Android enforces linker namespace restrictions. Shared libraries must be in a
directory referenced by `LD_LIBRARY_PATH` or in the same directory as the
executable. The `android_libs/` approach works for `/data/local/tmp/` binaries.

## Building With CMake

Subspace can also be cross-compiled for Android using CMake with the NDK
toolchain. CMake fetches the same third-party dependencies with
`FetchContent`, but protobuf code generation requires a host-native `protoc`
that matches the protobuf version used by the Android build.

```bash
export ANDROID_NDK_HOME=/path/to/ndk

# Build a host protoc matching Subspace's protobuf dependency.
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

Use `-DANDROID_ABI=x86_64` for an x86_64 emulator. The Android CMake build
produces the native test binaries plus the Java/JNI artifacts:

- `build/android/server/subspace_server`
- `build/android/client/client_test`
- `build/android/c_client/c_client_test`
- `build/android/android/libsubspace_jni.so`
- `build/android/android/subspace-java.jar`
- `build/android/android/subspace-java-test.jar`

The CI helper script `.github/scripts/cmake-android-test.sh` shows the expected
deployment layout and how to run the CMake-built tests on an emulator.

## AOSP / Soong (Blueprint) Build

Subspace provides `Android.bp` files for building as part of an AOSP source
tree using the Soong build system. This is the recommended approach for
integrating subspace into an Android platform image.

Soong builds require a full AOSP checkout. GitHub-hosted runners do not provide
one by default; use a local AOSP tree or a self-hosted CI runner with AOSP
already synced.

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
| `subspace_java_client_test` | java binary | Device-side Java integration test |

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
PRODUCT_PACKAGES += \
    subspace_server \
    libsubspace_client \
    libsubspace_jni \
    subspace-java \
    subspace_java_client_test
```

Then build from your AOSP root:

```bash
source build/envsetup.sh
lunch <product>-userdebug

m external.subspace-subspace_server-soong \
  external.subspace-libsubspace_client-soong \
  external.subspace-libsubspace_jni-soong \
  external.subspace-subspace-java-soong \
  external.subspace-subspace_java_client_test-soong
```

After flashing or installing those artifacts on a device image, the Java
integration test can be run through its wrapper:

```bash
adb shell subspace_java_client_test
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

## Bridging Between the Emulator and the Host

A Subspace server inside the emulator can bridge channels to a server running
natively on the host, in both directions (publish on Android / subscribe on the
host, and vice versa). The emulator's user‑mode network is a NAT where only the
guest can initiate connections and UDP broadcast does not cross the boundary, so
two server features are used:

- `--tcp_discovery` — run discovery over a single TCP connection that the guest
  dials to the host (instead of UDP broadcast/unicast). See
  [`server-architecture.md`](server-architecture.md) for details.
- `--bridge_advertise_address=127.0.0.1` — advertise a loopback endpoint for
  bridge listeners, reached through `adb` port tunnels.

The host and guest use **different** bridge ports so the `adb forward`/`reverse`
loopback listeners never collide with a server's own listener. With the host
acting as the discovery listener:

```bash
# Tunnels: guest dials out to the host (reverse); host dials into the guest (forward).
adb reverse tcp:6502 tcp:6502   # discovery: guest -> host
adb reverse tcp:7100 tcp:7100   # bridge data: guest publisher -> host subscriber
adb forward tcp:7200 tcp:7200   # bridge data: host publisher -> guest subscriber

# Host (discovery listener):
./subspace_server --socket=/tmp/subspace_host --tcp_discovery --disc_port=6502 \
    --bridge_ports=7100 --bridge_advertise_address=127.0.0.1 \
    --cleanup_filesystem=false

# Guest (discovery connector), inside the emulator:
adb shell "/data/local/tmp/subspace_server --socket=/data/local/tmp/subspace \
    --tcp_discovery --peer_address=127.0.0.1 --peer_port=6502 \
    --bridge_ports=7200 --bridge_advertise_address=127.0.0.1"
```

Publishers must be created with `local=false` for their channels to bridge.

The script `manual_tests/cross_host_bridge.sh` automates this end to end and
verifies both directions:

```bash
manual_tests/cross_host_bridge.sh native     # two servers on the host (loopback)
manual_tests/cross_host_bridge.sh emulator   # host <-> running emulator
```

