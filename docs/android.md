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

### Create shared memory directory

Subspace on Android uses regular files in a tmpfs-backed directory instead of
POSIX `shm_open` (which is unavailable on Android). The default directory is
`/dev/subspace` (defined by `kDefaultAndroidShmDir` in `common/channel.h`).

```bash
adb shell "mkdir -p /dev/subspace && chmod 777 /dev/subspace"
```

Without this directory, any channel creation will fail with a file-not-found
error.

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

The server uses the default socket `/data/local/tmp/subspace` and shared memory
directory `/dev/subspace` on Android. No flags are needed for local operation.

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

- Creates regular files in the `kDefaultAndroidShmDir` (`/dev/subspace`)
  directory using `open()`/`mkstemp()` instead of `shm_open()`
- Uses `ftruncate()` + `mmap()` on those files (same as POSIX shm)
- Passes file descriptors between processes via Unix domain sockets
  (`SCM_RIGHTS`)
- Cleans up with `unlink()` instead of `shm_unlink()`

The directory should be on a tmpfs mount for performance. On the emulator,
`/dev/` is typically tmpfs-backed.

### Split Buffers

Split buffer shared memory (`common/split_buffer.cc`) also uses the Android shm
directory for its backing files, following the same pattern as regular channel
buffers.

### Linker Namespaces

Android enforces linker namespace restrictions. Shared libraries must be in a
directory referenced by `LD_LIBRARY_PATH` or in the same directory as the
executable. The `android_libs/` approach works for `/data/local/tmp/` binaries.
