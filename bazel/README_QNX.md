# QNX 8 Cross-Compilation with Bazel

This directory contains Bazel toolchain configurations and build scripts for cross-compiling Subspace for QNX 8 on both ARM64 (aarch64) and x86_64 architectures.

## Prerequisites

1. **QNX Software Development Platform (SDP) 8.0** installed
   - Default location: `~/qnx800`
   - If installed elsewhere, you need to modify the default path in `bazel/toolchains/qnx/qnx_toolchain_config.bzl`
   - Or set `QNX_SDP_PATH` environment variable (note: toolchain uses hardcoded default, see Configuration section)

2. **Bazel 6.0 or later**

3. **Standard build tools** (make, etc.)

## Quick Start

Build directly with Bazel using the QNX configurations:

```bash
# Build for ARM64
bazel build --config=qnx_aarch64 //client:subspace_client

# Build for x86_64
bazel build --config=qnx_x86_64 //client:subspace_client

# Debug build
bazel build --config=qnx_aarch64 --compilation_mode=dbg //client:client_test

# Build all targets
bazel build --config=qnx_aarch64 //...

# Release build (default)
bazel build --config=qnx_aarch64 --compilation_mode=opt //client:subspace_client
```

## Configuration Options

### Environment Variables

- **`QNX_SDP_PATH`**: Path to QNX SDP installation
  - **Note**: The toolchain uses a hardcoded default path (`~/qnx800`)
  - To use a different path, edit `bazel/toolchains/qnx/qnx_toolchain_config.bzl` and change the `qnx_sdp_path` default value
  - The build script sets `QNX_SDP_PATH` but the toolchain config doesn't currently read it dynamically
  - Example modification in `qnx_toolchain_config.bzl`:
    ```python
    "qnx_sdp_path": attr.string(default = "/opt/qnx800"),  # Change this
    ```

### Bazel Configurations

The `.bazelrc` file defines the following configurations:

- **`qnx_aarch64`**: Build for QNX ARM64
  - Usage: `bazel build --config=qnx_aarch64 <target>`

- **`qnx_x86_64`**: Build for QNX x86_64
  - Usage: `bazel build --config=qnx_x86_64 <target>`

- **`qnx`**: Common flags for QNX builds
  - Automatically included with the above configs

## Architecture-Specific Details

### ARM64 (aarch64)

- Compiler variant: `gcc_ntoaarch64`
- Architecture flags: `-march=armv8-a+crc` (enables CRC32 instructions)
- Platform: `//bazel/toolchains/qnx/platforms:qnx_aarch64`

### x86_64

- Compiler variant: `gcc_ntox86_64`
- Architecture flags: `-m64`
- Platform: `//bazel/toolchains/qnx/platforms:qnx_x86_64`

## Toolchain Structure

The QNX toolchain is defined in `bazel/toolchains/qnx/`:

- **`BUILD.bazel`**: Toolchain definitions and declarations
- **`qnx_toolchain_config.bzl`**: Toolchain configuration rule
- **`platforms/BUILD.bazel`**: Platform definitions

### Toolchain Features

The toolchain automatically configures:

- **Compiler**: `qcc` with appropriate variant (`-Vgcc_ntoaarch64` or `-Vgcc_ntox86_64`)
- **C Standard**: C11 (`-Wc,-std=c11`)
- **C++ Standard**: C++17 (`-Wc,-std=c++17`)
- **Include Paths**: QNX headers from `target/qnx7/usr/include`
- **Library Paths**: QNX libraries from `target/qnx7/{arch}/usr/lib`
- **Preprocessor Defines**: `__QNX__` for conditional compilation
- **Assembly Support**: Handles `.S` files with appropriate architecture flags

## Building Specific Targets

```bash
# Build client library
bazel build --config=qnx_aarch64 //client:subspace_client

# Build tests (note: tests may not run on host, only build)
bazel build --config=qnx_aarch64 //client:client_test

# Build server
bazel build --config=qnx_aarch64 //server:subspace_server

# Build everything
bazel build --config=qnx_aarch64 //...
```

## Troubleshooting

### QNX_SDP_PATH not found

If you get errors about QNX_SDP_PATH:

1. The toolchain uses a hardcoded default path (`~/qnx800`)
2. To use a different path, edit `bazel/toolchains/qnx/qnx_toolchain_config.bzl` and change the `qnx_sdp_path` default value
3. The `QNX_SDP_PATH` environment variable is not currently used by the toolchain (Bazel limitation)

### qcc compiler not found

If Bazel can't find the qcc compiler:

1. Verify your QNX SDP path is correct
2. Check that `qcc` exists at: `$QNX_SDP_PATH/host/linux/x86_64/usr/bin/qcc`
3. Ensure the QNX SDP is properly installed

### Platform not found

If you see "platform not found" errors:

1. Ensure the toolchains are registered in `MODULE.bazel`
2. Verify the platform files exist in `bazel/toolchains/qnx/platforms/`
3. Try cleaning and rebuilding:
   ```bash
   bazel clean
   bazel build --config=qnx_aarch64 //client:subspace_client
   ```

### Missing QNX headers/libraries

If compilation fails due to missing headers or libraries:

1. Verify your QNX SDP installation is complete
2. Check that the sysroot path is correct: `$QNX_SDP_PATH/target/qnx7`
3. Ensure the target architecture libraries exist: `$QNX_SDP_PATH/target/qnx7/$QNX_ARCH/usr/lib`

### Assembly file compilation errors

If you see errors with `arm_crc32.S`:

1. Ensure the file includes QNX support (should have `defined(__QNX__)` check)
2. Verify the architecture flags are correct for your target
3. Check that CRC32 instructions are enabled for ARM64 (`-march=armv8-a+crc`)

## Testing

After building, you can test the binaries on a QNX target system:

```bash
# Copy binaries to QNX system
scp bazel-bin/client/client_test user@qnx-target:/tmp/

# Run on QNX system
ssh user@qnx-target /tmp/client_test
```

## Notes

- The toolchain uses QNX's `qcc` compiler wrapper, which internally uses GCC
- Assembly files (like `arm_crc32.S`) are automatically handled by Bazel
- The toolchain sets up proper include and library paths for QNX
- All QNX-specific compiler flags are automatically configured
- Tests built for QNX cannot run on the host system - they must be run on a QNX target

## Differences from CMake

- Bazel uses platform-based configuration rather than toolchain files
- Toolchains are registered in `MODULE.bazel` rather than being passed as arguments
- Build configurations are specified via `--config` flags
- QNX_SDP_PATH must be configured in the toolchain file (hardcoded default) rather than via environment variables

## See Also

- [QNX Documentation](https://www.qnx.com/developers/docs/)
- [Bazel C++ Toolchain Configuration](https://bazel.build/docs/cc-toolchain-config-reference)
- [Bazel Platforms and Toolchains](https://bazel.build/docs/platforms-intro)
