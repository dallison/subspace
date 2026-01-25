# QNX 8 Cross-Compilation Toolchain

This directory contains CMake toolchain files and build scripts for cross-compiling Subspace for QNX 8 on both ARM64 (aarch64) and x86_64 architectures.

## Prerequisites

1. **QNX Software Development Platform (SDP) 8.0** installed in your home directory at `~/qnx800`
   - If installed elsewhere, specify the path using `-DQNX_SDP_PATH`

2. **CMake 3.15 or later**

3. **Standard build tools** (make, etc.)

## Quick Start

### Using the Build Script (Recommended)

The easiest way to build for QNX is using the provided build script:

```bash
# Build for ARM64 (default)
./cmake/build_qnx.sh --arch aarch64

# Build for x86_64
./cmake/build_qnx.sh --arch x86_64

# Custom QNX SDP path
./cmake/build_qnx.sh --arch aarch64 --sdp-path /opt/qnx800

# Debug build
./cmake/build_qnx.sh --arch aarch64 --build-type Debug
```

### Manual CMake Configuration

You can also configure CMake manually:

```bash
# Create build directory
mkdir -p build_qnx_aarch64
cd build_qnx_aarch64

# Configure for ARM64
cmake .. \
    -DCMAKE_TOOLCHAIN_FILE=../cmake/QNX8-toolchain.cmake \
    -DQNX_ARCH=aarch64 \
    -DQNX_SDP_PATH=~/qnx800 \
    -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build .
```

For x86_64:

```bash
mkdir -p build_qnx_x86_64
cd build_qnx_x86_64

cmake .. \
    -DCMAKE_TOOLCHAIN_FILE=../cmake/QNX8-toolchain.cmake \
    -DQNX_ARCH=x86_64 \
    -DQNX_SDP_PATH=~/qnx800 \
    -DCMAKE_BUILD_TYPE=Release

cmake --build .
```

## Configuration Options

### CMake Variables

- **`QNX_ARCH`**: Target architecture
  - Options: `aarch64` (default) or `x86_64`
  - Example: `-DQNX_ARCH=x86_64`

- **`QNX_SDP_PATH`**: Path to QNX SDP installation
  - Default: `~/qnx800`
  - Example: `-DQNX_SDP_PATH=/opt/qnx800`

- **`CMAKE_BUILD_TYPE`**: Build configuration
  - Options: `Debug`, `Release`, `RelWithDebInfo`, `MinSizeRel`
  - Default: `Release`

### Build Script Options

The `build_qnx.sh` script accepts the following options:

- `--arch ARCH`: Target architecture (aarch64 or x86_64)
- `--sdp-path PATH`: Path to QNX SDP
- `--build-dir DIR`: Build directory name
- `--build-type TYPE`: Build type (Debug, Release, etc.)
- `--help`: Show help message

## Architecture-Specific Details

### ARM64 (aarch64)

- Compiler variant: `gcc_ntoaarch64`
- Architecture flags: `-march=armv8-a`
- Libraries: `target/qnx7/aarch64/usr/lib`

### x86_64

- Compiler variant: `gcc_ntox86_64`
- Architecture flags: `-m64`
- Libraries: `target/qnx7/x86_64/usr/lib`

## QNX Compiler Flags

The toolchain automatically sets up:

- **Compiler**: `qcc` with appropriate variant (`-Vgcc_ntoaarch64` or `-Vgcc_ntox86_64`)
- **C Standard**: C11 (`-Wc,-std=c11`)
- **C++ Standard**: C++17 (`-Wc,-std=c++17`)
- **Sysroot**: `target/qnx7`
- **Include paths**: Automatically configured for QNX headers
- **Library paths**: Automatically configured for QNX libraries

## Troubleshooting

### qcc compiler not found

If you get an error that `qcc` is not found:

1. Verify your QNX SDP path is correct
2. Check that `qcc` exists at: `$QNX_SDP_PATH/host/linux/x86_64/usr/bin/qcc`
3. Ensure the QNX SDP is properly installed

### Architecture mismatch

If you see architecture-related errors:

1. Verify `QNX_ARCH` is set correctly (`aarch64` or `x86_64`)
2. Check that the QNX SDP has the target architecture installed
3. Ensure the toolchain file is being used (`-DCMAKE_TOOLCHAIN_FILE`)

### Missing QNX headers/libraries

If compilation fails due to missing headers or libraries:

1. Verify your QNX SDP installation is complete
2. Check that the sysroot path is correct: `$QNX_SDP_PATH/target/qnx7`
3. Ensure the target architecture libraries exist: `$QNX_SDP_PATH/target/qnx7/$QNX_ARCH/usr/lib`

## Testing

After building, you can test the binaries on a QNX target system:

```bash
# Copy binaries to QNX system
scp build_qnx/client/client_test user@qnx-target:/tmp/

# Run on QNX system
ssh user@qnx-target /tmp/client_test
```

## Notes

- The toolchain uses QNX's `qcc` compiler wrapper, which internally uses GCC
- Assembly files (like `arm_crc32.S`) are automatically handled by CMake
- The toolchain sets up proper include and library paths for QNX
- All QNX-specific compiler flags are automatically configured

## See Also

- [QNX Documentation](https://www.qnx.com/developers/docs/)
- [CMake Cross-Compiling Guide](https://cmake.org/cmake/help/latest/manual/cmake-toolchains.7.html#cross-compiling)
