#!/bin/bash
# Build script for QNX 8 cross-compilation
# Supports both aarch64 and x86_64 architectures

set -e

# Default values
QNX_SDP_PATH="${HOME}/qnx800"
QNX_ARCH="aarch64"
BUILD_DIR="build_qnx"
BUILD_TYPE="Release"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --arch)
            QNX_ARCH="$2"
            shift 2
            ;;
        --sdp-path)
            QNX_SDP_PATH="$2"
            shift 2
            ;;
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --arch ARCH        Target architecture (aarch64 or x86_64) [default: aarch64]"
            echo "  --sdp-path PATH    Path to QNX SDP [default: ~/qnx800]"
            echo "  --build-dir DIR    Build directory [default: build_qnx]"
            echo "  --build-type TYPE  Build type (Debug, Release, RelWithDebInfo, MinSizeRel) [default: Release]"
            echo "  --help             Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --arch aarch64"
            echo "  $0 --arch x86_64 --sdp-path /opt/qnx800"
            echo "  $0 --arch aarch64 --build-type Debug"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate architecture
if [[ "$QNX_ARCH" != "aarch64" && "$QNX_ARCH" != "x86_64" ]]; then
    echo "Error: Invalid architecture '$QNX_ARCH'. Must be 'aarch64' or 'x86_64'"
    exit 1
fi

# Expand ~ in path
QNX_SDP_PATH="${QNX_SDP_PATH/#\~/$HOME}"

# Validate QNX SDP path
if [[ ! -d "$QNX_SDP_PATH" ]]; then
    echo "Error: QNX SDP path does not exist: $QNX_SDP_PATH"
    exit 1
fi

# Validate qcc compiler exists
QCC_PATH="$QNX_SDP_PATH/host/linux/x86_64/usr/bin/qcc"
if [[ ! -f "$QCC_PATH" ]]; then
    echo "Error: qcc compiler not found at: $QCC_PATH"
    exit 1
fi

echo "=========================================="
echo "QNX 8 Cross-Compilation Build"
echo "=========================================="
echo "Architecture: $QNX_ARCH"
echo "QNX SDP Path: $QNX_SDP_PATH"
echo "Build Directory: $BUILD_DIR"
echo "Build Type: $BUILD_TYPE"
echo "=========================================="
echo ""

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure CMake
cmake .. \
    -DCMAKE_TOOLCHAIN_FILE=../cmake/QNX8-toolchain.cmake \
    -DQNX_ARCH="$QNX_ARCH" \
    -DQNX_SDP_PATH="$QNX_SDP_PATH" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"

# Build
cmake --build . --config "$BUILD_TYPE"

echo ""
echo "=========================================="
echo "Build completed successfully!"
echo "=========================================="
echo "Binaries are in: $BUILD_DIR"
