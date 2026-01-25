# QNX 8 Toolchain Configuration for CMake
# Supports both aarch64 and x86_64 architectures
#
# Usage:
#   cmake -DCMAKE_TOOLCHAIN_FILE=cmake/QNX8-toolchain.cmake \
#         -DQNX_ARCH=aarch64 \
#         -DQNX_SDP_PATH=~/qnx800 \
#         <source_dir>
#
# Or for x86_64:
#   cmake -DCMAKE_TOOLCHAIN_FILE=cmake/QNX8-toolchain.cmake \
#         -DQNX_ARCH=x86_64 \
#         -DQNX_SDP_PATH=~/qnx800 \
#         <source_dir>

# Set the system name to QNX
set(CMAKE_SYSTEM_NAME QNX)

# Default QNX SDP path (can be overridden with -DQNX_SDP_PATH)
if(NOT DEFINED QNX_SDP_PATH)
    set(QNX_SDP_PATH "$ENV{HOME}/qnx800")
endif()

# Expand ~ to home directory if present
if(QNX_SDP_PATH MATCHES "^~")
    string(REPLACE "~" "$ENV{HOME}" QNX_SDP_PATH "${QNX_SDP_PATH}")
endif()

# Convert to absolute path
get_filename_component(QNX_SDP_PATH "${QNX_SDP_PATH}" ABSOLUTE)

# Validate QNX SDP path
if(NOT EXISTS "${QNX_SDP_PATH}")
    message(FATAL_ERROR "QNX SDP path does not exist: ${QNX_SDP_PATH}")
endif()

# Default architecture (can be overridden with -DQNX_ARCH)
if(NOT DEFINED QNX_ARCH)
    set(QNX_ARCH "aarch64")
endif()

# Validate architecture
if(NOT QNX_ARCH MATCHES "^(aarch64|x86_64)$")
    message(FATAL_ERROR "Invalid QNX_ARCH: ${QNX_ARCH}. Must be 'aarch64' or 'x86_64'")
endif()

# Set architecture-specific variables
if(QNX_ARCH STREQUAL "aarch64")
    set(QNX_TARGET_ARCH "aarch64")
    set(QNX_TARGET_PROCESSOR "aarch64")
    set(QNX_HOST_PLATFORM "linux")
    set(QNX_COMPILER_SUFFIX "gcc_ntoaarch64")
elseif(QNX_ARCH STREQUAL "x86_64")
    set(QNX_TARGET_ARCH "x86_64")
    set(QNX_TARGET_PROCESSOR "x86_64")
    set(QNX_HOST_PLATFORM "linux")
    set(QNX_COMPILER_SUFFIX "gcc_ntox86_64")
endif()

# Set QNX environment variables
set(ENV{QNX_HOST} "${QNX_SDP_PATH}/host/${QNX_HOST_PLATFORM}/x86_64")
set(ENV{QNX_TARGET} "${QNX_SDP_PATH}/target/qnx7")

# Find qcc compiler
find_program(QCC_COMPILER
    NAMES qcc
    PATHS "${QNX_SDP_PATH}/host/${QNX_HOST_PLATFORM}/x86_64/usr/bin"
    NO_DEFAULT_PATH
    REQUIRED
)

if(NOT QCC_COMPILER)
    message(FATAL_ERROR "qcc compiler not found in ${QNX_SDP_PATH}/host/${QNX_HOST_PLATFORM}/x86_64/usr/bin")
endif()

# Set the C and C++ compilers
set(CMAKE_C_COMPILER "${QCC_COMPILER}")
set(CMAKE_CXX_COMPILER "${QCC_COMPILER}")

# Set ASM compiler (qcc also handles assembly)
set(CMAKE_ASM_COMPILER "${QCC_COMPILER}")

# QNX qcc compiler identification
set(CMAKE_C_COMPILER_ID "QCC")
set(CMAKE_CXX_COMPILER_ID "QCC")
set(CMAKE_ASM_COMPILER_ID "QCC")

# Set compiler flags
# QNX qcc uses -V to specify the compiler variant
set(QNX_COMPILER_VARIANT "-V${QNX_COMPILER_SUFFIX}")

# Base compiler flags for QNX
set(CMAKE_C_FLAGS_INIT "${QNX_COMPILER_VARIANT}")
set(CMAKE_CXX_FLAGS_INIT "${QNX_COMPILER_VARIANT}")

# QNX-specific compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wc,-std=c11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wc,-std=c++17")

# Define QNX platform for conditional compilation
add_definitions(-D__QNX__)

# Set the sysroot
set(CMAKE_SYSROOT "${QNX_SDP_PATH}/target/qnx7")

# Set find program mode
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

# QNX-specific linker flags
set(CMAKE_EXE_LINKER_FLAGS_INIT "-V${QNX_COMPILER_SUFFIX}")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-V${QNX_COMPILER_SUFFIX}")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-V${QNX_COMPILER_SUFFIX}")

# Set architecture-specific flags
if(QNX_ARCH STREQUAL "aarch64")
    # Enable CRC32 instructions for ARM64
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wc,-march=armv8-a+crc")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wc,-march=armv8-a+crc")
    set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -march=armv8-a+crc")
elseif(QNX_ARCH STREQUAL "x86_64")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wc,-m64")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wc,-m64")
    set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -m64")
endif()

# Set include directories
include_directories(
    "${QNX_SDP_PATH}/target/qnx7/usr/include"
    "${QNX_SDP_PATH}/target/qnx7/usr/include/cpp"
)

# Set library search paths
link_directories(
    "${QNX_SDP_PATH}/target/qnx7/${QNX_TARGET_ARCH}/usr/lib"
    "${QNX_SDP_PATH}/target/qnx7/${QNX_TARGET_ARCH}/lib"
)

# Print configuration summary
message(STATUS "QNX Toolchain Configuration:")
message(STATUS "  QNX SDP Path: ${QNX_SDP_PATH}")
message(STATUS "  Target Architecture: ${QNX_ARCH}")
message(STATUS "  QCC Compiler: ${QCC_COMPILER}")
message(STATUS "  Compiler Variant: ${QNX_COMPILER_VARIANT}")
message(STATUS "  Sysroot: ${CMAKE_SYSROOT}")
