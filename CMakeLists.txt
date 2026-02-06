# CMakeLists.txt for the Subspace project
# This file converts a Bazel-based project with external dependencies
# to a pure CMake build system. It uses FetchContent to fetch and build
# Googletest, Abseil, Protobuf, cpp_toolbelt, and co using their native CMake
# build systems.

cmake_minimum_required(VERSION 3.15)
project(Subspace LANGUAGES CXX ASM)

# Set C++ standard and compiler flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # Prefer standard C++

set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "No protobuf tests")

# Add common compile options as found in subspace/BUILD
# These will apply to all targets in this CMakeLists.txt unless overridden.
add_compile_options(-Wall -Wextra -Wno-missing-field-initializers -Wno-unused-parameter)

# --- Apple Silicon (ARM64) specific settings ---
# Ensure that CMake targets the correct architecture when building on Apple platforms.
# This value will be propagated to sub-dependencies.
if(APPLE)
    # If CMAKE_OSX_ARCHITECTURES is not already set (e.g., by command line -DCMAKE_OSX_ARCHITECTURES=arm64)
    # then default it to arm64 for Apple Silicon.
    if(NOT CMAKE_OSX_ARCHITECTURES)
        set(CMAKE_OSX_ARCHITECTURES "arm64")
    endif()
    message(STATUS "Building for Apple platform. Using CMAKE_OSX_ARCHITECTURES: ${CMAKE_OSX_ARCHITECTURES}")
endif()

# Include FetchContent module for handling external dependencies
include(FetchContent)

# --- External Dependency: Abseil (using FetchContent for native CMake) ---
set(ABSL_ENABLE_INSTALL ON)
FetchContent_Declare(
    abseil
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20240722.0 # Matches the version in your WORKSPACE
    # Pass architecture settings and CXX standard propagation to Abseil's CMake build
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
        -DABSL_PROPAGATE_CXX_STD=ON # Propagate C++ standard settings
)
FetchContent_MakeAvailable(abseil)
# Abseil provides targets like absl::base, absl::strings, etc.

# --- External Dependency: co (using FetchContent for native CMake) ---
# Using the same version as Bazel build (commit cf1252b2f5952d7cba83b67dd69288971c0a2b57)
# MUST be declared before cpp_toolbelt since cpp_toolbelt depends on it
FetchContent_Declare(
    co
    GIT_REPOSITORY https://github.com/dallison/co.git
    GIT_TAG c1cd0992d51c7a9bf9e4ecc0b6204cca05adc4a6
    # Pass architecture settings to co's CMake build
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(co)
# co provides the 'co' target.

# --- External Dependency: cpp_toolbelt (using FetchContent for native CMake) ---
# Using the same version as Bazel build (commit 7ec0e2cd2a17a006f976df23d07ad530499249e0)
# Note: cpp_toolbelt's CMakeLists.txt will try to fetch co, but since we've already
# declared it above, FetchContent will use our version instead
FetchContent_Declare(
    cpp_toolbelt
    GIT_REPOSITORY https://github.com/dallison/cpp_toolbelt.git
    GIT_TAG 1aa8db7fd7096b53a1daa1bba86bbda8a312c7dd
    # Pass architecture settings to cpp_toolbelt's CMake build
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(cpp_toolbelt)
# cpp_toolbelt provides the 'toolbelt' target.

# --- External Dependency: Googletest (using FetchContent for native CMake) ---
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2 # Matches the version in MODULE.bazel
    # Pass architecture settings to Googletest's CMake build
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(googletest)
# Googletest provides targets like gtest, gtest_main, gmock, gmock_main

# --- External Dependency: Protobuf (using FetchContent for native CMake) ---
FetchContent_Declare(
    protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v29.5
    # Protobuf's CMake build can be configured to build only necessary components
    CMAKE_ARGS
        -Dprotobuf_BUILD_TESTS=OFF # Explicitly disable building tests to avoid gmock conflicts
        -Dprotobuf_BUILD_EXAMPLES=OFF
        -Dprotobuf_BUILD_SHARED_LIBS=OFF # Build static libs
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(protobuf)
# Protobuf provides targets like protobuf::libprotobuf and protobuf::protoc
# These targets can be used directly without find_package

#add to get */file.h includes working
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
#add to get proto/subspace.pb.h working
include_directories(${CMAKE_CURRENT_BINARY_DIR})
# --- Subspace Tests Target ---
# Corresponds to //subspace/tests:tests
enable_testing() # Enable CTest for running tests

# --- Add Subdirectories ---
# These directories are expected to contain their own CMakeLists.txt files
# that define targets (libraries, executables, etc.)
add_subdirectory(proto)
add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(common)
add_subdirectory(c_client)

