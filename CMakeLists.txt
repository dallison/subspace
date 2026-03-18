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
set(ABSL_BUILD_TEST_HELPERS ON CACHE INTERNAL "Enable Abseil TESTONLY+PUBLIC helpers (e.g. status_matchers)")
set(ABSL_USE_EXTERNAL_GOOGLETEST ON CACHE INTERNAL "Use our GoogleTest instead of Abseil downloading its own")
set(ABSL_FIND_GOOGLETEST OFF CACHE INTERNAL "Don't find_package(GTest); we provide it via FetchContent")

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

# --- External Dependency: Googletest (using FetchContent for native CMake) ---
# Fetched first so Abseil can find it when ABSL_BUILD_TEST_HELPERS is ON.
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2 # Matches the version in MODULE.bazel
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(googletest)

# --- External Dependency: Abseil (using FetchContent for native CMake) ---
set(ABSL_ENABLE_INSTALL ON)
FetchContent_Declare(
    abseil
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20240722.0
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
        -DABSL_PROPAGATE_CXX_STD=ON
)
FetchContent_MakeAvailable(abseil)

# --- External Dependency: co (using FetchContent for native CMake) ---
# MUST be declared before cpp_toolbelt since cpp_toolbelt depends on it
FetchContent_Declare(
    co
    GIT_REPOSITORY https://github.com/dallison/co.git
    GIT_TAG c1cd0992d51c7a9bf9e4ecc0b6204cca05adc4a6
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(co)

# --- External Dependency: cpp_toolbelt (using FetchContent for native CMake) ---
# cpp_toolbelt's CMakeLists.txt will try to fetch co, but since we've already
# declared it above, FetchContent will use our version instead
FetchContent_Declare(
    cpp_toolbelt
    GIT_REPOSITORY https://github.com/dallison/cpp_toolbelt.git
    GIT_TAG 1aa8db7fd7096b53a1daa1bba86bbda8a312c7dd
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(cpp_toolbelt)

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
add_subdirectory(common)
add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(shadow)
add_subdirectory(c_client)
add_subdirectory(rust_client)

