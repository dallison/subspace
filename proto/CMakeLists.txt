# CMakeLists.txt for the proto subdirectory
# This file defines the 'subspace_proto' library, which generates C++
# files from 'subspace.proto' and compiles them.

cmake_minimum_required(VERSION 3.15)

# Include FetchContent module for handling external dependencies
include(FetchContent)

# --- Apple Silicon (ARM64) specific settings ---
# CMAKE_OSX_ARCHITECTURES is expected to be set by the parent project or environment.
# We will propagate this value to sub-dependencies.
if(APPLE)
    message(STATUS "Building for Apple platform in proto subdirectory. Propagating CMAKE_OSX_ARCHITECTURES: ${CMAKE_OSX_ARCHITECTURES}")
endif()

# Protobuf provides targets like protobuf::libprotobuf and the protoc executable.
# --- External Dependency: Protobuf (using FetchContent for native CMake) ---
FetchContent_Declare(
    protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v29.5
    FIND_PACKAGE_ARGS NAMES protobuf
    # Protobuf's CMake build can be configured to build only necessary components
    CMAKE_ARGS
        -Dprotobuf_BUILD_TESTS=OFF # Explicitly disable building tests to avoid gmock conflicts
        -Dprotobuf_BUILD_EXAMPLES=OFF
        -Dprotobuf_BUILD_SHARED_LIBS=OFF # Build static libs
        # Pass architecture settings to Protobuf's CMake build
        CMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(protobuf)
# Protobuf provides targets like protobuf::libprotobuf and the protoc executable.
include(FindProtobuf)
find_package(protobuf CONFIG REQUIRED)


# Define the .proto file(s) in this subdirectory
set(PROTO_FILES
    subspace.proto
)

# Variables to store the generated source and header files
set(SUBSPACE_PROTO_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Define the subspace_proto library target
add_library(subspace_proto STATIC OBJECT "${CMAKE_CURRENT_LIST_DIR}/subspace.proto")
# Use the protobuf_generate function to generate C++ files
# This function automatically adds custom commands to perform the generation
# and sets up dependencies on the protoc executable.
protobuf_generate(
    TARGET subspace_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
    PROTOC_OUT_DIR ${SUBSPACE_PROTO_GENERATED_DIR})


# Add the directory containing the generated headers to the include paths
target_include_directories(subspace_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR} # Headers are generated into the build directory of this subdirectory
)

# Link the generated library against the main Protobuf library
target_link_libraries(subspace_proto PUBLIC
    protobuf::libprotobuf
)

