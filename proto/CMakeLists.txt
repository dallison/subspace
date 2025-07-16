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

# --- External Dependency: Protobuf (using FetchContent for native CMake) ---
FetchContent_Declare(
    protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v29.0
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

# Define the .proto file(s) in this subdirectory
set(PROTO_FILES
    subspace.proto
)

# Variables to store the generated source and header files
set(SUBSPACE_PROTO_GENERATED_SRCS "")
set(SUBSPACE_PROTO_GENERATED_HDRS "")

# Use the protobuf_generate_cpp function to generate C++ files
# This function automatically adds custom commands to perform the generation
# and sets up dependencies on the protoc executable.
protobuf_generate_cpp(SUBSPACE_PROTO_GENERATED_SRCS SUBSPACE_PROTO_GENERATED_HDRS ${PROTO_FILES})

# Define the subspace_proto library target
add_library(subspace_proto STATIC
    ${SUBSPACE_PROTO_GENERATED_SRCS} # Add the generated .cc files
)

# Add the directory containing the generated headers to the include paths
target_include_directories(subspace_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR} # Headers are generated into the build directory of this subdirectory
)

# Link the generated library against the main Protobuf library
target_link_libraries(subspace_proto PUBLIC
    protobuf::libprotobuf
)

# Ensure the generation step runs before compiling the library
# (protobuf_generate_cpp typically handles this implicitly, but explicit is clear)
add_dependencies(subspace_proto protobuf) # Depends on the main protobuf target being built


