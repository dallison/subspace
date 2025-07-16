# CMakeLists.txt for the Subspace project
# This file converts a Bazel-based project with external dependencies
# to a pure CMake build system. It uses FetchContent to fetch and build
# Googletest, Abseil, Protobuf, cpp_toolbelt, and co using their native CMake
# build systems.

cmake_minimum_required(VERSION 3.15)
project(Subspace LANGUAGES CXX)

# Set C++ standard and compiler flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # Prefer standard C++

set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "No protobuf tests")

# Add common compile options as found in subspace/BUILD
# These will apply to all targets in this CMakeLists.txt unless overridden.
add_compile_options(-Wall -Wextra)

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
# FetchContent_Declare(
#     googletest
#     GIT_REPOSITORY https://github.com/google/googletest.git
#     GIT_TAG release-1.11.0 # Matches the version in your WORKSPACE
#     # Pass architecture settings to Googletest's CMake build
#     CMAKE_ARGS
#         CMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
# )
# FetchContent_MakeAvailable(googletest)
# Googletest provides targets like gtest_main, gtest, gmock.

# --- External Dependency: Abseil (using FetchContent for native CMake) ---
FetchContent_Declare(
    abseil
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20230802.0 # Matches the version in your WORKSPACE
    # Pass architecture settings and CXX standard propagation to Abseil's CMake build
    CMAKE_ARGS
        CMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
        -DABSL_PROPAGATE_CXX_STD=ON # Propagate C++ standard settings
)
FetchContent_MakeAvailable(abseil)
# Abseil provides targets like absl::base, absl::strings, etc.

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
        # Pass architecture settings to Protobuf's CMake build
        CMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(protobuf)
# Protobuf provides targets like protobuf::libprotobuf and the protoc executable.

# --- External Dependency: cpp_toolbelt (using FetchContent for native CMake) ---
# Assumes cpp_toolbelt repository now contains a CMakeLists.txt at its root.
FetchContent_Declare(
    cpp_toolbelt
    GIT_REPOSITORY https://github.com/dallison/cpp_toolbelt.git
    GIT_TAG main
    # Pass architecture settings to cpp_toolbelt's CMake build
    CMAKE_ARGS
        CMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(cpp_toolbelt)
# cpp_toolbelt provides the 'toolbelt' target.


# --- External Dependency: co (using FetchContent for native CMake) ---
# Assumes co repository now contains a CMakeLists.txt at its root.
FetchContent_Declare(
    co
    GIT_REPOSITORY https://github.com/dallison/co.git
    GIT_TAG main
    # Pass architecture settings to co's CMake build
    CMAKE_ARGS
        CMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(co)
# co provides the 'co' target.

# Define the .proto file(s) in this subdirectory
set(PROTO_FILES
    proto/subspace.proto
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


# --- Add Subdirectories ---
# These directories are expected to contain their own CMakeLists.txt files
# that define targets (libraries, executables, etc.)
add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(common)
add_subdirectory(c_client)


# --- Subspace Tests Target ---
# Corresponds to //subspace/tests:tests
enable_testing() # Enable CTest for running tests

add_executable(subspace_tests
    # Source files for tests are omitted here for manual addition by the user.
    # These might also be moved into a 'tests' subdirectory.
)

target_link_libraries(subspace_tests PRIVATE
    # Link against your internal common library or other sub-project libraries here
    gtest_main # Googletest main target for tests
    protobuf::libprotobuf # Protobuf library for tests
    toolbelt # Link against the cpp_toolbelt library
    co # Link against the co library
)

target_include_directories(subspace_tests PRIVATE
    # Include generated protobuf headers for tests (if handled by a sub-project)
)

# Dependencies for tests (e.g., if they depend on targets defined in subdirectories)
# For example: add_dependencies(subspace_tests common_lib)

# Add the test executable to CTest
add_test(NAME subspace_all_tests COMMAND subspace_tests)


