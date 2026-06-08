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
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "No protobuf tests")
set(ABSL_BUILD_TEST_HELPERS ON CACHE INTERNAL "Enable Abseil TESTONLY+PUBLIC helpers (e.g. status_matchers)")
set(ABSL_USE_EXTERNAL_GOOGLETEST ON CACHE INTERNAL "Use our GoogleTest instead of Abseil downloading its own")
set(ABSL_FIND_GOOGLETEST OFF CACHE INTERNAL "Don't find_package(GTest); we provide it via FetchContent")
set(SUBSPACE_PROTOBUF_VERSION 29.5)

# When cross-compiling, don't build protoc (we use a host-native one).
if(CMAKE_CROSSCOMPILING)
    set(protobuf_BUILD_PROTOC_BINARIES OFF CACHE INTERNAL "Skip building protoc for target")
endif()

# Add common compile options as found in subspace/BUILD
# These will apply to all targets in this CMakeLists.txt unless overridden.
add_compile_options(-Wall -Wextra -Wno-missing-field-initializers -Wno-unused-parameter)

# --- Android-specific settings ---
if(ANDROID)
    add_compile_definitions(SUBSPACE_ANDROID)
endif()

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
    GIT_TAG 3.3.2
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(co)
if(TARGET co)
    target_link_libraries(co PUBLIC absl::flat_hash_map absl::flat_hash_set)
endif()
if(TARGET co_cpp20)
    target_link_libraries(co_cpp20 PUBLIC absl::flat_hash_map absl::flat_hash_set)
endif()
if(TARGET coroutines_test)
    target_link_libraries(coroutines_test PRIVATE absl::status_matchers gmock)
endif()

# --- External Dependency: cpp_toolbelt (using FetchContent for native CMake) ---
# cpp_toolbelt's CMakeLists.txt will try to fetch co, but since we've already
# declared it above, FetchContent will use our version instead
FetchContent_Declare(
    cpp_toolbelt
    GIT_REPOSITORY https://github.com/dallison/cpp_toolbelt.git
    GIT_TAG 2.1.2
    CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(cpp_toolbelt)

# --- External Dependency: Protobuf (using FetchContent for native CMake) ---
FetchContent_Declare(
    protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v${SUBSPACE_PROTOBUF_VERSION}
    CMAKE_ARGS
        -Dprotobuf_BUILD_TESTS=OFF
        -Dprotobuf_BUILD_EXAMPLES=OFF
        -Dprotobuf_BUILD_SHARED_LIBS=OFF
        -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
)
FetchContent_MakeAvailable(protobuf)
# Protobuf provides targets like protobuf::libprotobuf and protobuf::protoc
# These targets can be used directly without find_package

# --- Option: coroutine/networking backend for the server ---
# Mirrors the Bazel //:coro_backend flag.  "co" (default) builds against the co
# coroutine library; "asio" builds the server against Boost.Asio.  The value is
# translated into the SUBSPACE_CORO_BACKEND macro (see common/async/context.h)
# and applied globally so every translation unit that includes the async
# headers sees a consistent backend.
set(SUBSPACE_CORO_BACKEND "co" CACHE STRING "Server coroutine backend: co or asio")
set_property(CACHE SUBSPACE_CORO_BACKEND PROPERTY STRINGS "co" "asio")

if(SUBSPACE_CORO_BACKEND STREQUAL "asio")
    add_compile_definitions(SUBSPACE_CORO_BACKEND=2)
    message(STATUS "Subspace coroutine backend: asio (Boost.Asio)")

    # Boost.Asio is header-only but pulls in several Boost libraries.  Fetch a
    # Boost release only when the asio backend is requested so the default co
    # build does not pay for it.
    FetchContent_Declare(
        Boost
        GIT_REPOSITORY https://github.com/boostorg/boost.git
        GIT_TAG boost-1.87.0
        CMAKE_ARGS
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
    )
    set(BOOST_INCLUDE_LIBRARIES asio coroutine context)
    FetchContent_MakeAvailable(Boost)
else()
    # co backend leaves SUBSPACE_CORO_BACKEND unset; context.h defaults to _CO.
    message(STATUS "Subspace coroutine backend: co")
endif()

# --- Option: Python bindings ---
option(SUBSPACE_PYTHON "Build Python bindings (requires Python3 and pybind11)" OFF)

if(SUBSPACE_PYTHON)
    find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

    FetchContent_Declare(
        pybind11
        GIT_REPOSITORY https://github.com/pybind/pybind11.git
        GIT_TAG v2.13.6
        CMAKE_ARGS
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}"
    )
    FetchContent_MakeAvailable(pybind11)
endif()

#add to get proto/subspace.pb.h working (build dir first so generated files win)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
#add to get */file.h includes working
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
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
add_subdirectory(plugins)
add_subdirectory(shadow)
add_subdirectory(c_client)
add_subdirectory(android)
add_subdirectory(rust_client)

if(TARGET client_test AND TARGET nop_plugin)
    add_dependencies(client_test nop_plugin)
endif()

if(TARGET client_test AND TARGET split_buffer_free_test_plugin)
    add_dependencies(client_test split_buffer_free_test_plugin)
endif()

# --- Python bindings and tests ---
if(SUBSPACE_PYTHON)
    add_subdirectory(client/python)
    add_subdirectory(server/python)

    # Create __init__.py package stubs so that the hierarchical imports
    # (e.g. "import client.python.subspace") resolve from the build tree.
    file(WRITE "${CMAKE_BINARY_DIR}/client/__init__.py" "")
    file(WRITE "${CMAKE_BINARY_DIR}/client/python/__init__.py" "")
    file(WRITE "${CMAKE_BINARY_DIR}/server/__init__.py" "")
    file(WRITE "${CMAKE_BINARY_DIR}/server/python/__init__.py" "")

    add_test(
        NAME python_client_test
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/client/python/client_test.py -v
    )
    set_tests_properties(python_client_test PROPERTIES
        ENVIRONMENT "PYTHONPATH=${CMAKE_BINARY_DIR}"
    )
endif()

