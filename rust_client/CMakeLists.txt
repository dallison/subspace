# CMakeLists.txt for the Rust client subdirectory
#
# The Rust client is built via Cargo.  CMake invokes `cargo build` as a custom
# target so that the library is compiled as part of the overall project.  The
# proto file at ../proto/subspace.proto is picked up automatically by the Rust
# build script (build.rs).

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
    message(WARNING "cargo not found -- skipping rust_client build.  "
                    "Install Rust (https://rustup.rs) to enable the Rust client.")
    return()
endif()

if(CMAKE_CROSSCOMPILING)
    message(STATUS "Cross-compiling -- skipping rust_client (no cross-Cargo support)")
    return()
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CARGO_BUILD_FLAG "--release")
    set(CARGO_TARGET_DIR "release")
else()
    set(CARGO_BUILD_FLAG "")
    set(CARGO_TARGET_DIR "debug")
endif()

add_custom_target(subspace_client_rust ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAG}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Rust client library via Cargo"
)

# The Rust client opens named /dev/shm buffers and cannot consume buffers
# created by a server using the anonymous memfd backend. This mirrors the Bazel
# memfd job, which excludes the Rust tests for the same reason.
if(SUBSPACE_LINUX_USE_MEMFD AND CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT ANDROID)
    message(STATUS "Skipping Rust client tests with the memfd server backend")
else()
    # These tests use the subprocess server fallback since there is no FFI
    # linkage in the Cargo build. Build the server first so the binary is
    # available.
    add_test(
        NAME rust_client_test
        COMMAND ${CARGO_EXECUTABLE} test -- --test-threads=1
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    set_tests_properties(rust_client_test PROPERTIES
        ENVIRONMENT "PATH=$<TARGET_FILE_DIR:subspace_server>:$ENV{PATH}"
    )
endif()
