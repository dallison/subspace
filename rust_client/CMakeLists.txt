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

# The client sends the server native Phaser payloads, built through phaser's C
# frontend.  The phaser_c crate includes the generated Rust bindings.  Cargo
# cannot follow CMake's static libraries, so their C++ code and everything it
# uses are linked into one shared library.
phaser_add_library(subspace_phaser_c
    PROTOS proto/subspace.proto
    IMPORTS google/protobuf/any.proto
    IMPORT_DIRS ${CMAKE_SOURCE_DIR} ${protobuf_SOURCE_DIR}/src
    ADD_NAMESPACE phaser_c
    RUST
)
if(NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/subspace_phaser_c_rust.cc")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/subspace_phaser_c_rust.cc" "")
endif()
add_library(subspace_phaser_c_rust SHARED
    "${CMAKE_CURRENT_BINARY_DIR}/subspace_phaser_c_rust.cc"
)
# Nothing in the shared library calls the C API, so the linker would drop it.
if(APPLE)
    set(SUBSPACE_PHASER_C_WHOLE
        -Wl,-force_load,$<TARGET_FILE:subspace_phaser_c>
        -Wl,-force_load,$<TARGET_FILE:phaser_c>)
else()
    set(SUBSPACE_PHASER_C_WHOLE
        -Wl,--whole-archive
        $<TARGET_FILE:subspace_phaser_c>
        $<TARGET_FILE:phaser_c>
        -Wl,--no-whole-archive)
endif()
target_link_libraries(subspace_phaser_c_rust PRIVATE
    ${SUBSPACE_PHASER_C_WHOLE}
    subspace_phaser_c
)

set(SUBSPACE_CARGO_ENV
    SUBSPACE_MAX_CHANNELS=${SUBSPACE_MAX_CHANNELS}
    SUBSPACE_MAX_SLOT_OWNERS=${SUBSPACE_MAX_SLOT_OWNERS}
    SUBSPACE_MAX_BUFFERS=${SUBSPACE_MAX_BUFFERS}
    SUBSPACE_PHASER_C_RS=$<TARGET_PROPERTY:subspace_phaser_c,PHASER_RUST_SOURCE>
    SUBSPACE_PHASER_C_LIB_DIR=$<TARGET_FILE_DIR:subspace_phaser_c_rust>
)

add_custom_target(subspace_client_rust ALL
    COMMAND ${CMAKE_COMMAND} -E env ${SUBSPACE_CARGO_ENV}
            ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAG}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Rust client library via Cargo"
)
add_dependencies(subspace_client_rust subspace_phaser_c_rust)

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
        ENVIRONMENT "PATH=$<TARGET_FILE_DIR:subspace_server>:$ENV{PATH};${SUBSPACE_CARGO_ENV}"
    )
endif()
