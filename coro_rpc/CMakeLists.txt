# CMakeLists.txt for the coro_rpc subdirectory
#
# This mirrors the Bazel build under //coro_rpc.  It builds the C++20
# boost::asio::awaitable RPC runtime libraries, generates the coro-flavored RPC
# stubs from rpc/proto/rpc_test.proto using the subspace_rpc plugin defined in
# rpc/CMakeLists.txt, and builds the tests and example binaries.
#
# This file is only added from the top-level CMakeLists.txt when
# SUBSPACE_ASIO_RPC is enabled, which is also where Boost is fetched (coro_rpc
# uses C++20 coroutines on top of Boost.Asio).  It is processed after
# add_subdirectory(rpc) so the subspace_rpc_plugin host tool is already defined.
# Unlike asio_rpc (stackful yield_context), coro_rpc only needs Boost::asio --
# not Boost.Coroutine/Context -- but every target that includes the coro headers
# must be compiled as C++20.

cmake_minimum_required(VERSION 3.15)

if(NOT TARGET subspace_rpc_plugin)
    message(FATAL_ERROR
        "coro_rpc requires the subspace_rpc_plugin target from rpc/; ensure "
        "add_subdirectory(coro_rpc) follows add_subdirectory(rpc).")
endif()

include(FetchContent)
FetchContent_GetProperties(protobuf)

# Where the coro-flavored stubs are generated.  Its layout reproduces the Bazel
# include path "coro_rpc/proto/rpc_test_coro_rpc/rpc/proto/rpc_test.subspace.*".
# The plugin keeps the proto's "rpc/proto/" prefix, so this root must hold the
# files exactly where the sources #include them.
set(CORO_RPC_GEN_ROOT ${CMAKE_BINARY_DIR}/coro_rpc/proto/rpc_test_coro_rpc)

# The rpc/ build also emits a "rpc/proto/rpc_test.subspace.rpc_*.h" under
# ${CMAKE_BINARY_DIR} (the co::Coroutine flavor), which the top-level
# include_directories(${CMAKE_BINARY_DIR}) puts on the search path ahead of any
# target-level include directory.  Prepend the coro gen root for this whole
# subdirectory so the coro-flavored headers win over the co/ ones for the
# shared "rpc/proto/..." include path.
include_directories(BEFORE ${CORO_RPC_GEN_ROOT})

# --- Runtime libraries ---

# common/async_wait: Asio FD-readiness helpers (C++20 awaitable).
add_library(coro_async_wait STATIC
    common/async_wait.cc
    common/async_wait.h
)
target_link_libraries(coro_async_wait PUBLIC
    absl::status
    absl::statusor
    absl::str_format
    Boost::asio
)

# common/shared_ptr_pipe: header-only SharedPtrPipe over boost::asio::awaitable,
# built on the backend-agnostic rpc_common shared base.
add_library(coro_shared_ptr_pipe INTERFACE)
target_link_libraries(coro_shared_ptr_pipe INTERFACE
    coro_async_wait
    rpc_common
    absl::status
    absl::statusor
    Boost::asio
)

add_library(coro_rpc_server STATIC
    server/rpc_server.cc
    server/rpc_server.h
)
target_link_libraries(coro_rpc_server PUBLIC
    coro_async_wait
    coro_shared_ptr_pipe
    subspace_common
    subspace_client
    subspace_proto
    rpc_common
    absl::flat_hash_map
    absl::flat_hash_set
    absl::status
    absl::statusor
    absl::str_format
    Boost::asio
    protobuf::libprotobuf
)

add_library(coro_rpc_client STATIC
    client/rpc_client.cc
    client/rpc_client.h
)
target_link_libraries(coro_rpc_client PUBLIC
    coro_rpc_server
    coro_async_wait
    subspace_common
    subspace_client
    subspace_proto
    rpc_common
    absl::flat_hash_map
    absl::flat_hash_set
    absl::status
    absl::statusor
    absl::str_format
    Boost::asio
    protobuf::libprotobuf
)

# --- Generated coro RPC stubs ---
set(CORO_RPC_PROTO_FILE ${CMAKE_SOURCE_DIR}/rpc/proto/rpc_test.proto)

set(CORO_RPC_CLIENT_SRC ${CORO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_client.cc)
set(CORO_RPC_CLIENT_HDR ${CORO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_client.h)
set(CORO_RPC_SERVER_SRC ${CORO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_server.cc)
set(CORO_RPC_SERVER_HDR ${CORO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_server.h)

add_custom_command(
    OUTPUT ${CORO_RPC_CLIENT_SRC} ${CORO_RPC_CLIENT_HDR}
           ${CORO_RPC_SERVER_SRC} ${CORO_RPC_SERVER_HDR}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CORO_RPC_GEN_ROOT}
    COMMAND $<TARGET_FILE:protobuf::protoc>
    ARGS --plugin=protoc-gen-subspace_rpc=$<TARGET_FILE:subspace_rpc_plugin>
         --subspace_rpc_out=rpc_style=coro:${CORO_RPC_GEN_ROOT}
         --proto_path=${CMAKE_SOURCE_DIR}
         --proto_path=${protobuf_SOURCE_DIR}/src
         ${CORO_RPC_PROTO_FILE}
    DEPENDS ${CORO_RPC_PROTO_FILE} protobuf::protoc subspace_rpc_plugin
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Generating coro subspace RPC stubs from rpc/proto/rpc_test.proto"
)

# The generated sources #include their own headers via the short "rpc/proto/"
# prefix, so CORO_RPC_GEN_ROOT must be on the include path; the test sources use
# the full "coro_rpc/proto/rpc_test_coro_rpc/..." path resolved by CMAKE_BINARY_DIR.
add_library(coro_rpc_test_subspace_rpc_client STATIC
    ${CORO_RPC_CLIENT_SRC} ${CORO_RPC_CLIENT_HDR})
target_include_directories(coro_rpc_test_subspace_rpc_client PUBLIC
    ${CMAKE_BINARY_DIR} ${CORO_RPC_GEN_ROOT})
target_link_libraries(coro_rpc_test_subspace_rpc_client PUBLIC
    rpc_test_cc_proto coro_rpc_client)

add_library(coro_rpc_test_subspace_rpc_server STATIC
    ${CORO_RPC_SERVER_SRC} ${CORO_RPC_SERVER_HDR})
target_include_directories(coro_rpc_test_subspace_rpc_server PUBLIC
    ${CMAKE_BINARY_DIR} ${CORO_RPC_GEN_ROOT})
target_link_libraries(coro_rpc_test_subspace_rpc_server PUBLIC
    rpc_test_cc_proto coro_rpc_server)

# --- Tests ---
add_executable(coro_rpc_server_test server/server_test.cc)
add_test(NAME coro_rpc_server_test COMMAND coro_rpc_server_test)
set_tests_properties(coro_rpc_server_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(coro_rpc_server_test PUBLIC
    coro_rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

add_executable(coro_rpc_client_test client/client_test.cc)
add_test(NAME coro_rpc_client_test COMMAND coro_rpc_client_test)
set_tests_properties(coro_rpc_client_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(coro_rpc_client_test PUBLIC
    coro_rpc_client
    coro_rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

add_executable(coro_rpc_test test/rpc_test.cc)
add_test(NAME coro_rpc_test COMMAND coro_rpc_test)
set_tests_properties(coro_rpc_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(coro_rpc_test PUBLIC
    coro_rpc_test_subspace_rpc_client
    coro_rpc_test_subspace_rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

# --- Example binaries (built but not registered as tests, matching the
# "manual" tag in Bazel) ---
add_executable(coro_rpc_example_server example/server.cc)
target_link_libraries(coro_rpc_example_server PUBLIC
    coro_rpc_test_subspace_rpc_server
    rpc_test_cc_proto
    absl::flags
    absl::flags_parse
)

add_executable(coro_rpc_example_client example/client.cc)
target_link_libraries(coro_rpc_example_client PUBLIC
    coro_rpc_test_subspace_rpc_client
    rpc_test_cc_proto
    absl::flags
    absl::flags_parse
)

# boost::asio::awaitable / co_await require C++20, so every target that includes
# the coro headers must be compiled as C++20.
set_target_properties(
    coro_async_wait
    coro_rpc_server
    coro_rpc_client
    coro_rpc_test_subspace_rpc_client
    coro_rpc_test_subspace_rpc_server
    coro_rpc_server_test
    coro_rpc_client_test
    coro_rpc_test
    coro_rpc_example_server
    coro_rpc_example_client
    PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON
)
