# CMakeLists.txt for the asio_rpc subdirectory
#
# This mirrors the Bazel build under //asio_rpc.  It builds the Boost.Asio
# (stackful yield_context) RPC runtime libraries, generates the asio-flavored
# RPC stubs from rpc/proto/rpc_test.proto using the subspace_rpc plugin defined
# in rpc/CMakeLists.txt, and builds the tests and example binaries.
#
# This file is only added from the top-level CMakeLists.txt when
# SUBSPACE_ASIO_RPC is enabled, which is also where Boost is fetched.  It is
# processed after add_subdirectory(rpc) so the subspace_rpc_plugin host tool is
# already defined.

cmake_minimum_required(VERSION 3.15)

if(NOT TARGET subspace_rpc_plugin)
    message(FATAL_ERROR
        "asio_rpc requires the subspace_rpc_plugin target from rpc/; ensure "
        "add_subdirectory(asio_rpc) follows add_subdirectory(rpc).")
endif()

include(FetchContent)
FetchContent_GetProperties(protobuf)

# Where the asio-flavored stubs are generated.  Its layout reproduces the Bazel
# include path "asio_rpc/proto/rpc_test_asio_rpc/rpc/proto/rpc_test.subspace.*".
# The plugin keeps the proto's "rpc/proto/" prefix, so this root must hold the
# files exactly where the sources #include them.
set(ASIO_RPC_GEN_ROOT ${CMAKE_BINARY_DIR}/asio_rpc/proto/rpc_test_asio_rpc)

# The rpc/ build also emits a "rpc/proto/rpc_test.subspace.rpc_*.h" under
# ${CMAKE_BINARY_DIR} (the co::Coroutine flavor), which the top-level
# include_directories(${CMAKE_BINARY_DIR}) puts on the search path ahead of any
# target-level include directory.  Prepend the asio gen root for this whole
# subdirectory so the asio-flavored headers win over the co/ ones for the
# shared "rpc/proto/..." include path.
include_directories(BEFORE ${ASIO_RPC_GEN_ROOT})

# --- Runtime libraries ---

# common/async_wait: Asio FD-readiness helpers.
add_library(asio_async_wait STATIC
    common/async_wait.cc
    common/async_wait.h
)
target_link_libraries(asio_async_wait PUBLIC
    absl::status
    absl::statusor
    absl::str_format
    Boost::asio
)

# common/shared_ptr_pipe: header-only SharedPtrPipe over yield_context, built on
# the backend-agnostic rpc_common shared base.
add_library(asio_shared_ptr_pipe INTERFACE)
target_link_libraries(asio_shared_ptr_pipe INTERFACE
    rpc_common
    absl::status
    absl::statusor
    Boost::asio
)

add_library(asio_rpc_server STATIC
    server/rpc_server.cc
    server/rpc_server.h
)
target_link_libraries(asio_rpc_server PUBLIC
    asio_async_wait
    asio_shared_ptr_pipe
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
    Boost::coroutine
    Boost::context
    protobuf::libprotobuf
)

add_library(asio_rpc_client STATIC
    client/rpc_client.cc
    client/rpc_client.h
)
target_link_libraries(asio_rpc_client PUBLIC
    asio_rpc_server
    asio_async_wait
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
    Boost::coroutine
    Boost::context
    protobuf::libprotobuf
)

# --- protoc selection (native vs cross-compile) ---
# asio_rpc is native-only (Boost.Context does not cross-compile), so this always
# resolves to the in-tree protoc/plugin, but keep parity with rpc/.
set(ASIO_RPC_PROTOC $<TARGET_FILE:protobuf::protoc>)
set(ASIO_RPC_PLUGIN $<TARGET_FILE:subspace_rpc_plugin>)

# --- Generated asio RPC stubs ---
set(ASIO_RPC_PROTO_FILE ${CMAKE_SOURCE_DIR}/rpc/proto/rpc_test.proto)

set(ASIO_RPC_CLIENT_SRC ${ASIO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_client.cc)
set(ASIO_RPC_CLIENT_HDR ${ASIO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_client.h)
set(ASIO_RPC_SERVER_SRC ${ASIO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_server.cc)
set(ASIO_RPC_SERVER_HDR ${ASIO_RPC_GEN_ROOT}/rpc/proto/rpc_test.subspace.rpc_server.h)

add_custom_command(
    OUTPUT ${ASIO_RPC_CLIENT_SRC} ${ASIO_RPC_CLIENT_HDR}
           ${ASIO_RPC_SERVER_SRC} ${ASIO_RPC_SERVER_HDR}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${ASIO_RPC_GEN_ROOT}
    COMMAND ${ASIO_RPC_PROTOC}
    ARGS --plugin=protoc-gen-subspace_rpc=${ASIO_RPC_PLUGIN}
         --subspace_rpc_out=rpc_style=asio:${ASIO_RPC_GEN_ROOT}
         --proto_path=${CMAKE_SOURCE_DIR}
         --proto_path=${protobuf_SOURCE_DIR}/src
         ${ASIO_RPC_PROTO_FILE}
    DEPENDS ${ASIO_RPC_PROTO_FILE} protobuf::protoc subspace_rpc_plugin
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Generating asio subspace RPC stubs from rpc/proto/rpc_test.proto"
)

# The generated sources #include their own headers via the short "rpc/proto/"
# prefix, so ASIO_RPC_GEN_ROOT must be on the include path; the test sources use
# the full "asio_rpc/proto/rpc_test_asio_rpc/..." path resolved by CMAKE_BINARY_DIR.
add_library(asio_rpc_test_subspace_rpc_client STATIC
    ${ASIO_RPC_CLIENT_SRC} ${ASIO_RPC_CLIENT_HDR})
target_include_directories(asio_rpc_test_subspace_rpc_client PUBLIC
    ${CMAKE_BINARY_DIR} ${ASIO_RPC_GEN_ROOT})
target_link_libraries(asio_rpc_test_subspace_rpc_client PUBLIC
    rpc_test_cc_proto asio_rpc_client)

add_library(asio_rpc_test_subspace_rpc_server STATIC
    ${ASIO_RPC_SERVER_SRC} ${ASIO_RPC_SERVER_HDR})
target_include_directories(asio_rpc_test_subspace_rpc_server PUBLIC
    ${CMAKE_BINARY_DIR} ${ASIO_RPC_GEN_ROOT})
target_link_libraries(asio_rpc_test_subspace_rpc_server PUBLIC
    rpc_test_cc_proto asio_rpc_server)

# --- Tests ---
add_executable(asio_rpc_server_test server/server_test.cc)
add_test(NAME asio_rpc_server_test COMMAND asio_rpc_server_test)
set_tests_properties(asio_rpc_server_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(asio_rpc_server_test PUBLIC
    asio_rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

add_executable(asio_rpc_client_test client/client_test.cc)
add_test(NAME asio_rpc_client_test COMMAND asio_rpc_client_test)
set_tests_properties(asio_rpc_client_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(asio_rpc_client_test PUBLIC
    asio_rpc_client
    asio_rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

add_executable(asio_rpc_test test/rpc_test.cc)
add_test(NAME asio_rpc_test COMMAND asio_rpc_test)
set_tests_properties(asio_rpc_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(asio_rpc_test PUBLIC
    asio_rpc_test_subspace_rpc_client
    asio_rpc_test_subspace_rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

# --- Example binaries (built but not registered as tests, matching the
# "manual" tag in Bazel) ---
add_executable(asio_rpc_example_server example/server.cc)
target_link_libraries(asio_rpc_example_server PUBLIC
    asio_rpc_test_subspace_rpc_server
    rpc_test_cc_proto
    absl::flags
    absl::flags_parse
)

add_executable(asio_rpc_example_client example/client.cc)
target_link_libraries(asio_rpc_example_client PUBLIC
    asio_rpc_test_subspace_rpc_client
    rpc_test_cc_proto
    absl::flags
    absl::flags_parse
)
