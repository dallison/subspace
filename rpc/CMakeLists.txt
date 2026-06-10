# CMakeLists.txt for the rpc subdirectory
#
# This mirrors the Bazel build under //rpc.  It:
#   1. Builds the subspace_rpc protoc plugin (the IDL compiler).
#   2. Generates the C++ protobuf code and the subspace RPC client/server stubs
#      from rpc/proto/rpc_test.proto.
#   3. Defines the rpc_common / rpc_server / rpc_client libraries.
#   4. Builds the generated-stub libraries, the tests and the example binaries.
#
# Generated files are emitted under ${CMAKE_BINARY_DIR}/rpc/proto so that the
# same include paths used by the source (e.g. "rpc/proto/rpc_test.pb.h" and
# "rpc/proto/rpc_test.subspace.rpc_client.h") resolve via the project-wide
# include directories added in the top-level CMakeLists.txt.

cmake_minimum_required(VERSION 3.15)

# Protobuf is fetched by the parent CMakeLists.txt via FetchContent.  We need
# its source directory for the well-known type .proto files.
include(FetchContent)
FetchContent_GetProperties(protobuf)
if(NOT protobuf_POPULATED)
    message(FATAL_ERROR "protobuf must be populated before rpc/CMakeLists.txt is processed")
endif()

# --- protoc selection (native vs cross-compile), matching proto/CMakeLists.txt ---
if(CMAKE_CROSSCOMPILING)
    if(NOT PROTOC_EXECUTABLE)
        find_program(PROTOC_EXECUTABLE protoc REQUIRED)
    endif()
    set(RPC_PROTOC ${PROTOC_EXECUTABLE})
    set(RPC_PROTOC_DEP "")
else()
    set(RPC_PROTOC $<TARGET_FILE:protobuf::protoc>)
    set(RPC_PROTOC_DEP protobuf::protoc)
endif()

# --- The subspace_rpc protoc plugin (IDL compiler) ---
# This is a host tool: it runs during the build to generate the RPC stubs.
add_executable(subspace_rpc_plugin
    idl_compiler/main.cc
    idl_compiler/gen.cc
    idl_compiler/service_gen.cc
)
target_link_libraries(subspace_rpc_plugin PRIVATE
    protobuf::libprotoc
    protobuf::libprotobuf
    absl::flat_hash_map
    absl::flat_hash_set
    absl::status
    absl::statusor
    absl::strings
    absl::str_format
    absl::flags
    absl::flags_parse
)
# The fetched protobuf libprotoc target does not always export its source
# headers (e.g. google/protobuf/compiler/code_generator.h), so add them.
target_include_directories(subspace_rpc_plugin PRIVATE ${protobuf_SOURCE_DIR}/src)

# When cross-compiling we cannot run a target-architecture plugin on the build
# host, so require a prebuilt host plugin to be supplied (mirrors how a host
# protoc is required for cross builds).
if(CMAKE_CROSSCOMPILING)
    if(NOT SUBSPACE_RPC_PLUGIN)
        message(FATAL_ERROR
            "When cross-compiling, set -DSUBSPACE_RPC_PLUGIN=/path/to/host/subspace_rpc_plugin "
            "(a host-native build of //rpc/idl_compiler:subspace_rpc).")
    endif()
    set(RPC_PLUGIN ${SUBSPACE_RPC_PLUGIN})
    set(RPC_PLUGIN_DEP "")
else()
    set(RPC_PLUGIN $<TARGET_FILE:subspace_rpc_plugin>)
    set(RPC_PLUGIN_DEP subspace_rpc_plugin)
endif()

# --- Generated file locations ---
set(RPC_PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_test.proto)

set(RPC_PB_SRC ${CMAKE_BINARY_DIR}/rpc/proto/rpc_test.pb.cc)
set(RPC_PB_HDR ${CMAKE_BINARY_DIR}/rpc/proto/rpc_test.pb.h)

set(RPC_CLIENT_SRC ${CMAKE_BINARY_DIR}/rpc/proto/rpc_test.subspace.rpc_client.cc)
set(RPC_CLIENT_HDR ${CMAKE_BINARY_DIR}/rpc/proto/rpc_test.subspace.rpc_client.h)
set(RPC_SERVER_SRC ${CMAKE_BINARY_DIR}/rpc/proto/rpc_test.subspace.rpc_server.cc)
set(RPC_SERVER_HDR ${CMAKE_BINARY_DIR}/rpc/proto/rpc_test.subspace.rpc_server.h)

# Use the repository root as the proto path so the generated file names keep
# their "rpc/proto/" prefix, matching the #include paths used in the sources.
add_custom_command(
    OUTPUT ${RPC_PB_SRC} ${RPC_PB_HDR}
    COMMAND ${RPC_PROTOC}
    ARGS --cpp_out=${CMAKE_BINARY_DIR}
         --proto_path=${CMAKE_SOURCE_DIR}
         --proto_path=${protobuf_SOURCE_DIR}/src
         ${RPC_PROTO_FILE}
    DEPENDS ${RPC_PROTO_FILE} ${RPC_PROTOC_DEP}
    COMMENT "Generating C++ protobuf code from rpc/proto/rpc_test.proto"
)

# Run protoc with the subspace_rpc plugin to generate the client/server stubs.
# No plugin options are passed, so the default (empty) package/target names make
# the plugin emit files at <out_dir>/rpc/proto/rpc_test.subspace.rpc_*.{cc,h}.
add_custom_command(
    OUTPUT ${RPC_CLIENT_SRC} ${RPC_CLIENT_HDR} ${RPC_SERVER_SRC} ${RPC_SERVER_HDR}
    COMMAND ${RPC_PROTOC}
    ARGS --plugin=protoc-gen-subspace_rpc=${RPC_PLUGIN}
         --subspace_rpc_out=${CMAKE_BINARY_DIR}
         --proto_path=${CMAKE_SOURCE_DIR}
         --proto_path=${protobuf_SOURCE_DIR}/src
         ${RPC_PROTO_FILE}
    DEPENDS ${RPC_PROTO_FILE} ${RPC_PROTOC_DEP} ${RPC_PLUGIN_DEP}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Generating subspace RPC stubs from rpc/proto/rpc_test.proto"
)

# --- Libraries ---

# rpc_common is header-only.
add_library(rpc_common INTERFACE)
target_link_libraries(rpc_common INTERFACE subspace_client)

add_library(rpc_server STATIC
    server/rpc_server.cc
    server/rpc_server.h
)
target_link_libraries(rpc_server PUBLIC
    subspace_common
    subspace_client
    subspace_proto
    rpc_common
    absl::flat_hash_map
    absl::flat_hash_set
    absl::status
    absl::statusor
    absl::str_format
    absl::flags
    absl::flags_parse
    co
    protobuf::libprotobuf
)

add_library(rpc_client STATIC
    client/rpc_client.cc
    client/rpc_client.h
)
target_link_libraries(rpc_client PUBLIC
    rpc_server
    subspace_common
    subspace_client
    subspace_proto
    rpc_common
    absl::flat_hash_map
    absl::flat_hash_set
    absl::status
    absl::statusor
    absl::str_format
    absl::flags
    absl::flags_parse
    co
    protobuf::libprotobuf
)

# Generated protobuf messages for the test/example service.
add_library(rpc_test_cc_proto STATIC ${RPC_PB_SRC} ${RPC_PB_HDR})
target_include_directories(rpc_test_cc_proto PUBLIC ${CMAKE_BINARY_DIR})
target_link_libraries(rpc_test_cc_proto PUBLIC protobuf::libprotobuf)

# Generated client and server stubs.
add_library(rpc_test_subspace_rpc_client STATIC ${RPC_CLIENT_SRC} ${RPC_CLIENT_HDR})
target_include_directories(rpc_test_subspace_rpc_client PUBLIC ${CMAKE_BINARY_DIR})
target_link_libraries(rpc_test_subspace_rpc_client PUBLIC rpc_test_cc_proto rpc_client)

add_library(rpc_test_subspace_rpc_server STATIC ${RPC_SERVER_SRC} ${RPC_SERVER_HDR})
target_include_directories(rpc_test_subspace_rpc_server PUBLIC ${CMAKE_BINARY_DIR})
target_link_libraries(rpc_test_subspace_rpc_server PUBLIC rpc_test_cc_proto rpc_server)

# --- Tests ---
# These start the subspace server in-process (linking libserver), matching the
# other CMake tests in this project.

add_executable(rpc_client_test client/client_test.cc)
add_test(NAME rpc_client_test COMMAND rpc_client_test)
set_tests_properties(rpc_client_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(rpc_client_test PUBLIC
    rpc_client
    rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

add_executable(rpc_server_test server/server_test.cc)
add_test(NAME rpc_server_test COMMAND rpc_server_test)
set_tests_properties(rpc_server_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(rpc_server_test PUBLIC
    rpc_server
    rpc_test_cc_proto
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

add_executable(rpc_test test/rpc_test.cc)
add_test(NAME rpc_test COMMAND rpc_test)
set_tests_properties(rpc_test PROPERTIES WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
target_link_libraries(rpc_test PUBLIC
    rpc_test_subspace_rpc_client
    rpc_test_subspace_rpc_server
    subspace_client
    libserver
    gtest
    gmock
    absl::flags_parse
)

# --- Example binaries (built but not registered as tests, matching the
# "manual" tag in Bazel) ---
add_executable(rpc_example_server example/server.cc)
target_link_libraries(rpc_example_server PUBLIC
    rpc_test_subspace_rpc_server
    rpc_test_cc_proto
    absl::flags_parse
)

add_executable(rpc_example_client example/client.cc)
target_link_libraries(rpc_example_client PUBLIC
    rpc_test_subspace_rpc_client
    rpc_test_cc_proto
    absl::flags_parse
)
