# CMakeLists.txt for the client subdirectory

# Define targets (e.g., libraries, executables) and add source files here.
# Example for a client library:
add_library(subspace_client STATIC
    client.cc
    client.h
    client_channel.cc
    client_channel.h
    message.cc
    message.h
    publisher.cc
    publisher.h
    subscriber.cc
    subscriber.h
    checksum.h
)

target_include_directories(subspace_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link against other internal libraries (e.g., common) or external dependencies if needed:
target_link_libraries(subspace_client PUBLIC
    subspace_common
    subspace_crc32
    subspace_async
    subspace_proto
    subspace_phaser
    absl::flags
    absl::flat_hash_map
    absl::flat_hash_set
    absl::status
    absl::statusor
    absl::str_format
    absl::span
    co
    protobuf::libprotobuf
)


add_executable(client_test client_test.cc)
set_target_properties(client_test PROPERTIES ENABLE_EXPORTS ON)
add_test(NAME client_test COMMAND client_test)
set_tests_properties(client_test PROPERTIES
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
)
if(APPLE)
    # Linked first so the static libraries below resolve their references.
    target_link_libraries(client_test PUBLIC
        nop_plugin_lib
        split_buffer_free_test_plugin_lib
    )
endif()
target_link_libraries(client_test PUBLIC
    subspace_client
    subspace_common
    libserver
    gmock
    absl::flags_parse
    absl::failure_signal_handler
    absl::symbolize)

add_executable(latency_test latency_test.cc)
target_link_libraries(latency_test PUBLIC
    subspace_client
    libserver
    gmock
    absl::flags_parse)
#add_test(NAME latency_test COMMAND latency_test)

add_executable(stress_test stress_test.cc)
target_link_libraries(stress_test PUBLIC
    subspace_client
    libserver
    gmock
    absl::flags_parse)
