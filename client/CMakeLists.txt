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
)

target_include_directories(subspace_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link against other internal libraries (e.g., common) or external dependencies if needed:
target_link_libraries(subspace_client PUBLIC
    subspace_common
    subspace_proto
    absl::flags
    protobuf::libprotobuf
)


add_executable(client_test client_test.cc)
add_test(NAME client_test COMMAND client_test)
target_link_libraries(client_test PUBLIC
    subspace_client
    subspace_common
    libserver
    gmock
    absl::flags_parse)

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
