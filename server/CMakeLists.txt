# CMakeLists.txt for the server subdirectory

# Define targets (e.g., libraries, executables) and add source files here.
add_library(libserver STATIC
    client_handler.cc
    client_handler.h
    server_channel.cc
    server_channel.h
    server.cc
    server.h
)

target_include_directories(libserver PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link against other internal libraries (e.g., common) or external dependencies if needed:
target_link_libraries(libserver PUBLIC
    subspace_common
    subspace_client
    protobuf::libprotobuf
)

# Example for a server executable:
add_executable(subspace_server
    main.cc
)

target_link_libraries(subspace_server PUBLIC
    libserver
    absl::flags_parse
)

