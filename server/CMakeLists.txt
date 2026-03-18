# CMakeLists.txt for the server subdirectory

add_library(libserver STATIC
    client_handler.cc
    client_handler.h
    server_channel.cc
    server_channel.h
    shadow_replicator.cc
    shadow_replicator.h
    server.cc
    server.h
    plugin.h
)

target_include_directories(libserver PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(libserver PUBLIC
    subspace_common
    subspace_client
    protobuf::libprotobuf
)

add_executable(subspace_server
    main.cc
)

target_link_libraries(subspace_server PUBLIC
    libserver
    absl::flags_parse
)

if(NOT APPLE)
    target_link_libraries(subspace_server PRIVATE rt dl)
endif()

