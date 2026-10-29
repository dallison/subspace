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
    tunnel.cc
    tunnel.h
    tunnel_config.cc
    tunnel_config.h
    tunnel_protocol.cc
    tunnel_protocol.h
)

target_include_directories(libserver PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(libserver PUBLIC
    subspace_common
    subspace_client
    subspace_async
    protobuf::libprotobuf
)

add_executable(tunnel_wire_test tunnel_config_test.cc tunnel_protocol_test.cc)
add_executable(tunnel_test tunnel_test.cc)
add_executable(tunnel_fake_peer_test tunnel_fake_peer_test.cc)
foreach(TUNNEL_TEST tunnel_wire_test tunnel_test tunnel_fake_peer_test)
    add_test(NAME ${TUNNEL_TEST} COMMAND ${TUNNEL_TEST})
    target_link_libraries(${TUNNEL_TEST} PUBLIC
        libserver
        absl::status_matchers
        gmock
        gtest_main
    )
endforeach()

add_executable(subspace_server
    main.cc
)
set_target_properties(subspace_server PROPERTIES ENABLE_EXPORTS ON)

target_link_libraries(subspace_server PUBLIC
    libserver
    absl::flags_parse
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT ANDROID)
    target_link_libraries(subspace_server PRIVATE rt dl)
elseif(ANDROID)
    target_link_libraries(subspace_server PRIVATE dl log)
endif()

