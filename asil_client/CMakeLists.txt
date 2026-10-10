# CMakeLists.txt for the asil_client subdirectory

# The ASIL client core: C++17, the standard library and POSIX only.  It is
# built without exceptions or RTTI.
add_library(subspace_asil_client STATIC
    channel_memory.cc
    checksum.cc
    client.cc
    error.cc
    publisher.cc
    subscriber.cc
)
target_compile_options(subspace_asil_client PRIVATE -fno-exceptions -fno-rtti)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT ANDROID)
    target_link_libraries(subspace_asil_client PUBLIC rt)
endif()

add_executable(asil_layout_test layout_test.cc)
add_test(NAME asil_layout_test COMMAND asil_layout_test)
target_link_libraries(asil_layout_test PUBLIC
    subspace_asil_client
    subspace_client
    subspace_common
    gtest
    gtest_main
)

# The handshake with the server, using phaser messages in fixed buffers.
add_library(subspace_asil_phaser_connection STATIC
    phaser_connection.cc
)
target_compile_options(subspace_asil_phaser_connection PRIVATE -fno-exceptions -fno-rtti)
target_link_libraries(subspace_asil_phaser_connection PUBLIC
    subspace_asil_client
    subspace_phaser
)

add_executable(asil_client_test asil_client_test.cc)
add_test(NAME asil_client_test COMMAND asil_client_test)
target_link_libraries(asil_client_test PUBLIC
    subspace_asil_client
    subspace_asil_phaser_connection
    subspace_client
    libserver
    gmock
)
