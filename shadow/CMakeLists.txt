# CMakeLists.txt for the shadow subdirectory

add_library(shadow_lib STATIC
    shadow.cc
    shadow.h
)

target_include_directories(shadow_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(shadow_lib PUBLIC
    subspace_proto
    absl::flat_hash_map
    absl::status
    absl::statusor
    absl::str_format
    co
    toolbelt
    protobuf::libprotobuf
)

add_executable(subspace_shadow
    main.cc
)

target_link_libraries(subspace_shadow PUBLIC
    shadow_lib
    absl::flags_parse
)

if(NOT APPLE)
    target_link_libraries(subspace_shadow PRIVATE rt)
endif()

add_executable(shadow_test shadow_test.cc)
add_test(NAME shadow_test COMMAND shadow_test)
target_link_libraries(shadow_test PUBLIC
    shadow_lib
    subspace_client
    libserver
    gtest
    gtest_main
    absl::status
    absl::statusor
    absl::status_matchers
)

if(NOT APPLE)
    target_link_libraries(shadow_test PRIVATE rt dl)
endif()
