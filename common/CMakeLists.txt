# CMakeLists.txt for the common subdirectory
# This is likely where the core 'subspace' library components will reside.

# Define the common library target.
# Example for the core subspace library:
add_library(subspace_common STATIC
    atomic_bitset.h
    channel.cc
    channel.h
    fast_ring_buffer.h
    split_buffer.cc
    split_buffer.h
    system_info.cc
    system_info.h
    syscall_shim.cc
    syscall_shim.h
    syscall_shim_test_helper.h
)

target_link_libraries(subspace_common PUBLIC
    absl::base
    absl::flags
    absl::flat_hash_set
    absl::strings
    absl::str_format
    toolbelt
    co
    subspace_proto
    protobuf::libprotobuf
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT ANDROID)
    target_link_libraries(subspace_common PUBLIC rt)
elseif(ANDROID)
    target_link_libraries(subspace_common PUBLIC log)
endif()

add_executable(common_test common_test.cc)
add_test(NAME common_test COMMAND common_test)
target_link_libraries(common_test PUBLIC subspace_common gtest gtest_main)

add_executable(split_buffer_test split_buffer_test.cc)
add_test(NAME split_buffer_test COMMAND split_buffer_test)
target_link_libraries(split_buffer_test PUBLIC subspace_common gtest gtest_main)

