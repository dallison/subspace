# CMakeLists.txt for the common subdirectory
# This is likely where the core 'subspace' library components will reside.

# Define the common library target.
# Example for the core subspace library:
add_library(subspace_common STATIC
    atomic_bitset.h
    channel.cc
    channel.h
    fast_ring_buffer.h
)

target_link_libraries(subspace_common PUBLIC
    absl::base
    absl::flags
    absl::flat_hash_set
    absl::strings
    toolbelt
    co
    protobuf::libprotobuf # If common code uses protobuf
)

add_executable(common_test common_test.cc)
add_test(NAME common_test COMMAND common_test)
target_link_libraries(common_test PUBLIC subspace_common gtest gtest_main)

