# CMakeLists.txt for the common subdirectory
# This is likely where the core 'subspace' library components will reside.

# Define the common library target.
# Example for the core subspace library:
add_library(subspace_common STATIC
    atomic_bitset.h
    channel.cc
    channel.h
    common_test.cc
    fast_ring_buffer.h
)

target_include_directories(subspace_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR} # Assumes headers are directly in 'common'
)

# Link against external dependencies if needed by common components:
# target_link_libraries(subspace_common PUBLIC
#     absl::base
#     absl::strings
#     toolbelt
#     co
#     protobuf::libprotobuf # If common code uses protobuf
# )

