# CMakeLists.txt for the c_client subdirectory
# This might contain C-language client code or a C-compatible API.

# Define targets (e.g., libraries, executables) and add source files here.
# Example for a C client library:
add_library(subspace_c_client STATIC
    subspace.cc
    subspace.h
)

add_library(subspace_c_client_shared SHARED
    subspace.cc
    subspace.h
)
set_target_properties(subspace_c_client_shared PROPERTIES
    OUTPUT_NAME subspace_c_client
)

target_include_directories(subspace_c_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(subspace_c_client_shared PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
#
# Link against other internal libraries or external dependencies if needed.
# Note: C libraries might have different linking requirements than C++ ones.
target_link_libraries(subspace_c_client PUBLIC
    subspace_proto
    subspace_client
)
target_link_libraries(subspace_c_client_shared PUBLIC
    subspace_proto
    subspace_client
)

add_executable(c_client_test client_test.cc)
add_test(NAME c_client_test COMMAND c_client_test)
add_test(NAME c_client_split_buffer_test COMMAND c_client_test --use_split_buffers)
target_link_libraries(c_client_test PUBLIC subspace_c_client libserver gtest absl::flags_parse)
