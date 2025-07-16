# CMakeLists.txt for the c_client subdirectory
# This might contain C-language client code or a C-compatible API.

# Define targets (e.g., libraries, executables) and add source files here.
# Example for a C client library:
add_library(subspace_c_client STATIC
    subspace.cc
    subspace.h
)

target_include_directories(subspace_c_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
#
# Link against other internal libraries or external dependencies if needed.
# Note: C libraries might have different linking requirements than C++ ones.
target_link_libraries(subspace_c_client PUBLIC
    subspace_client
)

