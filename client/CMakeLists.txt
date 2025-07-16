# CMakeLists.txt for the client subdirectory

# Define targets (e.g., libraries, executables) and add source files here.
# Example for a client library:
add_library(subspace_client STATIC
    client.cc
    client.h
)

target_include_directories(subspace_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link against other internal libraries (e.g., common) or external dependencies if needed:
target_link_libraries(subspace_client PUBLIC
    subspace_common # If common defines a library
    protobuf::libprotobuf
)

# Example for a client executable:
# add_executable(client_app
#     client/main.cc
# )
# target_link_libraries(client_app PRIVATE
#     subspace_client # Link against your client library
#     # ... other dependencies
# )

