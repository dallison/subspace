# CMakeLists.txt for the Python server bindings (pybind11)

# The target name must differ from the existing 'subspace_server' executable,
# so we use a _py suffix for the CMake target and override the output name.
pybind11_add_module(subspace_server_py server.cc)

set_target_properties(subspace_server_py PROPERTIES
    OUTPUT_NAME subspace_server
)

target_link_libraries(subspace_server_py PRIVATE
    libserver
    co
)
