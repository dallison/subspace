# CMakeLists.txt for the Python client bindings (pybind11)

pybind11_add_module(subspace client.cc)

target_link_libraries(subspace PRIVATE
    subspace_client
)
