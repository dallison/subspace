add_library(nop_plugin SHARED
    nop_plugin.cc
)

set_target_properties(nop_plugin PROPERTIES
    PREFIX ""
)

add_dependencies(nop_plugin subspace_proto)

target_include_directories(nop_plugin PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${abseil_SOURCE_DIR}
    ${co_SOURCE_DIR}
    ${cpp_toolbelt_SOURCE_DIR}
    ${protobuf_SOURCE_DIR}/src
)
