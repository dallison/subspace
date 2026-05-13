add_library(nop_plugin MODULE
    nop_plugin.cc
)

target_include_directories(nop_plugin PRIVATE
    ${CMAKE_BINARY_DIR}
    $<TARGET_PROPERTY:co,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:toolbelt,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:subspace_common,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:subspace_client,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:subspace_proto,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:libserver,INTERFACE_INCLUDE_DIRECTORIES>
)

add_dependencies(nop_plugin subspace_proto)

target_link_libraries(nop_plugin PRIVATE
    absl::status
    absl::str_format
)

if(UNIX AND NOT APPLE)
    target_link_options(nop_plugin PRIVATE
        "LINKER:--unresolved-symbols=ignore-all"
    )
endif()

set_target_properties(nop_plugin PROPERTIES
    PREFIX ""
    OUTPUT_NAME "nop_plugin"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/plugins"
)

add_library(split_buffer_free_test_plugin MODULE
    split_buffer_free_test_plugin.cc
)

target_include_directories(split_buffer_free_test_plugin PRIVATE
    ${CMAKE_BINARY_DIR}
    $<TARGET_PROPERTY:co,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:toolbelt,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:subspace_common,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:subspace_proto,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:libserver,INTERFACE_INCLUDE_DIRECTORIES>
)

add_dependencies(split_buffer_free_test_plugin subspace_proto)

target_link_libraries(split_buffer_free_test_plugin PRIVATE
    absl::status
)

if(UNIX AND NOT APPLE)
    target_link_options(split_buffer_free_test_plugin PRIVATE
        "LINKER:--unresolved-symbols=ignore-all"
    )
endif()

set_target_properties(split_buffer_free_test_plugin PROPERTIES
    PREFIX ""
    OUTPUT_NAME "split_buffer_free_test_plugin"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/plugins"
)
