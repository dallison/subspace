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

add_dependencies(nop_plugin subspace_proto subspace_phaser)

if(ANDROID)
    # Android's lld requires all symbols resolved at link time for .so files
    target_link_libraries(nop_plugin PRIVATE
        libserver
        subspace_client
        subspace_common
        subspace_proto
        co
        toolbelt
        absl::status
        absl::str_format
        protobuf::libprotobuf
        log
    )
elseif(UNIX AND NOT APPLE)
    target_link_libraries(nop_plugin PRIVATE
        absl::status
        absl::str_format
    )
    target_link_options(nop_plugin PRIVATE
        "LINKER:--unresolved-symbols=ignore-all"
    )
else()
    target_link_libraries(nop_plugin PRIVATE
        absl::status
        absl::str_format
    )
    if(APPLE)
        # The server provides these symbols when it loads the plugin.
        target_link_options(nop_plugin PRIVATE "LINKER:-undefined,dynamic_lookup")
    endif()
endif()

set_target_properties(nop_plugin PROPERTIES
    PREFIX ""
    OUTPUT_NAME "nop_plugin"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/plugins"
)

if(APPLE)
    # A plugin loaded with dlopen gets its own thread-local co::self, so the
    # client test links these libraries and calls their *_Create() functions
    # directly.
    add_library(nop_plugin_lib STATIC
        nop_plugin.cc
    )
    target_include_directories(nop_plugin_lib PRIVATE
        ${CMAKE_BINARY_DIR}
        $<TARGET_PROPERTY:co,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:toolbelt,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:subspace_common,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:subspace_client,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:subspace_proto,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:libserver,INTERFACE_INCLUDE_DIRECTORIES>
    )
    add_dependencies(nop_plugin_lib subspace_proto subspace_phaser)

    add_library(split_buffer_free_test_plugin_lib STATIC
        split_buffer_free_test_plugin.cc
    )
    target_include_directories(split_buffer_free_test_plugin_lib PRIVATE
        ${CMAKE_BINARY_DIR}
        $<TARGET_PROPERTY:co,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:toolbelt,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:subspace_common,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:subspace_proto,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:libserver,INTERFACE_INCLUDE_DIRECTORIES>
    )
    add_dependencies(split_buffer_free_test_plugin_lib
        subspace_proto subspace_phaser)
endif()

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

add_dependencies(split_buffer_free_test_plugin subspace_proto subspace_phaser)

if(ANDROID)
    target_link_libraries(split_buffer_free_test_plugin PRIVATE
        libserver
        subspace_client
        subspace_common
        subspace_proto
        co
        toolbelt
        absl::status
        protobuf::libprotobuf
        log
    )
elseif(UNIX AND NOT APPLE)
    target_link_libraries(split_buffer_free_test_plugin PRIVATE
        absl::status
    )
    target_link_options(split_buffer_free_test_plugin PRIVATE
        "LINKER:--unresolved-symbols=ignore-all"
    )
else()
    target_link_libraries(split_buffer_free_test_plugin PRIVATE
        absl::status
    )
    if(APPLE)
        # The server provides these symbols when it loads the plugin.
        target_link_options(split_buffer_free_test_plugin PRIVATE
            "LINKER:-undefined,dynamic_lookup")
    endif()
endif()

set_target_properties(split_buffer_free_test_plugin PROPERTIES
    PREFIX ""
    OUTPUT_NAME "split_buffer_free_test_plugin"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/plugins"
)
