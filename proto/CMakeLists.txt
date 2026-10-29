# CMakeLists.txt for the proto subdirectory
# This file defines the 'subspace_proto' library, which generates C++
# files from 'subspace.proto' and compiles them.

cmake_minimum_required(VERSION 3.15)

# Protobuf is already fetched by the parent CMakeLists.txt via FetchContent.
# Native builds use protobuf::protoc directly. Cross-compiles use a host-native
# protoc that must match the fetched protobuf version.

# Include FetchContent to get protobuf source directory for well-known types
include(FetchContent)

# Get the protobuf source directory to find well-known types
FetchContent_GetProperties(protobuf)
if(NOT protobuf_POPULATED)
    message(FATAL_ERROR "protobuf must be populated before proto/CMakeLists.txt is processed")
endif()

# Define the .proto file(s) in this subdirectory
set(PROTO_FILES
    subspace.proto
    tunnel_config.proto
    tunnel_handshake.proto
)

# Variables to store the generated source and header files
set(SUBSPACE_PROTO_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}")

if(CMAKE_CROSSCOMPILING)
    find_program(PROTOC_EXECUTABLE protoc REQUIRED)
    execute_process(
        COMMAND ${PROTOC_EXECUTABLE} --version
        OUTPUT_VARIABLE PROTOC_VERSION_OUTPUT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE PROTOC_VERSION_RESULT
    )
    if(NOT PROTOC_VERSION_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to run host protoc at ${PROTOC_EXECUTABLE}")
    endif()
    string(REGEX MATCH "[0-9]+(\\.[0-9]+)+" PROTOC_VERSION "${PROTOC_VERSION_OUTPUT}")
    if(NOT PROTOC_VERSION)
        message(FATAL_ERROR "Could not parse host protoc version from: ${PROTOC_VERSION_OUTPUT}")
    endif()
    if(NOT PROTOC_VERSION VERSION_EQUAL SUBSPACE_PROTOBUF_VERSION)
        message(FATAL_ERROR
            "Host protoc version ${PROTOC_VERSION} does not match fetched protobuf "
            "version ${SUBSPACE_PROTOBUF_VERSION}. Pass a matching compiler with "
            "-DPROTOC_EXECUTABLE=/path/to/protoc."
        )
    endif()
    message(STATUS "Cross-compiling: using host protoc ${PROTOC_VERSION} at ${PROTOC_EXECUTABLE}")
    set(SUBSPACE_PROTOC ${PROTOC_EXECUTABLE})
    set(SUBSPACE_PROTOC_DEPENDS)
    set(SUBSPACE_PROTOC_COMMENT " (cross-compile)")
else()
    set(SUBSPACE_PROTOC $<TARGET_FILE:protobuf::protoc>)
    set(SUBSPACE_PROTOC_DEPENDS protobuf::protoc)
    set(SUBSPACE_PROTOC_COMMENT "")
endif()

set(PROTO_GENERATED)
foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc")
    set(PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h")
    add_custom_command(
        OUTPUT ${PROTO_SRC} ${PROTO_HDR}
        COMMAND ${SUBSPACE_PROTOC}
        ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
             --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
             --proto_path=${protobuf_SOURCE_DIR}/src
             ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
                ${SUBSPACE_PROTOC_DEPENDS}
        COMMENT "Generating C++ code from ${PROTO_FILE}${SUBSPACE_PROTOC_COMMENT}"
    )
    list(APPEND PROTO_GENERATED ${PROTO_SRC} ${PROTO_HDR})
endforeach()

# Define the subspace_proto library target with the generated sources
add_library(subspace_proto STATIC
    ${PROTO_GENERATED}
)


# Add the directory containing the generated headers to the include paths.
target_include_directories(subspace_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
)

# Link the generated library against the main Protobuf library
target_link_libraries(subspace_proto PUBLIC
    protobuf::libprotobuf
)

