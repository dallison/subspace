# CMakeLists.txt for the proto subdirectory
# This file defines the 'subspace_proto' library, which generates C++
# files from 'subspace.proto' and compiles them.

cmake_minimum_required(VERSION 3.15)

# Protobuf is already fetched by the parent CMakeLists.txt via FetchContent
# The targets like protobuf::libprotobuf and protobuf::protoc are available directly

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
)

# Variables to store the generated source and header files
set(SUBSPACE_PROTO_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Generated file paths
set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/subspace.pb.cc")
set(PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/subspace.pb.h")

# Generate C++ files from the proto file using protoc
# Include both the current source dir and protobuf src dir for well-known types
add_custom_command(
    OUTPUT ${PROTO_SRC} ${PROTO_HDR}
    COMMAND $<TARGET_FILE:protobuf::protoc>
    ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
         --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
         --proto_path=${protobuf_SOURCE_DIR}/src
         ${CMAKE_CURRENT_SOURCE_DIR}/subspace.proto
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/subspace.proto
            protobuf::protoc
    COMMENT "Generating C++ code from subspace.proto"
)

# Define the subspace_proto library target with the generated sources
add_library(subspace_proto STATIC
    ${PROTO_SRC}
    ${PROTO_HDR}
)


# Add the directory containing the generated headers to the include paths
target_include_directories(subspace_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR} # Headers are generated into the build directory of this subdirectory
)

# Link the generated library against the main Protobuf library
target_link_libraries(subspace_proto PUBLIC
    protobuf::libprotobuf
)

