cmake_minimum_required(VERSION 3.16)
project(subspace_vendor LANGUAGES CXX ASM)

find_package(ament_cmake REQUIRED)

get_filename_component(SUBSPACE_VENDOR_REAL_DIR
  "${CMAKE_CURRENT_LIST_DIR}" REALPATH)
set(SUBSPACE_SOURCE_DIR "${SUBSPACE_VENDOR_REAL_DIR}/../.." CACHE PATH
  "Path to the Subspace source tree")

set(SUBSPACE_PYTHON OFF CACHE BOOL "" FORCE)
set(SUBSPACE_ASIO_RPC OFF CACHE BOOL "" FORCE)
set(SUBSPACE_CO20_RPC OFF CACHE BOOL "" FORCE)

add_subdirectory(
  "${SUBSPACE_SOURCE_DIR}"
  "${CMAKE_CURRENT_BINARY_DIR}/subspace"
  EXCLUDE_FROM_ALL)

add_custom_target(subspace_vendor_build ALL
  DEPENDS subspace_c_client_shared subspace_server)

install(
  TARGETS subspace_c_client_shared subspace_server
  RUNTIME DESTINATION lib/${PROJECT_NAME}
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

install(
  DIRECTORY "${SUBSPACE_SOURCE_DIR}/c_client/"
  DESTINATION include/subspace/c_client
  FILES_MATCHING PATTERN "*.h")

ament_export_include_directories(include/subspace/c_client)
ament_export_libraries(subspace_c_client)
ament_package()
