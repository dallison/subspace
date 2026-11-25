# CMakeLists.txt for the common subdirectory
# This is likely where the core 'subspace' library components will reside.

# Define the common library target.
# Example for the core subspace library:
add_library(subspace_common STATIC
    atomic_bitset.h
    channel.cc
    channel.h
    fast_ring_buffer.h
    split_buffer.cc
    split_buffer.h
    system_info.cc
    system_info.h
    syscall_shim.cc
    syscall_shim.h
    syscall_shim_test_helper.h
)

target_link_libraries(subspace_common PUBLIC
    absl::base
    absl::flags
    absl::flat_hash_map
    absl::flat_hash_set
    absl::strings
    absl::str_format
    toolbelt
    co
    subspace_proto
    protobuf::libprotobuf
)

# channel.h refers to co20 types and the backend link-check symbol (defined by
# subspace_async, which also brings in co_cpp20) when the C++20 coroutine
# backend is selected.
if(SUBSPACE_CORO_BACKEND_RESOLVED STREQUAL "co20")
    target_link_libraries(subspace_common PUBLIC subspace_async)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT ANDROID)
    target_link_libraries(subspace_common PUBLIC rt)
elseif(ANDROID)
    target_link_libraries(subspace_common PUBLIC log)
endif()

# --- Async abstraction layer ---
# Build-time selectable coroutine/networking backend (see context.h).  The
# SUBSPACE_CORO_BACKEND macro is set globally in the top-level CMakeLists.txt.
add_library(subspace_async STATIC
    async/context.h
    async/runtime.h
    async/mtls.h
    async/byte_stream.h
    async/byte_stream.cc
    async/socket.h
    async/socket.cc
    async/task.h
    async/wait.h
    async/wait.cc
)
target_link_libraries(subspace_async PUBLIC
    absl::status
    absl::statusor
    absl::str_format
    toolbelt
    co
)
if(SUBSPACE_ENABLE_MTLS)
    find_package(OpenSSL REQUIRED)
    target_compile_definitions(subspace_async PUBLIC SUBSPACE_ENABLE_MTLS=1)
    target_link_libraries(subspace_async PUBLIC OpenSSL::SSL OpenSSL::Crypto)
else()
    target_compile_definitions(subspace_async PUBLIC SUBSPACE_ENABLE_MTLS=0)
endif()
if(SUBSPACE_CORO_BACKEND_RESOLVED STREQUAL "asio")
    target_link_libraries(subspace_async PUBLIC Boost::asio Boost::coroutine)
elseif(SUBSPACE_CORO_BACKEND_RESOLVED STREQUAL "co20")
    target_link_libraries(subspace_async PUBLIC co_cpp20)
endif()

add_executable(async_test async/async_test.cc)
add_test(NAME async_test COMMAND async_test)
target_link_libraries(async_test PUBLIC subspace_async gtest gtest_main)

add_executable(mtls_test async/mtls_test.cc)
add_test(NAME mtls_test COMMAND mtls_test)
target_link_libraries(mtls_test PUBLIC subspace_async absl::status_matchers gtest gtest_main)

add_executable(common_test common_test.cc)
add_test(NAME common_test COMMAND common_test)
target_link_libraries(common_test PUBLIC subspace_common gtest gtest_main)

add_executable(split_buffer_test split_buffer_test.cc)
add_test(NAME split_buffer_test COMMAND split_buffer_test)
target_link_libraries(split_buffer_test PUBLIC subspace_common gtest gtest_main)

