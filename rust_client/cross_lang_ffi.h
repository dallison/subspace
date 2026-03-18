// Copyright 2026 David Allison
// Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef RUST_CLIENT_CROSS_LANG_FFI_H
#define RUST_CLIENT_CROSS_LANG_FFI_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles for C++ client objects.
typedef void *CppClientHandle;
typedef void *CppPublisherHandle;
typedef void *CppSubscriberHandle;

CppClientHandle cpp_test_create_client(const char *socket_name,
                                       const char *name);
void cpp_test_destroy_client(CppClientHandle handle);

CppPublisherHandle cpp_test_create_publisher(CppClientHandle client,
                                             const char *channel,
                                             int32_t slot_size, int num_slots,
                                             int32_t checksum_size,
                                             int32_t metadata_size);
void cpp_test_destroy_publisher(CppPublisherHandle handle);

// Write payload and optional metadata, then publish.
// Returns the message ordinal on success, or -1 on error.
int64_t cpp_test_publish(CppPublisherHandle pub_handle, const void *payload,
                         size_t payload_len, const void *metadata,
                         size_t metadata_len);

CppSubscriberHandle cpp_test_create_subscriber(CppClientHandle client,
                                               const char *channel,
                                               bool checksum);
void cpp_test_destroy_subscriber(CppSubscriberHandle handle);

// Get the subscriber's notification file descriptor for poll().
int cpp_test_subscriber_fd(CppSubscriberHandle handle);

// Read a message.  Returns payload length (0 = no message, -1 = error).
// Copies payload into payload_out (up to payload_cap bytes).
// Copies metadata into metadata_out (up to metadata_cap bytes).
// Writes the channel's metadata size into *metadata_size_out.
int64_t cpp_test_read_message(CppSubscriberHandle handle, void *payload_out,
                              size_t payload_cap, void *metadata_out,
                              size_t metadata_cap, int32_t *metadata_size_out);

#ifdef __cplusplus
}
#endif

#endif // RUST_CLIENT_CROSS_LANG_FFI_H
