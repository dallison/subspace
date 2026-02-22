// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef SERVER_FFI_H
#define SERVER_FFI_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SubspaceServerHandle SubspaceServerHandle;

// Create a local server bound to the given Unix socket path.
// If notify_fd >= 0, the server will write 8 bytes (int64 value 1) when ready
// and 8 bytes (int64 value 2) when stopped.  The server takes ownership of
// notify_fd and will close it on destruction.
SubspaceServerHandle *subspace_server_create(const char *socket_name,
                                             int notify_fd);

// Run the server event loop (blocks until stopped).
// Returns 0 on success, -1 on error.
int subspace_server_run(SubspaceServerHandle *handle);

// Signal the server to stop.  This causes Run to return.
void subspace_server_stop(SubspaceServerHandle *handle);

// Remove shared-memory artifacts for this session.
void subspace_server_cleanup_after_session(SubspaceServerHandle *handle);

// Destroy the handle and free all resources.
void subspace_server_destroy(SubspaceServerHandle *handle);

#ifdef __cplusplus
}
#endif

#endif // SERVER_FFI_H
