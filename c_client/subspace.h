// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

// This is the C API for the Subspace client library.
// It provides a simple interface for creating clients, publishers, and
// subscribers.
//
// The API is designed to be simple and easy to use, while still providing
// the necessary functionality to publish and subscribe to messages on channels.
//
// The C API is simpler to integrate into other language bindings and has fewer
// dependencies on C++ things (like Abseil).

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/poll.h>

#if defined(__cplusplus)
extern "C" {
#endif

// Error Handling.  Most functions return a boolean indicating success or
// failure. If a function fails, you can call subspace_get_last_error() to get
// the error message.  The error message is a static string that is owned by the
// library and will be cleared by the API call.  The maximum
// size of the error message is defined by kMaxErrorMessageSize.  If there is
// no error, the error message will be an empty string.
//
// The error message is truncated to kMaxErrorMessageSize characters and is
// thread_local (one error message per thread, just like errno).
//
// The function subspace_has_error() can be used to check if there is an error
// and subspace_get_last_error() can be used to get the error message.

#define kMaxErrorMessageSize 256
typedef struct {
  char error_message[kMaxErrorMessageSize];
} SubspaceError;

// This is a subspace client.  This is connected to a server on the local
// computer via a Unix Domain Socket.  The socket name is "subspace" by default.
typedef struct {
  void *client;
} SubspaceClient;

// This is a subspace subscriber.  It is used to read messages from a channel.
typedef struct {
  void *subscriber;
} SubspaceSubscriber;

// This is a subspace publisher.  It is used to publish messages to a channel.
typedef struct {
  void *publisher;
} SubspacePublisher;

// This is used to hold the type information for a message.  The type is an
// opaque string that can be used by the application to determine the contents
// of the message. For example, it might be a ROS MD5 hash of the message type
// that can be used to look up a deserializer function for the message contents.
typedef struct {
  const char *type;
  size_t type_length;
} SubspaceTypeInfo;

typedef struct {
  const char *data;
  size_t length;
} SubspaceString;

typedef struct {
  uint16_t num_pub_updates;
  uint16_t num_sub_updates;
  uint16_t num_pubs;
  uint16_t num_reliable_pubs;
  uint16_t num_subs;
  uint16_t num_reliable_subs;
  uint16_t num_resizes;
} SubspaceChannelCounters;

typedef struct {
  SubspaceString channel_name;
  int num_publishers;
  int num_subscribers;
  int num_bridge_pubs;
  int num_bridge_subs;
  int num_tunnel_pubs;
  int num_tunnel_subs;
  SubspaceString type;
  uint64_t slot_size;
  int num_slots;
  bool reliable;
} SubspaceChannelInfo;

typedef struct {
  SubspaceString channel_name;
  uint64_t total_bytes;
  uint64_t total_messages;
  uint64_t max_message_size;
} SubspaceChannelStats;

typedef struct {
  const char *channel_name;
  uint64_t session_id;
  uint32_t buffer_index;
  uint32_t slot_id;
  bool is_prefix;
  uint64_t full_size;
  uint64_t allocation_size;
  uintptr_t handle;
} SubspaceSplitBufferInfo;

typedef struct {
  uintptr_t handle;
  void *address;
  size_t size;
  void *private_data;
} SubspaceSplitBufferMapping;

typedef struct {
  const uint8_t *data;
  size_t size;
} SubspaceChecksumSpan;

typedef void (*SubspaceChecksumCallback)(const SubspaceChecksumSpan *spans,
                                         size_t span_count, uint8_t *checksum,
                                         size_t checksum_size, void *user_data);

typedef bool (*SubspaceBufferTransformCallback)(void *buffer, int64_t size,
                                                int64_t *out_size,
                                                void *user_data);

typedef bool (*SubspaceSplitAllocateCallback)(
    const SubspaceSplitBufferInfo *info, SubspaceSplitBufferMapping *mapping,
    void *user_data);
typedef bool (*SubspaceSplitMapCallback)(const SubspaceSplitBufferInfo *info,
                                         SubspaceSplitBufferMapping *mapping,
                                         void *user_data);
typedef bool (*SubspaceSplitReleaseCallback)(
    const SubspaceSplitBufferInfo *info,
    const SubspaceSplitBufferMapping *mapping, void *user_data);

typedef struct {
  SubspaceSplitAllocateCallback allocate;
  SubspaceSplitMapCallback map;
  SubspaceSplitReleaseCallback unmap;
  SubspaceSplitReleaseCallback free;
  void *user_data;
} SubspaceSplitBufferCallbacks;

// This is a received message.  The 'message' member is an opaque owned message
// handle that is used to manage the message.  The 'length' member is
// the length of the message data in bytes and 'buffer' points to the start
// address of the message in shared memory.  If there is no message read, the
// length member will be zero and buffer will be nullptr.
//
// A subscriber may only have one reference to a message slot at one time but
// you can have multiple messages referring to different slots.  The
// 'max_active_messages' option in the SubscriberOptions struct determines how
// many messages the subscriber can read before they are freed.  Each message
// holds onto a shared memory slot until it is freed.  If you don't free the
// message, the subscriber will run out of slots and you will be unable to read
// any more messages.
typedef struct {
  void *message;      // Opaque owned message handle.
  size_t length;      // Length of the message
  const void *buffer; // Address of the message payload
  uint64_t ordinal;   // Monotonic number of the message
  uint64_t timestamp; // Nanosecond time message was published
  int vchan_id;
  bool is_activation;
  int32_t slot_id;
  bool checksum_error;
} SubspaceMessage;

typedef struct {
  void *slot;
} SubspaceMessageSlot;

typedef struct {
  void *prefix;
} SubspaceMessagePrefix;

typedef struct {
  int32_t id;
  uint64_t ordinal;
  uint64_t message_size;
  int16_t buffer_index;
  int16_t vchan_id;
  uint64_t timestamp;
  uint32_t flags;
  int32_t bridged_slot_id;
} SubspaceMessageSlotSnapshot;

typedef struct {
  int32_t slot_id;
  uint64_t message_size;
  uint64_t ordinal;
  uint64_t timestamp;
  int64_t flags;
  int32_t vchan_id;
  uint16_t checksum_size;
  uint16_t metadata_size;
  uint32_t checksum;
} SubspaceMessagePrefixSnapshot;

// There are the options avaialble for publishers.
typedef struct {
  const int32_t slot_size; // Initial size of slots (might be resized).
  const int num_slots;     // Number of slots (never changes)
  bool local;              // If true, messages stay local to this machine.
  bool reliable;           // Reliable publisher.
  bool bridge;             // This publisher is for the bridge.
  bool for_tunnel;         // Mark messages for external tunnel processes.
  bool fixed_size; // Don't resize the slot size if a larger message is sent.
  SubspaceTypeInfo type; // Type of the message.  This is an opaque string.
  bool activate;         // Send an activation message when created.
  const char *mux;       // Optional mux channel name for virtual channels.
  size_t mux_length;
  int vchan_id;           // Virtual channel id, or -1 for server-assigned.
  bool notify_retirement; // Notify publisher when slots retire.
  bool checksum;          // Calculate and attach message checksums.
  int32_t checksum_size;  // Bytes reserved for checksum (default 4).
  int32_t metadata_size;  // Bytes reserved for user metadata (default 0).

  // Free-slot allocator policy for unreliable publishers (no effect on
  // reliable ones).  When true (the default, set by
  // subspace_publisher_options_default), the publisher prefers to
  // recycle a recently retired slot — whose pages are already cache-hot
  // from the last publish/consume cycle — over pulling a fresh slot
  // out of the never-touched FreeSlots pool, which would demand-fault
  // new physical pages on first write.  In steady state this lets the
  // publisher cycle through a tiny working set of cache-hot slots
  // regardless of how deep num_slots is configured, while still
  // bursting into FreeSlots when the subscriber falls behind.  Set to
  // false to force the legacy FreeSlots-first allocator (useful when
  // reproducing pre-fix benchmarks).
  bool prefer_retired_slots;

  // Split-buffer options.  When use_split_buffers is true, Subspace keeps
  // message prefixes in regular shared memory and delegates payload slot
  // allocation/mapping/freeing to split_callbacks.
  bool use_split_buffers;
  // If true, bridge receivers create their mirror publisher with split payload
  // buffers. This is independent of use_split_buffers for the local channel.
  bool split_buffers_over_bridge;
  int32_t max_publishers;
  SubspaceSplitBufferCallbacks split_callbacks;
} SubspacePublisherOptions;

typedef struct {
  bool reliable;           // Reliable subscriber.
  bool bridge;             // This subscriber is for the bridge.
  bool for_tunnel;         // Mark subscriptions for external tunnels.
  SubspaceTypeInfo type;   // Type of the message.  This is an opaque string.
  int max_active_messages; // Max number of message that can be active at once.
  bool pass_activation;    // Pass activation message in read.
  bool log_dropped_messages; // Log dropped messages to stderr.
  bool detect_dropped_messages; // Detect and count ordinal gaps internally.
  bool read_write;           // Map buffers writable for this subscriber.
  const char *mux;           // Optional mux channel name for virtual channels.
  size_t mux_length;
  int vchan_id;              // Virtual channel id, or -1 for server-assigned.
  bool checksum;             // Verify message checksums.
  bool pass_checksum_errors; // Read messages even if checksum verification
                             // fails.
  bool keep_active_message;  // Keep the most recent active message referenced.

  // Optional callbacks used when the server reports that the publisher
  // created split payload buffers.
  SubspaceSplitBufferCallbacks split_callbacks;
} SubspaceSubscriberOptions;

typedef enum {
  kSubspaceReadNext = 0,   // Read the next message.
  kSubspaceReadNewest = 1, // Read the newest message.
} SubspaceReadMode;

// This is a message buffer that is used to publish a message.  The 'buffer'
// member is a pointer to the message buffer that can be used to publish a
// message which has size 'buffer_size' bytes.  The buffer is read/write and can
// be used to publish a message.
//
// If 'buffer' is nullptr, the publisher is not able to send a message at the
// present time.  This can happen if the publisher is reliable and there are
// no free slots available.  In this case, the publisher can be waited for
// using the 'waitForPublisher' function.
typedef struct {
  void *buffer;
  size_t buffer_size;
} SubspaceMessageBuffer;

// Get the last error message from the subspace library.  This will return
// a pointer to a string that is owned by the library.  The string will be
// empty if there is no error.  The string will be truncated to
// kMaxErrorMessageSize characters.  If there is an error, the string will
// contain a description of the error that occurred.
char *subspace_get_last_error(void);

// If the last call failed this indicates that there was an error.  Use
// subspace_get_last_error() to get the error message.  If there is no error,
// this will return false.
bool subspace_has_error(void);

// Create a client.  If the client is created OK, the 'client' pointer will be
// non-null.  If there is an error, the 'error_message' will be set to a
// string describing the error.  Use subspace_get_last_error() to get the error
// message.
//
// There are various options for creating a client.  Use easist one.  The
// defaults are:
// socket_name: "subspace"
// client_name: ""
SubspaceClient subspace_create_client(void);
SubspaceClient subspace_create_client_with_socket(const char *socket_name);
SubspaceClient
subspace_create_client_with_socket_and_name(const char *socket_name,
                                            const char *client_name);
bool subspace_set_client_debug(SubspaceClient client, bool debug);
bool subspace_set_client_thread_safe(SubspaceClient client, bool thread_safe);
bool subspace_channel_exists(SubspaceClient client, const char *channel_name,
                             bool *exists);
bool subspace_get_channel_counters(SubspaceClient client,
                                   const char *channel_name,
                                   SubspaceChannelCounters *counters);
bool subspace_get_channel_info(SubspaceClient client, const char *channel_name,
                               SubspaceChannelInfo *info);
bool subspace_get_all_channel_info(SubspaceClient client,
                                   SubspaceChannelInfo **infos, size_t *count);
bool subspace_get_channel_stats(SubspaceClient client, const char *channel_name,
                                SubspaceChannelStats *stats);
bool subspace_get_all_channel_stats(SubspaceClient client,
                                    SubspaceChannelStats **stats,
                                    size_t *count);

// Publisher and subscriber options struct creators.  Use these to create
// the options struct and then override with the values you want.
SubspaceSubscriberOptions subspace_subscriber_options_default(void);
SubspacePublisherOptions subspace_publisher_options_default(int32_t slot_size,
                                                            int num_slots);

// Create a subscriber or publisher.  If the subscriber or publisher is created
// OK, the 'subscriber' or 'publisher' pointer will be non-null.  If there is
// an error, the 'error_message' will be set to a string describing the error.
// The 'channel_name' is a NUL terminated C string.
SubspaceSubscriber
subspace_create_subscriber(SubspaceClient client, const char *channel_name,
                           SubspaceSubscriberOptions options);
SubspacePublisher subspace_create_publisher(SubspaceClient client,
                                            const char *channel_name,
                                            SubspacePublisherOptions options);

// When you are finished with a subscriber, publisher or client, call these to
// get rid of them. This will free the memory and tell the server to remove
// them.
bool subspace_remove_subscriber(SubspaceSubscriber *subscriber);
bool subspace_remove_publisher(SubspacePublisher *publisher);
bool subspace_remove_client(SubspaceClient *client);

// Subscriber API.

// Read a message from a subscriber.  If there are no available messages the
// 'length' field of the returned Message will be zero.  The 'buffer' field of
// the Message is set to the address of the message in shared memory which is
// read-only. When you are done with the message, it is important to call
// subspace_free_message to free up the slot.  The 'max_active_messages' field
// of the subscriber options determines how many messages the subscriber can
// read before they are freed.  Each message holds onto a shared memory slot
// until it is freed.  If you don't free the message, the subscriber will run
// out of slots and you will be unable to read any more messages.
SubspaceMessage subspace_read_message(SubspaceSubscriber subscriber);
SubspaceMessage subspace_read_message_with_mode(SubspaceSubscriber subscriber,
                                                SubspaceReadMode mode);
SubspaceMessage subspace_find_message(SubspaceSubscriber subscriber,
                                      uint64_t timestamp);
bool subspace_get_all_messages(SubspaceSubscriber subscriber,
                               SubspaceReadMode mode,
                               SubspaceMessage **messages, size_t *count);
bool subspace_free_messages(SubspaceMessage *messages, size_t count);
bool subspace_free_message(SubspaceMessage *message);

// The type information returned is owned by the subscriber and its lifetime is
// that of the subscriber. Don't free it.
SubspaceTypeInfo subspace_get_subscriber_type(SubspaceSubscriber subscriber);

// Get a struct that can be used to poll the subscriber.  This is used to
// wait for a message to be available. Use this in a call to poll().
// If you want to use epoll(), you can use the fd in the pollfd struct.
struct pollfd subspace_get_subscriber_poll_fd(SubspaceSubscriber subscriber);

// Get the subscriber file descriptor.  This is the file descriptor that can be
// used tell a subscriber that there is a message available to read
int subspace_get_subscriber_fd(SubspaceSubscriber subscriber);

// The slot size and number of slots will not be valid until the first message
// is received.
int32_t subspace_get_subscriber_slot_size(SubspaceSubscriber subscriber);
int subspace_get_subscriber_num_slots(SubspaceSubscriber subscriber);

// This is a shortcut to wait for a message to be available.  It will block
// until a message is available.
bool subspace_wait_for_subscriber(SubspaceSubscriber subscriber);

// Waits for a message to be available for up to timeout_ms.  Returns false on
// timeout or error.
bool subspace_wait_for_subscriber_with_timeout(SubspaceSubscriber subscriber,
                                               uint64_t timeout_ms);

// Waits with an additional file descriptor that can be used to interrupt the
// wait.  Returns the integer fd value of the file descriptor that triggered
// the wait.  Returns -1 on error.
int subspace_wait_for_subscriber_with_fd(SubspaceSubscriber subscriber, int fd);
int subspace_wait_for_subscriber_with_fd_and_timeout(
    SubspaceSubscriber subscriber, int fd, uint64_t timeout_ms);

bool subspace_register_subscriber_callback(SubspaceSubscriber subscriber,
                                           void (*callback)(SubspaceSubscriber,
                                                            SubspaceMessage));

// Unregister the callback and return true is successful.
bool subspace_remove_subscriber_callback(SubspaceSubscriber subscriber);

bool subspace_register_dropped_message_callback(
    SubspaceSubscriber subscriber,
    void (*callback)(SubspaceSubscriber, int64_t));

// Unregister the callback and return true is successful.
bool subspace_remove_dropped_message_callback(SubspaceSubscriber subscriber);

// Get all available messages from the subscriber and call the callback that has
// been previously registered using subspace_register_subscriber_callback.
bool subspace_process_all_messages(SubspaceSubscriber subscriber);
bool subspace_process_all_messages_with_mode(SubspaceSubscriber subscriber,
                                             SubspaceReadMode mode);

// Invoke the subscriber's registered message callback (the one installed via
// subspace_register_subscriber_callback) with the supplied message. Use this
// when you want to drive callback dispatch yourself after reading a message
// with subspace_read_message / subspace_read_message_with_mode (for example,
// from your own event loop) instead of running subspace_process_all_messages
// or relying on a worker thread.
//
// The supplied SubspaceMessage is *not* consumed by this call; the caller still
// owns it and must subspace_free_message it as usual. Returns false (and sets
// the last error) if either argument is invalid; returns true if dispatch was
// attempted (the callback is a no-op when none has been registered).
bool subspace_invoke_subscriber_callback(SubspaceSubscriber subscriber,
                                         SubspaceMessage message);
bool subspace_clear_active_message(SubspaceSubscriber subscriber);
bool subspace_trigger_reliable_publishers(SubspaceSubscriber subscriber);
bool subspace_trigger_subscriber(SubspaceSubscriber subscriber);
bool subspace_untrigger_subscriber(SubspaceSubscriber subscriber);
int64_t subspace_get_subscriber_current_ordinal(SubspaceSubscriber subscriber);
int64_t subspace_get_subscriber_timestamp(SubspaceSubscriber subscriber);
int subspace_get_subscriber_num_active_messages(SubspaceSubscriber subscriber);
bool subspace_is_subscriber_placeholder(SubspaceSubscriber subscriber);
bool subspace_is_subscriber_reliable(SubspaceSubscriber subscriber);
bool subspace_is_subscriber_for_tunnel(SubspaceSubscriber subscriber);
bool subspace_subscriber_uses_split_buffers(SubspaceSubscriber subscriber);
int subspace_get_subscriber_virtual_channel_id(SubspaceSubscriber subscriber);
int subspace_get_subscriber_configured_vchan_id(SubspaceSubscriber subscriber);
SubspaceString subspace_get_subscriber_name(SubspaceSubscriber subscriber);
SubspaceString subspace_get_subscriber_mux(SubspaceSubscriber subscriber);
int subspace_get_subscriber_num_subscribers(SubspaceSubscriber subscriber,
                                            int vchan_id);
uint64_t
subspace_get_subscriber_virtual_memory_usage(SubspaceSubscriber subscriber);
bool subspace_get_subscriber_counters(SubspaceSubscriber subscriber,
                                      SubspaceChannelCounters *counters);
bool subspace_get_subscriber_channel_counters(
    SubspaceSubscriber subscriber, SubspaceChannelCounters *counters);
SubspaceMessageSlot subspace_get_subscriber_slot(SubspaceSubscriber subscriber,
                                                 int slot_id);
SubspaceMessagePrefix
subspace_get_subscriber_prefix(SubspaceSubscriber subscriber,
                               SubspaceMessageSlot slot);
bool subspace_snapshot_message_slot(SubspaceMessageSlot slot,
                                    SubspaceMessageSlotSnapshot *snapshot);
bool subspace_snapshot_message_prefix(SubspaceMessagePrefix prefix,
                                      SubspaceMessagePrefixSnapshot *snapshot);
bool subspace_dump_subscriber_slots(SubspaceSubscriber subscriber,
                                    const char **data, size_t *length);
bool subspace_register_subscriber_on_receive_callback(
    SubspaceSubscriber subscriber, SubspaceBufferTransformCallback callback,
    void *user_data);
bool subspace_unregister_subscriber_on_receive_callback(
    SubspaceSubscriber subscriber);

// Publisher API.

// Get the address of the buffer you can use to publish a message.  An
// unreliable publisher will always have a buffer available.  If the max_size is
// greater than the current buffer size, the buffers will be resized.  If the
// publisher is reliable, it may not have a buffer available.
SubspaceMessageBuffer subspace_get_message_buffer(SubspacePublisher publisher,
                                                  size_t max_size);

// This sends the message in the publisher's buffer.  The messageSize
// argument specifies the actual size of the message to send.
const SubspaceMessage subspace_publish_message(SubspacePublisher publisher,
                                               size_t messageSize);
const SubspaceMessage
subspace_publish_message_with_prefix(SubspacePublisher publisher,
                                     size_t message_size,
                                     bool use_slot_id_from_prefix);
bool subspace_cancel_publish(SubspacePublisher publisher);

// Reliable publishers that cannot send a message at the present time can be
// waited for using this function.  It will block until the publisher is able to
// send a message.  Returns -1 on error.
bool subspace_wait_for_publisher(SubspacePublisher publisher);
bool subspace_wait_for_publisher_with_timeout(SubspacePublisher publisher,
                                              uint64_t timeout_ms);

// Waits with an additional file descriptor that can be used to interrupt the
// wait.  Returns the integer fd value of the file descriptor that triggered
// the wait.
int subspace_wait_for_publisher_with_fd(SubspacePublisher publisher, int fd);
int subspace_wait_for_publisher_with_fd_and_timeout(SubspacePublisher publisher,
                                                    int fd,
                                                    uint64_t timeout_ms);

struct pollfd subspace_get_publisher_poll_fd(SubspacePublisher publisher);
int subspace_get_publisher_fd(SubspacePublisher publisher);
int subspace_get_publisher_retirement_fd(SubspacePublisher publisher);
bool subspace_is_publisher_reliable(SubspacePublisher publisher);
bool subspace_is_publisher_local(SubspacePublisher publisher);
bool subspace_is_publisher_fixed_size(SubspacePublisher publisher);
bool subspace_is_publisher_for_tunnel(SubspacePublisher publisher);
bool subspace_publisher_uses_split_buffers(SubspacePublisher publisher);
int32_t subspace_get_publisher_slot_size(SubspacePublisher publisher);
int32_t subspace_get_publisher_num_slots(SubspacePublisher publisher);
SubspaceString subspace_get_publisher_name(SubspacePublisher publisher);
SubspaceString subspace_get_publisher_type(SubspacePublisher publisher);
SubspaceString subspace_get_publisher_mux(SubspacePublisher publisher);
int subspace_get_publisher_virtual_channel_id(SubspacePublisher publisher);
int subspace_get_publisher_num_subscribers(SubspacePublisher publisher,
                                           int vchan_id);
int subspace_get_publisher_current_slot_id(SubspacePublisher publisher);
uint64_t
subspace_get_publisher_virtual_memory_usage(SubspacePublisher publisher);
bool subspace_get_publisher_stats_counters(SubspacePublisher publisher,
                                           uint64_t *total_bytes,
                                           uint64_t *total_messages,
                                           uint32_t *max_message_size,
                                           uint32_t *total_drops);
bool subspace_get_publisher_counters(SubspacePublisher publisher,
                                     SubspaceChannelCounters *counters);
bool subspace_get_publisher_channel_counters(SubspacePublisher publisher,
                                             SubspaceChannelCounters *counters);
SubspaceString
subspace_get_publisher_buffer_shared_memory_name(SubspacePublisher publisher,
                                                 int buffer_index);
SubspaceMessageSlot
subspace_get_publisher_current_slot(SubspacePublisher publisher);
SubspaceMessagePrefix subspace_get_publisher_prefix(SubspacePublisher publisher,
                                                    SubspaceMessageSlot slot);
bool subspace_dump_publisher_slots(SubspacePublisher publisher,
                                   const char **data, size_t *length);

bool subspace_register_resize_callback(SubspacePublisher publisher,
                                       bool (*callback)(SubspacePublisher,
                                                        int32_t, int32_t));
bool subspace_unregister_resize_callback(SubspacePublisher publisher);
bool subspace_register_publisher_on_send_callback(
    SubspacePublisher publisher, SubspaceBufferTransformCallback callback,
    void *user_data);
bool subspace_unregister_publisher_on_send_callback(
    SubspacePublisher publisher);

// Register or unregister a custom checksum callback. When registered, the
// callback replaces the default CRC32 checksum calculation/verification for
// that publisher/subscriber. The callback receives the data spans included in
// the checksum plus a writable checksum buffer of the configured checksum_size.
bool subspace_register_publisher_checksum_callback(
    SubspacePublisher publisher, SubspaceChecksumCallback callback,
    void *user_data);
bool subspace_unregister_publisher_checksum_callback(
    SubspacePublisher publisher);
bool subspace_register_subscriber_checksum_callback(
    SubspaceSubscriber subscriber, SubspaceChecksumCallback callback,
    void *user_data);
bool subspace_unregister_subscriber_checksum_callback(
    SubspaceSubscriber subscriber);

// Per-message user-metadata accessors.
//
// A publisher / subscriber created with metadata_size > 0 reserves that
// many bytes of opaque user metadata in the prefix area of every slot.
// These calls expose that area:
//
//   - subspace_get_publisher_metadata returns a writable pointer into the
//     current message buffer's metadata region. It is valid between
//     subspace_get_message_buffer() and subspace_publish_message().
//
//   - subspace_get_subscriber_metadata returns a read-only pointer into
//     the most recently read message's metadata region. It is valid
//     between subspace_read_message() and the next read on the same
//     subscriber.
//
// In both cases *out_size receives the metadata-region size in bytes
// (0 if the channel was created with metadata_size == 0). On any error
// the returned pointer is NULL, *out_size is 0, and the call sets the
// thread-local error string accessible via subspace_get_last_error().
void *subspace_get_publisher_metadata(SubspacePublisher publisher,
                                      size_t *out_size);
const void *subspace_get_subscriber_metadata(SubspaceSubscriber subscriber,
                                             size_t *out_size);

// Convenience reporter: returns the configured per-message metadata
// region size (in bytes) for the publisher / subscriber. Returns 0 if
// the handle is invalid or metadata is not enabled.
int32_t subspace_get_publisher_metadata_size(SubspacePublisher publisher);
int32_t subspace_get_subscriber_metadata_size(SubspaceSubscriber subscriber);
int32_t subspace_get_publisher_prefix_size(SubspacePublisher publisher);
int32_t subspace_get_subscriber_prefix_size(SubspaceSubscriber subscriber);
int32_t subspace_get_publisher_checksum_size(SubspacePublisher publisher);
int32_t subspace_get_subscriber_checksum_size(SubspaceSubscriber subscriber);

// Returns one payload address per slot for the mapped channel. The returned
// array is owned by the publisher / subscriber handle and remains valid until
// the channel buffers change, the function is called again on the same handle,
// or the handle is destroyed.
bool subspace_get_publisher_addresses(SubspacePublisher publisher,
                                      void ***addresses, size_t *count);
bool subspace_get_subscriber_addresses(SubspaceSubscriber subscriber,
                                       void ***addresses, size_t *count);

// Split-buffer handle accessors. Handles are allocator-defined identifiers for
// payload memory, not normal pointers. For example, a Qualcomm memory-pool
// allocator may return handles that qcomm APIs can map or free. Returned handle
// arrays are owned by the publisher / subscriber and remain valid until the
// channel changes buffers or is destroyed.
bool subspace_get_publisher_split_buffer_handle_from_address(
    SubspacePublisher publisher, const void *address, uintptr_t *handle);
bool subspace_get_publisher_split_buffer_handles(SubspacePublisher publisher,
                                                 uintptr_t **handles,
                                                 size_t *count);
bool subspace_get_subscriber_split_buffer_handle_from_address(
    SubspaceSubscriber subscriber, const void *address, uintptr_t *handle);
bool subspace_get_subscriber_split_buffer_handles(SubspaceSubscriber subscriber,
                                                  uintptr_t **handles,
                                                  size_t *count);

#if defined(__cplusplus)
} // extern "C"
#endif
