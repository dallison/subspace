// Copyright 2025 David Allison
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
// The C API is simpler to integrate into other language bindings and has fewer dependencies
// on C++ things (like Abseil).

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/poll.h>

#if defined(__cplusplus)
extern "C" {
#endif

// Error Handling.  Most functions return a boolean indicating success or failure.
// If a function fails, you can call subspace_get_last_error() to get the
// error message.  The error message is a static string that is owned by the
// library and will be cleared by the API call.  The maximum
// size of the error message is defined by kMaxErrorMessageSize.  If there is
// no error, the error message will be an empty string.
//
// The error message is truncated to kMaxErrorMessageSize characters and is thread_local
// (one error message per thread, just like errno).
//
// The function subspace_has_error() can be used to check if there is an error and
// subspace_get_last_error() can be used to get the error message.

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

// This is a received message.  The 'message' member is a pointer to a smart
// message object that is used to manage the message.  The 'length' member is
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
  void *message;      // Smart message pointer
  size_t length;      // Length of the message
  const void *buffer; // Address of the message payload
  uint64_t ordinal;   // Monotonic number of the message
  uint64_t timestamp; // Nanosecond time message was published
  bool is_activation;
} SubspaceMessage;

// There are the options avaialble for publishers.
typedef struct {
  const int32_t slot_size; // Initial size of slots (might be resized).
  const int num_slots;     // Number of slots (never changes)
  bool reliable;           // Reliable publisher.
  bool fixed_size; // Don't resize the slot size if a larger message is sent.
  SubspaceTypeInfo type; // Type of the message.  This is an opaque string.
  bool activate;         // Send an activation message when created.
} SubspacePublisherOptions;

typedef struct {
  bool reliable;           // Reliable subscriber.
  SubspaceTypeInfo type;   // Type of the message.  This is an opaque string.
  int max_active_messages; // Max number of message that can be active at once.
  bool pass_activation;    // Pass activation message in read.
  bool log_dropped_messages; // Log dropped messages to stderr.
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

// Waits with an additional file descriptor that can be used to interrupt the
// wait.  Returns the integer fd value of the file descriptor that triggered
// the wait.  Returns -1 on error.
int subspace_wait_for_subscriber_with_fd(SubspaceSubscriber subscriber, int fd);

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

// Reliable publishers that cannot send a message at the present time can be
// waited for using this function.  It will block until the publisher is able to
// send a message.  Returns -1 on error.
bool subspace_wait_for_publisher(SubspacePublisher publisher);

// Waits with an additional file descriptor that can be used to interrupt the
// wait.  Returns the integer fd value of the file descriptor that triggered
// the wait.
int subspace_wait_for_publisher_with_fd(SubspacePublisher publisher, int fd);

struct pollfd subspace_get_publisher_poll_fd(SubspacePublisher publisher);
int subspace_get_publisher_fd(SubspacePublisher publisher);

bool subspace_register_resize_callback(SubspacePublisher publisher,
                                       bool (*callback)(SubspacePublisher,
                                                        int32_t, int32_t));
bool subspace_unregister_resize_callback(SubspacePublisher publisher);

#if defined(__cplusplus)
} // extern "C"
#endif
