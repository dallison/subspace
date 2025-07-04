// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/poll.h>

#if defined(__cplusplus)
extern "C" {
#endif

#define kMaxErrorMessageSize 256
// This is a subspace client.  This is connected to a server on the local computer
// via a Unix Domain Socket.  The socket name is "subspace" by default.
typedef struct {
    void* client;
    char error_message[kMaxErrorMessageSize];
} SubspaceClient;

// This is a subspace subscriber.  It is used to read messages from a channel.
typedef struct {
    void* subscriber;
    char error_message[kMaxErrorMessageSize];
} SubspaceSubscriber;

// This is a subspace publisher.  It is used to publish messages to a channel.
typedef struct {
    void* publisher;
    char error_message[kMaxErrorMessageSize];
} SubspacePublisher;

// This is used to hold the type information for a message.  The type is an opaque
// string that can be used by the application to determine the contents of the message.
// For example, it might be a ROS MD5 hash of the message type that can be used to
// look up a deserializer function for the message contents.
typedef struct {
    const char* type;
    size_t type_length;
} SubspaceTypeInfo;

// This is a received message.  The 'message' member is a pointer to a smart
// message object that is used to manage the message.  The 'length' member is the
// length of the message data in bytes and 'buffer' points to the
// start address of the message in shared memory.  If there is no message read,
// the length member will be zero and buffer will be nullptr.
//
// A subscriber may only have one reference to a message slot at one time but
// you can have multiple messages referring to different slots.  The 'max_active_messages'
// option in the SubscriberOptions struct determines how many messages the subscriber
// can read before they are freed.  Each message holds onto a shared memory slot
// until it is freed.  If you don't free the message, the subscriber will run out of
// slots and you will be unable to read any more messages.
typedef struct {
    void* message;             // Smart message pointer
    int32_t length;           // Length of the message
    const void* buffer;        // Address of the message payload
    uint64_t ordinal;          // Monotonic number of the message
    uint64_t timestamp;        // Nanosecond time message was published
    bool is_activation;
    char error_message[kMaxErrorMessageSize];
} SubspaceMessage;

// There are the options avaialble for publishers.
typedef struct {
    const int32_t slot_size;    // Initial size of slots (might be resized).
    const int num_slots;         // Number of slots (never changes)
    bool reliable;              // Reliable publisher.
    bool fixed_size;             // Don't resize the slot size if a larger message is sent.
    SubspaceTypeInfo type;      // Type of the message.  This is an opaque string.
    bool activate;              // Send an activation message when created.
} SubspacePublisherOptions;

typedef struct {
    bool reliable;          // Reliable subscriber.
    SubspaceTypeInfo type;  // Type of the message.  This is an opaque string.
    // Callback function to be called when a message is received.
    void (*callback)(SubspaceSubscriber*, SubspaceMessage*);
    int max_active_messages;      // Max number of message that can be active at once.
    bool pass_activation;        // Pass activation message in read.
    bool log_dropped_messages;    // Log dropped messages to stderr.
} SubspaceSubscriberOptions;

typedef enum {
    kSubspaceReadNext = 0,    // Read the next message.
    kSubspaceReadNewest = 1,  // Read the newest message.
} SubspaceReadMode;

// This is a message buffer that is used to publish a message.  The 'buffer' member
// is a pointer to the message buffer that can be used to publish a message which
// has size 'buffer_size' bytes.  The buffer is read/write and can be used to
// publish a message.
//
// If 'buffer' is nullptr, the publisher is not able to send a message at the
// present time.  This can happen if the publisher is reliable and there are
// no free slots available.  In this case, the publisher can be waited for
// using the 'waitForPublisher' function.
//
// If 'erroprMessage' is not empty, there was an error creating the buffer.
// The error message will be set to a string describing the error.
typedef struct {
    void* buffer;
    size_t buffer_size;
    char error_message[kMaxErrorMessageSize];
} SubspaceMessageBuffer;

// Create a client.  If the client is created OK, the 'client' pointer will be
// non-null.  If there is an error, the 'error_message' will be set to a
// string describing the error.
//
// There are various options for creating a client.  Use easist one.  The
// defaults are:
// socket_name: "subspace"
// client_name: ""
SubspaceClient subspace_create_client(void);
SubspaceClient subspace_create_client_with_socket(const char* socket_name);
SubspaceClient
subspace_create_client_with_socket_and_name(const char* socket_name, const char* client_name);


// Publisher and subscriber options struct creators.  Use these to create
// the options struct and then override with the values you want.
SubspaceSubscriberOptions subspace_subscriber_options_default(void);
SubspacePublisherOptions subspace_publisher_options_default(int32_t slot_size, int num_slots);

// Create a subscriber or publisher.  If the subscriber or publisher is created
// OK, the 'subscriber' or 'publisher' pointer will be non-null.  If there is
// an error, the 'error_message' will be set to a string describing the error.
// The 'channel_name' is a NUL terminated C string.
SubspaceSubscriber subspace_create_subscriber(
        SubspaceClient client,
        const char* channel_name,
        SubspaceSubscriberOptions options);
SubspacePublisher subspace_create_publisher(
        SubspaceClient client,
        const char* channel_name,
        SubspacePublisherOptions options);

// When you are finished with a subscriber, publisher or client, call these to get rid of them.
// This will free the memory and tell the server to remove them.
void subspace_remove_subscriber(SubspaceSubscriber* subscriber);
void subspace_remove_publisher(SubspacePublisher* publisher);
void subspace_remove_client(SubspaceClient* client);

// Subscriber API.

// Read a message from a subscriber.  If there are no available messages the 'length'
// field of the returned Message will be zero.  The 'buffer' field of the Message
// is set to the address of the message in shared memory which is read-only.
// When you are done with the message, it is important to call subspace_free_message
// to free up the slot.  The 'max_active_messages' field of the subscriber options
// determines how many messages the subscriber can read before they are freed.  Each
// message holds onto a shared memory slot until it is freed.  If you don't free the message,
// the subscriber will run out of slots and you will be unable to read any more messages.
SubspaceMessage subspace_read_message(SubspaceSubscriber subscriber);
SubspaceMessage
subspace_read_message_with_mode(SubspaceSubscriber subscriber, SubspaceReadMode mode);
void subspace_free_message(SubspaceMessage* message);

// The type information returned is owned by the subscriber and its lifetime is that of the
// subscriber. Don't free it.
SubspaceTypeInfo subspace_get_subscriber_type(SubspaceSubscriber subscriber);

// Get a struct that can be used to poll the subscriber.  This is used to
// wait for a message to be available. Use this in a call to poll().
// If you want to use epoll(), you can use the fd in the pollfd struct.
struct pollfd subspace_get_subscriber_poll_fd(SubspaceSubscriber subscriber);

// The slot size and number of slots will not be valid until the first message is received.
int32_t subspace_get_subscriber_slot_size(SubspaceSubscriber subscriber);
int subspace_get_subscriber_num_slots(SubspaceSubscriber subscriber);

// This is a shortcut to wait for a message to be available.  It will block
// until a message is available.
bool subspace_wait_for_subscriber(SubspaceSubscriber subscriber);

// Publisher API.

// Get the address of the buffer you can use to publish a message.  An unreliable publisher
// will always have a buffer available.  If the max_size is greater than the current buffer size,
// the buffers will be resized.  If the publisher is reliable, it may not have a buffer available.
SubspaceMessageBuffer subspace_get_message_buffer(SubspacePublisher publisher, size_t max_size);

// This sends the message in the publisher's buffer.  The messageSize
// argument specifies the actual size of the message to send.
const SubspaceMessage subspace_publish_message(SubspacePublisher publisher, size_t messageSize);

// Reliable publishers that cannot send a message at the present time can be waited for
// using this function.  It will block until the publisher is able to send a message.
bool subspace_wait_for_publisher(SubspacePublisher publisher);
struct pollfd subspace_get_publisher_poll_fd(SubspacePublisher publisher);

#if defined(__cplusplus)
}  // extern "C"
#endif
