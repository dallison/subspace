// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "c_client/subspace.h"
#include "client/client.h"
#include "client/message.h"
#include "client/options.h"
#include <chrono>
#include <memory>

#if defined(__cplusplus)
extern "C" {
#endif


// The SubspaceClient struct contains a pointer to a std::shared_ptr<subspace::Client>.

SubspaceClient subspace_create_client(void) {
    return subspace_create_client_with_socket_and_name("/tmp/subspace", "");
}
SubspaceClient subspace_create_client_with_socket(const char* socket_name) {
    return subspace_create_client_with_socket_and_name(socket_name, "");
}

SubspaceClient
subspace_create_client_with_socket_and_name(const char* socket_name, const char* client_name) {
    SubspaceClient client;
    memset(&client, 0, sizeof(client));
    std::shared_ptr<subspace::Client> sclient = std::make_shared<subspace::Client>();
    absl::Status status = sclient->Init(socket_name, client_name);
    if (!status.ok()) {
        strncpy(client.error_message,
                status.ToString().c_str(),
                sizeof(client.error_message) - 1);
        return client;
    }
    // Copy the shared ptr to the heap.
    client.client = reinterpret_cast<void*>(new std::shared_ptr<subspace::Client>(std::move(sclient)));
    return client;
}

SubspaceSubscriberOptions subspace_subscriber_options_default(void) {
    SubspaceSubscriberOptions options = {
            .reliable = false,
            .type = {.type = nullptr, .type_length = 0},
            .callback = nullptr,
            .max_active_messages = 1,
            .pass_activation = false,
            .log_dropped_messages = false,
    };
    return options;
}

SubspacePublisherOptions subspace_publisher_options_default(int32_t slot_size, int num_slots) {
    SubspacePublisherOptions options = {
            .slot_size = slot_size,
            .num_slots = num_slots,
            .reliable = false,
            .fixed_size = false,
            .type = {.type = nullptr, .type_length = 0},
            .activate = false,
    };
    return options;
}

SubspaceSubscriber subspace_create_subscriber(
        SubspaceClient client,
        const char* channel_name,
        SubspaceSubscriberOptions options) {
    subspace::SubscriberOptions subspace_options = {
            .reliable = options.reliable,
            .type = std::string(options.type.type, options.type.type_length),
            .max_active_messages = options.max_active_messages,
            .pass_activation = options.pass_activation,
            .log_dropped_messages = options.log_dropped_messages,
    };
    SubspaceSubscriber subscriber;
    memset(&subscriber, 0, sizeof(subscriber));
    std::shared_ptr<subspace::Client>* client_ptr =
            reinterpret_cast<std::shared_ptr<subspace::Client>*>(client.client);
    absl::StatusOr<subspace::Subscriber> status_or_sub =
            (*client_ptr)->CreateSubscriber(channel_name, subspace_options);
    if (!status_or_sub.ok()) {
        strncpy(subscriber.error_message,
                status_or_sub.status().ToString().c_str(),
                sizeof(subscriber.error_message) - 1);
        return subscriber;
    };

    subspace::Subscriber* sub_ptr = new subspace::Subscriber(std::move(*status_or_sub));
    subscriber.subscriber =
            reinterpret_cast<void*>(new std::shared_ptr<subspace::Subscriber>(sub_ptr));
    return subscriber;
}

SubspacePublisher subspace_create_publisher(
        SubspaceClient client,
        const char* channel_name,
        SubspacePublisherOptions options) {
    subspace::PublisherOptions subspace_options = {
            .slot_size = options.slot_size,
            .num_slots = options.num_slots,
            .reliable = options.reliable,
            .fixed_size = options.fixed_size,
            .type = std::string(options.type.type, options.type.type_length),
            .activate = options.activate,
    };
    SubspacePublisher publisher;
    memset(&publisher, 0, sizeof(publisher));
    std::shared_ptr<subspace::Client>* client_ptr =
            reinterpret_cast<std::shared_ptr<subspace::Client>*>(client.client);
    absl::StatusOr<subspace::Publisher> status_or_pub =
            (*client_ptr)->CreatePublisher(channel_name, subspace_options);
    if (!status_or_pub.ok()) {
        strncpy(publisher.error_message,
                status_or_pub.status().ToString().c_str(),
                sizeof(publisher.error_message) - 1);
        return publisher;
    }
    subspace::Publisher* pub_ptr = new subspace::Publisher(std::move(*status_or_pub));
    publisher.publisher = reinterpret_cast<void*>(new std::shared_ptr<subspace::Publisher>(pub_ptr));
    return publisher;
}

SubspaceMessage
subspace_read_message_with_mode(SubspaceSubscriber subscriber, SubspaceReadMode mode) {
    SubspaceMessage message = {
            .message = nullptr,
            .length = 0,
            .buffer = nullptr,
            .ordinal = uint64_t(-1),
            .timestamp = 0,
            .is_activation = false};
    if (subscriber.subscriber == nullptr) {
        return message;
    }
    // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a subspace::Subscriber.
    auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber>*>(subscriber.subscriber);
    subspace::ReadMode readMode = (mode == kSubspaceReadNewest) ? subspace::ReadMode::kReadNewest
                                                                 : subspace::ReadMode::kReadNext;
    absl::StatusOr<subspace::Message> status_or_msg = (*sub_ptr)->ReadMessage(readMode);
    if (!status_or_msg.ok()) {
        strncpy(message.error_message,
                status_or_msg.status().ToString().c_str(),
                sizeof(message.error_message) - 1);
        return message;
    }
    subspace::Message* msg = new subspace::Message(std::move(*status_or_msg));
    // This holds a pointer to a shared_ptr to the smart message.
    message.message = new std::shared_ptr<subspace::Message>(msg);
    message.length = msg->length;
    message.buffer = msg->buffer;
    message.timestamp = msg->timestamp;
    message.ordinal = msg->ordinal;
    message.is_activation = msg->is_activation;
    return message;
}

SubspaceMessage subspace_read_message(SubspaceSubscriber subscriber) {
    return subspace_read_message_with_mode(subscriber, kSubspaceReadNext);
}

void subspace_free_message(SubspaceMessage* message) {
    if (message->message == nullptr) {
        return;
    }
    std::shared_ptr<subspace::Message>* msg_ptr =
            reinterpret_cast<std::shared_ptr<subspace::Message>*>(message->message);
    delete msg_ptr;
    message->message = nullptr;
    message->length = 0;
    message->buffer = nullptr;
}

SubspaceMessageBuffer subspace_get_message_buffer(SubspacePublisher publisher, size_t max_size) {
    SubspaceMessageBuffer message_buffer;
    memset(&message_buffer, 0, sizeof(message_buffer));
    if (publisher.publisher == nullptr) {
        return message_buffer;
    }
    auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher>*>(publisher.publisher);
    absl::StatusOr<void*> statusOrBuffer = (*pub_ptr)->GetMessageBuffer(max_size);
    if (!statusOrBuffer.ok()) {
        strncpy(message_buffer.error_message,
                statusOrBuffer.status().ToString().c_str(),
                sizeof(message_buffer.error_message) - 1);
        return message_buffer;
    }
    auto buffer = std::move(*statusOrBuffer);
    message_buffer.buffer = buffer;
    message_buffer.buffer_size = max_size;
    return message_buffer;
}

const SubspaceMessage subspace_publish_message(SubspacePublisher publisher, size_t message_size) {
    SubspaceMessage published_message = {
            .length = 0, .ordinal = uint64_t(-1), .timestamp = 0};
    memset(&published_message.error_message, 0, sizeof(published_message.error_message));
    if (publisher.publisher == nullptr) {
        return published_message;
    }
    auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher>*>(publisher.publisher);
    absl::StatusOr<const subspace::Message> status_or_msg =
            (*pub_ptr)->PublishMessage(message_size);
    if (!status_or_msg.ok()) {
        strncpy(published_message.error_message,
                status_or_msg.status().ToString().c_str(),
                sizeof(published_message.error_message) - 1);
        return published_message;
    }
    auto msg = *status_or_msg;
    published_message.length = msg.length;
    published_message.ordinal = msg.ordinal;
    published_message.timestamp = msg.timestamp;
    return published_message;
}

void subspace_remove_subscriber(SubspaceSubscriber* subscriber) {
    if (subscriber->subscriber == nullptr) {
        return;
    }
    // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a subspace::Subscriber.
    auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber>*>(subscriber->subscriber);
    delete sub_ptr;
    subscriber->subscriber = nullptr;
}

void subspace_remove_publisher(SubspacePublisher* publisher) {
    if (publisher->publisher == nullptr) {
        return;
    }
    // The publisher contains a pointer to a shared_ptr to a shared_ptr to a subspace::Publisher.
    auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher>*>(publisher->publisher);
    delete pub_ptr;
    publisher->publisher = nullptr;
}

void subspace_remove_client(SubspaceClient* client) {
    if (client->client == nullptr) {
        return;
    }
    // The client contains a pointer to a shared_ptr to a shared_ptr to a subspace::Client.
    auto client_ptr =
            reinterpret_cast<std::shared_ptr<std::shared_ptr<subspace::Client>>*>(client->client);
    delete client_ptr;
    client->client = nullptr;
}

bool subspace_wait_for_subscriber(SubspaceSubscriber subscriber) {
    if (subscriber.subscriber == nullptr) {
        return false;
    }
    // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a subspace::Subscriber.
    auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber>*>(subscriber.subscriber);
    absl::StatusOr<bool> status = (*sub_ptr)->Wait();
    return status.ok() && *status;
}

bool subspace_wait_for_publisher(SubspacePublisher publisher) {
    if (publisher.publisher == nullptr) {
        return false;
    }
    // The publisher contains a pointer to a shared_ptr to a shared_ptr to a subspace::Publisher.
    auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher>*>(publisher.publisher);
    absl::StatusOr<bool> status = (*pub_ptr)->Wait();
    return status.ok() && *status;
}

SubspaceTypeInfo subspace_get_subscriber_type(SubspaceSubscriber subscriber) {
    SubspaceTypeInfo type_info = {.type = nullptr, .type_length = 0};
    if (subscriber.subscriber == nullptr) {
        return type_info;
    }
    // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a subspace::Subscriber.
    auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber>*>(subscriber.subscriber);
    auto type = (*sub_ptr)->TypeView();
    type_info.type = type.data();
    type_info.type_length = type.size();
    return type_info;
}

struct pollfd subspace_get_subscriber_poll_fd(SubspaceSubscriber subscriber) {
    struct pollfd pollFd = {.fd = -1, .events = 0};
    if (subscriber.subscriber == nullptr) {
        return pollFd;
    }
    // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a subspace::Subscriber.
    auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber>*>(subscriber.subscriber);
    pollFd = (*sub_ptr)->GetPollFd();
    return pollFd;
}

struct pollfd subspace_get_publisher_poll_fd(SubspacePublisher publisher) {
    struct pollfd pollFd = {.fd = -1, .events = 0};
    if (publisher.publisher == nullptr) {
        return pollFd;
    }
    // The publisher contains a pointer to a shared_ptr to a shared_ptr to a subspace::Publisher.
    auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher>*>(publisher.publisher);
    pollFd = (*pub_ptr)->GetPollFd();
    return pollFd;
}

int32_t subspace_get_subscriber_slot_size(SubspaceSubscriber subscriber) {
    if (subscriber.subscriber == nullptr) {
        return 0;
    }
    // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a subspace::Subscriber.
    auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber>*>(subscriber.subscriber);
    return (*sub_ptr)->SlotSize();
}
int subspace_get_subscriber_num_slots(SubspaceSubscriber subscriber) {
    if (subscriber.subscriber == nullptr) {
        return 0;
    }
    // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a subspace::Subscriber.
    auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber>*>(subscriber.subscriber);
    return (*sub_ptr)->NumSlots();
}


#if defined(__cplusplus)
}  // extern "C"
#endif
