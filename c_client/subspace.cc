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

thread_local SubspaceError subspace_error = {.error_message = ""};

void subspace_clear_error(void) { subspace_error.error_message[0] = '\0'; }

void subspace_set_error(const char *error_message) {
  if (error_message == nullptr) {
    return;
  }
  strncpy(subspace_error.error_message, error_message,
          sizeof(subspace_error.error_message) - 1);
}

char *subspace_get_last_error(void) { return subspace_error.error_message; }

bool subspace_has_error(void) {
  return subspace_error.error_message[0] != '\0';
}

// The SubspaceClient struct contains a pointer to a
// std::shared_ptr<subspace::Client>.

SubspaceClient subspace_create_client(void) {
  return subspace_create_client_with_socket_and_name("/tmp/subspace", "");
}
SubspaceClient subspace_create_client_with_socket(const char *socket_name) {
  return subspace_create_client_with_socket_and_name(socket_name, "");
}

SubspaceClient
subspace_create_client_with_socket_and_name(const char *socket_name,
                                            const char *client_name) {
  subspace_clear_error();
  SubspaceClient client;
  memset(&client, 0, sizeof(client));
  std::shared_ptr<subspace::Client> sclient =
      std::make_shared<subspace::Client>();
  absl::Status status = sclient->Init(socket_name, client_name);
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return client;
  }
  // Copy the shared ptr to the heap.
  client.client = reinterpret_cast<void *>(
      new std::shared_ptr<subspace::Client>(std::move(sclient)));
  return client;
}

SubspaceSubscriberOptions subspace_subscriber_options_default(void) {
  SubspaceSubscriberOptions options = {
      .reliable = false,
      .type = {.type = nullptr, .type_length = 0},
      .max_active_messages = 1,
      .pass_activation = false,
      .log_dropped_messages = false,
  };
  return options;
}

SubspacePublisherOptions subspace_publisher_options_default(int32_t slot_size,
                                                            int num_slots) {
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

SubspaceSubscriber
subspace_create_subscriber(SubspaceClient client, const char *channel_name,
                           SubspaceSubscriberOptions options) {
  subspace::SubscriberOptions subspace_options = {
      .reliable = options.reliable,
      .type = std::string(options.type.type, options.type.type_length),
      .max_active_messages = options.max_active_messages,
      .pass_activation = options.pass_activation,
      .log_dropped_messages = options.log_dropped_messages,
  };
  subspace_clear_error();
  SubspaceSubscriber subscriber;
  memset(&subscriber, 0, sizeof(subscriber));
  std::shared_ptr<subspace::Client> *client_ptr =
      reinterpret_cast<std::shared_ptr<subspace::Client> *>(client.client);
  absl::StatusOr<subspace::Subscriber> status_or_sub =
      (*client_ptr)->CreateSubscriber(channel_name, subspace_options);
  if (!status_or_sub.ok()) {
    subspace_set_error(status_or_sub.status().ToString().c_str());
    return subscriber;
  };

  subspace::Subscriber *sub_ptr =
      new subspace::Subscriber(std::move(*status_or_sub));
  subscriber.subscriber = reinterpret_cast<void *>(
      new std::shared_ptr<subspace::Subscriber>(sub_ptr));
  return subscriber;
}

SubspacePublisher subspace_create_publisher(SubspaceClient client,
                                            const char *channel_name,
                                            SubspacePublisherOptions options) {
  subspace::PublisherOptions subspace_options = {
      .slot_size = options.slot_size,
      .num_slots = options.num_slots,
      .reliable = options.reliable,
      .fixed_size = options.fixed_size,
      .type = std::string(options.type.type, options.type.type_length),
      .activate = options.activate,
  };
  subspace_clear_error();
  SubspacePublisher publisher;
  memset(&publisher, 0, sizeof(publisher));
  std::shared_ptr<subspace::Client> *client_ptr =
      reinterpret_cast<std::shared_ptr<subspace::Client> *>(client.client);
  absl::StatusOr<subspace::Publisher> status_or_pub =
      (*client_ptr)->CreatePublisher(channel_name, subspace_options);
  if (!status_or_pub.ok()) {
    subspace_set_error(status_or_pub.status().ToString().c_str());
    return publisher;
  }
  subspace::Publisher *pub_ptr =
      new subspace::Publisher(std::move(*status_or_pub));
  publisher.publisher = reinterpret_cast<void *>(
      new std::shared_ptr<subspace::Publisher>(pub_ptr));
  return publisher;
}

SubspaceMessage subspace_read_message_with_mode(SubspaceSubscriber subscriber,
                                                SubspaceReadMode mode) {
  SubspaceMessage message = {.message = nullptr,
                             .length = 0,
                             .buffer = nullptr,
                             .ordinal = uint64_t(-1),
                             .timestamp = 0,
                             .is_activation = false};
  if (subscriber.subscriber == nullptr) {
    return message;
  }
  subspace_clear_error();
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  subspace::ReadMode readMode = (mode == kSubspaceReadNewest)
                                    ? subspace::ReadMode::kReadNewest
                                    : subspace::ReadMode::kReadNext;
  absl::StatusOr<subspace::Message> status_or_msg =
      (*sub_ptr)->ReadMessage(readMode);
  if (!status_or_msg.ok()) {
    subspace_set_error(status_or_msg.status().ToString().c_str());
    return message;
  }
  // Take ownership of the message.
  subspace::Message *msg = new subspace::Message(std::move(*status_or_msg));
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

bool subspace_free_message(SubspaceMessage *message) {
  subspace_clear_error();
  if (message->message == nullptr) {
    subspace_set_error("Invalid message parameter");
    return false;
  }
  std::shared_ptr<subspace::Message> *msg_ptr =
      reinterpret_cast<std::shared_ptr<subspace::Message> *>(message->message);
  delete msg_ptr;
  message->message = nullptr;
  message->length = 0;
  message->buffer = nullptr;
  message->ordinal = uint64_t(-1);
  message->timestamp = 0;
  message->is_activation = false;
  return true;
}

bool subspace_register_subscriber_callback(SubspaceSubscriber subscriber,
                                           void (*callback)(SubspaceSubscriber,
                                                            SubspaceMessage)) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback parameter");
    return false;
  }
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);

  absl::Status status = (*sub_ptr)->RegisterMessageCallback(
      [subscriber, callback](const subspace::Subscriber *,
                             subspace::Message msg) {
        // This takes ownership of the message.  It must be freed by calline
        // subspace_free_message. by the callback function.
        SubspaceMessage subspace_msg = {
            .message = new std::shared_ptr<subspace::Message>(
                new subspace::Message(std::move(msg))),
            .length = msg.length,
            .buffer = msg.buffer,
            .ordinal = msg.ordinal,
            .timestamp = msg.timestamp,
            .is_activation = msg.is_activation};
        callback(subscriber, subspace_msg);
      });
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_unregister_subscriber_callback(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  subspace_clear_error();
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  auto status = (*sub_ptr)->UnregisterMessageCallback();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_register_dropped_message_callback(
    SubspaceSubscriber subscriber,
    void (*callback)(SubspaceSubscriber, int64_t)) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback parameter");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->RegisterDroppedMessageCallback(
      [subscriber, callback](const subspace::Subscriber *, int64_t count) {
        callback(subscriber, count);
      });
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

// Unregister the callback and return true is successful.
bool subspace_remove_dropped_message_callback(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber parameter");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->UnregisterDroppedMessageCallback();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_process_all_messages(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->ProcessAllMessages();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

SubspaceMessageBuffer subspace_get_message_buffer(SubspacePublisher publisher,
                                                  size_t max_size) {
  subspace_clear_error();
  SubspaceMessageBuffer message_buffer;
  memset(&message_buffer, 0, sizeof(message_buffer));
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher ");
    return message_buffer;
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::StatusOr<void *> status_or_buffer =
      (*pub_ptr)->GetMessageBuffer(max_size);
  if (!status_or_buffer.ok()) {
    subspace_set_error(status_or_buffer.status().ToString().c_str());
    return message_buffer;
  }
  auto buffer = std::move(*status_or_buffer);
  message_buffer.buffer = buffer;
  message_buffer.buffer_size = max_size;
  return message_buffer;
}

const SubspaceMessage subspace_publish_message(SubspacePublisher publisher,
                                               size_t message_size) {
  subspace_clear_error();
  SubspaceMessage published_message = {
      .length = 0, .ordinal = uint64_t(-1), .timestamp = 0};
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
  }
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::StatusOr<const subspace::Message> status_or_msg =
      (*pub_ptr)->PublishMessage(message_size);
  if (!status_or_msg.ok()) {
    subspace_set_error(status_or_msg.status().ToString().c_str());
    return published_message;
  }
  auto msg = *status_or_msg;
  published_message.length = msg.length;
  published_message.ordinal = msg.ordinal;
  published_message.timestamp = msg.timestamp;
  return published_message;
}

bool subspace_remove_subscriber(SubspaceSubscriber *subscriber) {
  subspace_clear_error();
  if (subscriber->subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber->subscriber);
  delete sub_ptr;
  subscriber->subscriber = nullptr;
  return true;
}

bool subspace_remove_publisher(SubspacePublisher *publisher) {
  if (publisher->publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher->publisher);
  delete pub_ptr;
  publisher->publisher = nullptr;
  return true;
}

bool subspace_remove_client(SubspaceClient *client) {
  subspace_clear_error();
  if (client->client == nullptr) {
    subspace_set_error("Invalid client");
    return false;
  }
  // The client contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Client.
  auto client_ptr =
      reinterpret_cast<std::shared_ptr<std::shared_ptr<subspace::Client>> *>(
          client->client);
  delete client_ptr;
  client->client = nullptr;
  return true;
}

bool subspace_wait_for_subscriber(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return false;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::Status status = (*sub_ptr)->Wait();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

int subspace_wait_for_subscriber_with_fd(SubspaceSubscriber subscriber, int fd) {
  subspace_clear_error();
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return -1;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  absl::StatusOr<int> status = (*sub_ptr)->Wait(toolbelt::FileDescriptor(fd));
  if (!status.ok()) {
    subspace_set_error(status.status().ToString().c_str());
    return false;
  }
  return *status;
}

bool subspace_wait_for_publisher(SubspacePublisher publisher) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::Status status = (*pub_ptr)->Wait();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

int subspace_wait_for_publisher_with_fd(SubspacePublisher publisher, int fd) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::StatusOr<int> status = (*pub_ptr)->Wait(toolbelt::FileDescriptor(fd));
  if (!status.ok()) {
    subspace_set_error(status.status().ToString().c_str());
    return false;
  }
  return *status;
}

bool subspace_register_resize_callback(SubspacePublisher publisher,
                                       bool (*callback)(SubspacePublisher,
                                                        int32_t, int32_t)) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  if (callback == nullptr) {
    subspace_set_error("Invalid callback");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::Status status = (*pub_ptr)->RegisterResizeCallback(
      [publisher, callback](subspace::Publisher *pub, int32_t old_size,
                            int32_t new_size) -> absl::Status {
        // Call the callback with the publisher and the old and new sizes.
        bool ok = callback(publisher, old_size, new_size);
        return ok ? absl::OkStatus()
                  : absl::InternalError("Resize callback prevented resize");
      });
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

bool subspace_unregister_resize_callback(SubspacePublisher publisher) {
  subspace_clear_error();
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return false;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  absl::Status status = (*pub_ptr)->UnregisterResizeCallback();
  if (!status.ok()) {
    subspace_set_error(status.ToString().c_str());
    return false;
  }
  return true;
}

SubspaceTypeInfo subspace_get_subscriber_type(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  SubspaceTypeInfo type_info = {.type = nullptr, .type_length = 0};
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return type_info;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  auto type = (*sub_ptr)->TypeView();
  type_info.type = type.data();
  type_info.type_length = type.size();
  return type_info;
}

struct pollfd subspace_get_subscriber_poll_fd(SubspaceSubscriber subscriber) {
  subspace_clear_error();
  struct pollfd pollFd = {.fd = -1, .events = 0};
  if (subscriber.subscriber == nullptr) {
    subspace_set_error("Invalid subscriber");
    return pollFd;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  pollFd = (*sub_ptr)->GetPollFd();
  return pollFd;
}

struct pollfd subspace_get_publisher_poll_fd(SubspacePublisher publisher) {
  subspace_clear_error();
  struct pollfd pollFd = {.fd = -1, .events = 0};
  if (publisher.publisher == nullptr) {
    subspace_set_error("Invalid publisher");
    return pollFd;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  pollFd = (*pub_ptr)->GetPollFd();
  return pollFd;
}

int subspace_get_publisher_fd(SubspacePublisher publisher) {
  if (publisher.publisher == nullptr) {
    return -1;
  }
  // The publisher contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Publisher.
  auto pub_ptr = reinterpret_cast<std::shared_ptr<subspace::Publisher> *>(
      publisher.publisher);
  return (*pub_ptr)->GetFileDescriptor().Fd();
}

int subspace_get_subscriber_fd(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return -1;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->GetFileDescriptor().Fd();
}

int32_t subspace_get_subscriber_slot_size(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->SlotSize();
}

int subspace_get_subscriber_num_slots(SubspaceSubscriber subscriber) {
  if (subscriber.subscriber == nullptr) {
    return 0;
  }
  // The subscriber contains a pointer to a shared_ptr to a shared_ptr to a
  // subspace::Subscriber.
  auto sub_ptr = reinterpret_cast<std::shared_ptr<subspace::Subscriber> *>(
      subscriber.subscriber);
  return (*sub_ptr)->NumSlots();
}

#if defined(__cplusplus)
} // extern "C"
#endif
