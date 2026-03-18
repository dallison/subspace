// Copyright 2026 David Allison
// Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#include "rust_client/cross_lang_ffi.h"
#include "client/client.h"
#include "client/message.h"
#include "client/options.h"

#include <cstring>
#include <memory>

extern "C" {

CppClientHandle cpp_test_create_client(const char *socket_name,
                                       const char *name) {
  auto client = std::make_unique<subspace::Client>();
  absl::Status status = client->Init(socket_name, name);
  if (!status.ok()) {
    return nullptr;
  }
  return static_cast<CppClientHandle>(client.release());
}

void cpp_test_destroy_client(CppClientHandle handle) {
  delete static_cast<subspace::Client *>(handle);
}

CppPublisherHandle cpp_test_create_publisher(CppClientHandle client_handle,
                                             const char *channel,
                                             int32_t slot_size, int num_slots,
                                             int32_t checksum_size,
                                             int32_t metadata_size) {
  auto *client = static_cast<subspace::Client *>(client_handle);
  subspace::PublisherOptions opts{
      .slot_size = slot_size,
      .num_slots = num_slots,
      .checksum_size = checksum_size,
      .metadata_size = metadata_size,
  };
  auto status_or = client->CreatePublisher(channel, opts);
  if (!status_or.ok()) {
    return nullptr;
  }
  return static_cast<CppPublisherHandle>(
      new subspace::Publisher(std::move(*status_or)));
}

void cpp_test_destroy_publisher(CppPublisherHandle handle) {
  delete static_cast<subspace::Publisher *>(handle);
}

int64_t cpp_test_publish(CppPublisherHandle pub_handle, const void *payload,
                         size_t payload_len, const void *metadata,
                         size_t metadata_len) {
  auto *pub_ptr = static_cast<subspace::Publisher *>(pub_handle);
  auto status_or_buf = pub_ptr->GetMessageBuffer(payload_len);
  if (!status_or_buf.ok()) {
    return -1;
  }
  std::memcpy(*status_or_buf, payload, payload_len);

  if (metadata != nullptr && metadata_len > 0) {
    auto meta_span = pub_ptr->GetMetadata();
    size_t to_copy = std::min(metadata_len, meta_span.size());
    std::memcpy(meta_span.data(), metadata, to_copy);
  }

  auto status_or_msg = pub_ptr->PublishMessage(payload_len);
  if (!status_or_msg.ok()) {
    return -1;
  }
  return static_cast<int64_t>(status_or_msg->ordinal);
}

CppSubscriberHandle cpp_test_create_subscriber(CppClientHandle client_handle,
                                               const char *channel,
                                               bool checksum) {
  auto *client = static_cast<subspace::Client *>(client_handle);
  subspace::SubscriberOptions opts{};
  opts.checksum = checksum;
  auto status_or = client->CreateSubscriber(channel, opts);
  if (!status_or.ok()) {
    return nullptr;
  }
  return static_cast<CppSubscriberHandle>(
      new subspace::Subscriber(std::move(*status_or)));
}

void cpp_test_destroy_subscriber(CppSubscriberHandle handle) {
  delete static_cast<subspace::Subscriber *>(handle);
}

int cpp_test_subscriber_fd(CppSubscriberHandle handle) {
  auto *sub = static_cast<subspace::Subscriber *>(handle);
  return sub->GetFileDescriptor().Fd();
}

int64_t cpp_test_read_message(CppSubscriberHandle handle, void *payload_out,
                              size_t payload_cap, void *metadata_out,
                              size_t metadata_cap,
                              int32_t *metadata_size_out) {
  auto *sub = static_cast<subspace::Subscriber *>(handle);
  auto status_or = sub->ReadMessage(subspace::ReadMode::kReadNext);
  if (!status_or.ok()) {
    return -1;
  }
  auto &msg = *status_or;
  if (msg.length == 0) {
    if (metadata_size_out != nullptr) {
      *metadata_size_out = sub->MetadataSize();
    }
    return 0;
  }

  size_t to_copy = std::min(msg.length, payload_cap);
  std::memcpy(payload_out, msg.buffer, to_copy);

  int32_t meta_sz = sub->MetadataSize();
  if (metadata_size_out != nullptr) {
    *metadata_size_out = meta_sz;
  }
  if (metadata_out != nullptr && meta_sz > 0) {
    auto meta_span = sub->GetMetadata();
    size_t meta_copy = std::min(static_cast<size_t>(meta_sz), metadata_cap);
    std::memcpy(metadata_out, meta_span.data(), meta_copy);
  }

  return static_cast<int64_t>(msg.length);
}

} // extern "C"
