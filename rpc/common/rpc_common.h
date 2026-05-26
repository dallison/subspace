// Copyright 2023-2026 David Allison
// Asio RPC support is Copyright 2026 Cruise LLC
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "client/client.h"
#include <cstdint>
#include <memory>
#include <string>

namespace subspace {

// Slot parameters for requests and responses for the open/close requests.
constexpr int32_t kRpcRequestSlotSize = 128;
constexpr int32_t kRpcResponseSlotSize = 128;
constexpr int32_t kRpcRequestNumSlots = 100;
constexpr int32_t kRpcResponseNumSlots = 100;

// Default slot parameters for method invocation requests.  The slot size
// can be expanded if the request is bigger.
constexpr int32_t kDefaultMethodSlotSize = 256;
constexpr int32_t kDefaultMethodNumSlots = 100;

constexpr int32_t kCancelChannelSlotSize = 64;
constexpr int32_t kCancelChannelNumSlots = 8;

struct MethodOptions {
  int32_t slot_size = kDefaultMethodSlotSize;
  int32_t num_slots = kDefaultMethodNumSlots;
  int id = -1;
};

namespace client_internal {

struct Method {
  std::string name;
  int id;
  std::string request_type;
  std::string response_type;
  int32_t slot_size;
  int32_t num_slots;
  std::shared_ptr<subspace::Publisher> request_publisher;
  std::shared_ptr<subspace::Subscriber> response_subscriber;
  std::shared_ptr<subspace::Publisher> cancel_publisher;
};

template <typename Response> class ResponseReceiverBase {
public:
  ResponseReceiverBase() = default;
  virtual ~ResponseReceiverBase() = default;

  virtual void OnResponse(Response &&response) = 0;
};

template <> class ResponseReceiverBase<void> {
public:
  ResponseReceiverBase() = default;
  virtual ~ResponseReceiverBase() = default;

  virtual void OnResponse() = 0;
};

} // namespace client_internal
} // namespace subspace
