// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_TYPES_H_
#define CLIENT_TYPES_H_

#include <cstdint>
#include <string>

namespace subspace {

enum class ReadMode {
  kReadNext,
  kReadNewest,
};

// Controls whether ReadMessage consumes the subscriber trigger fd (eventfd
// or pipe). The default, kClearTrigger, reads the fd so a later poll or wait
// blocks until a new message is published. kNoClearTrigger leaves the fd
// unread for callers that manage it from an external event loop.
enum class ClearTrigger {
  kClearTrigger,
  kNoClearTrigger,
};

struct ChannelInfo {
  std::string channel_name;
  int num_publishers;
  int num_subscribers;
  int num_bridge_pubs;
  int num_bridge_subs;
  int num_tunnel_pubs;
  int num_tunnel_subs;
  std::string type;
  uint64_t slot_size;
  int num_slots;
  int subscriber_queue_size;
  uint64_t subscriber_queue_arena_size;
  bool reliable;
  bool is_local = false;
};

struct ChannelStats {
  std::string channel_name;
  uint64_t total_bytes;
  uint64_t total_messages;
  uint64_t max_message_size;
  bool is_local = false;
};

} // namespace subspace

#endif // CLIENT_TYPES_H_
