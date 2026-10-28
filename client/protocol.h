// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_PROTOCOL_H_
#define CLIENT_PROTOCOL_H_

#include "client/options.h"
#include "common/channel.h"
#include "common/client_buffer.h"
#include "proto/subspace.pb.h"

#include <unistd.h>

#include <cstdint>
#include <iterator>
#include <string>

namespace subspace::details {

inline ClientBufferAllocator
ToProtoAllocator(ClientBufferAllocatorKind allocator) {
  switch (allocator) {
  case ClientBufferAllocatorKind::kAndroidMemfd:
    return CLIENT_BUFFER_ALLOCATOR_ANDROID_MEMFD;
  case ClientBufferAllocatorKind::kSplitShm:
    return CLIENT_BUFFER_ALLOCATOR_SPLIT_SHM;
  case ClientBufferAllocatorKind::kSplitCallback:
    return CLIENT_BUFFER_ALLOCATOR_SPLIT_CALLBACK;
  case ClientBufferAllocatorKind::kSplitBufferFreeTest:
    return CLIENT_BUFFER_ALLOCATOR_SPLIT_BUFFER_FREE_TEST;
  case ClientBufferAllocatorKind::kUnspecified:
  default:
    return CLIENT_BUFFER_ALLOCATOR_UNSPECIFIED;
  }
}

inline ClientBufferAllocatorKind
FromProtoAllocator(ClientBufferAllocator allocator) {
  switch (allocator) {
  case CLIENT_BUFFER_ALLOCATOR_ANDROID_MEMFD:
    return ClientBufferAllocatorKind::kAndroidMemfd;
  case CLIENT_BUFFER_ALLOCATOR_SPLIT_SHM:
    return ClientBufferAllocatorKind::kSplitShm;
  case CLIENT_BUFFER_ALLOCATOR_SPLIT_CALLBACK:
    return ClientBufferAllocatorKind::kSplitCallback;
  case CLIENT_BUFFER_ALLOCATOR_SPLIT_BUFFER_FREE_TEST:
    return ClientBufferAllocatorKind::kSplitBufferFreeTest;
  case CLIENT_BUFFER_ALLOCATOR_UNSPECIFIED:
  default:
    return ClientBufferAllocatorKind::kUnspecified;
  }
}

inline void ToProto(const ClientBufferHandleMetadata &metadata,
                    ClientBufferHandleMetadataProto *proto) {
  proto->set_channel_name(metadata.channel_name);
  proto->set_session_id(metadata.session_id);
  proto->set_buffer_index(metadata.buffer_index);
  proto->set_slot_id(metadata.slot_id);
  proto->set_is_prefix(metadata.is_prefix);
  proto->set_full_size(metadata.full_size);
  proto->set_allocation_size(metadata.allocation_size);
  proto->set_handle(static_cast<uint64_t>(metadata.handle));
  proto->set_shadow_file(metadata.shadow_file);
  proto->set_object_name(metadata.object_name);
  proto->set_allocator(ToProtoAllocator(metadata.allocator));
  proto->set_map_offset(metadata.map_offset);
}

inline ClientBufferHandleMetadata
FromProto(const ClientBufferHandleMetadataProto &proto) {
  return {
      .channel_name = proto.channel_name(),
      .session_id = proto.session_id(),
      .buffer_index = proto.buffer_index(),
      .slot_id = proto.slot_id(),
      .is_prefix = proto.is_prefix(),
      .full_size = proto.full_size(),
      .allocation_size = proto.allocation_size(),
      .handle = static_cast<uintptr_t>(proto.handle()),
      .shadow_file = proto.shadow_file(),
      .object_name = proto.object_name(),
      .allocator = FromProtoAllocator(proto.allocator()),
      .map_offset = proto.map_offset(),
  };
}

inline void FillCreatePublisherRequest(CreatePublisherRequest *request,
                                       const std::string &channel_name,
                                       const PublisherOptions &options,
                                       int publisher_id) {
  request->set_channel_name(channel_name);
  request->set_slot_size(Aligned(options.slot_size));
  request->set_num_slots(options.num_slots);
  request->set_is_local(options.IsLocal());
  request->set_is_reliable(options.IsReliable());
  request->set_is_bridge(options.IsBridge());
  request->set_for_tunnel(options.ForTunnel());
  request->set_is_fixed_size(options.IsFixedSize());
  request->set_type(options.Type());
  request->set_mux(options.Mux());
  request->set_vchan_id(options.VchanId());
  request->set_notify_retirement(options.notify_retirement);
  request->set_checksum_size(options.ChecksumSize());
  request->set_metadata_size(options.MetadataSize());
  request->set_publisher_id(publisher_id);
  request->set_max_publishers(options.MaxPublishers());
  request->set_use_split_buffers(options.UseSplitBuffers());
  request->set_split_buffers_over_bridge(options.SplitBuffersOverBridge());
  request->set_subscriber_queue_arena_size(options.SubscriberQueueArenaSize());
  request->set_process_id(static_cast<uint64_t>(getpid()));
  request->set_max_outstanding_slot_leases(options.MaxOutstandingSlotLeases());
  request->set_max_slot_size(
      options.max_slot_size > 0 ? Aligned(options.max_slot_size) : 0);
}

inline void FillCreateSubscriberRequest(CreateSubscriberRequest *request,
                                        const std::string &channel_name,
                                        const SubscriberOptions &options,
                                        int subscriber_id) {
  request->set_channel_name(channel_name);
  request->set_subscriber_id(subscriber_id);
  request->set_is_reliable(options.IsReliable());
  request->set_is_bridge(options.IsBridge());
  request->set_for_tunnel(options.ForTunnel());
  request->set_telemetry(options.Telemetry());
  request->set_type(options.Type());
  request->set_max_active_messages(options.MaxActiveMessages());
  request->set_max_subscribers(options.MaxSubscribers());
  request->set_mux(options.Mux());
  request->set_vchan_id(options.VchanId());
  request->set_subscriber_queue_size(options.SubscriberQueueSize());
  request->set_process_id(static_cast<uint64_t>(getpid()));
  request->set_is_local(options.IsLocal());
}

inline uint64_t ExpandSlotSize(uint64_t slot_size) {
  constexpr double multipliers[] = {2.0, 1.5, 1.25, 1.125, 1.0625, 1.03125};
  constexpr uint64_t ranges[] = {4096, 16384, 65536, 262144, 1048576};
  size_t index = 0;
  while (index < std::size(ranges) && slot_size > ranges[index]) {
    ++index;
  }
  return Aligned(static_cast<uint64_t>(slot_size * multipliers[index]));
}

} // namespace subspace::details

#endif // CLIENT_PROTOCOL_H_
