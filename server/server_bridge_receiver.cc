// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "proto/subspace.pb.h"
#include "toolbelt/sockets.h"
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace subspace {

struct BridgeReceiveTarget {
  MessagePrefix *prefix = nullptr;
  char *prefix_without_padding = nullptr;
  size_t prefix_length = 0;
  void *payload = nullptr;
};

struct BridgeReceivedMessage {
  MessagePrefix *prefix = nullptr;
  size_t payload_length = 0;
};

static absl::StatusOr<size_t>
ReceiveSplitBridgeMessage(async::Context ctx, async::StreamSocket &bridge,
                          const Subscribed &subscribed,
                          const BridgeReceiveTarget &target) {
  absl::StatusOr<ssize_t> prefix = bridge.ReceiveMessage(
      target.prefix_without_padding, target.prefix_length, ctx);
  if (!prefix.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge prefix: %s", prefix.status().ToString()));
  }
  if (static_cast<size_t>(*prefix) != target.prefix_length) {
    return absl::InternalError(
        absl::StrFormat("bridge prefix has invalid length %zd, expected %zu",
                        *prefix, target.prefix_length));
  }
  if (target.prefix->message_size >
      static_cast<uint64_t>(subscribed.slot_size())) {
    return absl::InternalError(absl::StrFormat(
        "bridge payload is too large: %llu > %d",
        static_cast<unsigned long long>(target.prefix->message_size),
        subscribed.slot_size()));
  }

  absl::StatusOr<ssize_t> payload =
      bridge.ReceiveMessage(static_cast<char *>(target.payload),
                            static_cast<size_t>(target.prefix->message_size),
                            ctx);
  if (!payload.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge payload: %s", payload.status().ToString()));
  }
  size_t payload_length = static_cast<size_t>(*payload);
  if (payload_length != static_cast<size_t>(target.prefix->message_size)) {
    return absl::InternalError(absl::StrFormat(
        "bridge payload has invalid length %zu, expected %llu", payload_length,
        static_cast<unsigned long long>(target.prefix->message_size)));
  }
  return payload_length;
}

static absl::StatusOr<size_t>
ReceiveCombinedBridgeMessageForSplitPublisher(async::Context ctx,
                                              async::StreamSocket &bridge,
                                              const Subscribed &subscribed,
                                              const BridgeReceiveTarget &target) {
  std::vector<char> combined(subscribed.slot_size() + target.prefix_length);
  absl::StatusOr<ssize_t> n =
      bridge.ReceiveMessage(combined.data(), combined.size(), ctx);
  if (!n.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge message: %s", n.status().ToString()));
  }
  if (static_cast<size_t>(*n) < target.prefix_length) {
    return absl::InternalError(
        absl::StrFormat("bridge message is too short: %zd < %zu", *n,
                        target.prefix_length));
  }
  memcpy(target.prefix_without_padding, combined.data(), target.prefix_length);
  size_t payload_length = static_cast<size_t>(*n) - target.prefix_length;
  memcpy(target.payload, combined.data() + target.prefix_length,
         payload_length);
  return payload_length;
}

static absl::StatusOr<size_t>
ReceiveCombinedBridgeMessage(async::Context ctx, async::StreamSocket &bridge,
                             const Subscribed &subscribed,
                             const BridgeReceiveTarget &target) {
  absl::StatusOr<ssize_t> n = bridge.ReceiveMessage(
      target.prefix_without_padding,
      subscribed.slot_size() + target.prefix_length, ctx);
  if (!n.ok()) {
    return absl::InternalError(absl::StrFormat(
        "failed to read bridge message: %s", n.status().ToString()));
  }
  if (static_cast<size_t>(*n) < target.prefix_length) {
    return absl::InternalError(absl::StrFormat(
        "bridge message is too short: %zd < %zu", *n, target.prefix_length));
  }
  return static_cast<size_t>(*n) - target.prefix_length;
}

static absl::StatusOr<BridgeReceivedMessage>
ReceiveBridgeMessage(async::Context ctx, async::StreamSocket &bridge,
                     Publisher &pub, const Subscribed &subscribed,
                     void *payload_buffer) {
  // The MessagePrefix struct contains 4 bytes of padding at offset 0. This is
  // to allow SendMessage to use it for the length in the combined wire format.
  size_t prefix_area = pub.PrefixSize();
  BridgeReceiveTarget target = {
      .prefix = pub.Prefix(),
      .prefix_without_padding = nullptr,
      .prefix_length = prefix_area - sizeof(int32_t),
      .payload = payload_buffer,
  };
  if (target.prefix == nullptr) {
    return absl::InternalError("failed to find message prefix");
  }
  char *prefix_addr = reinterpret_cast<char *>(target.prefix);
  target.prefix_without_padding = prefix_addr + sizeof(int32_t);

  if (subscribed.split_buffers()) {
    absl::StatusOr<size_t> payload_length =
        ReceiveSplitBridgeMessage(ctx, bridge, subscribed, target);
    if (!payload_length.ok()) {
      return payload_length.status();
    }
    return BridgeReceivedMessage{.prefix = target.prefix,
                                 .payload_length = *payload_length};
  }
  if (pub.UsesSplitBuffers()) {
    absl::StatusOr<size_t> payload_length =
        ReceiveCombinedBridgeMessageForSplitPublisher(ctx, bridge, subscribed,
                                                      target);
    if (!payload_length.ok()) {
      return payload_length.status();
    }
    return BridgeReceivedMessage{.prefix = target.prefix,
                                 .payload_length = *payload_length};
  }
  absl::StatusOr<size_t> payload_length =
      ReceiveCombinedBridgeMessage(ctx, bridge, subscribed, target);
  if (!payload_length.ok()) {
    return payload_length.status();
  }
  return BridgeReceivedMessage{.prefix = target.prefix,
                               .payload_length = *payload_length};
}

// Send a Subscribe message over UDP.
absl::Status
Server::SendSubscribeMessage(const std::string &channel_name, bool reliable,
                             toolbelt::InetAddress publisher,
                             async::StreamSocket &receiver_listener,
                             char *buffer, size_t buffer_size,
                             async::Context ctx) {
  const toolbelt::SocketAddress &receiver_addr =
      receiver_listener.BoundAddress();
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Sending subscribe with bridge receiver socket: %s",
              receiver_addr.ToString().c_str());
  // Send a subscribe request to the publisher.
  Discovery disc;
  disc.set_server_id(server_id_);
  auto *sub = disc.mutable_subscribe();
  sub->set_channel_name(channel_name);
  sub->set_reliable(reliable);
  auto *sub_addr = sub->mutable_receiver();
  switch (receiver_addr.Type()) {
  case toolbelt::SocketAddress::kAddressInet: {
    // IPv4 address.  Advertise the configured bridge address if set, so a
    // NAT'd peer can reach us via a forwarded loopback endpoint.
    in_addr ip_addr = bridge_advertise_address_.Valid()
                          ? bridge_advertise_address_.IpAddress()
                          : receiver_addr.GetInetAddress().IpAddress();
    sub_addr->set_address(&ip_addr, sizeof(ip_addr));
    sub_addr->set_family(ChannelAddress::FAMILY_INET);
    break;
  }
  case toolbelt::SocketAddress::kAddressVirtual: {
    // vsock: advertise our configured local CID (the listener is bound to
    // VMADDR_CID_ANY, so its own CID is not the one peers should dial).
    uint32_t cid = vsock_cid_;
    sub_addr->set_address(&cid, sizeof(cid));
    sub_addr->set_family(ChannelAddress::FAMILY_VSOCK);
    break;
  }
  default:
    return absl::InternalError(
        absl::StrFormat("Unsupported address type for subscribe: %s",
                        receiver_addr.ToString().c_str()));
  }
  sub_addr->set_port(receiver_addr.Port());

  (void)buffer;
  (void)buffer_size;
  logger_.Log(toolbelt::LogLevel::kDebug, "Sending subscribe to %s: %s",
              publisher.ToString().c_str(), disc.DebugString().c_str());
  absl::Status s = TransmitDiscovery(disc, publisher, ctx);
  if (!s.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to send subscribe: %s", s.ToString()));
  }
  return absl::OkStatus();
}

// This coroutine receives messages on a TCP socket and publishes
// them to a local channel.  The messages contain the prefix which
// is sent intact to the channel.  It first receives a 'Subscribed' message
// with the channel details, then enters a loop reading message from the
// bridge and publishing them to the local channel.
void Server::BridgeReceiverCoroutine(async::Context ctx,
                                     std::string channel_name,
                                     bool sub_reliable,
                                     bool split_buffers_over_bridge,
                                     toolbelt::InetAddress publisher,
                                     bool local_has_split_options,
                                     bool local_use_split_buffers) {
  // bridged_publishers_ and channels_ are owned by the client strand, but this
  // coroutine runs on the parallel io_context.  Post the teardown bookkeeping
  // onto the strand (capturing everything by value, since the guard's members
  // are gone by the time the posted work runs) so it is serialized with
  // discovery and the client handlers.
  struct BridgeGuard {
    Server *server;
    std::string channel_name;
    toolbelt::InetAddress publisher;
    bool sub_reliable;
    ~BridgeGuard() {
      server->runtime_.RunOnStrand(
          [server = server, channel_name = channel_name, publisher = publisher,
           sub_reliable = sub_reliable]() {
            auto it = server->channels_.find(channel_name);
            if (it != server->channels_.end()) {
              it->second->RemoveBridgedAddress(publisher, sub_reliable);
            }
          });
    }
  } bridge_guard{this, channel_name, publisher, sub_reliable};

  // Open a listening TCP socket for the incoming bridge connection.
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeReceiverCoroutine running");
  char buffer[kDiscoveryBufferSize];

  async::StreamSocket receiver_listener;
  absl::Status s = BindBridgeListener(receiver_listener);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Unable to bind socket for bridge receiver for %s: %s",
                channel_name.c_str(), s.ToString().c_str());
    return;
  }
  const toolbelt::SocketAddress &receiver_addr =
      receiver_listener.BoundAddress();
  logger_.Log(toolbelt::LogLevel::kDebug, "Bridge receiver socket: %s",
              receiver_addr.ToString().c_str());

  s = SendSubscribeMessage(channel_name, sub_reliable, publisher,
                           receiver_listener, buffer, sizeof(buffer), ctx);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send Subscribe message for channel %s: %s",
                channel_name.c_str(), s.ToString().c_str());
    return;
  }

  // Accept connection on listen socket.
  absl::StatusOr<async::StreamSocket> bridge =
      receiver_listener.Accept(ctx);
  if (!bridge.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to accept incoming bridge connection: %s",
                bridge.status().ToString().c_str());
    return;
  }

  // Wait for the Subscribed message from the server.
  Subscribed subscribed;
  // Recieve it into offset 3 in buffer so that we can write the length for
  // sending it out again to the bridge notification pipe.
  absl::StatusOr<ssize_t> n_recv = bridge->ReceiveMessage(
      buffer + sizeof(int32_t), sizeof(buffer) - sizeof(int32_t), ctx);
  if (!n_recv.ok()) {
    logger_.Log(toolbelt::LogLevel::kError, "Failed to receive Subscribed: %s",
                n_recv.status().ToString().c_str());
    return;
  }
  if (!subscribed.ParseFromArray(buffer + sizeof(int32_t),
                                 static_cast<int>(*n_recv))) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to parse Subscribed message");
    return;
  }
  bool bridge_publisher_split_buffers =
      split_buffers_over_bridge || subscribed.split_buffers_over_bridge();
  // The local channel's split-buffer preference was read on the client strand
  // (in SubscribeOverBridge) and passed in; it overrides the wire value.  We
  // must not look channels_ up here because this coroutine runs on the parallel
  // io_context.
  if (local_has_split_options) {
    bridge_publisher_split_buffers = local_use_split_buffers;
  }

  // Build a publisher to publish incoming bridge messages to the channel.
  Client client(ctx);
  s = client.Init(socket_name_);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to Subspace server: %s",
                s.ToString().c_str());
    return;
  }

  std::shared_ptr<async::StreamSocket> retirement_transmitter;
  struct CloseRetirementTransmitterOnExit {
    std::shared_ptr<async::StreamSocket> &socket;
    ~CloseRetirementTransmitterOnExit() {
      if (socket != nullptr) {
        socket->Close();
      }
    }
  } close_retirement_transmitter_on_exit{retirement_transmitter};

  if (subscribed.notify_retirement()) {
    retirement_transmitter = std::make_shared<async::StreamSocket>();
    // The address family travels in the message (see SendSubscribeMessage);
    // unset defaults to FAMILY_INET for compatibility with older peers.
    absl::StatusOr<toolbelt::SocketAddress> retirement_addr =
        ParseChannelAddress(subscribed.retirement_socket(), "retirement socket");
    if (!retirement_addr.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Invalid retirement socket for %s: %s", channel_name.c_str(),
                  retirement_addr.status().ToString().c_str());
      return;
    }

    s = retirement_transmitter->Connect(*retirement_addr);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to connect to retirement socket for %s: %s",
                  channel_name.c_str(), s.ToString().c_str());
      return;
    }
  }
  // For a reliable publisher, this will send an activation message
  // to the local channel.
  int32_t bridge_cs = subscribed.checksum_size();
  int32_t bridge_ms = subscribed.metadata_size();
  if (bridge_cs <= 0) {
    bridge_cs = 4;
  }
  if (bridge_ms < 0) {
    bridge_ms = 0;
  }
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      channel_name, subscribed.slot_size(), subscribed.num_slots(),
      PublisherOptions()
          .SetSubscriberQueueArenaSize(
              subscribed.subscriber_queue_arena_size())
          .SetReliable(subscribed.reliable())
          .SetBridge(true)
          .SetNotifyRetirement(subscribed.notify_retirement())
          .SetChecksumSize(bridge_cs)
          .SetMetadataSize(bridge_ms)
          .SetUseSplitBuffers(bridge_publisher_split_buffers));
  if (!pub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create bridge publisher for %s: %s",
                channel_name.c_str(), pub.status().ToString().c_str());
    return;
  }

  toolbelt::FileDescriptor retirement_fd;
  if (subscribed.notify_retirement()) {
    // We need to send the retirement fd to the publisher.
    // This is used to notify us when a slot is retired.
    retirement_fd = pub->GetRetirementFd();
    if (!retirement_fd.Valid()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get retirement fd for %s", channel_name.c_str());
      return;
    }

    // Add a coroutine to listen for retirement notifications and send them to
    // the socket connected to the other server.
    runtime_.SpawnOnNewStrand(
        [this, retirement_fd = std::move(retirement_fd),
         retirement_transmitter,
         channel_name](async::Context ret_ctx) mutable {
          RetirementCoroutine(ret_ctx, channel_name, std::move(retirement_fd),
                              retirement_transmitter);
        },
        {.name = absl::StrFormat("Retirement notifier for %s", channel_name),
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }

  if (bridge_notification_pipe_.WriteFd().Valid()) {
    // Also send the Subscribed message to the bridge notification pipe.  This
    // is sent without a coroutine.
    // TODO: use a coroutine to avoid blocking?
    // We send the whole buffer including the 4-byte length.
    // Write the length in big endian.
    buffer[0] = (*n_recv >> 24) & 0xff;
    buffer[1] = (*n_recv >> 16) & 0xff;
    buffer[2] = (*n_recv >> 8) & 0xff;
    buffer[3] = *n_recv & 0xff;
    if (absl::StatusOr<ssize_t> sz_or =
            bridge_notification_pipe_.WriteFd().Write(
                buffer, *n_recv + sizeof(int32_t));
        !sz_or.ok()) {
      logger_.Log(
          toolbelt::LogLevel::kError,
          "Failed to send subscribed message to bridge notification pipe: %s",
          sz_or.status().ToString().c_str());
      // Ignore the error.
    }
  }

  // Now we receive messages from the bridge and send to the
  // publisher.
  for (;;) {
    absl::StatusOr<void *> buf = pub->GetMessageBuffer();
    if (!buf.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get buffer for bridge subscriber %s: %s",
                  channel_name.c_str(), buf.status().ToString().c_str());
      break;
    }
    if (*buf == nullptr) {
      // Can only happen if publisher is reliable.
      if (!pub->IsReliable()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Got null buffer for unreliable publisher");
        break;
      }
      // Wait for a buffer to become available and try again.
      //
      // Since we don't read the network until we have a place to
      // put the message, we will apply backpressure to the transmitter
      // through buffering in the TCP stack.  When the transmitter's
      // buffers fill up, it will stop sending until we can read the
      // message.  The backpressure is thus propagated up the
      // chain.
      if (absl::Status status = pub->Wait(); !status.ok()) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to wait for reliable publisher: %s",
                    status.ToString().c_str());
        break;
      }
      continue;
    }

    absl::StatusOr<BridgeReceivedMessage> bridge_msg =
        ReceiveBridgeMessage(ctx, *bridge, *pub, subscribed, *buf);
    if (!bridge_msg.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to read bridge message for %s: %s",
                  channel_name.c_str(), bridge_msg.status().ToString().c_str());
      break;
    }

    // Set the kMessageBridged flag in the prefix so that this message isn't
    // forwarded again over a bridge.
    MessagePrefix *prefix = bridge_msg->prefix;
    if ((prefix->flags & kMessageActivate) != 0) {
      // Since we have created a reliable publisher and it has sent an
      // activation message through, we don't send another one.
      continue;
    }
    prefix->flags |= kMessageBridged;

    absl::StatusOr<const Message> pub_msg = pub->PublishMessageInternal(
        bridge_msg->payload_length, /*omit_prefix=*/true,
        /*omit_prefix_slot_id=*/true);
    if (!pub_msg.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish bridge message for %s: %s",
                  channel_name.c_str(), pub_msg.status().ToString().c_str());
    }
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Bridge receiver coroutine terminating");
}

// This coroutine receives retirement notifications from the bridge publisher
// and sends those over the bridge to the server that originally published the
// message.  The slot ID sent back to the original server is the slot of the
// original message, not this side's slot.  That is held in the MessagePrefix
// which is kept intact.
void Server::RetirementCoroutine(
    async::Context ctx, const std::string &channel_name,
    toolbelt::FileDescriptor &&retirement_fd,
    std::shared_ptr<async::StreamSocket> retirement_transmitter) {
  logger_.Log(toolbelt::LogLevel::kDebug, "Retirement coroutine for %s running",
              channel_name.c_str());
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
  // The retirement fd is read on the io_context thread, so the read must never
  // block it.  WaitReadable can report readiness that this reader cannot then
  // satisfy with a blocking read - e.g. a spurious epoll edge, or a second
  // retirement coroutine spawned for the same channel after the bridge
  // re-establishes consuming the byte first.  A blocking read in that case
  // would freeze the entire single-threaded server (every client handler,
  // discovery and bridge coroutine stalls behind it), which manifested as an
  // intermittent hang during client teardown.  Make the fd non-blocking and
  // treat EAGAIN as "wait again", exactly like the socket facade does.
  if (int fl = ::fcntl(retirement_fd.Fd(), F_GETFL, 0); fl >= 0) {
    (void)::fcntl(retirement_fd.Fd(), F_SETFL, fl | O_NONBLOCK);
  }
#endif
  for (;;) {
    int32_t slot_id;
#if SUBSPACE_CORO_BACKEND == SUBSPACE_CORO_BACKEND_ASIO
    absl::StatusOr<ssize_t> n;
    for (;;) {
      ssize_t r = ::read(retirement_fd.Fd(), &slot_id, sizeof(slot_id));
      if (r >= 0) {
        n = r;
        break;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        if (absl::Status w = async::WaitReadable(ctx, retirement_fd.Fd());
            !w.ok()) {
          return; // Cancelled (graceful shutdown) or wait failed.
        }
        continue;
      }
      return; // Read error, we're done.
    }
#else
    absl::StatusOr<ssize_t> n =
        retirement_fd.Read(&slot_id, sizeof(slot_id), ctx);
    if (!n.ok()) {
      // Failed to read the slot ID, we're done.
      return;
    }
#endif
    if (*n == 0) {
      return; // EOF, we're done.
    }

    // We received a retirement notification, send the fd.
    logger_.Log(toolbelt::LogLevel::kVerboseDebug,
                "Received retirement notification for %s, slot %d",
                channel_name.c_str(), slot_id);

    // Send the retirement fd to the other side.
    RetirementNotification msg;
    // If the slot id is zero the protobuf message will be serialized to 0
    // bytes. Sending a message of zero length is very confusing, so we adjust
    // it by adding 1.  We remove the adjustment when it is received.
    msg.set_slot_id(slot_id + 1);

    char buffer[32];
    bool ok = msg.SerializeToArray(buffer + sizeof(int32_t),
                                   sizeof(buffer) - sizeof(int32_t));
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize retirement notification for %s",
                  channel_name.c_str());
      return;
    }
    absl::StatusOr<ssize_t> nsent = retirement_transmitter->SendMessage(
        buffer + sizeof(int32_t), msg.ByteSizeLong(), ctx);
    if (!nsent.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to send retirement fd for %s: %s",
                  channel_name.c_str(), nsent.status().ToString().c_str());
      return;
    }
  }
}

} // namespace subspace
