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
#include <sys/socket.h>
#include <vector>

namespace subspace {

// Tracks the ActiveMessages that a bridge transmitter has sent but whose slots
// the peer has not yet retired, indexed by slot_id.  It is touched by two
// coroutines - the transmitter (Track/Release) and its retirement reader
// (Release) - but both are spawned on the same strand (see
// SpawnOnCurrentStrand at the retirement-reader spawn site), so their accesses
// are serialized by the strand and need no mutex, even when the io_context runs
// on multiple threads.
struct Server::BridgeRetirementState {
  explicit BridgeRetirementState(size_t num_slots)
      : active_messages(num_slots) {}

  ~BridgeRetirementState() {
    for (auto &active : active_messages) {
      if (active != nullptr) {
        active->DecRef();
      }
    }
  }

  void Track(int slot_id, std::shared_ptr<ActiveMessage> active) {
    if (active == nullptr) {
      return;
    }
    if (slot_id < 0 ||
        static_cast<size_t>(slot_id) >= active_messages.size()) {
      active->DecRef();
      return;
    }
    std::shared_ptr<ActiveMessage> previous =
        std::move(active_messages[slot_id]);
    active_messages[slot_id] = std::move(active);
    if (previous != nullptr) {
      previous->DecRef();
    }
  }

  void Release(int slot_id) {
    if (slot_id < 0 ||
        static_cast<size_t>(slot_id) >= active_messages.size()) {
      return;
    }
    std::shared_ptr<ActiveMessage> active =
        std::move(active_messages[slot_id]);
    if (active != nullptr) {
      active->DecRef();
    }
  }

  std::vector<std::shared_ptr<ActiveMessage>> active_messages;
};

static absl::Status SendSplitBridgeMessage(async::Context ctx,
                                           async::StreamSocket &bridge,
                                           const Message &msg,
                                           const MessagePrefix *prefix,
                                           size_t prefix_area) {
  const char *prefix_addr = reinterpret_cast<const char *>(prefix);
  char *prefix_without_padding =
      const_cast<char *>(prefix_addr) + sizeof(int32_t);
  size_t prefix_length = prefix_area - sizeof(int32_t);
  if (absl::StatusOr<ssize_t> n =
          bridge.SendMessage(prefix_without_padding, prefix_length, ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge prefix: %s",
                        n.status().ToString()));
  }

  int32_t payload_length = htonl(static_cast<int32_t>(msg.length));
  if (absl::StatusOr<ssize_t> n =
          bridge.Send(reinterpret_cast<const char *>(&payload_length),
                      sizeof(payload_length), ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge payload length: %s",
                        n.status().ToString()));
  }
  if (absl::StatusOr<ssize_t> n =
          bridge.Send(static_cast<const char *>(msg.buffer), msg.length,
                      ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge payload: %s",
                        n.status().ToString()));
  }
  return absl::OkStatus();
}

static absl::Status SendCombinedBridgeMessage(async::Context ctx,
                                              async::StreamSocket &bridge,
                                              const Message &msg,
                                              const MessagePrefix *prefix,
                                              size_t prefix_area) {
  const char *prefix_addr = reinterpret_cast<const char *>(prefix);
  // SendMessage uses the 4 bytes immediately below the buffer for the frame
  // length. The MessagePrefix padding exists for this purpose, so the legacy
  // bridge path can write prefix and payload in one chunk.
  char *data_addr = const_cast<char *>(prefix_addr) + sizeof(int32_t);
  size_t message_length = msg.length + prefix_area - sizeof(int32_t);
  if (absl::StatusOr<ssize_t> n =
          bridge.SendMessage(data_addr, message_length, ctx);
      !n.ok()) {
    return absl::InternalError(
        absl::StrFormat("failed to send bridge message: %s",
                        n.status().ToString()));
  }
  return absl::OkStatus();
}

static absl::Status SendBridgeMessage(async::Context ctx,
                                      async::StreamSocket &bridge,
                                      const Message &msg,
                                      const MessagePrefix *prefix,
                                      size_t prefix_area,
                                      bool split_buffers_on_wire) {
  if (split_buffers_on_wire) {
    return SendSplitBridgeMessage(ctx, bridge, msg, prefix, prefix_area);
  }
  return SendCombinedBridgeMessage(ctx, bridge, msg, prefix, prefix_area);
}

// This coroutine creates a subscriber to the given channel and sends
// every message received over the bridge to another server.  It first sends
// a 'Subscribed' message detailing the information about the channel then
// enters a loop reading messages sent to the channel on this side of the
// bridge and sending them over.
void Server::BridgeTransmitterCoroutine(async::Context ctx,
                                        BridgeChannelInfo info,
                                        bool pub_reliable, bool sub_reliable,
                                        toolbelt::SocketAddress subscriber,
                                        toolbelt::InetAddress sender,
                                        bool notify_retirement) {
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeTransmitterCoroutine running");

  // IncomingSubscribe added the bridged-publisher entry (keyed by the discovery
  // `sender` address) before spawning us.  Remove it on every exit path - not
  // just the normal end-of-stream teardown but also the early returns below
  // (failed connect, bind, handshake, ...) - otherwise a failed bridge would
  // leave the channel permanently marked as bridged and block re-establishment.
  // bridged_publishers_ is owned by the client strand, so the erase is posted
  // there; we look the channel up by name in case it was removed meanwhile.
  struct BridgeGuard {
    Server *server;
    std::string channel_name;
    toolbelt::InetAddress sender;
    bool sub_reliable;
    ~BridgeGuard() {
      server->runtime_.RunOnStrand([server = server, channel_name = channel_name,
                                    sender = sender, sub_reliable = sub_reliable]() {
        auto it = server->channels_.find(channel_name);
        if (it != server->channels_.end()) {
          it->second->RemoveBridgedAddress(sender, sub_reliable);
        }
      });
    }
  } bridge_guard{this, info.channel_name, sender, sub_reliable};

  async::StreamSocket bridge;
  if (absl::Status status = bridge.Connect(subscriber); !status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to bridge subscriber: %s",
                status.ToString().c_str());
    return;
  }
  if (absl::Status status = bridge.SetNonBlocking(); !status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError, "%s", status.ToString().c_str());
    return;
  }

  const std::string &channel_name = info.channel_name;

  // Send a Subscribed message to the transmitter.
  logger_.Log(toolbelt::LogLevel::kDebug, "Sending subscribed to %s",
              subscriber.ToString().c_str());
  char buffer[kDiscoveryBufferSize];

  // SendMessage needs 4 bytes below the buffer.
  char *databuf = buffer + sizeof(int32_t);
  size_t buflen = sizeof(buffer) - sizeof(int32_t);

  Subscribed subscribed;
  subscribed.set_channel_name(channel_name);
  subscribed.set_slot_size(info.slot_size);
  subscribed.set_num_slots(info.num_slots);
  subscribed.set_subscriber_queue_arena_size(
      info.subscriber_queue_arena_size);
  subscribed.set_reliable(pub_reliable);
  subscribed.set_checksum_size(info.checksum_size);
  subscribed.set_metadata_size(info.metadata_size);
  const bool wire_split_buffers = info.wire_split_buffers;
  subscribed.set_split_buffers(wire_split_buffers);
  subscribed.set_split_buffers_over_bridge(info.split_buffers_over_bridge);

  std::shared_ptr<async::StreamSocket> retirement_listener;
  struct CloseRetirementListenerOnExit {
    std::shared_ptr<async::StreamSocket> &socket;
    ~CloseRetirementListenerOnExit() {
      if (socket != nullptr) {
        socket->Close();
      }
    }
  } close_retirement_listener_on_exit{retirement_listener};
  toolbelt::SocketAddress retirement_addr;

  if (notify_retirement) {
    // We want to be notified when the slot has been retired.
    subscribed.set_notify_retirement(true);
    // Allocate a listen socket to wait for an incoming connection from the
    // other server.
    retirement_listener = std::make_shared<async::StreamSocket>();

    absl::Status s = BindBridgeListener(*retirement_listener);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unable to bind socket for retirement receiver for %s: %s",
                  channel_name.c_str(), s.ToString().c_str());
      return;
    }
    retirement_addr = retirement_listener->BoundAddress();
    logger_.Log(toolbelt::LogLevel::kDebug, "Retirement listener: %s",
                retirement_addr.ToString().c_str());

    // Tell the other side where to connect to notify us of slot retirement
    auto *ret_addr = subscribed.mutable_retirement_socket();
    switch (retirement_addr.Type()) {
    case toolbelt::SocketAddress::kAddressInet: {
      in_addr ip_addr = bridge_advertise_address_.Valid()
                            ? bridge_advertise_address_.IpAddress()
                            : retirement_addr.GetInetAddress().IpAddress();
      ret_addr->set_address(&ip_addr, sizeof(ip_addr));
      ret_addr->set_family(ChannelAddress::FAMILY_INET);
      break;
    }
    case toolbelt::SocketAddress::kAddressVirtual: {
      // vsock: advertise our configured local CID (see SendSubscribeMessage).
      uint32_t cid = vsock_cid_;
      ret_addr->set_address(&cid, sizeof(cid));
      ret_addr->set_family(ChannelAddress::FAMILY_VSOCK);
      break;
    }
    default:
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unsupported address type for retirement notification: %s",
                  retirement_addr.ToString().c_str());
      return;
    }

    ret_addr->set_port(retirement_addr.Port());
  }

  bool ok = subscribed.SerializeToArray(databuf, static_cast<int>(buflen));
  if (!ok) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to serialize subscribed message");
    return;
  }
  int64_t length = subscribed.ByteSizeLong();
  absl::StatusOr<ssize_t> n_sent_1 =
      bridge.SendMessage(databuf, length, ctx);
  if (!n_sent_1.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send subscribed for %s: %s", channel_name.c_str(),
                n_sent_1.status().ToString().c_str());
    return;
  }

  Client client(ctx);
  if (absl::Status s = client.Init(socket_name_); !s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to Subspace server: %s",
                s.ToString().c_str());
    return;
  }
  absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
      channel_name,
      SubscriberOptions().SetReliable(sub_reliable).SetBridge(true));
  if (!sub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create bridge subscriber for %s: %s",
                channel_name.c_str(), sub.status().ToString().c_str());
    return;
  }

  // For retirement notifications, we keep track of the Messages we
  // send over the bridge.  This extends the lifetime of the AciveMessage until
  // the other side of the bridge sends us a retirement notification for the
  // message's slot.
  std::shared_ptr<BridgeRetirementState> retirement_state;
  bool notifying_of_retirement = notify_retirement;

  // Spawn a coroutine to read from the retirement connection.  It runs on this
  // transmitter's own strand (not a fresh one) so that its Release calls are
  // serialized with our Track/Release on the shared retirement_state without a
  // mutex.  The two still interleave cooperatively: whenever this transmitter
  // is parked in a wait (subscriber trigger, or POLLOUT under bridge
  // backpressure) the strand is free to run the retirement reader, so incoming
  // retirements keep relieving that backpressure.
  if (notifying_of_retirement) {
    retirement_state = std::make_shared<BridgeRetirementState>(info.num_slots);
    runtime_.SpawnOnCurrentStrand(
        ctx,
        [this, retirement_listener,
         retirement_state](async::Context ret_ctx) mutable {
          return RetirementReceiverCoroutine(ret_ctx, retirement_listener,
                                             retirement_state);
        },
        {.name = absl::StrFormat("Retirement listener for %s", channel_name),
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }

  if (bridge_notification_pipe_.WriteFd().Valid()) {
    // Also send the Subscribed message to the bridge notification pipe.  This
    // is sent without a coroutine.
    // TODO: use a coroutine to avoid blocking?
    // We send the whole buffer including the 4-byte length.
    if (absl::StatusOr<ssize_t> s = bridge_notification_pipe_.WriteFd().Write(
            buffer, length + sizeof(int32_t));
        !s.ok()) {
      logger_.Log(
          toolbelt::LogLevel::kError,
          "Failed to send subscribed message to bridge notification pipe: %s",
          s.status().ToString().c_str());
      // Ignore the error.
    }
  }

  // Read messages from subscriber and send to bridge socket.
  bool done = false;
  while (!done) {
    if (absl::Status status = sub->Wait(); !status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to wait for subscriber: %s",
                  status.ToString().c_str());
      break;
    }
    // Read all available messages and send to transmitter.
    for (;;) {
      absl::StatusOr<Message> msg = sub->ReadMessageInternal(
          ReadMode::kReadNext, /*pass_activation=*/true,
          /*clear_trigger=*/true);
      if (!msg.ok()) {
        done = true;
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to read message from bridge subscriber for %s: %s",
                    channel_name.c_str(), msg.status().ToString().c_str());
        break;
      }
      if (msg->length == 0) {
        // End of messages, wait for more.
        break;
      }
      // We want to send the MessagePrefix along with the message.
      // The prefix area may be larger than sizeof(MessagePrefix) when
      // checksum_size or metadata_size requires additional 64-byte chunks.
      size_t prefix_area = sub->PrefixSize();
      const MessageSlot *slot = sub->GetSlot(msg->slot_id);
      const MessagePrefix *prefix =
          slot == nullptr ? nullptr
                          : sub->Prefix(const_cast<MessageSlot *>(slot));
      if (prefix == nullptr) {
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to find message prefix for bridge message on %s",
                    channel_name.c_str());
        continue;
      }
      // NOTE: there's a question here about whether we want to send an
      // activation message across the bridge.  Currently we do send
      // it but the receiver will disregard it.  I don't think we need
      // to actually send it but there's no harm.
      if ((prefix->flags & kMessageBridged) != 0) {
        // This message came from bridge.  We don't forward them again.
        continue;
      }
      // Note that for a reliable publisher where the subscriber is slower
      // we will get backpressure from the receiver because it won't
      // read from the socket.  We will keep transmitting until the TCP
      // buffers fill up, at which point we will stop sending until we
      // get a POLLOUT event.  We are using a nonblocking socket to write
      // to network and the check for EAGAIN is in SendMessage in the
      // socket.  Since we are using coroutines, the EAGAIN will yield
      // this coroutine until POLLOUT says we can try again.
      //
      // The backpressure received here will be applied upwards because
      // we will stop reading the messages from the channel and thus
      // backpressure any publishers writing to that channel.
      const int32_t slot_id = msg->slot_id;
      if (notifying_of_retirement) {
        // Track before sending: the peer can consume and retire the bridged
        // slot on the separate retirement socket as soon as SendBridgeMessage
        // returns.
        retirement_state->Track(slot_id, std::move(msg->active_message));
      }
      if (absl::Status status =
              SendBridgeMessage(ctx, bridge, *msg, prefix, prefix_area,
                                wire_split_buffers);
          !status.ok()) {
        if (notifying_of_retirement) {
          retirement_state->Release(slot_id);
        }
        done = true;
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to send bridge message for %s: %s",
                    channel_name.c_str(), status.ToString().c_str());
        break;
      }
    }

    const ChannelCounters &counters = sub->GetCounters();
    if (counters.num_pubs == 0) {
      // No publisher left to send anything.  We're done.
      break;
    }
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Bridge transmitter for %s terminating", channel_name.c_str());
  // bridge_guard removes the bridged-publisher entry (by `sender`) as we
  // unwind.
}

// This coroutine reads retirement messages from the bridge and removes
// the reference to the slot that has been retired on the other side.
// References to the active messages are kept in a vector, indexed by slot_id.
// If these references are the last reference to the slot, it will be retired
// on this side and the publisher's retirement FD is sent the slot.
void Server::RetirementReceiverCoroutine(
    async::Context ctx,
    std::shared_ptr<async::StreamSocket> retirement_listener,
    std::shared_ptr<BridgeRetirementState> retirement_state) {
  // Accept connection on the retirement listener socket.
  absl::StatusOr<async::StreamSocket> retirement_socket =
      retirement_listener->Accept(ctx);
  if (!retirement_socket.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to accept retirement connection: %s",
                retirement_socket.status().ToString().c_str());
    return;
  }

  // Read retirement messages from the socket.
  char buffer[kDiscoveryBufferSize];
  for (;;) {
    absl::StatusOr<ssize_t> n_recv =
        retirement_socket->ReceiveMessage(buffer, sizeof(buffer), ctx);
    if (!n_recv.ok()) {
      return;
    }
    if (*n_recv == 0) {
      // No more messages, we're done.
      break;
    }
    RetirementNotification retirement;
    if (!retirement.ParseFromArray(buffer, static_cast<int>(*n_recv))) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to parse Retirement message");
      continue;
    }
    int slot_id = retirement.slot_id();
    // We always send 1 greater than the slot id to prevent a slot id of 0
    // being sent (which is serialized as 0 bytes.  Remove the adustment.
    slot_id -= 1;

    retirement_state->Release(slot_id);
  }
}

} // namespace subspace
