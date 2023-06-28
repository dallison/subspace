// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "proto/subspace.pb.h"
#include "toolbelt/clock.h"
#include "toolbelt/sockets.h"
#include <cerrno>
#include <fcntl.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <setjmp.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <unistd.h>
#include <vector>

namespace subspace {

// Look for the IP address and calculate the broadcast address
// for the given interface.  If the interface name is empty
// choose the first interface that supports broadcast and
// has an IP address assigned.
static absl::Status FindIPAddresses(const std::string &interface,
                                    toolbelt::InetAddress &ipaddr,
                                    toolbelt::InetAddress &broadcast_addr,
                                    toolbelt::Logger &logger) {
  if (interface.empty()) {
    logger.Log(
        toolbelt::LogLevel::kInfo,
        "No interface supplied; will choose the first broadcast interface with "
        "an IPv4 address.\nThis may not be what you want.  Please provide a "
        "--interface argument.");
  }
  struct ifaddrs *addrs;

  int e = getifaddrs(&addrs);
  if (e == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to get ifaddrs: %s", strerror(errno)));
  }
  bool found = false;
  for (struct ifaddrs *addr = addrs; addr != nullptr; addr = addr->ifa_next) {
    struct sockaddr *saddr = addr->ifa_addr;
    if (!interface.empty() && interface != addr->ifa_name) {
      continue;
    }
    if (saddr == nullptr) {
      continue;
    }
    if ((addr->ifa_flags & IFF_LOOPBACK) != 0) {
      continue;
    }
    if ((addr->ifa_flags & IFF_BROADCAST) == 0) {
      continue;
    }
    if (saddr->sa_family == AF_INET) {
      // Extract IP address and netmask.
      struct sockaddr_in *ipp = reinterpret_cast<struct sockaddr_in *>(saddr);
      ipaddr = *ipp;
      struct sockaddr_in *nm =
          reinterpret_cast<struct sockaddr_in *>(addr->ifa_netmask);
      in_addr_t netmask = ntohl(nm->sin_addr.s_addr);

      // Construct the broadcast address
      in_addr_t ip = ntohl(ipp->sin_addr.s_addr);
      in_addr_t bcast = ip | ~netmask;
      struct sockaddr_in bcast_sa = *ipp; // Copy most of IP address.
      bcast_sa.sin_addr.s_addr = htonl(bcast);
      broadcast_addr = bcast_sa;
      found = true;
      break;
    }
  }
  freeifaddrs(addrs);
  if (!found) {
    if (interface.empty()) {
      return absl::InternalError("No IPv4 network interfaces can be found");
    }
    return absl::InternalError(
        absl::StrFormat("Interface %s either doesn't exists or can't be used",
                        interface.c_str()));
  }
  return absl::OkStatus();
}

Server::Server(co::CoroutineScheduler &scheduler,
               const std::string &socket_name, const std::string &interface,
               int disc_port, int peer_port, bool local, int notify_fd)
    : socket_name_(socket_name), interface_(interface),
      discovery_port_(disc_port), discovery_peer_port_(peer_port),
      local_(local), notify_fd_(notify_fd), co_scheduler_(scheduler) {}

Server::~Server() {
  // Clear this before other data members get destroyed.
  client_handlers_.clear();
}

void Server::Stop() { co_scheduler_.Stop(); }

void Server::CloseHandler(ClientHandler *handler) {
  for (auto it = client_handlers_.begin(); it != client_handlers_.end(); it++) {
    if (it->get() == handler) {
      client_handlers_.erase(it);
      break;
    }
  }
}

// This coroutine listens for incoming client connections on the given
// UDS and spawns a handler coroutine to handle the communication with
// the client.
void Server::ListenerCoroutine(toolbelt::UnixSocket listen_socket,
                               co::Coroutine *c) {
  for (;;) {
    absl::Status status = HandleIncomingConnection(listen_socket, c);
    if (!status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unable to make incoming connection: %s",
                  status.ToString().c_str());
    }
  }
}

absl::Status Server::Run() {
  std::vector<struct pollfd> poll_fds;

#ifndef __linux__
  // Remove socket name if it exists.  On Non-Linux systems the socket
  // is a file in the file system.  On Linux it's in the abstract
  // namespace (not a file).
  remove(socket_name_.c_str());
#endif

  toolbelt::UnixSocket listen_socket;
  absl::Status status = listen_socket.Bind(socket_name_, true);
  if (!status.ok()) {
    return status;
  }

  // Notify listener that the server is ready.
  if (notify_fd_.Valid()) {
    int64_t val = kServerReady;
    (void)::write(notify_fd_.Fd(), &val, 8);
  }
  absl::StatusOr<SystemControlBlock *> scb = CreateSystemControlBlock(scb_fd_);
  if (!scb.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to create SystemControlBlock in shared memory: %s",
        scb.status().ToString().c_str()));
  }
  scb_ = *scb;

  // Create the trigger fd for sending the channel directory.
  if (absl::Status s = channel_directory_trigger_fd_.Open(); !s.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to create channel directory trigger: %s",
                        s.ToString().c_str()));
  }

  char hostname[256] = "local";
  if (!local_) {
    if (gethostname(hostname, sizeof(hostname)) == -1) {
      return absl::InternalError(
          absl::StrFormat("Can't get local hostname: %s", strerror(errno)));
    }

    hostname_ = hostname;
  }

  // Make a unique server id to identify this server.
  server_id_ = absl::StrFormat("%s.%d", hostname, getpid());

  if (!local_) {
    // Find the IP and broadcast IPv4 addresses based on the interface supplied
    // as an argument.  If there is no interface, choose the first interface
    // with an IPv4 address that supports broadcast.
    toolbelt::InetAddress ip_addr;
    toolbelt::InetAddress bcast_addr;
    if (absl::Status s =
            FindIPAddresses(interface_, ip_addr, bcast_addr, logger_);
        !s.ok()) {
      return s;
    }
    logger_.Log(toolbelt::LogLevel::kInfo, "IPv4: %s, Broadcast: %s",
                ip_addr.ToString().c_str(), bcast_addr.ToString().c_str());

    // Bind the discovery transmitter to the network and any free
    // port on the requested interface.
    if (absl::Status s = discovery_transmitter_.Bind(ip_addr); !s.ok()) {
      return s;
    }

    discovery_addr_ = bcast_addr;
    discovery_addr_.SetPort(discovery_peer_port_);

    if (absl::Status s = discovery_transmitter_.SetBroadcast(); !s.ok()) {
      return s;
    }

    // Open the discovery receiver socket.
    if (absl::Status s = discovery_receiver_.Bind(
            toolbelt::InetAddress::AnyAddress(discovery_port_));
        !s.ok()) {
      return s;
    }
  }

  // Register a callback to be called when a coroutine completes.  The
  // server keeps track of all coroutines created.
  // This deletes them when they are done.
  co_scheduler_.SetCompletionCallback(
      [this](co::Coroutine *c) { coroutines_.erase(c); });

  // Start the listener coroutine.
  coroutines_.insert(std::make_unique<co::Coroutine>(
      co_scheduler_,
      [this, &listen_socket](co::Coroutine *c) {
        ListenerCoroutine(std::move(listen_socket), c);
      },
      "Listener UDS"));

#if 0
  // Start the channel directory coroutine.
  coroutines_.insert(std::make_unique<co::Coroutine>(
      co_scheduler_, [this](co::Coroutine *c) { ChannelDirectoryCoroutine(c); },
      "Channel directory"));

  // Start the channel stats coroutine.
  coroutines_.insert(std::make_unique<co::Coroutine>(
      co_scheduler_, [this](co::Coroutine *c) { StatisticsCoroutine(c); },
      "Channel stats"));
#endif

  if (!local_) {
    // Start the discovery receiver coroutine.
    coroutines_.insert(std::make_unique<co::Coroutine>(
        co_scheduler_,
        [this](co::Coroutine *c) { DiscoveryReceiverCoroutine(c); }));

    // Start the gratuitous Advertiser coroutine.  This sends Advertise messages
    // every 5 seconds.
    coroutines_.insert(std::make_unique<co::Coroutine>(
        co_scheduler_,
        [this](co::Coroutine *c) { GratuitousAdvertiseCoroutine(c); },
        "Gratuitous advertiser"));
  }

  // Run the coroutine main loop.
  co_scheduler_.Run();

  // Notify listener that we're stopped.
  if (notify_fd_.Valid()) {
    int64_t val = kServerStopped;
    (void)::write(notify_fd_.Fd(), &val, 8);
  }
  return absl::OkStatus();
}

absl::Status
Server::HandleIncomingConnection(toolbelt::UnixSocket &listen_socket,
                                 co::Coroutine *c) {
  absl::StatusOr<toolbelt::UnixSocket> s = listen_socket.Accept(c);
  if (!s.ok()) {
    return s.status();
  }
  client_handlers_.push_back(
      std::make_unique<ClientHandler>(this, std::move(*s)));
  ClientHandler *handler_ptr = client_handlers_.back().get();

  coroutines_.insert(std::make_unique<co::Coroutine>(
      co_scheduler_, [handler_ptr](co::Coroutine *c) { handler_ptr->Run(c); },
      "Client handler"));

  return absl::OkStatus();
}

absl::StatusOr<ServerChannel *>
Server::CreateChannel(const std::string &channel_name, int slot_size,
                      int num_slots, std::string type) {
  absl::StatusOr<int> channel_id = channel_ids_.Allocate("channel");
  if (!channel_id.ok()) {
    return channel_id.status();
  }
  ServerChannel *channel =
      new ServerChannel(*channel_id, channel_name, num_slots, std::move(type));
  channel->SetDebug(logger_.GetLogLevel() <= toolbelt::LogLevel::kVerboseDebug);

  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  channels_.emplace(std::make_pair(channel_name, channel));

  SendChannelDirectory();
  return channel;
}

absl::Status Server::RemapChannel(ServerChannel *channel, int slot_size,
                                  int num_slots) {
  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  return absl::OkStatus();
}

ServerChannel *Server::FindChannel(const std::string &channel_name) {
  auto it = channels_.find(channel_name);
  if (it == channels_.end()) {
    return nullptr;
  }
  return it->second.get();
}

void Server::RemoveChannel(ServerChannel *channel) {
  channel_ids_.Clear(channel->GetChannelId());
  auto it = channels_.find(channel->Name());
  channels_.erase(it);
  SendChannelDirectory();
}

void Server::RemoveAllUsersFor(ClientHandler *handler) {
  std::vector<ServerChannel *> empty_channels;
  for (auto &channel : channels_) {
    channel.second->RemoveAllUsersFor(handler);
    if (channel.second->IsEmpty()) {
      empty_channels.push_back(channel.second.get());
    }
  }
  // toolbelt::Now remove all empty channels.
  for (auto *channel : empty_channels) {
    RemoveChannel(channel);
  }
}

void Server::ChannelDirectoryCoroutine(co::Coroutine *c) {
  // Coroutine aware client.
  Client client(c);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(
        toolbelt::LogLevel::kFatal,
        "Failed to initialize Subspace client for channel directory: %s",
        status.ToString().c_str());
  }
  constexpr int kDirectorySlotSize = 128 * 1024 - sizeof(MessagePrefix);
  constexpr int kDirectoryNumSlots = 32;

  absl::StatusOr<Publisher> channel_directory = client.CreatePublisher(
      "/subspace/ChannelDirectory", kDirectorySlotSize, kDirectoryNumSlots,
      PublisherOptions().SetType("subspace.ChannelDirectory"));
  if (!channel_directory.ok()) {
    logger_.Log(toolbelt::LogLevel::kFatal,
                "Failed to create channel directory channel: %s",
                channel_directory.status().ToString().c_str());
  }
  for (;;) {
    c->Wait(channel_directory_trigger_fd_.GetPollFd().Fd(), POLLIN);
    channel_directory_trigger_fd_.Clear();

    ChannelDirectory directory;
    directory.set_server_id(server_id_);
    for (auto &channel : channels_) {
      auto info = directory.add_channels();
      channel.second->GetChannelInfo(info);
    }
    absl::StatusOr<void *> buffer = channel_directory->GetMessageBuffer();
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kFatal,
                  "Failed to get channel directory buffer: %s",
                  buffer.status().ToString().c_str());
    }
    bool ok = directory.SerializeToArray(*buffer, kDirectorySlotSize);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel directory");
      continue;
    }
    int64_t length = directory.ByteSizeLong();
    absl::StatusOr<const Message> s = channel_directory->PublishMessage(length);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish channel directory: %s",
                  s.status().ToString().c_str());
    }
  }
}

void Server::SendChannelDirectory() { channel_directory_trigger_fd_.Trigger(); }

void Server::StatisticsCoroutine(co::Coroutine *c) {
  Client client(c);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(toolbelt::LogLevel::kFatal,
                "Failed to initialize Subspace client for statistics: %s",
                status.ToString().c_str());
  }
  constexpr int kStatsSlotSize = 8192 - sizeof(MessagePrefix);
  constexpr int kStatsNumSlots = 32;

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "/subspace/Statistics", kStatsSlotSize, kStatsNumSlots,
      PublisherOptions().SetType("subspace.Statistics"));
  if (!pub.ok()) {
    logger_.Log(toolbelt::LogLevel::kFatal,
                "Failed to create statistics channel: %s",
                pub.status().ToString().c_str());
  }

  constexpr int kPeriodSecs = 2;
  for (;;) {
    c->Sleep(kPeriodSecs);
    Statistics stats;
    stats.set_timestamp(toolbelt::Now());
    stats.set_server_id(server_id_);
    for (auto &channel : channels_) {
      auto s = stats.add_channels();
      channel.second->GetChannelStats(s);
    }
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kFatal,
                  "Failed to get channel stats buffer: %s",
                  buffer.status().ToString().c_str());
    }
    bool ok = stats.SerializeToArray(*buffer, kStatsSlotSize);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel stats");
      continue;
    }
    int64_t length = stats.ByteSizeLong();
    absl::StatusOr<const Message> s = pub->PublishMessage(length);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish statistics: %s",
                  s.status().ToString().c_str());
    }
  }
}

// Send a query discovery message for the given channel.  This
// is sent on the IPv4 broadcast address over UDP.
void Server::SendQuery(const std::string &channel_name) {
  if (local_) {
    return;
  }
  // Spawn a coroutine to send the Query message.
  coroutines_.insert(std::make_unique<co::Coroutine>(
      co_scheduler_,
      [this, channel_name](co::Coroutine *c) {
        logger_.Log(toolbelt::LogLevel::kDebug, "Sending Query %s",
                    channel_name.c_str());
        char buffer[kDiscoveryBufferSize];
        Discovery disc;
        disc.set_server_id(server_id_);
        disc.set_port(discovery_port_);

        auto *query = disc.mutable_query();
        query->set_channel_name(channel_name);

        bool ok = disc.SerializeToArray(buffer, sizeof(buffer));
        if (!ok) {
          logger_.Log(toolbelt::LogLevel::kError,
                      "Failed to serialize Query message");
          return;
        }
        int64_t length = disc.ByteSizeLong();
        absl::Status s =
            discovery_transmitter_.SendTo(discovery_addr_, buffer, length, c);
        if (!s.ok()) {
          logger_.Log(toolbelt::LogLevel::kError, "Failed to send Query: %s",
                      s.ToString().c_str());
          return;
        }
      },
      "discovery query"));
}

// Send an advertise discovery message over UDP.
void Server::SendAdvertise(const std::string &channel_name, bool reliable) {
  if (local_) {
    return;
  }
  // Spawn a coroutine to send the Publish message.
  coroutines_.insert(std::make_unique<co::Coroutine>(
      co_scheduler_,
      [this, channel_name, reliable](co::Coroutine *c) {
        logger_.Log(toolbelt::LogLevel::kDebug, "Sending Advertise %s",
                    channel_name.c_str());
        char buffer[kDiscoveryBufferSize];
        Discovery disc;
        disc.set_server_id(server_id_);
        disc.set_port(discovery_port_);
        auto *advertise = disc.mutable_advertise();
        advertise->set_channel_name(channel_name);
        advertise->set_reliable(reliable);
        bool ok = disc.SerializeToArray(buffer, sizeof(buffer));
        if (!ok) {
          logger_.Log(toolbelt::LogLevel::kError,
                      "Failed to serialize Advertise message");
          return;
        }
        int64_t length = disc.ByteSizeLong();
        absl::Status s =
            discovery_transmitter_.SendTo(discovery_addr_, buffer, length, c);
        if (!s.ok()) {
          logger_.Log(toolbelt::LogLevel::kError,
                      "Failed to send Advertise: %s", s.ToString().c_str());
          return;
        }
      },
      "discovery advertiser"));
}

// This coroutine receives discovery messages over UDP.
void Server::DiscoveryReceiverCoroutine(co::Coroutine *c) {
  char buffer[kDiscoveryBufferSize];
  for (;;) {
    toolbelt::InetAddress sender;
    absl::StatusOr<ssize_t> n =
        discovery_receiver_.ReceiveFrom(sender, buffer, sizeof(buffer), c);
    if (!n.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to read discovery message: %s",
                  n.status().ToString().c_str());
      continue;
    }
    Discovery disc;
    if (!disc.ParseFromArray(buffer, *n)) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to parse discovery message");
      continue;
    }
    if (disc.server_id() == server_id_) {
      return;
    }
    sender.SetPort(disc.port());
    logger_.Log(toolbelt::LogLevel::kDebug, "Discovery message from %s\n%s",
                sender.ToString().c_str(), disc.DebugString().c_str());
    switch (disc.data_case()) {
    case Discovery::kQuery:
      IncomingQuery(disc.query(), sender);
      break;
    case Discovery::kAdvertise:
      IncomingAdvertise(disc.advertise(), sender);
      break;
    case Discovery::kSubscribe:
      IncomingSubscribe(disc.subscribe(), sender);
      break;
    default:
      break;
    }
  }
}

void Server::BridgeTransmitterCoroutine(ServerChannel *channel,
                                        bool pub_reliable, bool sub_reliable,
                                        toolbelt::InetAddress subscriber,
                                        co::Coroutine *c) {
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeTransmitterCoroutine running");
  toolbelt::TCPSocket bridge;
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

  const std::string &channel_name = channel->Name();

  // Send a Subscribed message to the transmitter.
  logger_.Log(toolbelt::LogLevel::kDebug, "Sending subscribed to %s",
              subscriber.ToString().c_str());
  char buffer[kDiscoveryBufferSize];

  // SendMessage needs 4 bytes below the buffer.
  char *databuf = buffer + sizeof(int32_t);
  size_t buflen = sizeof(buffer) - sizeof(int32_t);

  Subscribed subscribed;
  subscribed.set_channel_name(channel_name);
  subscribed.set_slot_size(channel->SlotSize());
  subscribed.set_num_slots(channel->NumSlots());
  subscribed.set_reliable(pub_reliable);

  bool ok = subscribed.SerializeToArray(databuf, buflen);
  if (!ok) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to serialize subscribed message");
    return;
  }
  int64_t length = subscribed.ByteSizeLong();
  absl::StatusOr<ssize_t> n = bridge.SendMessage(databuf, length, c);
  if (!n.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send subscribed for %s: %s", channel_name.c_str(),
                n.status().ToString().c_str());
    return;
  }

  Client client(c);
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
        // End of messages, wait for more.w
        break;
      }
      // We want to send the MessagePrefix along with the message.
      const char *prefix_addr =
          reinterpret_cast<const char *>(msg->buffer) - sizeof(MessagePrefix);
      const MessagePrefix *prefix =
          reinterpret_cast<const MessagePrefix *>(prefix_addr);
      // NOTE: there's a question here about whether we want to send an
      // activation message across the bridge.  Currently we do send
      // it but the receiver will disregard it.  I don't think we need
      // to actually send it but there's no harm.
      if ((prefix->flags & kMessageBridged) != 0) {
        // This message came from bridge.  We don't forward them again.
        continue;
      }
      // SendMessage uses the 4 bytes immediately below the buffer
      // for the length of the messages.  The MessagePrefix struct
      // contains a padding member for this purpose.  We don't
      // count the padding in the length sent.
      char *data_addr = const_cast<char *>(prefix_addr) + sizeof(int32_t);
      size_t msglen = msg->length + sizeof(MessagePrefix) - sizeof(int32_t);

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
      if (absl::StatusOr<ssize_t> n = bridge.SendMessage(data_addr, msglen, c);
          !n.ok()) {
        done = true;
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to send bridge message for %s: %s",
                    channel_name.c_str(), n.status().ToString().c_str());

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
  // We're done reading messages from the channel, remove the
  // bridge and subscriber.
  channel->RemoveBridgedAddress(subscriber, sub_reliable);
}

// Send a Subscribe message over UDP.
absl::Status Server::SendSubscribeMessage(
    const std::string &channel_name, bool reliable,
    toolbelt::InetAddress publisher, toolbelt::TCPSocket &receiver_listener,
    char *buffer, size_t buffer_size, co::Coroutine *c) {
  const toolbelt::InetAddress &receiver_addr = receiver_listener.BoundAddress();
  logger_.Log(toolbelt::LogLevel::kDebug, "Bridge receiver socket: %s",
              receiver_addr.ToString().c_str());
  // Send a subscribe request to the publisher.
  Discovery disc;
  disc.set_server_id(server_id_);
  auto *sub = disc.mutable_subscribe();
  sub->set_channel_name(channel_name);
  sub->set_reliable(reliable);
  auto *sub_addr = sub->mutable_receiver();
  in_addr ip_addr = receiver_addr.IpAddress();
  sub_addr->set_ip_address(&ip_addr, sizeof(ip_addr));
  sub_addr->set_port(receiver_addr.Port());

  bool ok = disc.SerializeToArray(buffer, buffer_size);
  if (!ok) {
    return absl::InternalError("Failed to serialize subscribe message");
  }
  int64_t length = disc.ByteSizeLong();
  logger_.Log(toolbelt::LogLevel::kDebug, "Sending subscribe to %s: %s",
              publisher.ToString().c_str(), disc.DebugString().c_str());
  absl::Status s = discovery_transmitter_.SendTo(publisher, buffer, length, c);
  if (!s.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to send subscribe: %s", s.ToString()));
  }
  return absl::OkStatus();
}

// This coroutine receives messages on a TCP socket and publishes
// them to a local channel.  The messages contain the prefix which
// is sent intact to the channel.
void Server::BridgeReceiverCoroutine(std::string channel_name,
                                     bool sub_reliable,
                                     toolbelt::InetAddress publisher,
                                     co::Coroutine *c) {
  // Open a listening TCP socket on a free port.
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeReceiverCoroutine running");
  char buffer[kDiscoveryBufferSize];

  toolbelt::TCPSocket receiver_listener;
  absl::Status s =
      receiver_listener.Bind(toolbelt::InetAddress(hostname_, 0), true);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Unable to bind socket for bridge receiver for %s: %s",
                channel_name.c_str(), s.ToString().c_str());
    return;
  }
  const toolbelt::InetAddress &receiver_addr = receiver_listener.BoundAddress();
  logger_.Log(toolbelt::LogLevel::kDebug, "Bridge receiver socket: %s",
              receiver_addr.ToString().c_str());

  s = SendSubscribeMessage(channel_name, sub_reliable, publisher,
                           receiver_listener, buffer, sizeof(buffer), c);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send Subscribe message for channel %s: %s",
                channel_name.c_str(), s.ToString().c_str());
    return;
  }

  // Accept connection on listen socket.
  absl::StatusOr<toolbelt::TCPSocket> bridge = receiver_listener.Accept(c);
  if (!bridge.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to accept incoming bridge connection: %s",
                bridge.status().ToString().c_str());
    return;
  }

  // Wait for the Subscribed message from the server.
  Subscribed subscribed;
  absl::StatusOr<ssize_t> n = bridge->ReceiveMessage(buffer, sizeof(buffer), c);
  if (!n.ok()) {
    logger_.Log(toolbelt::LogLevel::kError, "Failed to receive Subscribed: %s",
                n.status().ToString().c_str());
    return;
  }
  if (!subscribed.ParseFromArray(buffer, *n)) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to parse Subscribed message");
    return;
  }

  // Build a publisher to publish incoming bridge messages to the channel.
  Client client(c);
  s = client.Init(socket_name_);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to Subspace server: %s",
                s.ToString().c_str());
    return;
  }
  // For a reliable publisher, this will send an activation message
  // to the local channel.
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      channel_name, subscribed.slot_size(), subscribed.num_slots(),
      PublisherOptions().SetReliable(subscribed.reliable()).SetBridge(true));
  if (!pub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create bridge publisher for %s: %s",
                channel_name.c_str(), pub.status().ToString().c_str());
    return;
  }

  // toolbelt::Now we receive messages from the bridge and send to the
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

    // Read the received message into the prefix that is located just
    // before the message buffer.  When the message is published into
    // the channel, we tell the client to omit the prefix so that it
    // remains intact.  This means that the ordinal is carried intact
    // over the bridge.
    //
    // The MessagePrefix struct contains 4 bytes of padding at offset 0.
    // This is to allow SendMessage to use it for the length to avoid
    // 2 sends to the network.  We need to receive into the address
    // after the padding.
    constexpr size_t kAdjustedPrefixLength =
        sizeof(MessagePrefix) - sizeof(int32_t);
    char *prefix_addr = reinterpret_cast<char *>(*buf) - sizeof(MessagePrefix);
    char *after_padding = prefix_addr + sizeof(int32_t);

    absl::StatusOr<ssize_t> n = bridge->ReceiveMessage(
        after_padding, subscribed.slot_size() + kAdjustedPrefixLength, c);
    if (!n.ok()) {
      // This will happen when the bridge transmitter on the other
      // side of the bridge terminates.
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to read bridge message for %s: %s",
                  channel_name.c_str(), n.status().ToString().c_str());
      break;
    }

    // Set the kMessageBridged flag in the prefix so that this message isn't
    // forwarded again over a bridge.
    MessagePrefix *prefix = reinterpret_cast<MessagePrefix *>(prefix_addr);
    if ((prefix->flags & kMessageActivate) != 0) {
      // Since we have created a reliable publisher and it has sent an
      // activation message through, we don't send another one.
      continue;
    }
    prefix->flags |= kMessageBridged;

    absl::StatusOr<const Message> s =
        pub->PublishMessageInternal(*n, /*omit_prefix=*/true);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish bridge message for %s: %s",
                  channel_name.c_str(), s.status().ToString().c_str());
    }
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Bridge receiver coroutine terminating");
  // Socket has been closed, we're done.
}

void Server::SubscribeOverBridge(ServerChannel *channel, bool reliable,
                                 toolbelt::InetAddress publisher) {
  coroutines_.insert(std::make_unique<co::Coroutine>(
      co_scheduler_,
      [this, publisher, channel, reliable](co::Coroutine *c) {
        BridgeReceiverCoroutine(channel->Name(), reliable, publisher, c);
      },
      absl::StrFormat("Bridge receiver for %s", channel->Name()).c_str()));
}

void Server::IncomingQuery(const Discovery::Query &query,
                           const toolbelt::InetAddress &sender) {
  // Someone is asking who publishes a channel.  Do I publish it?  If so,
  // send an Advertise out.
  auto channel = channels_.find(query.channel_name());
  if (channel != channels_.end()) {
    if (channel->second->IsLocal() || channel->second->IsBridgePublisher()) {
      return;
    }
    SendAdvertise(query.channel_name(), channel->second->IsReliable());
  }
}

void Server::IncomingAdvertise(const Discovery::Advertise &advertise,
                               const toolbelt::InetAddress &sender) {
  // Do I want to subscribe to this channel?
  auto channel = channels_.find(advertise.channel_name());
  if (channel != channels_.end()) {
    if (channel->second->IsBridged(sender, advertise.reliable())) {
      // Already bridged to this sender.
      logger_.Log(toolbelt::LogLevel::kDebug,
                  "Channel %s is already bridged to this address",
                  advertise.channel_name().c_str());
      return;
    }
    if (channel->second->IsBridgeSubscriber()) {
      // All the local subscribers are bridge subscribers.
      return;
    }
    channel->second->AddBridgedAddress(sender, advertise.reliable());

    int num_pubs, num_subs;
    channel->second->CountUsers(num_pubs, num_subs);
    if (num_subs > 0) {
      SubscribeOverBridge(channel->second.get(), advertise.reliable(), sender);
    }
  }
}

void Server::IncomingSubscribe(const Discovery::Subscribe &subscribe,
                               const toolbelt::InetAddress &sender) {
  bool sub_reliable = subscribe.reliable();

  auto channel = channels_.find(subscribe.channel_name());
  if (channel != channels_.end()) {
    if (channel->second->IsLocal()) {
      return;
    }
    if (channel->second->IsBridged(sender, sub_reliable)) {
      // Already bridged to this sender.
      logger_.Log(toolbelt::LogLevel::kDebug,
                  "Channel %s is already bridged to this address",
                  subscribe.channel_name().c_str());
      return;
    }
    // We've been asked who publishes a public channel and we do.
    ServerChannel *ch = channel->second.get();
    bool pub_reliable = ch->IsReliable();

    channel->second->AddBridgedAddress(sender, sub_reliable);

    // The subscribe message contains the IP address and port of the TCP
    // socket listening for our connection for the channel.  Extract it.
    // TODO: IPv6?
    in_addr subscriber_ip;
    memcpy(&subscriber_ip, subscribe.receiver().ip_address().data(),
           sizeof(subscriber_ip));
    toolbelt::InetAddress subscriber_addr(subscriber_ip,
                                          subscribe.receiver().port());
    coroutines_.insert(std::make_unique<co::Coroutine>(
        co_scheduler_,
        [this, pub_reliable, sub_reliable, subscriber_addr,
         ch](co::Coroutine *c) {
          BridgeTransmitterCoroutine(ch, pub_reliable, sub_reliable,
                                     subscriber_addr, c);
        },
        absl::StrFormat("Bridge transmitter for %s", channel->first).c_str()));
  } else {
    logger_.Log(toolbelt::LogLevel::kDebug, "I don't publish channel %s",
                subscribe.channel_name().c_str());
  }
}

void Server::GratuitousAdvertiseCoroutine(co::Coroutine *c) {
  constexpr int kPeriodSecs = 5;
  for (;;) {
    c->Sleep(kPeriodSecs);
    for (auto &channel : channels_) {
      if (!channel.second->IsLocal() && !channel.second->IsBridgePublisher()) {
        SendAdvertise(channel.first, channel.second->IsReliable());
      }
    }
  }
}

} // namespace subspace
