// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "client/client.h"
#include "client_handler.h"
#include "proto/subspace.pb.h"
#include "toolbelt/clock.h"
#include "toolbelt/hexdump.h"
#include "toolbelt/sockets.h"
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <ifaddrs.h>
#include <net/if.h>
#include <setjmp.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace subspace {

// In multithreaded tests we can't dlclose the plugins because the dynamic
// linker doesn't play well with threads.
static std::atomic<bool> close_plugins_on_shutdown = false;
void ClosePluginsOnShutdown() { close_plugins_on_shutdown = true; }

bool ShouldClosePluginsOnShutdown() { return close_plugins_on_shutdown; }

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
               int disc_port, int peer_port, bool local, int notify_fd,
               int initial_ordinal, bool wait_for_clients,
               bool publish_server_channels)
    : socket_name_(socket_name), interface_(interface),
      discovery_port_(disc_port), discovery_peer_port_(peer_port),
      local_(local), notify_fd_(notify_fd), scheduler_(scheduler),
      logger_("Subspace server"), initial_ordinal_(initial_ordinal),
      wait_for_clients_(wait_for_clients),
      publish_server_channels_(publish_server_channels) {
  CreateShutdownTrigger();
}

Server::Server(co::CoroutineScheduler &scheduler,
               const std::string &socket_name, const std::string &interface,
               const toolbelt::InetAddress &peer, int disc_port, int peer_port,
               bool local, int notify_fd, int initial_ordinal,
               bool wait_for_clients, bool publish_server_channels)
    : socket_name_(socket_name), interface_(interface), peer_address_(peer),
      discovery_port_(disc_port), discovery_peer_port_(peer_port),
      local_(local), notify_fd_(notify_fd), scheduler_(scheduler),
      logger_("Subspace server"), initial_ordinal_(initial_ordinal),
      wait_for_clients_(wait_for_clients),
      publish_server_channels_(publish_server_channels) {
  CreateShutdownTrigger();
}

Server::~Server() {
  // Clear this before other data members get destroyed.
  client_handlers_.clear();
}

void Server::Stop(bool force) {
  if (shutting_down_) {
    return;
  }
  if (force || !wait_for_clients_) {
    scheduler_.Stop();
    return;
  }
  shutting_down_ = true;
  for (auto &plugin : plugins_) {
    plugin->interface->OnShutdown();
  }
  shutdown_trigger_fd_.Trigger();
  NotifyViaFd(kServerWaiting);
}

void Server::CreateShutdownTrigger() {
  auto fd = toolbelt::TriggerFd::Create();
  if (!fd.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create shutdown trigger fd: %s",
                fd.status().ToString().c_str());
    abort();
  }
  shutdown_trigger_fd_ = std::move(*fd);
}

absl::StatusOr<toolbelt::FileDescriptor>
Server::CreateBridgeNotificationPipe() {
  auto status = toolbelt::Pipe::Create();
  if (!status.ok()) {
    return absl::InternalError(
        absl::StrFormat("Failed to create bridge notification pipe: %s",
                        status.status().ToString().c_str()));
  }
  bridge_notification_pipe_ = *status;
  return bridge_notification_pipe_.ReadFd();
}

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
void Server::ListenerCoroutine(toolbelt::UnixSocket &listen_socket) {
  while (!shutting_down_) {
    absl::Status status = HandleIncomingConnection(listen_socket);
    if (!status.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unable to make incoming connection: %s",
                  status.ToString().c_str());
    }
  }
}

void Server::NotifyViaFd(int64_t val) {
  if (!notify_fd_.Valid()) {
    return;
  }
  const char *p = reinterpret_cast<const char *>(&val);
  size_t remaining = sizeof(val);
  while (remaining > 0) {
    ssize_t n = ::write(notify_fd_.Fd(), p, remaining);
    if (n <= 0) {
      return;
    }
    remaining -= n;
    p += n;
  }
}

void Server::ForeachChannel(std::function<void(ServerChannel *)> func) {
  for (auto &channel : channels_) {
    func(channel.second.get());
  }
}

void Server::CleanupAfterSession() {
  std::string session_shm_file_prefix =
      "subspace_." + std::to_string(session_id_);

#if defined(__APPLE__)
  // Remove all files starting with "subspace_SESSION" in /tmp.  These refer to
  // shared memory segments names "subspace_INODE".
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    if (entry.path().filename().string().rfind(session_shm_file_prefix, 0) ==
        0) {
      // Extrace the node and remove the shared memory segment.
      // The name of the shared memory segment is "subspace_<inode>".
      auto status = Channel::MacOsSharedMemoryName(entry.path().string());
      if (status.ok()) {
        shm_unlink(status->c_str());
      }
      (void)std::filesystem::remove(entry.path());
    }
  }
  // Remove all files in /tmp that contain .scb., .ccb. or .bcb. in the name.
  std::string ccb_shm_prefix = absl::StrFormat(".%d.ccb.", session_id_);
  std::string bcb_shm_prefix = absl::StrFormat(".%d.bcb.", session_id_);
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    std::string filename = entry.path().filename().string();
    if (filename.find(ccb_shm_prefix) != std::string::npos ||
        filename.find(bcb_shm_prefix) != std::string::npos) {
      (void)std::filesystem::remove(entry.path());
      // There is also a shm segment with the same name as the file.
      (void)shm_unlink(entry.path().filename().c_str());
    }
  }

#else
  // Remove all files starting with "subspace_SESSION" in /dev/shm.
  for (const auto &entry : std::filesystem::directory_iterator("/dev/shm")) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind(session_shm_file_prefix, 0) == 0) {
      (void)std::filesystem::remove(entry.path());
    }
  }
#endif
}

void Server::CleanupFilesystem() {
  logger_.Log(toolbelt::LogLevel::kInfo, "Cleaning up filesystem...");
#if defined(__APPLE__)
  // Remove all files starting with "subspace_" in /tmp.  These refer to
  // shared memory segments names "subspace_INODE".
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    if (entry.path().filename().string().rfind("subspace_", 0) == 0) {
      // Extrace the node and remove the shared memory segment.
      // The name of the shared memory segment is "subspace_<inode>".
      auto status = Channel::MacOsSharedMemoryName(entry.path().string());
      if (status.ok()) {
        shm_unlink(status->c_str());
      }
      (void)std::filesystem::remove(entry.path());
    }
  }
  // Remove all files in /tmp that contain .scb., .ccb. or .bcb. in the name.
  for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
    std::string filename = entry.path().filename().string();
    if (filename.find(".ccb.") != std::string::npos ||
        filename.find(".bcb.") != std::string::npos) {
      (void)std::filesystem::remove(entry.path());
      // There is also a shm segment with the same name as the file.
      (void)shm_unlink(entry.path().filename().c_str());
    }
  }

#else
  // Remove all files starting with "subspace_" in /dev/shm.
  for (const auto &entry : std::filesystem::directory_iterator("/dev/shm")) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind("subspace_", 0) == 0) {
      (void)std::filesystem::remove(entry.path());
    }
  }
#endif
}

absl::Status Server::Run() {
  std::vector<struct pollfd> poll_fds;

#ifndef __linux__
  // Remove socket name if it exists.  On Non-Linux systems the socket
  // is a file in the file system.  On Linux it's in the abstract
  // namespace (not a file).
  remove(socket_name_.c_str());
#endif
  session_id_ = AllocateSessionId();

  toolbelt::UnixSocket listen_socket;
  absl::Status status = listen_socket.Bind(socket_name_, true);
  if (!status.ok()) {
    return status;
  }

  // Notify listener that the server is ready.  Do this in a coroutine so that
  // it executes when we start running.
  if (notify_fd_.Valid()) {
    scheduler_.Spawn([this]() { NotifyViaFd(kServerReady); });
  }
  OnReady();

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
  server_id_ = absl::StrFormat("%s.%s.%d", hostname, socket_name_, getpid());

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

    my_address_ = ip_addr;
    // Bind the discovery transmitter to the network and any free
    // port on the requested interface.
    if (absl::Status s = discovery_transmitter_.Bind(ip_addr); !s.ok()) {
      return s;
    }

    if (peer_address_.Valid()) {
      // If peer address is supplied, use it.
      discovery_addr_ = peer_address_;
    } else {
      // Otherwise use the broadcast address.
      discovery_addr_ = bcast_addr;
      discovery_addr_.SetPort(discovery_peer_port_);
      if (absl::Status s = discovery_transmitter_.SetBroadcast(); !s.ok()) {
        return s;
      }
    }

    // Open the discovery receiver socket.
    if (absl::Status s = discovery_receiver_.Bind(
            toolbelt::InetAddress::AnyAddress(discovery_port_));
        !s.ok()) {
      return s;
    }
  }

  // Start the listener coroutine.
  scheduler_.Spawn(
      [this, &listen_socket]() { ListenerCoroutine(listen_socket); },
      {.name = "Listener UDS",
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

  if (publish_server_channels_) {
    // Start the channel directory coroutine.
    scheduler_.Spawn([this]() { ChannelDirectoryCoroutine(); },
                     {.name = "Channel directory",
                      .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

    // Start the channel stats coroutine.
    scheduler_.Spawn([this]() { StatisticsCoroutine(); },
                     {.name = "Channel stats",
                      .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }

  if (!local_) {
    // Start the discovery receiver coroutine.
    scheduler_.Spawn([this]() { DiscoveryReceiverCoroutine(); },
                     {.name = "Discovery receiver",
                      .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

    // Start the gratuitous Advertiser coroutine.  This sends Advertise messages
    // every 5 seconds.
    scheduler_.Spawn([this]() { GratuitousAdvertiseCoroutine(); },
                     {.name = "Gratuitous advertiser",
                      .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
  }

  // Run the coroutine main loop.
  scheduler_.Run();

  // Notify listener that we're stopped.
  NotifyViaFd(kServerStopped);
  return absl::OkStatus();
}

absl::Status
Server::HandleIncomingConnection(toolbelt::UnixSocket &listen_socket) {
  absl::StatusOr<toolbelt::UnixSocket> s = listen_socket.Accept(co::self);
  if (!s.ok()) {
    return s.status();
  }
  client_handlers_.push_back(
      std::make_unique<ClientHandler>(this, std::move(*s)));
  ClientHandler *handler_ptr = client_handlers_.back().get();

  scheduler_.Spawn(
      [this, handler_ptr]() {
        handler_ptr->Run();
        CloseHandler(handler_ptr);
      },
      {.name = "Client handler",
       .interrupt_fd =
           wait_for_clients_ ? shutdown_trigger_fd_.GetPollFd().Fd() : -1});

  return absl::OkStatus();
}

absl::StatusOr<ServerChannel *>
Server::CreateMultiplexer(const std::string &channel_name, int slot_size,
                          int num_slots, std::string type) {
  absl::StatusOr<int> channel_id = channel_ids_.Allocate("mux");
  if (!channel_id.ok()) {
    return channel_id.status();
  }
  logger_.Log(toolbelt::LogLevel::kDebug,
              "Creating multiplexer %s with %d slots", channel_name.c_str(),
              num_slots);
  ServerChannel *channel = new ChannelMultiplexer(
      *channel_id, channel_name, num_slots, std::move(type), session_id_);
  channel->SetDebug(logger_.GetLogLevel() <= toolbelt::LogLevel::kVerboseDebug);

  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  channels_.emplace(std::make_pair(channel_name, channel));
  return channel;
}

absl::StatusOr<ServerChannel *>
Server::CreateChannel(const std::string &channel_name, int slot_size,
                      int num_slots, const std::string &mux, int vchan_id,
                      std::string type) {
  if (!mux.empty()) {
    ServerChannel *mux_channel = FindChannel(mux);
    if (mux_channel == nullptr) {
      // No mux found, create one.
      absl::StatusOr<ServerChannel *> m =
          CreateMultiplexer(mux, slot_size, num_slots, type);
      if (!m.ok()) {
        return m.status();
      }
      mux_channel = *m;
    }
    if (!mux_channel->IsMux()) {
      return absl::InternalError(
          absl::StrFormat("Channel %s is not a multiplexer", mux));
    }
    if (mux_channel->IsPlaceholder()) {
      // Remap the memory now that we know the slots.
      absl::Status status = RemapChannel(mux_channel, slot_size, num_slots);
      if (!status.ok()) {
        return status;
      }
    }
    // Create a virtual channel associated with the multiplexer.
    ChannelMultiplexer *m = static_cast<ChannelMultiplexer *>(mux_channel);
    absl::StatusOr<std::unique_ptr<VirtualChannel>> vchan =
        m->CreateVirtualChannel(*this, channel_name, vchan_id);
    if (!vchan.ok()) {
      return vchan.status();
    }
    ServerChannel *channel = vchan->get();

    logger_.Log(toolbelt::LogLevel::kDebug,
                "Creating virtual channel %s with %d slots using mux %s",
                channel_name.c_str(), num_slots, mux.c_str());
    // The channels_ map owns all the server channels.
    channels_.emplace(std::make_pair(channel_name, std::move(*vchan)));
    OnNewChannel(channel_name);
    return channel;
  }

  absl::StatusOr<int> channel_id = channel_ids_.Allocate("channel");
  if (!channel_id.ok()) {
    return channel_id.status();
  }
  ServerChannel *channel =
      new ServerChannel(*channel_id, channel_name, num_slots, std::move(type),
                        false, session_id_);
  channel->SetDebug(logger_.GetLogLevel() <= toolbelt::LogLevel::kVerboseDebug);
  channel->SetLastKnownSlotSize(slot_size);

  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots, initial_ordinal_);
  if (!fds.ok()) {
    return fds.status();
  }
  channel->SetSharedMemoryFds(std::move(*fds));
  channels_.emplace(std::make_pair(channel_name, channel));
  OnNewChannel(channel_name);

  return channel;
}

uint64_t Server::GetVirtualMemoryUsage() const {
  uint64_t size = 0;
  for (const auto &channel : channels_) {
    size += channel.second->GetVirtualMemoryUsage();
  }
  return size;
}

absl::Status Server::RemapChannel(ServerChannel *channel, int slot_size,
                                  int num_slots) {
  if (channel->IsVirtual()) {
    ChannelMultiplexer *mux = static_cast<VirtualChannel *>(channel)->GetMux();
    logger_.Log(toolbelt::LogLevel::kDebug,
                "Remapping multiplexer %s with %d slots",
                channel->Name().c_str(), num_slots);
    return RemapChannel(mux, slot_size, num_slots);
  }
  absl::StatusOr<SharedMemoryFds> fds =
      channel->Allocate(scb_fd_, slot_size, num_slots, initial_ordinal_);
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
  OnRemoveChannel(channel->Name());
  channel->RemoveBuffer(session_id_);
  channel_ids_.Clear(channel->GetChannelId());
  auto it = channels_.find(channel->Name());
  if (it == channels_.end()) {
    return;
  }
  if (it->second->IsVirtual()) {
    auto vchan = static_cast<VirtualChannel *>(it->second.get());
    ChannelMultiplexer *mux =
        vchan->GetMux();
    mux->RemoveVirtualChannel(vchan);
    if (mux->IsEmpty()) {
      RemoveChannel(mux);
    }
  }
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

void Server::ChannelDirectoryCoroutine() {
  // Coroutine aware client.
  Client client(co::self);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(
        toolbelt::LogLevel::kError,
        "Failed to initialize Subspace client for channel directory: %s",
        status.ToString().c_str());
    return;
  }
  constexpr int kDirectorySlotSize = 1024;
  constexpr int kDirectoryNumSlots = 32;

  absl::StatusOr<Publisher> channel_directory = client.CreatePublisher(
      "/subspace/ChannelDirectory", kDirectorySlotSize, kDirectoryNumSlots,
      PublisherOptions().SetType("subspace.ChannelDirectory"));
  if (!channel_directory.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create channel directory channel: %s",
                channel_directory.status().ToString().c_str());
    return;
  }
  while (!shutting_down_) {
    co::Wait(channel_directory_trigger_fd_.GetPollFd().Fd(), POLLIN);
    channel_directory_trigger_fd_.Clear();

    ChannelDirectory directory;
    directory.set_server_id(server_id_);
    for (auto &channel : channels_) {
      auto info = directory.add_channels();
      channel.second->GetChannelInfo(info);
    }
    int64_t length = directory.ByteSizeLong();
    absl::StatusOr<void *> buffer =
        channel_directory->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get channel directory buffer: %s",
                  buffer.status().ToString().c_str());
      return;
    }
    bool ok = directory.SerializeToArray(*buffer, length);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel directory");
      continue;
    }
    absl::StatusOr<const Message> s = channel_directory->PublishMessage(length);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish channel directory: %s",
                  s.status().ToString().c_str());
    }
  }
}

void Server::SendChannelDirectory() { channel_directory_trigger_fd_.Trigger(); }

void Server::StatisticsCoroutine() {
  Client client(co::self);
  absl::Status status = client.Init(socket_name_);
  if (!status.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to initialize Subspace client for statistics: %s",
                status.ToString().c_str());
    return;
  }
  constexpr int kStatsSlotSize = 1024;
  constexpr int kStatsNumSlots = 32;

  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      "/subspace/Statistics", kStatsSlotSize, kStatsNumSlots,
      PublisherOptions().SetType("subspace.Statistics"));
  if (!pub.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to create statistics channel: %s",
                pub.status().ToString().c_str());
    return;
  }

  constexpr int kPeriodSecs = 2;
  while (!shutting_down_) {
    co::Sleep(kPeriodSecs);
    Statistics stats;
    stats.set_timestamp(toolbelt::Now());
    stats.set_server_id(server_id_);
    for (auto &channel : channels_) {
      auto s = stats.add_channels();
      channel.second->GetChannelStats(s);
    }
    int64_t length = stats.ByteSizeLong();

    absl::StatusOr<void *> buffer = pub->GetMessageBuffer(int32_t(length));
    if (!buffer.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to get channel stats buffer: %s",
                  buffer.status().ToString().c_str());
      return;
    }
    bool ok = stats.SerializeToArray(*buffer, length);
    if (!ok) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to serialize channel stats");
      continue;
    }
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
  scheduler_.Spawn(
      [this, channel_name]() {
        logger_.Log(toolbelt::LogLevel::kDebug,
                    "Sending Query %s with discovery port %d",
                    channel_name.c_str(), discovery_port_);
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
            discovery_transmitter_.SendTo(discovery_addr_, buffer, length);
        if (!s.ok()) {
          logger_.Log(toolbelt::LogLevel::kError, "Failed to send Query: %s",
                      s.ToString().c_str());
          return;
        }
      },
      {.name = "Send discovery query",
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
}

// Send an advertise discovery message over UDP.
void Server::SendAdvertise(const std::string &channel_name, bool reliable) {
  if (local_) {
    return;
  }
  // Spawn a coroutine to send the Publish message.
  scheduler_.Spawn(
      [this, channel_name, reliable]() {
        logger_.Log(toolbelt::LogLevel::kDebug,
                    "Sending Advertise %s with discovery port %d",
                    channel_name.c_str(), discovery_port_);
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
            discovery_transmitter_.SendTo(discovery_addr_, buffer, length);
        if (!s.ok()) {
          logger_.Log(toolbelt::LogLevel::kError,
                      "Failed to send Advertise: %s", s.ToString().c_str());
          return;
        }
      },
      {.name = "Send discovery advertise",
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
}

// This coroutine receives discovery messages over UDP.
void Server::DiscoveryReceiverCoroutine() {
  char buffer[kDiscoveryBufferSize];

  for (;;) {
    toolbelt::InetAddress sender;
    absl::StatusOr<ssize_t> n = discovery_receiver_.ReceiveFrom(
        sender, buffer, sizeof(buffer), co::self);
    if (!n.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to read discovery message: %s",
                  n.status().ToString().c_str());
      continue;
    }
    Discovery disc;
    if (!disc.ParseFromArray(buffer, static_cast<int>(*n))) {
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

// This coroutine creates a subscriber to the given channel and sends
// every message received over the bridge to another server.  It first sends
// a 'Subscribed' message detailing the information about the channel then
// enters a loop reading messages sent to the channel on this side of the
// bridge and sending them over.
void Server::BridgeTransmitterCoroutine(ServerChannel *channel,
                                        bool pub_reliable, bool sub_reliable,
                                        toolbelt::SocketAddress subscriber,
                                        bool notify_retirement) {
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeTransmitterCoroutine running");
  toolbelt::StreamSocket bridge;
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

  toolbelt::StreamSocket retirement_listener;
  toolbelt::SocketAddress retirement_addr;

  if (notify_retirement) {
    // We want to be notified when the slot has been retired.
    subscribed.set_notify_retirement(true);
    // Allocate a listen socket to wait for an incoming connection from the
    // other server.

    absl::Status s = retirement_listener.Bind(
        toolbelt::SocketAddress::AnyPort(my_address_), true);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unable to bind socket for retirement receiver for %s: %s",
                  channel_name.c_str(), s.ToString().c_str());
      return;
    }
    retirement_addr = retirement_listener.BoundAddress();
    logger_.Log(toolbelt::LogLevel::kDebug, "Retirement listener: %s",
                retirement_addr.ToString().c_str());

    // Tell the other side where to connect to notify us of slot retirement
    auto *ret_addr = subscribed.mutable_retirement_socket();
    switch (retirement_addr.Type()) {
    case toolbelt::SocketAddress::kAddressInet: {
      in_addr ip_addr = retirement_addr.GetInetAddress().IpAddress();
      ret_addr->set_address(&ip_addr, sizeof(ip_addr));
      break;
    }
    case toolbelt::SocketAddress::kAddressVirtual: {
      uint32_t cid = retirement_addr.GetVirtualAddress().Cid();
      ret_addr->set_address(&cid, sizeof(cid));
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
      bridge.SendMessage(databuf, length, co::self);
  if (!n_sent_1.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send subscribed for %s: %s", channel_name.c_str(),
                n_sent_1.status().ToString().c_str());
    return;
  }

  Client client(co::self);
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
  std::vector<std::shared_ptr<ActiveMessage>> active_retirement_msgs;
  bool notifying_of_retirement = !channel->GetRetirementFds().empty();

  // Spawn a coroutine to read from the retirement connection.
  if (notifying_of_retirement) {
    active_retirement_msgs.resize(channel->NumSlots());
    scheduler_.Spawn(
        [this, &retirement_listener, &active_retirement_msgs]() {
          return RetirementReceiverCoroutine(retirement_listener,
                                             active_retirement_msgs);
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
      if (absl::StatusOr<ssize_t> n_sent_2 =
              bridge.SendMessage(data_addr, msglen, co::self);
          !n_sent_2.ok()) {
        done = true;
        logger_.Log(toolbelt::LogLevel::kError,
                    "Failed to send bridge message for %s: %s",
                    channel_name.c_str(), n_sent_2.status().ToString().c_str());

        break;
      }
      if (notifying_of_retirement) {
        // We need to keep track of the message so that we can retire it
        // when the other side retires the slot.
        active_retirement_msgs[msg->slot_id] = std::move(msg->active_message);
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

// This coroutine reads retirement messages from the bridge and removes
// the reference to the slot that has been retired on the other side.
// References to the active messages are kept in a vector, indexed by slot_id.
// If these references are the last reference to the slot, it will be retired
// on this side and the publisher's retirement FD is sent the slot.
void Server::RetirementReceiverCoroutine(
    toolbelt::StreamSocket &retirement_listener,
    std::vector<std::shared_ptr<ActiveMessage>> &active_retirement_msgs) {
  // Accept connection on the retirement listener socket.
  absl::StatusOr<toolbelt::StreamSocket> retirement_socket =
      retirement_listener.Accept(co::self);
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
        retirement_socket->ReceiveMessage(buffer, sizeof(buffer), co::self);
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

    if (slot_id < 0 || slot_id >= active_retirement_msgs.size()) {
      continue;
    }
    active_retirement_msgs[slot_id].reset();
  }
}

// Send a Subscribe message over UDP.
absl::Status
Server::SendSubscribeMessage(const std::string &channel_name, bool reliable,
                             toolbelt::InetAddress publisher,
                             toolbelt::StreamSocket &receiver_listener,
                             char *buffer, size_t buffer_size) {
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
    // IPv4 address.
    in_addr ip_addr = receiver_addr.GetInetAddress().IpAddress();
    sub_addr->set_address(&ip_addr, sizeof(ip_addr));
    break;
  }
  case toolbelt::SocketAddress::kAddressVirtual: {
    // Virtual address.
    uint32_t cid = receiver_addr.GetVirtualAddress().Cid();
    sub_addr->set_address(&cid, sizeof(cid));
    break;
  }
  default:
    return absl::InternalError(
        absl::StrFormat("Unsupported address type for subscribe: %s",
                        receiver_addr.ToString().c_str()));
  }
  sub_addr->set_port(receiver_addr.Port());

  bool ok = disc.SerializeToArray(buffer, static_cast<int>(buffer_size));
  if (!ok) {
    return absl::InternalError("Failed to serialize subscribe message");
  }
  int64_t length = disc.ByteSizeLong();
  logger_.Log(toolbelt::LogLevel::kDebug, "Sending subscribe to %s: %s",
              publisher.ToString().c_str(), disc.DebugString().c_str());
  absl::Status s =
      discovery_transmitter_.SendTo(publisher, buffer, length, co::self);
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
void Server::BridgeReceiverCoroutine(std::string channel_name,
                                     bool sub_reliable,
                                     toolbelt::InetAddress publisher) {
  // Open a listening TCP socket on a free port.
  logger_.Log(toolbelt::LogLevel::kDebug, "BridgeReceiverCoroutine running");
  char buffer[kDiscoveryBufferSize];

  toolbelt::StreamSocket receiver_listener;
  absl::Status s = receiver_listener.Bind(
      toolbelt::SocketAddress::AnyPort(my_address_), true);
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
                           receiver_listener, buffer, sizeof(buffer));
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to send Subscribe message for channel %s: %s",
                channel_name.c_str(), s.ToString().c_str());
    return;
  }

  // Accept connection on listen socket.
  absl::StatusOr<toolbelt::StreamSocket> bridge =
      receiver_listener.Accept(co::self);
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
      buffer + sizeof(int32_t), sizeof(buffer) - sizeof(int32_t), co::self);
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

  // Build a publisher to publish incoming bridge messages to the channel.
  Client client(co::self);
  s = client.Init(socket_name_);
  if (!s.ok()) {
    logger_.Log(toolbelt::LogLevel::kError,
                "Failed to connect to Subspace server: %s",
                s.ToString().c_str());
    return;
  }

  std::unique_ptr<toolbelt::StreamSocket> retirement_transmitter;

  if (subscribed.notify_retirement()) {
    retirement_transmitter = std::make_unique<toolbelt::StreamSocket>();
    toolbelt::SocketAddress retirement_addr;
    switch (my_address_.Type()) {
    case toolbelt::SocketAddress::kAddressInet: {
      // IPv4 address.
      struct sockaddr_in addr;
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_port = htons(subscribed.retirement_socket().port());
      addr.sin_addr.s_addr = ntohl(*reinterpret_cast<const uint32_t *>(
          subscribed.retirement_socket().address().data()));
      retirement_addr = toolbelt::SocketAddress(addr);
      break;
    }
    case toolbelt::SocketAddress::kAddressVirtual: {
      // Virtual address.
      struct sockaddr_vm addr;
      memset(&addr, 0, sizeof(addr));
      addr.svm_family = AF_VSOCK;
      addr.svm_port = subscribed.retirement_socket().port();
      addr.svm_cid = *reinterpret_cast<const uint32_t *>(
          subscribed.retirement_socket().address().data());
      retirement_addr = toolbelt::SocketAddress(addr);
      break;
    }
    default:
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unsupported address type for retirement notification: %s",
                  my_address_.ToString().c_str());
      return;
    }

    s = retirement_transmitter->Connect(retirement_addr);
    if (!s.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to connect to retirement socket for %s: %s",
                  channel_name.c_str(), s.ToString().c_str());
      return;
    }
  }
  // For a reliable publisher, this will send an activation message
  // to the local channel.
  absl::StatusOr<Publisher> pub = client.CreatePublisher(
      channel_name, subscribed.slot_size(), subscribed.num_slots(),
      PublisherOptions()
          .SetReliable(subscribed.reliable())
          .SetBridge(true)
          .SetNotifyRetirement(subscribed.notify_retirement()));
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
    scheduler_.Spawn(
        [this, retirement_fd = std::move(retirement_fd),
         &retirement_transmitter, channel_name]() mutable {
          RetirementCoroutine(channel_name, std::move(retirement_fd),
                              std::move(retirement_transmitter));
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
        after_padding, subscribed.slot_size() + kAdjustedPrefixLength,
        co::self);
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

    absl::StatusOr<const Message> pub_msg = pub->PublishMessageInternal(
        *n - kAdjustedPrefixLength, /*omit_prefix=*/true,
        /*omit_prefix_slot_id=*/true);
    if (!pub_msg.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to publish bridge message for %s: %s",
                  channel_name.c_str(), pub_msg.status().ToString().c_str());
    }
  }

  logger_.Log(toolbelt::LogLevel::kDebug,
              "Bridge receiver coroutine terminating");
  // Socket has been closed, we're done.
}

// This coroutine receives retirement notifications from the bridge publisher
// and sends those over the bridge to the server that originally published the
// message.  The slot ID sent back to the original server is the slot of the
// original message, not this side's slot.  That is held in the MessagePrefix
// which is kept intact.
void Server::RetirementCoroutine(
    const std::string &channel_name, toolbelt::FileDescriptor &&retirement_fd,
    std::unique_ptr<toolbelt::StreamSocket> retirement_transmitter) {
  logger_.Log(toolbelt::LogLevel::kDebug, "Retirement coroutine for %s running",
              channel_name.c_str());
  for (;;) {
    int32_t slot_id;
    absl::StatusOr<ssize_t> n =
        retirement_fd.Read(&slot_id, sizeof(slot_id), co::self);
    if (!n.ok()) {
      // Failed to read the slot ID, we're done.
      return;
    }
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
        buffer + sizeof(int32_t), msg.ByteSizeLong(), co::self);
    if (!nsent.ok()) {
      logger_.Log(toolbelt::LogLevel::kError,
                  "Failed to send retirement fd for %s: %s",
                  channel_name.c_str(), nsent.status().ToString().c_str());
      return;
    }
  }
}

void Server::SubscribeOverBridge(ServerChannel *channel, bool reliable,
                                 toolbelt::InetAddress publisher) {
  scheduler_.Spawn(
      [this, channel, reliable, publisher]() {
        BridgeReceiverCoroutine(channel->Name(), reliable, publisher);
      },
      {.name = absl::StrFormat("Bridge receiver for %s", channel->Name()),
       .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});
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

    int num_pubs, num_subs, num_bridge_pubs, num_bridge_subs;
    channel->second->CountUsers(num_pubs, num_subs, num_bridge_pubs,
                                num_bridge_subs);
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

    // The subscribe message contains the IP address and port of the
    // socket listening for our connection for the channel.  Extract it.
    toolbelt::SocketAddress subscriber_addr;
    switch (my_address_.Type()) {
    case toolbelt::SocketAddress::kAddressInet: {
      in_addr subscriber_ip;
      memcpy(&subscriber_ip, subscribe.receiver().address().data(),
             sizeof(subscriber_ip));
      subscriber_addr =
          toolbelt::SocketAddress(subscriber_ip, subscribe.receiver().port());
      break;
    }
    case toolbelt::SocketAddress::kAddressVirtual: {
      uint32_t cid = *reinterpret_cast<const uint32_t *>(
          subscribe.receiver().address().data());
      subscriber_addr =
          toolbelt::SocketAddress(cid, subscribe.receiver().port());
      break;
    }
    default:
      logger_.Log(toolbelt::LogLevel::kError,
                  "Unknown address type for subscribe receiver");
      return;
    }

    bool notify_retirement = !channel->second->GetRetirementFds().empty();
    scheduler_.Spawn(
        [this, ch, pub_reliable, sub_reliable,
         subscriber_addr = std::move(subscriber_addr), notify_retirement]() {
          BridgeTransmitterCoroutine(ch, pub_reliable, sub_reliable,
                                     std::move(subscriber_addr),
                                     notify_retirement);
        },
        {.name = absl::StrFormat("Bridge transmitter for %s", channel->first)
                     .c_str(),
         .interrupt_fd = shutdown_trigger_fd_.GetPollFd().Fd()});

  } else {
    logger_.Log(toolbelt::LogLevel::kDebug, "I don't publish channel %s",
                subscribe.channel_name().c_str());
  }
}

void Server::GratuitousAdvertiseCoroutine() {
  constexpr int kPeriodSecs = 5;
  for (;;) {
    co::Sleep(kPeriodSecs);
    for (auto &channel : channels_) {
      if (!channel.second->IsLocal() && !channel.second->IsBridgePublisher()) {
        SendAdvertise(channel.first, channel.second->IsReliable());
      }
    }
  }
}

absl::Status Server::LoadPlugin(const std::string &name,
                                const std::string &path) {
  std::lock_guard<std::mutex> lock(plugin_lock_);

  void *handle = nullptr;
  if (path != "BUILTIN") {
    handle = dlopen(path.c_str(), RTLD_LAZY);
    if (handle == nullptr) {
      return absl::InternalError(
          absl::StrFormat("Can't open plugin file %s: %s", path, dlerror()));
    }
  }
  // Form the name of the init function and find it in the shared object.
  std::string interfaceFunc = absl::StrFormat("%s_Create", name);
  void *func = dlsym(handle, interfaceFunc.c_str());
  if (func == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Can't find plugin initialization symbol %s: %s",
                        interfaceFunc, dlerror()));
  }
  // Call the init function to get the interface.
  using InitFunc = PluginInterface *(*)();
  InitFunc init = reinterpret_cast<InitFunc>(func);
  auto interface = init();

  // Call the OnStartup function in the loaded plugin.
  absl::Status status = interface->OnStartup(*this, name);
  if (!status.ok()) {
    return status;
  }
  plugins_.push_back(std::make_unique<Plugin>(
      name, handle, std::unique_ptr<PluginInterface>(interface)));
  return absl::OkStatus();
}

absl::Status Server::UnloadPlugin(const std::string &name) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  auto it = std::find_if(
      plugins_.begin(), plugins_.end(),
      [&name](const std::unique_ptr<Plugin> &p) { return p->name == name; });
  if (it == plugins_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("No such plugin %s loaded", name.c_str()));
  }
  plugins_.erase(it);
  return absl::OkStatus();
}

void Server::OnReady() {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnReady(*this);
  }
}

void Server::OnNewChannel(const std::string &channel_name) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewChannel(*this, channel_name);
  }
}

void Server::OnRemoveChannel(const std::string &channel_name) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemoveChannel(*this, channel_name);
  }
}

void Server::OnNewPublisher(const std::string &channel_name, int publisher_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewPublisher(*this, channel_name, publisher_id);
  }
}

void Server::OnRemovePublisher(const std::string &channel_name,
                               int publisher_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemovePublisher(*this, channel_name, publisher_id);
  }
}

void Server::OnNewSubscriber(const std::string &channel_name,
                             int subscriber_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnNewSubscriber(*this, channel_name, subscriber_id);
  }
}

void Server::OnRemoveSubscriber(const std::string &channel_name,
                                int subscriber_id) {
  std::lock_guard<std::mutex> lock(plugin_lock_);
  for (const auto &plugin : plugins_) {
    plugin->interface->OnRemoveSubscriber(*this, channel_name, subscriber_id);
  }
}

} // namespace subspace
