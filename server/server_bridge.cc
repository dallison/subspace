// Copyright 2023-2026 David Allison
// All Rights Reserved
// Shadow support is Copyright 2026 Cruise LLC
// Tunnel support is Copyright 2026 General Motors LLC
// See LICENSE file for licensing information.

#include "server/server.h"
#include "absl/strings/str_format.h"
#include "proto/subspace.pb.h"
#include "toolbelt/sockets.h"
#include <sys/socket.h>

namespace subspace {

// VMADDR_CID_ANY (== (uint32_t)-1) without requiring <linux/vm_sockets.h>,
// which is Linux-only.  Used to bind a vsock listener that accepts connections
// to any of the host's local context ids.
static constexpr uint32_t kVsockCidAny = 0xFFFFFFFFu;

static absl::StatusOr<toolbelt::SocketAddress>
BridgeAddressWithPort(const toolbelt::SocketAddress &addr, int port) {
  switch (addr.Type()) {
  case toolbelt::SocketAddress::kAddressInet:
    return toolbelt::SocketAddress(
        addr.GetInetAddress().IpAddressInNetworkOrder(), port);
  case toolbelt::SocketAddress::kAddressVirtual:
    return toolbelt::SocketAddress(addr.GetVirtualAddress().Cid(), port);
  default:
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported bridge listener address: %s", addr.ToString()));
  }
}

absl::StatusOr<toolbelt::SocketAddress>
ParseChannelAddress(const ChannelAddress &address, const char *field_name) {
  constexpr size_t kAddressSize = sizeof(uint32_t);
  if (address.address().size() != kAddressSize) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "%s has invalid address length %zu; expected %zu", field_name,
        address.address().size(), kAddressSize));
  }

  switch (address.family()) {
  case ChannelAddress::FAMILY_INET: {
    in_addr ip_addr;
    memcpy(&ip_addr, address.address().data(), sizeof(ip_addr));
    // ChannelAddress stores IPv4 bytes in host order on the wire.
    ip_addr.s_addr = ntohl(ip_addr.s_addr);
    return toolbelt::SocketAddress(ip_addr, address.port());
  }
  case ChannelAddress::FAMILY_VSOCK: {
    uint32_t cid;
    memcpy(&cid, address.address().data(), sizeof(cid));
    return toolbelt::SocketAddress(cid, address.port());
  }
  default:
    return absl::InvalidArgumentError(absl::StrFormat(
        "%s has unsupported address family %d", field_name, address.family()));
  }
}

absl::Status Server::SetBridgePortRange(int first_port, int last_port,
                                        bool fallback_to_ephemeral) {
  if (first_port == 0 && last_port == 0) {
    bridge_port_range_ = {};
    bridge_ports_fallback_to_ephemeral_ = fallback_to_ephemeral;
    return absl::OkStatus();
  }
  if (first_port <= 0 || last_port <= 0 || first_port > 65535 ||
      last_port > 65535 || first_port > last_port) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "invalid bridge port range %d-%d", first_port, last_port));
  }

  bridge_port_range_ = {.first_port = first_port, .last_port = last_port};
  bridge_ports_fallback_to_ephemeral_ = fallback_to_ephemeral;
  return absl::OkStatus();
}

toolbelt::SocketAddress Server::BridgeBindBase() const {
  // vsock bridging: bind the listener to (VMADDR_CID_ANY, 0) so it accepts
  // connections addressed to any of this host's local CIDs and the kernel
  // assigns an ephemeral vsock port.  The CID we advertise to peers
  // (vsock_cid_) is handled separately at advertise time.
  if (use_vsock_) {
    return toolbelt::SocketAddress(kVsockCidAny, /*port=*/0);
  }
  // When advertising a separate (e.g. forwarded loopback) address, bind to the
  // any-address so the listener is reachable both on the local interface and
  // via a forwarded loopback port.  Otherwise bind to the local interface.
  if (bridge_advertise_address_.Valid()) {
    return toolbelt::InetAddress::AnyAddress(0);
  }
  return my_address_;
}

absl::Status Server::BindBridgeListener(async::StreamSocket &listener) {
  toolbelt::SocketAddress bind_base = BridgeBindBase();
  if (!bridge_port_range_.Enabled()) {
    return listener.Bind(toolbelt::SocketAddress::AnyPort(bind_base), true);
  }

  absl::Status last_status = absl::UnavailableError("no ports tried");
  for (int port = bridge_port_range_.first_port;
       port <= bridge_port_range_.last_port; ++port) {
    absl::StatusOr<toolbelt::SocketAddress> addr =
        BridgeAddressWithPort(bind_base, port);
    if (!addr.ok()) {
      return addr.status();
    }

    async::StreamSocket candidate;
    absl::Status status = candidate.Bind(*addr, true);
    if (status.ok()) {
      listener = std::move(candidate);
      return absl::OkStatus();
    }
    last_status = status;
  }

  if (bridge_ports_fallback_to_ephemeral_) {
    return listener.Bind(toolbelt::SocketAddress::AnyPort(bind_base), true);
  }

  return absl::UnavailableError(absl::StrFormat(
      "unable to bind bridge listener in configured port range %d-%d: %s",
      bridge_port_range_.first_port, bridge_port_range_.last_port,
      last_status.ToString()));
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

} // namespace subspace
