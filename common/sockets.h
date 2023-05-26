// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_SOCKETS_H
#define __COMMON_SOCKETS_H
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "toolbelt/fd.h"
#include "coroutine.h"
#include <netinet/in.h>
#include <string>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

namespace subspace {

static constexpr size_t kMaxMessage = 4096;

// This is an internet protocol address and port.  Used as the
// endpoint address for all network based sockets.  Only IPv4 is
// supported for now.
// TODO: support IPv6.
class toolbelt::InetAddress {
public:
  toolbelt::InetAddress() = default;

  // An address with INADDR_ANY and the give port number (host order)
  toolbelt::InetAddress(int port);

  // An address with a given host order IP address and a host order
  // port.
  toolbelt::InetAddress(const in_addr &ip, int port);

  // An address for a given hostname and port.  Performs name server lookup.
  toolbelt::InetAddress(const std::string &hostname, int port);

  // An address from a pre-constructed socket address in network order.
  toolbelt::InetAddress(const struct sockaddr_in &addr) : addr_(addr) {}

  const sockaddr_in &GetAddress() const { return addr_; }
  socklen_t GetLength() const { return sizeof(addr_); }
  bool Valid() const { return valid_; }

  // IP address and port are returned in host byte order.
  in_addr IpAddress() const { return {ntohl(addr_.sin_addr.s_addr)}; }
  int Port() const { return ntohs(addr_.sin_port); }

  // Port is in host byte order.
  void SetPort(int port) { addr_.sin_port = htons(port); }

  std::string ToString() const;

  // Provide support for Abseil hashing.
  friend bool operator==(const toolbelt::InetAddress &a, const toolbelt::InetAddress &b);
  template <typename H> friend H AbslHashValue(H h, const toolbelt::InetAddress &a);

  static toolbelt::InetAddress BroadcastAddress(int port);
  static toolbelt::InetAddress AnyAddress(int port);

private:
  struct sockaddr_in addr_; // In network byte order.
  bool valid_ = false;
};

inline bool operator==(const toolbelt::InetAddress &a, const toolbelt::InetAddress &b) {
  return memcmp(&a.addr_, &b.addr_, sizeof(a.addr_)) == 0;
}

template <typename H> inline H AbslHashValue(H h, const toolbelt::InetAddress &a) {
  return H::combine_contiguous(
      std::move(h), reinterpret_cast<const char *>(&a.addr_), sizeof(a.addr_));
}

// This is a general socket initialized with a file descriptor.  Subclasses
// implement the different socket types.
class Socket {
public:
  Socket() = default;
  explicit Socket(int fd, bool connected = false)
      : fd_(fd), connected_(connected) {}
  Socket(const Socket &s) = delete;
  Socket(Socket &&s) : fd_(std::move(s.fd_)), connected_(s.connected_) {
    s.connected_ = false;
  }
  Socket &operator=(const Socket &s) = delete;
  Socket &operator=(Socket &&s) {
    fd_ = std::move(s.fd_);
    connected_ = s.connected_;
    s.connected_ = false;
    return *this;
  }
  ~Socket() = default;

  void Close() {
    fd_.Close();
    connected_ = false;
  }
  bool Connected() const { return fd_.Valid() && connected_; }

  // Send and receive raw buffers.
  absl::StatusOr<ssize_t> Receive(char *buffer, size_t buflen,
                                  co::Coroutine *c = nullptr);
  absl::StatusOr<ssize_t> Send(const char *buffer, size_t length,
                               co::Coroutine *c = nullptr);
  // Send and receive length-delimited message.  The length is a 4-byte
  // network byte order (big endian) int as the first 4 bytes and
  // contains the length of the message.
  absl::StatusOr<ssize_t> ReceiveMessage(char *buffer, size_t buflen,
                                         co::Coroutine *c = nullptr);

  // For SendMessage, the buffer pointer must be 4 bytes beyond
  // the actual buffer start, which must be length+4 bytes
  // long.  We write exactly length+4 bytes to the socket starting
  // at buffer-4.  This is to allow us to do a single send
  // to the socket rather than splitting it into 2.
  absl::StatusOr<ssize_t> SendMessage(char *buffer, size_t length,
                                      co::Coroutine *c = nullptr);

  absl::Status SetNonBlocking() {
    if (absl::Status s = fd_.SetNonBlocking(); !s.ok()) {
      return s;
    }
    is_nonblocking_ = true;
    return absl::OkStatus();
  }

  bool IsNonBlocking() const { return is_nonblocking_; }
  bool IsBlocking() const { return !is_nonblocking_; }

protected:
  toolbelt::FileDescriptor fd_;
  bool connected_ = false;
  bool is_nonblocking_ = false;
};

// A Unix Domain socket bound to a pathname.  Depending on the OS, this
// may or may not be in the file system.
class toolbelt::UnixSocket : public Socket {
public:
  toolbelt::UnixSocket();
  explicit toolbelt::UnixSocket(int fd, bool connected = false) : Socket(fd, connected) {}
  toolbelt::UnixSocket(toolbelt::UnixSocket &&s) : Socket(std::move(s)) {}
  ~toolbelt::UnixSocket() = default;

  absl::Status Bind(const std::string &pathname, bool listen);
  absl::Status Connect(const std::string &pathname);

  absl::StatusOr<toolbelt::UnixSocket> Accept(co::Coroutine *c = nullptr);

  absl::Status SendFds(const std::vector<toolbelt::FileDescriptor> &fds,
                       co::Coroutine *c = nullptr);
  absl::Status ReceiveFds(std::vector<toolbelt::FileDescriptor> &fds,
                          co::Coroutine *c = nullptr);
};

// A socket for communication across the network.  This is the base
// class for UDP and TCP sockets.
class NetworkSocket : public Socket {
public:
  NetworkSocket() = default;
  explicit NetworkSocket(int fd, bool connected = false)
      : Socket(fd, connected) {}
  NetworkSocket(NetworkSocket &&s) : Socket(std::move(s)) {}
  ~NetworkSocket() = default;

  absl::Status Connect(const toolbelt::InetAddress &addr);

  const toolbelt::InetAddress &BoundAddress() { return bound_address_; }

  absl::Status SetReuseAddr();

protected:
  toolbelt::InetAddress bound_address_;
};

// A socket that uses the UDP datagram protocol.
class toolbelt::UDPSocket : public NetworkSocket {
public:
  toolbelt::UDPSocket();
  explicit toolbelt::UDPSocket(int fd, bool connected = false)
      : NetworkSocket(fd, connected) {}
  toolbelt::UDPSocket(toolbelt::UDPSocket &&s) : NetworkSocket(std::move(s)) {}
  ~toolbelt::UDPSocket() = default;

  absl::Status Bind(const toolbelt::InetAddress &addr);

  // NOTE: Read and Write may or may not work on UDP sockets.  Use SendTo and
  // Receive for datagrams.
  absl::Status SendTo(const toolbelt::InetAddress &addr, const void *buffer,
                      size_t length, co::Coroutine *c = nullptr);
  absl::StatusOr<ssize_t> Receive(void *buffer, size_t buflen,
                                  co::Coroutine *c = nullptr);
  absl::StatusOr<ssize_t> ReceiveFrom(toolbelt::InetAddress &sender, void *buffer,
                                      size_t buflen,
                                      co::Coroutine *c = nullptr);
  absl::Status SetBroadcast();
};

// A TCP based socket.
class toolbelt::TCPSocket : public NetworkSocket {
public:
  toolbelt::TCPSocket();
  explicit toolbelt::TCPSocket(int fd, bool connected = false)
      : NetworkSocket(fd, connected) {}
  toolbelt::TCPSocket(toolbelt::TCPSocket &&s) : NetworkSocket(std::move(s)) {}
  ~toolbelt::TCPSocket() = default;

  absl::Status Bind(const toolbelt::InetAddress &addr, bool listen);

  absl::StatusOr<toolbelt::TCPSocket> Accept(co::Coroutine *c = nullptr);
};
} // namespace subspace

#endif // __COMMON_SOCKETS_H