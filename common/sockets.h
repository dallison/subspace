// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_SOCKETS_H
#define __COMMON_SOCKETS_H
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/fd.h"
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
class InetAddress {
public:
  InetAddress() = default;

  // An address with INADDR_ANY and the give port number (host order)
  InetAddress(int port);

  // An address with a given host order IP address and a host order
  // port.
  InetAddress(const in_addr &ip, int port);

  // An address for a given hostname and port.  Performs name server lookup.
  InetAddress(const std::string &hostname, int port);

  // An address from a pre-constructed socket address in network order.
  InetAddress(const struct sockaddr_in &addr) : addr_(addr) {}

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
  friend bool operator==(const InetAddress &a, const InetAddress &b);
  template <typename H> friend H AbslHashValue(H h, const InetAddress &a);

  static InetAddress BroadcastAddress(int port);
  static InetAddress AnyAddress(int port);

private:
  struct sockaddr_in addr_; // In network byte order.
  bool valid_ = false;
};

inline bool operator==(const InetAddress &a, const InetAddress &b) {
  return memcmp(&a.addr_, &b.addr_, sizeof(a.addr_)) == 0;
}

template <typename H> inline H AbslHashValue(H h, const InetAddress &a) {
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
  FileDescriptor fd_;
  bool connected_ = false;
  bool is_nonblocking_ = false;
};

// A Unix Domain socket bound to a pathname.  Depending on the OS, this
// may or may not be in the file system.
class UnixSocket : public Socket {
public:
  UnixSocket();
  explicit UnixSocket(int fd, bool connected = false) : Socket(fd, connected) {}
  UnixSocket(UnixSocket &&s) : Socket(std::move(s)) {}
  ~UnixSocket() = default;

  absl::Status Bind(const std::string &pathname, bool listen);
  absl::Status Connect(const std::string &pathname);

  absl::StatusOr<UnixSocket> Accept(co::Coroutine *c = nullptr);

  absl::Status SendFds(const std::vector<FileDescriptor> &fds,
                       co::Coroutine *c = nullptr);
  absl::Status ReceiveFds(std::vector<FileDescriptor> &fds,
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

  absl::Status Connect(const InetAddress &addr);

  const InetAddress &BoundAddress() { return bound_address_; }

  absl::Status SetReuseAddr();

protected:
  InetAddress bound_address_;
};

// A socket that uses the UDP datagram protocol.
class UDPSocket : public NetworkSocket {
public:
  UDPSocket();
  explicit UDPSocket(int fd, bool connected = false)
      : NetworkSocket(fd, connected) {}
  UDPSocket(UDPSocket &&s) : NetworkSocket(std::move(s)) {}
  ~UDPSocket() = default;

  absl::Status Bind(const InetAddress &addr);

  // NOTE: Read and Write may or may not work on UDP sockets.  Use SendTo and
  // Receive for datagrams.
  absl::Status SendTo(const InetAddress &addr, const void *buffer,
                      size_t length, co::Coroutine *c = nullptr);
  absl::StatusOr<ssize_t> Receive(void *buffer, size_t buflen,
                                  co::Coroutine *c = nullptr);
  absl::StatusOr<ssize_t> ReceiveFrom(InetAddress &sender, void *buffer,
                                      size_t buflen,
                                      co::Coroutine *c = nullptr);
  absl::Status SetBroadcast();
};

// A TCP based socket.
class TCPSocket : public NetworkSocket {
public:
  TCPSocket();
  explicit TCPSocket(int fd, bool connected = false)
      : NetworkSocket(fd, connected) {}
  TCPSocket(TCPSocket &&s) : NetworkSocket(std::move(s)) {}
  ~TCPSocket() = default;

  absl::Status Bind(const InetAddress &addr, bool listen);

  absl::StatusOr<TCPSocket> Accept(co::Coroutine *c = nullptr);
};
} // namespace subspace

#endif // __COMMON_SOCKETS_H