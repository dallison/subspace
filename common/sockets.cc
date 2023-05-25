// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/sockets.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <string>

#include "absl/strings/str_format.h"
#include "common/hexdump.h"

namespace subspace {

InetAddress InetAddress::BroadcastAddress(int port) {
  return InetAddress("255.255.255.255", port);
}

InetAddress InetAddress::AnyAddress(int port) { return InetAddress(port); }

InetAddress::InetAddress(const in_addr &ip, int port) {
  addr_ = {
#if defined(_APPLE__)
    .sin_len = sizeof(int),
#endif
    .sin_family = AF_INET,
    .sin_port = htons(port),
    .sin_addr = {.s_addr = htonl(ip.s_addr)}
  };
}

InetAddress::InetAddress(int port) {
  valid_ = true;
  addr_ = {
#if defined(_APPLE__)
    .sin_len = sizeof(int),
#endif
    .sin_family = AF_INET,
    .sin_port = htons(port),
    .sin_addr = {.s_addr = INADDR_ANY}
  };
}

InetAddress::InetAddress(const std::string &hostname, int port) {
  struct hostent *entry = gethostbyname(hostname.c_str());
  in_addr_t ipaddr;
  if (entry != NULL) {
    ipaddr = ((struct in_addr *)entry->h_addr_list[0])->s_addr;
  } else {
    // No hostname found, try IP address.
    if (inet_pton(AF_INET, hostname.c_str(), &ipaddr) != 1) {
      fprintf(stderr, "Invalid IP address or unknown hostname %s\n",
              hostname.c_str());
      return;
    }
  }
  valid_ = true;
  addr_ = {
#if defined(_APPLE__)
    .sin_len = sizeof(int),
#endif
    .sin_family = AF_INET,
    .sin_port = htons(port),
    .sin_addr = {.s_addr = ipaddr}
  };
}

std::string InetAddress::ToString() const {
  char buf[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof(buf));
  return absl::StrFormat("%s:%d", buf, ntohs(addr_.sin_port));
}

static ssize_t ReceiveFully(co::Coroutine *c, int fd, int32_t length,
                            char *buffer, size_t buflen) {
  int offset = 0;
  size_t remaining = length;
  while (remaining > 0) {
    size_t readlen = std::min(remaining, buflen);
    if (c != nullptr) {
      c->Wait(fd, POLLIN);
    }
    ssize_t n = ::recv(fd, buffer + offset, readlen, 0);
    if (n == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        if (c == nullptr) {
          return -1; // Prevent infinite loop for non-coroutine.
        }
        // Back to the c->Wait call to wait to try again.
        continue;
      }
    }
    if (n == 0) {
      // Short read.
      return -1;
    }
    remaining -= n;
    offset += n;
  }
  return length;
}

static ssize_t SendFully(co::Coroutine *c, int fd, const char *buffer,
                         size_t length, bool blocking) {
  size_t remaining = length;
  size_t offset = 0;
  while (remaining > 0) {
    if (c != nullptr && blocking) {
      // If we are nonblocking there's no point in waiting for
      // POLLOUT before sending.  If it would block we will
      // get an EAGAIN and we will then yield the coroutine.
      // Yielding before sending to a nonblocking socket will
      // cause a context switch between coroutines and we want
      // the write to the network to be as fast as possible.
      c->Wait(fd, POLLOUT);
    }
    ssize_t n = ::send(fd, buffer + offset, remaining, 0);
    if (n == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        if (c == nullptr) {
          return -1; // Prevent infinite loop for non-coroutine.
        }
        // If we are nonblocking yield the coroutine now.  When we
        // are resumed we can write to the socket again.
        if (!blocking) {
          c->Wait(fd, POLLOUT);
        }
        continue;
      }
    }
    if (n == 0) {
      // EOF on write.
      return -1;
    }
    remaining -= n;
    offset += n;
  }
  return length;
}

absl::StatusOr<ssize_t> Socket::Receive(char *buffer, size_t buflen,
                                        co::Coroutine *c) {
  if (!Connected()) {
    return absl::InternalError("Socket is not connected");
  }

  ssize_t n = ReceiveFully(c, fd_.Fd(), buflen, buffer, buflen);
  if (n == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to read data from socket: %s", strerror(errno)));
  }
  return n;
}

absl::StatusOr<ssize_t> Socket::Send(const char *buffer, size_t length,
                                     co::Coroutine *c) {
  if (!Connected()) {
    return absl::InternalError("Socket is not connected");
  }

  ssize_t n = SendFully(c, fd_.Fd(), buffer, length, IsBlocking());
  if (n == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to write data to socket: %s", strerror(errno)));
  }
  return n;
}

absl::StatusOr<ssize_t> Socket::ReceiveMessage(char *buffer, size_t buflen,
                                               co::Coroutine *c) {
  if (!Connected()) {
    return absl::InternalError("Socket is not connected");
  }
  // Although the send is done using a single send to the socket by
  // prefixing it with the length, we can't use that trick for receiving.
  // We cannot avoid doing 2 receives:
  // 1. Receive the length
  // 2. Receive the data
  //
  // This is because if we receive more than the message length we will
  // be receiving data from the next message on the socket.
  int32_t length;
  char lenbuf[4];
  ssize_t n =
      ReceiveFully(c, fd_.Fd(), sizeof(int32_t), lenbuf, sizeof(lenbuf));
  if (n != sizeof(lenbuf)) {
    return absl::InternalError(absl::StrFormat(
        "Failed to read length from socket %d: %s", fd_.Fd(), strerror(errno)));
  }
  length = ntohl(*reinterpret_cast<int32_t *>(lenbuf));
  n = ReceiveFully(c, fd_.Fd(), length, buffer, buflen);
  if (n == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to read data from socket: %s", strerror(errno)));
  }
  return n;
}

absl::StatusOr<ssize_t> Socket::SendMessage(char *buffer, size_t length,
                                            co::Coroutine *c) {
  if (!Connected()) {
    return absl::InternalError("Socket is not connected");
  }
  // Insert length in network byte order immediately before
  // the address passed as the buffer.
  int32_t *lengthptr = reinterpret_cast<int32_t *>(buffer) - 1;
  *lengthptr = htonl(length);

  ssize_t n = SendFully(c, fd_.Fd(), reinterpret_cast<char *>(lengthptr),
                        length + sizeof(int32_t), IsBlocking());
  if (n == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to write to socket: %s", strerror(errno)));
  }
  return n;
}

// Unix Domain socket.
UnixSocket::UnixSocket() : Socket(socket(AF_UNIX, SOCK_STREAM, 0)) {}

static struct sockaddr_un BuildUnixSocketName(const std::string &pathname) {
  struct sockaddr_un addr;
  memset(reinterpret_cast<void *>(&addr), 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
#ifdef __linux__
  // On Linux we can create it in the abstract namespace which doesn't
  // consume a pathname.
  addr.sun_path[0] = '\0';
  memcpy(addr.sun_path + 1, pathname.c_str(), pathname.size());
#else
  // Portable uses the file system so it must be a valid path name.
  memcpy(addr.sun_path, pathname.c_str(), pathname.size());
#endif
  return addr;
}

absl::Status UnixSocket::Bind(const std::string &pathname, bool listen) {
  struct sockaddr_un addr = BuildUnixSocketName(pathname);

  int e =
      ::bind(fd_.Fd(), reinterpret_cast<const sockaddr *>(&addr), sizeof(addr));
  if (e == -1) {
    fd_.Reset();
    return absl::InternalError(absl::StrFormat(
        "Failed to bind unix socket to %s: %s", pathname, strerror(errno)));
  }
  if (listen) {
    ::listen(fd_.Fd(), 10);
  }
  return absl::OkStatus();
}

absl::StatusOr<UnixSocket> UnixSocket::Accept(co::Coroutine *c) {
  if (!fd_.Valid()) {
    return absl::InternalError("UnixSocket is not valid");
  }
  if (c != nullptr) {
    c->Wait(fd_.Fd(), POLLIN);
  }
  struct sockaddr_un sender;
  socklen_t sock_len = sizeof(sender);
  int new_fd = ::accept(fd_.Fd(), reinterpret_cast<struct sockaddr *>(&sender),
                        &sock_len);
  if (new_fd == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to accept unix socket connection on fd %d: %s",
                        fd_.Fd(), strerror(errno)));
  }
  return UnixSocket(new_fd, /*connected=*/true);
}

absl::Status UnixSocket::Connect(const std::string &pathname) {
  if (!fd_.Valid()) {
    return absl::InternalError("UnixSocket is not valid");
  }
  struct sockaddr_un addr = BuildUnixSocketName(pathname);

  int e = ::connect(fd_.Fd(), reinterpret_cast<const sockaddr *>(&addr),
                    sizeof(addr));
  if (e == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to connect unix socket to %s: %s", pathname, strerror(errno)));
  }
  connected_ = true;
  return absl::OkStatus();
}

absl::Status UnixSocket::SendFds(const std::vector<FileDescriptor> &fds,
                                 co::Coroutine *c) {
  if (!Connected()) {
    return absl::InternalError("Socket is not connected");
  }
  constexpr size_t kMaxFds = 252;
  union {
    char buf[CMSG_SPACE(kMaxFds * sizeof(int))];
    struct cmsghdr align;
  } u;
  memset(u.buf, 0, sizeof(u.buf));

  // Need to send at least one byte.
  char data[1] = {0};
  struct iovec iov = {.iov_base = reinterpret_cast<void *>(data), .iov_len = 1};
  size_t fds_size = fds.size() * sizeof(int);
  struct msghdr msg = {.msg_iov = &iov,
                       .msg_iovlen = 1,
                       .msg_control = u.buf,
                       .msg_controllen =
                           static_cast<socklen_t>(CMSG_SPACE(fds_size))};

  struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(fds_size);
  int *fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
  for (size_t i = 0; i < fds.size(); i++) {
    *fdptr++ = fds[i].Fd();
  }

  if (c != nullptr) {
    c->Wait(fd_.Fd(), POLLOUT);
  }
  int e = ::sendmsg(fd_.Fd(), &msg, 0);
  if (e == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to write fds to unix socket: %s", strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status UnixSocket::ReceiveFds(std::vector<FileDescriptor> &fds,
                                    co::Coroutine *c) {
  if (!Connected()) {
    return absl::InternalError("Socket is not connected");
  }
  constexpr size_t kMaxFds = 252;
  union {
    char buf[CMSG_SPACE(kMaxFds * sizeof(int))];
    struct cmsghdr align;
  } u;

  // Need to send at least one byte.
  char data[1] = {0};
  struct iovec iov = {.iov_base = reinterpret_cast<void *>(data), .iov_len = 1};

  struct msghdr msg = {.msg_iov = &iov,
                       .msg_iovlen = 1,
                       .msg_control = u.buf,
                       .msg_controllen = sizeof(u.buf)};

  if (c != nullptr) {
    c->Wait(fd_.Fd(), POLLIN);
  }
  ssize_t n = ::recvmsg(fd_.Fd(), &msg, 0);
  if (n == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to read fds to unix socket: %s", strerror(errno)));
  }
  if (n == 0) {
    return absl::InternalError(
        absl::StrFormat("EOF from socket while reading fds\n"));
  }
  struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
  if (cmsg == nullptr) {
    // This can happen, apparently.
    return absl::OkStatus();
  }
  int *fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
  int num_fds = (cmsg->cmsg_len - sizeof(struct cmsghdr)) / sizeof(int);
  for (int i = 0; i < num_fds; i++) {
    fds.emplace_back(fdptr[i]);
  }
  return absl::OkStatus();
}

// Network socket.
absl::Status NetworkSocket::Connect(const InetAddress &addr) {
  if (!fd_.Valid()) {
    return absl::InternalError("Socket is not valid");
  }
  int e = ::connect(fd_.Fd(),
                    reinterpret_cast<const sockaddr *>(&addr.GetAddress()),
                    addr.GetLength());
  if (e == -1) {
    return absl::InternalError(
        absl::StrFormat("Failed to connect socket to %s: %s",
                        addr.ToString().c_str(), strerror(errno)));
  }
  connected_ = true;
  return absl::OkStatus();
}

absl::Status NetworkSocket::SetReuseAddr() {
  int val = 1;
  int e = setsockopt(fd_.Fd(), SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int));
  if (e != 0) {
    return absl::InternalError(absl::StrFormat(
        "Unable to set SO_REUSEADDR on socket: %s", strerror(errno)));
  }
  return absl::OkStatus();
}

// TCP socket
TCPSocket::TCPSocket() : NetworkSocket(socket(AF_INET, SOCK_STREAM, 0)) {}

absl::Status TCPSocket::Bind(const InetAddress &addr, bool listen) {
  bool binding_to_zero = addr.Port() == 0;
  int e =
      ::bind(fd_.Fd(), reinterpret_cast<const sockaddr *>(&addr.GetAddress()),
             addr.GetLength());
  if (e == -1) {
    fd_.Reset();
    return absl::InternalError(
        absl::StrFormat("Failed to bind TCP socket to %s: %s",
                        addr.ToString().c_str(), strerror(errno)));
  }
  bound_address_ = addr;
  if (binding_to_zero) {
    struct sockaddr_in bound;
    socklen_t len = sizeof(bound);
    int e = getsockname(fd_.Fd(), reinterpret_cast<struct sockaddr *>(&bound),
                        &len);
    if (e == -1) {
      return absl::InternalError(
          absl::StrFormat("Failed to obtain bound address for %s: %s",
                          addr.ToString().c_str(), strerror(errno)));
    }
    bound_address_.SetPort(ntohs(bound.sin_port));
  }
  if (listen) {
    ::listen(fd_.Fd(), 10);
  }
  return absl::OkStatus();
}

absl::StatusOr<TCPSocket> TCPSocket::Accept(co::Coroutine *c) {
  if (!fd_.Valid()) {
    return absl::InternalError("Socket is not valid");
  }
  if (c != nullptr) {
    c->Wait(fd_.Fd(), POLLIN);
  }
  struct sockaddr_un sender;
  socklen_t sock_len = sizeof(sender);
  ;
  int new_fd = ::accept(fd_.Fd(), reinterpret_cast<struct sockaddr *>(&sender),
                        &sock_len);
  return TCPSocket(new_fd, /*connected=*/true);
}

// UDP socket
UDPSocket::UDPSocket() : NetworkSocket(socket(AF_INET, SOCK_DGRAM, 0)) {}

absl::Status UDPSocket::Bind(const InetAddress &addr) {
  int e =
      ::bind(fd_.Fd(), reinterpret_cast<const sockaddr *>(&addr.GetAddress()),
             addr.GetLength());
  if (e == -1) {
    fd_.Reset();
    return absl::InternalError(
        absl::StrFormat("Failed to bind UDP socket to %s: %s",
                        addr.ToString().c_str(), strerror(errno)));
  }
  bound_address_ = addr;
  connected_ = true; // UDP sockets are always connected when bound.
  return absl::OkStatus();
}

absl::Status UDPSocket::SetBroadcast() {
  int val = 1;
  int e = setsockopt(fd_.Fd(), SOL_SOCKET, SO_BROADCAST, &val, sizeof(int));
  if (e != 0) {
    return absl::InternalError(absl::StrFormat(
        "Unable to set broadcast on UDP socket: %s", strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status UDPSocket::SendTo(const InetAddress &addr, const void *buffer,
                               size_t length, co::Coroutine *c) {
  if (c != nullptr) {
    c->Wait(fd_.Fd(), POLLOUT);
  }
  size_t n = ::sendto(fd_.Fd(), buffer, length, 0,
                      reinterpret_cast<const sockaddr *>(&addr.GetAddress()),
                      addr.GetLength());
  if (n == -1) {
    return absl::InternalError(
        absl::StrFormat("Unable to send UDP datagram to %s: %s",
                        addr.ToString(), strerror(errno)));
  }
  return absl::OkStatus();
}

absl::StatusOr<ssize_t> UDPSocket::Receive(void *buffer, size_t buflen,
                                           co::Coroutine *c) {
  if (c != nullptr) {
    c->Wait(fd_.Fd(), POLLIN);
  }
  size_t n = recv(fd_.Fd(), buffer, buflen, 0);
  if (n == -1) {
    return absl::InternalError(
        absl::StrFormat("Unable to receive UDP datagram: %s", strerror(errno)));
  }
  return n;
}
absl::StatusOr<ssize_t> UDPSocket::ReceiveFrom(InetAddress &sender,
                                               void *buffer, size_t buflen,
                                               co::Coroutine *c) {
  if (c != nullptr) {
    c->Wait(fd_.Fd(), POLLIN);
  }
  struct sockaddr_in sender_addr;
  socklen_t sender_addr_length = sizeof(sender_addr);

  size_t n = recvfrom(fd_.Fd(), buffer, buflen, 0,
                      reinterpret_cast<struct sockaddr *>(&sender_addr),
                      &sender_addr_length);
  if (n == -1) {
    return absl::InternalError(
        absl::StrFormat("Unable to receive UDP datagram: %s", strerror(errno)));
  }
  sender = {sender_addr};
  return n;
}
} // namespace subspace
