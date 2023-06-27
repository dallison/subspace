// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __CLIENT_OPTIONS_H
#define __CLIENT_OPTIONS_H

namespace subspace {

// Options when creating a publisher.
class PublisherOptions {
public:
  // A public publisher's messages will be seen outside of the
  // publishing computer.
  PublisherOptions &SetLocal(bool v) {
    local_ = v;
    return *this;
  }
  // A reliable publisher's messages will never be missed by
  // a reliable subscriber.
  PublisherOptions &SetReliable(bool v) {
    reliable_ = v;
    return *this;
  }
  // Set the type of the message to be published.  The type is
  // not meaningful to the subspace system.  It's up to the
  // user to figure out what it means. The same type must
  // be used by all other subscribers and publishers.
  // By default there is no type.
  PublisherOptions &SetType(std::string type) {
    type_ = std::move(type);
    return *this;
  }

  // Set the option to allow the channel's slots to be resized
  // if necessary.
  PublisherOptions &SetFixedSize(bool v) {
    fixed_size_ = v;
    return *this;
  }

  bool IsLocal() const { return local_; }
  bool IsReliable() const { return reliable_; }
  bool IsFixedSize() const { return fixed_size_; }
  const std::string &Type() const { return type_; }

private:
  friend class Server;
  friend class Client;
  PublisherOptions &SetBridge(bool v) {
    bridge_ = v;
    return *this;
  }
  bool IsBridge() const { return bridge_; }

  bool local_ = false;
  bool reliable_ = false;
  bool bridge_ = false;
  bool fixed_size_ = false;
  std::string type_;
};

class SubscriberOptions {
public:
  // A reliable subscriber will never miss a message from a reliable
  // publisher.
  SubscriberOptions &SetReliable(bool v) {
    reliable_ = v;
    return *this;
  }
  // Set the type of the message on the channel.  The type is
  // not meaningful to the subspace system.  It's up to the
  // user to figure out what it means.  The same type must
  // be used by all other subscribers and publishers.
  // By default there is no type.
  SubscriberOptions &SetType(std::string type) {
    type_ = std::move(type);
    return *this;
  }

  SubscriberOptions &SetMaxSharedPtrs(int n) {
    max_shared_ptrs_ = n;
    return *this;
  }

  bool IsReliable() const { return reliable_; }
  const std::string &Type() const { return type_; }
  int MaxSharedPtrs() const { return max_shared_ptrs_; }

private:
  friend class Server;
  friend class Client;
  SubscriberOptions &SetBridge(bool v) {
    bridge_ = v;
    return *this;
  }
  bool IsBridge() const { return bridge_; }

  bool reliable_ = false;
  bool bridge_ = false;
  std::string type_;
  int max_shared_ptrs_ = 0;
};

} // namespace subspace

#endif // __CLIENT_OPTIONS_H
