// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __CLIENT_OPTIONS_H
#define __CLIENT_OPTIONS_H

namespace subspace {

// You can use the options in two ways depending on your
// coding guidelines.  You can either use the Google/Java-style
// chained setters method:
//   Options().SetA(1).SetB(2).SetC(3)
//
// or you can use the newer designated initializer style:
//   {.a = 1, .b = 2, .c = 3}.
// The designated initializer allows you to omit the call to
// the constructor and pass the initializer-list directly.
//
// When reading the option values, you can use the getter function
// or you can access the struct member directly.
//
// Your choice.

// Options when creating a publisher.
struct PublisherOptions {
  // A public publisher's messages will be seen outside of the
  // publishing computer.
  PublisherOptions &SetLocal(bool v) {
    local = v;
    return *this;
  }
  // A reliable publisher's messages will never be missed by
  // a reliable subscriber.
  PublisherOptions &SetReliable(bool v) {
    reliable = v;
    return *this;
  }
  // Set the type of the message to be published.  The type is
  // not meaningful to the subspace system.  It's up to the
  // user to figure out what it means. The same type must
  // be used by all other subscribers and publishers.
  // By default there is no type.
  PublisherOptions &SetType(std::string t) {
    type = std::move(t);
    return *this;
  }

  // Set the option to allow the channel's slots to be resized
  // if necessary.
  PublisherOptions &SetFixedSize(bool v) {
    fixed_size = v;
    return *this;
  }

  bool IsLocal() const { return local; }
  bool IsReliable() const { return reliable; }
  bool IsFixedSize() const { return fixed_size; }
  const std::string &Type() const { return type; }

  PublisherOptions &SetBridge(bool v) {
    bridge = v;
    return *this;
  }

  bool IsBridge() const { return bridge; }

  bool local = false;
  bool reliable = false;
  bool bridge = false;
  bool fixed_size = false;
  std::string type;
};

struct SubscriberOptions {
  // A reliable subscriber will never miss a message from a reliable
  // publisher.
  SubscriberOptions &SetReliable(bool v) {
    reliable = v;
    return *this;
  }
  // Set the type of the message on the channel.  The type is
  // not meaningful to the subspace system.  It's up to the
  // user to figure out what it means.  The same type must
  // be used by all other subscribers and publishers.
  // By default there is no type.
  SubscriberOptions &SetType(std::string t) {
    type = std::move(t);
    return *this;
  }

  SubscriberOptions &SetMaxSharedPtrs(int n) {
    max_shared_ptrs = n;
    return *this;
  }

  bool IsReliable() const { return reliable; }
  const std::string &Type() const { return type; }
  int MaxSharedPtrs() const { return max_shared_ptrs; }

  SubscriberOptions &SetBridge(bool v) {
    bridge = v;
    return *this;
  }
  bool IsBridge() const { return bridge; }

  bool reliable = false;
  bool bridge = false;
  std::string type;
  int max_shared_ptrs = 0;
};

} // namespace subspace

#endif // __CLIENT_OPTIONS_H
