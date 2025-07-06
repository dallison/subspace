// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.


#ifndef __CLIENT_OPTIONS_H
#define __CLIENT_OPTIONS_H

#include <string>

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
  int32_t SlotSize() const {
    return slot_size;
  }
  int32_t NumSlots() const {
    return num_slots;
  }
  PublisherOptions &SetSlotSize(int32_t size) {
    slot_size = size;
    return *this;
  }
  PublisherOptions &SetNumSlots(int32_t num) {
    num_slots = num;
    return *this;
  }

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

  PublisherOptions &SetMux(std::string m) {
    mux = std::move(m);
    return *this;
  }

  const std::string &Mux() const { return mux; }

  PublisherOptions &SetVchanId(int id) {
    vchan_id = id;
    return *this;
  }

  bool Activate() const { return activate; }
  PublisherOptions &SetActivate(bool v) {
    activate = v;
    return *this;
  }

  int VchanId() const { return vchan_id; }

  // If you use the new CreatePublisher API, set the slot size and num slots in here.
  int32_t slot_size = 0;
  int32_t num_slots = 0;

  bool local = false;
  bool reliable = false;
  bool bridge = false;
  bool fixed_size = false;
  std::string type;
  bool activate =
      false; // If true, the channel will be activated even if unreliable.

  std::string mux;
  int vchan_id = -1; // If -1, server will assign.
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
    max_active_messages = n + 1;
    return *this;
  }

  SubscriberOptions &SetMaxActiveMessages(int n) {
    max_active_messages = n;
    return *this;
  }

  bool IsReliable() const { return reliable; }
  const std::string &Type() const { return type; }
  int MaxSharedPtrs() const { return max_active_messages - 1; }
  int MaxActiveMessages() const { return max_active_messages; }
  bool LogDroppedMessages() const { return log_dropped_messages; }
  void SetLogDroppedMessages(bool v) { log_dropped_messages = v; }

  SubscriberOptions &SetBridge(bool v) {
    bridge = v;
    return *this;
  }
  bool IsBridge() const { return bridge; }

  SubscriberOptions &SetMux(std::string m) {
    mux = std::move(m);
    return *this;
  }

  const std::string &Mux() const { return mux; }

  SubscriberOptions &SetVchanId(int id) {
    vchan_id = id;
    return *this;
  }

  int VchanId() const { return vchan_id; }

  bool PassActivation() const { return pass_activation; }
  SubscriberOptions &SetPassActivation(bool v) {
    pass_activation = v;
    return *this;
  }


  bool reliable = false;
  bool bridge = false;
  std::string type;
  int max_active_messages = 1;
  bool log_dropped_messages = true;
  bool pass_activation = false; // If true, the subscriber will pass activation
                                // messages to the user.

  std::string mux;
  int vchan_id = -1; // If -1, server will assign.
};

} // namespace subspace

#endif // __CLIENT_OPTIONS_H
