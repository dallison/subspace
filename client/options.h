// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef _xCLIENT_OPTIONS_H
#define _xCLIENT_OPTIONS_H

#include <functional>
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

class Subscriber;

// Options when creating a publisher.
struct PublisherOptions {
  int32_t SlotSize() const { return slot_size; }
  int32_t NumSlots() const { return num_slots; }
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

  // Adds support for external tunnel processes that need to know whether
  // messages are locally or remotely generated.  When set, the cross-machine
  // flag is written into the MessagePrefix and reliable channel activation
  // is skipped (the existing local publisher will have already activated).
  PublisherOptions &SetForTunnel(bool v) {
    for_tunnel = v;
    return *this;
  }

  bool ForTunnel() const { return for_tunnel; }

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

  bool NotifyRetirement() const { return notify_retirement; }
  PublisherOptions &SetNotifyRetirement(bool v) {
    notify_retirement = v;
    return *this;
  }

  // If this is set to true all messages published will have a checksum
  // calculated and placed in the MessagePrefix metadata.  If subscribers want
  // to verify the checksum the must set the SubscriberOptions.Checksum to true.
  // Included in the checksum is the message prefix and the message
  // data.
  //
  // Using checksums on all messages will increase the latency of message transmission.  Use it
  // sparingly.  Subscribers generally map the buffers in read-only so the only way to corrup it is
  // for the publishing process to overwrite the buffer after it has been published.
  PublisherOptions &SetChecksum(bool v) {
    checksum = v;
    return *this;
  }
  bool Checksum() const { return checksum; }

  // Number of bytes reserved for the checksum starting at the checksum
  // field of MessagePrefix.  Default is 4 (CRC32).
  PublisherOptions &SetChecksumSize(int32_t size) {
    checksum_size = size;
    return *this;
  }
  int32_t ChecksumSize() const { return checksum_size; }

  // Number of bytes of user metadata stored immediately after the
  // checksum area in the prefix extensions.  Default is 0 (none).
  PublisherOptions &SetMetadataSize(int32_t size) {
    metadata_size = size;
    return *this;
  }
  int32_t MetadataSize() const { return metadata_size; }

  // Free-slot allocator policy for unreliable publishers (no effect on
  // reliable publishers).  When true, a publisher in FindFreeSlotUnreliable
  // will prefer to recycle a slot from the RetiredSlots set before pulling
  // a fresh slot from the never-touched FreeSlots pool.  Both choices are
  // equally valid (a retired slot has been seen by every current subscriber)
  // but a retired slot's pages are already cache-hot from the recent
  // publish/consume cycle, while a fresh FreeSlots slot will demand-fault the
  // kernel into allocating new physical pages on first write.  In steady
  // state this lets the publisher cycle through a tiny working set of
  // cache-hot slots regardless of how deep the channel's slot pool is
  // configured, while still bursting into FreeSlots when the subscriber
  // falls behind.  Defaults to false (FreeSlots-first) so multiple messages
  // remain in distinct slots until the free pool is exhausted, which
  // preserves behaviour for subscribers that attach after earlier messages
  // were published and consumed by another subscriber on the same channel.
  // Set true for the cache-friendly LIFO-style recycling behaviour.
  PublisherOptions &SetPreferRetiredSlots(bool v) {
    prefer_retired_slots = v;
    return *this;
  }
  bool PreferRetiredSlots() const { return prefer_retired_slots; }

  // If you use the new CreatePublisher API, set the slot size and num slots in
  // here.
  int32_t slot_size = 0;
  int32_t num_slots = 0;

  bool local = false;
  bool reliable = false;
  bool bridge = false;
  bool for_tunnel = false;
  bool fixed_size = false;
  std::string type;
  bool activate =
      false; // If true, the channel will be activated even if unreliable.

  std::string mux;
  int vchan_id = -1; // If -1, server will assign.
  bool notify_retirement = false;
  bool checksum = false;
  int32_t checksum_size = 4;
  int32_t metadata_size = 0;

  // See SetPreferRetiredSlots() for description.
  bool prefer_retired_slots = false;
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

  // Adds support for external tunnel processes that need to know whether
  // messages are locally or remotely generated.
  SubscriberOptions &SetForTunnel(bool v) {
    for_tunnel = v;
    return *this;
  }
  bool ForTunnel() const { return for_tunnel; }

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

  bool ReadWrite() const { return read_write; }
  SubscriberOptions &SetReadWrite(bool v) {
    read_write = v;
    return *this;
  }

  // If this options is set to true the checksum calculated by the publisher
  // will be verified. The checksum is placed in the MessagePrefix metadata. See
  // PassChecksumErrors below for options for handling checksum errors.
  SubscriberOptions &SetChecksum(bool v) {
    checksum = v;
    return *this;
  }
  bool Checksum() const { return checksum; }

  // If we get a checksum error and this is true the message will be received
  // but will have the checksum_error flag set.  If false, an error will be
  // returned from ReadMessage.
  SubscriberOptions &SetPassChecksumErrors(bool v) {
    pass_checksum_errors = v;
    return *this;
  }
  bool PassChecksumErrors() const { return pass_checksum_errors; }

  SubscriberOptions &SetKeepActiveMessage(bool v) {
    keep_active_message = v;
    return *this;
  }
  bool KeepActiveMessage() const { return keep_active_message; }

  bool reliable = false;
  bool bridge = false;
  bool for_tunnel = false;
  std::string type;
  int max_active_messages = 1;
  bool log_dropped_messages = true;
  bool pass_activation = false; // If true, the subscriber will pass activation
                                // messages to the user.
  bool read_write = false;

  std::string mux;
  int vchan_id = -1; // If -1, server will assign.
  bool checksum = false;
  bool pass_checksum_errors = false;

  // If true, the subscriber will keep a reference to the most recent
  // active message.  This is useful if you don't want to pass Message structs
  // around and you want to keep the message alive until you are done with it.
  // You should call ClearActiveMessage() to release the reference when you are done with it.
  bool keep_active_message = false;
};

} // namespace subspace

#endif // _xCLIENT_OPTIONS_H
