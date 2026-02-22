
#include "client/client.h"

#include "pybind11/functional.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include <stdexcept>
#include <string_view>

namespace subspace {
namespace python {

namespace py = pybind11;

PYBIND11_MODULE(subspace, m) {

  m.doc() = "This is a python module to pass messages over the Subspace "
            "inter-process communication protocol.";

  // ReadMode enum.
  py::enum_<ReadMode>(m, "ReadMode", "Mode for reading messages from a "
                                     "subscriber.")
      .value("READ_NEXT", ReadMode::kReadNext,
             "Read the next available message.")
      .value("READ_NEWEST", ReadMode::kReadNewest,
             "Read the newest available message.");

  // ChannelCounters struct.
  py::class_<ChannelCounters>(m, "ChannelCounters",
                              "Live counters for a channel stored in shared "
                              "memory.")
      .def_readonly("num_pub_updates", &ChannelCounters::num_pub_updates,
                    "Number of updates to publishers.")
      .def_readonly("num_sub_updates", &ChannelCounters::num_sub_updates,
                    "Number of updates to subscribers.")
      .def_readonly("num_pubs", &ChannelCounters::num_pubs,
                    "Current number of publishers.")
      .def_readonly("num_reliable_pubs", &ChannelCounters::num_reliable_pubs,
                    "Current number of reliable publishers.")
      .def_readonly("num_subs", &ChannelCounters::num_subs,
                    "Current number of subscribers.")
      .def_readonly("num_reliable_subs", &ChannelCounters::num_reliable_subs,
                    "Current number of reliable subscribers.")
      .def_readonly("num_resizes", &ChannelCounters::num_resizes,
                    "Number of times the channel has been resized.");

  // ChannelInfo struct.
  py::class_<ChannelInfo>(m, "ChannelInfo",
                          "Snapshot of information about a channel.")
      .def_readonly("channel_name", &ChannelInfo::channel_name)
      .def_readonly("num_publishers", &ChannelInfo::num_publishers)
      .def_readonly("num_subscribers", &ChannelInfo::num_subscribers)
      .def_readonly("num_bridge_pubs", &ChannelInfo::num_bridge_pubs)
      .def_readonly("num_bridge_subs", &ChannelInfo::num_bridge_subs)
      .def_readonly("type", &ChannelInfo::type)
      .def_readonly("slot_size", &ChannelInfo::slot_size)
      .def_readonly("num_slots", &ChannelInfo::num_slots)
      .def_readonly("reliable", &ChannelInfo::reliable);

  // ChannelStats struct.
  py::class_<ChannelStats>(m, "ChannelStats",
                           "Cumulative statistics for a channel.")
      .def_readonly("channel_name", &ChannelStats::channel_name)
      .def_readonly("total_bytes", &ChannelStats::total_bytes)
      .def_readonly("total_messages", &ChannelStats::total_messages)
      .def_readonly("max_message_size", &ChannelStats::max_message_size);

  // PublisherOptions class.
  py::class_<PublisherOptions>(m, "PublisherOptions",
                               "Options for creating a publisher.")
      .def(py::init<>())
      .def("set_local", &PublisherOptions::SetLocal,
           "Set whether the publisher is local.")
      .def("set_reliable", &PublisherOptions::SetReliable,
           "Set whether the publisher is reliable.")
      .def("set_fixed_size", &PublisherOptions::SetFixedSize,
           "Set whether the publisher has fixed size messages.")
      .def("set_type", &PublisherOptions::SetType,
           "Set the type of the message carried.")
      .def("is_local", &PublisherOptions::IsLocal,
           "Get whether the publisher is local.")
      .def("is_reliable", &PublisherOptions::IsReliable,
           "Get whether the publisher is reliable.")
      .def("is_fixed_size", &PublisherOptions::IsFixedSize,
           "Get whether the publisher has fixed size messages.")
      .def("type", &PublisherOptions::Type, "Get the type of the publisher.")
      .def("set_bridge", &PublisherOptions::SetBridge,
           "Set whether the publisher is a bridge.")
      .def("is_bridge", &PublisherOptions::IsBridge,
           "Get whether the publisher is a bridge.")
      .def("set_mux", &PublisherOptions::SetMux,
           "Set the mux for the publisher.")
      .def("mux", &PublisherOptions::Mux, "Get the mux for the publisher.")
      .def("set_vchan_id", &PublisherOptions::SetVchanId,
           "Set the virtual channel ID for the publisher.")
      .def("vchan_id", &PublisherOptions::VchanId,
           "Get the virtual channel ID for the publisher.")
      .def("set_activate", &PublisherOptions::SetActivate,
           "Set whether the publisher should activate the channel.")
      .def("activate", &PublisherOptions::Activate,
           "Get whether the publisher should activate the channel.")
      .def("set_slot_size", &PublisherOptions::SetSlotSize,
           "Set the size of the slots for the publisher.")
      .def("slot_size", &PublisherOptions::SlotSize,
           "Get the size of the slots for the publisher.")
      .def("set_num_slots", &PublisherOptions::SetNumSlots,
           "Set the number of slots for the publisher.")
      .def("num_slots", &PublisherOptions::NumSlots,
           "Get the number of slots for the publisher.")
      .def("set_notify_retirement", &PublisherOptions::SetNotifyRetirement,
           "Set whether the publisher notifies on message retirement.")
      .def("notify_retirement", &PublisherOptions::NotifyRetirement,
           "Get whether the publisher notifies on message retirement.")
      .def("set_checksum", &PublisherOptions::SetChecksum,
           "Set whether published messages include a checksum.")
      .def("checksum", &PublisherOptions::Checksum,
           "Get whether published messages include a checksum.");

  // SubscriberOptions class.
  py::class_<SubscriberOptions>(m, "SubscriberOptions",
                                "Options for creating a subscriber.")
      .def(py::init<>())
      .def("set_reliable", &SubscriberOptions::SetReliable,
           "Set whether the subscriber is reliable.")
      .def("set_pass_activation", &SubscriberOptions::SetPassActivation,
           "Set whether the subscriber passes activation messages.")
      .def("is_reliable", &SubscriberOptions::IsReliable,
           "Get whether the subscriber is reliable.")
      .def("pass_activation", &SubscriberOptions::PassActivation,
           "Get whether the subscriber passes activation messages.")
      .def("set_type", &SubscriberOptions::SetType,
           "Set the type of the message carried.")
      .def("type", &SubscriberOptions::Type, "Get the type of the subscriber.")
      .def("set_max_active_messages", &SubscriberOptions::SetMaxActiveMessages,
           "Set the maximum number of active messages for the subscriber.")
      .def("max_active_messages", &SubscriberOptions::MaxActiveMessages,
           "Get the maximum number of active messages for the subscriber.")
      .def("set_log_dropped_messages",
           &SubscriberOptions::SetLogDroppedMessages,
           "Sets whether the subscriber logs dropped messages.")
      .def("log_dropped_messages", &SubscriberOptions::LogDroppedMessages,
           "Get whether the subscriber logs dropped messages.")
      .def("set_bridge", &SubscriberOptions::SetBridge,
           "Set whether the subscriber is a bridge.")
      .def("is_bridge", &SubscriberOptions::IsBridge,
           "Get whether the subscriber is a bridge.")
      .def("set_mux", &SubscriberOptions::SetMux,
           "Set the mux for the subscriber.")
      .def("mux", &SubscriberOptions::Mux, "Get the mux for the subscriber.")
      .def("set_vchan_id", &SubscriberOptions::SetVchanId,
           "Set the virtual channel ID for the subscriber.")
      .def("vchan_id", &SubscriberOptions::VchanId,
           "Get the virtual channel ID for the subscriber.")
      .def("set_max_shared_ptrs", &SubscriberOptions::SetMaxSharedPtrs,
           "Set the maximum number of shared pointers for the subscriber.")
      .def("max_shared_ptrs", &SubscriberOptions::MaxSharedPtrs,
           "Get the maximum number of shared pointers for the subscriber.")
      .def("set_read_write", &SubscriberOptions::SetReadWrite,
           "Set whether the subscriber maps buffers as read-write.")
      .def("read_write", &SubscriberOptions::ReadWrite,
           "Get whether the subscriber maps buffers as read-write.")
      .def("set_checksum", &SubscriberOptions::SetChecksum,
           "Set whether the subscriber verifies message checksums.")
      .def("checksum", &SubscriberOptions::Checksum,
           "Get whether the subscriber verifies message checksums.")
      .def("set_pass_checksum_errors",
           &SubscriberOptions::SetPassChecksumErrors,
           "Set whether checksum errors are passed through as a flag instead "
           "of raising errors.")
      .def("pass_checksum_errors", &SubscriberOptions::PassChecksumErrors,
           "Get whether checksum errors are passed through.")
      .def("set_keep_active_message", &SubscriberOptions::SetKeepActiveMessage,
           "Set whether the subscriber keeps a reference to the most recent "
           "active message.")
      .def("keep_active_message", &SubscriberOptions::KeepActiveMessage,
           "Get whether the subscriber keeps a reference to the most recent "
           "active message.");

  // Message class returned from read_message.
  py::class_<Message>(m, "Message",
                      "A message is a single message sent over a channel.")
      .def(py::init<>())
      .def("__enter__", [](Message &self) { return &self; })
      .def("__exit__",
           [](Message &self, py::object, py::object, py::object) {
             self.Reset();
             return py::none();
           })
      .def_readonly("length", &Message::length,
                    "The length of the message in bytes.")
      .def_readonly("timestamp", &Message::timestamp,
                    "The timestamp of the message.")
      .def_readonly("ordinal", &Message::ordinal, "The ordinal of the message.")
      .def_readonly("vchan_id", &Message::vchan_id,
                    "The virtual channel ID of the message. This is used to "
                    "identify the virtual channel in which the message was "
                    "sent.")
      .def_property_readonly("buffer",
                             [](const Message &self) {
                               return py::bytes(
                                   reinterpret_cast<const char *>(self.buffer),
                                   self.length);
                             },
                             "The buffer containing the message data.")
      .def("Reset", &Message::Reset,
           "Release the message slot. Called automatically by __exit__.")
      .def_readonly("is_activation", &Message::is_activation,
                    "Whether this is a channel activation message.")
      .def_readonly("slot_id", &Message::slot_id,
                    "The slot ID of the message.")
      .def_readonly("checksum_error", &Message::checksum_error,
                    "Whether a checksum error was detected for this message.");

  // -------------------------------------------------------------------------
  // Publisher
  // -------------------------------------------------------------------------
  py::class_<Publisher> publisher_class(
      m, "Publisher",
      "The Publisher class is the main interface for sending messages.");

  // Existing: publish_message(bytes) - copies data into buffer and publishes.
  publisher_class.def(
      "publish_message",
      [](Publisher *self, py::bytes message_data) {
        auto msg_view = static_cast<std::string_view>(message_data);
        absl::StatusOr<void *> dest_buffer =
            self->GetMessageBuffer(msg_view.size());
        if (!dest_buffer.ok()) {
          throw std::runtime_error(dest_buffer.status().ToString());
        }
        std::memcpy(*dest_buffer, msg_view.data(), msg_view.size());
        absl::StatusOr<Message> send_result =
            self->PublishMessage(msg_view.size());
        if (!send_result.ok()) {
          throw std::runtime_error(send_result.status().ToString());
        }
      },
      "Publish the message in the publisher's buffer.");

  // Existing: wait() - waits for a reliable publisher to be able to send.
  publisher_class.def(
      "wait",
      [](Publisher *self) {
        absl::Status result = self->Wait();
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Wait until a reliable publisher can try again to send a message.");

  // Existing accessors.
  publisher_class.def("type", &Publisher::Type);
  publisher_class.def("is_reliable", &Publisher::IsReliable);
  publisher_class.def("is_local", &Publisher::IsLocal);
  publisher_class.def("is_fixed_size", &Publisher::IsFixedSize);
  publisher_class.def("slot_size", &Publisher::SlotSize);

  // New accessors.
  publisher_class.def("name", &Publisher::Name,
                      "Get the channel name of this publisher.");

  publisher_class.def("num_slots", &Publisher::NumSlots,
                      "Get the number of message slots.");

  publisher_class.def("virtual_channel_id", &Publisher::VirtualChannelId,
                      "Get the virtual channel ID assigned to this publisher.");

  publisher_class.def("mux", &Publisher::Mux,
                      "Get the mux name for this publisher.");

  publisher_class.def("num_subscribers", &Publisher::NumSubscribers,
                      "Get the number of subscribers on this channel.",
                      py::arg("vchan_id") = -1);

  publisher_class.def("get_virtual_memory_usage",
                      &Publisher::GetVirtualMemoryUsage,
                      "Get the virtual memory usage of this publisher in "
                      "bytes.");

  publisher_class.def(
      "get_channel_counters",
      [](Publisher *self) -> ChannelCounters {
        return self->GetChannelCounters();
      },
      "Get a snapshot of the channel counters for this publisher's channel.");

  // Zero-copy publishing: get a writable buffer, fill it, then call
  // publish_buffer().
  publisher_class.def(
      "get_message_buffer",
      [](Publisher *self, int32_t max_size) -> py::memoryview {
        absl::StatusOr<void *> buffer = self->GetMessageBuffer(max_size);
        if (!buffer.ok()) {
          throw std::runtime_error(buffer.status().ToString());
        }
        if (*buffer == nullptr) {
          throw std::runtime_error(
              "No buffer available (reliable publisher has no free slots)");
        }
        return py::memoryview::from_memory(*buffer,
                                           static_cast<Py_ssize_t>(max_size));
      },
      R"doc(Get a writable memoryview for zero-copy message publishing.
Write your message data into the returned buffer, then call
publish_buffer(size) to publish. If you decide not to publish,
call cancel_publish() to release the buffer. In thread-safe mode
the client lock is held until publish_buffer() or cancel_publish()
is called.)doc",
      py::arg("max_size"));

  publisher_class.def(
      "publish_buffer",
      [](Publisher *self, int64_t message_size) -> Message {
        absl::StatusOr<const Message> result =
            self->PublishMessage(message_size);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      R"doc(Publish the data written into the buffer obtained from
get_message_buffer(). The message_size is the number of bytes
actually written.)doc",
      py::arg("message_size"));

  publisher_class.def("cancel_publish", &Publisher::CancelPublish,
                      "Cancel a pending zero-copy publish and release the "
                      "buffer lock.");

  // Wait variants with timeout and extra fd.
  publisher_class.def(
      "wait_with_timeout",
      [](Publisher *self, int64_t timeout_ns) {
        absl::Status result =
            self->Wait(std::chrono::nanoseconds(timeout_ns));
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Wait with a timeout (in nanoseconds) for the publisher to become "
      "ready.",
      py::arg("timeout_ns"));

  publisher_class.def(
      "wait_with_fd",
      [](Publisher *self, int fd) -> int {
        toolbelt::FileDescriptor extra_fd(fd, /*owned=*/false);
        absl::StatusOr<int> result = self->Wait(extra_fd);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      R"doc(Wait for the publisher with an additional file descriptor that can
interrupt the wait. Returns the fd that triggered the wake-up.)doc",
      py::arg("fd"));

  // Poll / file descriptor access.
  publisher_class.def(
      "get_poll_fd",
      [](Publisher *self) -> int {
        struct pollfd pfd = self->GetPollFd();
        return pfd.fd;
      },
      "Get the poll file descriptor for use with select/poll/epoll.");

  publisher_class.def(
      "get_file_descriptor",
      [](Publisher *self) -> int {
        return self->GetFileDescriptor().Fd();
      },
      "Get the underlying trigger file descriptor.");

  publisher_class.def(
      "get_retirement_fd",
      [](Publisher *self) -> int {
        return self->GetRetirementFd().Fd();
      },
      "Get the file descriptor notified when message slots are retired.");

  // Resize callback.
  publisher_class.def(
      "register_resize_callback",
      [](Publisher *self, py::function callback) {
        absl::Status result = self->RegisterResizeCallback(
            [callback](Publisher *, int old_size, int new_size) -> absl::Status {
              py::gil_scoped_acquire acquire;
              py::object ret = callback(old_size, new_size);
              if (py::isinstance<py::bool_>(ret) && !ret.cast<bool>()) {
                return absl::FailedPreconditionError(
                    "Resize rejected by Python callback");
              }
              return absl::OkStatus();
            });
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      R"doc(Register a callback invoked when the channel is about to resize.
The callback receives (old_size, new_size) and should return True to
allow the resize or False to prevent it.)doc",
      py::arg("callback"));

  publisher_class.def(
      "unregister_resize_callback",
      [](Publisher *self) {
        absl::Status result = self->UnregisterResizeCallback();
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Unregister the resize callback.");

  // -------------------------------------------------------------------------
  // Subscriber
  // -------------------------------------------------------------------------
  py::class_<Subscriber> subscriber_class(
      m, "Subscriber",
      "The Subscriber class is the main interface for receiving messages.");

  // Existing: read_message(skip_to_newest) - returns bytes.
  subscriber_class.def(
      "read_message",
      [](Subscriber *self, bool skip_to_newest) {
        absl::StatusOr<Message> read_result = self->ReadMessage(
            skip_to_newest ? ReadMode::kReadNewest : ReadMode::kReadNext);
        if (!read_result.ok()) {
          throw std::runtime_error(read_result.status().ToString());
        }
        auto r = py::bytes(reinterpret_cast<const char *>(read_result->buffer),
                           read_result->length);
        read_result->Reset(); // Release the message buffer.
        return r;
      },
      R"doc("Read a message from a subscriber. If there are no available messages,
the returned bytes will have zero length. Setting the 'skip_to_newest' argument
to True, causes the read to skip ahead to the newest available message, otherwise,
it reads the next available message (oldest message not read yet).)doc",
      py::arg("skip_to_newest") = false, py::return_value_policy::copy);

  // Existing: wait().
  subscriber_class.def(
      "wait",
      [](Subscriber *self) {
        absl::Status result = self->Wait();
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Wait until there's a message available to be read by the subscriber.");

  // Existing accessors.
  subscriber_class.def("type", &Subscriber::Type);
  subscriber_class.def("is_reliable", &Subscriber::IsReliable);
  subscriber_class.def("slot_size", &Subscriber::SlotSize);

  // New: read_message_object - returns a full Message with metadata.
  subscriber_class.def(
      "read_message_object",
      [](Subscriber *self, bool skip_to_newest) -> Message {
        absl::StatusOr<Message> read_result = self->ReadMessage(
            skip_to_newest ? ReadMode::kReadNewest : ReadMode::kReadNext);
        if (!read_result.ok()) {
          throw std::runtime_error(read_result.status().ToString());
        }
        return std::move(*read_result);
      },
      R"doc(Read a message and return a Message object with full metadata
(length, buffer, timestamp, ordinal, vchan_id, is_activation, slot_id,
checksum_error).  Use as a context manager to auto-release the slot:
    with sub.read_message_object() as msg:
        process(msg.buffer))doc",
      py::arg("skip_to_newest") = false, py::return_value_policy::move);

  // New accessors.
  subscriber_class.def("name", &Subscriber::Name,
                       "Get the channel name of this subscriber.");

  subscriber_class.def("num_slots", &Subscriber::NumSlots,
                       "Get the number of message slots.");

  subscriber_class.def("get_current_ordinal", &Subscriber::GetCurrentOrdinal,
                       "Get the most recently received ordinal.");

  subscriber_class.def("current_ordinal", &Subscriber::CurrentOrdinal,
                       "Get the current ordinal.");

  subscriber_class.def("timestamp", &Subscriber::Timestamp,
                       "Get the timestamp of the most recent message.");

  subscriber_class.def(
      "get_channel_counters",
      [](Subscriber *self) -> ChannelCounters {
        return self->GetChannelCounters();
      },
      "Get a snapshot of the channel counters.");

  subscriber_class.def("is_placeholder", &Subscriber::IsPlaceholder,
                       "Check if the subscriber is a placeholder (channel "
                       "does not yet exist).");

  subscriber_class.def("virtual_channel_id", &Subscriber::VirtualChannelId,
                       "Get the virtual channel ID.");

  subscriber_class.def("configured_vchan_id", &Subscriber::ConfiguredVchanId,
                       "Get the configured virtual channel ID.");

  subscriber_class.def("mux", &Subscriber::Mux,
                       "Get the mux name for this subscriber.");

  subscriber_class.def("num_subscribers", &Subscriber::NumSubscribers,
                       "Get the number of subscribers on this channel.",
                       py::arg("vchan_id") = -1);

  subscriber_class.def("get_virtual_memory_usage",
                       &Subscriber::GetVirtualMemoryUsage,
                       "Get virtual memory usage in bytes.");

  subscriber_class.def("num_active_messages", &Subscriber::NumActiveMessages,
                       "Get the number of messages currently held active.");

  subscriber_class.def("clear_active_message", &Subscriber::ClearActiveMessage,
                       "Release the reference to the current active message.");

  subscriber_class.def("trigger", &Subscriber::Trigger,
                       "Manually trigger the subscriber's poll fd.");

  subscriber_class.def("untrigger", &Subscriber::Untrigger,
                       "Clear the subscriber's trigger.");

  // find_message by timestamp.
  subscriber_class.def(
      "find_message",
      [](Subscriber *self, uint64_t timestamp) -> Message {
        absl::StatusOr<Message> result = self->FindMessage(timestamp);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return std::move(*result);
      },
      "Find a message by its timestamp. Returns a Message object.",
      py::arg("timestamp"), py::return_value_policy::move);

  // Wait variants.
  subscriber_class.def(
      "wait_with_timeout",
      [](Subscriber *self, int64_t timeout_ns) {
        absl::Status result =
            self->Wait(std::chrono::nanoseconds(timeout_ns));
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Wait with a timeout (in nanoseconds) for a message to arrive.",
      py::arg("timeout_ns"));

  subscriber_class.def(
      "wait_with_fd",
      [](Subscriber *self, int fd) -> int {
        toolbelt::FileDescriptor extra_fd(fd, /*owned=*/false);
        absl::StatusOr<int> result = self->Wait(extra_fd);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      R"doc(Wait for a message with an additional file descriptor that can
interrupt the wait. Returns the fd that triggered the wake-up.)doc",
      py::arg("fd"));

  // Poll / file descriptor access.
  subscriber_class.def(
      "get_poll_fd",
      [](Subscriber *self) -> int {
        struct pollfd pfd = self->GetPollFd();
        return pfd.fd;
      },
      "Get the poll file descriptor for use with select/poll/epoll.");

  subscriber_class.def(
      "get_file_descriptor",
      [](Subscriber *self) -> int {
        return self->GetFileDescriptor().Fd();
      },
      "Get the underlying trigger file descriptor.");

  // Batch message processing.
  subscriber_class.def(
      "process_all_messages",
      [](Subscriber *self, bool read_newest) {
        absl::Status result = self->ProcessAllMessages(
            read_newest ? ReadMode::kReadNewest : ReadMode::kReadNext);
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      R"doc(Read all available messages and invoke the registered message
callback for each one. You must first call register_message_callback().)doc",
      py::arg("read_newest") = false);

  subscriber_class.def(
      "get_all_messages",
      [](Subscriber *self, bool read_newest) -> py::list {
        absl::StatusOr<std::vector<Message>> result = self->GetAllMessages(
            read_newest ? ReadMode::kReadNewest : ReadMode::kReadNext);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        py::list messages;
        for (auto &msg : *result) {
          messages.append(std::move(msg));
        }
        return messages;
      },
      "Get all available messages as a list of Message objects.",
      py::arg("read_newest") = false);

  // Callbacks.
  subscriber_class.def(
      "register_message_callback",
      [](Subscriber *self, py::function callback) {
        absl::Status result = self->RegisterMessageCallback(
            [callback](Subscriber *, Message msg) {
              py::gil_scoped_acquire acquire;
              callback(std::move(msg));
            });
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      R"doc(Register a callback invoked for each message when
process_all_messages() is called. The callback receives a Message
object.)doc",
      py::arg("callback"));

  subscriber_class.def(
      "unregister_message_callback",
      [](Subscriber *self) {
        absl::Status result = self->UnregisterMessageCallback();
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Unregister the message callback.");

  subscriber_class.def(
      "register_dropped_message_callback",
      [](Subscriber *self, py::function callback) {
        absl::Status result = self->RegisterDroppedMessageCallback(
            [callback](Subscriber *, int64_t num_dropped) {
              py::gil_scoped_acquire acquire;
              callback(num_dropped);
            });
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      R"doc(Register a callback invoked when the subscriber drops messages.
The callback receives an integer count of messages that were missed.)doc",
      py::arg("callback"));

  subscriber_class.def(
      "unregister_dropped_message_callback",
      [](Subscriber *self) {
        absl::Status result = self->UnregisterDroppedMessageCallback();
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Unregister the dropped-message callback.");

  // -------------------------------------------------------------------------
  // Client
  // -------------------------------------------------------------------------
  py::class_<Client> client_class(m, "Client",
                                  R"doc(This is an Subspace client.
It must be initialized by calling Init() before
it can be used. The Init() function connects it to an Subspace server that
is listening on the same Unix Domain Socket.)doc");

  client_class.def(py::init<>());

  // Existing: init.
  client_class.def("init",
                   [](Client *self, const std::string &server_socket,
                      const std::string &client_name) {
                     absl::Status result =
                         self->Init(server_socket, client_name);
                     if (!result.ok()) {
                       throw std::runtime_error(result.ToString());
                     }
                   },
                   "Initialize the client by connecting to the server.",
                   py::arg("server_socket") = std::string("/tmp/subspace"),
                   py::arg("client_name") = std::string(""));

  // Existing: create_publisher overload 1 (slot_size, num_slots, flags).
  client_class.def(
      "create_publisher",
      [](Client *self, const std::string &channel_name, int slot_size,
         int num_slots, bool local, bool reliable, bool fixed_size,
         const std::string &type) -> Publisher {
        absl::StatusOr<Publisher> result =
            self->CreatePublisher(channel_name, slot_size, num_slots,
                                  PublisherOptions()
                                      .SetLocal(local)
                                      .SetReliable(reliable)
                                      .SetFixedSize(fixed_size)
                                      .SetType(type));
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return std::move(*result);
      },
      R"doc(Create a publisher for the given channel.  If the channel doesn't exit
it will be created with num_slots slots, each of which is slot_size
bytes long.)doc",
      py::arg("channel_name"), py::arg("slot_size"), py::arg("num_slots"),
      py::arg("local") = false, py::arg("reliable") = false,
      py::arg("fixed_size") = false, py::arg("type") = std::string(""),
      py::return_value_policy::move);

  // Existing: create_publisher overload 2 (options only).
  client_class.def(
      "create_publisher",
      [](Client *self, const std::string &channel_name,
         const PublisherOptions &options) -> Publisher {
        absl::StatusOr<Publisher> result =
            self->CreatePublisher(channel_name, options);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return std::move(*result);
      },
      R"doc(Create a publisher for the given channel with the specified options.)doc",
      py::arg("channel_name"), py::arg("options") = PublisherOptions(),
      py::return_value_policy::move);

  // Existing: create_publisher overload 3 (slot_size, num_slots, options).
  client_class.def(
      "create_publisher",
      [](Client *self, const std::string &channel_name, int slot_size,
         int num_slots, const PublisherOptions &options) -> Publisher {
        absl::StatusOr<Publisher> result =
            self->CreatePublisher(channel_name, slot_size, num_slots, options);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return std::move(*result);
      },
      R"doc(Create a publisher for the given channel with the specified slot size and number of slots.)doc",
      py::arg("channel_name"), py::arg("slot_size"), py::arg("num_slots"),
      py::arg("options") = PublisherOptions(), py::return_value_policy::move);

  // Existing: create_subscriber overload 1 (reliable, type flags).
  client_class.def(
      "create_subscriber",
      [](Client *self, const std::string &channel_name, bool reliable,
         const std::string &type) -> Subscriber {
        absl::StatusOr<Subscriber> result = self->CreateSubscriber(
            channel_name,
            SubscriberOptions().SetReliable(reliable).SetType(type));
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return std::move(*result);
      },
      R"doc(Create a subscriber for the given channel. This can be done before there
are any publishers on the channel.)doc",
      py::arg("channel_name"), py::arg("reliable") = false,
      py::arg("type") = std::string(""), py::return_value_policy::move);

  // Existing: create_subscriber overload 2 (options).
  client_class.def(
      "create_subscriber",
      [](Client *self, const std::string &channel_name,
         const SubscriberOptions &options) -> Subscriber {
        absl::StatusOr<Subscriber> result =
            self->CreateSubscriber(channel_name, options);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return std::move(*result);
      },
      R"doc(Create a subscriber for the given channel with the specified options.)doc",
      py::arg("channel_name"), py::arg("options") = SubscriberOptions(),
      py::return_value_policy::move);

  // New client methods.
  client_class.def("get_name", &Client::GetName,
                   "Get the name of this client.",
                   py::return_value_policy::copy);

  client_class.def("set_debug", &Client::SetDebug,
                   "Enable or disable debug output.", py::arg("v"));

  client_class.def("set_thread_safe", &Client::SetThreadSafe,
                   "Enable or disable thread-safe mode. When enabled, "
                   "get_message_buffer() holds a lock until publish_buffer() "
                   "or cancel_publish() is called.",
                   py::arg("v"));

  client_class.def(
      "get_channel_counters",
      [](Client *self, const std::string &channel_name) -> ChannelCounters {
        absl::StatusOr<const ChannelCounters> result =
            self->GetChannelCounters(channel_name);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      "Get the live channel counters for the named channel.",
      py::arg("channel_name"));

  client_class.def(
      "get_channel_info",
      [](Client *self, const std::string &channel_name) -> ChannelInfo {
        absl::StatusOr<const ChannelInfo> result =
            self->GetChannelInfo(channel_name);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      "Get information about a specific channel.", py::arg("channel_name"));

  client_class.def(
      "get_all_channel_info",
      [](Client *self) -> std::vector<ChannelInfo> {
        absl::StatusOr<const std::vector<ChannelInfo>> result =
            self->GetChannelInfo();
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      "Get information about all channels on the server.");

  client_class.def(
      "get_channel_stats",
      [](Client *self, const std::string &channel_name) -> ChannelStats {
        absl::StatusOr<const ChannelStats> result =
            self->GetChannelStats(channel_name);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      "Get cumulative statistics for a specific channel.",
      py::arg("channel_name"));

  client_class.def(
      "get_all_channel_stats",
      [](Client *self) -> std::vector<ChannelStats> {
        absl::StatusOr<const std::vector<ChannelStats>> result =
            self->GetChannelStats();
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      "Get cumulative statistics for all channels on the server.");

  client_class.def(
      "channel_exists",
      [](Client *self, const std::string &channel_name) -> bool {
        absl::StatusOr<bool> result = self->ChannelExists(channel_name);
        if (!result.ok()) {
          throw std::runtime_error(result.status().ToString());
        }
        return *result;
      },
      "Check whether a channel exists on the server.",
      py::arg("channel_name"));
}

} // namespace python
} // namespace subspace
