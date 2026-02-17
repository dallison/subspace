
#include "client/client.h"

#include "pybind11/pybind11.h"
#include <stdexcept>
#include <string_view>

namespace subspace {
namespace python {

namespace py = pybind11;

PYBIND11_MODULE(subspace, m) {

  m.doc() = "This is a python module to pass messages over the Subspace "
            "inter-process communication protocol.";

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
           "Get the number of slots for the publisher.");

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
           "Get the virtual channel ID for the subscriber.");

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
                             "The buffer containing the message data.");

  py::class_<Publisher> publisher_class(
      m, "Publisher",
      "The Publisher class is the main interface for sending messages.");

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

  publisher_class.def(
      "wait",
      [](Publisher *self) {
        absl::Status result = self->Wait();
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Wait until a reliable publisher can try again to send a message.");

  publisher_class.def("type", &Publisher::Type);
  publisher_class.def("is_reliable", &Publisher::IsReliable);
  publisher_class.def("is_local", &Publisher::IsLocal);
  publisher_class.def("is_fixed_size", &Publisher::IsFixedSize);
  publisher_class.def("slot_size", &Publisher::SlotSize);

  py::class_<Subscriber> subscriber_class(
      m, "Subscriber",
      "The Subscriber class is the main interface for receiving messages.");

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

  subscriber_class.def(
      "wait",
      [](Subscriber *self) {
        absl::Status result = self->Wait();
        if (!result.ok()) {
          throw std::runtime_error(result.ToString());
        }
      },
      "Wait until there's a message available to be read by the subscriber.");

  subscriber_class.def("type", &Subscriber::Type);
  subscriber_class.def("is_reliable", &Subscriber::IsReliable);
  subscriber_class.def("slot_size", &Subscriber::SlotSize);

  py::class_<Client> client_class(m, "Client",
                                  R"doc(This is an Subspace client.
It must be initialized by calling Init() before
it can be used. The Init() function connects it to an Subspace server that
is listening on the same Unix Domain Socket.)doc");

  client_class.def(py::init<>());

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

  // Create a publisher with publisher options
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

  // Create a publisher with slot size, number of slots and options
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

  // Create a subscriber with options
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
}

} // namespace python
} // namespace subspace
