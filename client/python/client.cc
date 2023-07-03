
#include "client/client.h"

#include "pybind11/pybind11.h"
#include <stdexcept>
#include <string_view>

namespace subspace {
namespace python {

namespace py = pybind11;

PYBIND11_MODULE(subspace, m) {

  m.doc() = "This is a python module to pass messages over the Subspace inter-process communication protocol.";

  py::class_<Publisher> publisher_class(m, "Publisher", "The Publisher class is the main interface for sending messages.");

  publisher_class.def(
    "publish_message",
    [](Publisher* self, py::bytes message_data) {
      auto msg_view = static_cast<std::string_view>(message_data);
      absl::StatusOr<void *> dest_buffer = self->GetMessageBuffer(msg_view.size());
      if (!dest_buffer.ok()) {
        throw std::runtime_error(dest_buffer.status().ToString());
      }
      std::memcpy(*dest_buffer, msg_view.data(), msg_view.size());
      absl::StatusOr<Message> send_result = self->PublishMessage(msg_view.size());
      if (!send_result.ok()) {
        throw std::runtime_error(send_result.status().ToString());
      }
    },
    "Publish the message in the publisher's buffer.");

  publisher_class.def(
    "wait",
    [](Publisher* self) {
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


  py::class_<Subscriber> subscriber_class(m, "Subscriber", "The Subscriber class is the main interface for receiving messages.");

  subscriber_class.def(
    "read_message",
    [](Subscriber* self, bool skip_to_newest) {
      absl::StatusOr<Message> read_result = self->ReadMessage(skip_to_newest ? ReadMode::kReadNewest : ReadMode::kReadNext);
      if (!read_result.ok()) {
        throw std::runtime_error(read_result.status().ToString());
      }
      return py::bytes(reinterpret_cast<const char*>(read_result->buffer), read_result->length);
    },
    R"doc("Read a message from a subscriber. If there are no available messages,
the returned bytes will have zero length. Setting the 'skip_to_newest' argument
to True, causes the read to skip ahead to the newest available message, otherwise,
it reads the next available message (oldest message not read yet).)doc",
    py::arg("skip_to_newest") = false,
    py::return_value_policy::copy);

  subscriber_class.def(
    "wait",
    [](Subscriber* self) {
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

  client_class.def(
    "init",
    [](Client* self, const std::string& server_socket, const std::string& client_name) {
      absl::Status result = self->Init(server_socket, client_name);
      if (!result.ok()) {
        throw std::runtime_error(result.ToString());
      }
    },
    "Initialize the client by connecting to the server.",
    py::arg("server_socket") = std::string("/tmp/subspace"),
    py::arg("client_name") = std::string(""));


  client_class.def(
    "create_publisher",
    [](Client* self, const std::string &channel_name, int slot_size, int num_slots,
       bool local, bool reliable, bool fixed_size, const std::string& type) -> Publisher {
      absl::StatusOr<Publisher> result = self->CreatePublisher(channel_name, slot_size, num_slots,
        PublisherOptions().SetLocal(local).SetReliable(reliable).SetFixedSize(fixed_size).SetType(type));
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


  client_class.def(
    "create_subscriber",
    [](Client* self, const std::string &channel_name,
       bool reliable, const std::string& type) -> Subscriber {
      absl::StatusOr<Subscriber> result = self->CreateSubscriber(channel_name,
        SubscriberOptions().SetReliable(reliable).SetType(type));
      if (!result.ok()) {
        throw std::runtime_error(result.status().ToString());
      }
      return std::move(*result);
    },
    R"doc(Create a subscriber for the given channel. This can be done before there
are any publishers on the channel.)doc",
    py::arg("channel_name"), py::arg("reliable") = false, py::arg("type") = std::string(""),
    py::return_value_policy::move);

}

}  // namespace python
}  // namespace subspace


