
#include "server/server.h"

#include "co/coroutine.h"
#include "pybind11/pybind11.h"
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace subspace {
namespace python {

namespace py = pybind11;

// Python-friendly wrapper around the Subspace Server.
//
// Owns the CoroutineScheduler, notification pipe, and background thread
// so that Python tests can start and stop a server without dealing with
// any of those details.
class ServerWrapper {
public:
  ServerWrapper(std::string socket_name, bool local)
      : socket_name_(std::move(socket_name)), local_(local) {}

  ~ServerWrapper() {
    if (running_) {
      StopInternal();
    }
  }

  void Start() {
    if (running_) {
      throw std::runtime_error("Server is already running");
    }

    int pipe_fds[2];
    if (::pipe(pipe_fds) != 0) {
      throw std::runtime_error("Failed to create notification pipe");
    }
    notify_read_fd_ = pipe_fds[0];

    server_ = std::make_unique<Server>(scheduler_, socket_name_,
                                       /*interface=*/"",
                                       /*disc_port=*/0,
                                       /*peer_port=*/0, local_,
                                       /*notify_fd=*/pipe_fds[1]);

    server_thread_ = std::thread([this]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Subspace server error: %s\n",
                s.ToString().c_str());
      }
    });

    // Block until the server signals it is ready.  Release the GIL so
    // the server thread (and other Python threads) can make progress.
    {
      py::gil_scoped_release release;
      char buf[8];
      (void)::read(notify_read_fd_, buf, sizeof(buf));
    }
    running_ = true;
  }

  void Stop() {
    if (!running_) {
      throw std::runtime_error("Server is not running");
    }
    StopInternal();
  }

  const std::string &GetSocketName() const { return socket_name_; }
  bool IsRunning() const { return running_; }

private:
  void StopInternal() {
    server_->Stop();

    // Wait for the server to finish shutting down.
    {
      py::gil_scoped_release release;
      char buf[8];
      (void)::read(notify_read_fd_, buf, sizeof(buf));
      server_thread_.join();
    }

    ::close(notify_read_fd_);
    notify_read_fd_ = -1;
    server_.reset();
    running_ = false;
  }

  std::string socket_name_;
  bool local_;
  co::CoroutineScheduler scheduler_;
  std::unique_ptr<Server> server_;
  std::thread server_thread_;
  int notify_read_fd_ = -1;
  bool running_ = false;
};

PYBIND11_MODULE(subspace_server, m) {
  m.doc() = "Python bindings for the Subspace server. Intended for use in "
            "test suites that need an in-process server running on a "
            "background thread.";

  py::class_<ServerWrapper>(m, "Server",
                            R"doc(An in-process Subspace server that runs on a
background thread.  Typical usage in a test:

    server = subspace_server.Server("/tmp/my_socket", local=True)
    server.start()
    # ... run tests against server.socket_name ...
    server.stop()

Or as a context manager:

    with subspace_server.Server("/tmp/my_socket", local=True) as server:
        # ... run tests against server.socket_name ...
)doc")
      .def(py::init<std::string, bool>(),
           R"doc(Create a server bound to the given Unix-domain socket path.

Args:
    socket_name: Path for the Unix-domain socket.
    local: If True the server will not attempt network discovery.)doc",
           py::arg("socket_name"), py::arg("local") = true)
      .def("start", &ServerWrapper::Start,
           "Start the server on a background thread.  Blocks until the "
           "server is ready to accept connections.")
      .def("stop", &ServerWrapper::Stop,
           "Stop the server and wait for the background thread to finish.")
      .def_property_readonly("socket_name", &ServerWrapper::GetSocketName,
                             "The Unix-domain socket path the server is "
                             "listening on.")
      .def_property_readonly("is_running", &ServerWrapper::IsRunning,
                             "Whether the server is currently running.")
      .def("__enter__",
           [](ServerWrapper *self) {
             self->Start();
             return self;
           })
      .def("__exit__",
           [](ServerWrapper *self, py::object, py::object, py::object) {
             if (self->IsRunning()) {
               self->Stop();
             }
             return py::none();
           });
}

} // namespace python
} // namespace subspace
