# Subspace Remote Procedure Calls
This library provides a remote procedure call (RPC) facility built on top of Subspace IPC.
It has the following features:

1. Reliable shared memory transport
1. Intra and iter-computer calls
1. Type safe client and server API
1. Coroutine aware (you don't need to use coroutines)
1. Bazel integration using Starlark
1. gRPC style services in proto file
1. Server streaming calls
1. Protobuf serialization

The API looks something like a simpler version of gRPC.

## Service definition
A service is defined in a `.proto` file, just like gRPC does.  A Starlark
Bazel extension is provided to handle the service and generate code for both
the client and server side.

The Bazel extension is in `//rpc/subspace_rpc_library.bzl` and provides a
`subspace_rpc_library` macro that generates the client and server `cc_library`
rules.

For example, given the following `rpc_test.proto`file:

```
syntax = "proto3";

package rpc;

message TestRequest {
  string message = 1;
  int32 stream_period = 2;    // In milliseconds
}

message TestResponse {
  string message = 1;
  int32 foo = 2;
}

message PerfRequest {
  uint64 send_time = 1;
}

message PerfResponse {
  uint64 client_send_time = 1;   // Time sent by client.
  uint64 server_send_time = 2;   // Time sent by server.
}

service TestService {
  rpc TestMethod(TestRequest) returns (TestResponse);
  rpc PerfMethod(PerfRequest) returns (PerfResponse);
  rpc StreamMethod(TestRequest) returns (stream TestResponse);
}
```

You can write a BUILD.bazel (or just BUILD) that contains:

```
load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("//:rpc/subspace_rpc_library.bzl", "subspace_rpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "rpc_test_proto",
    srcs = ["rpc_test.proto"],
)

cc_proto_library(
    name = "rpc_test_cc_proto",
    deps = [":rpc_test_proto"],
)

# Generates both client and server:
# :rpc_test_subspace_rpc_client
# :rpc_test_subspace_rpc_server

subspace_rpc_library(
    name = "rpc_test_subspace_rpc",
    deps = [
        ":rpc_test_proto",
    ],
)

```

The target names for the macro are:

1. rpc_test_subspace_rpc_client
1. rpc_test_subspace_rpc_server

These are both `cc_library` rules that generate the client and server libraries
respectively.

The client and server will implement the `TestService` service with the following
RPC functions:

1. TestMethod
2. PerfMethod
3. StreamMethod

The client library will define a class with a member function for each of these
functions and the server library will define a class that has a pure virtual
function for each of them

## RPC server
Each service that wants to export a set of remote procedure calls runs an
`RPC Server` that handles those calls.  This may be in a process with other
RPC servers or might be in its own process.  The user registers a set of
functions with the server and those are called when the client asks for the
server to invoke them.  The server is aware of coroutines (my own coroutine
library that uses multiplex IO for scheduling) but you can just ignore that
and use a thread if you want.

The easist way to implement a server is to use the gRPC style `service` in
a `.proto` file with the messages that are used as arguments and results
for the RPC functions.

This gives you a class that you can override and provide implementations for
the functions the service provides.

For example, given the previously described `rpc_test.proto` file, it defines
a service called `TestService`, and the output is:

```c++
namespace rpc {
class TestServiceServer {
public:
  TestServiceServer(const std::string& subspace_socket) : server_(std::make_shared<subspace::RpcServer>("TestService", std::move(subspace_socket))) {}
  virtual ~TestServiceServer() = default;

  absl::Status RegisterMethods();

  absl::Status Run(co::CoroutineScheduler* scheduler = nullptr) {
    return server_->Run(scheduler);
  }

  void Stop() {
    server_->Stop();
  }

  void SetLogLevel(const std::string& level) {
    server_->SetLogLevel(level);
  }

protected:
  virtual absl::StatusOr<TestResponse> TestMethod(const TestRequest& request, co::Coroutine* c = nullptr) = 0;
  virtual absl::StatusOr<PerfResponse> PerfMethod(const PerfRequest& request, co::Coroutine* c = nullptr) = 0;
  virtual absl::Status StreamMethod(const TestRequest& request, subspace::StreamWriter<TestResponse>& writer, co::Coroutine* c = nullptr) = 0;

private:
  std::shared_ptr<subspace::RpcServer> server_;
};
} // namespace rpc

```

This is an abstract class for which the user is expected to provide a derived class that
implements the pure virtual functions.

To provide an implementation you can do something like this:

```c++
class MyServer : public rpc::TestServiceServer {
public:
  MyServer(const std::string &socket) : rpc::TestServiceServer(socket) {}

  absl::StatusOr<rpc::TestResponse> TestMethod(const rpc::TestRequest &request,
                                               co::Coroutine *c) override {
    // Implement TestMethod, returning a TestResponse.
  }

  absl::StatusOr<rpc::PerfResponse> PerfMethod(const rpc::PerfRequest &request,
                                               co::Coroutine *c) override {
     // Implement PerfMethod, returning a PerfResponse.
  }

  absl::Status StreamMethod(const rpc::TestRequest &request,
                            subspace::StreamWriter<rpc::TestResponse> &writer,
                            co::Coroutine *c) override {
    // Stream a bunch of TestResponses to the writer.
  }
};

```

Then to create your server:

```c++
  auto server = std::make_shared<MyServer>(RpcTest::Socket());
  auto s = server->RegisterMethods();
  // Check 's' is ok.
```
Note that the creation of the server and the registration of its methods are
separate calls because the base class is abstract and can't be created using
a static `Create` method.

To run the server, you call the `Run` function.  This can either be called
without a coroutine pointer, in which case it will block until it is stopped, or
if you are cool, you can use a coroutine which will allow it to run cooperatively
with other servers in the same process.

Obviously the easiest way is to run the server in a thread but you can get more
elegant if you want.

## Client
In order to invoke methods on a service you need to create a client.  There may
be many clients attached to a single service (server).

Each client must be given a unique 64-bit number when it is created.  This would
usually come from the process ID (call `getpid()`) or maybe a thread id obtained
from the OS, or you could just pick a number (I like 6502 for nostalgic reasons).

For our afforementioned `rpc_test.proto` file, our generated client looks like:

```c++
namespace rpc {
class TestServiceClient {
public:
  TestServiceClient(uint64_t client_id, std::string subspace_socket) : client_(std::make_shared<subspace::RpcClient>("TestService", client_id, std::move(subspace_socket))) {
  }
  static absl::StatusOr<std::shared_ptr<TestServiceClient>> Create(uint64_t client_id, const std::string& subspace_socket, co::Coroutine* c = nullptr) {
    auto client = std::make_shared<TestServiceClient>(client_id, subspace_socket);
    auto status = client->Open(c);
    if (!status.ok()) {
      return status;
    }
    return client;
  }

  absl::Status Open(co::Coroutine* c = nullptr) {
    return client_->Open(c);
  }
  absl::Status Close(co::Coroutine* c = nullptr) {
    return client_->Close(c);
  }
  void SetLogLevel(const std::string& level) {
    client_->SetLogLevel(level);
  }

  absl::StatusOr<TestResponse> TestMethod(const TestRequest& request, std::chrono::nanoseconds timeout, co::Coroutine* c = nullptr);
  absl::StatusOr<TestResponse> TestMethod(const TestRequest& request, co::Coroutine* c = nullptr) {
    return TestMethod(request, std::chrono::nanoseconds(0), c);
  }
  absl::StatusOr<PerfResponse> PerfMethod(const PerfRequest& request, std::chrono::nanoseconds timeout, co::Coroutine* c = nullptr);
  absl::StatusOr<PerfResponse> PerfMethod(const PerfRequest& request, co::Coroutine* c = nullptr) {
    return PerfMethod(request, std::chrono::nanoseconds(0), c);
  }
  absl::Status StreamMethod(const TestRequest& request, subspace::ResponseReceiver<TestResponse>& receiver, std::chrono::nanoseconds timeout, co::Coroutine* c = nullptr);
  absl::Status StreamMethod(const TestRequest& request, subspace::ResponseReceiver<TestResponse>& receiver, co::Coroutine* c = nullptr) {
    return StreamMethod(request, receiver, std::chrono::nanoseconds(0), c);
  }


private:
  std::shared_ptr<subspace::RpcClient> client_;
};
```

This is a concrete class that has a static `Create` function you can use to create it, or you can use the constructor
and `Open` calls if you prefer.  If you are in a coroutine environment, you will need to pass a pointer to the coroutine
if you want to avoid blocking.  The `subspace_socket` is the name of a Unix Domain Socket that the `Subspace` server
is listening on.  This is usually `/tmp/subspace` but your environment might provide a different name.

It contains a function for each RPC function defined in the service.  These are all `blocking` methods that send
the request and wait for the response.  For each RPC there are 2 functions provided, one with a timeout and one
without. If there is no timeout the client will wait forever for the response.  If there is a timeout, an
error will be returned.  If you are using coroutines, the `blocking` is coroutine aware and will not block
the coroutine scheduler.

To create a client to the `TestService` service running on Subspace socket `/tmp/mysubspace` that is not in a
coroutine environemnt:

```c++
  auto cl = rpc::TestServiceClient::Create(getpid(), "/tmp/mysubspace");
  if (!cl.ok()) {
    // Handle error.
  }
  auto client = *cl;
```

Then, to call an RPC with no timeout (and no coroutine):

```c++
    rpc::TestRequest req;
    req.set_message("this is a test");
    absl::StatusOr<rpc::TestResponse> r = client->TestMethod(req);
    // Check 'r' for ok() and use result.
```

If you want to provide a timeout (of 10 seconds say):

```c++
    rpc::TestRequest req;
    req.set_message("this is a test");
    absl::StatusOr<rpc::TestResponse> r = client->TestMethod(req, 10s);
    // Check 'r' for ok() and use result.
```

This assumes you have done the following to pick up the `10s` syntax.

```c++
#include <chrono>
using namespace std::chrono_literals;

```
If the server doesn't respond withing 10 seconds, you will get an error.

## Streaming functions
Like gRPC, we provide a way for a single RPC function to result in multiple responses
sent by the server.  This is referred to as `server streaming`.  gRPC also provides a
`client streaming` facility but that isn't supported at time of writing.

You need to implement streaming on both the client and the server.  In the `.proto`
file you enable it by:

```
rpc StreamMethod(TestRequest) returns (stream TestResponse);

```
This says that the `StreamMethod` RPC takes a `TestRequest` argument and results in
a stream of `TestResponse` messages.

### Server stream
On the server side your override of the pure virtual function will be called with
an instance of `StreamWriter` templated object.  This is defined as:

```c++
template <typename Response> struct StreamWriter {
  // Returns true if the write worked, false if the request was cancelled.
  bool Write(const Response &res, co::Coroutine *c);

  void Finish(co::Coroutine *c);

  void Cancel();

  bool IsCancelled() const;
};
```

The 4 public functions are:

* Write: write a response to the client
* Finish: finish up a sequence of reponses with an empty one that terminates the stream
* Cancel: cancel the stream from the server end
* IsCancelled: has the client or the server cancelled this stream?

Here's an example of an implementation of the `StreamMethod` that sends 200 responses.  It
is defined inside the derived class for the service.  It checks for cancellation by the
client.

```c++
  absl::Status StreamMethod(const rpc::TestRequest &request,
                            subspace::StreamWriter<rpc::TestResponse> &writer,
                            co::Coroutine *c) override {
    constexpr int kNumResults = 200;
    for (int i = 0; i < kNumResults; i++) {
      if (writer.IsCancelled()) {
        writer.Finish(c);
        return absl::OkStatus();
      }
      rpc::TestResponse r;
      r.set_message("Hello from StreamMethod part " + std::to_string(i));
      writer.Write(r, c);
      c->Millisleep(request.stream_period());
    }

    writer.Finish(c);
    return absl::OkStatus();
```

Unless it is cancelled, it will send 200 results at a frequency passed in the request.

### Client side
On the client side, we provide a similar concept.  The function for the `StreamMethod` is
defined as:

```c++
  absl::Status StreamMethod(const TestRequest& request, subspace::ResponseReceiver<TestResponse>& receiver, co::Coroutine* c = nullptr);
```

This takes a request and sends it to the server, then blocks until the server finishes sending the
data for the stream.  Each time a response is received the `receiver` will be invoked.  This is
defined as an instance of a templated class:

```c++
template <typename Response> class ResponseReceiver {
public:
  ResponseReceiver() = default;
  virtual ~ResponseReceiver() = default;

  // Called when a response is received.
  virtual void OnResponse(const Response &response) = 0;

  virtual void OnFinish() = 0;

  // Called when an error occurs.
  virtual void OnError(const absl::Status &status) = 0;

  virtual void OnCancel() = 0;

  bool IsCancelled() const { return cancelled_; }

  absl::Status Cancel(std::chrono::nanoseconds timeout,
                      co::Coroutine *c = nullptr);

  absl::Status Cancel(co::Coroutine *c = nullptr) {
    return Cancel(std::chrono::nanoseconds(0), c);
  }
};

```

The intention is that you provide a class derived from this and override the
pure virtual member functions to provide your own implementation.  It's pretty
straightforward and boiler-plate:

```c++
    class MyResponseReceiver
        : public subspace::ResponseReceiver<rpc::TestResponse> {
    public:
      void OnResponse(const rpc::TestResponse &response) override {
        // Implement reception of a response
      }
      void OnError(const absl::Status &status) override {
        // Received an ereror
      }
      void OnCancel() override {
        // We got cancelled.
      }
      void OnFinish() override {
        // Stream is done
      }
    };

```

If you want to cancel a stream you can call 'Cancel' on the `ResponseReceiver` object and when the
cancellation happens, the `onCancel` function will be called.  Of course, the stream may have
finished before that occurs so don't rely on `onCancel` always being called.


## Advanced usage
You don't need to use the gRPC style interface to use this.  The `subspace::RpcClient` and `subspace::RpcServer`
classes are very usable and the gRPC stuff is just a wrapper around them.

Take a look at the `rpc/client/rpc_client.h` and `rpc/server/rpc_server.h` to see what those
classes provide.

I promise I will document them fully soon...


## How it works
As the name suggests, this uses Subspace IPC for its transport.  Subspace already supports reliable
communication (as an option) so this creates a set of publishers and subscribers for a bunch
of channels and uses them to send and receive messages.  We use protobuf as the serialization
mechanism for now (but maybe we can provide zero-copy in the future)

When you create a service, the server creates:

1. A subscriber to the channel `/rpc/SERVICE/request`
2. A publisher to the channel `/rpc/SERVICE/response`

Where `SERVICE` is the name of the service.

The client then creates:

1. A publisher to `/rpc/SERVICE/request`
2. A subscriber to `/rpc/SERVICE/response`

The channel parameters (slot size and number of slots) is set to something reasonable but
can be overridden as needed.  The slot size will be increased on demand.

The client opens a connection to the server by sending an 'RpcServerRequest' protobuf message
to the `/rpc/SERVICE/request` channel containing an `RpcOpenRequest`.  These messages are
defined as:

```
message RpcOpenRequest {}

message RpcServerRequest {
  uint64 client_id = 1;
  int32 request_id = 2;
  oneof request {
    RpcOpenRequest open = 3;
    RpcCloseRequest close = 4;
  }
}

```

The `RpcServerRequest` contains a unique client ID and a request ID specific for this request.  These
are used to correlate the responses.

The server will respond with a `RpcServerResponse` containing `RpcOpenResponse` defined as:

```
message RpcOpenResponse {
  message RequestChannel {
    string name = 1;     // Name of the channel.
    int32 slot_size = 2; // Size of each slot in bytes.
    int32 num_slots = 3; // Number of slots in the channel.
    string type = 4;     // Type of data carried on this channel.
  }

  message ResponseChannel {
    string name = 1; // Name of the channel.
    string type = 2; // Type of data carried on this channel.
  }
  message Method {
    string name = 1; // Name of the method to call
    int32 id = 2;
    RequestChannel request_channel = 3;   // Channel to send request on.
    ResponseChannel response_channel = 4; // Channel to receive response on.
    string cancel_channel = 5;            // To cancel streams.
  }
  int32 session_id = 1;        // Session ID for this server.
  repeated Method methods = 2; // List of methods available on this server.
  uint64 client_id = 3;        // Client that this is destined for.
}

message RpcServerResponse {
  uint64 client_id = 1;
  int32 request_id = 2;
  oneof response {
    RpcOpenResponse open = 3;
    RpcCloseResponse close = 4;
  }
  string error = 5;
}

```
This is reasonably complex but really just contains the set of methods that are supported by
the server and information associated with them.  The important fields are:

1. request_channel: information about the channel the client will use to send requests
2. response_channel: info about the response channel
3. id: a number identifying this method on the server.

The server also returns a `session_id` which uniquely identifies the session that was
opened.

The client uses this information to create publishers and subscribers to the method
channels.

For completeness, the channel names are formatted as `/rpc/SERVICE/METHOD/request/CLIENT/SESSION` for
the requests and (unsurprisingly) `/rpc/SERVICE/METHOD/response/CLIENT/SESSION` for the responses.

Where:

* SERVICE is the service name
* METHOD is the method name
* CLIENT is the client ID provided by the client
* SESSION is the session ID provided by the server

For example, the request channel for the `TestMethod` in `TestService` would be `/rpc/TestService/TestMethod/request/1234/1`
for client 1234 and session 1.

To invoke a method, the client sends a `RpcRequest` message to the request channel:

```
message RpcRequest {
  int32 method = 1;
  google.protobuf.Any argument = 2; // Data to send to the server.
  int32 session_id = 3;             // Session ID for this request.
  int32 request_id = 4;             // Unique ID for this request.
  uint64 client_id = 5;             // Client ID making the request.
}

```
In addition to identifying the session, request and client, it also contains the number of the method to
invoke.  The numbers are assigned by the server and correspond to method name strings.

The argument for the method is sent as a `google.protobuf.Any` message.

The response comes back as `RpcResponse`:

```
message RpcResponse {
  string error = 1;               // Error message if any.
  google.protobuf.Any result = 2; // Data returned by the server.
  int32 session_id = 3;           // Session ID for this response.
  int32 request_id = 4;           // Unique ID for this response, matches request.
  uint64 client_id = 5;           // Client ID making the request.
  bool is_last = 6;               // Whether this is the last response in a stream.
  bool is_cancelled = 7;          // Whether this response is for a cancelled request.
}

```
If `error` is not empty there is an error.  The `result` will contain the result if there was one.

Since this uses reliable channels, no messages will be dropped and the publisher may be
blocked from sending a message if a subscriber will miss it.

The performance is really good.
