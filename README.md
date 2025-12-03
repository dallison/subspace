# Subspace IPC
Next Generation, sub-microsecond latency shared memory IPC.

This is a shared-memory based pub/sub Interprocess Communication system that can be used
in robotics and other applications.  Why *subspace*?  If your messages are transported
between processes on the same computer, they travel through extremely low latency
and high bandwidth shared memory buffers, kind of like they are going
faster than light (not really, of course).  If they go between computers, they are
transported over the network at sub-light speed.

## Acknowledgments

Some of the code in this project was contributed by Cruise LLC.

## Features

It has the following features:

1.	Single threaded coroutine based server process written in C++17
1.	Coroutine-aware client library, in C++17.
1.	Publish/subscribe methodology with multiple publisher and multiple subscribers per channel.
1.	No communication with server for message transfer.
1.	Message type agnostic transmission – bring your own serialization.
2.  Channel types, meaningful to user, not system.
1.	Single lock POSIX shared memory channels
1.	Both unreliable and reliable communications between publishers and subscribers.
1.	Ability to read the next or newest message in a channel.
1.	File-descriptor-based event triggers.
1.	Automatic UDP discovery and TCP bridging of channels between servers.
1.	Shared and weak pointers for message references.
1.	Ports to MacOS and Linux, ARM64 and x86_64.
1.	Builds using Bazel and uses Abseil and Protocol Buffers from Google.
1.	Uses my C++ coroutine library (https://github.com/dallison/co)

See the file docs/subspace.pdf for full documentation.

# Building

Subspace can be built using either Bazel or CMake. Both build systems will automatically download and build all required dependencies.

## Building with Bazel

This uses Google's Bazel to build.  You will need to download Bazel to build it.
The build also needs some external libraries, but Bazel takes care of downloading them.
The *.bazelrc* file contains some configuration options.

### To build on Mac Apple Silicon
```
bazel build --config=apple_silicon ...
```

### To build on Linux
Subspace really wants to be built using *clang* but modern *GCC* versions work well too.  Depending on how your OS is configured, you
might need to tell bazel what compiler to use.

```
CC=clang bazel build ...
```


### Example: Ubuntu 20.04
Build a minimal set of binaries:

```
CC=clang bazel build //server:subspace_server //manual_tests:{pub,sub}
```

Then run each in a separate terminal:

 * `./bazel-bin/server/subspace_server`
 * `./bazel-bin/manual_tests/sub`
 * `./bazel-bin/manual_tests/pub`

### Running Tests with Bazel

You can run tests directly using `bazel run` or `bazel test`. The `bazel run` command will build and execute the test in one step, while `bazel test` runs tests in test mode (useful for CI/CD).

**Note:** All tests automatically start a subspace server in a separate thread, so you don't need to run the server separately. The tests handle server lifecycle management internally.

#### client_test

The `client_test` is a comprehensive test suite that validates the core client functionality including publishers, subscribers, reliable/unreliable channels, message reading modes, and more.

```bash
# Run the test
bazel run //client:client_test

# Or run as a test (better for CI)
bazel test //client:client_test
```

#### latency_test

The `latency_test` measures message transmission latency between publishers and subscribers. This is useful for benchmarking performance.

```bash
# Run the latency test
bazel run //client:latency_test

# Run with custom options (if supported)
bazel run //client:latency_test -- --help
```

#### stress_test

The `stress_test` performs stress testing with high message rates and multiple publishers/subscribers to verify system stability under load.

```bash
# Run the stress test (may take a while)
bazel run //client:stress_test

# Or run as a test
bazel test //client:stress_test
```

#### Running All Tests

To run all tests at once:

```bash
# Run all tests
bazel test //...

# Run all tests in a specific directory
bazel test //client/...
bazel test //common/...
```

## Building with CMake

Subspace also supports building with CMake (version 3.15 or later). CMake uses FetchContent to automatically download and build all dependencies including Abseil, Protobuf, Googletest, cpp_toolbelt, and co.

### Prerequisites

- CMake 3.15 or later
- C++17 compatible compiler (clang or g++)
- Git (for fetching dependencies)

### Basic Build

```bash
mkdir build
cd build
cmake ..
make
```

### Build Options

You can customize the build with CMake options:

```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

### Running Tests

After building, you can run the tests:

```bash
cd build
ctest
```

Or run individual tests:

```bash
./client/client_test
./common/common_test
```

### Example: Building and Running

```bash
# Configure and build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Run the server in one terminal
./server/subspace_server

# Run publisher/subscriber examples in other terminals
./client/latency_test
./client/stress_test
```

### CMake Integration in Your Project

To use Subspace in your CMake project, you can add it as a subdirectory:

```cmake
# In your CMakeLists.txt
add_subdirectory(subspace)
target_link_libraries(your_target
    subspace_client
    subspace_common
    subspace_proto
)
```

Or use FetchContent:

```cmake
include(FetchContent)
FetchContent_Declare(
    subspace
    GIT_REPOSITORY https://github.com/dallison/subspace.git
    GIT_TAG main  # or specific tag/commit
)
FetchContent_MakeAvailable(subspace)

target_link_libraries(your_target
    subspace_client
    subspace_common
    subspace_proto
)
```

### CMake Build Targets

The CMake build provides the following targets:

- `subspace_client` - Client library
- `subspace_common` - Common utilities library
- `subspace_proto` - Protocol buffer definitions library
- `libserver` - Server library
- `subspace_server` - Server executable
- `client_test`, `latency_test`, `stress_test` - Test executables
- `common_test` - Common library tests
- `c_client_test` - C client tests

# Bazel WORKSPACE
Add this to your Bazel WORKSPACE file to get access to this library without downloading it manually.

```
http_archive(
  name = "subspace",
  urls = ["https://github.com/dallison/subspace/archive/refs/tags/A.B.C.tar.gz"],
  strip_prefix = "subspace-A.B.C",
)

```

You can also add a sha256 field to ensure a canonical build if you like.  Bazel
will tell you what to put in for the hash when you first build it.

# Using Subspace

## Overview

Subspace provides a high-performance, shared-memory based publish/subscribe IPC system. Messages are transmitted through POSIX shared memory with sub-microsecond latency. The system supports both reliable and unreliable message delivery, allowing you to choose the appropriate semantics for your use case.

## Client API

### Creating a Client

The `Client` class is the main entry point for using Subspace. You can create a client in two ways:

**Method 1: Using `Create()` (recommended)**
```cpp
#include "client/client.h"

auto client_or = subspace::Client::Create("/tmp/subspace", "my_client");
if (!client_or.ok()) {
    // Handle error
    return;
}
auto client = client_or.value();
```

**Method 2: Constructor + Init()**
```cpp
subspace::Client client;
auto status = client.Init("/tmp/subspace", "my_client");
if (!status.ok()) {
    // Handle error
    return;
}
```

**Parameters:**
- `server_socket` (default: `"/tmp/subspace"`): Path to the Unix domain socket where the Subspace server is listening
- `client_name` (default: `""`): Optional name for this client instance
- `c` (optional): Pointer to a coroutine if using coroutine-aware mode

### Client Methods

```cpp
class Client {
public:
    // Initialize the client by connecting to the server
    absl::Status Init(const std::string &server_socket = "/tmp/subspace",
                      const std::string &client_name = "");

    // Create a publisher for a channel
    absl::StatusOr<Publisher>
    CreatePublisher(const std::string &channel_name, 
                    int slot_size, 
                    int num_slots,
                    const PublisherOptions &opts = PublisherOptions());

    // Create a publisher with options specifying slot size and count
    absl::StatusOr<Publisher>
    CreatePublisher(const std::string &channel_name,
                    const PublisherOptions &opts = PublisherOptions());

    // Create a subscriber for a channel
    absl::StatusOr<Subscriber>
    CreateSubscriber(const std::string &channel_name,
                     const SubscriberOptions &opts = SubscriberOptions());

    // Get information about channels
    absl::StatusOr<const ChannelInfo> GetChannelInfo(const std::string &channelName);
    absl::StatusOr<const std::vector<ChannelInfo>> GetChannelInfo();
    
    absl::StatusOr<const ChannelStats> GetChannelStats(const std::string &channelName);
    absl::StatusOr<bool> ChannelExists(const std::string &channelName);

    // Enable/disable debug output
    void SetDebug(bool v);
    
    // Enable/disable thread-safe mode
    void SetThreadSafe(bool v);
};
```

## Publisher API

### Creating a Publisher

Publishers send messages to channels. You can create a publisher in two ways:

**Method 1: Explicit slot size and count**
```cpp
auto pub_or = client->CreatePublisher("my_channel", 1024, 10);
if (!pub_or.ok()) {
    // Handle error
    return;
}
auto pub = pub_or.value();
```

**Method 2: Using PublisherOptions**
```cpp
auto pub_or = client->CreatePublisher("my_channel", 
    subspace::PublisherOptions()
        .SetSlotSize(1024)
        .SetNumSlots(10)
        .SetReliable(true));
```

### Publishing Messages

```cpp
// Get a message buffer
auto buffer_or = pub.GetMessageBuffer(1024);
if (!buffer_or.ok()) {
    // Handle error (e.g., no free slots for reliable publisher)
    return;
}
void* buffer = buffer_or.value();

// Fill in your message data
MyMessageType* msg = reinterpret_cast<MyMessageType*>(buffer);
msg->field1 = 42;
msg->field2 = "hello";

// Publish the message
auto msg_info_or = pub.PublishMessage(sizeof(MyMessageType));
if (!msg_info_or.ok()) {
    // Handle error
    return;
}
auto msg_info = msg_info_or.value();
// msg_info.ordinal contains the message sequence number
// msg_info.timestamp contains the publish timestamp
```

**Using GetMessageBufferSpan (C++17 style):**
```cpp
auto span_or = pub.GetMessageBufferSpan(1024);
if (!span_or.ok() || span_or.value().empty()) {
    // Handle error
    return;
}
auto span = span_or.value();
// span is an absl::Span<std::byte>
MyMessageType* msg = reinterpret_cast<MyMessageType*>(span.data());
// ... fill message ...
pub.PublishMessage(sizeof(MyMessageType));
```

### Publisher Methods

```cpp
class Publisher {
public:
    // Get a message buffer for writing
    absl::StatusOr<void*> GetMessageBuffer(int32_t max_size = -1, bool lock = true);
    absl::StatusOr<absl::Span<std::byte>> GetMessageBufferSpan(int32_t max_size = -1, bool lock = true);
    
    // Publish a message
    absl::StatusOr<const Message> PublishMessage(int64_t message_size);
    
    // Cancel a publish (releases lock in thread-safe mode)
    void CancelPublish();
    
    // Wait for a reliable publisher to have a free slot
    absl::Status Wait(const co::Coroutine *c = nullptr);
    absl::Status Wait(std::chrono::nanoseconds timeout, const co::Coroutine *c = nullptr);
    absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd, const co::Coroutine *c = nullptr);
    
    // Get file descriptor for polling
    struct pollfd GetPollFd() const;
    toolbelt::FileDescriptor GetFileDescriptor() const;
    const toolbelt::FileDescriptor& GetRetirementFd() const;
    
    // Channel information
    std::string Name() const;
    std::string Type() const;
    bool IsReliable() const;
    bool IsLocal() const;
    bool IsFixedSize() const;
    int32_t SlotSize() const;
    int32_t NumSlots() const;
    
    // Statistics
    void GetStatsCounters(uint64_t &total_bytes, uint64_t &total_messages,
                          uint32_t &max_message_size, uint32_t &total_drops);
    
    // Resize callback registration
    absl::Status RegisterResizeCallback(
        std::function<absl::Status(Publisher*, int, int)> callback);
};
```

### Reliable Publisher Example

```cpp
// Create a reliable publisher
auto pub_or = client->CreatePublisher("reliable_channel", 256, 5,
    subspace::PublisherOptions().SetReliable(true));

auto pub = pub_or.value();

while (true) {
    // Wait for a free slot (blocks until available)
    auto status = pub.Wait();
    if (!status.ok()) {
        // Handle error
        break;
    }
    
    // Get message buffer
    auto buffer_or = pub.GetMessageBuffer(256);
    if (!buffer_or.ok()) {
        continue; // Should not happen after Wait()
    }
    
    // Fill and publish
    MyMessage* msg = reinterpret_cast<MyMessage*>(buffer_or.value());
    msg->data = compute_data();
    pub.PublishMessage(sizeof(MyMessage));
}
```

## Subscriber API

### Creating a Subscriber

```cpp
auto sub_or = client->CreateSubscriber("my_channel");
if (!sub_or.ok()) {
    // Handle error
    return;
}
auto sub = sub_or.value();
```

### Reading Messages

**Method 1: Read next message**
```cpp
auto msg_or = sub.ReadMessage(subspace::ReadMode::kReadNext);
if (!msg_or.ok()) {
    // Handle error
    return;
}
auto msg = msg_or.value();
if (msg.length == 0) {
    // No message available
    return;
}
// msg.buffer points to the message data
// msg.length is the message size in bytes
// msg.ordinal is the sequence number
// msg.timestamp is the publish timestamp
const MyMessageType* data = reinterpret_cast<const MyMessageType*>(msg.buffer);
```

**Method 2: Read newest message**
```cpp
auto msg_or = sub.ReadMessage(subspace::ReadMode::kReadNewest);
// This skips to the most recent message, discarding older ones
```

**Method 3: Typed read (returns shared_ptr)**
```cpp
auto msg_ptr_or = sub.ReadMessage<MyMessageType>();
if (!msg_ptr_or.ok() || !msg_ptr_or.value()) {
    // No message or error
    return;
}
auto msg_ptr = msg_ptr_or.value();
// msg_ptr is a subspace::shared_ptr<MyMessageType>
// Access data: msg_ptr->field1, (*msg_ptr).field2
// Message is automatically released when msg_ptr goes out of scope
```

### Waiting for Messages

```cpp
// Wait indefinitely
auto status = sub.Wait();
if (!status.ok()) {
    // Handle error
    return;
}

// Wait with timeout
auto status = sub.Wait(std::chrono::milliseconds(100));
if (status.code() == absl::StatusCode::kDeadlineExceeded) {
    // Timeout
}

// Wait with file descriptor (for integration with event loops)
toolbelt::FileDescriptor fd = /* your fd */;
auto fd_or = sub.Wait(fd);
if (fd_or.ok()) {
    int triggered_fd = fd_or.value();
    // Process message
}
```

### Subscriber Methods

```cpp
class Subscriber {
public:
    // Read messages
    absl::StatusOr<Message> ReadMessage(ReadMode mode = ReadMode::kReadNext);
    template <typename T>
    absl::StatusOr<shared_ptr<T>> ReadMessage(ReadMode mode = ReadMode::kReadNext);
    
    // Find message by timestamp
    absl::StatusOr<Message> FindMessage(uint64_t timestamp);
    template <typename T>
    absl::StatusOr<shared_ptr<T>> FindMessage(uint64_t timestamp);
    
    // Wait for messages
    absl::Status Wait(const co::Coroutine *c = nullptr);
    absl::Status Wait(std::chrono::nanoseconds timeout, const co::Coroutine *c = nullptr);
    absl::StatusOr<int> Wait(const toolbelt::FileDescriptor &fd, const co::Coroutine *c = nullptr);
    
    // Get file descriptor for polling
    struct pollfd GetPollFd() const;
    toolbelt::FileDescriptor GetFileDescriptor() const;
    
    // Channel information
    std::string Name() const;
    std::string Type() const;
    bool IsReliable() const;
    int32_t SlotSize() const;
    int32_t NumSlots() const;
    int64_t GetCurrentOrdinal() const;
    
    // Callbacks
    absl::Status RegisterDroppedMessageCallback(
        std::function<void(Subscriber*, int64_t)> callback);
    absl::Status RegisterMessageCallback(
        std::function<void(Subscriber*, Message)> callback);
    absl::Status ProcessAllMessages(ReadMode mode = ReadMode::kReadNext);
    
    // Statistics
    const ChannelCounters& GetChannelCounters();
    int NumActiveMessages() const;
};
```

### Subscriber Example with Callbacks

```cpp
auto sub_or = client->CreateSubscriber("my_channel",
    subspace::SubscriberOptions().SetReliable(true));

auto sub = sub_or.value();

// Register callback for dropped messages
sub.RegisterDroppedMessageCallback([](subspace::Subscriber* sub, int64_t count) {
    std::cerr << "Dropped " << count << " messages on " << sub->Name() << std::endl;
});

// Register callback for received messages
sub.RegisterMessageCallback([](subspace::Subscriber* sub, subspace::Message msg) {
    if (msg.length > 0) {
        process_message(msg);
    }
});

// In your event loop
while (true) {
    // Process all available messages
    sub.ProcessAllMessages();
    
    // Or wait and read manually
    sub.Wait();
    auto msg = sub.ReadMessage();
    if (msg.ok() && msg->length > 0) {
        process_message(*msg);
    }
}
```

## Reliable vs Unreliable Channels

### Reliable Channels

Reliable channels guarantee that **reliable subscribers** will never miss a message from **reliable publishers**. This is achieved through reference counting: a reliable publisher cannot reuse a slot until all reliable subscribers have released it.

**Characteristics:**
- Messages are never dropped for reliable subscribers
- Publishers may block if all slots are in use
- Higher memory usage (slots held until all subscribers release)
- Use `Wait()` to block until a slot is available

**When to use:**
- Critical data that must not be lost
- Control messages
- State synchronization
- Any scenario where message loss is unacceptable

**Example:**
```cpp
// Reliable publisher
auto pub = client->CreatePublisher("control", 128, 10,
    subspace::PublisherOptions().SetReliable(true)).value();

// Reliable subscriber
auto sub = client->CreateSubscriber("control",
    subspace::SubscriberOptions().SetReliable(true)).value();
```

### Unreliable Channels

Unreliable channels provide best-effort delivery with no guarantees. If a subscriber cannot keep up, messages may be dropped. This provides the lowest latency and highest throughput.

**Characteristics:**
- Messages may be dropped if subscriber is slow
- Publishers never block (always get a slot immediately)
- Lower memory usage
- Highest performance

**When to use:**
- High-frequency sensor data where occasional loss is acceptable
- Video/audio streaming
- Telemetry data
- Any scenario where latency is more important than reliability

**Example:**
```cpp
// Unreliable publisher (default)
auto pub = client->CreatePublisher("sensor_data", 64, 100).value();

// Unreliable subscriber (default)
auto sub = client->CreateSubscriber("sensor_data").value();
```

### Mixed Reliability

You can mix reliable and unreliable publishers/subscribers on the same channel:
- **Reliable subscriber + Reliable publisher**: Guaranteed delivery
- **Reliable subscriber + Unreliable publisher**: Best effort (may drop)
- **Unreliable subscriber + Reliable publisher**: May drop if slow
- **Unreliable subscriber + Unreliable publisher**: Best effort, may drop

## PublisherOptions

The `PublisherOptions` struct configures publisher behavior. You can use it in two ways:

### Method 1: Chained Setters (Fluent API)

```cpp
auto opts = subspace::PublisherOptions()
    .SetSlotSize(1024)
    .SetNumSlots(10)
    .SetReliable(true)
    .SetLocal(false)
    .SetType("MyMessageType")
    .SetFixedSize(false)
    .SetChecksum(true);

auto pub = client->CreatePublisher("channel", opts).value();
```

### Method 2: Designated Initializer (C++20)

```cpp
auto pub = client->CreatePublisher("channel", 
    subspace::PublisherOptions{
        .slot_size = 1024,
        .num_slots = 10,
        .reliable = true,
        .local = false,
        .type = "MyMessageType",
        .fixed_size = false,
        .checksum = true
    }).value();
```

### PublisherOptions Fields and Methods

| Field/Method | Type | Default | Description |
|--------------|------|---------|-------------|
| `slot_size` / `SetSlotSize()` | `int32_t` | `0` | Size of each message slot in bytes. Must be set if using options-only CreatePublisher. |
| `num_slots` / `SetNumSlots()` | `int32_t` | `0` | Number of slots in the channel. Must be set if using options-only CreatePublisher. |
| `reliable` / `SetReliable()` | `bool` | `false` | If true, reliable delivery (see Reliable Channels section). |
| `local` / `SetLocal()` | `bool` | `false` | If true, messages are only visible on the local machine (not bridged). |
| `type` / `SetType()` | `std::string` | `""` | User-defined message type identifier. All publishers/subscribers must use the same type. |
| `fixed_size` / `SetFixedSize()` | `bool` | `false` | If true, prevents automatic resizing of slots. |
| `bridge` / `SetBridge()` | `bool` | `false` | Internal: marks this as a bridge publisher. |
| `mux` / `SetMux()` | `std::string` | `""` | Multiplexer name for virtual channels. |
| `vchan_id` / `SetVchanId()` | `int` | `-1` | Virtual channel ID (-1 for server-assigned). |
| `activate` / `SetActivate()` | `bool` | `false` | If true, channel is activated even if unreliable. |
| `notify_retirement` / `SetNotifyRetirement()` | `bool` | `false` | If true, notify when slots are retired. |
| `checksum` / `SetChecksum()` | `bool` | `false` | If true, calculate checksums for all messages. |

**Getter Methods:**
- `int32_t SlotSize() const`
- `int32_t NumSlots() const`
- `bool IsReliable() const`
- `bool IsLocal() const`
- `bool IsFixedSize() const`
- `const std::string& Type() const`
- `bool IsBridge() const`
- `const std::string& Mux() const`
- `int VchanId() const`
- `bool Activate() const`
- `bool NotifyRetirement() const`
- `bool Checksum() const`

**Example: Creating a reliable publisher with checksums**
```cpp
auto pub = client->CreatePublisher("secure_channel", 512, 20,
    subspace::PublisherOptions()
        .SetReliable(true)
        .SetChecksum(true)
        .SetType("SecureMessage")).value();
```

## SubscriberOptions

The `SubscriberOptions` struct configures subscriber behavior. Like `PublisherOptions`, it supports both chained setters and designated initializers.

### Method 1: Chained Setters

```cpp
auto opts = subspace::SubscriberOptions()
    .SetReliable(true)
    .SetType("MyMessageType")
    .SetMaxActiveMessages(10)
    .SetChecksum(true)
    .SetPassChecksumErrors(false);

auto sub = client->CreateSubscriber("channel", opts).value();
```

### Method 2: Designated Initializer

```cpp
auto sub = client->CreateSubscriber("channel",
    subspace::SubscriberOptions{
        .reliable = true,
        .type = "MyMessageType",
        .max_active_messages = 10,
        .checksum = true,
        .pass_checksum_errors = false
    }).value();
```

### SubscriberOptions Fields and Methods

| Field/Method | Type | Default | Description |
|--------------|------|---------|-------------|
| `reliable` / `SetReliable()` | `bool` | `false` | If true, reliable delivery (see Reliable Channels section). |
| `type` / `SetType()` | `std::string` | `""` | User-defined message type identifier. Must match publisher type. |
| `max_active_messages` / `SetMaxActiveMessages()` | `int` | `1` | Maximum number of active messages (shared_ptrs) that can be held simultaneously. |
| `max_active_messages` / `SetMaxSharedPtrs()` | `int` | `0` | Alias: sets max_active_messages to n+1. |
| `log_dropped_messages` / `SetLogDroppedMessages()` | `bool` | `true` | If true, log when messages are dropped. |
| `bridge` / `SetBridge()` | `bool` | `false` | Internal: marks this as a bridge subscriber. |
| `mux` / `SetMux()` | `std::string` | `""` | Multiplexer name for virtual channels. |
| `vchan_id` / `SetVchanId()` | `int` | `-1` | Virtual channel ID (-1 for server-assigned). |
| `pass_activation` / `SetPassActivation()` | `bool` | `false` | If true, activation messages are passed to the user. |
| `read_write` / `SetReadWrite()` | `bool` | `false` | If true, map buffers as read-write instead of read-only. |
| `checksum` / `SetChecksum()` | `bool` | `false` | If true, verify checksums on received messages. |
| `pass_checksum_errors` / `SetPassChecksumErrors()` | `bool` | `false` | If true, pass messages with checksum errors (with flag set). If false, return error. |

**Getter Methods:**
- `bool IsReliable() const`
- `const std::string& Type() const`
- `int MaxActiveMessages() const`
- `int MaxSharedPtrs() const`
- `bool LogDroppedMessages() const`
- `bool IsBridge() const`
- `const std::string& Mux() const`
- `int VchanId() const`
- `bool PassActivation() const`
- `bool ReadWrite() const`
- `bool Checksum() const`
- `bool PassChecksumErrors() const`

**Example: Creating a reliable subscriber with checksum verification**
```cpp
auto sub = client->CreateSubscriber("secure_channel",
    subspace::SubscriberOptions()
        .SetReliable(true)
        .SetChecksum(true)
        .SetPassChecksumErrors(false)  // Return error on checksum failure
        .SetType("SecureMessage")
        .SetMaxActiveMessages(5)).value();
```

## Complete Example

Here's a complete example showing publisher and subscriber:

```cpp
#include "client/client.h"
#include <iostream>

struct SensorData {
    double temperature;
    double pressure;
    uint64_t timestamp;
};

int main() {
    // Create client
    auto client_or = subspace::Client::Create("/tmp/subspace", "sensor_app");
    if (!client_or.ok()) {
        std::cerr << "Failed to create client: " << client_or.status() << std::endl;
        return 1;
    }
    auto client = client_or.value();

    // Create reliable publisher
    auto pub_or = client->CreatePublisher("sensors", sizeof(SensorData), 10,
        subspace::PublisherOptions()
            .SetReliable(true)
            .SetType("SensorData"));
    if (!pub_or.ok()) {
        std::cerr << "Failed to create publisher: " << pub_or.status() << std::endl;
        return 1;
    }
    auto pub = pub_or.value();

    // Create reliable subscriber
    auto sub_or = client->CreateSubscriber("sensors",
        subspace::SubscriberOptions()
            .SetReliable(true)
            .SetType("SensorData"));
    if (!sub_or.ok()) {
        std::cerr << "Failed to create subscriber: " << sub_or.status() << std::endl;
        return 1;
    }
    auto sub = sub_or.value();

    // Publisher loop
    for (int i = 0; i < 100; ++i) {
        // Wait for free slot
        pub.Wait();
        
        // Get buffer
        auto buffer_or = pub.GetMessageBuffer(sizeof(SensorData));
        if (!buffer_or.ok()) continue;
        
        // Fill message
        SensorData* data = reinterpret_cast<SensorData*>(buffer_or.value());
        data->temperature = 20.0 + i * 0.1;
        data->pressure = 1013.25;
        data->timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        
        // Publish
        auto msg_or = pub.PublishMessage(sizeof(SensorData));
        if (msg_or.ok()) {
            std::cout << "Published message " << msg_or->ordinal << std::endl;
        }
    }

    // Subscriber loop
    for (int i = 0; i < 100; ++i) {
        // Wait for message
        sub.Wait();
        
        // Read message
        auto msg_or = sub.ReadMessage<SensorData>();
        if (!msg_or.ok() || !msg_or.value()) {
            continue;
        }
        
        auto msg = msg_or.value();
        std::cout << "Received: temp=" << msg->temperature 
                  << ", pressure=" << msg->pressure 
                  << ", ordinal=" << msg.GetMessage().ordinal << std::endl;
    }

    return 0;
}
```

## C Client Interface

Subspace provides a C API (`c_client/subspace.h`) for applications that need to use Subspace from C code or integrate it into other language bindings. The C API is simpler and has fewer dependencies than the C++ API, making it easier to integrate into projects that don't use C++.

### Error Handling

The C API uses a thread-local error mechanism similar to `errno`. Most functions return a boolean indicating success (`true`) or failure (`false`). When a function fails, you can check for errors and retrieve the error message:

```c
#include "c_client/subspace.h"

// Check if there was an error
if (subspace_has_error()) {
    // Get the error message
    char* error = subspace_get_last_error();
    fprintf(stderr, "Error: %s\n", error);
}
```

The error message is a static string owned by the library and is thread-local (one error message per thread).

### Creating a Client

```c
// Create client with default socket ("/tmp/subspace") and no name
SubspaceClient client = subspace_create_client();

// Create client with custom socket
SubspaceClient client = subspace_create_client_with_socket("/tmp/my_subspace");

// Create client with socket and name
SubspaceClient client = subspace_create_client_with_socket_and_name(
    "/tmp/subspace", "my_client_name");

// Check if client was created successfully
if (client.client == NULL) {
    fprintf(stderr, "Failed to create client: %s\n", subspace_get_last_error());
    return 1;
}

// Clean up when done
subspace_remove_client(&client);
```

### Creating Publishers and Subscribers

**Publisher Options:**

```c
// Create default publisher options
SubspacePublisherOptions pub_opts = subspace_publisher_options_default(1024, 10);
// pub_opts.slot_size = 1024
// pub_opts.num_slots = 10
// pub_opts.reliable = false
// pub_opts.fixed_size = false
// pub_opts.activate = false

// Customize options
pub_opts.reliable = true;
pub_opts.fixed_size = false;
pub_opts.type.type = "MyMessageType";
pub_opts.type.type_length = strlen(pub_opts.type.type);

// Create publisher
SubspacePublisher pub = subspace_create_publisher(client, "my_channel", pub_opts);
if (pub.publisher == NULL) {
    fprintf(stderr, "Failed to create publisher: %s\n", subspace_get_last_error());
    return 1;
}
```

**Subscriber Options:**

```c
// Create default subscriber options
SubspaceSubscriberOptions sub_opts = subspace_subscriber_options_default();
// sub_opts.reliable = false
// sub_opts.max_active_messages = 1
// sub_opts.pass_activation = false
// sub_opts.log_dropped_messages = false

// Customize options
sub_opts.reliable = true;
sub_opts.max_active_messages = 10;
sub_opts.type.type = "MyMessageType";
sub_opts.type.type_length = strlen(sub_opts.type.type);

// Create subscriber
SubspaceSubscriber sub = subspace_create_subscriber(client, "my_channel", sub_opts);
if (sub.subscriber == NULL) {
    fprintf(stderr, "Failed to create subscriber: %s\n", subspace_get_last_error());
    return 1;
}
```

### Publishing Messages

```c
// Get a message buffer
SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, 1024);
if (buffer.buffer == NULL) {
    // For reliable publishers, you may need to wait
    if (pub_opts.reliable) {
        subspace_wait_for_publisher(pub);
        buffer = subspace_get_message_buffer(pub, 1024);
    } else {
        fprintf(stderr, "Failed to get buffer: %s\n", subspace_get_last_error());
        return 1;
    }
}

// Fill in your message data
MyMessageType* msg = (MyMessageType*)buffer.buffer;
msg->field1 = 42;
msg->field2 = 3.14;

// Publish the message
const SubspaceMessage pub_status = subspace_publish_message(pub, sizeof(MyMessageType));
if (pub_status.length == 0) {
    fprintf(stderr, "Failed to publish: %s\n", subspace_get_last_error());
    return 1;
}
// pub_status.ordinal contains the message sequence number
// pub_status.timestamp contains the publish timestamp
```

### Reading Messages

```c
// Read next message
SubspaceMessage msg = subspace_read_message(sub);
if (msg.length == 0) {
    // No message available
    // For reliable subscribers, you may want to wait
    if (sub_opts.reliable) {
        subspace_wait_for_subscriber(sub);
        msg = subspace_read_message(sub);
    }
}

if (msg.length > 0) {
    // Process the message
    const MyMessageType* data = (const MyMessageType*)msg.buffer;
    printf("Received message ordinal: %lu\n", msg.ordinal);
    printf("Message timestamp: %lu\n", msg.timestamp);
    
    // IMPORTANT: Free the message when done
    subspace_free_message(&msg);
}

// Read newest message (skips to most recent)
SubspaceMessage newest = subspace_read_message_with_mode(sub, kSubspaceReadNewest);
if (newest.length > 0) {
    // Process message
    subspace_free_message(&newest);
}
```

**Important:** You must call `subspace_free_message()` when done with a message. The `max_active_messages` option determines how many messages you can hold simultaneously. If you don't free messages, the subscriber will run out of slots and be unable to read more messages.

### Waiting for Messages

```c
// Wait indefinitely for a message
if (!subspace_wait_for_subscriber(sub)) {
    fprintf(stderr, "Wait failed: %s\n", subspace_get_last_error());
    return 1;
}

// Wait with file descriptor (for integration with event loops)
int fd = /* your file descriptor */;
int triggered_fd = subspace_wait_for_subscriber_with_fd(sub, fd);
if (triggered_fd < 0) {
    fprintf(stderr, "Wait failed: %s\n", subspace_get_last_error());
    return 1;
}
```

### Using Poll/Epoll

The C API provides file descriptors that can be used with `poll()`, `epoll()`, or other event notification mechanisms:

```c
// Get pollfd structure for subscriber
struct pollfd pfd = subspace_get_subscriber_poll_fd(sub);
// pfd.fd is the file descriptor
// pfd.events should be set to POLLIN

// Use in poll() call
int ret = poll(&pfd, 1, timeout_ms);
if (ret > 0 && (pfd.revents & POLLIN)) {
    // Message available, read it
    SubspaceMessage msg = subspace_read_message(sub);
    // ... process message ...
    subspace_free_message(&msg);
}

// Or get the raw file descriptor
int fd = subspace_get_subscriber_fd(sub);
// Use fd with epoll, select, etc.
```

### Callbacks

The C API supports callbacks for message reception and dropped messages:

```c
// Message callback
void message_callback(SubspaceSubscriber sub, SubspaceMessage msg) {
    if (msg.length > 0) {
        printf("Received message of size %zu\n", msg.length);
        // Process message
        // IMPORTANT: Free the message when done
        subspace_free_message(&msg);
    }
}

// Register callback
if (!subspace_register_subscriber_callback(sub, message_callback)) {
    fprintf(stderr, "Failed to register callback: %s\n", subspace_get_last_error());
    return 1;
}

// Process all available messages (calls the callback for each)
subspace_process_all_messages(sub);

// Unregister callback
subspace_remove_subscriber_callback(sub);

// Dropped message callback
void dropped_callback(SubspaceSubscriber sub, int64_t count) {
    fprintf(stderr, "Dropped %ld messages\n", count);
}

subspace_register_dropped_message_callback(sub, dropped_callback);
```

### Complete C Example

```c
#include "c_client/subspace.h"
#include <stdio.h>
#include <string.h>

struct SensorData {
    double temperature;
    double pressure;
    uint64_t timestamp;
};

int main() {
    // Create client
    SubspaceClient client = subspace_create_client();
    if (client.client == NULL) {
        fprintf(stderr, "Failed to create client: %s\n", subspace_get_last_error());
        return 1;
    }

    // Create reliable publisher
    SubspacePublisherOptions pub_opts = subspace_publisher_options_default(
        sizeof(SensorData), 10);
    pub_opts.reliable = true;
    pub_opts.type.type = "SensorData";
    pub_opts.type.type_length = strlen(pub_opts.type.type);
    
    SubspacePublisher pub = subspace_create_publisher(client, "sensors", pub_opts);
    if (pub.publisher == NULL) {
        fprintf(stderr, "Failed to create publisher: %s\n", subspace_get_last_error());
        return 1;
    }

    // Create reliable subscriber
    SubspaceSubscriberOptions sub_opts = subspace_subscriber_options_default();
    sub_opts.reliable = true;
    sub_opts.type.type = "SensorData";
    sub_opts.type.type_length = strlen(sub_opts.type.type);
    
    SubspaceSubscriber sub = subspace_create_subscriber(client, "sensors", sub_opts);
    if (sub.subscriber == NULL) {
        fprintf(stderr, "Failed to create subscriber: %s\n", subspace_get_last_error());
        return 1;
    }

    // Publisher loop
    for (int i = 0; i < 100; ++i) {
        // Wait for free slot (reliable publisher)
        subspace_wait_for_publisher(pub);
        
        // Get buffer
        SubspaceMessageBuffer buffer = subspace_get_message_buffer(pub, sizeof(SensorData));
        if (buffer.buffer == NULL) {
            continue;
        }
        
        // Fill message
        struct SensorData* data = (struct SensorData*)buffer.buffer;
        data->temperature = 20.0 + i * 0.1;
        data->pressure = 1013.25;
        data->timestamp = /* get current time */;
        
        // Publish
        const SubspaceMessage pub_status = subspace_publish_message(pub, sizeof(SensorData));
        if (pub_status.length > 0) {
            printf("Published message %lu\n", pub_status.ordinal);
        }
    }

    // Subscriber loop
    for (int i = 0; i < 100; ++i) {
        // Wait for message
        subspace_wait_for_subscriber(sub);
        
        // Read message
        SubspaceMessage msg = subspace_read_message(sub);
        if (msg.length > 0) {
            const struct SensorData* data = (const struct SensorData*)msg.buffer;
            printf("Received: temp=%.2f, pressure=%.2f, ordinal=%lu\n",
                   data->temperature, data->pressure, msg.ordinal);
            subspace_free_message(&msg);
        }
    }

    // Cleanup
    subspace_remove_subscriber(&sub);
    subspace_remove_publisher(&pub);
    subspace_remove_client(&client);
    
    return 0;
}
```

### C API Reference

**Client Functions:**
- `SubspaceClient subspace_create_client(void)`
- `SubspaceClient subspace_create_client_with_socket(const char *socket_name)`
- `SubspaceClient subspace_create_client_with_socket_and_name(const char *socket_name, const char *client_name)`
- `bool subspace_remove_client(SubspaceClient *client)`

**Publisher Functions:**
- `SubspacePublisherOptions subspace_publisher_options_default(int32_t slot_size, int num_slots)`
- `SubspacePublisher subspace_create_publisher(SubspaceClient client, const char *channel_name, SubspacePublisherOptions options)`
- `SubspaceMessageBuffer subspace_get_message_buffer(SubspacePublisher publisher, size_t max_size)`
- `const SubspaceMessage subspace_publish_message(SubspacePublisher publisher, size_t messageSize)`
- `bool subspace_wait_for_publisher(SubspacePublisher publisher)`
- `int subspace_wait_for_publisher_with_fd(SubspacePublisher publisher, int fd)`
- `struct pollfd subspace_get_publisher_poll_fd(SubspacePublisher publisher)`
- `int subspace_get_publisher_fd(SubspacePublisher publisher)`
- `bool subspace_register_resize_callback(SubspacePublisher publisher, bool (*callback)(SubspacePublisher, int32_t, int32_t))`
- `bool subspace_unregister_resize_callback(SubspacePublisher publisher)`
- `bool subspace_remove_publisher(SubspacePublisher *publisher)`

**Subscriber Functions:**
- `SubspaceSubscriberOptions subspace_subscriber_options_default(void)`
- `SubspaceSubscriber subspace_create_subscriber(SubspaceClient client, const char *channel_name, SubspaceSubscriberOptions options)`
- `SubspaceMessage subspace_read_message(SubspaceSubscriber subscriber)`
- `SubspaceMessage subspace_read_message_with_mode(SubspaceSubscriber subscriber, SubspaceReadMode mode)`
- `bool subspace_free_message(SubspaceMessage *message)`
- `bool subspace_wait_for_subscriber(SubspaceSubscriber subscriber)`
- `int subspace_wait_for_subscriber_with_fd(SubspaceSubscriber subscriber, int fd)`
- `struct pollfd subspace_get_subscriber_poll_fd(SubspaceSubscriber subscriber)`
- `int subspace_get_subscriber_fd(SubspaceSubscriber subscriber)`
- `int32_t subspace_get_subscriber_slot_size(SubspaceSubscriber subscriber)`
- `int subspace_get_subscriber_num_slots(SubspaceSubscriber subscriber)`
- `SubspaceTypeInfo subspace_get_subscriber_type(SubspaceSubscriber subscriber)`
- `bool subspace_register_subscriber_callback(SubspaceSubscriber subscriber, void (*callback)(SubspaceSubscriber, SubspaceMessage))`
- `bool subspace_remove_subscriber_callback(SubspaceSubscriber subscriber)`
- `bool subspace_register_dropped_message_callback(SubspaceSubscriber subscriber, void (*callback)(SubspaceSubscriber, int64_t))`
- `bool subspace_remove_dropped_message_callback(SubspaceSubscriber subscriber)`
- `bool subspace_process_all_messages(SubspaceSubscriber subscriber)`
- `bool subspace_remove_subscriber(SubspaceSubscriber *subscriber)`

**Error Functions:**
- `char* subspace_get_last_error(void)`
- `bool subspace_has_error(void)`

## Message Types and Serialization

Subspace is message-type agnostic. You can send any data structure as long as it fits in the slot size. Common approaches:

1. **Plain C structs** (as shown above) - fastest, no serialization overhead
2. **Protocol Buffers** - cross-language, versioned
3. **Zero-copy facilities** like [Phaser](https://github.com/dallison/phaser) or [Neutron](https://github.com/dallison/neutron) - zero-copy, schema evolution
4. **JSON** - human-readable, flexible
5. **Custom binary formats**

The `type` field in `PublisherOptions` and `SubscriberOptions` is purely for application-level type checking - Subspace doesn't validate or enforce it.

## Thread Safety

By default, the `Client` class is **not thread-safe**. To enable thread-safe mode:

```cpp
client->SetThreadSafe(true);
```

In thread-safe mode:
- `GetMessageBuffer()` acquires a lock that is held until `PublishMessage()` or `CancelPublish()` is called
- You must call `PublishMessage()` or `CancelPublish()` after `GetMessageBuffer()`
- Multiple threads can safely use the same client instance


## Coroutine Support

Subspace is coroutine-aware. If you pass a coroutine pointer when creating the client, blocking operations will yield to other coroutines:

```cpp
co::CoroutineScheduler scheduler;
co::Coroutine* co = scheduler.CreateCoroutine([]() {
    auto client = subspace::Client::Create("/tmp/subspace", "co_client", 
                                           co::Coroutine::Current()).value();
    // ... use client ...
});
scheduler.Run();
```

When using coroutines, `Wait()` operations will yield instead of blocking the thread.
