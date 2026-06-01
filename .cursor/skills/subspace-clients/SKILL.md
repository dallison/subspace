---
name: subspace-clients
description: Write Subspace clients in C++, Python, Rust, or Java. Use when the user asks how to connect to Subspace, create publishers or subscribers, publish messages, read messages, or build language client libraries with Bazel, CMake, or Android Soong/Blueprint.
---

# Subspace Clients

## Core Model

- A `subspace_server` process must be running before clients connect.
- Non-Android default socket: `/tmp/subspace`.
- Android default socket: `/data/local/tmp/subspace`.
- A publisher creates a channel with a slot size and slot count, writes into a
  shared-memory message buffer, then publishes the written byte count.
- A subscriber attaches to a channel, waits or polls for data, then reads until
  the API reports no more messages.
- User-facing build examples use `bazel`; `bazelisk` is a drop-in replacement
  when `bazel` is not available.

## C++ Client

Include `client/client.h` and link `//client:subspace_client` or the CMake
`subspace_client` target.

Use a `subspace::Client` when one process needs to create multiple publishers
or subscribers that share one server connection. If the publisher or subscriber
can own its own connection, use the convenience free functions:

```cpp
auto pub_or = subspace::CreatePublisher(
    "example", subspace::PublisherOptions().SetSlotSize(256).SetNumSlots(8),
    "/tmp/subspace", "cpp-publisher");

auto sub_or = subspace::CreateSubscriber(
    "example", subspace::SubscriberOptions(), "/tmp/subspace",
    "cpp-subscriber");
```

Publish:

```cpp
#include "client/client.h"
#include <cstring>

subspace::Client client;
auto status = client.Init("/tmp/subspace", "cpp-publisher");
if (!status.ok()) throw std::runtime_error(status.ToString());

auto pub_or = client.CreatePublisher("example", 256, 8);
if (!pub_or.ok()) throw std::runtime_error(pub_or.status().ToString());
subspace::Publisher pub = *std::move(pub_or);

auto buffer_or = pub.GetMessageBuffer();
if (!buffer_or.ok() || *buffer_or == nullptr) {
  throw std::runtime_error("no publisher buffer available");
}

const char payload[] = "hello from cpp";
std::memcpy(*buffer_or, payload, sizeof(payload) - 1);
auto msg_or = pub.PublishMessage(sizeof(payload) - 1);
if (!msg_or.ok()) throw std::runtime_error(msg_or.status().ToString());
```

Subscribe:

```cpp
subspace::Client client;
auto status = client.Init("/tmp/subspace", "cpp-subscriber");
if (!status.ok()) throw std::runtime_error(status.ToString());

auto sub_or = client.CreateSubscriber("example");
if (!sub_or.ok()) throw std::runtime_error(sub_or.status().ToString());
subspace::Subscriber sub = *std::move(sub_or);

status = sub.Wait();
if (!status.ok()) throw std::runtime_error(status.ToString());

for (;;) {
  auto msg_or = sub.ReadMessage();
  if (!msg_or.ok()) throw std::runtime_error(msg_or.status().ToString());
  if (msg_or->length == 0) break;
  auto bytes = static_cast<const char *>(msg_or->buffer);
  // Process bytes[0..msg_or->length).
}
```

## Python Client

The Python binding is `client.python.subspace`. The server binding used by
tests is `server.python.subspace_server`.

```python
import client.python.subspace as subspace

client = subspace.Client()
client.init(server_socket="/tmp/subspace", client_name="python-client")

publisher = client.create_publisher(
    channel_name="example", slot_size=256, num_slots=8
)
subscriber = client.create_subscriber(channel_name="example")

publisher.publish_message(b"hello from python")
subscriber.wait()

while True:
    data = subscriber.read_message()
    if len(data) == 0:
        break
    # Process data as bytes.
```

For message metadata and ordinals, use `read_message_object()`:

```python
with subscriber.read_message_object() as msg:
    print(msg.ordinal, msg.timestamp, msg.buffer)
```

## Rust Client

The Rust crate name is `subspace_client`.

```rust
use subspace_client::{Client, PublisherOptions, ReadMode, SubscriberOptions};

let client = Client::new("/tmp/subspace", "rust-client").unwrap();

let pub_opts = PublisherOptions::new()
    .set_slot_size(256)
    .set_num_slots(8);
let publisher = client.create_publisher("example", &pub_opts).unwrap();

let payload = b"hello from rust";
let (buf_ptr, _capacity) = publisher
    .get_message_buffer(payload.len() as i64)
    .unwrap()
    .unwrap();
unsafe {
    std::ptr::copy_nonoverlapping(payload.as_ptr(), buf_ptr, payload.len());
}
publisher.publish_message(payload.len() as i64).unwrap();

let sub_opts = SubscriberOptions::new();
let subscriber = client.create_subscriber("example", &sub_opts).unwrap();
subscriber.wait(Some(5000)).unwrap();

loop {
    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    if msg.is_empty() {
        break;
    }
    let data = unsafe { msg.as_slice() };
    // Process data.
}
```

## Java Client

The Java client is Android-focused and uses JNI. Load `libsubspace_jni.so`
normally via `System.loadLibrary("subspace_jni")`; device-side tests may preload
native libraries with absolute paths when using `/data/local/tmp`.

```java
import com.subspace.SubspaceClient;
import com.subspace.SubspaceMessage;
import com.subspace.SubspacePublisher;
import com.subspace.SubspaceSubscriber;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

byte[] payload = "hello from java".getBytes(StandardCharsets.UTF_8);

try (SubspaceClient client =
             new SubspaceClient("/data/local/tmp/subspace", "java-client");
     SubspacePublisher publisher =
             client.createPublisher("/example", 256, 8);
     SubspaceSubscriber subscriber =
             client.createSubscriber("/example")) {

    ByteBuffer buffer = publisher.getMessageBuffer(payload.length);
    if (buffer == null) {
        throw new IllegalStateException("no publisher buffer available");
    }
    buffer.put(payload);
    publisher.publishMessage(payload.length);

    SubspaceMessage message = subscriber.readMessage();
    if (message != null) {
        ByteBuffer data = message.getData();
        // Process data[0..message.getLength()).
    }
}
```

## Build The Clients

C++:

```bash
bazel build //client:subspace_client
cmake -S . -B build/cmake-Debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/cmake-Debug --target subspace_client
```

Python:

```bash
bazel build //client/python:subspace
bazel test //client/python:client_test

cmake -S . -B build/cmake-Debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/cmake-Debug --target subspace
```

Rust:

```bash
bazel build //rust_client:subspace_client_rust
bazel test //rust_client:client_test

cmake -S . -B build/cmake-Debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/cmake-Debug --target subspace_client_rust

cd rust_client && cargo build && cargo test -- --test-threads=1
```

Java/JNI for Android:

```bash
bazel build \
  //android/jni:libsubspace_jni.so \
  //android/java:subspace-java \
  //android/java:subspace-java-test \
  --config=android_x86_64

cmake -S . -B build/android \
  -DCMAKE_TOOLCHAIN_FILE=$ANDROID_NDK_HOME/build/cmake/android.toolchain.cmake \
  -DANDROID_ABI=x86_64 \
  -DANDROID_PLATFORM=android-28 \
  -DANDROID_STL=c++_shared \
  -DCMAKE_BUILD_TYPE=Release \
  -DPROTOC_EXECUTABLE=/path/to/matching/host/protoc
cmake --build build/android --target subspace_jni subspace_java subspace_java_test
```

Soong/Blueprint in AOSP:

```bash
m external.subspace-libsubspace_client-soong \
  external.subspace-libsubspace_jni-soong \
  external.subspace-subspace-java-soong \
  external.subspace-subspace_java_client_test-soong
```

## References

- C++ API: `client/client.h`
- Python examples: `client/python/client_test.py`
- Rust guide: `docs/rust-client.md`
- Java API: `android/java/com/subspace/*.java`
- Android build/run details: `docs/android.md`
- Platform build skill: `.cursor/skills/subspace-builds/SKILL.md`
