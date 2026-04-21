# Subspace RPC — Rust Client & Server

An async RPC framework built on top of Subspace shared-memory IPC. Services are
defined in Protocol Buffer `.proto` files. The `subspace_rpc` IDL compiler plugin
generates type-safe Rust client and server stubs.

## Prerequisites

- A running **subspace server** (`subspace_server`).
- The **protoc** compiler (Protocol Buffers).
- The **subspace_rpc** protoc plugin (`//rpc/idl_compiler:subspace_rpc`).
- Rust 2021 edition or later.

## Defining a Service

Write a standard `.proto` file with `service` definitions. Both unary and
server-streaming methods are supported:

```proto
syntax = "proto3";
package myapp;

message GreetRequest {
  string name = 1;
}

message GreetResponse {
  string greeting = 1;
}

message EventRequest {
  int32 count = 1;
}

message Event {
  string data = 1;
  int32 seq = 2;
}

service Greeter {
  // Unary RPC.
  rpc SayHello(GreetRequest) returns (GreetResponse);

  // Server-streaming RPC.
  rpc Subscribe(EventRequest) returns (stream Event);
}
```

## Generating Rust Stubs

The IDL compiler produces two files per `.proto`:

| File | Contents |
|------|----------|
| `<name>.subspace.rpc_client.rs` | Typed client struct with async methods |
| `<name>.subspace.rpc_server.rs` | Handler trait + server struct |

The generated code uses `subspace_rpc::` import paths. When including the files
inside the `subspace-rpc` crate itself (e.g., for testing), add
`extern crate self as subspace_rpc;` to your `lib.rs`.

### Via Bazel

Add to your `BUILD.bazel`:

```python
load("//:rpc/subspace_rpc_library.bzl", "subspace_rpc_rust_library")

subspace_rpc_rust_library(
    name = "greeter_rust_rpc",
    deps = [":greeter_proto"],  # your proto_library target
)
```

This produces three targets:

| Target | Description |
|--------|-------------|
| `:greeter_rust_rpc_files` | All generated `.rs` files |
| `:greeter_rust_rpc_client` | Client stubs only |
| `:greeter_rust_rpc_server` | Server stubs only |

Build with:

```bash
bazel build //path/to:greeter_rust_rpc_files
```

### Via Cargo `build.rs`

If you want `cargo build` to generate stubs automatically, invoke protoc with
the plugin in your `build.rs`:

```rust
use std::path::{Path, PathBuf};

fn generate_service_stubs(out_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let plugin = std::env::var("SUBSPACE_RPC_PLUGIN")
        .unwrap_or_else(|_| "../bazel-bin/rpc/idl_compiler/subspace_rpc".into());

    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".into());

    let status = std::process::Command::new(&protoc)
        .arg(format!("--plugin=protoc-gen-subspace_rpc={}", plugin))
        .arg(format!("--subspace_rpc_out=rpc_style=rust:{}", out_dir))
        .arg("-Ipath/to/protos")
        .arg("greeter.proto")
        .status()?;

    if !status.success() {
        return Err("protoc subspace_rpc plugin failed".into());
    }
    Ok(())
}
```

The plugin binary must be built first:

```bash
bazel build //rpc/idl_compiler:subspace_rpc
```

Or point `SUBSPACE_RPC_PLUGIN` to its location.

Then include the generated files in your crate:

```rust
extern crate self as subspace_rpc;

pub mod my_service {
    pub use crate::my_proto_types::*;  // re-export proto message types

    #[allow(unused_imports, dead_code)]
    pub mod greeter_client {
        include!(concat!(env!("OUT_DIR"), "/greeter.subspace.rpc_client.rs"));
    }
    #[allow(unused_imports, dead_code)]
    pub mod greeter_server {
        include!(concat!(env!("OUT_DIR"), "/greeter.subspace.rpc_server.rs"));
    }
}
```

The parent module must re-export the proto message types (`pub use ...::*`) so
that the generated `use super::*` resolves the request/response types.

## Implementing a Server

### 1. Implement the handler trait

The generated server file defines a `<Service>Handler` trait with one async
method per RPC:

```rust
use std::sync::Arc;
use subspace_rpc::error::Result;
use subspace_rpc::stream::TypedStreamWriter;

use my_crate::my_service::greeter_server::{GreeterHandler, GreeterServer};
use my_crate::my_proto_types::*;

struct MyHandler;

#[async_trait::async_trait]
impl GreeterHandler for MyHandler {
    // Unary method: receive a request, return a response.
    async fn say_hello(&self, request: GreetRequest) -> Result<GreetResponse> {
        Ok(GreetResponse {
            greeting: format!("Hello, {}!", request.name),
        })
    }

    // Server-streaming method: receive a request, write multiple responses.
    async fn subscribe(
        &self,
        request: EventRequest,
        writer: TypedStreamWriter<Event>,
    ) -> Result<()> {
        for i in 0..request.count {
            let event = Event {
                data: format!("event {}", i),
                seq: i,
            };
            // write() returns false if the client cancelled the stream.
            if !writer.write(&event).await? {
                return Ok(());
            }
        }
        // Signal end-of-stream.
        writer.finish().await?;
        Ok(())
    }
}
```

### 2. Start the server

```rust
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<()> {
    let handler = Arc::new(MyHandler);
    let server = GreeterServer::new("/tmp/subspace.sock", handler);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Optionally wire up signal handling to send `true` to shutdown_tx.

    server.run(shutdown_rx).await?;
    Ok(())
}
```

The server blocks on `run()` until the shutdown channel receives `true`. Each
client session spawns its own set of tokio tasks for method dispatch.

## Using the Client

### 1. Create and connect

```rust
use std::time::Duration;
use my_crate::my_service::greeter_client::GreeterClient;
use my_crate::my_proto_types::*;

let mut client = GreeterClient::create(/*client_id=*/ 1, "/tmp/subspace.sock")
    .await
    .expect("failed to connect");

// Or with a connection timeout:
let mut client = GreeterClient::create_with_timeout(
    1,
    "/tmp/subspace.sock",
    Duration::from_secs(5),
).await?;
```

Each client must have a unique `client_id` (a `u64`). The `create` call opens
a session with the server, which sets up per-method shared-memory channels.

### 2. Unary calls

Every unary method takes a reference to the request and an optional timeout:

```rust
let request = GreetRequest { name: "Alice".into() };
let response = client.say_hello(&request, Some(Duration::from_secs(5))).await?;
println!("{}", response.greeting);  // "Hello, Alice!"
```

Pass `None` for no timeout (blocks until the server responds).

### 3. Server-streaming calls

Streaming methods return a `ResponseStream<T>` which you consume with `.next()`:

```rust
let request = EventRequest { count: 10 };
let mut stream = client
    .subscribe(&request, Some(Duration::from_secs(30)))
    .await?;

while let Some(result) = stream.next().await {
    let event = result?;
    println!("seq={} data={}", event.seq, event.data);
}
// Stream is exhausted when next() returns None.
```

To cancel a stream early:

```rust
stream.cancel()?;
```

### 4. Close the client

```rust
client.close().await?;

// Or with a timeout:
client.close_with_timeout(Duration::from_secs(2)).await?;
```

Dropping a client without calling `close()` will log a warning.

## Error Handling

All fallible operations return `subspace_rpc::error::Result<T>`, which is
`std::result::Result<T, RpcError>`.

| Variant | Meaning |
|---------|---------|
| `RpcError::Subspace(e)` | Underlying Subspace IPC error |
| `RpcError::SessionNotOpen` | Attempted an operation before `open()` / `create()` |
| `RpcError::SessionAlreadyOpen` | Called `open()` twice |
| `RpcError::MethodNotFound(msg)` | Unknown method ID |
| `RpcError::Timeout(msg)` | Poll-level timeout |
| `RpcError::DeadlineExceeded(msg)` | Application-level deadline exceeded |
| `RpcError::Cancelled` | Stream cancelled by client |
| `RpcError::ServerError(msg)` | Server returned an error string |
| `RpcError::Internal(msg)` | Internal RPC framework error |
| `RpcError::Encode(e)` | Protobuf encoding failure |
| `RpcError::Decode(e)` | Protobuf decoding failure |
| `RpcError::Io(e)` | OS-level I/O error |
| `RpcError::InvalidResponse(msg)` | Malformed response from server |

Convenience methods:

```rust
if err.is_timeout() { /* DeadlineExceeded or Timeout */ }
if err.is_cancelled() { /* stream was cancelled */ }
```

## Dependencies

Add to your `Cargo.toml`:

```toml
[dependencies]
subspace-rpc = { path = "path/to/rust_rpc" }
tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros", "sync", "time"] }
prost = "0.13"
prost-types = "0.13"
async-trait = "0.1"

[build-dependencies]
prost-build = "0.13"
```

## Complete Example

See `rust_rpc/tests/rpc_test.rs` for a working end-to-end test covering unary
calls, server streaming, error propagation, and timeouts.
