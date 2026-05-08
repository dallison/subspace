# QNX PMEM Support

Subspace supports QNX as a target platform. Default QNX builds use the same
POSIX-shadow shared-memory path used by other non-Linux platforms. PMEM-backed
message buffers are available as an optional build-time and per-channel feature.

PMEM support is provided by General Motors.

## Build-Time Enablement

PMEM code is compiled only when explicitly enabled. This keeps default QNX
builds on the POSIX shared-memory path.

With Bazel:

```bash
bazel build --config=qnx_pmem //server:subspace_server //client:subspace_client
```

With CMake:

```bash
cmake -DSUBSPACE_QNX_PMEM=ON ..
```

## Runtime Channel Options

PMEM is also opt-in per channel. Publishers request PMEM-backed buffers with:

```cpp
auto opts = subspace::PublisherOptions()
    .SetSlotSize(4096)
    .SetNumSlots(16)
    .SetUseQnxPmem(true)
    .SetPmemAlignment(4096)
    .SetPmemPoolId("camera")
    .SetPmemCacheEnabled(true);
```

Subscribers can require compatible PMEM channel settings:

```cpp
auto opts = subspace::SubscriberOptions()
    .SetUseQnxPmem(true)
    .SetPmemAlignment(4096)
    .SetPmemPoolId("camera")
    .SetPmemCacheEnabled(true);
```

If a channel already exists, the server validates that later publishers and
subscribers request compatible PMEM settings.

The C API exposes the same settings through `SubspacePublisherOptions` and
`SubspaceSubscriberOptions` when PMEM support is compiled in.

## How It Works

For PMEM channels, the publisher still allocates message buffers client-side.
The publisher creates PMEM objects and writes a shadow metadata file in `/tmp`
using a name derived from the channel and buffer. Subscribers read the metadata
to map the same PMEM objects.

The publisher also sends a fire-and-forget registration request to the server.
The server records enough metadata to destroy PMEM objects when the channel is
removed or when session cleanup runs. If shadow servers are configured, this
metadata is replicated to them and restored to the server during crash recovery
so recovered channels keep their PMEM cleanup records. Message transfer remains
direct between clients through shared memory; the server is not on the message
data path.

## Linux PMEM Shim

The `linux_pmem_shim` Bazel config enables the QNX PMEM transport code on Linux
while backing the allocations with Linux shared memory. It is intended for
development and CI testing when QNX hardware or a QNX runtime is unavailable:

```bash
bazel test --config=linux_pmem_shim //common:pmem_test //client:client_test //server:server_test
```

The shim is a test facility. It should not be used as a production replacement
for QNX PMEM.
