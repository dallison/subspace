# Setting the BCB buffer-table limit from another Bazel build

Use this when Subspace is a `bazel_dep` and a channel may need more distinct
shared-memory payload segments than the default allows (for example many slot-size
resizes). The setting applies to every Subspace target in that build. It sizes
the Buffer Control Block (BCB) in shared memory, so those targets must share
one value.

Each segment is tracked with a ref count and size in the BCB. The live count is
`ccb_->num_buffers`; it cannot exceed `kMaxBuffers` compiled into the build.

The value must be a positive integer. The default is **341**, chosen so
`sizeof(BufferControlBlock)` is **4096 bytes** (one 4 KiB page) on typical
platforms with 4-byte `std::atomic<int32_t>` and 8-byte `std::atomic<uint64_t>`
entries.

## Set it with CMake

The root `CMakeLists.txt` exposes `SUBSPACE_MAX_BUFFERS` (default `341`). It
adds `-DSUBSPACE_MAX_BUFFERS=N` for all C++ targets and passes the same value
into the Rust client build.

```bash
cmake -B build \
  -DSUBSPACE_MAX_BUFFERS=512 \
  -DSUBSPACE_MAX_CHANNELS=1024 \
  -DSUBSPACE_MAX_SLOT_OWNERS=1024
cmake --build build
```

Use the same values for the server and every client.

## Set it in MODULE.bazel

Add this next to the `bazel_dep` in the root `MODULE.bazel`:

```python
bazel_dep(name = "subspace", version = "3.2.9")

subspace = use_extension("@subspace//:extensions.bzl", "subspace")
subspace.max_buffers(count = 512)
```

Call `subspace.max_buffers` at most once. Only the root module can set it.

## Check the result

```bash
bazel aquery 'mnemonic(CppCompile, //your:target)' | grep SUBSPACE_MAX_BUFFERS
```

The compile lines contain `-DSUBSPACE_MAX_BUFFERS=512`. The server binary and
the clients must show the same number.

## Override for one build

`--@subspace//:max_buffers=N` overrides the `MODULE.bazel` value when `N` is not
`341`. Leaving the flag unset, or leaving it at `341`, uses the
`subspace.max_buffers` count. `--//:max_buffers` is the flag for a build whose
root is the Subspace repo itself.
