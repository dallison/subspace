# Setting the channel limit from another Bazel build

Use this when Subspace is a `bazel_dep` and the downstream build needs more
than the default 1024 channels. The setting applies to every Subspace target
in that build: the server binary, the server library, and the C++, C, and
Rust clients. It sizes the shared-memory system control block, so those
targets must share one value.

The value must be a positive multiple of 64. `1024` is the default. `1000`
fails when the module is resolved.

## Set it in MODULE.bazel

Add this next to the `bazel_dep` in the root `MODULE.bazel`:

```python
bazel_dep(name = "subspace", version = "3.2.5")

subspace = use_extension("@subspace//:extensions.bzl", "subspace")
subspace.max_channels(count = 8192)
```

No extra `.bzl` file, no wrapper per target, and no build-line flag. Call
`subspace.max_channels` once. Only the root module can set it.

If `bazel_dep` uses `repo_name`, the extension label follows that name. With
`repo_name = "subspace_ipc"` the load is
`use_extension("@subspace_ipc//:extensions.bzl", "subspace")`.

## Check the result

Build any downstream target that links Subspace, then confirm the compiled
define. For a C++ target `//your:target` with the limit set to 8192:

```bash
bazel aquery 'mnemonic(CppCompile, //your:target)' | grep SUBSPACE_MAX_CHANNELS
```

The compile lines contain `-DSUBSPACE_MAX_CHANNELS=8192`. The server binary
and the clients must show the same number. A mismatch is a different
shared-memory layout, and those processes cannot attach to each other.

## Override for one build

`--@subspace//:max_channels=N` overrides the `MODULE.bazel` value when `N` is
not `1024`. Leaving the flag unset, or leaving it at `1024`, uses the
`subspace.max_channels` count. `--//:max_channels` is the flag for a build
whose root is the Subspace repo itself.
