# Setting the slot-owner limit from another Bazel build

Use this when Subspace is a `bazel_dep` and the downstream build needs more
than the default 1024 publisher/subscriber slots **per channel**. The setting
applies to every Subspace target in that build: the server binary, the server
library, and the C++, C, and Rust clients. It sizes fields in the channel
control block (CCB) in shared memory, so those targets must share one value.

The value must be a positive integer. `1024` is the default. Unlike the
session channel limit, it does not need to be a multiple of 64.

## Set it with CMake

The root `CMakeLists.txt` exposes a cache variable `SUBSPACE_MAX_SLOT_OWNERS`
(default `1024`). It adds `-DSUBSPACE_MAX_SLOT_OWNERS=N` for all C++ targets
and passes the same value into the Rust client build.

Configure once for the whole tree (set `SUBSPACE_MAX_CHANNELS` here too if you
change the session channel limit):

```bash
cmake -B build \
  -DSUBSPACE_MAX_SLOT_OWNERS=2048 \
  -DSUBSPACE_MAX_CHANNELS=1024
cmake --build build
```

Use the same values for the server and every client. A mismatch changes the
CCB layout in shared memory.

## Set it in MODULE.bazel

Add this next to the `bazel_dep` in the root `MODULE.bazel` (alongside
`subspace.max_channels` if you raise the session channel limit too):

```python
bazel_dep(name = "subspace", version = "3.2.9")

subspace = use_extension("@subspace//:extensions.bzl", "subspace")
subspace.max_slot_owners(count = 2048)
```

Call `subspace.max_slot_owners` at most once. Only the root module can set it.

## Check the result

Build any downstream target that links Subspace, then confirm the compiled
define. For a C++ target `//your:target` with the limit set to 2048:

```bash
bazel aquery 'mnemonic(CppCompile, //your:target)' | grep SUBSPACE_MAX_SLOT_OWNERS
```

The compile lines contain `-DSUBSPACE_MAX_SLOT_OWNERS=2048`. The server binary
and the clients must show the same number. A mismatch is a different
shared-memory layout, and those processes cannot attach to each other.

## Override for one build

`--@subspace//:max_slot_owners=N` overrides the `MODULE.bazel` value when `N`
is not `1024`. Leaving the flag unset, or leaving it at `1024`, uses the
`subspace.max_slot_owners` count. `--//:max_slot_owners` is the flag for a
build whose root is the Subspace repo itself.
