# Setting the server coroutine stack size

Use this when a downstream build wants a different stack for the coroutines
the Subspace server and the shadow process spawn. The default is 64 KiB, the
same size the co library uses when a stack size is not specified. Each client
connection is one coroutine, so the total is one stack per connection.

The size is ordinary process memory. It does not change shared memory, and
clients do not need to be built with the same value.

The value must be a positive multiple of 4096. `65536` is the default.

AddressSanitizer builds use at least 256 KiB. On macOS, ASan's `strerror`
interceptor takes about 56 KiB of stack, and a 64 KiB coroutine that formats
`errno` overruns that stack. A configured size larger than 256 KiB is kept.

## Set it in MODULE.bazel

Add this next to the `bazel_dep` in the root `MODULE.bazel`:

```python
bazel_dep(name = "subspace", version = "3.3.0")

subspace = use_extension("@subspace//:extensions.bzl", "subspace")
subspace.coroutine_stack_size(bytes = 16384)
```

Call `subspace.coroutine_stack_size` once. Only the root module can set it.

## Set it with CMake

```bash
cmake -B build -DSUBSPACE_COROUTINE_STACK_SIZE=16384
cmake --build build
```

## Check the result

```bash
bazel aquery 'mnemonic(CppCompile, //server:subspace_server)' | grep SUBSPACE_COROUTINE_STACK_SIZE
```

The server compile lines contain `-DSUBSPACE_COROUTINE_STACK_SIZE=16384`.

## Override for one build

`--@subspace//:coroutine_stack_size=N` overrides the `MODULE.bazel` value when
`N` is not `65536`. Leaving the flag unset, or leaving it at `65536`, uses the
`subspace.coroutine_stack_size` count. `--//:coroutine_stack_size` is the flag
for a build whose root is the Subspace repo itself.
