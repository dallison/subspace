# Setting the channel limit from another Bazel build

Use this when Subspace is a `bazel_dep` and the downstream build needs more
than the default 1024 channels. The limit is `--@subspace//:max_channels`. It
is compiled into the shared-memory system control block, so the server and
every client in that build must get the same value.

The value is a string of digits. It must be a positive multiple of 64.
`1024` is the default. `1000` fails at analysis time.

`--//:max_channels` is the flag for a build whose root is the Subspace repo
itself. A downstream repo that writes `--//:max_channels` is setting a
different flag, in its own root package.

## Set it for every target

Put this in the downstream repo's `.bazelrc`:

```
build --@subspace//:max_channels=4096
```

That one line compiles every Subspace target the build uses:

| Target | What it is |
|---|---|
| `@subspace//server:subspace_server` | Server binary |
| `@subspace//server:server` | Server library |
| `@subspace//client:subspace_client` | C++ client |
| `@subspace//c_client:subspace_c_client` | C client |
| `@subspace//rust_client:subspace_client_rust` | Rust client |
| `@subspace//plugins:nop_plugin.so` | Plugin shared libraries |

Users of that repo then build normally. They do not pass the flag themselves.

The label uses the `bazel_dep` repo name. This `MODULE.bazel` entry:

```python
bazel_dep(name = "subspace", version = "3.2.5")
```

makes the flag `@subspace//:max_channels`. A `repo_name = "something_else"`
argument changes the label to `@something_else//:max_channels`.

## Set it from a BUILD file

A `BUILD` file can pin the flag onto one dependency. Do this only when the
downstream repo cannot put the flag in `.bazelrc`. Every Subspace target that
the build links or runs needs its own wrapper, and every wrapper must pass
the same `max_channels` string. A target left as a direct `@subspace//...`
dependency stays at 1024.

`subspace_channels.bzl`:

```python
def _set_max_channels_impl(settings, attr):
    return {"@subspace//:max_channels": attr.max_channels}

_set_max_channels = transition(
    implementation = _set_max_channels_impl,
    inputs = [],
    outputs = ["@subspace//:max_channels"],
)

def _apply_impl(ctx):
    dep = ctx.attr.dep[0]
    providers = [dep[DefaultInfo]]
    if CcInfo in dep:
        providers.append(dep[CcInfo])
    return providers

apply_max_channels = rule(
    implementation = _apply_impl,
    attrs = {
        "dep": attr.label(cfg = _set_max_channels, mandatory = True),
        "max_channels": attr.string(mandatory = True),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
```

`BUILD`:

```python
load(":subspace_channels.bzl", "apply_max_channels")

apply_max_channels(
    name = "subspace_server",
    dep = "@subspace//server:subspace_server",
    max_channels = "4096",
)

apply_max_channels(
    name = "subspace_client",
    dep = "@subspace//client:subspace_client",
    max_channels = "4096",
)
```

Depend on those wrappers. Use `@subspace//server:server` instead of
`@subspace//server:subspace_server` when the server is linked as a library.
Add the same wrapper for `@subspace//c_client:subspace_c_client` and
`@subspace//rust_client:subspace_client_rust` when the build uses them. The
Rust client also needs its Rust providers forwarded; the `.bazelrc` setting
above is the one that covers it without another wrapper.

## Check the result

Build any downstream target that links Subspace, then confirm the compiled
define. For a C++ target `//your:target` with the limit set to 4096:

```bash
bazel aquery 'mnemonic(CppCompile, //your:target)' | grep SUBSPACE_MAX_CHANNELS
```

The compile lines contain `-DSUBSPACE_MAX_CHANNELS=4096`. The server binary
and the clients must show the same number. A mismatch is a different
shared-memory layout, and those processes cannot attach to each other.
