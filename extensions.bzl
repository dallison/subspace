"""Module extension for configuring Subspace from a downstream Bazel module.

The root module sets build limits next to its bazel_dep:

    subspace = use_extension("@subspace//:extensions.bzl", "subspace")
    subspace.max_channels(count = 8192)
    subspace.max_slot_owners(count = 2048)
    subspace.max_buffers(count = 512)
    subspace.coroutine_stack_size(bytes = 16384)

max_channels, max_slot_owners, and max_buffers size shared-memory control
blocks.  max_channels must be a positive multiple of 64; max_slot_owners and
max_buffers must be positive integers.  coroutine_stack_size is the stack of
each server and shadow coroutine, in bytes, and must be a positive multiple
of 4096.  It is not part of the shared-memory layout.
"""

def _subspace_config_repo_impl(repository_ctx):
    repository_ctx.file(
        "max_channels.bzl",
        "MAX_CHANNELS = %d\n" % repository_ctx.attr.max_channels,
    )
    repository_ctx.file(
        "max_slot_owners.bzl",
        "MAX_SLOT_OWNERS = %d\n" % repository_ctx.attr.max_slot_owners,
    )
    repository_ctx.file(
        "max_buffers.bzl",
        "MAX_BUFFERS = %d\n" % repository_ctx.attr.max_buffers,
    )
    repository_ctx.file(
        "coroutine_stack_size.bzl",
        "COROUTINE_STACK_SIZE = %d\n" % repository_ctx.attr.coroutine_stack_size,
    )
    repository_ctx.file(
        "BUILD.bazel",
        "exports_files([\"max_channels.bzl\", \"max_slot_owners.bzl\", \"max_buffers.bzl\", \"coroutine_stack_size.bzl\"])\n",
    )

_subspace_config_repo = repository_rule(
    implementation = _subspace_config_repo_impl,
    attrs = {
        "max_channels": attr.int(mandatory = True),
        "max_slot_owners": attr.int(mandatory = True),
        "max_buffers": attr.int(mandatory = True),
        "coroutine_stack_size": attr.int(mandatory = True),
    },
)

def _validate_multiple_of_64(count, label):
    if count <= 0 or count % 64 != 0:
        fail("%s count must be a positive multiple of 64, got %s" % (label, count))

def _validate_positive(count, label):
    if count <= 0:
        fail("%s count must be a positive integer, got %s" % (label, count))

def _validate_stack_size(size):
    if size <= 0 or size % 4096 != 0:
        fail("subspace.coroutine_stack_size bytes must be a positive multiple of 4096, got %s" % size)

def _subspace_impl(module_ctx):
    max_channels = 1024
    max_slot_owners = 1024
    max_buffers = 341
    coroutine_stack_size = 65536
    for module in module_ctx.modules:
        channel_tags = module.tags.max_channels
        if channel_tags:
            if not module.is_root:
                fail("Only the root module can set subspace.max_channels. Add it to the root MODULE.bazel next to bazel_dep(name = \"subspace\").")
            if len(channel_tags) != 1:
                fail("Call subspace.max_channels once in the root MODULE.bazel")
            max_channels = channel_tags[0].count
            _validate_multiple_of_64(max_channels, "subspace.max_channels")
        owner_tags = module.tags.max_slot_owners
        if owner_tags:
            if not module.is_root:
                fail("Only the root module can set subspace.max_slot_owners. Add it to the root MODULE.bazel next to bazel_dep(name = \"subspace\").")
            if len(owner_tags) != 1:
                fail("Call subspace.max_slot_owners once in the root MODULE.bazel")
            max_slot_owners = owner_tags[0].count
            _validate_positive(max_slot_owners, "subspace.max_slot_owners")
        buffer_tags = module.tags.max_buffers
        if buffer_tags:
            if not module.is_root:
                fail("Only the root module can set subspace.max_buffers. Add it to the root MODULE.bazel next to bazel_dep(name = \"subspace\").")
            if len(buffer_tags) != 1:
                fail("Call subspace.max_buffers once in the root MODULE.bazel")
            max_buffers = buffer_tags[0].count
            _validate_positive(max_buffers, "subspace.max_buffers")
        stack_tags = module.tags.coroutine_stack_size
        if stack_tags:
            if not module.is_root:
                fail("Only the root module can set subspace.coroutine_stack_size. Add it to the root MODULE.bazel next to bazel_dep(name = \"subspace\").")
            if len(stack_tags) != 1:
                fail("Call subspace.coroutine_stack_size once in the root MODULE.bazel")
            coroutine_stack_size = stack_tags[0].bytes
            _validate_stack_size(coroutine_stack_size)
    _subspace_config_repo(
        name = "subspace_max_channels",
        max_channels = max_channels,
        max_slot_owners = max_slot_owners,
        max_buffers = max_buffers,
        coroutine_stack_size = coroutine_stack_size,
    )
    return module_ctx.extension_metadata(reproducible = True)

subspace = module_extension(
    implementation = _subspace_impl,
    tag_classes = {
        "max_channels": tag_class(
            attrs = {
                "count": attr.int(
                    mandatory = True,
                    doc = "Maximum channels in one server session. Positive multiple of 64. Default is 1024.",
                ),
            },
            doc = "Set the maximum number of channels compiled into Subspace.",
        ),
        "max_slot_owners": tag_class(
            attrs = {
                "count": attr.int(
                    mandatory = True,
                    doc = "Maximum publishers plus subscribers per channel. Positive integer. Default is 1024.",
                ),
            },
            doc = "Set kMaxSlotOwners compiled into Subspace. See docs/max-slot-owners.md.",
        ),
        "max_buffers": tag_class(
            attrs = {
                "count": attr.int(
                    mandatory = True,
                    doc = "Maximum distinct payload buffer segments per channel (BCB table size). Positive integer. Default is 341 (4 KiB BCB).",
                ),
            },
            doc = "Set kMaxBuffers compiled into Subspace. See docs/max-buffers.md.",
        ),
        "coroutine_stack_size": tag_class(
            attrs = {
                "bytes": attr.int(
                    mandatory = True,
                    doc = "Stack size in bytes for each server and shadow coroutine. Positive multiple of 4096. Default is 65536.",
                ),
            },
            doc = "Set the server coroutine stack size. See docs/coroutine-stack-size.md.",
        ),
    },
    doc = "Configures Subspace for the root module. See docs/max-channels.md, docs/max-slot-owners.md, docs/max-buffers.md, and docs/coroutine-stack-size.md.",
)
