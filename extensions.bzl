"""Module extension for configuring Subspace from a downstream Bazel module.

The root module sets the channel limit next to its bazel_dep:

    subspace = use_extension("@subspace//:extensions.bzl", "subspace")
    subspace.max_channels(count = 8192)

That value sizes the shared-memory system control block for every Subspace
target in the build.  It must be a positive multiple of 64.
"""

def _subspace_max_channels_repo_impl(repository_ctx):
    repository_ctx.file(
        "max_channels.bzl",
        "MAX_CHANNELS = %d\n" % repository_ctx.attr.count,
    )
    repository_ctx.file(
        "BUILD.bazel",
        "exports_files([\"max_channels.bzl\"])\n",
    )

_subspace_max_channels_repo = repository_rule(
    implementation = _subspace_max_channels_repo_impl,
    attrs = {
        "count": attr.int(mandatory = True),
    },
)

def _validate_count(count):
    if count <= 0 or count % 64 != 0:
        fail("subspace.max_channels count must be a positive multiple of 64, got %s" % count)

def _subspace_impl(module_ctx):
    count = 1024
    for module in module_ctx.modules:
        tags = module.tags.max_channels
        if not tags:
            continue
        if not module.is_root:
            fail("Only the root module can set subspace.max_channels. Add it to the root MODULE.bazel next to bazel_dep(name = \"subspace\").")
        if len(tags) != 1:
            fail("Call subspace.max_channels once in the root MODULE.bazel")
        count = tags[0].count
        _validate_count(count)
    _subspace_max_channels_repo(
        name = "subspace_max_channels",
        count = count,
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
    },
    doc = "Configures Subspace for the root module. See docs/max-channels.md.",
)
