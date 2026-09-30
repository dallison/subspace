"""Build setting that sizes the shared-memory channel table.

A downstream module sets the limit with the `subspace` module extension.  See
docs/max-channels.md.  `--//:max_channels` overrides that value when it is not
left at the default of 1024.  The value is compiled into the system control
block, so the server and every client, including Rust, must be built with the
same number.  It must be a positive multiple of 64 because channel ids are
stored in a bitset of 64-bit words.
"""

load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")
load("@subspace_max_channels//:max_channels.bzl", "MAX_CHANNELS")

# Matches build_setting_default on //:max_channels.  Leaving the flag at this
# value means "use the module extension", which is also 1024 unless the root
# module calls subspace.max_channels.
_FLAG_DEFAULT = "1024"

def _parse_max_channels(raw):
    if not raw.isdigit() or raw.startswith("0"):
        fail("--//:max_channels must be a positive multiple of 64, got '%s'" % raw)
    count = int(raw)
    if count <= 0 or count % 64 != 0:
        fail("--//:max_channels must be a positive multiple of 64, got '%s'" % raw)
    return count

def _resolve_max_channels(flag_value):
    if flag_value != _FLAG_DEFAULT:
        return _parse_max_channels(flag_value)
    if type(MAX_CHANNELS) != "int" or MAX_CHANNELS <= 0 or MAX_CHANNELS % 64 != 0:
        fail("subspace.max_channels count must be a positive multiple of 64, got %s" % MAX_CHANNELS)
    return MAX_CHANNELS

def _max_channels_setting_impl(ctx):
    count = _resolve_max_channels(ctx.attr._max_channels[BuildSettingInfo].value)
    value_file = ctx.actions.declare_file(ctx.attr.name + ".txt")
    ctx.actions.write(output = value_file, content = "%d\n" % count)
    return [
        DefaultInfo(files = depset([value_file])),
        CcInfo(
            compilation_context = cc_common.create_compilation_context(
                defines = depset(["SUBSPACE_MAX_CHANNELS=%d" % count]),
            ),
        ),
    ]

max_channels_setting = rule(
    implementation = _max_channels_setting_impl,
    attrs = {
        "_max_channels": attr.label(default = "//:max_channels"),
    },
    doc = "Propagates the configured channel limit as a C++ define and a text file for Rust.",
)
