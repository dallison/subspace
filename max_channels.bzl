"""Build setting that sizes the shared-memory channel table.

`--//:max_channels` selects how many channels one server session can hold.
The value is compiled into the system control block, so the server and every
client, including Rust, must be built with the same number.  It must be a
positive multiple of 64 because channel ids are stored in a bitset of 64-bit
words.
"""

load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

def _parse_max_channels(raw):
    if not raw.isdigit() or raw.startswith("0"):
        fail("--//:max_channels must be a positive multiple of 64, got '%s'" % raw)
    count = int(raw)
    if count <= 0 or count % 64 != 0:
        fail("--//:max_channels must be a positive multiple of 64, got '%s'" % raw)
    return count

def _max_channels_setting_impl(ctx):
    count = _parse_max_channels(ctx.attr._max_channels[BuildSettingInfo].value)
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
    doc = "Propagates --//:max_channels as a C++ define and a text file for Rust.",
)
