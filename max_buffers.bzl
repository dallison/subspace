"""Build setting that sizes the shared-memory Buffer Control Block (BCB).

A downstream module sets the limit with the `subspace` module extension.  See
docs/max-buffers.md.  `--//:max_buffers` overrides that value when it is not
left at the default of 341.  The value is compiled into the BCB layout, so the
server and every client, including Rust, must be built with the same number.
It must be a positive integer.
"""

load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")
load("@subspace_max_channels//:max_buffers.bzl", "MAX_BUFFERS")

_FLAG_DEFAULT = "341"

def _parse_max_buffers(raw):
    if not raw.isdigit() or raw.startswith("0"):
        fail("--//:max_buffers must be a positive integer, got '%s'" % raw)
    count = int(raw)
    if count <= 0:
        fail("--//:max_buffers must be a positive integer, got '%s'" % raw)
    return count

def _resolve_max_buffers(flag_value):
    if flag_value != _FLAG_DEFAULT:
        return _parse_max_buffers(flag_value)
    if type(MAX_BUFFERS) != "int" or MAX_BUFFERS <= 0:
        fail("subspace.max_buffers count must be a positive integer, got %s" % MAX_BUFFERS)
    return MAX_BUFFERS

def _max_buffers_setting_impl(ctx):
    count = _resolve_max_buffers(ctx.attr._max_buffers[BuildSettingInfo].value)
    value_file = ctx.actions.declare_file(ctx.attr.name + ".txt")
    ctx.actions.write(output = value_file, content = "%d\n" % count)
    return [
        DefaultInfo(files = depset([value_file])),
        CcInfo(
            compilation_context = cc_common.create_compilation_context(
                defines = depset(["SUBSPACE_MAX_BUFFERS=%d" % count]),
            ),
        ),
    ]

max_buffers_setting = rule(
    implementation = _max_buffers_setting_impl,
    attrs = {
        "_max_buffers": attr.label(default = "//:max_buffers"),
    },
    doc = "Propagates the configured BCB buffer-table limit as a C++ define and a text file for Rust.",
)
