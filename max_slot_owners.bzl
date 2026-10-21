"""Build setting that sizes per-channel publisher/subscriber tables in the CCB.

A downstream module sets the limit with the `subspace` module extension.  See
docs/max-slot-owners.md.  `--//:max_slot_owners` overrides that value when it
is not left at the default of 1024.  The value is compiled into the channel
control block layout, so the server and every client, including Rust, must be
built with the same number.  It must be a positive integer.
"""

load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")
load("@subspace_max_channels//:max_slot_owners.bzl", "MAX_SLOT_OWNERS")

_FLAG_DEFAULT = "1024"

def _parse_max_slot_owners(raw):
    if not raw.isdigit() or raw.startswith("0"):
        fail("--//:max_slot_owners must be a positive integer, got '%s'" % raw)
    count = int(raw)
    if count <= 0:
        fail("--//:max_slot_owners must be a positive integer, got '%s'" % raw)
    return count

def _resolve_max_slot_owners(flag_value):
    if flag_value != _FLAG_DEFAULT:
        return _parse_max_slot_owners(flag_value)
    if type(MAX_SLOT_OWNERS) != "int" or MAX_SLOT_OWNERS <= 0:
        fail("subspace.max_slot_owners count must be a positive integer, got %s" % MAX_SLOT_OWNERS)
    return MAX_SLOT_OWNERS

def _max_slot_owners_setting_impl(ctx):
    count = _resolve_max_slot_owners(ctx.attr._max_slot_owners[BuildSettingInfo].value)
    value_file = ctx.actions.declare_file(ctx.attr.name + ".txt")
    ctx.actions.write(output = value_file, content = "%d\n" % count)
    return [
        DefaultInfo(files = depset([value_file])),
        CcInfo(
            compilation_context = cc_common.create_compilation_context(
                defines = depset(["SUBSPACE_MAX_SLOT_OWNERS=%d" % count]),
            ),
        ),
    ]

max_slot_owners_setting = rule(
    implementation = _max_slot_owners_setting_impl,
    attrs = {
        "_max_slot_owners": attr.label(default = "//:max_slot_owners"),
    },
    doc = "Propagates the configured slot-owner limit as a C++ define and a text file for Rust.",
)
