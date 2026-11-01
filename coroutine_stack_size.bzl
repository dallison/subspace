"""Build setting for the server coroutine stack size.

A downstream module sets the size with the `subspace` module extension.  See
docs/coroutine-stack-size.md.  `--//:coroutine_stack_size` overrides that
value when it is not left at the default of 65536.  The size is the stack of
each coroutine the server and shadow process spawn.  It is not part of the
shared-memory layout, so clients do not need to match it.  It must be a
positive multiple of 4096.
"""

load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")
load("@subspace_max_channels//:coroutine_stack_size.bzl", "COROUTINE_STACK_SIZE")

# Matches build_setting_default on //:coroutine_stack_size.  Leaving the flag
# at this value means "use the module extension", which is also 65536 unless
# the root module calls subspace.coroutine_stack_size.
_FLAG_DEFAULT = "65536"

def _parse_coroutine_stack_size(raw):
    if not raw.isdigit() or raw.startswith("0"):
        fail("--//:coroutine_stack_size must be a positive multiple of 4096, got '%s'" % raw)
    size = int(raw)
    if size <= 0 or size % 4096 != 0:
        fail("--//:coroutine_stack_size must be a positive multiple of 4096, got '%s'" % raw)
    return size

def _resolve_coroutine_stack_size(flag_value):
    if flag_value != _FLAG_DEFAULT:
        return _parse_coroutine_stack_size(flag_value)
    if type(COROUTINE_STACK_SIZE) != "int" or COROUTINE_STACK_SIZE <= 0 or COROUTINE_STACK_SIZE % 4096 != 0:
        fail("subspace.coroutine_stack_size bytes must be a positive multiple of 4096, got %s" % COROUTINE_STACK_SIZE)
    return COROUTINE_STACK_SIZE

def _coroutine_stack_size_setting_impl(ctx):
    size = _resolve_coroutine_stack_size(ctx.attr._coroutine_stack_size[BuildSettingInfo].value)
    value_file = ctx.actions.declare_file(ctx.attr.name + ".txt")
    ctx.actions.write(output = value_file, content = "%d\n" % size)
    return [
        DefaultInfo(files = depset([value_file])),
        CcInfo(
            compilation_context = cc_common.create_compilation_context(
                defines = depset(["SUBSPACE_COROUTINE_STACK_SIZE=%d" % size]),
            ),
        ),
    ]

coroutine_stack_size_setting = rule(
    implementation = _coroutine_stack_size_setting_impl,
    attrs = {
        "_coroutine_stack_size": attr.label(default = "//:coroutine_stack_size"),
    },
    doc = "Propagates the configured server coroutine stack size as a C++ define.",
)
