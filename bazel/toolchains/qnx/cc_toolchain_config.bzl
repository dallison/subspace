# Skeleton cc_toolchain_config for QNX cross compilation with qcc/q++.
#
# This file exists so that //bazel/toolchains/qnx:BUILD.bazel can load it and
# declare the toolchain config targets it needs.  The current implementation
# returns a minimal CcToolchainConfigInfo that is enough for Bazel to load and
# analyse the targets, but compile/link actions will not yet succeed because
# we don't yet know the absolute paths to the qcc binaries on the host.
#
# To turn this into a working toolchain you typically need to:
#
#   * Add a repository rule (in bzl/, or via bazel_dep) that locates the QNX
#     SDP via the QNX_SDP_PATH environment variable and exposes the relevant
#     host binaries (host/linux/x86_64/usr/bin/qcc, q++, ntoaarch64-ar, ...)
#     and the target sysroot (target/qnx7|target/qnx8/<cpu>/{usr/include,
#     usr/lib, lib}).
#   * Replace the placeholders in tool_paths below with labels resolving to
#     those binaries.
#   * Add the standard cc_action_config / feature definitions (compile, link,
#     archive, default_compile_flags, default_link_flags, sysroot, ...).
#
# Reference: https://bazel.build/docs/cc-toolchain-config-reference

load(
    "@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
)

# Action names mirror those used by cc_toolchain rules.  These constants are
# normally imported from @bazel_tools//tools/build_defs/cc:action_names.bzl,
# but we re-declare a minimal subset here to avoid an additional load when
# this file is still a stub.
_ACTION_COMPILE = "c-compile"
_ACTION_CPP_COMPILE = "c++-compile"
_ACTION_CPP_LINK_EXECUTABLE = "c++-link-executable"
_ACTION_CPP_LINK_DYNAMIC_LIBRARY = "c++-link-dynamic-library"
_ACTION_CPP_LINK_STATIC_LIBRARY = "c++-link-static-library"

def _qnx_cc_toolchain_config_impl(ctx):
    # NOTE: Until a real QNX SDP repository rule is added, these tool paths
    # point at the host qcc binary by name only.  This will resolve to
    # whatever is on PATH at action-execution time, which is rarely what you
    # want for a hermetic build but is enough to load the toolchain.
    qcc_variant = ctx.attr.qcc_variant
    tool_paths = [
        tool_path(name = "gcc", path = "qcc"),
        tool_path(name = "ld", path = "qcc"),
        tool_path(name = "ar", path = "nto" + ctx.attr.cpu + "-ar"),
        tool_path(name = "cpp", path = "qcc"),
        tool_path(name = "gcov", path = "/bin/false"),
        tool_path(name = "nm", path = "nto" + ctx.attr.cpu + "-nm"),
        tool_path(name = "objdump", path = "nto" + ctx.attr.cpu + "-objdump"),
        tool_path(name = "strip", path = "nto" + ctx.attr.cpu + "-strip"),
    ]

    default_compile_flags = feature(
        name = "default_compile_flags",
        enabled = True,
        flag_sets = [flag_set(
            actions = [_ACTION_COMPILE, _ACTION_CPP_COMPILE],
            flag_groups = [flag_group(flags = [
                "-V" + qcc_variant,
                "-D__QNX__",
                "-D_QNX_SOURCE",
                "-fexceptions",
            ])],
        )],
    )

    default_link_flags = feature(
        name = "default_link_flags",
        enabled = True,
        flag_sets = [flag_set(
            actions = [
                _ACTION_CPP_LINK_EXECUTABLE,
                _ACTION_CPP_LINK_DYNAMIC_LIBRARY,
            ],
            flag_groups = [flag_group(flags = [
                "-V" + qcc_variant,
                "-lsocket",
                "-lm",
            ])],
        )],
    )

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = "qnx_" + ctx.attr.cpu,
        host_system_name = "x86_64-unknown-linux-gnu",
        target_system_name = ctx.attr.cpu + "-unknown-nto-qnx",
        target_cpu = ctx.attr.cpu,
        target_libc = "qnx",
        compiler = "qcc",
        abi_version = "qnx",
        abi_libc_version = "qnx",
        tool_paths = tool_paths,
        features = [default_compile_flags, default_link_flags],
        cxx_builtin_include_directories = [],
        builtin_sysroot = None,
    )

qnx_cc_toolchain_config = rule(
    implementation = _qnx_cc_toolchain_config_impl,
    attrs = {
        "cpu": attr.string(mandatory = True),
        "qcc_variant": attr.string(mandatory = True),
    },
    provides = [CcToolchainConfigInfo],
)
