load("@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl", "tool_path")

def _impl(ctx):
    # Map to QNX qcc wrapper
    tool_paths = [
        tool_path(name = "gcc", path = "qcc_wrapper.sh"),
        tool_path(name = "ld", path = "qcc_wrapper.sh"),
        tool_path(name = "ar", path = "qcc_wrapper.sh"),
        tool_path(name = "cpp", path = "qcc_wrapper.sh"),
        tool_path(name = "nm", path = "qcc_wrapper.sh"),
        tool_path(name = "objdump", path = "qcc_wrapper.sh"),
        tool_path(name = "strip", path = "qcc_wrapper.sh"),
    ]

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = "qnx-toolchain",
        host_system_name = "local",
        target_system_name = "qnx",
        target_cpu = "aarch64",
        target_libc = "unknown",
        compiler = "qcc",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        tool_paths = tool_paths,
    )

qnx_toolchain_config = rule(
    implementation = _impl,
    attrs = {},
    provides = [CcToolchainConfigInfo],
)
