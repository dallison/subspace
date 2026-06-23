load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:defs.bzl", "CcToolchainConfigInfo", "cc_common")
load(
    "@rules_cc//cc:cc_toolchain_config_lib.bzl",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
)

_COMPILE_ACTIONS = [
    ACTION_NAMES.c_compile,
    ACTION_NAMES.cpp_compile,
    ACTION_NAMES.assemble,
    ACTION_NAMES.preprocess_assemble,
    ACTION_NAMES.cpp_header_parsing,
    ACTION_NAMES.cpp_module_compile,
    ACTION_NAMES.cpp_module_codegen,
    ACTION_NAMES.clif_match,
    ACTION_NAMES.linkstamp_compile,
]

_LINK_ACTIONS = [
    ACTION_NAMES.cpp_link_executable,
    ACTION_NAMES.cpp_link_dynamic_library,
    ACTION_NAMES.cpp_link_nodeps_dynamic_library,
]

def _qnx_cc_toolchain_config_impl(ctx):
    qnx_sdp_path = ctx.var.get("QNX_SDP_PATH", "/home/dallison/qnx800")
    gcc_root = "{}/host/linux/x86_64/usr/lib/gcc/{}/{}".format(
        qnx_sdp_path,
        ctx.attr.gcc_triple,
        ctx.attr.gcc_version,
    )

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = ctx.attr.toolchain_identifier,
        host_system_name = "local",
        target_system_name = "qnx",
        target_cpu = ctx.attr.cpu,
        target_libc = "qnx",
        compiler = "gcc",
        abi_version = "gcc",
        abi_libc_version = "qnx",
        cxx_builtin_include_directories = [
            "{}/target/qnx/usr/include/c++/v1".format(qnx_sdp_path),
            "{}/include".format(gcc_root),
            "{}/target/qnx/usr/include".format(qnx_sdp_path),
        ],
        tool_paths = [
            tool_path(name = "ar", path = "tools/qnx_{}_ar".format(ctx.attr.cpu)),
            tool_path(name = "cpp", path = "tools/qnx_{}_cpp".format(ctx.attr.cpu)),
            tool_path(name = "gcc", path = "tools/qnx_{}_compiler".format(ctx.attr.cpu)),
            tool_path(name = "gcov", path = "tools/qnx_{}_gcov".format(ctx.attr.cpu)),
            tool_path(name = "ld", path = "tools/qnx_{}_ld".format(ctx.attr.cpu)),
            tool_path(name = "nm", path = "tools/qnx_{}_nm".format(ctx.attr.cpu)),
            tool_path(name = "objcopy", path = "tools/qnx_{}_objcopy".format(ctx.attr.cpu)),
            tool_path(name = "objdump", path = "tools/qnx_{}_objdump".format(ctx.attr.cpu)),
            tool_path(name = "strip", path = "tools/qnx_{}_strip".format(ctx.attr.cpu)),
        ],
        features = [
            feature(
                name = "default_compile_flags",
                enabled = True,
                flag_sets = [
                    flag_set(
                        actions = _COMPILE_ACTIONS,
                        flag_groups = [
                            flag_group(flags = [
                                "-D_QNX_SOURCE",
                            ]),
                        ],
                    ),
                ],
            ),
            feature(
                name = "dependency_file",
                enabled = True,
                flag_sets = [
                    flag_set(
                        actions = _COMPILE_ACTIONS,
                        flag_groups = [
                            flag_group(
                                flags = ["-MD", "-MF", "%{dependency_file}"],
                                expand_if_available = "dependency_file",
                            ),
                        ],
                    ),
                ],
            ),
            feature(name = "supports_pic", enabled = True),
            feature(
                name = "pic",
                enabled = True,
                flag_sets = [
                    flag_set(
                        actions = _COMPILE_ACTIONS,
                        flag_groups = [
                            flag_group(
                                flags = ["-fPIC"],
                                expand_if_available = "pic",
                            ),
                        ],
                    ),
                ],
            ),
            feature(
                name = "user_compile_flags",
                enabled = True,
                flag_sets = [
                    flag_set(
                        actions = _COMPILE_ACTIONS,
                        flag_groups = [
                            flag_group(
                                flags = ["%{user_compile_flags}"],
                                iterate_over = "user_compile_flags",
                                expand_if_available = "user_compile_flags",
                            ),
                        ],
                    ),
                ],
            ),
            feature(
                name = "user_link_flags",
                enabled = True,
                flag_sets = [
                    flag_set(
                        actions = _LINK_ACTIONS,
                        flag_groups = [
                            flag_group(
                                flags = ["%{user_link_flags}"],
                                iterate_over = "user_link_flags",
                                expand_if_available = "user_link_flags",
                            ),
                        ],
                    ),
                ],
            ),
            feature(name = "supports_dynamic_linker", enabled = True),
            feature(name = "supports_param_files", enabled = True),
        ],
    )

qnx_cc_toolchain_config = rule(
    implementation = _qnx_cc_toolchain_config_impl,
    attrs = {
        "cpu": attr.string(mandatory = True),
        "gcc_triple": attr.string(mandatory = True),
        "gcc_version": attr.string(default = "12.2.0"),
        "toolchain_identifier": attr.string(mandatory = True),
    },
    provides = [CcToolchainConfigInfo],
)
