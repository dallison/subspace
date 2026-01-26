# QNX Toolchain Configuration for Bazel (rules_cc-style)

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load(
    "@rules_cc//cc:cc_toolchain_config_lib.bzl",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
)
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/toolchains:cc_toolchain_config_info.bzl", "CcToolchainConfigInfo")

_compile_actions = [
    ACTION_NAMES.c_compile,
    ACTION_NAMES.cpp_compile,
    ACTION_NAMES.linkstamp_compile,
    ACTION_NAMES.preprocess_assemble,
    ACTION_NAMES.cpp_header_parsing,
    ACTION_NAMES.cpp_module_compile,
    ACTION_NAMES.cpp_module_codegen,
]

_link_actions = [
    ACTION_NAMES.cpp_link_executable,
    ACTION_NAMES.cpp_link_dynamic_library,
    ACTION_NAMES.cpp_link_nodeps_dynamic_library,
]

def _qnx_host_bin(qnx_sdp_path):
    return qnx_sdp_path + "/host/linux/x86_64/usr/bin"

def _nto_tool(host_bin, cpu, tool):
    return host_bin + "/nto" + cpu + "-" + tool

def _impl(ctx):
    cpu = ctx.attr.cpu
    compiler_suffix = ctx.attr.compiler_suffix

    # Use --define=QNX_SDP_PATH to provide an absolute path.
    qnx_sdp_path = ctx.var.get("QNX_SDP_PATH", ctx.attr.qnx_sdp_path)
    if qnx_sdp_path.startswith("~"):
        fail("QNX_SDP_PATH must be an absolute path (no ~). Set --define=QNX_SDP_PATH=/path")

    qnx_host_bin = _qnx_host_bin(qnx_sdp_path)
    qcc_path = qnx_host_bin + "/qcc"
    qnx_target = qnx_sdp_path + "/target/qnx7"

    compiler_variant_flag = "-V" + compiler_suffix

    tool_paths = [
        tool_path(name = "gcc", path = qcc_path),
        tool_path(name = "ld", path = qcc_path),
        tool_path(name = "cpp", path = qcc_path),
        tool_path(name = "ar", path = _nto_tool(qnx_host_bin, cpu, "ar")),
        tool_path(name = "nm", path = _nto_tool(qnx_host_bin, cpu, "nm")),
        tool_path(name = "objdump", path = _nto_tool(qnx_host_bin, cpu, "objdump")),
        tool_path(name = "strip", path = _nto_tool(qnx_host_bin, cpu, "strip")),
        tool_path(name = "gcov", path = "/bin/false"),
    ]

    default_compile_flags = feature(
        name = "default_compile_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _compile_actions,
                flag_groups = [
                    flag_group(flags = [compiler_variant_flag]),
                ],
            ),
        ],
    )

    default_link_flags = feature(
        name = "default_link_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _link_actions,
                flag_groups = [
                    flag_group(flags = [
                        compiler_variant_flag,
                        "-L" + qnx_target + "/" + cpu + "/usr/lib",
                        "-L" + qnx_target + "/" + cpu + "/lib",
                    ]),
                ],
            ),
        ],
    )

    user_compile_flags_feature = feature(
        name = "user_compile_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _compile_actions,
                flag_groups = [
                    flag_group(
                        flags = ["%{user_compile_flags}"],
                        iterate_over = "user_compile_flags",
                        expand_if_available = "user_compile_flags",
                    ),
                ],
            ),
        ],
    )

    user_link_flags_feature = feature(
        name = "user_link_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _link_actions,
                flag_groups = [
                    flag_group(
                        flags = ["%{user_link_flags}"],
                        iterate_over = "user_link_flags",
                        expand_if_available = "user_link_flags",
                    ),
                ],
            ),
        ],
    )

    include_paths_feature = feature(
        name = "include_paths",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _compile_actions,
                flag_groups = [
                    flag_group(
                        flags = ["-iquote", "%{quote_include_paths}"],
                        iterate_over = "quote_include_paths",
                    ),
                    flag_group(
                        flags = ["-I%{include_paths}"],
                        iterate_over = "include_paths",
                    ),
                    flag_group(
                        flags = ["-isystem", "%{system_include_paths}"],
                        iterate_over = "system_include_paths",
                    ),
                ],
            ),
        ],
    )

    external_include_paths_feature = feature(
        name = "external_include_paths",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _compile_actions,
                flag_groups = [
                    flag_group(
                        flags = ["-isystem", "%{external_include_paths}"],
                        iterate_over = "external_include_paths",
                        expand_if_available = "external_include_paths",
                    ),
                ],
            ),
        ],
    )

    library_search_directories_feature = feature(
        name = "library_search_directories",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _link_actions,
                flag_groups = [
                    flag_group(
                        flags = ["-L%{library_search_directories}"],
                        iterate_over = "library_search_directories",
                        expand_if_available = "library_search_directories",
                    ),
                ],
            ),
        ],
    )

    pic_feature = feature(
        name = "pic",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _compile_actions,
                flag_groups = [
                    flag_group(flags = ["-fPIC"], expand_if_available = "pic"),
                ],
            ),
        ],
    )

    dependency_file_feature = feature(
        name = "dependency_file",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = _compile_actions,
                flag_groups = [
                    flag_group(
                        flags = ["-Wc,-MD,-MF,%{dependency_file}"],
                        expand_if_available = "dependency_file",
                    ),
                ],
            ),
        ],
    )

    cxx_builtin_includes = [
        qnx_target + "/usr/include",
        qnx_target + "/usr/include/cpp",
    ]

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        features = [
            default_compile_flags,
            default_link_flags,
            user_compile_flags_feature,
            user_link_flags_feature,
            include_paths_feature,
            external_include_paths_feature,
            library_search_directories_feature,
            pic_feature,
            dependency_file_feature,
        ],
        cxx_builtin_include_directories = cxx_builtin_includes,
        toolchain_identifier = "qnx_" + cpu,
        host_system_name = "local",
        target_system_name = "qnx",
        target_cpu = cpu,
        target_libc = "qnx",
        compiler = "qcc",
        abi_version = "local",
        abi_libc_version = "local",
        tool_paths = tool_paths,
        builtin_sysroot = qnx_target,
    )

qnx_cc_toolchain_config = rule(
    implementation = _impl,
    attrs = {
        "cpu": attr.string(mandatory = True),
        "compiler_suffix": attr.string(mandatory = True),
        "qnx_sdp_path": attr.string(default = "qnx800"),
    },
    fragments = ["cpp"],
    provides = [CcToolchainConfigInfo],
)
