# QNX Toolchain Configuration for Bazel

load("@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl", 
    "feature", 
    "flag_group", 
    "flag_set", 
    "tool_path", 
    "with_feature_set",
)
load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/toolchains:cc_toolchain_config_info.bzl", "CcToolchainConfigInfo")

def _impl(ctx):
    cpu = ctx.attr.cpu
    compiler_suffix = ctx.attr.compiler_suffix
    
    # Get QNX SDP path from attribute (defaults to ~/qnx800)
    # Use --define=QNX_SDP_PATH to provide an absolute path.
    qnx_sdp_path = ctx.var.get("QNX_SDP_PATH", ctx.attr.qnx_sdp_path)
    if qnx_sdp_path.startswith("~"):
        fail("QNX_SDP_PATH must be an absolute path (no ~). Set --define=QNX_SDP_PATH=/path")
    
    qcc_path = qnx_sdp_path + "/host/linux/x86_64/usr/bin/qcc"
    qnx_target = qnx_sdp_path + "/target/qnx7"
    
    # Tool paths - qcc handles everything
    tool_paths = [
        tool_path(
            name = "gcc",
            path = qcc_path,
        ),
        tool_path(
            name = "ld",
            path = qcc_path,
        ),
        tool_path(
            name = "ar",
            path = qcc_path,
        ),
        tool_path(
            name = "cpp",
            path = qcc_path,
        ),
        tool_path(
            name = "gcov",
            path = "/bin/false",  # Not used
        ),
        tool_path(
            name = "nm",
            path = "/bin/false",  # Not used
        ),
        tool_path(
            name = "objdump",
            path = "/bin/false",  # Not used
        ),
        tool_path(
            name = "strip",
            path = "/bin/false",  # Not used
        ),
    ]
    
    # Compiler variant flag
    compiler_variant_flag = "-V" + compiler_suffix
    
    # Architecture-specific flags
    if cpu == "aarch64":
        arch_flags = ["-Wc,-march=armv8-a+crc"]
        link_arch_flags = []
    else:  # x86_64
        arch_flags = ["-Wc,-m64"]
        link_arch_flags = ["-m64"]
    
    # Compiler flags
    compiler_flags = feature(
        name = "compiler_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [
                    ACTION_NAMES.c_compile,
                    ACTION_NAMES.cpp_compile,
                    ACTION_NAMES.linkstamp_compile,
                    ACTION_NAMES.preprocess_assemble,
                    ACTION_NAMES.cpp_header_parsing,
                    ACTION_NAMES.cpp_module_compile,
                    ACTION_NAMES.cpp_module_codegen,
                    ACTION_NAMES.clif_match,
                ],
                flag_groups = [
                    flag_group(
                        flags = [compiler_variant_flag] + arch_flags + [
                            "-Wc,-std=c++17",
                            "-Wc,-Wall",
                            "-Wc,-Wextra",
                            "-D__QNX__",
                        ],
                    ),
                ],
            ),
            flag_set(
                actions = [ACTION_NAMES.c_compile],
                flag_groups = [
                    flag_group(
                        flags = ["-Wc,-std=c11"],
                    ),
                ],
            ),
        ],
    )
    
    # Linker flags
    linker_flags = feature(
        name = "linker_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [
                    ACTION_NAMES.cpp_link_executable,
                    ACTION_NAMES.cpp_link_dynamic_library,
                    ACTION_NAMES.cpp_link_nodeps_dynamic_library,
                ],
                flag_groups = [
                    flag_group(
                        flags = [compiler_variant_flag] + link_arch_flags + [
                            "-L" + qnx_target + "/" + cpu + "/usr/lib",
                            "-L" + qnx_target + "/" + cpu + "/lib",
                        ],
                    ),
                ],
            ),
        ],
    )
    
    # Include paths
    include_paths = feature(
        name = "include_paths",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [
                    ACTION_NAMES.c_compile,
                    ACTION_NAMES.cpp_compile,
                    ACTION_NAMES.preprocess_assemble,
                    ACTION_NAMES.cpp_header_parsing,
                    ACTION_NAMES.cpp_module_compile,
                ],
                flag_groups = [
                    flag_group(
                        flags = [
                            "-I" + qnx_target + "/usr/include",
                            "-I" + qnx_target + "/usr/include/cpp",
                        ],
                    ),
                ],
            ),
        ],
    )
    
    # Assembly flags
    asm_flags = feature(
        name = "asm_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [ACTION_NAMES.assemble],
                flag_groups = [
                    flag_group(
                        flags = [compiler_variant_flag] + (["-march=armv8-a+crc"] if cpu == "aarch64" else ["-m64"]),
                    ),
                ],
            ),
        ],
    )
    
    # Default features
    default_features = [
        compiler_flags,
        linker_flags,
        include_paths,
        asm_flags,
    ]
    
    cxx_builtin_includes = [
        qnx_target + "/usr/include",
        qnx_target + "/usr/include/cpp",
    ]

    # Create CcToolchainConfigInfo via cc_common from rules_cc.
    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        features = default_features,
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
    )

qnx_cc_toolchain_config = rule(
    implementation = _impl,
    attrs = {
        "cpu": attr.string(mandatory = True),
        "compiler_suffix": attr.string(mandatory = True),
        # Default QNX SDP path - override by setting QNX_SDP_PATH env var before building
        # The build script handles this automatically
        "qnx_sdp_path": attr.string(default = "~/qnx800"),
    },
    fragments = ["cpp"],
    provides = [CcToolchainConfigInfo],
)
