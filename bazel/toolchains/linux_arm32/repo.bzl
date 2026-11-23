"""Local ARMv7 hard-float Linux C++ toolchain.

Uses the distro `arm-linux-gnueabihf` cross compiler
(`g++-arm-linux-gnueabihf`). The repository is fetched only when a build
selects `@linux_arm32_toolchain`, so hosts without the cross compiler can
still build for the machine they are running on.
"""

_TOOLS = [
    "ar",
    "cpp",
    "dwp",
    "gcov",
    "ld",
    "nm",
    "objcopy",
    "objdump",
    "strip",
]

def _include_dirs(repository_ctx, gxx):
    result = repository_ctx.execute(
        [gxx, "-E", "-x", "c++", "-", "-v"],
        environment = {"LANG": "C"},
    )
    if result.return_code != 0:
        fail("failed to query {} include paths:\n{}".format(gxx, result.stderr))

    includes = []
    in_list = False
    for line in result.stderr.splitlines():
        if line.startswith("#include <...>"):
            in_list = True
            continue
        if line.startswith("End of search list"):
            break
        if not in_list:
            continue
        path = line.strip()
        # The cross compiler also lists the host /usr/include. Those headers
        # are the wrong architecture.
        if path == "/usr/include" or path.startswith("/usr/include/"):
            continue
        resolved = repository_ctx.execute(["readlink", "-f", path]).stdout.strip()
        if resolved and resolved not in includes:
            includes.append(resolved)
    if not includes:
        fail("no target include directories reported by " + gxx)
    return includes

def _write_wrapper(repository_ctx):
    repository_ctx.file(
        "bin/armhf-tool.sh",
        """#!/usr/bin/env bash
set -euo pipefail

tool="$1"
shift
prefix="/usr/bin/arm-linux-gnueabihf"

if [[ "${tool}" == "compiler" ]]; then
  compiler_kind="g++"
  previous=""
  for arg in "$@"; do
    if [[ "${previous}" == "-x" ]]; then
      case "${arg}" in
        c|assembler|assembler-with-cpp) compiler_kind="gcc" ;;
      esac
      previous=""
      continue
    fi
    case "${arg}" in
      -x) previous="-x" ;;
      *.c|*.s|*.S) compiler_kind="gcc" ;;
    esac
  done
  exec "${prefix}-${compiler_kind}" "$@"
fi

exec "${prefix}-${tool}" "$@"
""",
        executable = True,
    )
    repository_ctx.file(
        "bin/armhf-compiler",
        """#!/usr/bin/env bash
exec "$(dirname "$0")/armhf-tool.sh" compiler "$@"
""",
        executable = True,
    )
    for tool in _TOOLS:
        repository_ctx.file(
            "bin/armhf-" + tool,
            """#!/usr/bin/env bash
exec "$(dirname "$0")/armhf-tool.sh" {tool} "$@"
""".format(tool = tool),
            executable = True,
        )

def _linux_arm32_toolchain_impl(repository_ctx):
    gxx = repository_ctx.which("arm-linux-gnueabihf-g++")
    gcc = repository_ctx.which("arm-linux-gnueabihf-gcc")
    if not gxx or not gcc:
        fail(
            "arm-linux-gnueabihf-g++ was not found on PATH. " +
            "Install g++-arm-linux-gnueabihf (Debian/Ubuntu) and retry " +
            "`bazel build --config=linux_arm32 ...`.",
        )
    includes = _include_dirs(repository_ctx, str(gxx))
    _write_wrapper(repository_ctx)
    # -nostdinc drops the compiler's default search path, which ends in the
    # host /usr/include. Headers found there (valgrind, for example) are the
    # wrong architecture and Bazel rejects the absolute include.
    compile_flags = [
        "-fPIC",
        "-no-canonical-prefixes",
        "-fno-canonical-system-headers",
        "-nostdinc",
        "-march=armv7-a",
        "-mfpu=vfpv3-d16",
        "-mfloat-abi=hard",
    ]
    for inc in includes:
        compile_flags.append("-isystem")
        compile_flags.append(inc)

    tool_paths = {
        "gcc": "bin/armhf-compiler",
        "ar": "bin/armhf-ar",
        "cpp": "bin/armhf-cpp",
        "dwp": "bin/armhf-dwp",
        "gcov": "bin/armhf-gcov",
        "ld": "bin/armhf-ld",
        "nm": "bin/armhf-nm",
        "objcopy": "bin/armhf-objcopy",
        "objdump": "bin/armhf-objdump",
        "strip": "bin/armhf-strip",
    }
    repository_ctx.file(
        "BUILD.bazel",
        """load("@rules_cc//cc:defs.bzl", "cc_toolchain")
load("@rules_cc//cc/private/toolchain:unix_cc_toolchain_config.bzl", "cc_toolchain_config")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_files",
    srcs = glob(["bin/**"]),
)

filegroup(
    name = "empty",
    srcs = [],
)

cc_toolchain_config(
    name = "config",
    abi_libc_version = "glibc",
    abi_version = "gnueabihf",
    compile_flags = {compile_flags},
    compiler = "gcc",
    cpu = "armv7",
    cxx_builtin_include_directories = {includes},
    cxx_flags = ["-std=c++17"],
    dbg_compile_flags = ["-g"],
    host_system_name = "local",
    link_flags = [
        "-no-canonical-prefixes",
        "-lstdc++",
        "-lm",
        "-ldl",
    ],
    opt_compile_flags = [
        "-g0",
        "-O2",
        "-DNDEBUG",
    ],
    opt_link_flags = ["-Wl,-S"],
    supports_start_end_lib = False,
    target_libc = "glibc",
    target_system_name = "arm-linux-gnueabihf",
    tool_paths = {tool_paths},
    toolchain_identifier = "linux-arm32-gnueabihf",
    unfiltered_compile_flags = [
        "-no-canonical-prefixes",
        "-fno-canonical-system-headers",
        "-Wno-builtin-macro-redefined",
        '-D__DATE__="redacted"',
        '-D__TIMESTAMP__="redacted"',
        '-D__TIME__="redacted"',
    ],
)

cc_toolchain(
    name = "cc-compiler",
    all_files = ":all_files",
    ar_files = ":all_files",
    compiler_files = ":all_files",
    dwp_files = ":empty",
    linker_files = ":all_files",
    objcopy_files = ":all_files",
    strip_files = ":all_files",
    supports_param_files = 1,
    toolchain_config = ":config",
    toolchain_identifier = "linux-arm32-gnueabihf",
)

toolchain(
    name = "cc_toolchain",
    exec_compatible_with = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
    target_compatible_with = [
        "@platforms//cpu:armv7",
        "@platforms//os:linux",
    ],
    toolchain = ":cc-compiler",
    toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
)
""".format(
            compile_flags = repr(compile_flags),
            includes = repr(includes),
            tool_paths = repr(tool_paths),
        ),
    )

linux_arm32_toolchain = repository_rule(
    implementation = _linux_arm32_toolchain_impl,
    configure = True,
    local = True,
    environ = ["PATH"],
)
