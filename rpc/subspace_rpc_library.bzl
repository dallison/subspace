"""
This module provides a rule to generate subspace_rpc message files from proto_library targets.
"""

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@rules_proto//proto:defs.bzl", "ProtoInfo")
load("@rules_cc//cc:defs.bzl", "cc_library")

MessageInfo = provider(fields = ["direct_sources", "transitive_sources", "cpp_outputs"])

def _subspace_rpc_action(
        ctx,
        direct_sources,
        transitive_sources,
        out_dir,
        package_name,
        outputs,
        add_namespace,
        target_name,
        rpc_style):
    # The protobuf compiler allow plugins to get arguments specified in the --plugin_out
    # argument.  The args are passed as a comma separated list of key=value pairs followed
    # by a colon and the output directory.
    options = []
    if add_namespace != "":
        options.append("add_namespace={}".format(add_namespace))
    options.append("package_name={}".format(package_name))
    options.append("target_name={}".format(target_name))
    if rpc_style != "co":
        options.append("rpc_style={}".format(rpc_style))
    options_and_out_dir = "--subspace_rpc_out={}:{}".format(",".join(options), out_dir)

    inputs = depset(direct = direct_sources, transitive = transitive_sources)

    import_paths = []
    for s in transitive_sources:
        for f in s.to_list():
            if not f.is_source:
                index = f.path.find("_virtual_imports")
                if index != -1:
                    # Go to first slash after _virtual_imports/
                    slash = f.path.find("/", index + 17)
                    import_paths.append("-I" + f.path[:slash])

    plugin, _, plugin_manifests = ctx.resolve_command(tools = [ctx.attr.subspace_rpc_plugin])
    plugin_arg = "--plugin=protoc-gen-subspace_rpc={}".format(ctx.executable.subspace_rpc_plugin.path)

    args = ctx.actions.args()
    args.add(plugin_arg)
    args.add(options_and_out_dir)
    args.add_all(inputs)
    args.add_all(import_paths)
    args.add("-I.")

    ctx.actions.run(
        inputs = inputs,
        tools = plugin,
        input_manifests = plugin_manifests,
        executable = ctx.executable.protoc,
        outputs = outputs,
        arguments = [args],
        progress_message = "Generating subspace_rpc message files %s" % ctx.label,
        mnemonic = "Phaser",
    )

# This aspect generates the MessageInfo provider containing the files we
# will generate from running the Phaser plugin.
def _subspace_rpc_aspect_impl(target, _ctx):
    direct_sources = []
    transitive_sources = depset()
    cpp_outputs = []

    def add_output(base):
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_client.cc"))
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_client.h"))
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_server.cc"))
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_server.h"))

    if ProtoInfo in target:
        transitive_sources = target[ProtoInfo].transitive_sources
        for s in transitive_sources.to_list():
            direct_sources.append(s)
            file_path = s.short_path
            if "_virtual_imports" in file_path:
                # For a file that is not in this package, we need to generate the
                # output in our package.
                # The path looks like:
                # ../protobuf/_virtual_imports/any_proto/google/protobuf/any.proto
                # We want to declare the file as:ƒ
                # google/protobuf/any.subspace_rpc.cc
                v = file_path.split("_virtual_imports/")

                # Remove the first directory of v[1] to get the path relative to the package.
                file_path = v[1].split("/", 1)[1]
            add_output(file_path)

    return [MessageInfo(
        direct_sources = direct_sources,
        transitive_sources = transitive_sources,
        cpp_outputs = cpp_outputs,
    )]

subspace_rpc_aspect = aspect(
    attr_aspects = ["deps"],
    provides = [MessageInfo],
    implementation = _subspace_rpc_aspect_impl,
)

# The subspace_rpc rule runs the Subspace RPC plugin from the protoc compiler.
# The deps for the rule are proto_libraries that contain the protobuf files.
def _subspace_rpc_impl(ctx):
    outputs = []

    direct_sources = []
    transitive_sources = []
    cpp_outputs = []
    package_name = ctx.attr.package_name
    for dep in ctx.attr.deps:
        dep_outs = []
        for out in dep[MessageInfo].cpp_outputs:
            out_name = ctx.attr.target_name + "/" + out
            out_file = ctx.actions.declare_file(out_name)
            dep_outs.append(out_file)

            # If we are creating a header file in our package, we need to create a symlink to it.
            # This is because the header file will be something like
            # subspace_rpc/testdata/subspace_rpc/testdata/Test.subspace.rpc_client.h
            # but we want to be able to do:
            # #include "subspace_rpc/testdata/Test.subspace.rpc_client.h"
            # so we create the symlink:
            # Test.subspace.rpc_client.h -> subspace_rpc/testdata/subspace_rpc/testdata/Test.subspace.rpc_client.h
            if out_file.extension == "h":
                prefix = paths.join(ctx.attr.target_name, package_name)
                symlink_name = out_file.short_path[len(prefix) + 1:]
                if symlink_name.startswith(package_name):
                    # Header is in our package, remove the package name.
                    # If the header is outside our package (like google/protobuf/any.h),
                    # we don't want to create a symlink to it becuase it's in
                    # the right place already.
                    symlink_name = symlink_name[len(package_name) + 1:]
                    symlink = ctx.actions.declare_file(symlink_name)
                    ctx.actions.symlink(output = symlink, target_file = out_file)
                    dep_outs.append(symlink)
            cpp_outputs.append(out_file)

        direct_sources += dep[MessageInfo].direct_sources
        transitive_sources.append(dep[MessageInfo].transitive_sources)
        outputs += dep_outs

    _subspace_rpc_action(
        ctx,
        direct_sources,
        transitive_sources,
        ctx.bin_dir.path,
        ctx.attr.package_name,
        cpp_outputs,
        ctx.attr.add_namespace,
        ctx.attr.target_name,
        ctx.attr.rpc_style,
    )

    return [DefaultInfo(files = depset(outputs))]

_subspace_rpc_gen = rule(
    attrs = {
        "protoc": attr.label(
            executable = True,
            default = Label("@protobuf//:protoc"),
            cfg = "exec",
        ),
        "subspace_rpc_plugin": attr.label(
            executable = True,
            default = Label("//rpc/idl_compiler:subspace_rpc"),
            cfg = "exec",
        ),
        "deps": attr.label_list(
            aspects = [subspace_rpc_aspect],
        ),
        "add_namespace": attr.string(),
        "package_name": attr.string(),
        "target_name": attr.string(),
        "rpc_style": attr.string(default = "co"),
    },
    implementation = _subspace_rpc_impl,
)

def get_rpc_extension(filename):
    """
    Returns the file extension from a given filename or path.
    Returns an empty string if no extension is found.
    """
    parts = filename.split(".")
    if len(parts) > 2:
        return ".".join(parts[-2:])
    else:
        return ""

def _split_files_impl(ctx):
    files = []
    for file in ctx.files.deps:
        ext = get_rpc_extension(file.basename)
        if ext == ctx.attr.ext:
            files.append(file)

    return [DefaultInfo(files = depset(files))]

_split_files = rule(
    attrs = {
        "deps": attr.label_list(mandatory = True),
        "ext": attr.string(mandatory = True),
    },
    implementation = _split_files_impl,
)

RustRpcInfo = provider(fields = ["direct_sources", "transitive_sources", "rs_outputs", "cpp_outputs"])

def _subspace_rpc_rust_aspect_impl(target, _ctx):
    direct_sources = []
    transitive_sources = depset()
    rs_outputs = []
    cpp_outputs = []

    def add_output(base):
        rs_outputs.append(paths.replace_extension(base, ".subspace.rpc_client.rs"))
        rs_outputs.append(paths.replace_extension(base, ".subspace.rpc_server.rs"))
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_client.cc"))
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_client.h"))
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_server.cc"))
        cpp_outputs.append(paths.replace_extension(base, ".subspace.rpc_server.h"))

    if ProtoInfo in target:
        transitive_sources = target[ProtoInfo].transitive_sources
        for s in transitive_sources.to_list():
            direct_sources.append(s)
            file_path = s.short_path
            if "_virtual_imports" in file_path:
                v = file_path.split("_virtual_imports/")
                file_path = v[1].split("/", 1)[1]
            add_output(file_path)

    return [RustRpcInfo(
        direct_sources = direct_sources,
        transitive_sources = transitive_sources,
        rs_outputs = rs_outputs,
        cpp_outputs = cpp_outputs,
    )]

subspace_rpc_rust_aspect = aspect(
    attr_aspects = ["deps"],
    provides = [RustRpcInfo],
    implementation = _subspace_rpc_rust_aspect_impl,
)

def _subspace_rpc_rust_impl(ctx):
    all_action_outputs = []
    rs_outputs = []
    direct_sources = []
    transitive_sources = []

    for dep in ctx.attr.deps:
        info = dep[RustRpcInfo]

        for out in info.rs_outputs:
            out_name = ctx.attr.target_name + "/" + out
            out_file = ctx.actions.declare_file(out_name)
            all_action_outputs.append(out_file)
            rs_outputs.append(out_file)

        for out in info.cpp_outputs:
            out_name = ctx.attr.target_name + "/" + out
            out_file = ctx.actions.declare_file(out_name)
            all_action_outputs.append(out_file)

        direct_sources += info.direct_sources
        transitive_sources.append(info.transitive_sources)

    _subspace_rpc_action(
        ctx,
        direct_sources,
        transitive_sources,
        ctx.bin_dir.path,
        ctx.attr.package_name,
        all_action_outputs,
        ctx.attr.add_namespace,
        ctx.attr.target_name,
        "rust",
    )

    return [DefaultInfo(files = depset(rs_outputs))]

_subspace_rpc_rust_gen = rule(
    attrs = {
        "protoc": attr.label(
            executable = True,
            default = Label("@protobuf//:protoc"),
            cfg = "exec",
        ),
        "subspace_rpc_plugin": attr.label(
            executable = True,
            default = Label("//rpc/idl_compiler:subspace_rpc"),
            cfg = "exec",
        ),
        "deps": attr.label_list(
            aspects = [subspace_rpc_rust_aspect],
        ),
        "add_namespace": attr.string(),
        "package_name": attr.string(),
        "target_name": attr.string(),
    },
    implementation = _subspace_rpc_rust_impl,
)

def subspace_rpc_rust_library(name, deps = [], add_namespace = ""):
    """
    Generate Rust service stubs from proto_library targets.

    Produces .rs files for client and server stubs that can be included
    in a Rust crate via include!().  The generated files use subspace_rpc::
    import paths.

    Creates targets:
        :{name}_files - all generated Rust source files
        :{name}_client - just the client .rs files
        :{name}_server - just the server .rs files

    Args:
        name: name
        deps: proto_library targets containing the protobuf files
        add_namespace: add given namespace to the generated output
    """
    all_files = name + "_files"

    _subspace_rpc_rust_gen(
        name = all_files,
        deps = deps,
        add_namespace = add_namespace,
        package_name = native.package_name(),
        target_name = name,
    )

    client_name = name + "_client"
    _split_files(
        name = client_name,
        ext = "rpc_client.rs",
        deps = [all_files],
    )

    server_name = name + "_server"
    _split_files(
        name = server_name,
        ext = "rpc_server.rs",
        deps = [all_files],
    )

def subspace_rpc_library(name, deps = [], add_namespace = "", rpc_style = "co"):
    """
    Generate a cc_library for protobuf files specified in deps.

    Args:
        name: name
        deps: proto_libraries that contain the protobuf files
        add_namespace: add given namespace to the message output
        rpc_style: RPC coroutine style - "co" (default, co::Coroutine),
                   "asio" (Boost.Asio stackful yield_context),
                   "coro" (C++20 Boost.Asio awaitable),
                   "co20" (C++20 co20::Coroutine)
    """
    all_files = name + "_files"

    _subspace_rpc_gen(
        name = all_files,
        deps = deps,
        add_namespace = add_namespace,
        package_name = native.package_name(),
        target_name = name,
        rpc_style = rpc_style,
    )

    client_srcs = name + "_client_srcs"
    _split_files(
        name = client_srcs,
        ext = "rpc_client.cc",
        deps = [all_files],
    )

    client_hdrs = name + "_client_hdrs"
    _split_files(
        name = client_hdrs,
        ext = "rpc_client.h",
        deps = [all_files],
    )

    if rpc_style == "asio":
        client_deps = ["//asio_rpc/client:rpc_client"]
        server_deps = ["//asio_rpc/server:rpc_server"]
    elif rpc_style == "coro":
        client_deps = ["//coro_rpc/client:rpc_client"]
        server_deps = ["//coro_rpc/server:rpc_server"]
    elif rpc_style == "co20":
        client_deps = ["//co20_rpc/client:rpc_client"]
        server_deps = ["//co20_rpc/server:rpc_server"]
    else:
        client_deps = ["//rpc/client:rpc_client"]
        server_deps = ["//rpc/server:rpc_server"]

    copts = []
    if rpc_style == "coro" or rpc_style == "co20":
        copts = ["-std=c++20"]

    libdeps = []
    for dep in deps:
        if dep.endswith("_proto"):
            cc_proto = dep.replace("_proto", "_cc_proto")
            libdeps.append(cc_proto)

    client_name = name + "_client"
    cc_library(
        name = client_name,
        srcs = [client_srcs],
        hdrs = [client_hdrs],
        copts = copts,
        deps = libdeps + client_deps,
    )

    server_srcs = name + "_server_srcs"
    _split_files(
        name = server_srcs,
        ext = "rpc_server.cc",
        deps = [all_files],
    )

    server_hdrs = name + "_server_hdrs"
    _split_files(
        name = server_hdrs,
        ext = "rpc_server.h",
        deps = [all_files],
    )

    server_name = name + "_server"
    cc_library(
        name = server_name,
        srcs = [server_srcs],
        hdrs = [server_hdrs],
        copts = copts,
        deps = libdeps + server_deps,
    )
