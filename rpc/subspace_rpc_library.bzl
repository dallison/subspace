"""
This module provides a rule to generate subspace_rpc message files from proto_library targets.
"""

load("@bazel_skylib//lib:paths.bzl", "paths")

MessageInfo = provider(fields = ["direct_sources", "transitive_sources", "cpp_outputs"])

def _subspace_rpc_action(
        ctx,
        direct_sources,
        transitive_sources,
        out_dir,
        package_name,
        outputs,
        add_namespace,
        target_name):
    # The protobuf compiler allow plugins to get arguments specified in the --plugin_out
    # argument.  The args are passed as a comma separated list of key=value pairs followed
    # by a colon and the output directory.
    options_and_out_dir = ""
    if add_namespace != "":
        options_and_out_dir = "--subspace_rpc_out=add_namespace={},package_name={},target_name={}:{}".format(add_namespace, package_name, target_name, out_dir)
    else:
        options_and_out_dir = "--subspace_rpc_out=package_name={},target_name={}:{}".format(package_name, target_name, out_dir)

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
                # ../com_google_protobuf/_virtual_imports/any_proto/google/protobuf/any.proto
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

def subspace_rpc_library(name, deps = [], add_namespace = ""):
    """
    Generate a cc_libary for protobuf files specified in deps.

    Args:
        name: name
        deps: proto_libraries that contain the protobuf files
        deps: dependencies
        runtime: label for subspace_rpc runtime.
        add_namespace: add given namespace to the message output
    """
    all_files = name + "_files"

    _subspace_rpc_gen(
        name = all_files,
        deps = deps,
        add_namespace = add_namespace,
        package_name = native.package_name(),
        target_name = name,
    )

    client_srcs = name + "_client_srcs"
    _split_files(
        name = client_srcs,
        ext = "rpc_client.cc",
        deps = [all_files],
    )
    client_deps = ["//rpc/client:rpc_client"]

    client_hdrs = name + "_client_hdrs"
    _split_files(
        name = client_hdrs,
        ext = "rpc_client.h",
        deps = [all_files],
    )

    libdeps = []
    for dep in deps:
        if dep.endswith("_proto"):
            cc_proto = dep.replace("_proto", "_cc_proto")
            libdeps.append(cc_proto)

    client_name = name + "_client"
    native.cc_library(
        name = client_name,
        srcs = [client_srcs],
        hdrs = [client_hdrs],
        deps = libdeps + client_deps
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

    server_deps = ["//rpc/server:rpc_server"]

    server_name = name + "_server"
    native.cc_library(
        name = server_name,
        srcs = [server_srcs],
        hdrs = [server_hdrs],
        deps = libdeps + server_deps,
    )
