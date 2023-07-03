"""Rule(s) for creation of pybind python bindings of C/C++ code."""

load("@rules_cc//cc:defs.bzl", "cc_binary")
load("@rules_python//python:defs.bzl", "py_library")

PYBIND_DEPS = [
    "@pybind11",
]

def pybind_module(
        name,
        srcs = [],
        deps = [],
        data = [],
        **kwargs):
    """Build a pybind target.

    This creates a cc_binary with the proper link options and a corresponding
    py_library.

    Args:
        name: pybind target name
        srcs: source files. Defaults to [].
        deps: dependencies. Defaults to [].
        data: data targets. Defaults to [].
        **kwargs: additional keyword arguments to
            pass to cc_library AND py_library.
    """
    cc_output_name = name + ".so"
    cc_binary(
        name = name + ".so",
        srcs = srcs,
        linkshared = 1,
        deps = deps + PYBIND_DEPS,
        **kwargs
    )

    py_library(
        name = name,
        srcs = [],
        data = [cc_output_name] + data,
        **kwargs
    )
