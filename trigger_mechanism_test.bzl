"""Rebuild a test with a non-default trigger mechanism.

`//:trigger_mechanism` selects how a publisher wakes a waiting subscriber.
Linux defaults to eventfd, so a socketpair regression never runs in the
ordinary Linux suite. This rule rebuilds one test and its dependencies with
another mechanism.
"""

def _trigger_mechanism_transition_impl(settings, attr):
    _ = settings
    return {"//:trigger_mechanism": attr.mechanism}

_trigger_mechanism_transition = transition(
    implementation = _trigger_mechanism_transition_impl,
    inputs = [],
    outputs = ["//:trigger_mechanism"],
)

def _trigger_mechanism_test_impl(ctx):
    inner = ctx.attr.target[0][DefaultInfo]
    inner_exe = inner.files_to_run.executable
    if inner_exe == None:
        fail("trigger_mechanism_test target must be executable")

    # The test executable has to be produced by this rule. Launch the binary
    # that was built with the transitioned trigger mechanism.
    launcher = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(
        output = launcher,
        is_executable = True,
        content = """#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${{RUNFILES_DIR:-}}" ]]; then
  root="${{RUNFILES_DIR}}"
elif [[ -n "${{TEST_SRCDIR:-}}" ]]; then
  root="${{TEST_SRCDIR}}"
else
  root="${{0}}.runfiles"
fi
exec "${{root}}/{workspace}/{path}" "$@"
""".format(workspace = ctx.workspace_name, path = inner_exe.short_path),
    )
    runfiles = ctx.runfiles(files = [inner_exe]).merge(inner.default_runfiles)
    return [DefaultInfo(executable = launcher, runfiles = runfiles)]

trigger_mechanism_test = rule(
    implementation = _trigger_mechanism_test_impl,
    test = True,
    attrs = {
        "mechanism": attr.string(
            mandatory = True,
            values = ["eventfd", "pipe", "socketpair"],
            doc = "Value passed to --//:trigger_mechanism for this test.",
        ),
        "target": attr.label(
            mandatory = True,
            cfg = _trigger_mechanism_transition,
            doc = "Test to rebuild and run.",
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
