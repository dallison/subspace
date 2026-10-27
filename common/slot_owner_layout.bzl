"""Build a test under a non-default slot-owner limit.

`//:max_slot_owners` sizes shared-memory structs. The layout checks in
`channel.h` and the Rust client only see the count they were compiled with, so
a regression that pins those sizes to the default 1024 still passes the normal
suite. This rule rebuilds one test (and its dependencies) at another count.
"""

def _slot_owner_limit_transition_impl(settings, attr):
    _ = settings
    return {"//:max_slot_owners": attr.slot_owners}

_slot_owner_limit_transition = transition(
    implementation = _slot_owner_limit_transition_impl,
    inputs = [],
    outputs = ["//:max_slot_owners"],
)

def _slot_owner_limit_test_impl(ctx):
    inner = ctx.attr.target[0][DefaultInfo]
    inner_exe = inner.files_to_run.executable
    if inner_exe == None:
        fail("slot_owner_limit_test target must be executable")

    # The test executable has to be produced by this rule. Launch the binary
    # that was built with the transitioned slot-owner limit.
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

slot_owner_limit_test = rule(
    implementation = _slot_owner_limit_test_impl,
    test = True,
    attrs = {
        "slot_owners": attr.string(
            mandatory = True,
            doc = "Value passed to --//:max_slot_owners for this test.",
        ),
        "target": attr.label(
            mandatory = True,
            cfg = _slot_owner_limit_transition,
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
