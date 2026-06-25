#!/usr/bin/env bash
set -euo pipefail

tool_name="$(basename "$0")"

case "$tool_name" in
  qnx_aarch64_*)
    prefix="ntoaarch64"
    requested="${tool_name#qnx_aarch64_}"
    ;;
  qnx_x86_64_*)
    prefix="ntox86_64"
    requested="${tool_name#qnx_x86_64_}"
    ;;
  *)
    echo "Unknown QNX Bazel tool wrapper: $tool_name" >&2
    exit 1
    ;;
esac

if [[ -n "${QNX_SDP_PATH:-}" ]]; then
  qnx_sdp_path="${QNX_SDP_PATH}"
else
  qnx_sdp_path="${HOME:-/home/dallison}/qnx800"
fi
if [[ ! -f "${qnx_sdp_path}/qnxsdp-env.sh" ]]; then
  echo "QNX SDP not found at ${qnx_sdp_path}; set QNX_SDP_PATH to your SDP root" >&2
  exit 1
fi

# Bazel actions run with a minimal environment. The QNX SDP environment script
# expects HOME to exist, so synthesize one when the action environment omits it.
export HOME="${HOME:-${qnx_sdp_path%/*}}"

# qnxsdp-env.sh defines QNX_HOST/QNX_TARGET and prepends the host tools to PATH.
# shellcheck disable=SC1090
. "${qnx_sdp_path}/qnxsdp-env.sh" >/dev/null

compiler_kind="g++"
if [[ "$requested" == "compiler" ]]; then
  compiler_kind="g++"
  compile_action=0
  previous=""
  for arg in "$@"; do
    if [[ "$previous" == "-x" ]]; then
      case "$arg" in
        c|assembler|assembler-with-cpp) compiler_kind="gcc" ;;
      esac
      previous=""
      continue
    fi

    case "$arg" in
      -c|-S|-E)
        compile_action=1
        ;;
      -x)
        previous="-x"
        ;;
      *.c|*.s|*.S)
        compiler_kind="gcc"
        ;;
    esac
  done

  if [[ "$compile_action" == 0 ]]; then
    tmp_files=()
    cleanup() {
      rm -f "${tmp_files[@]}"
    }
    trap cleanup EXIT

    filtered_args=()
    for arg in "$@"; do
      if [[ "$arg" == "-lpthread" ]]; then
        continue
      fi

      if [[ "$arg" == @* ]]; then
        src="${arg#@}"
        dst="$(mktemp "${TMPDIR:-/tmp}/qnx-bazel-params.XXXXXX")"
        tmp_files+=("$dst")
        while IFS= read -r line || [[ -n "$line" ]]; do
          [[ "$line" == "-lpthread" ]] && continue
          printf '%s\n' "$line"
        done < "$src" > "$dst"
        filtered_args+=("@${dst}")
      else
        filtered_args+=("$arg")
      fi
    done

    exec "${QNX_HOST}/usr/bin/${prefix}-${compiler_kind}" "${filtered_args[@]}"
  fi

  exec "${QNX_HOST}/usr/bin/${prefix}-${compiler_kind}" "$@"
fi

exec "${QNX_HOST}/usr/bin/${prefix}-${requested}" "$@"
