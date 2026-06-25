#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Stage and copy Subspace's QNX x86_64 client_test bundle to a QNX VM.

Usage:
  .github/scripts/qnx-deploy-client-test.sh [options]

Options:
  --qnx-ip IP          QNX VM IP address. If omitted, the script tries
                      "mkqnximage --getip" in --vm-dir.
  --vm-dir DIR         mkqnximage VM directory. Default: $HOME/qnx-vm
  --remote-dir DIR     Destination on QNX. Default: /tmp/subspace-client-test
  --deploy-dir DIR     Local staging directory. Default:
                      $HOME/qnx-deploy/client-test
  --ssh-key PATH       SSH private key. Default: $HOME/.ssh/id_ed25519
  --user USER          SSH user. Default: root
  --bazel CMD          Bazel command. Default: bazelisk
  --config CONFIG      Bazel config. Default: qnx_x86_64
  --no-build           Do not build first; use existing bazel-bin outputs.
  --no-strip           Do not strip copied QNX binaries/libraries.
  --stage-only         Only create the local bundle; do not copy to QNX.
  -h, --help           Show this help.

The bundle layout is:
  client/client_test
  _solib_x86_64/*.so
  plugins/*.so

Run it on QNX from the bundle root:
  cd /tmp/subspace-client-test
  ./client/client_test
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

quote_sh() {
  printf "'%s'" "$(printf "%s" "$1" | sed "s/'/'\\\\''/g")"
}

extract_ipv4() {
  printf "%s\n" "$1" | grep -Eo '([0-9]{1,3}\.){3}[0-9]{1,3}' | tail -n 1
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-$HOME/qnx-deploy/client-test}"
REMOTE_DIR="${REMOTE_DIR:-/tmp/subspace-client-test}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_USER="${SSH_USER:-root}"
VM_DIR="${VM_DIR:-$HOME/qnx-vm}"
BAZEL="${BAZEL:-bazelisk}"
CONFIG="${CONFIG:-qnx_x86_64}"
QNX_IP="${QNX_IP:-}"
BUILD=1
COPY_TO_QNX=1
STRIP=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --qnx-ip)
      QNX_IP="${2:?missing value for --qnx-ip}"
      shift 2
      ;;
    --vm-dir)
      VM_DIR="${2:?missing value for --vm-dir}"
      shift 2
      ;;
    --remote-dir)
      REMOTE_DIR="${2:?missing value for --remote-dir}"
      shift 2
      ;;
    --deploy-dir)
      DEPLOY_DIR="${2:?missing value for --deploy-dir}"
      shift 2
      ;;
    --ssh-key)
      SSH_KEY="${2:?missing value for --ssh-key}"
      shift 2
      ;;
    --user)
      SSH_USER="${2:?missing value for --user}"
      shift 2
      ;;
    --bazel)
      BAZEL="${2:?missing value for --bazel}"
      shift 2
      ;;
    --config)
      CONFIG="${2:?missing value for --config}"
      shift 2
      ;;
    --no-build)
      BUILD=0
      shift
      ;;
    --no-strip)
      STRIP=0
      shift
      ;;
    --stage-only)
      COPY_TO_QNX=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

cd "$REPO_ROOT"

case "$DEPLOY_DIR" in
  ""|"/"|"/tmp"|"/tmp/"|"$HOME")
    die "refusing unsafe --deploy-dir: $DEPLOY_DIR"
    ;;
esac

if [[ "$BUILD" -eq 1 ]]; then
  [[ -n "${QNX_SDP_PATH:-}" ]] ||
    die "set QNX_SDP_PATH first, e.g. export QNX_SDP_PATH=\$HOME/qnx800"

  echo "Building QNX client_test and plugin shared libraries..."
  "$BAZEL" build \
    //client:client_test \
    //plugins:nop_plugin.so \
    //plugins:split_buffer_free_test_plugin.so \
    "--config=${CONFIG}"
fi

CLIENT_TEST="bazel-bin/client/client_test"
SOLIB_DIR="bazel-bin/_solib_x86_64"
NOP_PLUGIN="bazel-bin/plugins/nop_plugin.so"
SPLIT_PLUGIN="bazel-bin/plugins/split_buffer_free_test_plugin.so"

[[ -x "$CLIENT_TEST" ]] || die "missing executable: $CLIENT_TEST"
[[ -d "$SOLIB_DIR" ]] || die "missing shared library directory: $SOLIB_DIR"
[[ -f "$NOP_PLUGIN" ]] || die "missing plugin: $NOP_PLUGIN"
[[ -f "$SPLIT_PLUGIN" ]] || die "missing plugin: $SPLIT_PLUGIN"

if [[ -e "$DEPLOY_DIR" && ! -w "$DEPLOY_DIR" ]]; then
  die "$DEPLOY_DIR is not writable. Remove it or choose --deploy-dir under your home directory."
fi

echo "Creating local bundle: $DEPLOY_DIR"
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR/client" "$DEPLOY_DIR/_solib_x86_64" "$DEPLOY_DIR/plugins"

install -m 0755 "$CLIENT_TEST" "$DEPLOY_DIR/client/client_test"

shopt -s nullglob
solibs=("$SOLIB_DIR"/*.so)
[[ "${#solibs[@]}" -gt 0 ]] || die "no .so files found in $SOLIB_DIR"
for lib in "${solibs[@]}"; do
  cp -L "$lib" "$DEPLOY_DIR/_solib_x86_64/"
done
shopt -u nullglob

cp -L "$NOP_PLUGIN" "$DEPLOY_DIR/plugins/"
cp -L "$SPLIT_PLUGIN" "$DEPLOY_DIR/plugins/"

if [[ "$STRIP" -eq 1 ]]; then
  [[ -n "${QNX_SDP_PATH:-}" ]] ||
    die "set QNX_SDP_PATH or pass --no-strip"
  # Copied Bazel outputs may be read-only; make staged copies writable first.
  chmod -R u+w "$DEPLOY_DIR"
  # shellcheck disable=SC1091
  . "$QNX_SDP_PATH/qnxsdp-env.sh" >/dev/null
  "${QNX_HOST}/usr/bin/ntox86_64-strip" \
    "$DEPLOY_DIR/client/client_test" \
    "$DEPLOY_DIR/plugins/"*.so \
    "$DEPLOY_DIR/_solib_x86_64/"*.so
fi

echo "Bundle contents:"
echo "  $DEPLOY_DIR/client/client_test"
echo "  $DEPLOY_DIR/_solib_x86_64/ (${#solibs[@]} libraries)"
echo "  $DEPLOY_DIR/plugins/"

if [[ "$COPY_TO_QNX" -eq 0 ]]; then
  echo "Stage-only mode. To run after copying manually:"
  echo "  cd $REMOTE_DIR"
  echo "  ./client/client_test"
  exit 0
fi

if [[ -z "$QNX_IP" ]]; then
  [[ -d "$VM_DIR" ]] || die "VM dir not found: $VM_DIR; pass --qnx-ip or --vm-dir"
  [[ -n "${QNX_SDP_PATH:-}" ]] ||
    die "set QNX_SDP_PATH or pass --qnx-ip so the script does not need mkqnximage"

  echo "Discovering QNX VM IP from $VM_DIR..."
  # shellcheck disable=SC1091
  QNX_IP="$(extract_ipv4 "$(cd "$VM_DIR" && . "$QNX_SDP_PATH/qnxsdp-env.sh" >/dev/null && mkqnximage --getip)")"
  [[ -n "$QNX_IP" ]] || die "mkqnximage --getip returned an empty IP address"
else
  raw_qnx_ip="$QNX_IP"
  QNX_IP="$(extract_ipv4 "$raw_qnx_ip")"
  [[ -n "$QNX_IP" ]] || die "--qnx-ip did not contain an IPv4 address: $raw_qnx_ip"
fi

ssh_opts=()
if [[ -f "$SSH_KEY" ]]; then
  ssh_opts=(-i "$SSH_KEY")
else
  echo "warning: SSH key not found at $SSH_KEY; using ssh defaults" >&2
fi

remote="${SSH_USER}@${QNX_IP}"
quoted_remote_dir="$(quote_sh "$REMOTE_DIR")"
remote_parent="$(dirname "$REMOTE_DIR")"
quoted_remote_parent="$(quote_sh "$remote_parent")"

echo "Remote filesystem before copy:"
ssh "${ssh_opts[@]}" "$remote" "mkdir -p $quoted_remote_parent && df -k $quoted_remote_parent || true"

echo "Streaming bundle to ${remote}:${REMOTE_DIR}"
if ! tar -C "$DEPLOY_DIR" -cf - . | ssh "${ssh_opts[@]}" "$remote" "\
  rm -rf $quoted_remote_dir && \
  mkdir -p $quoted_remote_dir && \
  tar -xf - -C $quoted_remote_dir && \
  chmod +x $quoted_remote_dir/client/client_test && \
  df -k $quoted_remote_parent || true"; then
  die "copy failed. Check free space on QNX with: ssh ${ssh_opts[*]} $remote 'df -k $quoted_remote_parent'. Try a larger filesystem or a different --remote-dir."
fi

echo
echo "Copied successfully."
echo "Run on QNX:"
echo "  ssh ${ssh_opts[*]} $remote"
echo "  cd $REMOTE_DIR"
echo "  ./client/client_test"
