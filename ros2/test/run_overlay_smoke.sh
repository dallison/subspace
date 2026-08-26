#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROS2_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="${1:-$(pwd)}"

if [[ ! -f "${WORKSPACE_DIR}/install/setup.bash" ]]; then
  echo "usage: $0 /path/to/jazzy_overlay_workspace" >&2
  echo "missing ${WORKSPACE_DIR}/install/setup.bash" >&2
  exit 2
fi

# shellcheck source=/dev/null
set +u
source "${WORKSPACE_DIR}/install/setup.bash"
set -u

export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
export CYCLONEDDS_URI="file://${ROS2_DIR}/cyclonedds_subspace.xml"

SUBSPACE_SERVER="${WORKSPACE_DIR}/install/subspace_vendor/lib/subspace_vendor/subspace_server"
if [[ ! -x "${SUBSPACE_SERVER}" ]]; then
  echo "missing ${SUBSPACE_SERVER}; build subspace_vendor first" >&2
  exit 2
fi

LOG_DIR="${WORKSPACE_DIR}/log/subspace_overlay_smoke"
mkdir -p "${LOG_DIR}"

SERVICE_PID=""
LOANED_LISTENER_PID=""
"${SUBSPACE_SERVER}" >"${LOG_DIR}/subspace_server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  if [[ -n "${LOANED_LISTENER_PID}" ]]; then
    kill "${LOANED_LISTENER_PID}" >/dev/null 2>&1 || true
    wait "${LOANED_LISTENER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${SERVICE_PID}" ]]; then
    kill "${SERVICE_PID}" >/dev/null 2>&1 || true
    wait "${SERVICE_PID}" >/dev/null 2>&1 || true
  fi
  kill "${SERVER_PID}" >/dev/null 2>&1 || true
  wait "${SERVER_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 1

timeout 12 ros2 run demo_nodes_cpp listener >"${LOG_DIR}/listener.log" 2>&1 &
LISTENER_PID=$!
sleep 2

timeout 6 ros2 run demo_nodes_cpp talker >"${LOG_DIR}/talker.log" 2>&1 || true
wait "${LISTENER_PID}" || true

if ! grep -q "I heard" "${LOG_DIR}/listener.log"; then
  echo "listener did not receive talker samples" >&2
  echo "logs are in ${LOG_DIR}" >&2
  exit 1
fi

timeout 12 ros2 run demo_nodes_cpp add_two_ints_server >"${LOG_DIR}/service_server.log" 2>&1 &
SERVICE_PID=$!
sleep 2

timeout 8 ros2 run demo_nodes_cpp add_two_ints_client >"${LOG_DIR}/service_client.log" 2>&1 || true
kill "${SERVICE_PID}" >/dev/null 2>&1 || true
wait "${SERVICE_PID}" >/dev/null 2>&1 || true

if ! grep -q "Result of add_two_ints" "${LOG_DIR}/service_client.log"; then
  echo "service client did not receive a response" >&2
  echo "logs are in ${LOG_DIR}" >&2
  exit 1
fi

timeout 12 ros2 run demo_nodes_cpp listener >"${LOG_DIR}/loaned_listener.log" 2>&1 &
LOANED_LISTENER_PID=$!
sleep 2

timeout 6 ros2 run demo_nodes_cpp talker_loaned_message >"${LOG_DIR}/loaned_talker.log" 2>&1 || true
wait "${LOANED_LISTENER_PID}" || true

if ! grep -q "I heard" "${LOG_DIR}/loaned_listener.log"; then
  echo "loaned-message listener did not receive samples" >&2
  echo "logs are in ${LOG_DIR}" >&2
  exit 1
fi

echo "Subspace overlay smoke test passed"
echo "logs are in ${LOG_DIR}"
