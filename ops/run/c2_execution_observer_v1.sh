#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
if [[ -n "${PYTHON_BIN_OVERRIDE:-}" ]]; then
  PY="${PYTHON_BIN_OVERRIDE}"
elif [[ -x "${REPO_ROOT}/.venv_c2/bin/python" ]]; then
  PY="${REPO_ROOT}/.venv_c2/bin/python"
else
  PY="python3"
fi
ENTRYPOINT_NAME="c2_execution_observer_v1.sh"
ENTRYPOINT_PATH="${REPO_ROOT}/ops/run/c2_execution_observer_v1.sh"
SERVICE_NAME="c2-execution-observer.service"
RUN_ID=""
CHILD_PID=""
STARTUP_ID=""
STARTUP_IDENTITY_PATH=""
LIFECYCLE_START_RECEIPT_PATH=""
LIFECYCLE_TERMINATION_SIGNAL=""
LIFECYCLE_LAUNCH_PHASE="PRELAUNCH"
LIFECYCLE_STOP_RECORDED=0

_runtime_lifecycle_cleanup() {
  local exit_rc=$?
  local stop_rc=0
  local stop_output=""
  if [[ -n "${RUN_ID:-}" && "${LIFECYCLE_STOP_RECORDED:-0}" != "1" ]]; then
    if stop_output="$("${PY}" "${REPO_ROOT}/ops/tools/run_runtime_lifecycle_v1.py" record-stop \
      --entrypoint_name "${ENTRYPOINT_NAME}" \
      --entrypoint_path "${ENTRYPOINT_PATH}" \
      --service_name "${SERVICE_NAME}" \
      --launcher_pid "$$" \
      --run_id "${RUN_ID}" \
      --launch_phase "${LIFECYCLE_LAUNCH_PHASE}" \
      --wrapper_exit_code "${exit_rc}" \
      --termination_signal "${LIFECYCLE_TERMINATION_SIGNAL}" 2>&1)"; then
      stop_rc=0
    else
      stop_rc=$?
    fi
    if [[ -n "${stop_output}" ]]; then
      echo "${stop_output}"
    fi
    if (( stop_rc == 0 )); then
      LIFECYCLE_STOP_RECORDED=1
    fi
  fi
  if (( stop_rc != 0 )); then
    echo "FAIL: runtime lifecycle stop finalization failed for ${SERVICE_NAME}" >&2
    exit 2
  fi
  return "${exit_rc}"
}

_runtime_lifecycle_forward_signal() {
  local signal_name="$1"
  LIFECYCLE_TERMINATION_SIGNAL="${signal_name}"
  if [[ -n "${CHILD_PID:-}" ]]; then
    kill "-${signal_name}" "${CHILD_PID}" >/dev/null 2>&1 || true
  fi
}

cd "${REPO_ROOT}"

command -v "${PY}" >/dev/null 2>&1

HOSTED_PREFLIGHT_OUTPUT="$("${PY}" "${REPO_ROOT}/ops/tools/run_single_node_hosted_preflight_v1.py" \
  --entrypoint_name "${ENTRYPOINT_NAME}" \
  --entrypoint_path "${ENTRYPOINT_PATH}" \
  --service_name "${SERVICE_NAME}" \
  --requested_python "${PY}" \
  --required_module ib_insync \
  --required_module ibapi)"
if [[ -n "${HOSTED_PREFLIGHT_OUTPUT}" ]]; then
  echo "${HOSTED_PREFLIGHT_OUTPUT}"
fi

RUNTIME_IDENTITY_JSON="$("${PY}" -c 'import json; from pathlib import Path; from constellation_2.common.runtime_identity_v1 import load_active_runtime_identity_snapshot_v1; print(json.dumps(load_active_runtime_identity_snapshot_v1(repo_root=Path.cwd().resolve())))')"
AUTHORITATIVE_REPO_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.authoritative_repo_root')"
HOST="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.resolved_execution_identity.host')"
PORT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.resolved_execution_identity.port')"
CLIENT_ID="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.resolved_execution_identity.client_id_observer')"
TRUTH_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.resolved_execution_roots.execution_root_path')"
RESOLVED_ENVIRONMENT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.resolved_execution_identity.environment')"
RESOLVED_SLEEVE_ID="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.resolved_execution_identity.sleeve_id')"

for required_value in "${AUTHORITATIVE_REPO_ROOT}" "${HOST}" "${PORT}" "${CLIENT_ID}" "${TRUTH_ROOT}" "${RESOLVED_ENVIRONMENT}" "${RESOLVED_SLEEVE_ID}"; do
  if [[ -z "${required_value}" || "${required_value}" == "null" ]]; then
    echo "FAIL: governed execution observer config incomplete" >&2
    exit 2
  fi
done
if [[ "$(readlink -f "${REPO_ROOT}")" != "$(readlink -f "${AUTHORITATIVE_REPO_ROOT}")" ]]; then
  echo "FAIL: execution observer wrapper must run from ${AUTHORITATIVE_REPO_ROOT}, got ${REPO_ROOT}" >&2
  exit 2
fi
if [[ ! -d "${TRUTH_ROOT}" ]]; then
  echo "FAIL: governed execution observer truth root missing: ${TRUTH_ROOT}" >&2
  exit 2
fi

LIFECYCLE_ADMISSION_RC=0
LIFECYCLE_ADMISSION_OUTPUT=""
if LIFECYCLE_ADMISSION_OUTPUT="$("${PY}" "${REPO_ROOT}/ops/tools/run_runtime_lifecycle_v1.py" admit \
  --entrypoint_name "${ENTRYPOINT_NAME}" \
  --entrypoint_path "${ENTRYPOINT_PATH}" \
  --service_name "${SERVICE_NAME}" \
  --launcher_pid "$$" 2>&1)"; then
  LIFECYCLE_ADMISSION_RC=0
else
  LIFECYCLE_ADMISSION_RC=$?
fi
if [[ -n "${LIFECYCLE_ADMISSION_OUTPUT}" ]]; then
  echo "${LIFECYCLE_ADMISSION_OUTPUT}"
fi
if (( LIFECYCLE_ADMISSION_RC != 0 )); then
  exit "${LIFECYCLE_ADMISSION_RC}"
fi
RUN_ID="$(printf '%s\n' "${LIFECYCLE_ADMISSION_OUTPUT}" | jq -r '.run_id // empty' 2>/dev/null || true)"
if [[ -z "${RUN_ID}" ]]; then
  echo "FAIL: runtime lifecycle admission did not return run_id" >&2
  exit 2
fi
trap _runtime_lifecycle_cleanup EXIT
trap '_runtime_lifecycle_forward_signal TERM' TERM
trap '_runtime_lifecycle_forward_signal INT' INT

STARTUP_IDENTITY_OUTPUT="$("${PY}" "${REPO_ROOT}/ops/tools/run_runtime_startup_identity_v1.py" \
  --entrypoint_name "${ENTRYPOINT_NAME}" \
  --entrypoint_path "${ENTRYPOINT_PATH}" \
  --service_name "${SERVICE_NAME}" \
  --requested_python "${PY}")"
if [[ -n "${STARTUP_IDENTITY_OUTPUT}" ]]; then
  echo "${STARTUP_IDENTITY_OUTPUT}"
fi
STARTUP_IDENTITY_PATH="$(printf '%s\n' "${STARTUP_IDENTITY_OUTPUT}" | jq -r '.path // empty' 2>/dev/null || true)"
STARTUP_ID="$(printf '%s\n' "${STARTUP_IDENTITY_OUTPUT}" | jq -r '.startup_id // empty' 2>/dev/null || true)"
if [[ -z "${STARTUP_IDENTITY_PATH}" || -z "${STARTUP_ID}" ]]; then
  echo "FAIL: runtime startup identity emission did not return startup receipt path and startup_id" >&2
  exit 2
fi
LIFECYCLE_RECORD_OUTPUT="$("${PY}" "${REPO_ROOT}/ops/tools/run_runtime_lifecycle_v1.py" record-start \
  --entrypoint_name "${ENTRYPOINT_NAME}" \
  --entrypoint_path "${ENTRYPOINT_PATH}" \
  --service_name "${SERVICE_NAME}" \
  --launcher_pid "$$" \
  --run_id "${RUN_ID}" \
  --startup_id "${STARTUP_ID}" \
  --startup_receipt_path "${STARTUP_IDENTITY_PATH}")"
if [[ -n "${LIFECYCLE_RECORD_OUTPUT}" ]]; then
  echo "${LIFECYCLE_RECORD_OUTPUT}"
fi
LIFECYCLE_START_RECEIPT_PATH="$(printf '%s\n' "${LIFECYCLE_RECORD_OUTPUT}" | jq -r '.path // empty' 2>/dev/null || true)"
if [[ -z "${LIFECYCLE_START_RECEIPT_PATH}" ]]; then
  echo "FAIL: runtime lifecycle start recording did not return receipt path" >&2
  exit 2
fi

if "${PY}" "${REPO_ROOT}/ops/ib/c2_execution_observer_v1.py" \
  --repo-root "${REPO_ROOT}" \
  --truth_root "${TRUTH_ROOT}" \
  --host "${HOST}" \
  --port "${PORT}" \
  --client-id "${CLIENT_ID}" \
  --poll-seconds 10 \
  --environment "${RESOLVED_ENVIRONMENT}" \
  --sleeve-id "${RESOLVED_SLEEVE_ID}" &
then
  CHILD_PID=$!
  LIFECYCLE_LAUNCH_PHASE="RUNTIME"
else
  echo "FAIL: unable to launch canonical execution observer" >&2
  exit 2
fi
if wait "${CHILD_PID}"; then
  OBSERVER_RC=0
else
  OBSERVER_RC=$?
fi
CHILD_PID=""
exit "${OBSERVER_RC}"
