#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
PY="${PYTHON_BIN_OVERRIDE:-python3}"
ENTRYPOINT_NAME="c2_paper_day_orchestrator_systemd_entry_v1.sh"
ENTRYPOINT_PATH="${REPO_ROOT}/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
SERVICE_NAME="c2-paper-day-orchestrator.service"
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
CONTROL_PLANE_READ_TOOL="${REPO_ROOT}/ops/tools/read_control_plane_surface_v1.py"

HOSTED_PREFLIGHT_OUTPUT="$("${PY}" "${REPO_ROOT}/ops/tools/run_single_node_hosted_preflight_v1.py" \
  --entrypoint_name "${ENTRYPOINT_NAME}" \
  --entrypoint_path "${ENTRYPOINT_PATH}" \
  --service_name "${SERVICE_NAME}" \
  --requested_python "${PY}")"
if [[ -n "${HOSTED_PREFLIGHT_OUTPUT}" ]]; then
  echo "${HOSTED_PREFLIGHT_OUTPUT}"
fi

RUNTIME_IDENTITY_JSON="$("${PY}" -c 'import json; from pathlib import Path; from constellation_2.common.runtime_identity_v1 import load_active_runtime_identity_snapshot_v1; print(json.dumps(load_active_runtime_identity_snapshot_v1(repo_root=Path().resolve())))')"
AUTHORITATIVE_REPO_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.authoritative_repo_root')"
RELEASE_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.release_root')"
RUNTIME_DATA_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.runtime_data_root')"
RUNTIME_TRUTH_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.canonical_truth_root')"
RUNTIME_ENVIRONMENT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.runtime_environment')"
PRIMARY_SLEEVE_ID="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.primary_execution_identity_ref.sleeve_id')"
RUNTIME_IDENTITY_CONTRACT_PATH="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.contract_path')"
RUNTIME_IDENTITY_CONTRACT_SHA256="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.contract_sha256')"
if [[ -z "${AUTHORITATIVE_REPO_ROOT}" || "${AUTHORITATIVE_REPO_ROOT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve authoritative_repo_root" >&2
  exit 2
fi
if [[ -z "${RELEASE_ROOT}" || "${RELEASE_ROOT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve release_root" >&2
  exit 2
fi
if [[ -z "${RUNTIME_DATA_ROOT}" || "${RUNTIME_DATA_ROOT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve runtime_data_root" >&2
  exit 2
fi
if [[ -z "${RUNTIME_TRUTH_ROOT}" || "${RUNTIME_TRUTH_ROOT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve canonical_truth_root" >&2
  exit 2
fi
if [[ -z "${RUNTIME_ENVIRONMENT}" || "${RUNTIME_ENVIRONMENT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve runtime_environment" >&2
  exit 2
fi
if [[ -z "${PRIMARY_SLEEVE_ID}" || "${PRIMARY_SLEEVE_ID}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve primary execution sleeve id" >&2
  exit 2
fi
if [[ -z "${RUNTIME_IDENTITY_CONTRACT_PATH}" || "${RUNTIME_IDENTITY_CONTRACT_PATH}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve contract_path" >&2
  exit 2
fi
if [[ -z "${RUNTIME_IDENTITY_CONTRACT_SHA256}" || "${RUNTIME_IDENTITY_CONTRACT_SHA256}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve contract_sha256" >&2
  exit 2
fi
RUNTIME_AUTHORITY_JSON="$("${PY}" -c 'import json; from constellation_2.common.runtime_authority_bridge_v1 import load_release_current_runtime_authority_v1; print(json.dumps(load_release_current_runtime_authority_v1(caller="ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")))' )"
RUNTIME_AUTHORITY_RELEASE_ROOT="$(printf '%s' "${RUNTIME_AUTHORITY_JSON}" | jq -r '.release_root')"
if [[ -z "${RUNTIME_AUTHORITY_RELEASE_ROOT}" || "${RUNTIME_AUTHORITY_RELEASE_ROOT}" == "null" ]]; then
  echo "FAIL: runtime authority bridge did not resolve release_root" >&2
  exit 2
fi
if [[ "$(readlink -f "${RUNTIME_AUTHORITY_RELEASE_ROOT}")" != "$(readlink -f "${RELEASE_ROOT}")" ]]; then
  echo "FAIL: runtime authority bridge release_root mismatch with runtime identity release_root" >&2
  exit 2
fi
REPO_ROOT_REAL="$(readlink -f "${REPO_ROOT}")"
RELEASE_ROOT_REAL="$(readlink -f "${RELEASE_ROOT}")"
AUTHORITATIVE_REPO_ROOT_REAL="$(readlink -f "${AUTHORITATIVE_REPO_ROOT}")"
if [[ "${REPO_ROOT_REAL}" != "${RELEASE_ROOT_REAL}" && "${REPO_ROOT_REAL}" != "${AUTHORITATIVE_REPO_ROOT_REAL}" ]]; then
  echo "FAIL: validated paper-day execution must run from active release root ${RELEASE_ROOT} or authoritative repo root ${AUTHORITATIVE_REPO_ROOT}, got ${REPO_ROOT}" >&2
  exit 2
fi
if [[ "${REPO_ROOT_REAL}" != "${RELEASE_ROOT_REAL}" ]]; then
  echo "WARN: PAPER_ORCHESTRATOR_RUNNING_FROM_AUTHORITATIVE_REPO_ROOT repo_root=${REPO_ROOT_REAL} release_root=${RELEASE_ROOT_REAL}"
fi
if [[ ! -d "${RUNTIME_DATA_ROOT}" ]]; then
  echo "FAIL: runtime identity runtime_data_root missing: ${RUNTIME_DATA_ROOT}" >&2
  exit 2
fi
if [[ ! -d "${RUNTIME_TRUTH_ROOT}" ]]; then
  echo "FAIL: runtime identity canonical_truth_root missing: ${RUNTIME_TRUTH_ROOT}" >&2
  exit 2
fi

SHA="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || true)"
if [[ -z "${SHA}" || "${SHA}" == "null" ]]; then
  echo "FAIL: unable to resolve authoritative repo git SHA from ${REPO_ROOT}" >&2
  exit 2
fi
ACTIVE_SESSION_PATH=""
STATUS_PATH=""
ALERT_PATH=""
COVERAGE_STATUS_PATH=""
MONITOR_SCRIPT="${REPO_ROOT}/ops/run/c2_session_authority_monitor_v1.sh"
BOOTSTRAP_TOOL="${REPO_ROOT}/ops/tools/run_paper_session_bootstrap_v1.py"

echo "PRODUCER_GIT_SHA=${SHA}"
echo "PYTHON=${PY}"
echo "RUNTIME_ENVIRONMENT=${RUNTIME_ENVIRONMENT}"
echo "PRIMARY_EXECUTION_SLEEVE_ID=${PRIMARY_SLEEVE_ID}"
echo "CANONICAL_MORNING_OPERATIONS_PATH=ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh->ops/tools/run_paper_session_bootstrap_v1.py->ops/tools/run_pre_open_materializer_v1.py->ops/tools/run_session_authority_v1.py->ops/tools/run_day_open_attempt_v1.py->ops/tools/run_c2_multi_sleeve_orchestrator_v1.py->ops/tools/run_c2_paper_day_orchestrator_v2.py"

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

command -v "${PY}" >/dev/null 2>&1
COVERAGE_CHECK_RC=0
COVERAGE_CHECK_OUTPUT=""
echo "STEP=market_calendar_coverage_check"
if COVERAGE_CHECK_OUTPUT="$("${PY}" "${REPO_ROOT}/ops/tools/run_market_calendar_coverage_status_v1.py" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --mode CHECK 2>&1)"; then
  COVERAGE_CHECK_RC=0
else
  COVERAGE_CHECK_RC=$?
fi
if [[ -n "${COVERAGE_CHECK_OUTPUT}" ]]; then
  echo "${COVERAGE_CHECK_OUTPUT}"
fi
echo "MARKET_CALENDAR_COVERAGE_CHECK_RC=${COVERAGE_CHECK_RC}"

BOOTSTRAP_RC=0
BOOTSTRAP_OUTPUT=""
echo "STEP=paper_session_bootstrap"
if BOOTSTRAP_OUTPUT="$("${PY}" "${BOOTSTRAP_TOOL}" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --environment "${RUNTIME_ENVIRONMENT}" 2>&1)"; then
  BOOTSTRAP_RC=0
else
  BOOTSTRAP_RC=$?
fi
if [[ -n "${BOOTSTRAP_OUTPUT}" ]]; then
  echo "${BOOTSTRAP_OUTPUT}"
fi
BOOTSTRAP_STATUS="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.bootstrap_status // empty' 2>/dev/null || true)"
BOOTSTRAP_SEMANTIC_STATUS="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.bootstrap_semantic_status // empty' 2>/dev/null || true)"
BOOTSTRAP_TARGET_DAY="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.target_day // empty' 2>/dev/null || true)"
BOOTSTRAP_ROOT_BLOCKER_CLASS="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.root_blocker_class // empty' 2>/dev/null || true)"
BOOTSTRAP_PRE_OPEN_STATE="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.pre_open_materialization_state // empty' 2>/dev/null || true)"
BOOTSTRAP_PROMOTION_STATE="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.promotion_state // empty' 2>/dev/null || true)"
BOOTSTRAP_PROMOTION_PATH="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.promotion_decision_path // empty' 2>/dev/null || true)"
BOOTSTRAP_ADMISSION_STATUS="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.target_day_admission_status // empty' 2>/dev/null || true)"
BOOTSTRAP_BLOCKER_CHAIN="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '(.blocker_chain // []) | join(",")' 2>/dev/null || true)"
BOOTSTRAP_STOP_SURFACE="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.canonical_stop_surface // empty' 2>/dev/null || true)"
BOOTSTRAP_STOP_ARTIFACT_PATH="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.canonical_stop_artifact_path // empty' 2>/dev/null || true)"
BOOTSTRAP_STOP_REASON_CODES="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '(.canonical_stop_reason_codes // []) | join(",")' 2>/dev/null || true)"
BOOTSTRAP_RUNTIME_PREREQ_STAGE="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.runtime_prerequisite_stage // empty' 2>/dev/null || true)"
BOOTSTRAP_FIRST_PREREQ_ID="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.earliest_failing_prerequisite_id // empty' 2>/dev/null || true)"
BOOTSTRAP_FIRST_PREREQ_OWNER_TOOL="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.earliest_failing_prerequisite_owner_tool // empty' 2>/dev/null || true)"
BOOTSTRAP_FIRST_PREREQ_ARTIFACT_PATH="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.earliest_failing_prerequisite_artifact_path // empty' 2>/dev/null || true)"
BOOTSTRAP_FIRST_PREREQ_BLOCKER_CLASS="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.earliest_failing_prerequisite_blocker_class // empty' 2>/dev/null || true)"
BOOTSTRAP_FIRST_PREREQ_REASON_CODES="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '(.earliest_failing_prerequisite_reason_codes // []) | join(",")' 2>/dev/null || true)"
BOOTSTRAP_FIX_THEN_RERUN_RULE="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.fix_then_rerun_rule // empty' 2>/dev/null || true)"
BOOTSTRAP_DO_NOT_RUN_MANUALLY="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '(.do_not_run_manually // []) | join(",")' 2>/dev/null || true)"
BOOTSTRAP_REPORT_PATH="$(printf '%s\n' "${BOOTSTRAP_OUTPUT}" | jq -r '.path // empty' 2>/dev/null || true)"
echo "BOOTSTRAP_STATUS=${BOOTSTRAP_STATUS}"
echo "BOOTSTRAP_SEMANTIC_STATUS=${BOOTSTRAP_SEMANTIC_STATUS}"
echo "BOOTSTRAP_TARGET_DAY=${BOOTSTRAP_TARGET_DAY}"
echo "BOOTSTRAP_ROOT_BLOCKER_CLASS=${BOOTSTRAP_ROOT_BLOCKER_CLASS}"
echo "BOOTSTRAP_PRE_OPEN_STATE=${BOOTSTRAP_PRE_OPEN_STATE}"
echo "BOOTSTRAP_PROMOTION_STATE=${BOOTSTRAP_PROMOTION_STATE}"
echo "BOOTSTRAP_PROMOTION_PATH=${BOOTSTRAP_PROMOTION_PATH}"
echo "BOOTSTRAP_TARGET_DAY_ADMISSION_STATUS=${BOOTSTRAP_ADMISSION_STATUS}"
echo "BOOTSTRAP_BLOCKER_CHAIN=${BOOTSTRAP_BLOCKER_CHAIN:-NONE}"
echo "BOOTSTRAP_STOP_SURFACE=${BOOTSTRAP_STOP_SURFACE}"
echo "BOOTSTRAP_STOP_ARTIFACT_PATH=${BOOTSTRAP_STOP_ARTIFACT_PATH}"
echo "BOOTSTRAP_STOP_REASON_CODES=${BOOTSTRAP_STOP_REASON_CODES:-NONE}"
echo "BOOTSTRAP_RUNTIME_PREREQ_STAGE=${BOOTSTRAP_RUNTIME_PREREQ_STAGE}"
echo "BOOTSTRAP_FIRST_PREREQ_ID=${BOOTSTRAP_FIRST_PREREQ_ID}"
echo "BOOTSTRAP_FIRST_PREREQ_OWNER_TOOL=${BOOTSTRAP_FIRST_PREREQ_OWNER_TOOL}"
echo "BOOTSTRAP_FIRST_PREREQ_ARTIFACT_PATH=${BOOTSTRAP_FIRST_PREREQ_ARTIFACT_PATH}"
echo "BOOTSTRAP_FIRST_PREREQ_BLOCKER_CLASS=${BOOTSTRAP_FIRST_PREREQ_BLOCKER_CLASS}"
echo "BOOTSTRAP_FIRST_PREREQ_REASON_CODES=${BOOTSTRAP_FIRST_PREREQ_REASON_CODES:-NONE}"
echo "BOOTSTRAP_FIX_THEN_RERUN_RULE=${BOOTSTRAP_FIX_THEN_RERUN_RULE}"
echo "BOOTSTRAP_DO_NOT_RUN_MANUALLY=${BOOTSTRAP_DO_NOT_RUN_MANUALLY}"
echo "BOOTSTRAP_REPORT_PATH=${BOOTSTRAP_REPORT_PATH}"
if (( BOOTSTRAP_RC != 0 )); then
  echo "FAIL: canonical morning bootstrap blocked deeper startup status=${BOOTSTRAP_STATUS:-UNKNOWN} pre_open_state=${BOOTSTRAP_PRE_OPEN_STATE:-UNKNOWN} promotion_state=${BOOTSTRAP_PROMOTION_STATE:-UNKNOWN} admission_status=${BOOTSTRAP_ADMISSION_STATUS:-UNKNOWN} stop_surface=${BOOTSTRAP_STOP_SURFACE:-UNKNOWN} stop_artifact=${BOOTSTRAP_STOP_ARTIFACT_PATH:-UNKNOWN} stop_reason_codes=${BOOTSTRAP_STOP_REASON_CODES:-NONE} earliest_stage=${BOOTSTRAP_RUNTIME_PREREQ_STAGE:-UNKNOWN} earliest_prerequisite=${BOOTSTRAP_FIRST_PREREQ_ID:-UNKNOWN} earliest_owner=${BOOTSTRAP_FIRST_PREREQ_OWNER_TOOL:-UNKNOWN} earliest_artifact=${BOOTSTRAP_FIRST_PREREQ_ARTIFACT_PATH:-UNKNOWN} earliest_blocker_class=${BOOTSTRAP_FIRST_PREREQ_BLOCKER_CLASS:-UNKNOWN} earliest_reason_codes=${BOOTSTRAP_FIRST_PREREQ_REASON_CODES:-NONE} rerun_rule=${BOOTSTRAP_FIX_THEN_RERUN_RULE:-UNKNOWN} do_not_run_manually=${BOOTSTRAP_DO_NOT_RUN_MANUALLY:-NONE} root_blocker_class=${BOOTSTRAP_ROOT_BLOCKER_CLASS:-UNKNOWN} blocker_chain=${BOOTSTRAP_BLOCKER_CHAIN:-NONE} report_path=${BOOTSTRAP_REPORT_PATH}" >&2
  exit "${BOOTSTRAP_RC}"
fi

MONITOR_RC=0
if [[ -x "${MONITOR_SCRIPT}" ]]; then
  if "${MONITOR_SCRIPT}"; then
    MONITOR_RC=0
  else
    MONITOR_RC=$?
  fi
else
  echo "FAIL: session authority monitor script missing or not executable: ${MONITOR_SCRIPT}" >&2
  exit 2
fi

STATUS_JSON=""
if STATUS_JSON="$("${PY}" "${CONTROL_PLANE_READ_TOOL}" --domain session --surface session_authority_status_current --truth_root "${RUNTIME_TRUTH_ROOT}" 2>/dev/null)"; then
  STATUS_PATH="$(printf '%s\n' "${STATUS_JSON}" | jq -r '.path // empty' 2>/dev/null || true)"
  STATUS_SEVERITY="$(printf '%s\n' "${STATUS_JSON}" | jq -r '.payload.status_severity // empty' 2>/dev/null || true)"
  STATUS_BLOCKERS="$(printf '%s\n' "${STATUS_JSON}" | jq -r '(.payload.top_blocker_reason_codes // []) | join(",")' 2>/dev/null || true)"
  echo "SESSION_AUTHORITY_STATUS_SEVERITY=${STATUS_SEVERITY}"
  echo "SESSION_AUTHORITY_STATUS_BLOCKERS=${STATUS_BLOCKERS:-NONE}"
fi

COVERAGE_STATUS_JSON=""
if COVERAGE_STATUS_JSON="$("${PY}" "${CONTROL_PLANE_READ_TOOL}" --domain session --surface market_calendar_coverage_status_current --truth_root "${RUNTIME_TRUTH_ROOT}" 2>/dev/null)"; then
  COVERAGE_STATUS_PATH="$(printf '%s\n' "${COVERAGE_STATUS_JSON}" | jq -r '.path // empty' 2>/dev/null || true)"
  COVERAGE_SEVERITY="$(printf '%s\n' "${COVERAGE_STATUS_JSON}" | jq -r '.payload.severity // empty' 2>/dev/null || true)"
  COVERAGE_REASON_CODES="$(printf '%s\n' "${COVERAGE_STATUS_JSON}" | jq -r '(.payload.reason_codes // []) | join(",")' 2>/dev/null || true)"
  COVERAGE_REQUIRED_TARGET_DAY="$(printf '%s\n' "${COVERAGE_STATUS_JSON}" | jq -r '.payload.required_target_day // empty' 2>/dev/null || true)"
  echo "MARKET_CALENDAR_COVERAGE_SEVERITY=${COVERAGE_SEVERITY}"
  echo "MARKET_CALENDAR_COVERAGE_REASON_CODES=${COVERAGE_REASON_CODES:-NONE}"
  echo "MARKET_CALENDAR_COVERAGE_REQUIRED_TARGET_DAY=${COVERAGE_REQUIRED_TARGET_DAY}"
fi

ALERT_JSON=""
if ALERT_JSON="$("${PY}" "${CONTROL_PLANE_READ_TOOL}" --domain session --surface session_authority_alert_current --truth_root "${RUNTIME_TRUTH_ROOT}" 2>/dev/null)"; then
  ALERT_PATH="$(printf '%s\n' "${ALERT_JSON}" | jq -r '.path // empty' 2>/dev/null || true)"
  ALERT_STATUS="$(printf '%s\n' "${ALERT_JSON}" | jq -r '.payload.alert_status // empty' 2>/dev/null || true)"
  ALERT_SEVERITY="$(printf '%s\n' "${ALERT_JSON}" | jq -r '.payload.severity // empty' 2>/dev/null || true)"
  ALERT_REASON_CODES="$(printf '%s\n' "${ALERT_JSON}" | jq -r '(.payload.alert_reason_codes // []) | join(",")' 2>/dev/null || true)"
  echo "SESSION_AUTHORITY_ALERT_STATUS=${ALERT_STATUS}"
  echo "SESSION_AUTHORITY_ALERT_SEVERITY=${ALERT_SEVERITY}"
  echo "SESSION_AUTHORITY_ALERT_REASON_CODES=${ALERT_REASON_CODES:-NONE}"
fi

ACTIVE_SESSION_JSON="$("${PY}" "${CONTROL_PLANE_READ_TOOL}" --domain session --surface active_session_current --truth_root "${RUNTIME_TRUTH_ROOT}")"
ACTIVE_SESSION_PATH="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.path // empty' 2>/dev/null || true)"
TARGET_DAY="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.target_day // empty' 2>/dev/null || true)"
ACTIVE_DAY="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.active_day // empty' 2>/dev/null || true)"
ROLLOVER_STATUS="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.rollover_status // empty' 2>/dev/null || true)"
ROLLOVER_REASON="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.rollover_reason // empty' 2>/dev/null || true)"
ROLLOVER_REASON_CODE="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.rollover_reason_code // empty' 2>/dev/null || true)"
ROLLOVER_REASON_SUMMARY="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.rollover_reason_summary // empty' 2>/dev/null || true)"
ADMISSION_STATUS="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.target_day_admission_status // empty' 2>/dev/null || true)"
BLOCKED_TARGET_DAY="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.blocked_target_day // empty' 2>/dev/null || true)"
BLOCKED_ADMISSION_REF="$(printf '%s\n' "${ACTIVE_SESSION_JSON}" | jq -r '.payload.blocked_admission_ref // empty' 2>/dev/null || true)"

echo "TARGET_DAY=${TARGET_DAY}"
echo "ACTIVE_DAY=${ACTIVE_DAY}"
echo "ROLLOVER_STATUS=${ROLLOVER_STATUS}"
echo "TARGET_DAY_ADMISSION_STATUS=${ADMISSION_STATUS}"
echo "ROLLOVER_REASON_CODE=${ROLLOVER_REASON_CODE}"

if [[ "${RUNTIME_ENVIRONMENT}" != "PAPER" && "${ADMISSION_STATUS}" != "ADMIT" ]]; then
  echo "FAIL: session authority withheld rollover for target day ${BLOCKED_TARGET_DAY:-${TARGET_DAY}} code=${ROLLOVER_REASON_CODE:-UNKNOWN} summary=${ROLLOVER_REASON_SUMMARY:-${ROLLOVER_REASON}} admission_ref=${BLOCKED_ADMISSION_REF} status_path=${STATUS_PATH} alert_path=${ALERT_PATH}" >&2
  exit 2
fi
if [[ "${RUNTIME_ENVIRONMENT}" == "PAPER" && "${ADMISSION_STATUS}" != "ADMIT" ]]; then
  echo "WARN: PAPER_OPEN_CONTRACT_NON_BLOCKING_ADMISSION status=${ADMISSION_STATUS:-UNKNOWN} code=${ROLLOVER_REASON_CODE:-UNKNOWN} summary=${ROLLOVER_REASON_SUMMARY:-${ROLLOVER_REASON}} admission_ref=${BLOCKED_ADMISSION_REF}"
fi
if [[ -z "${ACTIVE_DAY}" ]]; then
  if [[ "${RUNTIME_ENVIRONMENT}" == "PAPER" ]]; then
    ACTIVE_DAY="${BOOTSTRAP_TARGET_DAY:-${TARGET_DAY}}"
    if [[ -n "${ACTIVE_DAY}" ]]; then
      echo "WARN: PAPER_ACTIVE_DAY_FALLBACK target_day=${ACTIVE_DAY} rollover_status=${ROLLOVER_STATUS:-UNKNOWN} admission_status=${ADMISSION_STATUS:-UNKNOWN}"
    else
      echo "FAIL: paper open day could not be resolved from bootstrap target_day or active session target_day" >&2
      exit 2
    fi
  else
    echo "FAIL: session authority did not publish an active day" >&2
    exit 2
  fi
fi
if [[ "${RUNTIME_ENVIRONMENT}" != "PAPER" && "${ROLLOVER_STATUS}" == "ROLLOVER_WITHHELD" ]]; then
  echo "FAIL: session authority reports withheld rollover code=${ROLLOVER_REASON_CODE:-UNKNOWN} summary=${ROLLOVER_REASON_SUMMARY:-${ROLLOVER_REASON}} admission_ref=${BLOCKED_ADMISSION_REF} status_path=${STATUS_PATH} alert_path=${ALERT_PATH}" >&2
  exit 2
fi

if (( MONITOR_RC != 0 )) && [[ "${STATUS_SEVERITY:-}" == "CRITICAL" ]]; then
  echo "FAIL: session authority monitoring reported CRITICAL severity before orchestrator handoff status_path=${STATUS_PATH} alert_path=${ALERT_PATH}" >&2
  exit "${MONITOR_RC}"
fi

LEDGER_JSON="$("${PY}" "${CONTROL_PLANE_READ_TOOL}" --domain execution --surface paper_session_ledger --truth_root "${RUNTIME_TRUTH_ROOT}" --day_utc "${ACTIVE_DAY}")"
LEDGER_PATH="$(printf '%s\n' "${LEDGER_JSON}" | jq -r '.path // empty' 2>/dev/null || true)"
if [[ -z "${LEDGER_PATH}" ]]; then
  echo "FAIL: admitted active day ledger missing through control-plane read gateway for ${ACTIVE_DAY}" >&2
  exit 2
fi

echo "CANONICAL_EXECUTION_SEQUENCE_OWNER=ops/tools/run_c2_paper_day_orchestrator_v2.py"
echo "CANONICAL_EXECUTION_SEQUENCE_ROUTE=ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh->ops/tools/run_day_open_attempt_v1.py->ops/tools/run_c2_multi_sleeve_orchestrator_v1.py->ops/tools/run_c2_paper_day_orchestrator_v2.py"

if "${PY}" "${REPO_ROOT}/ops/tools/run_day_open_attempt_v1.py" \
  --day_utc "${ACTIVE_DAY}" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --paper_session_ledger_path "${LEDGER_PATH}" \
  --actor_name "${SERVICE_NAME}" \
  --actor_path "${ENTRYPOINT_PATH}" \
  --environment "${RUNTIME_ENVIRONMENT}" \
  --symbol SPY \
  --runtime_run_id "${RUN_ID}" \
  --runtime_identity_contract_path "${RUNTIME_IDENTITY_CONTRACT_PATH}" \
  --runtime_identity_contract_sha256 "${RUNTIME_IDENTITY_CONTRACT_SHA256}" \
  --runtime_startup_identity_receipt_path "${STARTUP_IDENTITY_PATH}" \
  --runtime_lifecycle_start_receipt_path "${LIFECYCLE_START_RECEIPT_PATH}" &
then
  CHILD_PID=$!
  LIFECYCLE_LAUNCH_PHASE="RUNTIME"
else
  echo "FAIL: unable to launch canonical day-open attempt" >&2
  exit 2
fi
if wait "${CHILD_PID}"; then
  DAY_OPEN_RC=0
else
  DAY_OPEN_RC=$?
fi
CHILD_PID=""
exit "${DAY_OPEN_RC}"
