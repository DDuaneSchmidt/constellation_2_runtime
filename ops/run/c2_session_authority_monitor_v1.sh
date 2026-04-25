#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
PY="${PYTHON_BIN_OVERRIDE:-python3}"

cd "${REPO_ROOT}"

command -v "${PY}" >/dev/null 2>&1

RUNTIME_IDENTITY_JSON="$("${PY}" -c 'import json; from pathlib import Path; from constellation_2.common.runtime_identity_v1 import load_active_runtime_identity_snapshot_v1; print(json.dumps(load_active_runtime_identity_snapshot_v1(repo_root=Path.cwd().resolve())))')"
AUTHORITATIVE_REPO_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.authoritative_repo_root')"
CANONICAL_TRUTH_ROOT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.canonical_truth_root')"
IDENTITY_ENVIRONMENT="$(printf '%s' "${RUNTIME_IDENTITY_JSON}" | jq -r '.runtime_environment')"
ENVIRONMENT="${C2_SESSION_AUTHORITY_ENVIRONMENT:-${IDENTITY_ENVIRONMENT}}"
RUNTIME_TRUTH_ROOT="${C2_TRUTH_ROOT:-${CANONICAL_TRUTH_ROOT}}"
if [[ -z "${AUTHORITATIVE_REPO_ROOT}" || "${AUTHORITATIVE_REPO_ROOT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve authoritative_repo_root" >&2
  exit 2
fi
if [[ -z "${CANONICAL_TRUTH_ROOT}" || "${CANONICAL_TRUTH_ROOT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve canonical_truth_root" >&2
  exit 2
fi
if [[ -z "${IDENTITY_ENVIRONMENT}" || "${IDENTITY_ENVIRONMENT}" == "null" ]]; then
  echo "FAIL: runtime identity did not resolve runtime_environment" >&2
  exit 2
fi
if [[ "$(readlink -f "${REPO_ROOT}")" != "$(readlink -f "${AUTHORITATIVE_REPO_ROOT}")" ]]; then
  echo "FAIL: authoritative session authority monitor must run from ${AUTHORITATIVE_REPO_ROOT}, got ${REPO_ROOT}" >&2
  exit 2
fi
if [[ ! -d "${CANONICAL_TRUTH_ROOT}" ]]; then
  echo "FAIL: runtime identity canonical_truth_root missing: ${CANONICAL_TRUTH_ROOT}" >&2
  exit 2
fi
if [[ "$(readlink -f "${RUNTIME_TRUTH_ROOT}")" != "$(readlink -f "${CANONICAL_TRUTH_ROOT}")" ]]; then
  echo "FAIL: session authority monitor requires canonical truth root ${CANONICAL_TRUTH_ROOT}, got ${RUNTIME_TRUTH_ROOT}" >&2
  exit 2
fi
if [[ "${ENVIRONMENT}" != "${IDENTITY_ENVIRONMENT}" ]]; then
  echo "FAIL: session authority monitor environment override must equal runtime identity ${IDENTITY_ENVIRONMENT}, got ${ENVIRONMENT}" >&2
  exit 2
fi

set +e

"${PY}" "${REPO_ROOT}/ops/tools/run_market_calendar_coverage_authority_v1.py" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --mode WRITE
coverage_rc=$?

"${PY}" "${REPO_ROOT}/ops/tools/run_subsystem_authority_v1.py" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --environment "${ENVIRONMENT}" \
  --mode WRITE
subsystem_rc=$?

"${PY}" "${REPO_ROOT}/ops/tools/run_session_authority_status_v1.py" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --environment "${ENVIRONMENT}" \
  --mode WRITE
status_rc=$?

"${PY}" "${REPO_ROOT}/ops/tools/run_session_authority_alert_v1.py" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --environment "${ENVIRONMENT}" \
  --refresh-status \
  --mode WRITE
alert_rc=$?

set -e

exit_rc=0
for rc in "${coverage_rc}" "${subsystem_rc}" "${status_rc}" "${alert_rc}"; do
  if [ "${rc}" -gt "${exit_rc}" ]; then
    exit_rc="${rc}"
  fi
done

exit "${exit_rc}"
