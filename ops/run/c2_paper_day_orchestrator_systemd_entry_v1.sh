#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
RELEASE_MANIFEST="${REPO_ROOT}/release_manifest.v1.json"
RUNTIME_DATA_ROOT="/home/node/constellation_runtime_data"
RUNTIME_TRUTH_ROOT="${RUNTIME_DATA_ROOT}/truth"

cd "${REPO_ROOT}"

DAY="$(TZ=America/New_York date +%F)"
if [[ ! -f "${RELEASE_MANIFEST}" ]]; then
  echo "FAIL: validated paper-day execution requires release manifest: ${RELEASE_MANIFEST}" >&2
  exit 2
fi
SHA="$(jq -r '.git_sha // empty' "${RELEASE_MANIFEST}")"
if [[ -z "${SHA}" || "${SHA}" == "null" ]]; then
  echo "FAIL: release manifest missing git_sha: ${RELEASE_MANIFEST}" >&2
  exit 2
fi
PY="${PYTHON_BIN_OVERRIDE:-python3}"

echo "DAY_UTC=${DAY}"
echo "PRODUCER_GIT_SHA=${SHA}"
echo "PYTHON=${PY}"

command -v "${PY}" >/dev/null 2>&1
exec "${PY}" "${REPO_ROOT}/ops/tools/run_paper_session_admission_v1.py" \
  --day_utc "${DAY}" \
  --input_day_utc "${DAY}" \
  --truth_root "${RUNTIME_TRUTH_ROOT}" \
  --symbol SPY \
  --execute YES
