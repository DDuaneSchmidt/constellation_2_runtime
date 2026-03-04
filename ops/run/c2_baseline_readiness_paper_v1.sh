#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

DAY="$(TZ=America/New_York date +%F)"
PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"
SHA="$(/usr/bin/git rev-parse HEAD)"

TRUTH="constellation_2/runtime/truth"
OP_STMT="constellation_2/operator_inputs/cash_ledger_operator_statements/${DAY}/operator_statement.v1.json"
NAV_PATH="${TRUTH}/accounting_v2/nav/${DAY}/nav.v2.json"
READY_DIR="${TRUTH}/readiness_v1/baseline_ready/${DAY}"
READY_PATH="${READY_DIR}/baseline_ready.v1.json"
POS_OUT="${TRUTH}/positions_v1/snapshots/${DAY}/positions_snapshot.v2.json"
CASH_OUT="${TRUTH}/cash_ledger_v1/snapshots/${DAY}/cash_ledger_snapshot.v1.json"


echo "DAY_UTC=${DAY}"
echo "PRODUCER_GIT_SHA=${SHA}"

echo "=== STEP: ensure operator statement (DUO847203) ==="
"${PY}" ops/tools/ensure_cash_ledger_operator_statement_v1.py \
  --day_utc "${DAY}" \
  --ib_account DUO847203 \
  --mode SEED_100K \
  --allow_create YES
test -f "${OP_STMT}"

echo "=== STEP: ensure submissions dir invariant ==="
mkdir -p "${TRUTH}/execution_evidence_v1/submissions/${DAY}"

echo "=== STEP: positions snapshot ==="
echo "=== STEP: positions snapshot (idempotent) ==="
if [ -f "${POS_OUT}" ]; then
  echo "SKIP: positions snapshot exists ${POS_OUT}"
else
  "${PY}" -m constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2 \
    --day_utc "${DAY}" \
    --producer_git_sha "${SHA}" \
    --producer_repo constellation_2_runtime
fi

echo "=== STEP: cash ledger snapshot (idempotent) ==="
if [ -f "${CASH_OUT}" ]; then
  echo "SKIP: cash ledger snapshot exists ${CASH_OUT}"
else
  "${PY}" -m constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1 \
    --day_utc "${DAY}" \
    --operator_statement_json "${OP_STMT}" \
    --producer_repo constellation_2_runtime \
    --producer_git_sha "${SHA}"
fi
echo "=== STEP: accounting NAV v2 ==="
"${PY}" ops/tools/run_accounting_nav_v2_day_v1.py \
  --day_utc "${DAY}" \
  --producer_repo constellation_2_runtime \
  --producer_git_sha "${SHA}"

echo "=== STEP: verify NAV ACTIVE and >0 ==="
python3 -c "import json; p='${NAV_PATH}'; o=json.load(open(p,'r',encoding='utf-8')); nav=o.get('nav') or {}; \
st=str(o.get('status') or ''); nt=nav.get('nav_total'); ct=nav.get('cash_total'); \
print('status=',st); print('nav_total=',nt); print('cash_total=',ct); \
assert st=='ACTIVE', 'NAV_NOT_ACTIVE'; \
assert isinstance(nt,int) and nt>0, 'NAV_TOTAL_NOT_POSITIVE'; \
assert isinstance(ct,int) and ct>0, 'CASH_TOTAL_NOT_POSITIVE'"

echo "=== STEP: publish readiness (attempt + pointer index + canonical) ==="
ATTEMPT_ID="${DAY}__R$(date -u +%H%M%S)__${SHA:0:7}__${RANDOM}${RANDOM}"

"${PY}" /home/node/constellation_2_runtime/ops/run/c2_baseline_readiness_publish_v1.py \
  --day_utc "${DAY}" \
  --attempt_id "${ATTEMPT_ID}" \
  --ib_account DUO847203 \
  --nav_path "/home/node/constellation_2_runtime/${NAV_PATH}" \
  --producer_git_sha "${SHA}"

echo "BASELINE_READINESS_OK day=${DAY}"
