#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

DAY="$(TZ=America/New_York date +%F)"
SHA="$(/usr/bin/git rev-parse HEAD)"
PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"
IB_ACCOUNT="$("${PY}" -c 'from pathlib import Path; import sys; sys.path.insert(0, "/home/node/constellation_2_runtime"); from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry; print(resolve_single_paper_ib_account_from_sleeve_registry(Path("/home/node/constellation_2_runtime").resolve()))')"
OPERATOR_STATEMENT_PATH="/home/node/constellation_2_runtime/constellation_2/operator_inputs/cash_ledger_operator_statements/${DAY}/operator_statement.v1.json"

echo "DAY_UTC=${DAY}"
echo "PRODUCER_GIT_SHA=${SHA}"
echo "PYTHON=${PY}"
echo "IB_ACCOUNT=${IB_ACCOUNT}"
echo "OPERATOR_STATEMENT_PATH=${OPERATOR_STATEMENT_PATH}"

test -x "${PY}"

if [[ ! -f "${OPERATOR_STATEMENT_PATH}" ]]; then
  echo "PRESTART_BLOCKED_OPERATOR_INPUT_MISSING day_utc=${DAY} ib_account=${IB_ACCOUNT} required_path=${OPERATOR_STATEMENT_PATH}" >&2
  exit 3
fi

"${PY}" ops/tools/ensure_cash_ledger_operator_statement_v1.py \
  --day_utc "${DAY}" \
  --ib_account "${IB_ACCOUNT}" \
  --mode SEED_100K \
  --allow_create NO

exec "${PY}" ops/tools/run_c2_multi_sleeve_orchestrator_v1.py \
  --day_utc "${DAY}" \
  --input_day_utc "${DAY}" \
  --symbol SPY
