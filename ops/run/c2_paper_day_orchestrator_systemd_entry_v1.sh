#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

# Use NY day key so it matches readiness + avoids UTC midnight trap.
DAY="$(TZ=America/New_York date +%F)"
PRODUCED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SHA="$(/usr/bin/git rev-parse HEAD)"

PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"

echo "DAY_UTC=${DAY}"
echo "PRODUCED_UTC=${PRODUCED_UTC}"
echo "PRODUCER_GIT_SHA=${SHA}"

# Ensure operator statement exists (deterministic 100k seed)
"${PY}" ops/tools/ensure_cash_ledger_operator_statement_v1.py \
  --day_utc "${DAY}" \
  --ib_account DUO847203 \
  --mode SEED_100K \
  --allow_create YES

# Run orchestrator v2 (single-account enforced inside tool)
exec "${PY}" ops/tools/run_c2_paper_day_orchestrator_v2.py \
  --day_utc "${DAY}" \
  --mode PAPER \
  --symbol SPY \
  --ib_account DUO847203 \
  --produced_utc "${PRODUCED_UTC}"
