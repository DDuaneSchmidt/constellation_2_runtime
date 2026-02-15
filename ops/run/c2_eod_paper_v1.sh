#!/usr/bin/bash
set -euo pipefail

cd /home/node/constellation_2_runtime

VENV_PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"

DAY_UTC="$(/bin/date -u -d "yesterday" +%F)"

# Bundle completion proof (immutable outputs exist -> DO NOT rerun bundle)
NAV_JSON="/home/node/constellation_2_runtime/constellation_2/runtime/truth/accounting_v1/nav/${DAY_UTC}/nav.json"

# Producer sha: if day outputs exist, lock to the day’s existing cash_ledger producer sha; else use HEAD.
CASH_LEDGER_SNAP="/home/node/constellation_2_runtime/constellation_2/runtime/truth/cash_ledger_v1/snapshots/${DAY_UTC}/cash_ledger_snapshot.v1.json"
if test -f "$CASH_LEDGER_SNAP"; then
  GIT_SHA="$("$VENV_PY" -c 'import json,sys; o=json.load(open(sys.argv[1],"r",encoding="utf-8")); print(o.get("producer",{}).get("git_sha",""))' "$CASH_LEDGER_SNAP")"
  if test -z "$GIT_SHA"; then
    echo "FAIL: existing cash_ledger snapshot missing producer.git_sha: $CASH_LEDGER_SNAP" >&2
    exit 2
  fi
else
  GIT_SHA="$(/usr/bin/git rev-parse HEAD)"
fi

PRODUCED_UTC="${DAY_UTC}T23:59:59Z"
SEED="C2_EOD_V1|day_utc=${DAY_UTC}|git_sha=${GIT_SHA}"

echo "C2_EOD_V1_START day_utc=${DAY_UTC} produced_utc=${PRODUCED_UTC} git_sha=${GIT_SHA}"

# --- Bundle F->G (ONLY if not already completed) ---
if test -f "$NAV_JSON"; then
  echo "C2_EOD_V1_SKIP bundle_f_to_g: accounting nav already exists for day_utc=${DAY_UTC}"
else
  OP_STMT="/home/node/constellation_2_runtime/constellation_2/runtime/truth/operator_inputs_v1/cash_ledger_operator_statements/${DAY_UTC}/operator_statement.v1.json"
  if test ! -f "$OP_STMT"; then
    echo "FAIL: missing operator_statement.v1.json for day_utc=${DAY_UTC}" >&2
    echo "FAIL: expected: $OP_STMT" >&2
    exit 2
  fi

  "$VENV_PY" -m constellation_2.phaseG.bundles.run.run_bundle_f_to_g_day_v1 \
    --day_utc "$DAY_UTC" \
    --producer_git_sha "$GIT_SHA" \
    --producer_repo "constellation_2_runtime" \
    --operator_statement_json "$OP_STMT"
fi

# --- Monitoring nav series (immutable) ---
NAV_SERIES_OUT="/home/node/constellation_2_runtime/constellation_2/runtime/truth/monitoring_v1/nav_series/${DAY_UTC}/portfolio_nav_series.v1.json"
if test -f "$NAV_SERIES_OUT"; then
  echo "C2_EOD_V1_SKIP nav_series: already exists for day_utc=${DAY_UTC}"
else
  "$VENV_PY" -m constellation_2.phaseJ.monitoring.run.run_portfolio_nav_series_day_v1 \
    --day_utc "$DAY_UTC" \
    --window_days 30
fi

# --- Daily snapshot v2 (immutable) ---
SNAP_OUT="/home/node/constellation_2_runtime/constellation_2/runtime/truth/reports/daily_portfolio_snapshot_v2_$(echo "$DAY_UTC" | tr -d '-').json"
if test -f "$SNAP_OUT"; then
  echo "C2_EOD_V1_SKIP daily_snapshot: already exists for day_utc=${DAY_UTC}"
else
  "$VENV_PY" -m constellation_2.phaseJ.reporting.daily_snapshot_v1 \
    --day_utc "$DAY_UTC" \
    --produced_utc "$PRODUCED_UTC" \
    --seed "$SEED" \
    --allow_degraded_report true
fi

echo "C2_EOD_V1_OK day_utc=${DAY_UTC}"
