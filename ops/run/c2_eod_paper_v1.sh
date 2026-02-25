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

# --- EOD certificate (always written; PASS only if required outputs exist) ---
CERT_DIR="/home/node/constellation_2_runtime/constellation_2/runtime/truth/reports/eod_run_certificate_v1/${DAY_UTC}"
CERT_PATH="${CERT_DIR}/eod_run_certificate.v1.json"
STARTED_UTC="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"

NAV_SERIES_OUT="/home/node/constellation_2_runtime/constellation_2/runtime/truth/monitoring_v1/nav_series/${DAY_UTC}/portfolio_nav_series.v1.json"
SNAP_OUT="/home/node/constellation_2_runtime/constellation_2/runtime/truth/reports/daily_portfolio_snapshot_v2_$(echo "$DAY_UTC" | tr -d '-').json"

_write_eod_cert() {
  local rc="$1"
  local ended_utc="$2"
  local err_summary="$3"

  local status="FAIL"
  if test "$rc" -eq 0; then
    status="PASS"
  fi

  env \
    C2_DAY_UTC="$DAY_UTC" \
    C2_CERT_PATH="$CERT_PATH" \
    C2_NAV_JSON="$NAV_JSON" \
    C2_NAV_SERIES_OUT="$NAV_SERIES_OUT" \
    C2_SNAP_OUT="$SNAP_OUT" \
    C2_GIT_SHA="$GIT_SHA" \
    C2_CERT_STATUS="$status" \
    C2_EXIT_CODE="$rc" \
    C2_STARTED_UTC="$STARTED_UTC" \
    C2_ENDED_UTC="$ended_utc" \
    C2_SEED="$SEED" \
    C2_ERROR_SUMMARY="$err_summary" \
    "$VENV_PY" -c '
import json, os
from pathlib import Path
from datetime import datetime, timezone

def iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

day = os.environ["C2_DAY_UTC"]
out_path = Path(os.environ["C2_CERT_PATH"])
out_path.parent.mkdir(parents=True, exist_ok=True)

expected = [os.environ["C2_NAV_JSON"], os.environ["C2_NAV_SERIES_OUT"], os.environ["C2_SNAP_OUT"]]
observed = [p for p in expected if Path(p).is_file()]
missing = [p for p in expected if not Path(p).is_file()]

doc = {
  "schema_id": "eod_run_certificate",
  "schema_version": 1,
  "day_utc": day,
  "produced_utc": iso_now(),
  "producer": {
    "repo": "constellation_2_runtime",
    "module": "ops/run/c2_eod_paper_v1.sh",
    "git_sha": os.environ.get("C2_GIT_SHA", "UNKNOWN"),
  },
  "status": os.environ["C2_CERT_STATUS"],
  "run": {
    "exit_code": int(os.environ["C2_EXIT_CODE"]),
    "started_utc": os.environ["C2_STARTED_UTC"],
    "ended_utc": os.environ["C2_ENDED_UTC"],
    "seed": os.environ["C2_SEED"],
    "error_summary": os.environ.get("C2_ERROR_SUMMARY",""),
  },
  "artifacts": {
    "expected_outputs": expected,
    "observed_outputs": observed,
    "missing_outputs": missing,
  }
}

data = json.dumps(doc, indent=2, sort_keys=True) + "\n"
tmp = out_path.with_suffix(out_path.suffix + ".tmp")
tmp.write_text(data, encoding="utf-8")
os.replace(str(tmp), str(out_path))
print(f"OK: EOD_RUN_CERT_WRITTEN day_utc={day} status={doc['status']} path={out_path}")
'
}

_on_exit() {
  local rc="$?"
  local ended_utc
  ended_utc="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"

  local err_summary=""
  if test "$rc" -ne 0; then
    err_summary="rc=${rc} last_cmd=${BASH_COMMAND}"
  fi

  # Always attempt to write certificate; never block process exit if cert writing fails.
  set +e
  _write_eod_cert "$rc" "$ended_utc" "$err_summary" >/dev/null 2>&1
  set -e

  return "$rc"
}
trap _on_exit EXIT

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
if test -f "$NAV_SERIES_OUT"; then
  echo "C2_EOD_V1_SKIP nav_series: already exists for day_utc=${DAY_UTC}"
else
  "$VENV_PY" -m constellation_2.phaseJ.monitoring.run.run_portfolio_nav_series_day_v1 \
    --day_utc "$DAY_UTC" \
    --window_days 30
fi

# --- Daily snapshot v2 (immutable) ---
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
