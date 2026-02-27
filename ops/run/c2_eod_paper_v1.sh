#!/usr/bin/bash
set -euo pipefail

cd /home/node/constellation_2_runtime

VENV_PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"

# Allow Instance B to override truth root (service sets C2_TRUTH_ROOT).
TRUTH_ROOT="${C2_TRUTH_ROOT:-/home/node/constellation_2_runtime/constellation_2/runtime/truth}"

DAY_UTC="$(/bin/date -u -d "yesterday" +%F)"

# --- NAV bridge: ensure accounting_v1/nav exists using accounting_v2/nav if needed ---
V1_NAV_JSON="${TRUTH_ROOT}/accounting_v1/nav/${DAY_UTC}/nav.json"
V1_NAV_SNAP="${TRUTH_ROOT}/accounting_v1/nav/${DAY_UTC}/nav_snapshot.v1.json"
V2_NAV_JSON="${TRUTH_ROOT}/accounting_v2/nav/${DAY_UTC}/nav.v2.json"

# Seed sentinel anchor day once (some consumers expect a historical default day).
ANCHOR_DAY="2001-01-01"
ANCHOR_V1_NAV="${TRUTH_ROOT}/accounting_v1/nav/${ANCHOR_DAY}/nav.json"
if test ! -f "$ANCHOR_V1_NAV"; then
  ANCHOR_DAY="$ANCHOR_DAY" ANCHOR_V1_NAV="$ANCHOR_V1_NAV" "$VENV_PY" -c '
import json, os
from pathlib import Path

anchor = os.environ["ANCHOR_DAY"]
out = Path(os.environ["ANCHOR_V1_NAV"])
out.parent.mkdir(parents=True, exist_ok=True)

doc = {
  "day_utc": anchor,
  "history": {"drawdown_abs": 0, "drawdown_pct": "0.000000", "peak_nav": 0},
  "input_manifest": [],
  "nav": {
    "cash_total": 0,
    "components": [],
    "currency": "USD",
    "gross_positions_value": 0,
    "nav_total": 0,
    "notes": ["ANCHOR_DAY_STUB_V1"],
    "realized_pnl_to_date": 0,
    "unrealized_pnl": 0
  },
  "produced_utc": f"{anchor}T00:00:00Z",
  "producer": {"repo": "constellation_2_runtime", "module": "ops/run/c2_eod_paper_v1.sh", "git_sha": "ANCHOR"},
  "reason_codes": ["SEEDED_ANCHOR_DAY_FOR_V1_COMPAT"],
  "schema_id": "C2_ACCOUNTING_NAV_V1",
  "schema_version": 1,
  "status": "BOOTSTRAP"
}

raw = json.dumps(doc, sort_keys=True, separators=(",",":"), ensure_ascii=False) + "\n"
tmp = out.with_suffix(out.suffix + ".tmp")
tmp.write_text(raw, encoding="utf-8")
os.replace(tmp, out)
print(f"OK: SEEDED_V1_NAV_ANCHOR path={out}")
'
fi

# If v1 nav is missing but v2 nav exists, bridge it (write both nav.json and nav_snapshot.v1.json).
if test ! -f "$V1_NAV_JSON" && test -f "$V2_NAV_JSON"; then
  V2_NAV_JSON="$V2_NAV_JSON" V1_NAV_JSON="$V1_NAV_JSON" V1_NAV_SNAP="$V1_NAV_SNAP" "$VENV_PY" -c '
import json, os
from pathlib import Path

v2p = Path(os.environ["V2_NAV_JSON"])
v1p = Path(os.environ["V1_NAV_JSON"])
v1s = Path(os.environ["V1_NAV_SNAP"])

v2 = json.loads(v2p.read_text(encoding="utf-8"))
day = str(v2.get("day_utc") or "").strip()
nav = v2.get("nav") or {}
prod = v2.get("producer") or {}
produced_utc = str(v2.get("produced_utc") or "").strip()

doc = {
  "day_utc": day,
  "history": v2.get("history") or {},
  "input_manifest": [{
    "day_utc": day,
    "path": str(v2p),
    "producer": "accounting_nav_v2",
    "sha256": "",
    "type": "bridge_source_nav_v2"
  }],
  "nav": nav,
  "produced_utc": produced_utc,
  "producer": {"repo": "constellation_2_runtime", "module": "ops/run/c2_eod_paper_v1.sh", "git_sha": str(prod.get("git_sha") or "UNKNOWN")},
  "reason_codes": ["BRIDGED_FROM_NAV_V2"],
  "schema_id": "C2_ACCOUNTING_NAV_V1",
  "schema_version": 1,
  "status": "BRIDGED_FROM_V2"
}

def atomic_write(path: Path, obj: dict):
  path.parent.mkdir(parents=True, exist_ok=True)
  raw = json.dumps(obj, sort_keys=True, separators=(",",":"), ensure_ascii=False) + "\n"
  tmp = path.with_suffix(path.suffix + ".tmp")
  tmp.write_text(raw, encoding="utf-8")
  os.replace(tmp, path)

atomic_write(v1p, doc)
atomic_write(v1s, doc)
print(f"OK: BRIDGED_V1_NAV_FROM_V2 v1_nav={v1p} v1_snap={v1s} src_v2={v2p}")
'
fi

# Bundle completion proof (immutable outputs exist -> DO NOT rerun bundle)
NAV_JSON="${TRUTH_ROOT}/accounting_v1/nav/${DAY_UTC}/nav.json"

# Producer sha: if day outputs exist, lock to the day’s existing cash_ledger producer sha; else use HEAD.
CASH_LEDGER_SNAP="${TRUTH_ROOT}/cash_ledger_v1/snapshots/${DAY_UTC}/cash_ledger_snapshot.v1.json"
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
CERT_DIR="${TRUTH_ROOT}/reports/eod_run_certificate_v1/${DAY_UTC}"
CERT_PATH="${CERT_DIR}/eod_run_certificate.v1.json"
STARTED_UTC="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"

NAV_SERIES_OUT="${TRUTH_ROOT}/monitoring_v1/nav_series/${DAY_UTC}/portfolio_nav_series.v1.json"
SNAP_OUT="${TRUTH_ROOT}/reports/daily_portfolio_snapshot_v2_$(echo "$DAY_UTC" | tr -d '-').json"

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

  set +e
  _write_eod_cert "$rc" "$ended_utc" "$err_summary" >/dev/null 2>&1
  set -e

  return "$rc"
}
trap _on_exit EXIT

echo "C2_EOD_V1_START day_utc=${DAY_UTC} produced_utc=${PRODUCED_UTC} git_sha=${GIT_SHA} truth_root=${TRUTH_ROOT}"

# --- Defensive Tail required inputs bridge (immutable; skip if already present) ---
DEF_MD="${TRUTH_ROOT}/market_data_snapshot_v1/snapshots/${DAY_UTC}/SPY.market_data_snapshot.v1.json"
DEF_NAV="${TRUTH_ROOT}/accounting_v1/nav/${DAY_UTC}/nav_snapshot.v1.json"
DEF_POS="${TRUTH_ROOT}/positions_snapshot_v2/snapshots/${DAY_UTC}/positions_snapshot.v2.json"
if test -f "$DEF_MD" && test -f "$DEF_NAV" && test -f "$DEF_POS"; then
  echo "C2_EOD_V1_SKIP def_tail_inputs_bridge: already exists for day_utc=${DAY_UTC}"
else
  "$VENV_PY" constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py \
    --day_utc "$DAY_UTC" \
    --symbol "SPY"
fi

# --- Bundle F->G (ONLY if not already completed) ---
if test -f "$NAV_JSON"; then
  echo "C2_EOD_V1_SKIP bundle_f_to_g: accounting nav already exists for day_utc=${DAY_UTC}"
else
  OP_STMT="/home/node/constellation_2_runtime/constellation_2/operator_inputs/cash_ledger_operator_statements/${DAY_UTC}/operator_statement.v1.json"
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

# =========================
# NEW (append-only): Lifecycle monitoring artifacts for OpsDash
# =========================

echo "C2_EOD_V1_LIFECYCLE_MONITORING_START day_utc=${DAY_UTC}"

# 1) Engine risk budget ledger (authoritative engine set)
"$VENV_PY" ops/tools/run_engine_risk_budget_ledger_v1.py --day_utc "$DAY_UTC"

# 2) Monitoring summaries (these now emit by_engine when engine budget exists)
"$VENV_PY" ops/tools/run_intents_summary_day_v1.py --day_utc "$DAY_UTC"
"$VENV_PY" ops/tools/run_submissions_summary_day_v1.py --day_utc "$DAY_UTC"
"$VENV_PY" ops/tools/run_activity_ledger_rollup_day_v1.py --asof_day_utc "$DAY_UTC"

# 3) Submission index (UI expects under execution_evidence_v1/submissions/<DAY>/)
SUBIDX="${TRUTH_ROOT}/execution_evidence_v1/submissions/${DAY_UTC}/submission_index.v1.json"
if test -f "$SUBIDX"; then
  echo "C2_EOD_V1_SKIP submission_index: already exists for day_utc=${DAY_UTC}"
else
  "$VENV_PY" -m constellation_2.phaseF.execution_evidence.run.run_submission_index_day_v1 --day "$DAY_UTC"
fi
echo "C2_EOD_V1_LIFECYCLE_MONITORING_OK day_utc=${DAY_UTC}"

echo "C2_EOD_V1_OK day_utc=${DAY_UTC}"
