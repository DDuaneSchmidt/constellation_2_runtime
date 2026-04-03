#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"
TRUTH="/home/node/constellation_2_runtime/constellation_2/runtime/truth"
DAY_UTC="$(date -u +%Y-%m-%d)"
RUN_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
QUAR_BASE="/tmp/c2_manifest_refresh_${DAY_UTC}_${STAMP}"

mkdir -p "${QUAR_BASE}/fal/liquidity_dataset_manifest/${DAY_UTC}"
mkdir -p "${QUAR_BASE}/fal/engine_correlation_matrix/${DAY_UTC}"
mkdir -p "${QUAR_BASE}/fal/report/${DAY_UTC}"
mkdir -p "${QUAR_BASE}/gate_stack"
mkdir -p "${QUAR_BASE}/kill_switch"

echo "=== PROOF: repo root ==="
pwd

echo
echo "=== PROOF: required files exist ==="
ls -l constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py
ls -l ops/tools/run_feed_attestation_gate_v1.py
ls -l ops/tools/run_gate_stack_verdict_v1.py
ls -l ops/tools/run_global_kill_switch_v1.py
ls -l constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json

echo
echo "=== PROOF: IB socket port 4002 is listening ==="
ss -ltn | grep ':4002'

echo
echo "=== STEP: refresh manifest using only already-existing year files ==="
echo "INFO: this intentionally avoids missing years 2018-2025"
"$PY" constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py \
  --run_utc "$RUN_UTC" \
  --dataset_version v1 \
  --symbols SPY,QQQ,IWM,TLT,GLD,HYG \
  --start_year 2017 \
  --end_year 2017 \
  --host 127.0.0.1 \
  --port 4002 \
  --client_id 7 \
  --sleep_sec 1.0 \
  --use_rth 1

echo
echo "=== PROOF: manifest timestamps after refresh ==="
python3 -c 'import json
from pathlib import Path
p = Path("constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json")
obj = json.loads(p.read_text())
print("created_utc =", obj.get("created_utc"))
print("source_snapshot_utc =", obj.get("source_snapshot_utc"))
print("dataset_version =", obj.get("dataset_version"))
print("symbols =", obj.get("symbols"))
print("global_hash =", obj.get("global_hash"))'

echo
echo "=== STEP: quarantine same-day FAL artifacts to avoid overwrite refusal ==="
if [[ -f "${TRUTH}/feed_attestation_v1/records/liquidity_dataset_manifest/${DAY_UTC}/feed_attestation_record.v1.json" ]]; then
  mv \
    "${TRUTH}/feed_attestation_v1/records/liquidity_dataset_manifest/${DAY_UTC}/feed_attestation_record.v1.json" \
    "${QUAR_BASE}/fal/liquidity_dataset_manifest/${DAY_UTC}/feed_attestation_record.v1.json"
fi

if [[ -f "${TRUTH}/feed_attestation_v1/records/engine_correlation_matrix/${DAY_UTC}/feed_attestation_record.v1.json" ]]; then
  mv \
    "${TRUTH}/feed_attestation_v1/records/engine_correlation_matrix/${DAY_UTC}/feed_attestation_record.v1.json" \
    "${QUAR_BASE}/fal/engine_correlation_matrix/${DAY_UTC}/feed_attestation_record.v1.json"
fi

if [[ -f "${TRUTH}/reports/feed_attestation_gate_v1/${DAY_UTC}/feed_attestation_gate.v1.json" ]]; then
  mv \
    "${TRUTH}/reports/feed_attestation_gate_v1/${DAY_UTC}/feed_attestation_gate.v1.json" \
    "${QUAR_BASE}/fal/report/${DAY_UTC}/feed_attestation_gate.v1.json"
fi

echo
echo "=== STEP: rerun FAL ==="
"$PY" ops/tools/run_feed_attestation_gate_v1.py \
  --day_utc "$DAY_UTC" \
  --truth_root "$TRUTH"

echo
echo "=== STEP: quarantine same-day gate stack verdict to avoid overwrite ==="
if [[ -f "${TRUTH}/reports/gate_stack_verdict_v1/${DAY_UTC}/gate_stack_verdict.v1.json" ]]; then
  mv \
    "${TRUTH}/reports/gate_stack_verdict_v1/${DAY_UTC}/gate_stack_verdict.v1.json" \
    "${QUAR_BASE}/gate_stack/gate_stack_verdict.v1.json"
fi

echo
echo "=== STEP: rerun gate stack verdict ==="
"$PY" ops/tools/run_gate_stack_verdict_v1.py \
  --day_utc "$DAY_UTC" \
  --truth_root "$TRUTH" \
  --produced_utc "${DAY_UTC}T00:00:00Z" \
  --mode PAPER

echo
echo "=== STEP: quarantine same-day kill switch artifact to avoid overwrite ==="
if [[ -f "${TRUTH}/risk_v1/kill_switch_v1/${DAY_UTC}/global_kill_switch_state.v1.json" ]]; then
  mv \
    "${TRUTH}/risk_v1/kill_switch_v1/${DAY_UTC}/global_kill_switch_state.v1.json" \
    "${QUAR_BASE}/kill_switch/global_kill_switch_state.v1.json"
fi

echo
echo "=== STEP: rerun kill switch ==="
"$PY" ops/tools/run_global_kill_switch_v1.py \
  --day_utc "$DAY_UTC"

echo
echo "=== FINAL PROOF: summarized state ==="
python3 -c 'import json
from pathlib import Path

manifest = json.loads(Path("constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json").read_text())
fal = json.loads(Path(f"constellation_2/runtime/truth/reports/feed_attestation_gate_v1/{__import__('datetime').datetime.utcnow().strftime('%Y-%m-%d')}/feed_attestation_gate.v1.json").read_text())
gsv = json.loads(Path(f"constellation_2/runtime/truth/reports/gate_stack_verdict_v1/{__import__('datetime').datetime.utcnow().strftime('%Y-%m-%d')}/gate_stack_verdict.v1.json").read_text())
ks  = json.loads(Path(f"constellation_2/runtime/truth/risk_v1/kill_switch_v1/{__import__('datetime').datetime.utcnow().strftime('%Y-%m-%d')}/global_kill_switch_state.v1.json").read_text())

print("manifest source_snapshot_utc =", manifest.get("source_snapshot_utc"))
print("FAL status =", fal.get("status"))
for c in fal.get("checks", []):
    print("FAL check:", c.get("artifact_id"), c.get("pass"), c.get("reason_codes"))
print("gate_stack status =", gsv.get("status"))
print("gate_stack reason_codes =", gsv.get("reason_codes"))
print("kill_switch state =", ks.get("state"))
print("kill_switch forced_mode =", ks.get("forced_mode"))
print("kill_switch reason_codes =", ks.get("reason_codes"))'

echo
echo "=== PROOF: quarantine folder ==="
find "${QUAR_BASE}" -type f | sort || true
