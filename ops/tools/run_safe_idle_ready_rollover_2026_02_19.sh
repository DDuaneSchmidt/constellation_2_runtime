#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

DAY="2026-02-19"

echo "== PROOF: today_utc =="
date -u +%Y-%m-%d

echo
echo "== ACTION: regime_snapshot_v3 =="
python3 ops/tools/run_regime_snapshot_v3.py --day_utc "$DAY"

echo
echo "== ACTION: pipeline_manifest_v2 =="
python3 ops/tools/run_pipeline_manifest_v2.py --day_utc "$DAY"

echo
echo "== ACTION: operator_gate_verdict_v3 =="
python3 ops/tools/run_operator_gate_verdict_v3.py --day_utc "$DAY"

echo
echo "== PROOF: artifacts exist =="
ls -la "constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v3/${DAY}/regime_snapshot.v3.json"
ls -la "constellation_2/runtime/truth/reports/pipeline_manifest_v2/${DAY}/pipeline_manifest.v2.json"
ls -la "constellation_2/runtime/truth/reports/operator_gate_verdict_v3/${DAY}/operator_gate_verdict.v3.json"

echo
echo "== PROOF: statuses =="
python3 -c '
import json, pathlib
day="2026-02-19"
paths=[
("regime_snapshot_v3", f"constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v3/{day}/regime_snapshot.v3.json"),
("pipeline_manifest_v2", f"constellation_2/runtime/truth/reports/pipeline_manifest_v2/{day}/pipeline_manifest.v2.json"),
("operator_gate_verdict_v3", f"constellation_2/runtime/truth/reports/operator_gate_verdict_v3/{day}/operator_gate_verdict.v3.json"),
]
for name,p in paths:
  o=json.loads(pathlib.Path(p).read_text())
  print(name, "status=", o.get("status"), "blocking=", o.get("blocking"), "ready=", o.get("ready"))
'
