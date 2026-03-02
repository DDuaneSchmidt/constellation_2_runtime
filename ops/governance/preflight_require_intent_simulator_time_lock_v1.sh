#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
ORCH="${REPO_ROOT}/ops/tools/run_c2_paper_day_orchestrator_v1.py"
REG="${REPO_ROOT}/governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
SIM="${REPO_ROOT}/constellation_2/phaseH/intent_simulator/run/run_intent_simulator_day_v1.py"

echo "[preflight] require intent simulator time lock v1"

test -f "${ORCH}" || { echo "FAIL: missing orchestrator: ${ORCH}" >&2; exit 2; }
test -f "${REG}"  || { echo "FAIL: missing engine registry: ${REG}" >&2; exit 2; }
test -f "${SIM}"  || { echo "FAIL: missing intent simulator: ${SIM}" >&2; exit 2; }

SIM_STATUS="$(python3 - <<'PY'
import json, sys
p="governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
j=json.load(open(p,"r",encoding="utf-8"))
e=[x for x in j.get("engines",[]) if x.get("engine_id")=="C2_INTENT_SIMULATOR_V1"]
if len(e)!=1:
    print("MISSING")
    sys.exit(0)
print(str(e[0].get("activation_status") or "").strip())
PY
)"

if [ "${SIM_STATUS}" != "ACTIVE" ]; then
  echo "[preflight] simulator not ACTIVE (status=${SIM_STATUS}); skipping simulator invariants"
  exit 0
fi

# Orchestrator must call the simulator stage.
rg -n 'H_INTENT_SIMULATOR_V1' "${ORCH}" >/dev/null || { echo "FAIL: orchestrator missing H_INTENT_SIMULATOR_V1" >&2; exit 2; }
rg -n 'constellation_2\.phaseH\.intent_simulator\.run\.run_intent_simulator_day_v1' "${ORCH}" >/dev/null || { echo "FAIL: orchestrator missing simulator module invocation" >&2; exit 2; }

# Simulator must enforce exact 10:00:00 America/New_York.
rg -n 'ZoneInfo\("America/New_York"\)' "${SIM}" >/dev/null || { echo "FAIL: simulator missing America/New_York ZoneInfo" >&2; exit 2; }
rg -n 'dt_local\.hour == 10' "${SIM}" >/dev/null || { echo "FAIL: simulator missing hour==10 check" >&2; exit 2; }
rg -n 'dt_local\.minute == 0' "${SIM}" >/dev/null || { echo "FAIL: simulator missing minute==0 check" >&2; exit 2; }
rg -n 'dt_local\.second == 0' "${SIM}" >/dev/null || { echo "FAIL: simulator missing second==0 check" >&2; exit 2; }

# One-run-per-day must be enforced.
rg -n 'INTENTS_ALREADY_EXIST_FOR_DAY' "${SIM}" >/dev/null || { echo "FAIL: simulator missing one-run-per-day refusal" >&2; exit 2; }

# Scenario count must be exactly 7.
rg -n 'SCENARIO_COUNT_MISMATCH' "${SIM}" >/dev/null || { echo "FAIL: simulator missing scenario count enforcement" >&2; exit 2; }
rg -n 'len\(scenarios\) != 7' "${SIM}" >/dev/null || { echo "FAIL: simulator missing exact ==7 check" >&2; exit 2; }

# Symbol must be SPY only.
rg -n 'symbol = "SPY"' "${SIM}" >/dev/null || { echo "FAIL: simulator missing SPY lock" >&2; exit 2; }

# Registry must include simulator ACTIVE and runner sha must match file sha.
ENGINE_SHA="$(sha256sum "${SIM}" | awk '{print $1}')"

python3 - <<'PY'
import json, sys
p="governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
j=json.load(open(p,"r",encoding="utf-8"))
e=[x for x in j.get("engines",[]) if x.get("engine_id")=="C2_INTENT_SIMULATOR_V1"]
if len(e)!=1:
    print("FAIL: registry missing simulator engine entry", file=sys.stderr); sys.exit(2)
if e[0].get("activation_status")!="ACTIVE":
    print("FAIL: simulator engine not ACTIVE", file=sys.stderr); sys.exit(2)
if not e[0].get("engine_runner_sha256"):
    print("FAIL: simulator engine missing engine_runner_sha256", file=sys.stderr); sys.exit(2)
PY

rg -n "\"engine_runner_sha256\"\\s*:\\s*\"${ENGINE_SHA}\"" "${REG}" >/dev/null || {
  echo "FAIL: registry engine_runner_sha256 does not match simulator runner sha" >&2
  echo "expected_sha=${ENGINE_SHA}" >&2
  exit 2
}

echo "[preflight] PASS require intent simulator time lock v1"
