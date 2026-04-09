#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

DAY="${DAY_UTC_OVERRIDE:-$(TZ=America/New_York date +%F)}"
PY="/usr/bin/python3"
PY_SYS="/usr/bin/python3"
export PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
TRUTH="constellation_2/runtime/truth"
OUT_DIR="${TRUTH}/reports/preopen_preflight_v1/${DAY}"
OUT_PATH="${OUT_DIR}/preopen_preflight.v1.json"
CONTROL_PLANE_PATH="${TRUTH}/reports/trading_day_state_machine_v1/${DAY}/trading_day_state_machine.v1.json"
OPERATOR_SUMMARY_PATH="${TRUTH}/reports/operator_summary_v1/${DAY}/operator_summary.v1.json"

REASONS_FILE="/tmp/c2_preopen_preflight_reasons_${DAY}.txt"
rm -f "${REASONS_FILE}"
mkdir -p "${OUT_DIR}"

fail=0
CONTROL_PLANE_RC=0
FINAL_START_DECISION=""
FIRST_TRUE_BLOCKER=""
CONTROL_PLANE_ID=""
LEDGER_ID=""

refresh_preopen_operator_summary() {
  "${PY_SYS}" ops/tools/run_operator_summary_v1.py --summary_kind preopen --day_utc "${DAY}" >/dev/null || true
}

add_reason() {
  echo "$1" >> "${REASONS_FILE}"
  fail=1
}

need_path() {
  local p="$1"
  if [ ! -e "$p" ]; then
    add_reason "MISSING:${p}"
  fi
}

echo "DAY_UTC=${DAY}"

need_path "ops/tools/run_intents_day_completeness_v1.py"
need_path "ops/tools/run_trading_day_intent_generation_v1.py"
need_path "ops/tools/run_trading_day_state_machine_v1.py"
need_path "ops/tools/run_trading_day_execution_control_plane_v1.py"
need_path "ops/tools/run_trading_day_control_plane_v1.py"
need_path "ops/tools/run_startup_materialization_v1.py"
need_path "ops/tools/run_paper_trading_posture_v1.py"
need_path "ops/tools/run_submit_boundary_status_v1.py"
need_path "ops/tools/run_paper_session_ledger_v1.py"
need_path "ops/tools/run_startup_proof_validation_v1.py"
need_path "ops/tools/run_operator_summary_v1.py"
need_path "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"

if [ "$fail" -eq 0 ]; then
  set +e
  "${PY}" ops/tools/run_trading_day_state_machine_v1.py --day_utc "${DAY}"
  CONTROL_PLANE_RC=$?
  set -e
  if [ ! -f "${CONTROL_PLANE_PATH}" ]; then
        add_reason "TRADING_DAY_CONTROL_PLANE_REPORT_MISSING:${CONTROL_PLANE_PATH}"
  else
    FINAL_START_DECISION="$("${PY}" -c "import json, pathlib; obj=json.loads(pathlib.Path('${CONTROL_PLANE_PATH}').read_text(encoding='utf-8')); print(obj.get('final_start_decision') or '')")"
        FIRST_TRUE_BLOCKER="$("${PY}" -c "import json, pathlib; obj=json.loads(pathlib.Path('${CONTROL_PLANE_PATH}').read_text(encoding='utf-8')); print((obj.get('first_true_blocker') or {}).get('first_true_blocker_code') or '')")"
        CONTROL_PLANE_ID="$("${PY}" -c "import json, pathlib; obj=json.loads(pathlib.Path('${CONTROL_PLANE_PATH}').read_text(encoding='utf-8')); print(obj.get('state_machine_id') or '')")"
        LEDGER_ID="$("${PY}" -c "import json, pathlib; obj=json.loads(pathlib.Path('${CONTROL_PLANE_PATH}').read_text(encoding='utf-8')); print((obj.get('supporting_session_authority') or {}).get('ledger_id') or '')")"
        if [ -n "${FINAL_START_DECISION}" ]; then
          add_reason "TRADING_DAY_STATE_MACHINE:${FINAL_START_DECISION}"
        fi
    if [ -n "${FIRST_TRUE_BLOCKER}" ]; then
      add_reason "${FIRST_TRUE_BLOCKER}"
    fi
    if [ "${FINAL_START_DECISION}" = "READY_NOW" ]; then
      fail=0
      : > "${REASONS_FILE}"
    fi
  fi
fi

if [ -f "${CONTROL_PLANE_PATH}" ]; then
  refresh_preopen_operator_summary
fi

STATUS="PASS"
if [ "$fail" -ne 0 ]; then
  STATUS="FAIL"
fi

"${PY}" -c "import json,os; day='${DAY}'; out_path='${OUT_PATH}'; reasons_path='${REASONS_FILE}'; repo_root='${REPO_ROOT}'; reasons=([ln.strip() for ln in open(reasons_path,'r',encoding='utf-8').read().splitlines() if ln.strip()] if os.path.exists(reasons_path) else []); obj={'schema_id':'C2_PREOPEN_PREFLIGHT_V1','schema_version':1,'day_utc':day,'produced_utc':f'{day}T00:00:00Z','status':'${STATUS}','reason_codes':reasons,'control_plane_path':'${CONTROL_PLANE_PATH}','control_plane_id':'${CONTROL_PLANE_ID}','state_machine_id':'${CONTROL_PLANE_ID}','final_start_decision':'${FINAL_START_DECISION}','first_true_blocker_code':'${FIRST_TRUE_BLOCKER}','ledger_id':'${LEDGER_ID}','operator_summary_path':'${OPERATOR_SUMMARY_PATH}','producer':{'repo':os.path.basename(repo_root),'module':'ops/run/c2_preopen_preflight_v1.sh'}}; os.makedirs(os.path.dirname(out_path), exist_ok=True); tmp=out_path+'.tmp'; open(tmp,'w',encoding='utf-8').write(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\\n'); os.replace(tmp,out_path); print('OK: PREOPEN_PREFLIGHT_WRITTEN status='+'${STATUS}'+' path='+out_path)"

if [ "$STATUS" != "PASS" ]; then
  echo "FAIL: PREOPEN_PREFLIGHT_FAIL"
  exit 2
fi

echo "OK: PREOPEN_PREFLIGHT_PASS"
