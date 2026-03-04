#!/usr/bin/env bash
set -euo pipefail
cd /home/node/constellation_2_runtime

DAY="$(TZ=America/New_York date +%F)"
TRUTH="constellation_2/runtime/truth"
OUT_DIR="${TRUTH}/reports/preopen_preflight_v1/${DAY}"
OUT_PATH="${OUT_DIR}/preopen_preflight.v1.json"

REASONS_FILE="/tmp/c2_preopen_preflight_reasons_${DAY}.txt"
rm -f "${REASONS_FILE}"
mkdir -p "${OUT_DIR}"

fail=0

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

need_exec() {
  local p="$1"
  if [ ! -x "$p" ]; then
    add_reason "NOT_EXECUTABLE:${p}"
  fi
}

echo "DAY_UTC=${DAY}"

# --- Required artifacts for automation spine ---
need_exec "ops/run/c2_baseline_readiness_paper_v1.sh"
need_exec "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
need_path "ops/run/c2_check_baseline_ready_v1.py"
need_path "ops/run/c2_baseline_readiness_publish_v1.py"
need_path "ops/run/c2_kill_switch_engage_v1.py"

# --- Timers must be enabled (institutional automation) ---
if ! systemctl --user is-enabled c2-baseline-readiness.timer >/dev/null 2>&1; then
  add_reason "TIMER_DISABLED:c2-baseline-readiness.timer"
fi
if ! systemctl --user is-enabled c2-paper-day-orchestrator.timer >/dev/null 2>&1; then
  add_reason "TIMER_DISABLED:c2-paper-day-orchestrator.timer"
fi

# --- Orchestrator must be hard-gated by ExecStartPre ---
if ! systemctl --user cat c2-paper-day-orchestrator.service | rg -n "^ExecStartPre=" >/dev/null 2>&1; then
  add_reason "MISSING_GATE:ExecStartPre"
fi

# --- Orchestrator must not reference wrong account id ---
if systemctl --user cat c2-paper-day-orchestrator.service | rg -n "DUQ[0-9]{6}" >/dev/null 2>&1; then
  add_reason "ACCOUNT_MISMATCH_IN_UNIT:DUQ_FOUND"
fi
if systemctl --user cat c2-paper-day-orchestrator.service | rg -n "DUO847203" >/dev/null 2>&1; then
  : # ok
else
  add_reason "ACCOUNT_MISSING_IN_UNIT:DUO847203_NOT_FOUND"
fi

STATUS="PASS"
if [ "$fail" -ne 0 ]; then
  STATUS="FAIL"
fi

python3 -c "import json,os; \
day='${DAY}'; out_path='${OUT_PATH}'; reasons_path='${REASONS_FILE}'; \
reasons=([ln.strip() for ln in open(reasons_path,'r',encoding='utf-8').read().splitlines() if ln.strip()] if os.path.exists(reasons_path) else []); \
obj={ \
  'schema_id':'C2_PREOPEN_PREFLIGHT_V1', \
  'schema_version':1, \
  'day_utc':day, \
  'produced_utc':f'{day}T00:00:00Z', \
  'status':'${STATUS}', \
  'reason_codes':reasons, \
  'producer':{'repo':'constellation_2_runtime','module':'ops/run/c2_preopen_preflight_v1.sh'} \
}; \
os.makedirs(os.path.dirname(out_path), exist_ok=True); \
tmp=out_path+'.tmp'; \
open(tmp,'w',encoding='utf-8').write(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\\n'); \
os.replace(tmp,out_path); \
print('OK: PREOPEN_PREFLIGHT_WRITTEN status='+'${STATUS}'+' path='+out_path)"

if [ "$fail" -ne 0 ]; then
  echo "FAIL: PREOPEN_PREFLIGHT_FAIL"
  exit 2
fi

bash /home/node/constellation_2_runtime/ops/run/c2_verify_multi_sleeve_rollup_v1.sh

echo "OK: PREOPEN_PREFLIGHT_PASS"
