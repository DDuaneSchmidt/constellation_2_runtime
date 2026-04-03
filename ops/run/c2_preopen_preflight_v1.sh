#!/usr/bin/env bash
set -euo pipefail
cd /home/node/constellation_2_runtime

DAY="${DAY_UTC_OVERRIDE:-$(TZ=America/New_York date +%F)}"
PY="/usr/bin/python3"
PY_SYS="/usr/bin/python3"
export PYTHONPATH="/home/node/constellation_2_runtime${PYTHONPATH:+:${PYTHONPATH}}"
TRUTH="constellation_2/runtime/truth"
OUT_DIR="${TRUTH}/reports/preopen_preflight_v1/${DAY}"
OUT_PATH="${OUT_DIR}/preopen_preflight.v1.json"
STARTUP_MATERIALIZATION_PATH="${TRUTH}/reports/startup_materialization_v1/${DAY}/startup_materialization.v1.json"
PREMARKET_PATH="${TRUTH}/reports/pre_market_system_check_v1/${DAY}/pre_market_system_check.v1.json"
DAY_START_BLOCKED_PATH="${TRUTH}/reports/day_start_blocked_v1/${DAY}/day_start_blocked.v1.json"
DAY_START_CLEAR_PATH="${TRUTH}/reports/day_start_clear_v1/${DAY}/day_start_clear.v1.json"
TRADING_DAY_STATE_PATH="${TRUTH}/reports/trading_day_state_v1/${DAY}/trading_day_state.v1.json"

REASONS_FILE="/tmp/c2_preopen_preflight_reasons_${DAY}.txt"
rm -f "${REASONS_FILE}"
mkdir -p "${OUT_DIR}"

fail=0

refresh_trading_day_state() {
  "${PY_SYS}" ops/tools/run_trading_day_state_v1.py --day_utc "${DAY}" --producer_module "ops/run/c2_preopen_preflight_v1.sh" >/dev/null || true
}

refresh_preopen_operator_summary() {
  "${PY_SYS}" ops/tools/run_operator_summary_v1.py --summary_kind preopen --day_utc "${DAY}" >/dev/null || true
}

refresh_downstream_readiness_surfaces() {
  "${PY_SYS}" ops/tools/run_paper_readiness_monitor_v2.py --day_utc "${DAY}" >/dev/null || true
  "${PY_SYS}" ops/tools/run_startup_proof_validation_v1.py --day_utc "${DAY}" >/dev/null || true
  "${PY_SYS}" ops/tools/run_trading_day_state_v2.py --day_utc "${DAY}" >/dev/null || true
}

publish_blocked_day() {
  local stage="$1"
  local prerequisite="$2"
  local failing_path="$3"
  local reason_code="$4"
  "${PY}" ops/tools/run_day_start_blocked_v1.py \
    --day_utc "${DAY}" \
    --blocked_stage "${stage}" \
    --first_failing_prerequisite "${prerequisite}" \
    --first_failing_path "${failing_path}" \
    --first_failing_reason_code "${reason_code}" \
    --failing_service "c2-preopen-preflight.service" \
    --failing_script "ops/run/c2_preopen_preflight_v1.sh" \
    --dependency "c2-preopen-preflight.timer" \
    --dependency "c2-preopen-preflight.service" \
    --dependency "ops/run/c2_preopen_preflight_v1.sh" \
    --dependency "ops/run/c2_verify_multi_sleeve_rollup_v1.sh" \
    --orchestrator_started false
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

need_exec() {
  local p="$1"
  if [ ! -x "$p" ]; then
    add_reason "NOT_EXECUTABLE:${p}"
  fi
}

echo "DAY_UTC=${DAY}"
refresh_trading_day_state

need_exec "ops/run/c2_baseline_readiness_paper_v1.sh"
need_exec "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
need_exec "ops/run/c2_verify_multi_sleeve_rollup_v1.sh"
need_path "ops/tools/run_pre_market_system_check_v1.py"
need_path "ops/tools/run_c2_market_data_preopen_prepare_v1.py"
need_path "ops/tools/run_startup_materialization_v1.py"
need_path "ops/tools/run_session_readiness_refresh_v1.py"
need_path "ops/run/c2_check_baseline_ready_v1.py"
need_path "ops/run/c2_baseline_readiness_publish_v1.py"
need_path "ops/run/c2_kill_switch_engage_v1.py"
need_path "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"
need_path "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py"
need_path "ops/tools/run_trading_day_state_v1.py"
need_path "ops/tools/run_day_start_clear_v1.py"

PREMARKET_RC=0
if [ "$fail" -eq 0 ]; then
  set +e
  "${PY}" ops/tools/run_pre_market_system_check_v1.py --day_utc "${DAY}"
  PREMARKET_RC=$?
  set -e
  if [ -f "${PREMARKET_PATH}" ]; then
    "${PY}" -c "import json, pathlib; obj=json.loads(pathlib.Path('${PREMARKET_PATH}').read_text(encoding='utf-8')); [print(x) for x in obj.get('reason_codes') or []]" >> "${REASONS_FILE}"
  fi
  if [ "$PREMARKET_RC" -ne 0 ]; then
    fail=1
  fi
fi

STARTUP_MATERIALIZATION_RC=0
if [ "$fail" -eq 0 ]; then
  set +e
  "${PY}" ops/tools/run_startup_materialization_v1.py --day_utc "${DAY}"
  STARTUP_MATERIALIZATION_RC=$?
  set -e
  if [ "$STARTUP_MATERIALIZATION_RC" -ne 0 ]; then
    add_reason "STARTUP_MATERIALIZATION_FAILED"
    fail=1
  fi
  if [ ! -f "${STARTUP_MATERIALIZATION_PATH}" ]; then
    add_reason "STARTUP_MATERIALIZATION_REPORT_MISSING:${STARTUP_MATERIALIZATION_PATH}"
    fail=1
  fi
fi

SESSION_REFRESH_RC=0
if [ "$fail" -eq 0 ]; then
  set +e
  "${PY}" ops/tools/run_session_readiness_refresh_v1.py --day_utc "${DAY}"
  SESSION_REFRESH_RC=$?
  set -e
  if [ "$SESSION_REFRESH_RC" -ne 0 ]; then
    add_reason "SESSION_READINESS_REFRESH_FAILED"
    fail=1
  fi
fi

MARKET_DATA_PREP_RC=0
if [ "$fail" -eq 0 ]; then
  set +e
  "${PY}" ops/tools/run_c2_market_data_preopen_prepare_v1.py --day_utc "${DAY}" --produced_utc "${DAY}T00:00:00Z"
  MARKET_DATA_PREP_RC=$?
  set -e
  if [ "$MARKET_DATA_PREP_RC" -ne 0 ]; then
    add_reason "CANONICAL_MARKET_DATA_PREPARATION_FAILED"
    fail=1
  fi
fi

STATUS="PASS"
if [ "$fail" -ne 0 ]; then
  STATUS="FAIL"
fi

"${PY}" -c "import json,os; day='${DAY}'; out_path='${OUT_PATH}'; reasons_path='${REASONS_FILE}'; reasons=([ln.strip() for ln in open(reasons_path,'r',encoding='utf-8').read().splitlines() if ln.strip()] if os.path.exists(reasons_path) else []); obj={'schema_id':'C2_PREOPEN_PREFLIGHT_V1','schema_version':1,'day_utc':day,'produced_utc':f'{day}T00:00:00Z','status':'${STATUS}','reason_codes':reasons,'producer':{'repo':'constellation_2_runtime','module':'ops/run/c2_preopen_preflight_v1.sh'}}; os.makedirs(os.path.dirname(out_path), exist_ok=True); tmp=out_path+'.tmp'; open(tmp,'w',encoding='utf-8').write(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\\n'); os.replace(tmp,out_path); print('OK: PREOPEN_PREFLIGHT_WRITTEN status='+'${STATUS}'+' path='+out_path)"

if [ "$fail" -ne 0 ]; then
  if [ "$PREMARKET_RC" -ne 0 ] && [ -f "${PREMARKET_PATH}" ]; then
    FIRST_REASON="$("${PY}" -c "import json, pathlib; obj=json.loads(pathlib.Path('${PREMARKET_PATH}').read_text(encoding='utf-8')); print(obj.get('first_failing_prerequisite') or '')")"
    FIRST_PATH="$("${PY}" -c "import json, pathlib; obj=json.loads(pathlib.Path('${PREMARKET_PATH}').read_text(encoding='utf-8')); print(obj.get('first_failing_path') or '')")"
    publish_blocked_day "PRE_MARKET_SYSTEM_CHECK" "${FIRST_REASON}" "${FIRST_PATH}" "PRE_MARKET_SYSTEM_CHECK_BLOCKED"
  elif [ "$STARTUP_MATERIALIZATION_RC" -ne 0 ]; then
    publish_blocked_day "STARTUP_MATERIALIZATION" "STARTUP_MATERIALIZATION_FAILED" "ops/tools/run_startup_materialization_v1.py" "STARTUP_MATERIALIZATION_FAILED"
  elif [ "$MARKET_DATA_PREP_RC" -ne 0 ]; then
    publish_blocked_day "CANONICAL_MARKET_DATA_PREPARATION" "CANONICAL_MARKET_DATA_PREPARATION_FAILED" "ops/tools/run_c2_market_data_preopen_prepare_v1.py" "CANONICAL_MARKET_DATA_PREPARATION_FAILED"
  elif [ "$SESSION_REFRESH_RC" -ne 0 ]; then
    publish_blocked_day "SESSION_READINESS_REFRESH" "SESSION_READINESS_REFRESH_FAILED" "ops/tools/run_session_readiness_refresh_v1.py" "SESSION_READINESS_REFRESH_FAILED"
  else
    FIRST_REASON="$("${PY}" -c "from pathlib import Path; p=Path('${REASONS_FILE}'); lines=[ln.strip() for ln in p.read_text(encoding='utf-8').splitlines() if ln.strip()] if p.exists() else []; print(lines[0] if lines else '')")"
    FIRST_PATH="$("${PY}" -c "reason='${FIRST_REASON}'; print(reason.split(':',1)[1] if ':' in reason else '')")"
    publish_blocked_day "PREOPEN_PREFLIGHT" "${FIRST_REASON}" "${FIRST_PATH}" "PREOPEN_PREFLIGHT_FAILED"
  fi
  refresh_trading_day_state
  refresh_downstream_readiness_surfaces
  refresh_preopen_operator_summary
  echo "FAIL: PREOPEN_PREFLIGHT_FAIL"
  exit 2
fi

if ! DAY_UTC="${DAY}" bash /home/node/constellation_2_runtime/ops/run/c2_verify_multi_sleeve_rollup_v1.sh; then
  publish_blocked_day "PREOPEN_ROLLUP_VERIFICATION" "PRIOR_ROLLUP_VERIFICATION" "" ""
  refresh_trading_day_state
  refresh_downstream_readiness_surfaces
  refresh_preopen_operator_summary
  echo "FAIL: PREOPEN_ROLLUP_VERIFICATION"
  exit 2
fi

"${PY}" ops/tools/run_day_start_clear_v1.py \
  --day_utc "${DAY}" \
  --producer_module "ops/run/c2_preopen_preflight_v1.sh" \
  --authoritative_preopen_write YES \
  --dependency "c2-preopen-preflight.timer" \
  --dependency "c2-preopen-preflight.service" \
  --dependency "ops/run/c2_preopen_preflight_v1.sh" \
  --dependency "ops/run/c2_verify_multi_sleeve_rollup_v1.sh"
if [ ! -f "${DAY_START_CLEAR_PATH}" ]; then
  add_reason "DAY_START_CLEAR_MISSING:${DAY_START_CLEAR_PATH}"
  STATUS="FAIL"
  echo "FAIL: DAY_START_CLEAR_MISSING"
  exit 2
fi
"${PY_SYS}" ops/tools/run_trading_day_state_v1.py --day_utc "${DAY}" --producer_module "ops/run/c2_preopen_preflight_v1.sh" >/dev/null
if [ ! -f "${TRADING_DAY_STATE_PATH}" ]; then
  add_reason "TRADING_DAY_STATE_MISSING:${TRADING_DAY_STATE_PATH}"
  STATUS="FAIL"
  publish_blocked_day "TRADING_DAY_STATE_WRITE" "TRADING_DAY_STATE_MISSING" "${TRADING_DAY_STATE_PATH}" "TRADING_DAY_STATE_MISSING"
  refresh_downstream_readiness_surfaces
  refresh_preopen_operator_summary
  echo "FAIL: TRADING_DAY_STATE_MISSING"
  exit 2
fi
refresh_downstream_readiness_surfaces
refresh_preopen_operator_summary

echo "OK: PREOPEN_PREFLIGHT_PASS"
