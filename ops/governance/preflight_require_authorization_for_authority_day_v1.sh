#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
TRUTH_ROOT="${REPO_ROOT}/constellation_2/runtime/truth"

echo "[c2-preflight] check: require authorization artifacts when authority head is PASS"

AH="${TRUTH_ROOT}/run_pointer_v2/canonical_authority_head.v1.json"
if [ ! -f "${AH}" ]; then
  echo "[c2-preflight] WARN: authority head missing: ${AH} (skip)"
  exit 0
fi

# Extract authority status fields (fail-closed if unreadable)
AUTH_DAY="$(
  python3 -c "import json; o=json.load(open('${AH}','r',encoding='utf-8')); print(str(o.get('day_utc') or '').strip())"
)"
AUTH_STATUS="$(
  python3 -c "import json; o=json.load(open('${AH}','r',encoding='utf-8')); print(str(o.get('status') or '').strip().upper())"
)"
AUTH_FLAG="$(
  python3 -c "import json; o=json.load(open('${AH}','r',encoding='utf-8')); print('YES' if bool(o.get('authoritative')) else 'NO')"
)"

if [ -z "${AUTH_DAY}" ]; then
  echo "[c2-preflight] FAIL: authority head day_utc empty: ${AH}"
  exit 2
fi

echo "[c2-preflight] authority_head day_utc=${AUTH_DAY} status=${AUTH_STATUS} authoritative=${AUTH_FLAG}"

# Only enforce when the authority head is truly authoritative PASS.
if [ "${AUTH_STATUS}" != "PASS" ] || [ "${AUTH_FLAG}" != "YES" ]; then
  echo "[c2-preflight] OK: authority head not PASS+authoritative (skip)"
  exit 0
fi

# Required upstream: allocation artifact (authorization tool depends on it)
ALLOC="${TRUTH_ROOT}/allocation_v1/capital_authority_allocation_v1/${AUTH_DAY}/capital_authority_allocation.v1.json"
if [ ! -f "${ALLOC}" ]; then
  echo "[c2-preflight] FAIL: allocation missing for authority day: ${ALLOC}"
  exit 2
fi

# Required upstream: intents day dir
INTENTS_DIR="${TRUTH_ROOT}/intents_v1/snapshots/${AUTH_DAY}"
if [ ! -d "${INTENTS_DIR}" ]; then
  echo "[c2-preflight] FAIL: intents dir missing for authority day: ${INTENTS_DIR}"
  exit 2
fi

# For each exposure intent file, require matching authorization artifact named by sha256(file-bytes).
AUTH_DIR="${TRUTH_ROOT}/engine_activity_v1/authorization_v1/${AUTH_DAY}"
if [ ! -d "${AUTH_DIR}" ]; then
  echo "[c2-preflight] FAIL: authorization dir missing for authority day: ${AUTH_DIR}"
  exit 2
fi

MISS=0
TOTAL=0

while IFS= read -r f; do
  if [ -z "${f}" ]; then
    continue
  fi
  TOTAL=$((TOTAL + 1))
  H="$(sha256sum "${f}" | awk '{print $1}')"
  OUT="${AUTH_DIR}/${H}.authorization.v1.json"
  if [ ! -f "${OUT}" ]; then
    echo "[c2-preflight] FAIL: missing authorization for intent: intent_file=${f} sha256=${H} expected=${OUT}"
    MISS=$((MISS + 1))
    continue
  fi

  # Minimal schema sanity (fail-closed)
  python3 -c "import json; o=json.load(open('${OUT}','r',encoding='utf-8')); 
sid=str(o.get('schema_id') or '').strip(); sv=o.get('schema_version'); 
assert sid=='C2_AUTHORIZATION_V1', f'schema_id={sid}'; 
assert int(sv)==1, f'schema_version={sv}'; 
d=str(o.get('day_utc') or '').strip(); 
assert d=='${AUTH_DAY}', f'day_utc={d}'; 
ih=str(o.get('intent_hash') or '').strip(); 
assert ih=='${H}', f'intent_hash={ih}'; 
auth=o.get('authorization') or {}; 
dec=str(auth.get('decision') or '').strip().upper(); 
qty=int(auth.get('authorized_quantity') or 0); 
assert dec in ('AUTHORIZED','REJECTED'), f'decision={dec}'; 
assert qty>=0, f'qty={qty}'; 
print('OK_AUTHZ', '${OUT}')"
done < <(find "${INTENTS_DIR}" -maxdepth 1 -type f -name "*.exposure_intent.v1.json" | sort)

if [ "${TOTAL}" -eq 0 ]; then
  echo "[c2-preflight] FAIL: no exposure_intent.v1 files found under: ${INTENTS_DIR}"
  exit 2
fi

if [ "${MISS}" -ne 0 ]; then
  echo "[c2-preflight] FAIL: missing authorization artifacts: missing=${MISS} total=${TOTAL}"
  exit 2
fi

echo "[c2-preflight] OK: authorization artifacts present for authority day intents total=${TOTAL}"
exit 0
