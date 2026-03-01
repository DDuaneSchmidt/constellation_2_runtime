#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

echo "[c2-preflight] check: forbid latest.json references in executable code"

# Strictly executable code only; exclude documentation.
# Targets:
#  - python under constellation_2/ and ops/
#  - shell under ops/
#  - js under constellation_2/ and ops/
if rg -n "latest\.json" -S \
  --glob "constellation_2/**/*.py" \
  --glob "ops/**/*.py" \
  --glob "ops/**/*.sh" \
  --glob "constellation_2/**/*.js" \
  --glob "ops/**/*.js" \
  >/dev/null; then
  echo "[c2-preflight] FAIL: latest.json references found in executable code:"
  rg -n "latest\.json" -S \
    --glob "constellation_2/**/*.py" \
    --glob "ops/**/*.py" \
    --glob "ops/**/*.sh" \
    --glob "constellation_2/**/*.js" \
    --glob "ops/**/*.js" | sed -n '1,200p'
  exit 1
fi

echo "[c2-preflight] OK: no latest.json references in executable code"
exit 0
