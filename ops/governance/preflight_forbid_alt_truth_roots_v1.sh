#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
cd "$REPO_ROOT"

RUNTIME_DIR="constellation_2/runtime"

echo "[c2-preflight] check: forbid alternate truth roots under ${RUNTIME_DIR}"

# Canonical truth root name is exactly: truth
# Any other directory starting with truth_ is forbidden unless it is already quarantined (name begins with __quarantine).
BAD=0

if test -d "${RUNTIME_DIR}"; then
  for d in "${RUNTIME_DIR}"/truth_*; do
    # If the glob doesn't match anything, it returns the pattern; skip.
    if test "$d" = "${RUNTIME_DIR}/truth_*"; then
      continue
    fi

    bn="$(basename "$d")"

    # Allow quarantine directories (we preserve evidence there)
    case "$bn" in
      __quarantine_* )
        continue
        ;;
    esac

    echo "[c2-preflight] FAIL: non-canonical truth root exists: $d"
    BAD=1
  done
fi

if test "$BAD" -ne 0; then
  exit 2
fi

echo "[c2-preflight] OK: no alternate truth roots detected"
