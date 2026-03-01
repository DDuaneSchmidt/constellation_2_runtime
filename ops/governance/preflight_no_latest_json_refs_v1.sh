#!/usr/bin/env bash
set -euo pipefail

echo "[c2-preflight] check: forbid legacy latest pointer references in executable code"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# Build the forbidden filename pattern without embedding the literal string in this file.
PATTERN="latest""\.json"

# Check only executable code surfaces (not docs)
HITS="$(rg -n "$PATTERN" -S constellation_2 ops || true)"

if [ -n "${HITS}" ]; then
  echo "[c2-preflight] FAIL: legacy latest pointer references found in executable code:"
  echo "${HITS}"
  exit 2
fi

echo "[c2-preflight] OK: no legacy latest pointer references in executable code"
