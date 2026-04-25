#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

echo "FAIL: LEGACY_ENTRYPOINT_QUARANTINED use ${REPO_ROOT}/ops/tools/run_c2_multi_sleeve_orchestrator_v1.py" >&2
exit 2
