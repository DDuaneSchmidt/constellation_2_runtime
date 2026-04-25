#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

echo "FAIL: LEGACY_ENTRYPOINT_QUARANTINED use ${REPO_ROOT}/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh" >&2
exit 2
