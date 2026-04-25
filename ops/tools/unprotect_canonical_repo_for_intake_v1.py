#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import (
    CANONICAL_REPO_ROOT,
    append_protection_audit_v1,
    read_protection_status_v1,
    set_canonical_repo_protection_v1,
    write_protection_status_v1,
)


def main() -> int:
    ap = argparse.ArgumentParser(prog="unprotect_canonical_repo_for_intake_v1")
    ap.add_argument("--reason", required=True)
    args = ap.parse_args()

    reason = str(args.reason or "").strip()
    if not reason:
        raise SystemExit("FAIL: --reason is required")
    if "apply_codex_patch_bundle_v1" not in reason:
        raise SystemExit(
            "FAIL: unprotect only allowed for apply_codex_patch_bundle_v1 workflow"
        )

    changed_count = set_canonical_repo_protection_v1(protect=False)
    status_path = write_protection_status_v1(
        protected=False,
        actor="ops/tools/unprotect_canonical_repo_for_intake_v1.py",
        reason=reason,
        changed_path_count=changed_count,
    )
    append_protection_audit_v1(
        action="UNPROTECT",
        actor="ops/tools/unprotect_canonical_repo_for_intake_v1.py",
        reason=reason,
        changed_path_count=changed_count,
        status="UNPROTECTED",
    )
    print(
        json.dumps(
            {
                "status": "UNPROTECTED",
                "canonical_repo_root": str(CANONICAL_REPO_ROOT),
                "changed_path_count": changed_count,
                "protection_status_path": str(status_path),
                "current_status": read_protection_status_v1(),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
