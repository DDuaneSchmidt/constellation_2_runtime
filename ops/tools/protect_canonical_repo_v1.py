#!/usr/bin/env python3
from __future__ import annotations

import json

from ops.tools.repo_protection_common_v1 import (
    CANONICAL_REPO_ROOT,
    append_protection_audit_v1,
    read_protection_status_v1,
    set_canonical_repo_protection_v1,
    write_protection_status_v1,
)


def main() -> int:
    changed_count = set_canonical_repo_protection_v1(protect=True)
    status_path = write_protection_status_v1(
        protected=True,
        actor="ops/tools/protect_canonical_repo_v1.py",
        reason="manual_or_automated_protection",
        changed_path_count=changed_count,
    )
    append_protection_audit_v1(
        action="PROTECT",
        actor="ops/tools/protect_canonical_repo_v1.py",
        reason="manual_or_automated_protection",
        changed_path_count=changed_count,
        status="PROTECTED",
    )
    print(
        json.dumps(
            {
                "status": "PROTECTED",
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

