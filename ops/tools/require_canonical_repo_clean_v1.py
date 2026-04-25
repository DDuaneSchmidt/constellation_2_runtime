#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import (
    CANONICAL_REPO_ROOT,
    git_status_porcelain_paths_v1,
    require_canonical_repo_root_v1,
)


def evaluate_canonical_cleanliness_v1(repo_root: Path) -> dict:
    require_canonical_repo_root_v1(repo_root)
    dirty_paths = git_status_porcelain_paths_v1(repo_root)
    if dirty_paths:
        return {
            "status": "DIRTY",
            "canonical_repo_root": str(CANONICAL_REPO_ROOT),
            "dirty_path_count": len(dirty_paths),
            "dirty_paths": dirty_paths,
        }
    return {
        "status": "CLEAN",
        "canonical_repo_root": str(CANONICAL_REPO_ROOT),
        "dirty_path_count": 0,
    }


def main() -> int:
    payload = evaluate_canonical_cleanliness_v1(Path("/home/node/constellation").resolve())
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["status"] == "CLEAN" else 1


if __name__ == "__main__":
    raise SystemExit(main())
