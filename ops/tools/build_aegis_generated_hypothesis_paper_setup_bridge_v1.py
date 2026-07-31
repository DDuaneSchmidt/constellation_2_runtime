#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.generated_hypothesis_paper_setup_bridge_v1 import write_generated_hypothesis_paper_setup_bridge_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis generated hypothesis paper setup bridge V1 artifact.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    path = write_generated_hypothesis_paper_setup_bridge_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(json.dumps({
        "artifact": payload.get("artifact_id"),
        "ok": True,
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": payload.get("summary") or {},
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
