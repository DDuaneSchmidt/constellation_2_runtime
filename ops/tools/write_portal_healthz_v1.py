#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.projection_writer_v1 import atomic_write_json


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--portal-site-root", default="/home/node/projects/sv_portal/site")
    args = ap.parse_args()
    truth_path = Path(args.truth_root) / "reports" / "unified_truth_state_v1" / args.target_day / "unified_truth_state.v1.json"
    projection_path = Path(args.truth_root) / "reports" / "aegis_operator_projection_v1" / args.target_day / "projection.v1.json"
    now = datetime.now(UTC).replace(microsecond=0)
    payload = {
        "schema_version": "aegis_portal_health.v1",
        "status": "OK",
        "generated_at_utc": now.isoformat().replace("+00:00", "Z"),
        "origin_process": "OK",
        "truth_root_readable": Path(args.truth_root).is_dir(),
        "unified_truth_state_path": str(truth_path),
        "unified_truth_state_age_seconds": _age_seconds(truth_path, now),
        "projection_age_seconds": _age_seconds(projection_path, now),
        "build_commit": _git_commit(REPO_ROOT),
        "blocker": None,
    }
    if not truth_path.exists():
        payload["status"] = "UNKNOWN"
        payload["blocker"] = "unified truth state missing"
    atomic_write_json(Path(args.portal_site_root) / "healthz", payload)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "OK" else 2


def _age_seconds(path: Path, now) -> int | None:
    if not path.exists():
        return None
    return max(0, int(now.timestamp() - path.stat().st_mtime))


def _git_commit(root: Path) -> str:
    head = root / ".git" / "HEAD"
    if not head.exists():
        return "UNKNOWN"
    raw = head.read_text(encoding="utf-8").strip()
    if raw.startswith("ref:"):
        ref = root / ".git" / raw.split(" ", 1)[1]
        return ref.read_text(encoding="utf-8").strip()[:12] if ref.exists() else "UNKNOWN"
    return raw[:12]


if __name__ == "__main__":
    raise SystemExit(main())
