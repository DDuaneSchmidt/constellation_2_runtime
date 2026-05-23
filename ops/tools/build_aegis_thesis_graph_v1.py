#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.thesis_graph.thesis_graph_v1 import (  # noqa: E402
    build_evidence_item_v1,
    build_thesis_graph_projection_v1,
    create_thesis_from_investigation_v1,
    update_thesis_with_evidence_v1,
    write_evidence_artifact_v1,
    write_thesis_artifact_v1,
    write_thesis_graph_projection_v1,
)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"failed to read {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{path} is not a JSON object")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build governed Aegis thesis graph projection.")
    parser.add_argument("--truth-root", "--truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", default="")
    parser.add_argument("--seed-investigation-json", default="", help="Optional investigation JSON used to create or attach a thesis artifact.")
    parser.add_argument("--seed-evidence-json", default="", help="Optional evidence JSON attached to the latest matching thesis.")
    args = parser.parse_args(argv)

    day = str(args.day or "").strip()
    if not day:
        from datetime import date

        day = date.today().isoformat()
    root = Path(args.truth_root).expanduser().resolve()
    written: dict[str, Any] = {"theses": [], "evidence": []}

    if args.seed_investigation_json:
        investigation = _read_json(Path(args.seed_investigation_json))
        thesis = create_thesis_from_investigation_v1(investigation)
        written["theses"].append(write_thesis_artifact_v1(truth_root=root, thesis=thesis, day_utc=day))

    if args.seed_evidence_json:
        evidence_seed = _read_json(Path(args.seed_evidence_json))
        thesis_id = str(evidence_seed.get("thesis_id") or "").strip()
        if not thesis_id:
            raise SystemExit("seed evidence must include thesis_id")
        evidence = build_evidence_item_v1(**evidence_seed)
        written["evidence"].append(write_evidence_artifact_v1(truth_root=root, evidence=evidence, day_utc=day))

    projection = build_thesis_graph_projection_v1(truth_root=root, day_utc=day)
    paths = write_thesis_graph_projection_v1(truth_root=root, projection=projection, day_utc=day)
    print(json.dumps({"ok": True, "day_utc": day, "thesis_count": projection.get("thesis_count"), "evidence_count": projection.get("evidence_count"), "written": written, "paths": paths, "safety": projection.get("safety")}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
