#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.sleeve_edge_measurement_v1 import load_sleeve_edge_policy_v1, materialize_sleeve_edge_snapshot_v1
from constellation_2.common.truth_root_v1 import resolve_truth_root

CAPITAL_POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _resolve_truth_root(raw: str) -> Path:
    if str(raw or "").strip():
        path = Path(str(raw).strip()).resolve()
        if not path.exists() or not path.is_dir():
            raise SystemExit(f"FAIL: TRUTH_ROOT_MISSING: {path}")
        return path
    return resolve_truth_root(repo_root=REPO_ROOT)


def _load_capital_sleeves() -> List[Dict[str, Any]]:
    payload = _read_json_obj(CAPITAL_POLICY_PATH)
    sleeves = payload.get("sleeves")
    if not isinstance(sleeves, list) or not sleeves:
        raise SystemExit("FAIL: CAPITAL_POLICY_SLEEVES_INVALID")
    parsed: List[Dict[str, Any]] = []
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip()
        display_name = str(row.get("display_name") or sleeve_id).strip() or sleeve_id
        engine_ids = [str(item).strip() for item in row.get("engine_ids") or [] if str(item).strip()]
        if not sleeve_id or not engine_ids:
            raise SystemExit(f"FAIL: CAPITAL_POLICY_SLEEVE_INVALID sleeve_id={sleeve_id!r}")
        parsed.append({"sleeve_id": sleeve_id, "display_name": display_name, "engine_ids": engine_ids})
    return parsed


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_sleeve_edge_measurement_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--sleeve_id", action="append", default=[])
    parser.add_argument("--revision_type", default="")
    parser.add_argument("--revision_reason", default="")
    args = parser.parse_args(argv)

    truth_root = _resolve_truth_root(args.truth_root)
    try:
        policy = load_sleeve_edge_policy_v1(REPO_ROOT)
    except ValueError as exc:
        raise SystemExit(f"FAIL: {exc}") from exc
    requested = {str(item).strip() for item in args.sleeve_id if str(item).strip()}
    sleeves = _load_capital_sleeves()
    selected = [row for row in sleeves if not requested or row["sleeve_id"] in requested]
    if requested and len(selected) != len(requested):
        found = {row["sleeve_id"] for row in selected}
        missing = sorted(requested - found)
        raise SystemExit(f"FAIL: SLEEVE_ID_NOT_IN_CAPITAL_POLICY missing={','.join(missing)}")

    for row in selected:
        materialization = materialize_sleeve_edge_snapshot_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=str(args.day_utc),
            sleeve_id=row["sleeve_id"],
            strategy_family=row["display_name"],
            engine_ids=row["engine_ids"],
            qualification_policy=policy,
            revision_type=str(args.revision_type),
            revision_reason=str(args.revision_reason),
        )
        print(
            "OK: SLEEVE_EDGE_SNAPSHOT_V1 "
            f"sleeve_id={row['sleeve_id']} "
            f"snapshot_id={materialization.snapshot_id} "
            f"path={materialization.snapshot_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
