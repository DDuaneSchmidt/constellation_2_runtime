#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.constitutional_review_resolution_v1 import (  # noqa: E402
    build_constitutional_operator_decision_v1,
    build_review_packet_from_authorization_artifact_v1,
    build_review_packet_from_post_entry_boundary_v1,
    write_constitutional_operator_decision_v1,
)


def _read_json_object(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record a constitutional operator decision for a review-required artifact.")
    parser.add_argument("--source_artifact_path", required=True)
    parser.add_argument("--source_artifact_type", required=True, choices=["authorization_v1", "post_entry_submit_boundary_v1"])
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--operator_action", required=True, choices=["APPROVE", "REJECT", "DEFER"])
    parser.add_argument("--decided_at_utc", required=True)
    parser.add_argument("--operator_id", default="")
    parser.add_argument("--operator_note", default="")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_path = Path(str(args.source_artifact_path)).resolve()
    if not source_path.exists() or not source_path.is_file():
        raise SystemExit(f"FAIL: SOURCE_ARTIFACT_MISSING: {source_path}")
    source_payload = _read_json_object(source_path)
    constitutional_shadow = dict(source_payload.get("constitutional_shadow") or {})
    review_packet = dict(constitutional_shadow.get("review_packet") or {})
    if not review_packet:
        if args.source_artifact_type == "authorization_v1":
            review_packet = build_review_packet_from_authorization_artifact_v1(source_payload)
        else:
            review_packet = build_review_packet_from_post_entry_boundary_v1(source_payload)
    day_utc = str(source_payload.get("day_utc") or source_payload.get("evaluated_at_utc") or source_payload.get("produced_utc") or "")[0:10]
    if len(day_utc) != 10:
        raise SystemExit(f"FAIL: DAY_UTC_UNRESOLVED: {source_path}")
    decision_record = build_constitutional_operator_decision_v1(
        review_packet=review_packet,
        operator_action=str(args.operator_action).strip().upper(),
        operator_id=(str(args.operator_id).strip() or None),
        decided_at=str(args.decided_at_utc).strip(),
        source_artifact_type=str(args.source_artifact_type).strip(),
        source_artifact_path=str(source_path),
        source_artifact_hash=_sha256_file(source_path),
        operator_note=str(args.operator_note or "").strip(),
    )
    written_path = write_constitutional_operator_decision_v1(
        truth_root=Path(str(args.truth_root)).resolve(),
        day_utc=day_utc,
        decision_record=decision_record,
    )
    print(
        json.dumps(
            {
                "proposal_hash": decision_record["proposal_hash"],
                "operator_action": decision_record["operator_action"],
                "final_decision_applied": decision_record["final_decision_applied"],
                "decision_record_path": str(written_path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
