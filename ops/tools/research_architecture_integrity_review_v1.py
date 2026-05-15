#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_research_architecture_integrity_review_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _load_family(truth_root: Path, family: str) -> list[dict[str, Any]]:
    root = truth_root / "research_lab" / family
    return [_read_json(path) for path in sorted(root.rglob("*.json"))] if root.exists() else []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="research_architecture_integrity_review_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--generated_at_utc", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    generated_at = args.generated_at_utc or _now()
    taxonomies = _load_family(truth_root, "edge_taxonomy_v1")
    review = build_research_architecture_integrity_review_v1(
        review_id=f"research-integrity-{args.day_utc}",
        generated_at_utc=generated_at,
        hypotheses=_load_family(truth_root, "research_hypothesis_v1"),
        task_queues=_load_family(truth_root, "research_task_queue_v1"),
        evidence_packets=_load_family(truth_root, "research_evidence_packet_v1"),
        result_ledgers=_load_family(truth_root, "research_result_ledger_v1"),
        conclusions=_load_family(truth_root, "research_conclusion_v1"),
        promotions=_load_family(truth_root, "research_to_lite_promotion_v1"),
        knowledge_graphs=_load_family(truth_root, "research_knowledge_graph_v1"),
        taxonomy=taxonomies[-1] if taxonomies else None,
        legacy_registries=_load_family(truth_root, "hypothesis_registry_v1"),
    )
    validate_research_lab_artifact_v1(review)
    path = write_research_lab_artifact_v1(truth_root=truth_root, day_utc=args.day_utc, payload=review)
    print(json.dumps({"review_status": review["review_status"], "path": str(path), "blocker_count": len(review["blockers"])}, sort_keys=True))
    return 1 if review["review_status"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
