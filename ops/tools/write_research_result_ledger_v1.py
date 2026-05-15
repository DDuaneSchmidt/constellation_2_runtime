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
    build_research_result_ledger_v1,
    build_research_result_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _day(value: str) -> str:
    return value or datetime.now(UTC).strftime("%Y-%m-%d")


def _strings(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _ledger_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "research_lab" / "research_result_ledger_v1" / day_utc / "index" / "research_result_ledger.v1.json"


def _existing_results(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [row for row in payload.get("results", []) if isinstance(row, dict)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_research_result_ledger_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--created_at_utc", default="")
    parser.add_argument("--result_id", required=True)
    parser.add_argument("--hypothesis_id", required=True)
    parser.add_argument("--task_id", required=True)
    parser.add_argument("--result_status", required=True)
    parser.add_argument("--evidence_refs", default="")
    parser.add_argument("--conclusion_summary", default="")
    parser.add_argument("--confidence_before", default="")
    parser.add_argument("--confidence_after", default="")
    parser.add_argument("--confidence_change", default="")
    parser.add_argument("--metrics_summary_json", default="{}")
    parser.add_argument("--failure_mode_notes", default="")
    parser.add_argument("--invalidation_notes", default="")
    parser.add_argument("--next_recommended_task", default="")
    parser.add_argument("--promotion_recommendation", default="NONE")
    args = parser.parse_args(argv)

    if args.result_status.upper() in {"INVALIDATED", "CONTRADICTS_HYPOTHESIS"} and args.promotion_recommendation.upper() == "PROMOTION_CANDIDATE":
        raise ValueError("INVALIDATED_OR_CONTRADICTED_RESULT_CANNOT_RECOMMEND_PROMOTION")
    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = _day(args.day_utc)
    created_at = args.created_at_utc or _now()
    result = build_research_result_v1(
        result_id=args.result_id,
        hypothesis_id=args.hypothesis_id,
        task_id=args.task_id,
        result_status=args.result_status,
        evidence_refs=_strings(args.evidence_refs),
        conclusion_summary=args.conclusion_summary,
        confidence_before=args.confidence_before,
        confidence_after=args.confidence_after,
        confidence_change=args.confidence_change,
        metrics_summary=json.loads(args.metrics_summary_json),
        failure_mode_notes=args.failure_mode_notes,
        invalidation_notes=args.invalidation_notes,
        next_recommended_task=args.next_recommended_task,
        promotion_recommendation=args.promotion_recommendation,
        created_at_utc=created_at,
        artifact_lineage=[{"artifact_type": "manual_research_result_writer", "path": str(Path(__file__).resolve())}],
        reason_codes=[f"RESULT_STATUS:{args.result_status.upper()}"],
        reproducibility_notes="Manual offline result ledger writer; no broker or Lite runtime mutation.",
    )
    ledger = build_research_result_ledger_v1(
        generated_at_utc=created_at,
        results=[*_existing_results(_ledger_path(truth_root, day_utc)), result],
    )
    validate_research_lab_artifact_v1(ledger)
    path = write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=ledger)
    print(json.dumps({"result_count": len(ledger["results"]), "ledger_path": str(path), "research_lab_only": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
