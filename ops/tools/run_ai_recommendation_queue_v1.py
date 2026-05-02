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

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, report_path_v1, status_of_v1, write_json_v1

SCHEMA_VERSION = "ai_recommendation_queue.v1"

SOURCES: tuple[dict[str, str], ...] = (
    {"source_type": "OUTCOME_ATTRIBUTION", "family": "outcome_attribution_v1", "filename": "outcome_attribution.v1.json", "target": "post_trade_review"},
    {"source_type": "MISSED_OPPORTUNITY", "family": "missed_opportunity_v1", "filename": "missed_opportunity.v1.json", "target": "entry_filter"},
    {"source_type": "DRIFT", "family": "insight_engine_v1", "filename": "insight_engine.v1.json", "target": "drift_controls"},
    {"source_type": "EDGE", "family": "edge_attribution_v1", "filename": "edge_attribution.v1.json", "target": "edge_filter"},
    {"source_type": "REGIME", "family": "regime_confidence_v1", "filename": "regime_confidence.v1.json", "target": "regime_filter"},
)


def ai_recommendation_queue_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "ai_recommendation_queue_v1" / day_utc / "ai_recommendation_queue.v1.json").resolve()


def _strength(payload: dict[str, Any]) -> str:
    status = status_of_v1(payload)
    if not payload or status in {"UNKNOWN", "NOT_APPLICABLE", "INSUFFICIENT_EVIDENCE", "UNPROVEN", "MISSING"}:
        return "LOW"
    if status in {"PASS", "ADVISORY_ONLY"}:
        return "MEDIUM"
    return "LOW"


def _entry(ctx: bod.BodContext, source: dict[str, str], payload: dict[str, Any], path: Path) -> dict[str, Any]:
    strength = _strength(payload)
    confidence = {"LOW": "LOW", "MEDIUM": "MEDIUM", "HIGH": "HIGH"}[strength]
    source_type = source["source_type"]
    return {
        "recommendation_id": f"{ctx.day_utc}:{source_type}:{source['target']}",
        "source_artifacts": [str(path)],
        "source_type": source_type,
        "target_component": source["target"],
        "target_sleeve": "PRIMARY",
        "recommendation_summary": f"Review {source_type.lower()} evidence before considering a governed strategy change.",
        "proposed_change_type": "HUMAN_REVIEW_REQUIRED",
        "expected_benefit": "Potentially improve future decision quality; unproven until shadow evaluation.",
        "risk_assessment": "Advisory only. No strategy, sizing, readiness, or submit behavior may change from this queue.",
        "evidence_strength": strength,
        "confidence": confidence,
        "advisory_only": True,
        "human_review_required": True,
        "status": "OPEN",
        "operator_next_action": "Human reviewer may reject or draft an inert strategy change proposal.",
    }


def build_ai_recommendation_queue_v1(ctx: bod.BodContext) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    input_paths: list[Path] = []
    for source in SOURCES:
        path = report_path_v1(ctx, source["family"], source["filename"])
        payload = read_json_v1(path)
        input_paths.append(path)
        if not payload or _strength(payload) == "LOW" or status_of_v1(payload) not in {"PASS"}:
            entries.append(_entry(ctx, source, payload, path))
    return {
        "schema_id": "ai_recommendation_queue",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS",
        "canonical_blocker": "",
        "operator_next_action": "Review open advisory recommendations." if entries else "",
        "recommendation_count": len(entries),
        "open_recommendation_count": len([row for row in entries if row["status"] == "OPEN"]),
        "recommendations": entries,
        "advisory_only": True,
        "readiness_effect": "NONE",
        "submit_boundary_effect": "NONE",
        "input_artifact_paths": [str(path) for path in input_paths],
    }


def run_ai_recommendation_queue_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_ai_recommendation_queue_v1(ctx)
    path = ai_recommendation_queue_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_ai_recommendation_queue_v1.py", producer_command=f"python3 ops/tools/run_ai_recommendation_queue_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=payload["input_artifact_paths"], output_artifacts=[path], schema_versions={"ai_recommendation_queue": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_ai_recommendation_queue_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_ai_recommendation_queue_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "recommendation_count": payload["recommendation_count"], "ai_recommendation_queue_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
