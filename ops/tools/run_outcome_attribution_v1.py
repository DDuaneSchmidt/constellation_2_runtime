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
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1

SCHEMA_VERSION = "outcome_attribution.v1"


def outcome_attribution_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "outcome_attribution_v1" / day_utc / "outcome_attribution.v1.json").resolve()


def _submission_root(ctx: bod.BodContext) -> Path:
    return (ctx.execution_root / "execution_evidence_v1" / "submissions" / ctx.day_utc).resolve()


def _submission_records(ctx: bod.BodContext) -> list[Path]:
    root = _submission_root(ctx)
    if not root.exists():
        return []
    return sorted(root.rglob("broker_submission_record.v2.json"))


def _record_for_submission(path: Path) -> dict[str, Any]:
    payload = read_json_v1(path)
    status = str(payload.get("status") or payload.get("broker_status") or payload.get("submission_status") or "UNKNOWN").upper()
    trade_id = str(payload.get("trade_id") or payload.get("submission_id") or path.parent.name)
    completed = status in {"FILLED", "COMPLETE", "COMPLETED", "ACCEPTED"}
    canceled = status in {"CANCELED", "CANCELLED"}
    if completed:
        attribution_status = "INSUFFICIENT_EVIDENCE"
        confidence = "LOW"
        action = "Collect complete outcome, regime, edge, and execution-quality evidence before drawing conclusions."
    elif canceled:
        attribution_status = "UNKNOWN"
        confidence = "LOW"
        action = "Canceled trade has no completed outcome attribution; review cancellation evidence if needed."
    else:
        attribution_status = "INSUFFICIENT_EVIDENCE"
        confidence = "LOW"
        action = "Submission evidence is incomplete for outcome attribution."
    return {
        "trade_id": trade_id,
        "intent_id": str(payload.get("intent_id") or ""),
        "attribution_status": attribution_status,
        "decision_quality": "UNKNOWN",
        "execution_quality": "UNKNOWN",
        "regime_fit": "UNKNOWN",
        "timing_quality": "UNKNOWN",
        "structure_quality": "UNKNOWN",
        "realized_outcome": {},
        "expected_outcome": {},
        "luck_or_noise_assessment": "UNKNOWN",
        "missed_opportunity_notes": "",
        "evidence_paths": [str(path)],
        "confidence": confidence,
        "advisory_only": True,
        "human_review_required": True,
        "prohibited_actions_attempted": False,
        "operator_next_action": action,
    }


def build_outcome_attribution_v1(ctx: bod.BodContext) -> dict[str, Any]:
    submissions = _submission_records(ctx)
    if not submissions:
        records = [
            {
                "trade_id": "",
                "intent_id": "",
                "attribution_status": "NOT_APPLICABLE",
                "decision_quality": "UNKNOWN",
                "execution_quality": "UNKNOWN",
                "regime_fit": "UNKNOWN",
                "timing_quality": "UNKNOWN",
                "structure_quality": "UNKNOWN",
                "realized_outcome": {},
                "expected_outcome": {},
                "luck_or_noise_assessment": "UNKNOWN",
                "missed_opportunity_notes": "",
                "evidence_paths": [str(_submission_root(ctx))],
                "confidence": "NONE",
                "advisory_only": True,
                "human_review_required": False,
                "prohibited_actions_attempted": False,
                "operator_next_action": "No completed trade exists; no outcome attribution is available.",
            }
        ]
        status = "NOT_APPLICABLE"
    else:
        records = [_record_for_submission(path) for path in submissions]
        status = "UNKNOWN" if any(row["attribution_status"] != "PASS" for row in records) else "PASS"
    return {
        "schema_id": "outcome_attribution",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": status,
        "canonical_blocker": "",
        "operator_next_action": "Outcome attribution is advisory only and cannot alter readiness.",
        "advisory_only": True,
        "readiness_effect": "NONE",
        "submit_boundary_effect": "NONE",
        "attribution_records": records,
    }


def run_outcome_attribution_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_outcome_attribution_v1(ctx)
    path = outcome_attribution_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_outcome_attribution_v1.py", producer_command=f"python3 ops/tools/run_outcome_attribution_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=[_submission_root(ctx)], output_artifacts=[path], schema_versions={"outcome_attribution": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_outcome_attribution_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_outcome_attribution_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "outcome_attribution_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
