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
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, report_path_v1, write_json_v1

SCHEMA_VERSION = "strategy_change_proposal.v1"


def strategy_change_proposal_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "strategy_change_proposal_v1" / day_utc / "strategy_change_proposal.v1.json").resolve()


def _queue_path(ctx: bod.BodContext) -> Path:
    return report_path_v1(ctx, "ai_recommendation_queue_v1", "ai_recommendation_queue.v1.json")


def _proposal(ctx: bod.BodContext, rec: dict[str, Any]) -> dict[str, Any]:
    evidence = str(rec.get("evidence_strength") or "LOW")
    return {
        "proposal_id": f"{ctx.day_utc}:proposal:{rec.get('recommendation_id')}",
        "linked_recommendation_ids": [str(rec.get("recommendation_id") or "")],
        "target_strategy_or_sleeve": str(rec.get("target_sleeve") or "PRIMARY"),
        "proposed_rule_or_model_change": str(rec.get("recommendation_summary") or ""),
        "rationale": "Converted from advisory recommendation queue for human review only.",
        "supporting_evidence_paths": list(rec.get("source_artifacts") or []),
        "expected_effect": str(rec.get("expected_benefit") or ""),
        "risk_assessment": str(rec.get("risk_assessment") or ""),
        "rollback_plan": "Revert to prior governed strategy configuration if promoted change fails criteria.",
        "success_criteria": ["Shadow evaluation passes", "No risk delta above MEDIUM", "Post-promotion monitor remains within expected effect"],
        "failure_criteria": ["Shadow evaluation fails", "Hard kernel blocker exists", "Post-promotion monitor recommends rollback"],
        "human_approval_status": "PENDING" if evidence == "LOW" else "PENDING",
        "activation_status": "NOT_ACTIVE",
        "advisory_only": True,
    }


def build_strategy_change_proposal_v1(ctx: bod.BodContext) -> dict[str, Any]:
    queue_path = _queue_path(ctx)
    queue = read_json_v1(queue_path)
    recs = queue.get("recommendations") if isinstance(queue.get("recommendations"), list) else []
    proposals = [_proposal(ctx, rec) for rec in recs if isinstance(rec, dict) and rec.get("status") in {"OPEN", "PROPOSED"}]
    return {
        "schema_id": "strategy_change_proposal",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS" if proposals else "EMPTY",
        "canonical_blocker": "",
        "operator_next_action": "Human reviewer must approve, reject, or leave proposals pending." if proposals else "",
        "proposal_count": len(proposals),
        "pending_proposal_count": len([row for row in proposals if row["human_approval_status"] == "PENDING"]),
        "proposals": proposals,
        "active_strategy_mutation": False,
        "readiness_effect": "NONE",
        "submit_boundary_effect": "NONE",
    }


def run_strategy_change_proposal_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_strategy_change_proposal_v1(ctx)
    path = strategy_change_proposal_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_strategy_change_proposal_v1.py", producer_command=f"python3 ops/tools/run_strategy_change_proposal_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=[_queue_path(ctx)], output_artifacts=[path], schema_versions={"strategy_change_proposal": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_strategy_change_proposal_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_strategy_change_proposal_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "proposal_count": payload["proposal_count"], "strategy_change_proposal_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
