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

SCHEMA_VERSION = "shadow_evaluation.v1"


def shadow_evaluation_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "shadow_evaluation_v1" / day_utc / "shadow_evaluation.v1.json").resolve()


def _proposal_path(ctx: bod.BodContext) -> Path:
    return report_path_v1(ctx, "strategy_change_proposal_v1", "strategy_change_proposal.v1.json")


def _baseline_path(ctx: bod.BodContext) -> Path:
    return report_path_v1(ctx, "strategy_decision_authority_v1", "strategy_decision_authority.v1.json")


def _evaluation(ctx: bod.BodContext, proposal: dict[str, Any], baseline: dict[str, Any], baseline_path: Path) -> dict[str, Any]:
    baseline_decision = {
        "status": str(baseline.get("status") or "UNKNOWN"),
        "strategy_decision_state": str(baseline.get("strategy_decision_state") or baseline.get("state") or "UNKNOWN"),
        "source_path": str(baseline_path),
    }
    shadow_decision = {
        **baseline_decision,
        "observe_only": True,
        "proposal_id": str(proposal.get("proposal_id") or ""),
        "active_strategy_mutation": False,
    }
    return {
        "proposal_id": str(proposal.get("proposal_id") or ""),
        "baseline_decision": baseline_decision,
        "shadow_decision": shadow_decision,
        "decision_delta": "NO_ACTIVE_BEHAVIOR_CHANGE",
        "expected_trade_delta": "UNKNOWN_UNTIL_OBSERVED",
        "risk_delta": "LOW" if proposal.get("human_approval_status") != "APPROVED" else "MEDIUM",
        "missed_opportunity_delta": "UNKNOWN",
        "shadow_status": "PASS",
        "evidence_paths": [str(baseline_path), str(_proposal_path(ctx))],
        "operator_next_action": "Review shadow differences; no orders were submitted and baseline remains unchanged.",
    }


def build_shadow_evaluation_v1(ctx: bod.BodContext) -> dict[str, Any]:
    proposal_path = _proposal_path(ctx)
    proposal_payload = read_json_v1(proposal_path)
    proposals = proposal_payload.get("proposals") if isinstance(proposal_payload.get("proposals"), list) else []
    baseline_path = _baseline_path(ctx)
    baseline = read_json_v1(baseline_path)
    evaluations = [_evaluation(ctx, row, baseline, baseline_path) for row in proposals if isinstance(row, dict)] if baseline else []
    meaningful_status = "PASS" if evaluations else "NOT_APPLICABLE"
    return {
        "schema_id": "shadow_evaluation",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": meaningful_status,
        "canonical_blocker": "",
        "operator_next_action": "Review shadow evaluation before promotion." if evaluations else "No baseline strategy decision exists; shadow evaluation is not applicable.",
        "shadow_evaluations": evaluations,
        "proposal_id": str((evaluations[0] if evaluations else {}).get("proposal_id") or ""),
        "baseline_decision": (evaluations[0] if evaluations else {}).get("baseline_decision", {}),
        "shadow_decision": (evaluations[0] if evaluations else {}).get("shadow_decision", {}),
        "decision_delta": str((evaluations[0] if evaluations else {}).get("decision_delta") or ""),
        "expected_trade_delta": str((evaluations[0] if evaluations else {}).get("expected_trade_delta") or ""),
        "risk_delta": str((evaluations[0] if evaluations else {}).get("risk_delta") or "UNKNOWN"),
        "missed_opportunity_delta": str((evaluations[0] if evaluations else {}).get("missed_opportunity_delta") or "UNKNOWN"),
        "shadow_status": str((evaluations[0] if evaluations else {}).get("shadow_status") or "NOT_APPLICABLE"),
        "evidence_paths": [str(proposal_path), str(baseline_path)],
        "submit_allowed": False,
        "readiness_effect": "NONE",
        "active_strategy_mutation": False,
    }


def run_shadow_evaluation_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_shadow_evaluation_v1(ctx)
    path = shadow_evaluation_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_shadow_evaluation_v1.py", producer_command=f"python3 ops/tools/run_shadow_evaluation_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=payload["evidence_paths"], output_artifacts=[path], schema_versions={"shadow_evaluation": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_shadow_evaluation_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_shadow_evaluation_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "shadow_status": payload["shadow_status"], "shadow_evaluation_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
