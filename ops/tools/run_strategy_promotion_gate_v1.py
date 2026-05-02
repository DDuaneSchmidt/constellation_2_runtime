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
from ops.tools.aegis_truth_integrity_common_v1 import blocker_of_v1, now_iso_v1, read_json_v1, report_path_v1, status_of_v1, write_json_v1

SCHEMA_VERSION = "strategy_promotion_gate.v1"


def strategy_promotion_gate_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "strategy_promotion_gate_v1" / day_utc / "strategy_promotion_gate.v1.json").resolve()


def _first(rows: Any) -> dict[str, Any]:
    return rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}


def _action_allowed(action_validity: dict[str, Any]) -> bool:
    rows = action_validity.get("action_rules") if isinstance(action_validity.get("action_rules"), list) else []
    return any(isinstance(row, dict) and row.get("action_id") == "promote_strategy_change" and row.get("status") == "ALLOWED" for row in rows)


def build_strategy_promotion_gate_v1(ctx: bod.BodContext) -> dict[str, Any]:
    proposal_path = report_path_v1(ctx, "strategy_change_proposal_v1", "strategy_change_proposal.v1.json")
    shadow_path = report_path_v1(ctx, "shadow_evaluation_v1", "shadow_evaluation.v1.json")
    consistency_path = report_path_v1(ctx, "state_consistency_v1", "state_consistency.v1.json")
    kernel_path = report_path_v1(ctx, "unified_truth_kernel_v1", "unified_truth_kernel.v1.json")
    action_path = report_path_v1(ctx, "action_validity_v1", "action_validity.v1.json")
    proposal = _first(read_json_v1(proposal_path).get("proposals"))
    shadow = read_json_v1(shadow_path)
    consistency = read_json_v1(consistency_path)
    kernel = read_json_v1(kernel_path)
    action_validity = read_json_v1(action_path)
    conditions = [
        "human_approval",
        "shadow_evaluation_pass",
        "rollback_plan_present",
        "state_consistency_pass",
        "risk_delta_acceptable",
        "unified_truth_kernel_no_hard_blocker",
        "action_validity_allows_promotion",
    ]
    satisfied: list[str] = []
    blockers: list[str] = []
    if proposal.get("human_approval_status") == "APPROVED":
        satisfied.append("human_approval")
    else:
        blockers.append("HUMAN_APPROVAL_MISSING")
    if shadow.get("shadow_status") == "PASS":
        satisfied.append("shadow_evaluation_pass")
    else:
        blockers.append("SHADOW_EVALUATION_NOT_PASS")
    if proposal.get("rollback_plan"):
        satisfied.append("rollback_plan_present")
    else:
        blockers.append("ROLLBACK_PLAN_MISSING")
    if status_of_v1(consistency) == "PASS":
        satisfied.append("state_consistency_pass")
    else:
        blockers.append("STATE_CONSISTENCY_NOT_PASS")
    if str(shadow.get("risk_delta") or "UNKNOWN") in {"LOW", "MEDIUM"}:
        satisfied.append("risk_delta_acceptable")
    else:
        blockers.append("RISK_DELTA_UNACCEPTABLE")
    if not blocker_of_v1(kernel):
        satisfied.append("unified_truth_kernel_no_hard_blocker")
    else:
        blockers.append("UNIFIED_TRUTH_KERNEL_BLOCKED")
    if _action_allowed(action_validity):
        satisfied.append("action_validity_allows_promotion")
    else:
        blockers.append("ACTION_VALIDITY_PROMOTION_NOT_ALLOWED")
    promotion_status = "APPROVED_FOR_PROMOTION" if not blockers else "BLOCKED"
    return {
        "schema_id": "strategy_promotion_gate",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS" if promotion_status == "APPROVED_FOR_PROMOTION" else "BLOCKED",
        "canonical_blocker": blockers[0] if blockers else "",
        "operator_next_action": "Promotion is eligible for governed activation." if not blockers else "Resolve promotion blockers; no strategy state has changed.",
        "proposal_id": str(proposal.get("proposal_id") or ""),
        "promotion_status": promotion_status,
        "blockers": blockers,
        "required_conditions": conditions,
        "satisfied_conditions": satisfied,
        "human_approval_evidence": str(proposal.get("human_approval_status") or "MISSING"),
        "rollback_plan_present": bool(proposal.get("rollback_plan")),
        "submit_allowed": False,
        "readiness_effect": "NONE",
        "active_strategy_mutation": False,
        "evidence_paths": [str(proposal_path), str(shadow_path), str(consistency_path), str(kernel_path), str(action_path)],
    }


def run_strategy_promotion_gate_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_strategy_promotion_gate_v1(ctx)
    path = strategy_promotion_gate_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_strategy_promotion_gate_v1.py", producer_command=f"python3 ops/tools/run_strategy_promotion_gate_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=payload["evidence_paths"], output_artifacts=[path], schema_versions={"strategy_promotion_gate": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_strategy_promotion_gate_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_strategy_promotion_gate_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"promotion_status": payload["promotion_status"], "canonical_blocker": payload["canonical_blocker"], "strategy_promotion_gate_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
