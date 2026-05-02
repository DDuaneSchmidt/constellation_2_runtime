#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1


SCHEMAS = {
    "research_proposal_ledger": REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/research_proposal_ledger.v1.schema.json",
    "strategy_evidence_ledger": REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/strategy_evidence_ledger.v1.schema.json",
    "active_strategy_set": REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/active_strategy_set.v1.schema.json",
    "strategy_allocation_plan": REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/strategy_allocation_plan.v1.schema.json",
    "strategy_execution_eligibility": REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/strategy_execution_eligibility.v1.schema.json",
}


def _report_path(truth_root: Path, family: str, day_utc: str, filename: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / family / day_utc / filename).resolve()


def research_proposal_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return _report_path(truth_root, "research_proposal_ledger_v1", day_utc, "research_proposal_ledger.v1.json")


def strategy_evidence_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return _report_path(truth_root, "strategy_evidence_ledger_v1", day_utc, "strategy_evidence_ledger.v1.json")


def active_strategy_set_path(*, truth_root: Path, day_utc: str) -> Path:
    return _report_path(truth_root, "active_strategy_set_v1", day_utc, "active_strategy_set.v1.json")


def strategy_allocation_plan_path(*, truth_root: Path, day_utc: str) -> Path:
    return _report_path(truth_root, "strategy_allocation_plan_v1", day_utc, "strategy_allocation_plan.v1.json")


def strategy_execution_eligibility_path(*, truth_root: Path, day_utc: str) -> Path:
    return _report_path(truth_root, "strategy_execution_eligibility_v1", day_utc, "strategy_execution_eligibility.v1.json")


def sleeve_performance_control_path(*, truth_root: Path, day_utc: str) -> Path:
    return _report_path(truth_root, "sleeve_performance_control_v1", day_utc, "sleeve_performance_control.v1.json")


def _validate(payload: dict[str, Any], schema_id: str) -> None:
    schema = json.loads(SCHEMAS[schema_id].read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError(f"{schema_id.upper()}_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def _read_list(path: Path) -> list[dict[str, Any]]:
    payload = read_json_v1(path)
    rows = payload.get("items") if isinstance(payload.get("items"), list) else payload.get("proposals")
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]


def build_research_proposal_ledger_v1(*, day_utc: str, proposals: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in proposals or []:
        item = dict(row)
        item["status"] = "PROPOSED"
        rows.append(item)
    return {
        "schema_id": "research_proposal_ledger",
        "schema_version": "research_proposal_ledger.v1",
        "day_utc": day_utc,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS" if rows else "EMPTY",
        "authority": "RESEARCH_PROPOSAL_ONLY",
        "readiness_effect": "NONE",
        "submit_effect": "NONE",
        "proposals": rows,
    }


def build_strategy_evidence_ledger_v1(*, day_utc: str, research_ledger: dict[str, Any], evidence: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    proposal_ids = {
        str(row.get("proposal_id") or "")
        for row in research_ledger.get("proposals", [])
        if isinstance(row, dict)
    }
    rows: list[dict[str, Any]] = []
    for row in evidence or []:
        item = dict(row)
        blockers: list[str] = []
        if str(item.get("proposal_id") or "") not in proposal_ids:
            blockers.append("PROPOSAL_NOT_FOUND")
        if float(item.get("confidence_score") or 0) < 0.7:
            blockers.append("CONFIDENCE_BELOW_THRESHOLD")
        if str(item.get("approval_recommendation") or "").upper() != "APPROVE":
            blockers.append("EVIDENCE_DOES_NOT_RECOMMEND_APPROVAL")
        item["status"] = "PASS" if not blockers else ("REVIEW" if "CONFIDENCE_BELOW_THRESHOLD" in blockers else "FAIL")
        item["blockers"] = blockers
        rows.append(item)
    return {
        "schema_id": "strategy_evidence_ledger",
        "schema_version": "strategy_evidence_ledger.v1",
        "day_utc": day_utc,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS" if rows and all(row["status"] == "PASS" for row in rows) else ("EMPTY" if not rows else "BLOCKED"),
        "authority": "EVIDENCE_VALIDATION_ONLY",
        "approval_power": "NONE",
        "evidence": rows,
    }


def _evidence_by_strategy(evidence_ledger: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in evidence_ledger.get("evidence", []):
        if isinstance(row, dict):
            out[str(row.get("strategy_id") or "")] = row
    return out


def build_active_strategy_set_v1(
    *,
    day_utc: str,
    evidence_ledger: dict[str, Any],
    strategies: list[dict[str, Any]] | None = None,
    current_regime: str = "UNKNOWN",
) -> dict[str, Any]:
    evidence = _evidence_by_strategy(evidence_ledger)
    rows: list[dict[str, Any]] = []
    for row in strategies or []:
        item = dict(row)
        strategy_id = str(item.get("strategy_id") or "")
        ev = evidence.get(strategy_id, {})
        blockers: list[str] = []
        if ev.get("status") != "PASS":
            blockers.append("EVIDENCE_NOT_APPROVED_FOR_USE")
        if not str(item.get("approval_artifact") or "").strip():
            blockers.append("STRATEGY_APPROVAL_ARTIFACT_MISSING")
        approved_regimes = [str(value) for value in item.get("approved_regimes", [])]
        if current_regime not in approved_regimes:
            blockers.append("REGIME_MISMATCH")
        kill_rows = item.get("kill_criteria") if isinstance(item.get("kill_criteria"), list) else []
        if any(isinstance(kill, dict) and kill.get("triggered") is True for kill in kill_rows):
            blockers.append("STRATEGY_KILL_CRITERIA_TRIGGERED")
        item["status"] = "APPROVED_ACTIVE" if not blockers else "BLOCKED"
        item["current_lifecycle_state"] = "ACTIVE" if not blockers else ("DISABLED" if "STRATEGY_KILL_CRITERIA_TRIGGERED" in blockers else "BLOCKED")
        item["blockers"] = blockers
        rows.append(item)
    return {
        "schema_id": "active_strategy_set",
        "schema_version": "active_strategy_set.v1",
        "day_utc": day_utc,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS" if rows and all(row["current_lifecycle_state"] == "ACTIVE" for row in rows) else ("EMPTY" if not rows else "BLOCKED"),
        "authority": "STRATEGY_CONTROL_PLANE",
        "active_strategy_set": rows,
        "readiness_effect": "NONE",
        "submit_effect": "NONE",
    }


def _eligible_sleeves(sleeve_performance_control: dict[str, Any] | None) -> set[str]:
    if not isinstance(sleeve_performance_control, dict) or not sleeve_performance_control:
        return set()
    if str(sleeve_performance_control.get("authority") or "") != "SLEEVE_PERFORMANCE_CONTROL":
        return set()
    if Path(str(sleeve_performance_control.get("truth_root") or "/")).name != "production_truth":
        return set()
    rows = sleeve_performance_control.get("sleeve_results") if isinstance(sleeve_performance_control.get("sleeve_results"), list) else []
    return {
        str(row.get("sleeve_id") or "").strip().upper()
        for row in rows
        if isinstance(row, dict) and row.get("allocation_eligible") is True
    }


def build_strategy_allocation_plan_v1(
    *,
    day_utc: str,
    active_strategy_set: dict[str, Any],
    allocations: list[dict[str, Any]] | None = None,
    sleeve_performance_control: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_rows = {
        str(row.get("strategy_id") or ""): row
        for row in active_strategy_set.get("active_strategy_set", [])
        if isinstance(row, dict) and row.get("current_lifecycle_state") == "ACTIVE"
    }
    eligible_sleeves = _eligible_sleeves(sleeve_performance_control)
    rows: list[dict[str, Any]] = []
    for row in allocations or []:
        item = dict(row)
        blockers: list[str] = []
        strategy_id = str(item.get("strategy_id") or "")
        active = active_rows.get(strategy_id)
        if active is None:
            blockers.append("STRATEGY_NOT_ACTIVE_APPROVED")
        sleeve_id = str(item.get("sleeve_id") or (active or {}).get("sleeve_id") or strategy_id).strip().upper()
        item["sleeve_id"] = sleeve_id
        if not isinstance(sleeve_performance_control, dict) or not sleeve_performance_control:
            blockers.append("SLEEVE_PERFORMANCE_CONTROL_MISSING")
        elif str(sleeve_performance_control.get("authority") or "") != "SLEEVE_PERFORMANCE_CONTROL":
            blockers.append("SLEEVE_PERFORMANCE_CONTROL_INVALID_AUTHORITY")
        elif Path(str(sleeve_performance_control.get("truth_root") or "/")).name != "production_truth":
            blockers.append("SLEEVE_PERFORMANCE_CONTROL_NOT_PRODUCTION_TRUTH")
        elif sleeve_id not in eligible_sleeves:
            blockers.append("SLEEVE_NOT_PERFORMANCE_ELIGIBLE")
        risk = item.get("risk_budget") if isinstance(item.get("risk_budget"), dict) else {}
        if float(risk.get("max_capital_at_risk_pct") or 0) <= 0:
            blockers.append("RISK_BUDGET_NOT_POSITIVE")
        item["status"] = "APPROVED" if not blockers else "BLOCKED"
        item["blockers"] = blockers
        rows.append(item)
    return {
        "schema_id": "strategy_allocation_plan",
        "schema_version": "strategy_allocation_plan.v1",
        "day_utc": day_utc,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS" if rows and all(row["status"] == "APPROVED" for row in rows) else ("EMPTY" if not rows else "BLOCKED"),
        "authority": "ALLOCATION_PLANE",
        "safety_override_power": "NONE",
        "performance_control_path": str(sleeve_performance_control.get("artifact_path") or "") if isinstance(sleeve_performance_control, dict) else "",
        "allocations": rows,
    }


def build_strategy_execution_eligibility_v1(
    *,
    day_utc: str,
    truth_root: Path,
    strategy_id: str,
    active_strategy_set: dict[str, Any],
    allocation_plan: dict[str, Any],
) -> dict[str, Any]:
    control_path = _report_path(truth_root, "aegis_control_plane_v1", day_utc, "control_plane.v1.json")
    control = read_json_v1(control_path)
    active = {
        str(row.get("strategy_id") or ""): row
        for row in active_strategy_set.get("active_strategy_set", [])
        if isinstance(row, dict)
    }
    allocations = {
        str(row.get("strategy_id") or ""): row
        for row in allocation_plan.get("allocations", [])
        if isinstance(row, dict)
    }
    blockers: list[dict[str, str]] = []
    if control.get("final_status") != "READY" or control.get("submit_allowed") is not True:
        blockers.append(
            {
                "code": "AEGIS_CONTROL_PLANE_NOT_READY",
                "control_plane_path": str(control_path),
                "final_status": str(control.get("final_status") or "MISSING"),
                "canonical_blocker": str(control.get("canonical_blocker") or ""),
            }
        )
    if active.get(strategy_id, {}).get("current_lifecycle_state") != "ACTIVE":
        blockers.append({"code": "STRATEGY_NOT_APPROVED_ACTIVE", "strategy_id": strategy_id})
    if allocations.get(strategy_id, {}).get("status") != "APPROVED":
        blockers.append({"code": "STRATEGY_ALLOCATION_NOT_APPROVED", "strategy_id": strategy_id})
    return {
        "schema_id": "strategy_execution_eligibility",
        "schema_version": "strategy_execution_eligibility.v1",
        "day_utc": day_utc,
        "generated_at_utc": now_iso_v1(),
        "strategy_id": strategy_id,
        "status": "ALLOWED" if not blockers else "BLOCKED",
        "submit_allowed": not blockers,
        "authority": "AEGIS_EXECUTION_PLANE_VIEW",
        "aegis_readiness_authority": "aegis_control_plane_v1",
        "blockers": blockers,
    }


def _attach_and_write(*, payload: dict[str, Any], path: Path, producer_name: str, command: str, inputs: list[Path], schema_id: str) -> tuple[Path, dict[str, Any]]:
    attach_producer_contract_v1(
        payload,
        producer_name=producer_name,
        producer_command=command,
        input_artifacts=inputs,
        output_artifacts=[path],
        schema_versions={schema_id: f"{schema_id}.v1"},
    )
    _validate(payload, schema_id)
    write_json_v1(path, payload)
    return path, payload


def run_all_v1(day_utc: str, truth_root: Path) -> dict[str, Any]:
    day = parse_day_utc_v1(day_utc)
    root = Path(truth_root).resolve()
    research = build_research_proposal_ledger_v1(day_utc=day, proposals=[])
    research_path, research = _attach_and_write(
        payload=research,
        path=research_proposal_ledger_path(truth_root=root, day_utc=day),
        producer_name="ops/tools/run_strategy_edge_governance_v1.py",
        command=f"python3 ops/tools/run_strategy_edge_governance_v1.py --day_utc {day}",
        inputs=[],
        schema_id="research_proposal_ledger",
    )
    evidence = build_strategy_evidence_ledger_v1(day_utc=day, research_ledger=research, evidence=[])
    evidence_path, evidence = _attach_and_write(
        payload=evidence,
        path=strategy_evidence_ledger_path(truth_root=root, day_utc=day),
        producer_name="ops/tools/run_strategy_edge_governance_v1.py",
        command=f"python3 ops/tools/run_strategy_edge_governance_v1.py --day_utc {day}",
        inputs=[research_path],
        schema_id="strategy_evidence_ledger",
    )
    active = build_active_strategy_set_v1(day_utc=day, evidence_ledger=evidence, strategies=[], current_regime="UNKNOWN")
    active_path, active = _attach_and_write(
        payload=active,
        path=active_strategy_set_path(truth_root=root, day_utc=day),
        producer_name="ops/tools/run_strategy_edge_governance_v1.py",
        command=f"python3 ops/tools/run_strategy_edge_governance_v1.py --day_utc {day}",
        inputs=[evidence_path],
        schema_id="active_strategy_set",
    )
    performance_path = sleeve_performance_control_path(truth_root=root, day_utc=day)
    performance_control = read_json_v1(performance_path)
    allocation = build_strategy_allocation_plan_v1(day_utc=day, active_strategy_set=active, allocations=[], sleeve_performance_control=performance_control)
    allocation_path, allocation = _attach_and_write(
        payload=allocation,
        path=strategy_allocation_plan_path(truth_root=root, day_utc=day),
        producer_name="ops/tools/run_strategy_edge_governance_v1.py",
        command=f"python3 ops/tools/run_strategy_edge_governance_v1.py --day_utc {day}",
        inputs=[active_path, performance_path],
        schema_id="strategy_allocation_plan",
    )
    eligibility = build_strategy_execution_eligibility_v1(
        day_utc=day,
        truth_root=root,
        strategy_id="NONE",
        active_strategy_set=active,
        allocation_plan=allocation,
    )
    eligibility_path, eligibility = _attach_and_write(
        payload=eligibility,
        path=strategy_execution_eligibility_path(truth_root=root, day_utc=day),
        producer_name="ops/tools/run_strategy_edge_governance_v1.py",
        command=f"python3 ops/tools/run_strategy_edge_governance_v1.py --day_utc {day}",
        inputs=[active_path, allocation_path, _report_path(root, "aegis_control_plane_v1", day, "control_plane.v1.json")],
        schema_id="strategy_execution_eligibility",
    )
    return {
        "research_proposal_ledger_path": str(research_path),
        "strategy_evidence_ledger_path": str(evidence_path),
        "active_strategy_set_path": str(active_path),
        "strategy_allocation_plan_path": str(allocation_path),
        "strategy_execution_eligibility_path": str(eligibility_path),
        "execution_status": eligibility["status"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    result = run_all_v1(parse_day_utc_v1(args.day_utc), truth_root)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
