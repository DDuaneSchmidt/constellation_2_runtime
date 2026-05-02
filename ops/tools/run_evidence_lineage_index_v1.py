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
from ops.tools.aegis_truth_integrity_common_v1 import (
    artifact_specs_v1,
    blocker_of_v1,
    generated_at_v1,
    now_iso_v1,
    read_json_v1,
    report_path_v1,
    write_json_v1,
)

SCHEMA_VERSION = "evidence_lineage_index.v1"
PHASE_RANK = {
    "SOURCE_INTEGRITY": 0,
    "BROKER_HEALTH": 1,
    "BOD_INPUTS": 2,
    "SESSION_AUTHORITY": 3,
    "MARKET_DATA_BOD_PREP": 4,
    "MARKET_OPEN_DATA_GATE": 5,
    "STRATEGY_AND_RISK": 6,
    "AUTHORIZATION": 7,
    "SUBMIT_BOUNDARY": 8,
}
ARTIFACT_PHASE = {
    "aegis_day_run_v1": "SOURCE_INTEGRITY",
    "broker_supply_v1": "BROKER_HEALTH",
    "capital_supply_v1": "BOD_INPUTS",
    "aegis_requirement_graph_v1": "MARKET_DATA_BOD_PREP",
    "market_data_supply_v1": "MARKET_DATA_BOD_PREP",
    "market_open_data_gate_v1": "MARKET_OPEN_DATA_GATE",
    "risk_budget_supply_v1": "STRATEGY_AND_RISK",
    "authorization_supply_v1": "AUTHORIZATION",
    "submit_boundary_status_v1": "SUBMIT_BOUNDARY",
    "action_validity_v1": "SUBMIT_BOUNDARY",
}


def evidence_lineage_index_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "evidence_lineage_index_v1" / day_utc / "evidence_lineage_index.v1.json").resolve()


def _input_refs(contract: dict[str, Any]) -> list[dict[str, Any]]:
    rows = contract.get("input_artifacts") if isinstance(contract.get("input_artifacts"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _required_now(ctx: bod.BodContext, spec: dict[str, Any]) -> bool:
    if not spec.get("authoritative"):
        return False
    artifact_type = str(spec.get("artifact_type") or "")
    if artifact_type == "aegis_day_run_v1":
        return True
    ledger = read_json_v1(report_path_v1(ctx, "aegis_day_run_v1", "day_run.v1.json"))
    blocked_phase = str(ledger.get("canonical_phase") or "").strip()
    if not blocked_phase:
        return True
    artifact_phase = ARTIFACT_PHASE.get(artifact_type, "SUBMIT_BOUNDARY")
    return PHASE_RANK.get(artifact_phase, 99) <= PHASE_RANK.get(blocked_phase, 99)


def _optional_or_downstream_missing_input(ctx: bod.BodContext, row: dict[str, Any]) -> bool:
    path = str(row.get("path") or "")
    if "/execution_evidence_v1/submissions/" in path:
        return True
    if "/trading_day_closure_authority_v1/" in path:
        return True
    if "/ib_broker_event_probe_v1/" in path:
        return True
    ledger = read_json_v1(report_path_v1(ctx, "aegis_day_run_v1", "day_run.v1.json"))
    blocked_phase = str(ledger.get("canonical_phase") or "").strip()
    if blocked_phase and PHASE_RANK.get(blocked_phase, 99) < PHASE_RANK["SUBMIT_BOUNDARY"]:
        return any(token in path for token in ("/authorization", "/submit", "/strategy", "/trading_day_closure"))
    return False


def _node(ctx: bod.BodContext, spec: dict[str, Any]) -> dict[str, Any]:
    path = report_path_v1(ctx, str(spec["family"]), str(spec["filename"]))
    payload = read_json_v1(path)
    contract = payload.get("producer_contract_v1") if isinstance(payload.get("producer_contract_v1"), dict) else {}
    inputs = _input_refs(contract)
    missing_inputs = [row for row in inputs if row.get("exists") is False]
    hard_missing_inputs = [row for row in missing_inputs if not _optional_or_downstream_missing_input(ctx, row)]
    authoritative = bool(spec.get("authoritative"))
    blocking_class = str(spec.get("blocking_class") or "HARD_BLOCKER")
    required_now = _required_now(ctx, spec)
    if authoritative and not required_now:
        status = "PASS"
        blocker = ""
        action = "Artifact is downstream of the current day-run blocker and is not required for this blocked state."
    elif not path.exists():
        status = "FAIL" if authoritative else "WARN"
        blocker = f"{spec['artifact_type'].upper()}_MISSING"
        action = f"Run the producer for {spec['artifact_type']}."
    elif not contract:
        status = "FAIL" if authoritative else "WARN"
        blocker = "PRODUCER_CONTRACT_MISSING" if authoritative else ""
        action = "Add producer_contract_v1 metadata to this artifact producer."
    elif hard_missing_inputs:
        status = "FAIL" if authoritative else "WARN"
        blocker = "LINEAGE_INPUT_ARTIFACT_MISSING" if authoritative else ""
        action = (
            "Regenerate missing authoritative lineage input artifacts, then rerun the producer."
            if authoritative
            else "Optional diagnostic inputs are missing; rerun upstream advisory producers if this evidence is needed."
        )
    elif missing_inputs:
        status = "PASS" if authoritative else "WARN"
        blocker = ""
        action = (
            "Lineage is complete for required inputs; optional or downstream inputs are absent for the current blocked state."
            if authoritative
            else "Only optional or downstream lineage inputs are missing for the current blocked state."
        )
    elif not inputs:
        status = "UNKNOWN"
        blocker = ""
        action = "Producer contract declares no inputs; lineage cannot be proven."
    else:
        status = "PASS"
        blocker = ""
        action = ""
    return {
        "artifact_path": str(path),
        "artifact_type": str(spec["artifact_type"]),
        "schema_version": str(payload.get("schema_version") or ""),
        "producer_name": str(contract.get("producer_name") or ""),
        "producer_command": str(contract.get("producer_command") or ""),
        "producer_contract_present": bool(contract),
        "generated_at_utc": generated_at_v1(payload),
        "input_artifacts": inputs,
        "upstream_artifacts": [str(row.get("path") or "") for row in inputs if str(row.get("path") or "").strip()],
        "root_evidence": [str(row.get("path") or "") for row in inputs if row.get("exists") is True],
        "source_git_commit": str(contract.get("code_version_git_commit") or ""),
        "source_dirty_status": str(contract.get("source_dirty_status") or ""),
        "runtime_contract_path": str(contract.get("runtime_contract_path") or ""),
        "deterministic_fingerprint": str(contract.get("deterministic_fingerprint") or ""),
        "blocking_class": blocking_class,
        "authoritative": authoritative,
        "required_for_current_day": required_now,
        "lineage_status": status,
        "canonical_blocker": blocker or blocker_of_v1(payload),
        "operator_next_action": action,
    }


def build_evidence_lineage_index_v1(ctx: bod.BodContext) -> dict[str, Any]:
    nodes = [_node(ctx, spec) for spec in artifact_specs_v1()]
    hard_failures = [row for row in nodes if row["lineage_status"] == "FAIL" and row["blocking_class"] == "HARD_BLOCKER"]
    warn_rows = [row for row in nodes if row["lineage_status"] in {"WARN", "UNKNOWN"}]
    status = "FAIL" if hard_failures else ("WARN" if warn_rows else "PASS")
    blocker = str(hard_failures[0].get("canonical_blocker") or "EVIDENCE_LINEAGE_FAILED") if hard_failures else ""
    return {
        "schema_id": "evidence_lineage_index",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": status,
        "canonical_blocker": blocker,
        "operator_next_action": "Resolve failed authoritative lineage nodes." if blocker else "",
        "lineage_nodes": nodes,
    }


def run_evidence_lineage_index_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_evidence_lineage_index_v1(ctx)
    path = evidence_lineage_index_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_evidence_lineage_index_v1.py",
        producer_command=f"python3 ops/tools/run_evidence_lineage_index_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=[row["artifact_path"] for row in payload["lineage_nodes"]],
        output_artifacts=[path],
        schema_versions={"evidence_lineage_index": SCHEMA_VERSION},
    )
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_evidence_lineage_index_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_evidence_lineage_index_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "evidence_lineage_index_path": str(path)}, sort_keys=True))
    return 0 if payload["status"] != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
