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

from ops.tools.aegis_runtime_mode_v1 import CANDIDATE_TRUTH_ROOT, PRODUCTION_TRUTH_ROOT, git_commit_v1, read_json_v1, write_json_v1, now_iso_v1
from ops.tools.run_aegis_promotion_candidate_v1 import promotion_candidate_path

SCHEMA_VERSION = "aegis_production_promotion_gate.v1"


def approval_path(candidate_root: Path, promotion_id: str) -> Path:
    return (candidate_root / "governance" / "promotion_approvals" / f"{promotion_id}.json").resolve()


def promotion_gate_path(candidate_root: Path, day_utc: str, promotion_id: str) -> Path:
    return (candidate_root / "reports" / "aegis_production_promotion_gate_v1" / day_utc / f"{promotion_id}.json").resolve()


def build_promotion_gate_v1(*, day_utc: str, promotion_id: str, candidate_root: Path, production_root: Path) -> dict[str, Any]:
    del production_root
    candidate = read_json_v1(promotion_candidate_path(candidate_root=candidate_root, day_utc=day_utc))
    approval = read_json_v1(approval_path(candidate_root, promotion_id))
    required = [
        "repo_local_import_proof",
        "candidate_tests_pass",
        "candidate_packet_current",
        "no_readiness_regression",
        "submit_firewall_fail_closed",
        "control_plane_one_blocker",
        "operator_projection_has_evidence",
        "explicit_human_approval",
    ]
    satisfied: list[str] = []
    blockers: list[dict[str, str]] = []
    if candidate:
        satisfied.append("repo_local_import_proof")
    else:
        blockers.append({"code": "PROMOTION_CANDIDATE_MISSING"})
    tests = candidate.get("tests_run") if isinstance(candidate.get("tests_run"), list) else []
    if tests and all("failed" not in str(item).lower() for item in tests):
        satisfied.append("candidate_tests_pass")
    else:
        blockers.append({"code": "CANDIDATE_TESTS_NOT_PROVEN_PASS"})
    packet = candidate.get("packet_currentness") if isinstance(candidate.get("packet_currentness"), dict) else {}
    candidate_packet = packet.get("candidate") if isinstance(packet.get("candidate"), dict) else {}
    if candidate_packet.get("status") == "CURRENT":
        satisfied.append("candidate_packet_current")
    else:
        blockers.append({"code": "CANDIDATE_PACKET_STALE"})
    if candidate.get("regression_detected") is False:
        satisfied.append("no_readiness_regression")
    else:
        blockers.append({"code": "READINESS_REGRESSION"})
    cand_blockers = candidate.get("blockers") if isinstance(candidate.get("blockers"), list) else []
    if not any(isinstance(row, dict) and row.get("code") == "SUBMIT_PERMISSION_EXPANSION_REQUIRES_APPROVAL" for row in cand_blockers):
        satisfied.append("submit_firewall_fail_closed")
    else:
        blockers.append({"code": "SUBMIT_PERMISSION_EXPANSION"})
    evidence = candidate.get("evidence_paths") if isinstance(candidate.get("evidence_paths"), list) else []
    if evidence:
        satisfied.extend(["control_plane_one_blocker", "operator_projection_has_evidence"])
    else:
        blockers.append({"code": "PROMOTION_EVIDENCE_MISSING"})
    if approval.get("status") == "APPROVED" and approval.get("promotion_id") == promotion_id and approval.get("candidate_commit") == git_commit_v1():
        satisfied.append("explicit_human_approval")
    elif approval.get("status") == "REJECTED":
        blockers.append({"code": "HUMAN_APPROVAL_REJECTED"})
    else:
        blockers.append({"code": "HUMAN_APPROVAL_MISSING"})
    status = "APPROVED_FOR_PROMOTION" if not blockers else ("REJECTED" if any(row["code"] == "HUMAN_APPROVAL_REJECTED" for row in blockers) else "BLOCKED")
    return {
        "schema_id": "aegis_production_promotion_gate",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "promotion_id": promotion_id,
        "generated_at_utc": now_iso_v1(),
        "promotion_status": status,
        "blockers": blockers,
        "required_conditions": required,
        "satisfied_conditions": sorted(set(satisfied)),
        "rollback_commit": str(approval.get("rollback_commit") or ""),
        "operator_next_action": "Run promote_aegis_candidate_to_production_v1.py." if status == "APPROVED_FOR_PROMOTION" else "Resolve promotion gate blockers; do not promote.",
        "approval_path": str(approval_path(candidate_root, promotion_id)),
        "candidate_report_path": str(promotion_candidate_path(candidate_root=candidate_root, day_utc=day_utc)),
    }


def run_promotion_gate_v1(day_utc: str, promotion_id: str, candidate_root: str = "", production_root: str = "") -> tuple[Path, dict[str, Any]]:
    candidate = Path(candidate_root or CANDIDATE_TRUTH_ROOT).resolve()
    production = Path(production_root or PRODUCTION_TRUTH_ROOT).resolve()
    payload = build_promotion_gate_v1(day_utc=day_utc, promotion_id=promotion_id, candidate_root=candidate, production_root=production)
    path = promotion_gate_path(candidate, day_utc, promotion_id)
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--promotion_id", required=True)
    parser.add_argument("--candidate_root", default="")
    parser.add_argument("--production_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_promotion_gate_v1(args.day_utc, args.promotion_id, args.candidate_root, args.production_root)
    print(json.dumps({"promotion_gate_path": str(path), "promotion_status": payload["promotion_status"], "blockers": payload["blockers"]}, sort_keys=True))
    return 0 if payload["promotion_status"] == "APPROVED_FOR_PROMOTION" else 2


if __name__ == "__main__":
    raise SystemExit(main())
