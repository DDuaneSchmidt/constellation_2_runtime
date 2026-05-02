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

from ops.tools.aegis_runtime_mode_v1 import (
    CANDIDATE_TRUTH_ROOT,
    PRODUCTION_TRUTH_ROOT,
    git_commit_v1,
    read_json_v1,
    read_production_version_v1,
    write_json_v1,
    now_iso_v1,
)
from ops.tools.aegis_submit_enforcement_v1 import packet_currentness_v1

SCHEMA_VERSION = "aegis_promotion_candidate.v1"
READY = {"READY", "PRE_MARKET_READY", "PAPER_READY", "PAPER_READY_WITH_DELAYED_DATA", "TRADING_ACTIVE", "EOD_COMPLETE"}


def promotion_candidate_path(*, candidate_root: Path, day_utc: str) -> Path:
    return (candidate_root / "reports" / "aegis_promotion_candidate_v1" / day_utc / "promotion_candidate.v1.json").resolve()


def _report(root: Path, family: str, day: str, filename: str) -> Path:
    return (root / "reports" / family / day / filename).resolve()


def _status(payload: dict[str, Any]) -> str:
    for key in ("final_status", "status", "boundary_status"):
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    return "MISSING"


def _submit_allowed(root: Path, day: str) -> bool:
    boundary = read_json_v1(_report(root, "submit_boundary_status_v1", day, "submit_boundary_status.v1.json"))
    action = read_json_v1(_report(root, "action_validity_v1", day, "action_validity.v1.json"))
    action_rows = action.get("action_rules") if isinstance(action.get("action_rules"), list) else []
    action_allowed = any(isinstance(row, dict) and row.get("action_id") == "submit_paper_order" and row.get("status") == "ALLOWED" for row in action_rows)
    return bool((boundary.get("submit_allowed") is True or boundary.get("submission_authorized") is True) and action_allowed)


def build_promotion_candidate_v1(*, day_utc: str, candidate_root: Path, production_root: Path, tests_run: list[str] | None = None) -> dict[str, Any]:
    candidate_commit = git_commit_v1()
    production_version = read_production_version_v1(production_root)
    production_commit = str(production_version.get("promoted_commit") or "")
    candidate_ledger = read_json_v1(_report(candidate_root, "aegis_day_run_v1", day_utc, "day_run.v1.json"))
    production_ledger = read_json_v1(_report(production_root, "aegis_day_run_v1", day_utc, "day_run.v1.json"))
    candidate_control = read_json_v1(_report(candidate_root, "aegis_control_plane_v1", day_utc, "control_plane.v1.json"))
    production_control = read_json_v1(_report(production_root, "aegis_control_plane_v1", day_utc, "control_plane.v1.json"))
    candidate_packet = packet_currentness_v1(runtime_root=candidate_root, runtime_mode="CANDIDATE")
    production_packet = packet_currentness_v1(runtime_root=production_root, runtime_mode="PRODUCTION")
    candidate_status = _status(candidate_ledger)
    production_status = _status(production_ledger)
    candidate_submit = _submit_allowed(candidate_root, day_utc)
    production_submit = _submit_allowed(production_root, day_utc)
    blockers: list[dict[str, str]] = []
    regression = False
    if production_status in READY and candidate_status not in READY:
        regression = True
        blockers.append({"code": "READINESS_REGRESSION", "candidate_status": candidate_status, "production_status": production_status})
    if candidate_packet["status"] != "CURRENT":
        blockers.append({"code": "CANDIDATE_PACKET_NOT_CURRENT", "path": str(candidate_packet.get("path") or "")})
    if candidate_submit and not production_submit:
        blockers.append({"code": "SUBMIT_PERMISSION_EXPANSION_REQUIRES_APPROVAL"})
    recommendation = "APPROVE" if not blockers else ("BLOCK" if regression or candidate_packet["status"] != "CURRENT" else "REVIEW")
    evidence_paths = [
        str(_report(candidate_root, "aegis_day_run_v1", day_utc, "day_run.v1.json")),
        str(_report(candidate_root, "aegis_control_plane_v1", day_utc, "control_plane.v1.json")),
        str(_report(candidate_root, "submit_boundary_status_v1", day_utc, "submit_boundary_status.v1.json")),
        str(_report(candidate_root, "action_validity_v1", day_utc, "action_validity.v1.json")),
        str(_report(candidate_root, "truth_freshness_v1", day_utc, "truth_freshness.v1.json")),
        str(_report(candidate_root, "state_consistency_v1", day_utc, "state_consistency.v1.json")),
        str(_report(candidate_root, "aegis_requirement_graph_v1", day_utc, "requirement_graph.v1.json")),
        str(_report(candidate_root, "aegis_operator_projection_v1", day_utc, "operator_projection.v1.json")),
    ]
    return {
        "schema_id": "aegis_promotion_candidate",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": now_iso_v1(),
        "candidate_commit": candidate_commit,
        "production_commit": production_commit,
        "candidate_status": candidate_status,
        "production_status": production_status,
        "readiness_delta": f"{production_status}->{candidate_status}",
        "blocker_delta": f"{production_control.get('canonical_blocker','')}->{candidate_control.get('canonical_blocker','')}",
        "submit_permission_delta": f"{production_submit}->{candidate_submit}",
        "regression_detected": bool(regression),
        "promotion_recommendation": recommendation,
        "blockers": blockers,
        "evidence_paths": evidence_paths,
        "tests_run": tests_run or [],
        "packet_currentness": {"candidate": candidate_packet, "production": production_packet},
    }


def run_promotion_candidate_v1(day_utc: str, candidate_root: str = "", production_root: str = "", tests_run: list[str] | None = None) -> tuple[Path, dict[str, Any]]:
    candidate = Path(candidate_root or CANDIDATE_TRUTH_ROOT).resolve()
    production = Path(production_root or PRODUCTION_TRUTH_ROOT).resolve()
    payload = build_promotion_candidate_v1(day_utc=day_utc, candidate_root=candidate, production_root=production, tests_run=tests_run)
    path = promotion_candidate_path(candidate_root=candidate, day_utc=day_utc)
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--candidate_root", default="")
    parser.add_argument("--production_root", default="")
    parser.add_argument("--test", action="append", default=[])
    args = parser.parse_args(argv)
    path, payload = run_promotion_candidate_v1(args.day_utc, args.candidate_root, args.production_root, args.test)
    print(json.dumps({"promotion_candidate_path": str(path), "promotion_recommendation": payload["promotion_recommendation"], "regression_detected": payload["regression_detected"]}, sort_keys=True))
    return 0 if payload["promotion_recommendation"] == "APPROVE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
