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

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_truth_integrity_common_v1 import artifact_specs_v1, generated_at_v1, now_iso_v1, parse_iso_v1, read_json_v1, report_path_v1, write_json_v1

SCHEMA_VERSION = "truth_freshness.v1"
POLICIES = {
    "HARD_BLOCKER": {"freshness_policy_id": "current_day_authoritative_max_24h", "max_age_seconds": 86400, "session_scope": "TRADING_DAY"},
    "DIAGNOSTIC_ONLY": {"freshness_policy_id": "diagnostic_max_48h", "max_age_seconds": 172800, "session_scope": "REPORTING"},
}
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


def truth_freshness_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "truth_freshness_v1" / day_utc / "truth_freshness.v1.json").resolve()


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


def _record(ctx: bod.BodContext, spec: dict[str, Any], observed: datetime) -> dict[str, Any]:
    path = report_path_v1(ctx, str(spec["family"]), str(spec["filename"]))
    payload = read_json_v1(path)
    blocking_class = str(spec.get("blocking_class") or "HARD_BLOCKER")
    required_now = _required_now(ctx, spec)
    policy = POLICIES.get(blocking_class)
    generated = generated_at_v1(payload)
    generated_dt = parse_iso_v1(generated)
    age = int((observed - generated_dt).total_seconds()) if generated_dt else None
    if not required_now and blocking_class == "HARD_BLOCKER":
        status = "NOT_REQUIRED"
        blocker = ""
        action = ""
    elif not path.exists():
        status = "UNKNOWN"
        blocker = f"{spec['artifact_type'].upper()}_MISSING" if blocking_class == "HARD_BLOCKER" else ""
        action = f"Generate {spec['artifact_type']} before evaluating freshness."
    elif not policy:
        status = "UNKNOWN"
        blocker = ""
        action = "Attach explicit freshness policy for this artifact type."
    elif not generated_dt:
        status = "UNKNOWN"
        blocker = "GENERATED_AT_UTC_MISSING" if blocking_class == "HARD_BLOCKER" else ""
        action = "Regenerate artifact with generated_at_utc or producer contract timestamp."
    elif age is not None and age > int(policy["max_age_seconds"]):
        status = "EXPIRED"
        blocker = "TRUTH_ARTIFACT_EXPIRED" if blocking_class == "HARD_BLOCKER" else ""
        action = "Regenerate expired authoritative artifact." if blocker else "Refresh diagnostic artifact when useful."
    else:
        status = "FRESH"
        blocker = ""
        action = ""
    return {
        "artifact_path": str(path),
        "artifact_type": str(spec["artifact_type"]),
        "generated_at_utc": generated,
        "observed_at_utc": observed.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "freshness_policy_id": str((policy or {}).get("freshness_policy_id") or ""),
        "max_age_seconds": int((policy or {}).get("max_age_seconds") or 0),
        "session_scope": str((policy or {}).get("session_scope") or ""),
        "trading_day": ctx.day_utc,
        "freshness_status": status,
        "age_seconds": age,
        "blocking_class": blocking_class,
        "required_for_current_day": required_now,
        "canonical_blocker": blocker,
        "operator_next_action": action,
    }


def build_truth_freshness_v1(ctx: bod.BodContext, *, observed_at_utc: str = "") -> dict[str, Any]:
    observed = parse_iso_v1(observed_at_utc) or datetime.now(UTC)
    records = [_record(ctx, spec, observed) for spec in artifact_specs_v1()]
    hard = [row for row in records if row["canonical_blocker"] and row["blocking_class"] == "HARD_BLOCKER"]
    unknown = [row for row in records if row["freshness_status"] == "UNKNOWN" and row.get("required_for_current_day") is True]
    return {
        "schema_id": "truth_freshness",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": "FAIL" if hard else ("UNKNOWN" if unknown else "PASS"),
        "canonical_blocker": str(hard[0]["canonical_blocker"]) if hard else "",
        "operator_next_action": str(hard[0]["operator_next_action"]) if hard else "",
        "freshness_records": records,
    }


def run_truth_freshness_v1(day_utc: str, environment: str, truth_root: str = "", observed_at_utc: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_truth_freshness_v1(ctx, observed_at_utc=observed_at_utc)
    path = truth_freshness_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_truth_freshness_v1.py", producer_command=f"python3 ops/tools/run_truth_freshness_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=[row["artifact_path"] for row in payload["freshness_records"]], output_artifacts=[path], schema_versions={"truth_freshness": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_truth_freshness_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--observed_at_utc", default="")
    args = parser.parse_args(argv)
    path, payload = run_truth_freshness_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""), str(args.observed_at_utc or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "truth_freshness_path": str(path)}, sort_keys=True))
    return 0 if payload["status"] != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
