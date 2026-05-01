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
from ops.tools.aegis_truth_integrity_common_v1 import READY_FINAL_STATUSES, blocker_of_v1, now_iso_v1, read_json_v1, report_path_v1, status_of_v1, write_json_v1

SCHEMA_VERSION = "action_validity.v1"
REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/aegis_action_registry_v1.json").resolve()


def action_validity_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "action_validity_v1" / day_utc / "action_validity.v1.json").resolve()


def _registry() -> list[dict[str, Any]]:
    payload = read_json_v1(REGISTRY_PATH)
    rows = payload.get("actions") if isinstance(payload.get("actions"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _rule(ctx: bod.BodContext, action: dict[str, Any], ledger: dict[str, Any], market_supply: dict[str, Any], authorization: dict[str, Any]) -> dict[str, Any]:
    action_id = str(action.get("action_id") or "")
    final_status = str(ledger.get("final_status") or "UNKNOWN").upper()
    blocker = blocker_of_v1(ledger)
    ready = final_status in READY_FINAL_STATUSES and not blocker
    status = "UNKNOWN"
    reason = "No state-specific rule matched."
    canonical_blocker = ""
    next_action = ""
    risk = str(action.get("risk_level") or "LOW")
    if action_id in {"rerun_day", "rerun_requirement_graph", "rerun_operator_projection", "rerun_live_intelligence"}:
        status = "ALLOWED"
        reason = "Read-only/regeneration action is allowed."
    elif action_id == "clean_and_protect_repo":
        status = "ALLOWED" if blocker == "SOURCE_REPRODUCIBILITY_BLOCKED" else "BLOCKED"
        reason = "Source integrity is blocked." if status == "ALLOWED" else "Source integrity is not the current blocker."
    elif action_id == "inspect_market_data_artifacts":
        status = "ALLOWED" if blocker.startswith("MARKET") or blocker.startswith("OPTIONS") or status_of_v1(market_supply) == "BLOCKED" else "BLOCKED"
        reason = "Market data is blocked or degraded." if status == "ALLOWED" else "Market data is not the current blocker."
    elif action_id == "inspect_authorization_diagnostics":
        auth_blocker = blocker_of_v1(authorization)
        status = "ALLOWED" if auth_blocker == "NO_ELIGIBLE_OPTION_STRUCTURE" or blocker == "NO_ELIGIBLE_OPTION_STRUCTURE" else "BLOCKED"
        reason = "Authorization/structure diagnostics are relevant." if status == "ALLOWED" else "Authorization diagnostics are not the current blocker."
    elif action_id == "rerun_market_open_data_gate":
        if blocker == "MARKET_CLOSED":
            status = "BLOCKED"
            reason = "Market is closed; rerun is not useful unless an after-hours diagnostic policy explicitly permits it."
            canonical_blocker = "MARKET_CLOSED"
        else:
            status = "ALLOWED"
            reason = "Market-open data gate rerun is safe as a governed producer action."
    elif action_id in {"submit_paper_order", "enable_broker_transmit"}:
        if not ready:
            status = "FORBIDDEN"
            reason = "Day-run ledger is not ready; trade/transmit action is forbidden."
            canonical_blocker = blocker or "DAY_RUN_LEDGER_NOT_READY"
            risk = "PROHIBITED" if action_id == "enable_broker_transmit" else risk
        else:
            status = "ALLOWED"
            reason = "Day-run ledger is ready; submit boundary must still authorize the concrete action."
    if status in {"FORBIDDEN", "BLOCKED"}:
        next_action = "Use an allowed diagnostic or regeneration action instead."
    return {
        **action,
        "allowed_when": ["day_run_ready"] if action_id in {"submit_paper_order", "enable_broker_transmit"} else [],
        "forbidden_when": ["day_run_blocked"] if action_id in {"submit_paper_order", "enable_broker_transmit"} else [],
        "required_preconditions": ["aegis_day_run_ledger_v1"],
        "expected_output_artifacts": [],
        "risk_level": risk,
        "status": status,
        "reason": reason,
        "canonical_blocker": canonical_blocker,
        "operator_next_action": next_action,
    }


def build_action_validity_v1(ctx: bod.BodContext) -> dict[str, Any]:
    ledger = read_json_v1(report_path_v1(ctx, "aegis_day_run_v1", "day_run.v1.json"))
    market_supply = read_json_v1(report_path_v1(ctx, "market_data_supply_v1", "market_data_supply.v1.json"))
    authorization = read_json_v1(report_path_v1(ctx, "authorization_supply_v1", "authorization_supply.v1.json"))
    rules = [_rule(ctx, action, ledger, market_supply, authorization) for action in _registry()]
    forbidden = [row for row in rules if row["status"] == "FORBIDDEN"]
    return {
        "schema_id": "action_validity",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": "PASS",
        "canonical_blocker": "",
        "operator_next_action": "",
        "action_rules": rules,
        "unsafe_actions": [row["action_id"] for row in rules if row["status"] in {"FORBIDDEN", "BLOCKED"}],
        "forbidden_trade_actions": [row["action_id"] for row in forbidden],
    }


def run_action_validity_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_action_validity_v1(ctx)
    path = action_validity_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_action_validity_v1.py", producer_command=f"python3 ops/tools/run_action_validity_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=[REGISTRY_PATH, report_path_v1(ctx, "aegis_day_run_v1", "day_run.v1.json"), report_path_v1(ctx, "market_data_supply_v1", "market_data_supply.v1.json"), report_path_v1(ctx, "authorization_supply_v1", "authorization_supply.v1.json")], output_artifacts=[path], schema_versions={"action_validity": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_action_validity_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_action_validity_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "action_validity_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
