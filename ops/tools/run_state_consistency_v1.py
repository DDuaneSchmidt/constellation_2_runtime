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

SCHEMA_VERSION = "state_consistency.v1"


def state_consistency_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "state_consistency_v1" / day_utc / "state_consistency.v1.json").resolve()


def _result(invariant_id: str, severity: str, status: str, owner: str, checked: list[Path], expected: str, observed: str, blocker: str = "", action: str = "") -> dict[str, Any]:
    return {
        "invariant_id": invariant_id,
        "severity": severity,
        "status": status,
        "owner": owner,
        "checked_artifacts": [str(path) for path in checked],
        "expected_condition": expected,
        "observed_condition": observed,
        "canonical_blocker": blocker if status == "FAIL" else "",
        "operator_next_action": action if status == "FAIL" else "",
    }


def _authorized_count(auth: dict[str, Any]) -> int:
    export = auth.get("authorization_export") if isinstance(auth.get("authorization_export"), dict) else {}
    rows = export.get("authorized_intents") if isinstance(export.get("authorized_intents"), list) else []
    return len(rows)


def build_state_consistency_v1(ctx: bod.BodContext) -> dict[str, Any]:
    paths = {
        "ledger": report_path_v1(ctx, "aegis_day_run_v1", "day_run.v1.json"),
        "requirement_graph": report_path_v1(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json"),
        "market_data_authority": report_path_v1(ctx, "market_data_authority_v1", "market_data_authority.v1.json"),
        "strategy_decision": report_path_v1(ctx, "strategy_decision_authority_v1", "strategy_decision_authority.v1.json"),
        "authorization": report_path_v1(ctx, "authorization_supply_v1", "authorization_supply.v1.json"),
        "submit_boundary": report_path_v1(ctx, "submit_boundary_status_v1", "submit_boundary_status.v1.json"),
        "operator_projection": report_path_v1(ctx, "aegis_operator_projection_v1", "operator_projection.v1.json"),
        "live": report_path_v1(ctx, "aegis_live_intelligence_v1", "live_intelligence.v1.json"),
    }
    ledger = read_json_v1(paths["ledger"])
    graph = read_json_v1(paths["requirement_graph"])
    market = read_json_v1(paths["market_data_authority"])
    strategy = read_json_v1(paths["strategy_decision"])
    auth = read_json_v1(paths["authorization"])
    submit = read_json_v1(paths["submit_boundary"])
    projection = read_json_v1(paths["operator_projection"])
    live = read_json_v1(paths["live"])
    final_status = str(ledger.get("final_status") or "UNKNOWN").upper()
    ledger_blocker = blocker_of_v1(ledger)
    source = ledger.get("source_repo_status") if isinstance(ledger.get("source_repo_status"), dict) else {}
    source_passed = source.get("source_reproducibility_status") == "REPRODUCIBLE" and source.get("git_dirty_status") == "CLEAN"
    results: list[dict[str, Any]] = []
    blockers_seen = [ledger_blocker, blocker_of_v1(projection), blocker_of_v1(live), blocker_of_v1(submit)]
    results.append(_result("SOURCE_INTEGRITY_CLEAN_IMPLIES_NO_SOURCE_BLOCKER", "HARD_BLOCKER", "PASS" if not source_passed or "SOURCE_REPRODUCIBILITY_BLOCKED" not in blockers_seen else "FAIL", "source_reproducibility_authority", [paths["ledger"], paths["operator_projection"], paths["live"], paths["submit_boundary"]], "clean source means no current SOURCE_REPRODUCIBILITY_BLOCKED", str(blockers_seen), "SOURCE_BLOCKER_CONTRADICTION", "Rerun reports after source integrity passes."))
    surfaces_match = str(projection.get("final_status") or final_status).upper() == final_status and str(live.get("final_status_observed") or final_status).upper() == final_status
    results.append(_result("DAY_RUN_IS_FINAL_READINESS_AUTHORITY", "HARD_BLOCKER", "PASS" if surfaces_match else "FAIL", "aegis_day_run_ledger_v1", [paths["ledger"], paths["operator_projection"], paths["live"]], "operator/live final status equals day-run final status", f"ledger={final_status} projection={projection.get('final_status')} live={live.get('final_status_observed')}", "FINAL_READINESS_AUTHORITY_MISMATCH", "Regenerate projection and live intelligence from day-run ledger."))
    live_ok = final_status in READY_FINAL_STATUSES or (status_of_v1(live) == "NOT_READY" and live.get("advisory_only") is True and not (live.get("opportunity_radar") or {}).get("actionable_recommendations"))
    results.append(_result("BLOCKED_LEDGER_CAPS_LIVE_INTELLIGENCE", "HARD_BLOCKER", "PASS" if live_ok else "FAIL", "aegis_live_intelligence_v1", [paths["ledger"], paths["live"]], "blocked ledger caps live intelligence to advisory NOT_READY", f"ledger={final_status} live={status_of_v1(live)} advisory={live.get('advisory_only')}", "LIVE_INTELLIGENCE_UNCAPPED", "Regenerate live intelligence as advisory-only."))
    auth_pass_ok = status_of_v1(auth) != "PASS" or _authorized_count(auth) > 0
    results.append(_result("AUTHORIZATION_PASS_REQUIRES_AUTHORIZED_INTENT", "HARD_BLOCKER", "PASS" if auth_pass_ok else "FAIL", "authorization_supply_v1", [paths["authorization"]], "PASS requires authorized_intent_count > 0", f"status={status_of_v1(auth)} count={_authorized_count(auth)}", "AUTHORIZATION_PASS_WITHOUT_INTENT", "Regenerate authorization supply."))
    market_state = str(market.get("market_data_state") or market.get("state") or "").upper()
    strategy_state = status_of_v1(strategy) or str(strategy.get("strategy_decision_state") or "").upper()
    market_ok = market_state != "MISSING_REQUIRED_DATA" or strategy_state not in {"READY", "PASS"}
    results.append(_result("MARKET_DATA_BLOCKED_PROPAGATES_TO_STRATEGY", "HARD_BLOCKER", "PASS" if market_ok else "FAIL", "market_data_authority", [paths["market_data_authority"], paths["strategy_decision"]], "missing market data prevents READY/PASS strategy", f"market={market_state} strategy={strategy_state}", "MARKET_DATA_STRATEGY_CONTRADICTION", "Regenerate strategy decision after market data authority."))
    submit_allowed = submit.get("submit_allowed") is True or submit.get("submission_authorized") is True
    submit_ok = final_status in READY_FINAL_STATUSES or not submit_allowed
    results.append(_result("SUBMIT_FORBIDDEN_WHILE_LEDGER_BLOCKED", "HARD_BLOCKER", "PASS" if submit_ok else "FAIL", "submit_boundary_status_v1", [paths["ledger"], paths["submit_boundary"]], "blocked day-run forbids broker submission", f"ledger={final_status} submit_allowed={submit_allowed}", "SUBMIT_ALLOWED_WHILE_LEDGER_BLOCKED", "Regenerate submit boundary from readiness authorities."))
    reqs = graph.get("requirements") if isinstance(graph.get("requirements"), list) else []
    missing = [row.get("expected_path") for row in reqs if isinstance(row, dict) and row.get("blocking_class", "HARD_BLOCKER") == "HARD_BLOCKER" and row.get("status") == "SATISFIED" and not Path(str(row.get("expected_path") or "")).exists()]
    graph_ok = status_of_v1(graph) != "PASS" or not missing
    results.append(_result("REQUIREMENT_GRAPH_PASS_REQUIRES_REQUIRED_ARTIFACTS_PRESENT", "HARD_BLOCKER", "PASS" if graph_ok else "FAIL", "aegis_requirement_graph_v1", [paths["requirement_graph"]], "PASS graph has present satisfied required artifacts", f"missing={missing}", "REQUIREMENT_GRAPH_ARTIFACT_MISSING", "Regenerate requirement graph and missing artifacts."))
    hard_failures = [row for row in results if row["status"] == "FAIL" and row["severity"] == "HARD_BLOCKER"]
    return {
        "schema_id": "state_consistency",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": "FAIL" if hard_failures else "PASS",
        "canonical_blocker": str(hard_failures[0]["canonical_blocker"]) if hard_failures else "",
        "operator_next_action": str(hard_failures[0]["operator_next_action"]) if hard_failures else "",
        "invariant_results": results,
    }


def run_state_consistency_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_state_consistency_v1(ctx)
    path = state_consistency_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    checked_inputs: list[str] = []
    seen_inputs: set[str] = set()
    for result in payload["invariant_results"]:
        for item in result.get("checked_artifacts", []):
            text = str(item or "").strip()
            if text and text not in seen_inputs:
                seen_inputs.add(text)
                checked_inputs.append(text)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_state_consistency_v1.py", producer_command=f"python3 ops/tools/run_state_consistency_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=checked_inputs, output_artifacts=[path], schema_versions={"state_consistency": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_state_consistency_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_state_consistency_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "state_consistency_path": str(path)}, sort_keys=True))
    return 0 if payload["status"] != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
