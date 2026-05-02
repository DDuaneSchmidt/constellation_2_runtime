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
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1, git_commit_v1, git_dirty_status_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1


PRODUCER = "ops/tools/run_sleeve_outcome_generation_readiness_v1.py"
SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_outcome_generation_readiness.v1.schema.json"
SCORING_ELIGIBLE_SLEEVES = ("C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1")


def report_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "sleeve_outcome_generation_readiness_v1" / day_utc / "sleeve_outcome_generation_readiness.v1.json"


def _report(root: Path, family: str, day: str, filename: str) -> Path:
    return Path(root).resolve() / "reports" / family / day / filename


def _read_inputs(root: Path, day: str) -> tuple[dict[str, Path], dict[str, dict[str, Any]]]:
    paths = {
        "sleeve_intent_quality_diagnostics": _report(root, "sleeve_intent_quality_diagnostics_v1", day, "sleeve_intent_quality_diagnostics.v1.json"),
        "portfolio_scoring": _report(root, "portfolio_scoring_v1", day, "portfolio_scoring.v1.json"),
        "risk_sizing_authority": _report(root, "risk_sizing_authority_v1", day, "risk_sizing_authority.v1.json"),
        "market_data_authority": _report(root, "market_data_authority_v1", day, "market_data_authority.v1.json"),
        "submit_boundary_status": _report(root, "submit_boundary_status_v1", day, "submit_boundary_status.v1.json"),
        "trade_outcome": _report(root, "trade_outcome_v1", day, "trade_outcome.v1.json"),
        "sleeve_intent_trade_attribution": _report(root, "sleeve_intent_trade_attribution_v1", day, "sleeve_intent_trade_attribution.v1.json"),
    }
    return paths, {name: read_json_v1(path) for name, path in paths.items()}


def _rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    return [row for row in payload.get(key, []) if isinstance(row, dict)] if isinstance(payload.get(key), list) else []


def _by_intent(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("intent_id") or ""): row for row in rows if str(row.get("intent_id") or "")}


def _authorization_path(execution_root: Path, day: str, intent_hash: str) -> Path:
    return execution_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_hash}.authorization.v1.json"


def _find_submit_trace(root: Path, day: str, intent_id: str) -> list[str]:
    trace_root = root / "reports" / "submit_decision_trace_v1" / day
    if not trace_root.is_dir():
        return []
    out: list[str] = []
    for path in sorted(trace_root.rglob("submit_decision_trace.v1.json")):
        try:
            payload = read_json_v1(path)
        except Exception:
            payload = {}
        text = json.dumps(payload, sort_keys=True)
        if intent_id in text:
            out.append(str(path))
    return out


def _outcome_matches(outcome: dict[str, Any], intent_id: str) -> bool:
    return str(outcome.get("intent_id") or "") == intent_id


def _authorization_blockers(auth: dict[str, Any]) -> list[str]:
    if not auth:
        return ["AUTHORIZATION_ARTIFACT_MISSING"]
    blockers: list[str] = []
    status = str(auth.get("status") or "").upper()
    decision = str((auth.get("authorization") or {}).get("decision") or auth.get("decision_enum") or "").upper()
    qty = (auth.get("authorization") or {}).get("authorized_quantity")
    if status and status != "PASS":
        blockers.append(f"AUTHORIZATION_STATUS_{status}")
    if decision in {"REJECTED", "BLOCK"}:
        blockers.append("AUTHORIZATION_REJECTED")
    if qty in (None, "", 0):
        blockers.append("AUTHORIZED_QUANTITY_ZERO_OR_MISSING")
    for code in auth.get("reason_codes", []) if isinstance(auth.get("reason_codes"), list) else []:
        if str(code):
            blockers.append(str(code))
    return sorted(set(blockers))


def _authorization_details(auth: dict[str, Any], auth_path: Path, day_utc: str, truth_root: Path) -> dict[str, Any]:
    authorization = auth.get("authorization") if isinstance(auth.get("authorization"), dict) else {}
    policy_rules = auth.get("constitutional_shadow", {}).get("decision", {}).get("blocker_rules") if isinstance(auth.get("constitutional_shadow"), dict) else []
    if not isinstance(policy_rules, list):
        policy_rules = []
    reason_codes = auth.get("reason_codes") if isinstance(auth.get("reason_codes"), list) else []
    rejection_reason = ",".join(str(code) for code in reason_codes if str(code)) or ",".join(str(rule) for rule in policy_rules if str(rule))
    return {
        "artifact_path": str(auth_path),
        "authorization_status": str(auth.get("status") or "MISSING").upper(),
        "decision": str(authorization.get("decision") or auth.get("decision_enum") or "").upper(),
        "authorized_quantity": authorization.get("authorized_quantity"),
        "rejection_reason": rejection_reason,
        "policy_rules": [str(rule) for rule in policy_rules if str(rule)],
        "required_recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_authorization_artifacts_day_v1.py --day_utc {day_utc} --truth_root {truth_root}",
    }


def _classifications(blockers: list[str]) -> list[str]:
    out: list[str] = []
    if "BUNDLE_B_HEADROOM_REJECTED" in blockers:
        out.append("REAL_POLICY_REJECT")
    if "OPTIONS_CHAIN_SNAPSHOT_MISSING" in blockers:
        out.append("MISSING_OPTIONS_CHAIN_INPUT")
    if any(code in blockers for code in ("AUTHZ_MISSING_DEFINED_RISK_EVIDENCE", "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN", "BUNDLE_B_REQUESTED_QUANTITY_ZERO")):
        out.append("MISSING_DEFINED_RISK_OR_QUANTITY_EVIDENCE")
    if "EXECUTION_PACKAGE_MISSING" in blockers:
        out.append("EXECUTION_PACKAGE_DOWNSTREAM_OF_AUTHORIZATION")
    if "SUBMIT_DECISION_TRACE_MISSING" in blockers:
        out.append("SUBMIT_TRACE_DOWNSTREAM_OF_SUBMIT_BOUNDARY")
    if "COMPLETED_OUTCOME_MISSING" in blockers:
        out.append("OUTCOME_MISSING_NO_REAL_FILL_OR_POSITION_LIFECYCLE")
    return sorted(set(out or ["UNKNOWN"]))


def _execution_package_readiness(*, blockers: list[str], execution_package_path: str, day_utc: str) -> dict[str, Any]:
    if execution_package_path:
        status = "PRESENT"
    elif "AUTHORIZATION_REJECTED" in blockers or "AUTHORIZED_QUANTITY_ZERO_OR_MISSING" in blockers:
        status = "BLOCKED_BY_AUTHORIZATION_REJECTED"
    else:
        status = "MISSING"
    return {
        "status": status,
        "expected_path": execution_package_path,
        "producer": "ops/tools/run_execution_build_authority_v1.py",
        "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_execution_build_authority_v1.py --operation_type PAPER_SUBMIT --candidate_path <GOVERNED_PHASEC_CANDIDATE_PATH_FOR_{day_utc}>",
    }


def _submit_trace_readiness(*, blockers: list[str], submit_traces: list[str], day_utc: str, truth_root: Path) -> dict[str, Any]:
    if submit_traces:
        status = "PRESENT"
    elif "EXECUTION_PACKAGE_MISSING" in blockers or "AUTHORIZATION_REJECTED" in blockers:
        status = "EXPECTED_MISSING_SUBMIT_BOUNDARY_NOT_REACHED"
    else:
        status = "MISSING"
    return {
        "status": status,
        "producer": "ops/tools/run_submit_decision_trace_v1.py",
        "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_submit_decision_trace_v1.py --day_utc {day_utc} --truth_root {truth_root} --environment PAPER",
        "trace_paths": submit_traces,
    }


def _symbol_requires_market_input(market: dict[str, Any], symbol: str, code: str) -> bool:
    required_symbols = {str(sym).upper() for sym in market.get("required_symbols", []) if str(sym)}
    if code == "OPTIONS_CHAIN_SNAPSHOT_MISSING":
        return str(symbol).upper() in required_symbols
    return True


def _next_step(blockers: list[str]) -> tuple[str, str]:
    if "OPTIONS_CHAIN_SNAPSHOT_MISSING" in blockers:
        return (
            "ops/tools/run_options_chain_snapshot_required_day_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc <DAY>",
        )
    if "AUTHORIZATION_REJECTED" in blockers or "AUTHORIZED_QUANTITY_ZERO_OR_MISSING" in blockers:
        return (
            "ops/tools/run_authorization_artifacts_day_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_authorization_artifacts_day_v1.py --day_utc <DAY> --truth_root <TRUTH_ROOT>",
        )
    if "EXECUTION_PACKAGE_MISSING" in blockers:
        return (
            "ops/tools/run_execution_build_authority_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_execution_build_authority_v1.py --operation_type PAPER_SUBMIT --candidate_path <GOVERNED_ORDER_PLAN_CANDIDATE_PATH>",
        )
    if "SUBMIT_DECISION_TRACE_MISSING" in blockers:
        return (
            "ops/tools/run_submit_decision_trace_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_submit_decision_trace_v1.py --day_utc <DAY> --truth_root <TRUTH_ROOT> --environment PAPER",
        )
    return (
        "ops/tools/run_trade_outcome_v1.py",
        "PYTHONPATH=\"$PWD\" python3 ops/tools/run_trade_outcome_v1.py --day_utc <DAY> --truth_root <TRUTH_ROOT> --environment PAPER",
    )


def build_sleeve_outcome_generation_readiness_v1(*, day_utc: str, truth_root: Path, runtime_root: Path | None = None, execution_root: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    runtime = Path(runtime_root or truth_root).resolve()
    execution = Path(execution_root or truth_root).resolve()
    paths, payloads = _read_inputs(root, day_utc)
    diag = _by_intent(_rows(payloads["sleeve_intent_quality_diagnostics"], "intent_diagnostics"))
    scoring_rows = [
        row for row in _rows(payloads["portfolio_scoring"], "rankings")
        if str(row.get("sleeve_id") or "") in SCORING_ELIGIBLE_SLEEVES and row.get("executable_eligible") is True
    ]
    risk = _by_intent(_rows(payloads["risk_sizing_authority"], "sizing_decisions"))
    outcome = payloads["trade_outcome"]
    submit_boundary = payloads["submit_boundary_status"]
    market = payloads["market_data_authority"]
    results: list[dict[str, Any]] = []
    for score in sorted(scoring_rows, key=lambda row: int(row.get("rank") or 999)):
        intent_id = str(score.get("intent_id") or "")
        sleeve_id = str(score.get("sleeve_id") or "")
        drow = diag.get(intent_id, {})
        rrow = risk.get(intent_id, {})
        intent_path = str(rrow.get("intent_path") or next(iter(score.get("evidence_paths", []) or []), ""))
        intent_hash = str(rrow.get("intent_hash") or Path(intent_path).name.split(".", 1)[0]).strip()
        auth_path = _authorization_path(execution, day_utc, intent_hash)
        auth = read_json_v1(auth_path) if auth_path.exists() else {}
        submit_traces = _find_submit_trace(root, day_utc, intent_id)
        execution_package_path = str(rrow.get("execution_package_path") or "")
        symbol = str(score.get("symbol") or drow.get("symbol") or "")
        raw_missing_inputs = drow.get("missing_inputs") if isinstance(drow.get("missing_inputs"), list) else []
        missing_inputs = [
            str(code)
            for code in raw_missing_inputs
            if _symbol_requires_market_input(market, symbol, str(code))
        ]
        blockers = _authorization_blockers(auth)
        missing_evidence: list[str] = []
        if not execution_package_path:
            blockers.append("EXECUTION_PACKAGE_MISSING")
            missing_evidence.append("execution_package_v1")
        if not submit_traces:
            blockers.append("SUBMIT_DECISION_TRACE_MISSING")
            missing_evidence.append("submit_decision_trace_v1")
        if str(submit_boundary.get("submit_allowed") or "").lower() != "true":
            blockers.append(str(submit_boundary.get("canonical_blocker") or submit_boundary.get("first_blocker_code") or "SUBMIT_BOUNDARY_NOT_READY"))
        if not _outcome_matches(outcome, intent_id) or str(outcome.get("outcome_status") or "").upper() not in {"OPEN", "CLOSED"}:
            blockers.append("COMPLETED_OUTCOME_MISSING")
            missing_evidence.append("trade_outcome_v1")
        if str(market.get("status") or "").upper() == "FAIL" and str(symbol).upper() in {str(sym).upper() for sym in market.get("required_symbols", []) if str(sym)}:
            blockers.append(str(market.get("first_blocker") or "MARKET_DATA_AUTHORITY_FAIL"))
        unique_blockers = sorted(set(item for item in blockers if item))
        producer, command = _next_step(unique_blockers)
        results.append(
            {
                "sleeve_id": sleeve_id,
                "intent_id": intent_id,
                "intent_artifact": intent_path,
                "signal_trigger": str(drow.get("signal_trigger_reason") or ",".join(str(code) for code in score.get("reason_codes", []) if str(code))),
                "market_inputs": {
                    "symbol": symbol,
                    "missing_inputs": missing_inputs,
                    "market_data_status": str(market.get("status") or "UNKNOWN"),
                },
                "risk_sizing_result": {
                    "sizing_state": str(rrow.get("sizing_state") or "MISSING"),
                    "execution_package_path": execution_package_path,
                    "final_quantity": rrow.get("final_quantity"),
                    "reason_code": str(rrow.get("reason_code") or ""),
                    "account_net_liquidation_cents": rrow.get("account_net_liquidation_cents"),
                    "allowed_risk_cents": rrow.get("allowed_risk_cents"),
                },
                "authorization_details": _authorization_details(auth, auth_path, day_utc, root),
                "execution_package_readiness": _execution_package_readiness(blockers=unique_blockers, execution_package_path=execution_package_path, day_utc=day_utc),
                "submit_trace_readiness": _submit_trace_readiness(blockers=unique_blockers, submit_traces=submit_traces, day_utc=day_utc, truth_root=root),
                "scoring_eligible": True,
                "execution_ready_if_aegis_ready": not unique_blockers,
                "root_cause_classification": _classifications(unique_blockers),
                "why_did_not_submit": unique_blockers,
                "missing_evidence": sorted(set(missing_evidence)),
                "next_governed_producer": producer,
                "recovery_command": command.replace("<DAY>", day_utc).replace("<TRUTH_ROOT>", str(root)),
                "outcome_generation_blockers": unique_blockers,
                "evidence_paths": sorted(set([intent_path, str(auth_path), *submit_traces, *[str(path) for path in paths.values() if path.exists()]])),
            }
        )
    return {
        "schema_id": "sleeve_outcome_generation_readiness",
        "schema_version": "sleeve_outcome_generation_readiness.v1",
        "day_utc": day_utc,
        "generated_at": now_iso_v1(),
        "git_commit": git_commit_v1(),
        "git_dirty_status": git_dirty_status_v1(),
        "truth_root": str(root),
        "runtime_root": str(runtime),
        "execution_root": str(execution),
        "producer": PRODUCER,
        "authority": "DIAGNOSTIC_ONLY",
        "readiness_effect": "NONE",
        "submit_effect": "NONE",
        "allocation_effect": "NONE",
        "scoring_eligible_sleeves": list(SCORING_ELIGIBLE_SLEEVES),
        "sleeve_results": results,
        "summary": {
            "eligible_intent_count": len(results),
            "execution_ready_count": sum(1 for row in results if row["execution_ready_if_aegis_ready"] is True),
            "completed_outcome_ready_count": 0,
            "allocation_eligibility_effect": "NONE",
        },
    }


def _validate(payload: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("SLEEVE_OUTCOME_GENERATION_READINESS_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def write_report_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any], command: str) -> Path:
    path = report_path(truth_root=truth_root, day_utc=day_utc)
    input_paths, _ = _read_inputs(truth_root, day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name=PRODUCER,
        producer_command=command,
        input_artifacts=input_paths.values(),
        output_artifacts=[path],
        schema_versions={"sleeve_outcome_generation_readiness_v1": "sleeve_outcome_generation_readiness.v1"},
    )
    _validate(payload)
    write_json_v1(path, payload)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_sleeve_outcome_generation_readiness_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_root", default="")
    parser.add_argument("--execution_root", default="")
    args = parser.parse_args(argv)
    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    runtime_root = Path(args.runtime_root).expanduser().resolve() if str(args.runtime_root or "").strip() else truth_root
    execution_root = Path(args.execution_root).expanduser().resolve() if str(args.execution_root or "").strip() else truth_root
    payload = build_sleeve_outcome_generation_readiness_v1(day_utc=day, truth_root=truth_root, runtime_root=runtime_root, execution_root=execution_root)
    command = f"PYTHONPATH=\"$PWD\" python3 {PRODUCER} --day_utc {day} --truth_root {truth_root} --runtime_root {runtime_root} --execution_root {execution_root}"
    path = write_report_v1(truth_root=truth_root, day_utc=day, payload=payload, command=command)
    print(json.dumps({"status": "PASS", "path": str(path), "eligible_intent_count": payload["summary"]["eligible_intent_count"], "execution_ready_count": payload["summary"]["execution_ready_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
