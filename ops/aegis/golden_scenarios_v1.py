from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.ai_operations_assistant_v1 import build_ai_operations_context_v1, build_ai_operations_response_v1, write_ai_operations_context_v1, write_ai_operations_response_v1
from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.operator_surface_contract_v1 import build_operator_surface_contract_v1, write_operator_surface_contract_v1
from ops.aegis.semantic_invariants_v1 import build_semantic_invariants_v1, write_semantic_invariants_v1
from ops.aegis.surface_readiness_v1 import SURFACES, build_surface_readiness_v1, write_surface_readiness_v1

REPORT_FAMILY = "aegis_golden_scenarios_v1"
REPORT_FILENAME = "golden_scenarios.v1.json"
CANONICAL_DAY = "2026-05-29"
FAIL_CLOSED_DAY = "2026-05-30"
SAFETY = {
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
}


def golden_scenarios_path_v1(*, truth_root: Path | str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / "latest" / REPORT_FILENAME


def build_golden_scenarios_v1(*, truth_root: Path | str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = now_utc_v1()
    scenarios = [
        _canonical_performance_day(root, CANONICAL_DAY, generated_at),
        _fail_closed_day(root, FAIL_CLOSED_DAY, generated_at),
    ]
    checks = [check for scenario in scenarios for check in scenario.get("checks", [])]
    fail_count = sum(1 for check in checks if check.get("status") == "FAIL")
    warning_count = sum(1 for check in checks if check.get("status") == "WARNING")
    return {
        "schema_id": REPORT_FAMILY,
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "generated_at": generated_at,
        "status": "FAIL" if fail_count else "WARNING" if warning_count else "PASS",
        "summary": {
            "scenario_count": len(scenarios),
            "check_count": len(checks),
            "pass_count": sum(1 for check in checks if check.get("status") == "PASS"),
            "fail_count": fail_count,
            "warning_count": warning_count,
        },
        "scenarios": scenarios,
        "safety": dict(SAFETY),
        **SAFETY,
    }


def write_golden_scenarios_v1(*, truth_root: Path | str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_golden_scenarios_v1(truth_root=truth_root))
    return write_json_v1(golden_scenarios_path_v1(truth_root=truth_root), body)


def run_golden_scenarios_self_check_v1(*, truth_root: Path | str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    payload = read_json_v1(golden_scenarios_path_v1(truth_root=root))
    if not payload:
        payload = build_golden_scenarios_v1(truth_root=root)
        write_golden_scenarios_v1(truth_root=root, payload=payload)
    failures: list[dict[str, Any]] = []
    for scenario in payload.get("scenarios", []) if isinstance(payload.get("scenarios"), list) else []:
        if not isinstance(scenario, Mapping):
            failures.append({"check": "scenario_shape", "reason": "scenario row is not an object"})
            continue
        for check in scenario.get("checks", []) if isinstance(scenario.get("checks"), list) else []:
            if isinstance(check, Mapping) and check.get("status") == "FAIL":
                failures.append({"scenario_id": scenario.get("scenario_id"), "check_id": check.get("check_id"), "reason": check.get("reason"), "evidence": check.get("evidence")})
    return {
        "ok": not failures,
        "status": "PASS" if not failures else "FAIL",
        "path": str(golden_scenarios_path_v1(truth_root=root)),
        "summary": payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {},
        "failures": failures,
        "safety": dict(SAFETY),
    }


def _canonical_performance_day(root: Path, day: str, generated_at: str) -> dict[str, Any]:
    _ensure_supporting_artifacts(root, day)
    reports = root / "reports"
    pnl = _read(reports / "aegis_paper_pnl_report_v1" / day / "paper_pnl_report.v1.json")
    sleeve = _read(reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json")
    queue = _read(reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json")
    ai_ctx = _read(reports / "aegis_ai_operations_context_v1" / day / "ai_operations_context.v1.json")
    ai_rsp = _read(reports / "aegis_ai_operations_response_v1" / day / "ai_operations_response.v1.json")
    contract = _read(reports / "aegis_operator_surface_contract_v1" / day / "operator_surface_contract.v1.json")
    checks = [
        _check("performance_canonical_when_marks_present", _full_status(pnl) == "CANONICAL" and _data_quality(pnl) == "PASS", "Performance must be canonical on the locked canonical day.", {"full_portfolio_pnl_status": _full_status(pnl), "data_quality_status": _data_quality(pnl)}),
        _check("sleeve_analytics_canonical", str(sleeve.get("status") or "") == "CANONICAL", "Sleeve Analytics must be canonical on the locked canonical day.", {"status": sleeve.get("status")}),
        _check("no_unknown_sleeve_bucket", not any(str(row.get("sleeve_id") or "").upper() == "UNKNOWN" for row in _list(sleeve.get("sleeves"))), "Canonical day must not contain UNKNOWN sleeve bucket.", {"sleeve_count": len(_list(sleeve.get("sleeves")))}),
        _check("no_stale_action_queue_rows", int((queue.get("summary") or {}).get("incorrectly_shown_as_awaiting_review_count") or 0) == 0, "Queue audit must not expose stale/non-actionable rows as awaiting review.", queue.get("summary") or {}),
        _check("ask_aegis_grounded", _ai_grounded(ai_ctx, ai_rsp, day), "Ask Aegis must be grounded to the scenario day.", {"context_day": ai_ctx.get("day_utc"), "response_day": (ai_rsp.get("latest_response") or {}).get("context_day"), "source_count": len((ai_rsp.get("latest_response") or {}).get("source_artifacts") or [])}),
        _check("operator_surface_contract_complete", int((contract.get("summary") or {}).get("surface_count") or 0) == len(SURFACES), "Operator Surface Contract must emit all required surface rows.", contract.get("summary") or {}),
    ]
    return {"scenario_id": "canonical_performance_day", "day_utc": day, "description": "2026-05-29 canonical performance day", "generated_at": generated_at, "checks": checks}


def _fail_closed_day(root: Path, day: str, generated_at: str) -> dict[str, Any]:
    _ensure_supporting_artifacts(root, day)
    reports = root / "reports"
    queue = _read(reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json")
    surface = _read(reports / "aegis_surface_readiness_v1" / day / "surface_readiness.v1.json")
    semantic = _read(reports / "aegis_semantic_invariants_v1" / day / "semantic_invariants.v1.json")
    pnl = _read(reports / "aegis_paper_pnl_report_v1" / day / "paper_pnl_report.v1.json")
    sleeve = _read(reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json")
    ai_ctx = _read(reports / "aegis_ai_operations_context_v1" / day / "ai_operations_context.v1.json")
    ai_rsp = _read(reports / "aegis_ai_operations_response_v1" / day / "ai_operations_response.v1.json")
    contract = _read(reports / "aegis_operator_surface_contract_v1" / day / "operator_surface_contract.v1.json")
    surface_by_id = surface.get("surface_by_id") if isinstance(surface.get("surface_by_id"), Mapping) else {}
    command_surface = surface_by_id.get("command_center") if isinstance(surface_by_id.get("command_center"), Mapping) else {}
    perf_surface = surface_by_id.get("performance") if isinstance(surface_by_id.get("performance"), Mapping) else {}
    sleeve_surface = surface_by_id.get("sleeve_analytics") if isinstance(surface_by_id.get("sleeve_analytics"), Mapping) else {}
    checks = [
        _check("no_stale_actionable_rows", int((queue.get("summary") or {}).get("operator_action_required_count") or 0) == 0 and int((queue.get("summary") or {}).get("incorrectly_shown_as_awaiting_review_count") or 0) == 0, "Fail-closed day must have no stale actionable rows.", queue.get("summary") or {}),
        _check("command_center_actions_fail_closed", command_surface.get("actions_allowed") is False, "Command Center actions must be disabled on fail-closed day.", {"surface_status": command_surface.get("surface_status"), "actions_allowed": command_surface.get("actions_allowed")}),
        _check("performance_degraded_cleanly", perf_surface.get("surface_status") in {"DEGRADED", "BLOCKED", "UNAVAILABLE"} or _full_status(pnl) != "CANONICAL", "Performance must be blocked/degraded or explicitly non-canonical.", {"surface_status": perf_surface.get("surface_status"), "full_portfolio_pnl_status": _full_status(pnl), "data_quality_status": _data_quality(pnl)}),
        _check("sleeve_analytics_degraded_cleanly", sleeve_surface.get("surface_status") in {"DEGRADED", "BLOCKED", "UNAVAILABLE"} or str(sleeve.get("status") or "") != "CANONICAL", "Sleeve Analytics must be blocked/degraded or explicitly non-canonical/partial.", {"surface_status": sleeve_surface.get("surface_status"), "status": sleeve.get("status"), "data_quality_status": (sleeve.get("summary") or {}).get("data_quality_status")}),
        _check("no_contradictory_metrics", int((semantic.get("summary") or {}).get("blocking_failure_count") or 0) == 0, "Semantic invariants must not report blocking contradictions.", semantic.get("summary") or {}),
        _check("ask_aegis_grounded_to_fail_closed_day", _ai_grounded(ai_ctx, ai_rsp, day), "Ask Aegis must be grounded to 2026-05-30.", {"context_day": ai_ctx.get("day_utc"), "response_day": (ai_rsp.get("latest_response") or {}).get("context_day"), "source_count": len((ai_rsp.get("latest_response") or {}).get("source_artifacts") or [])}),
        _check("operator_surface_contract_fail_closed", int((contract.get("summary") or {}).get("actions_allowed_count") or 0) == 0 and int((contract.get("summary") or {}).get("surface_count") or 0) == len(SURFACES), "Operator Surface Contract must fail closed with no actions on 2026-05-30.", contract.get("summary") or {}),
    ]
    return {"scenario_id": "fail_closed_non_trading_day", "day_utc": day, "description": "2026-05-30 non-trading / fail-closed day", "generated_at": generated_at, "checks": checks}


def _ensure_supporting_artifacts(root: Path, day: str) -> None:
    semantic = build_semantic_invariants_v1(truth_root=root, day_utc=day)
    write_semantic_invariants_v1(truth_root=root, day_utc=day, payload=semantic)
    surface = build_surface_readiness_v1(truth_root=root, day_utc=day)
    write_surface_readiness_v1(truth_root=root, day_utc=day, payload=surface)
    context = build_ai_operations_context_v1(truth_root=root, day_utc=day)
    write_ai_operations_context_v1(truth_root=root, day_utc=day, payload=context)
    response = build_ai_operations_response_v1(truth_root=root, day_utc=day, question="What should be fixed first?")
    write_ai_operations_response_v1(truth_root=root, day_utc=day, payload=response)
    contract = build_operator_surface_contract_v1(truth_root=root, day_utc=day)
    write_operator_surface_contract_v1(truth_root=root, day_utc=day, payload=contract)


def _check(check_id: str, passed: bool, reason: str, evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "reason": "Check passed." if passed else reason, "evidence": dict(evidence)}


def _ai_grounded(context: Mapping[str, Any], response_payload: Mapping[str, Any], day: str) -> bool:
    latest = response_payload.get("latest_response") if isinstance(response_payload.get("latest_response"), Mapping) else {}
    return bool(
        context.get("day_utc") == day
        and context.get("source_day") == day
        and latest.get("context_day") == day
        and latest.get("source_day") == day
        and latest.get("context_hash")
        and latest.get("confidence")
        and latest.get("source_artifacts")
        and not latest.get("unsupported_claims")
    )


def _read(path: Path) -> dict[str, Any]:
    return read_json_v1(path)


def _list(value: Any) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _full_status(payload: Mapping[str, Any]) -> str:
    overview = payload.get("overview") if isinstance(payload.get("overview"), Mapping) else {}
    return str(overview.get("full_portfolio_pnl_status") or payload.get("full_portfolio_pnl_status") or "").upper()


def _data_quality(payload: Mapping[str, Any]) -> str:
    overview = payload.get("overview") if isinstance(payload.get("overview"), Mapping) else {}
    return str(overview.get("data_quality") or payload.get("data_quality_status") or "").upper()
