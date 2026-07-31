from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_models import FORBIDDEN_ARTIFACT_TYPES
from .governance import validate_no_forbidden_artifacts
from .worker_governance import validate_connected_worker_outputs

ALLOWED_RECOMMENDATIONS = {
    "Continue research execution.",
    "Review blocked backlog item.",
    "Run more paper-forward observations.",
    "Investigate repeated failure.",
}

FORBIDDEN_RECOMMENDATIONS = {
    "Trade this.",
    "Promote candidate.",
    "Deploy sleeve.",
    "Allocate capital.",
    "Increase position size.",
}

AUTHORITY_FLAGS = {
    "live_use_authorized",
    "capital_authorized",
    "candidate_promotion_authorized",
    "trade_advice_allowed",
    "broker_execution_allowed",
    "sleeve_deployment_authorized",
    "portfolio_recommendation_authorized",
    "position_sizing_authorized",
    "candidate_factory_modified",
    "candidate_generation_modified",
    "recurring_loop_enabled",
    "scheduler_enabled",
}

AUTHORITY_MARKERS = {
    "live trading",
    "broker execution",
    "trade advice",
    "promote candidate",
    "candidate promotion",
    "authorize capital",
    "capital authorization",
    "deploy sleeve",
    "construct portfolio",
    "position sizing",
    "increase position size",
}


def validate_autonomous_research_execution_allowed(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = dict(payload or {})
    failures: list[str] = []
    if payload.get("bounded_once") is not True:
        failures.append("bounded research execution must be explicitly one-shot")
    if payload.get("scheduler_enabled") is True or payload.get("recurring_loop_enabled") is True or payload.get("daemon_enabled") is True:
        failures.append("bounded research execution cannot enable scheduling or recurring operation")
    mode = str(payload.get("mode") or "run_once")
    if mode not in {"run_once", "dry_run", "audit"}:
        failures.append(f"unsupported bounded research mode: {mode}")
    return _result("autonomous_research_execution_allowed", failures)


def validate_autonomous_research_no_authority_escalation(payload: dict[str, Any] | list[dict[str, Any]]) -> dict[str, Any]:
    rows = payload if isinstance(payload, list) else [payload]
    failures: list[str] = []
    for row in rows:
        metadata = row.get("metadata", {}) if isinstance(row, dict) else {}
        if isinstance(metadata, dict):
            for flag in AUTHORITY_FLAGS:
                if row.get(flag) is True or metadata.get(flag) is True:
                    failures.append(f"authority flag set: {flag}")
        recommendation = row.get("recommendation") or (metadata.get("recommendation") if isinstance(metadata, dict) else None)
        if recommendation in FORBIDDEN_RECOMMENDATIONS:
            failures.append(f"forbidden recommendation: {recommendation}")
        elif recommendation and recommendation not in ALLOWED_RECOMMENDATIONS:
            failures.append(f"unsupported recommendation: {recommendation}")
        text = str(row).lower().replace("_", " ").replace("-", " ")
        for marker in AUTHORITY_MARKERS:
            if marker in text:
                failures.append(f"authority escalation marker present: {marker}")
                break
    return _result("autonomous_research_no_authority_escalation", failures)


def validate_autonomous_research_no_forbidden_artifacts(root: str | Path, payloads: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    ok, failures = validate_no_forbidden_artifacts(root)
    rows = list(payloads or [])
    for row in rows:
        artifact_type = str(row.get("artifact_type") or "")
        if artifact_type in FORBIDDEN_ARTIFACT_TYPES:
            failures.append(f"forbidden artifact type: {artifact_type}")
        text = str(row)
        for forbidden in FORBIDDEN_ARTIFACT_TYPES:
            if f'"artifact_type": "{forbidden}"' in text:
                failures.append(f"forbidden artifact marker: {forbidden}")
    return _result("autonomous_research_no_forbidden_artifacts", [] if ok and not failures else failures)


def validate_autonomous_research_worker_outputs(input_artifacts: list[dict[str, Any]], output_artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    result = validate_connected_worker_outputs(output_artifacts, input_artifacts)
    return _result("autonomous_research_worker_outputs", list(result.violations), warnings=list(result.warnings))


def validate_autonomous_research_candidate_isolation(payload: dict[str, Any] | list[dict[str, Any]]) -> dict[str, Any]:
    rows = payload if isinstance(payload, list) else [payload]
    failures: list[str] = []
    blocked_terms = ["candidate_factory_modified", "candidate_generation_modified", "candidate_promotion_authorized", "capital_authorized", "trade_advice_allowed"]
    for row in rows:
        metadata = row.get("metadata", {}) if isinstance(row, dict) else {}
        for term in blocked_terms:
            if row.get(term) is True or (isinstance(metadata, dict) and metadata.get(term) is True):
                failures.append(f"candidate isolation violation: {term}")
    return _result("autonomous_research_candidate_isolation", failures)


def safety_gate_result(gate_id: str, failures: list[str], *, evidence_refs: list[str] | None = None, warnings: list[str] | None = None) -> dict[str, Any]:
    payload = _result(gate_id, failures, warnings=warnings)
    payload["evidence_refs"] = list(evidence_refs or [])
    return payload


def _result(gate_id: str, failures: list[str], *, warnings: list[str] | None = None) -> dict[str, Any]:
    return {
        "gate_id": gate_id,
        "result": "FAIL" if failures else "PASS",
        "details": list(failures),
        "warnings": list(warnings or []),
    }
