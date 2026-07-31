from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_models import EvidenceLevel
from .artifact_store import ArtifactStore
from .governance import validate_no_forbidden_artifacts
from .lineage import validate_lineage_integrity
from .orchestrator_models import FORBIDDEN_ARTIFACT_MARKERS, SafetyGateId, SafetyGateResult


def run_safety_gates(
    root: str | Path,
    *,
    selected_backlog_items: list[dict[str, Any]] | None = None,
    artifacts_created: list[dict[str, Any]] | None = None,
    runtime_truth: dict[str, Any] | None = None,
    verified_graph: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    selected = list(selected_backlog_items or [])
    artifacts = list(artifacts_created or [])
    return [
        _label_integrity_gate(selected, artifacts),
        _authority_boundary_gate(runtime_truth, verified_graph),
        _forbidden_artifact_gate(root, selected, artifacts),
        _lineage_integrity_gate(root, artifacts),
        _evidence_level_gate(selected, artifacts),
        _candidate_capital_isolation_gate(selected, artifacts),
    ]


def safety_gates_passed(results: list[dict[str, Any]]) -> bool:
    return all(str(row.get("result")) == "PASS" for row in results)


def _result(gate_id: SafetyGateId, failures: list[str], evidence_refs: list[str] | None = None) -> dict[str, Any]:
    return SafetyGateResult(
        gate_id=gate_id.value,
        result="FAIL" if failures else "PASS",
        evidence_refs=list(evidence_refs or []),
        details=failures,
    ).to_dict()


def _label_integrity_gate(selected: list[dict[str, Any]], artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    failures: list[str] = []
    for item in selected + artifacts:
        labels = [str(label).lower() for label in item.get("labels", [])]
        text = json.dumps(item, sort_keys=True).lower()
        if "operator_approved" in labels:
            failures.append("OPERATOR_APPROVED is authority/disposition, not evidence maturity")
        if "validated" in labels and "generated_only" in text:
            failures.append("generated-only material cannot be labeled as validated truth")
    return _result(SafetyGateId.LABEL_INTEGRITY, failures)


def _authority_boundary_gate(runtime_truth: dict[str, Any] | None, verified_graph: dict[str, Any] | None) -> dict[str, Any]:
    failures: list[str] = []
    evidence_refs: list[str] = []
    if runtime_truth is None:
        failures.append("runtime truth kernel unavailable")
    else:
        evidence_refs.append(str(runtime_truth.get("path") or "runtime_truth_kernel"))
        if runtime_truth.get("autonomous_execution_allowed") is True:
            failures.append("runtime truth unexpectedly permits autonomous execution for this shell")
        if runtime_truth.get("trade_advice_allowed") is True:
            failures.append("runtime truth unexpectedly permits trade advice for this shell")
    if verified_graph is None:
        failures.append("verified runtime graph unavailable")
    else:
        evidence_refs.append(str(verified_graph.get("path") or "verified_runtime_graph"))
        if str(verified_graph.get("graph_status") or "") == "BLOCKED":
            failures.append("verified runtime graph is BLOCKED")
    return _result(SafetyGateId.AUTHORITY_BOUNDARY, failures, evidence_refs)


def _forbidden_artifact_gate(root: str | Path, selected: list[dict[str, Any]], artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    ok, failures = validate_no_forbidden_artifacts(root)
    all_failures = list(failures if not ok else [])
    markers = {item.lower() for item in FORBIDDEN_ARTIFACT_MARKERS}
    for item in selected + artifacts:
        text = json.dumps(item, sort_keys=True).lower()
        for marker in markers:
            if marker in text:
                all_failures.append(f"forbidden artifact marker in orchestrator payload: {marker}")
    return _result(SafetyGateId.FORBIDDEN_ARTIFACT_AUDIT, all_failures)


def _lineage_integrity_gate(root: str | Path, artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    ok, failures = validate_lineage_integrity(ArtifactStore(root))
    for artifact in artifacts:
        if artifact.get("artifact_id") and not artifact.get("lineage_bundle_id") and artifact.get("source_artifact_ids"):
            failures.append(f"created artifact missing lineage bundle: {artifact['artifact_id']}")
    return _result(SafetyGateId.LINEAGE_INTEGRITY, failures if not ok or failures else [])


def _evidence_level_gate(selected: list[dict[str, Any]], artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    allowed = {item.value for item in EvidenceLevel} - {EvidenceLevel.OPERATOR_APPROVED.value}
    failures: list[str] = []
    for item in selected + artifacts:
        evidence = item.get("evidence_level") or item.get("evidence_maturity_level")
        if evidence and str(evidence) not in allowed:
            failures.append(f"invalid or authority-shaped evidence level: {evidence}")
    return _result(SafetyGateId.EVIDENCE_LEVEL_VALIDATION, failures)


def _candidate_capital_isolation_gate(selected: list[dict[str, Any]], artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    forbidden = ["capital_authority", "candidate_generation_authority", "candidate_promotion", "portfolio_recommendation", "position_sizing"]
    failures: list[str] = []
    for item in selected + artifacts:
        text = json.dumps(item, sort_keys=True).lower()
        for term in forbidden:
            if term in text:
                failures.append(f"candidate/capital isolation violation: {term}")
    return _result(SafetyGateId.CANDIDATE_CAPITAL_ISOLATION, failures)
