from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_models import DERIVED_TYPES, ROOT_TYPES, LifecycleState
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .certification_governance import (
    authority_boundary_metadata,
    scan_authority_boundary,
    validate_artifact_governance,
    validate_evidence_maturity_boundaries,
    validate_memory_governance,
)
from .certification_models import CertificationReport, CertificationStatus, CertificationType, result
from .failure_observatory import record_certification_block
from .governance import validate_no_forbidden_artifacts
from .learning_lifecycle import ALLOWED_TRANSITIONS, transitions_path
from .lineage import validate_lineage_integrity
from .memory_index import validate_memory_integrity
from .priority_engine import PriorityEngine, score_item
from .research_backlog import BACKLOG_ITEM_TYPES, BACKLOG_STATES, ResearchBacklog
from .worker_adapters import create_default_worker_adapters
from .worker_connection_rules import CONNECTED_STATUSES, is_output_artifact_allowed, is_worker_connection_allowed
from .worker_execution_records import validate_worker_run_record_integrity
from .worker_governance import validate_worker_connection

REPORT_SCHEMA_ID = "atlas_v2_research_os_certification_report_v1"
REPORT_SCHEMA_VERSION = "v1"


def run_research_os_certification(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> CertificationReport:
    root_path = Path(root)
    day_value = day or date.today().isoformat()
    store = ArtifactStore(root_path)
    backlog = ResearchBacklog(root_path)

    checks = [
        _check_artifact_store_integrity(store),
        _check_lineage_integrity(store),
        _check_memory_integrity(root_path, store),
        _check_backlog_integrity(backlog, store),
        _check_priority_engine_determinism(backlog, store),
        _check_lifecycle_transition_validity(store),
        _check_forbidden_artifact_absence(root_path),
        _check_authority_boundary(root_path, store),
        _check_evidence_maturity_boundaries(root_path, store),
        _check_operator_disposition_boundary(root_path, store),
        _check_candidate_quality_optional(),
        _check_worker_connection_contracts(),
        _check_worker_connection_preservation(),
        _check_worker_run_record_integrity(root_path),
    ]
    blockers = [
        f"{check.check_id}: {detail or check.summary}"
        for check in checks
        if check.status in {CertificationStatus.FAIL.value, CertificationStatus.BLOCKED.value}
        for detail in (check.details or [""])
    ]
    warnings = [
        f"{check.check_id}: {detail or check.summary}"
        for check in checks
        if check.status == CertificationStatus.WARNING.value
        for detail in (check.details or [""])
    ]
    status = CertificationStatus.PASS.value
    if any(check.status in {CertificationStatus.FAIL.value, CertificationStatus.BLOCKED.value} for check in checks):
        status = CertificationStatus.FAIL.value
    elif any(check.status == CertificationStatus.WARNING.value for check in checks):
        status = CertificationStatus.WARNING.value
    if status == CertificationStatus.FAIL.value:
        record_certification_block(root=root_path, error_message="Research OS certification failed.", certification_id=f"research-os-certification-{day_value}", metadata={"blocker_count": len(blockers), "warning_count": len(warnings)})
    return CertificationReport(
        schema_id=REPORT_SCHEMA_ID,
        schema_version=REPORT_SCHEMA_VERSION,
        day=day_value,
        root=str(root_path),
        status=status,
        checks=checks,
        blockers=blockers,
        warnings=warnings,
        optional_capabilities={"candidate_quality_certification_detected": _candidate_quality_present()},
        authority_boundary=authority_boundary_metadata(),
    )


def _check_artifact_store_integrity(store: ArtifactStore):
    failures: list[str] = []
    try:
        index = json.loads(store.index_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return result(CertificationType.FOUNDATION_CERTIFICATION, "artifact_store_integrity", CertificationStatus.FAIL, "artifact index is unreadable", details=[str(exc)])
    rows = index.get("artifacts")
    if not isinstance(rows, list):
        failures.append("artifact_index.json artifacts must be a list")
        rows = []
    seen: set[str] = set()
    for row in rows:
        artifact_id = str(row.get("artifact_id") or "")
        if not artifact_id:
            failures.append("artifact index row missing artifact_id")
            continue
        if artifact_id in seen:
            failures.append(f"duplicate artifact index row: {artifact_id}")
        seen.add(artifact_id)
        if not store.artifact_path(artifact_id).exists():
            failures.append(f"indexed artifact file missing: {artifact_id}")
            continue
        try:
            artifact = store.get_artifact(artifact_id)
        except Exception as exc:
            failures.append(f"artifact unreadable {artifact_id}: {exc}")
            continue
        if artifact.get("artifact_id") != artifact_id:
            failures.append(f"artifact id mismatch: {artifact_id}")
        if artifact.get("artifact_type") in DERIVED_TYPES and not artifact.get("source_artifact_ids"):
            failures.append(f"derived artifact missing source: {artifact_id}")
        if artifact.get("artifact_type") in ROOT_TYPES and not artifact.get("source_artifact_ids") and not artifact.get("is_root"):
            failures.append(f"root artifact not explicitly rooted: {artifact_id}")
    return result(CertificationType.FOUNDATION_CERTIFICATION, "artifact_store_integrity", _status(failures), "artifact store index and files are internally consistent", details=failures, metadata={"artifact_count": len(rows)})


def _check_lineage_integrity(store: ArtifactStore):
    ok, failures = validate_lineage_integrity(store)
    return result(CertificationType.LINEAGE_CERTIFICATION, "lineage_integrity", CertificationStatus.PASS if ok else CertificationStatus.FAIL, "lineage references are valid", details=failures)


def _check_memory_integrity(root: Path, store: ArtifactStore):
    try:
        ok, failures = validate_memory_integrity(root, artifact_store=store)
    except Exception as exc:
        return result(CertificationType.MEMORY_CERTIFICATION, "memory_integrity", CertificationStatus.FAIL, "memory index is invalid", details=[str(exc)])
    gov_ok, gov_failures = validate_memory_governance(root)
    failures = failures + gov_failures
    return result(CertificationType.MEMORY_CERTIFICATION, "memory_integrity", CertificationStatus.PASS if ok and gov_ok else CertificationStatus.FAIL, "memory objects and links are valid", details=failures)


def _check_backlog_integrity(backlog: ResearchBacklog, store: ArtifactStore):
    failures: list[str] = []
    for row in backlog.list_backlog_items():
        item_id = row.get("backlog_item_id", "<missing>")
        if row.get("item_type") not in BACKLOG_ITEM_TYPES:
            failures.append(f"invalid backlog item_type for {item_id}: {row.get('item_type')}")
        if row.get("state") not in BACKLOG_STATES:
            failures.append(f"invalid backlog state for {item_id}: {row.get('state')}")
        if row.get("state") == "BLOCKED" and not str(row.get("blocked_reason", "")).strip():
            failures.append(f"blocked backlog item missing blocked_reason: {item_id}")
        missing = [artifact_id for artifact_id in row.get("source_artifact_ids", []) if not store.exists(artifact_id)]
        if missing:
            failures.append(f"backlog item {item_id} references missing artifacts: {missing}")
        recomputed = score_item(row)
        if round(float(row.get("priority_score", 0.0)), 6) != recomputed:
            failures.append(f"backlog item {item_id} priority_score is not deterministic: stored={row.get('priority_score')} recomputed={recomputed}")
    return result(CertificationType.BACKLOG_PRIORITY_CERTIFICATION, "backlog_integrity", _status(failures), "backlog items are valid and priority scores recompute", details=failures, metadata={"backlog_count": len(backlog.list_backlog_items())})


def _check_priority_engine_determinism(backlog: ResearchBacklog, store: ArtifactStore):
    engine = PriorityEngine(backlog, store)
    deterministic_a = engine.select_next_items(limit=10, exploration_rate=0.0)
    deterministic_b = engine.select_next_items(limit=10, exploration_rate=0.0)
    seeded_a = engine.select_next_items(limit=10, exploration_rate=0.5, seed=42)
    seeded_b = engine.select_next_items(limit=10, exploration_rate=0.5, seed=42)
    failures = []
    if deterministic_a != deterministic_b:
        failures.append("priority mode selection changed across identical calls")
    if seeded_a != seeded_b:
        failures.append("seeded exploration selection changed across identical calls")
    return result(CertificationType.BACKLOG_PRIORITY_CERTIFICATION, "priority_engine_deterministic_behavior", _status(failures), "priority engine behavior is deterministic for priority mode and fixed seeds", details=failures, metadata={"sample_count": len(deterministic_a)})


def _check_lifecycle_transition_validity(store: ArtifactStore):
    path = transitions_path(store)
    if not path.exists():
        return result(CertificationType.LIFECYCLE_CERTIFICATION, "lifecycle_transition_validity", CertificationStatus.PASS, "no lifecycle transitions recorded")
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return result(CertificationType.LIFECYCLE_CERTIFICATION, "lifecycle_transition_validity", CertificationStatus.FAIL, "lifecycle transition file is unreadable", details=[str(exc)])
    failures: list[str] = []
    if not isinstance(rows, list):
        failures.append("lifecycle transitions must be a list")
        rows = []
    for row in rows:
        transition_id = row.get("transition_id", "<missing>")
        from_state = str(row.get("from_state", ""))
        to_state = str(row.get("to_state", ""))
        if row.get("artifact_id") and not store.exists(str(row["artifact_id"])):
            failures.append(f"transition references missing artifact: {transition_id}")
        if from_state not in {state.value for state in LifecycleState}:
            failures.append(f"invalid from_state for {transition_id}: {from_state}")
        if to_state not in {state.value for state in LifecycleState}:
            failures.append(f"invalid to_state for {transition_id}: {to_state}")
        if to_state not in ALLOWED_TRANSITIONS.get(from_state, set()):
            failures.append(f"invalid lifecycle transition for {transition_id}: {from_state} -> {to_state}")
        if not str(row.get("reason", "")).strip():
            failures.append(f"transition missing reason: {transition_id}")
    return result(CertificationType.LIFECYCLE_CERTIFICATION, "lifecycle_transition_validity", _status(failures), "lifecycle transitions follow allowed state edges", details=failures, metadata={"transition_count": len(rows)})


def _check_forbidden_artifact_absence(root: Path):
    ok, failures = validate_no_forbidden_artifacts(root)
    return result(CertificationType.FORBIDDEN_ARTIFACT_CERTIFICATION, "forbidden_artifact_absence", CertificationStatus.PASS if ok else CertificationStatus.FAIL, "forbidden trading/capital/candidate artifact markers are absent", details=failures)


def _check_authority_boundary(root: Path, store: ArtifactStore):
    scan_ok, scan_failures = scan_authority_boundary(root)
    gov_ok, gov_failures = validate_artifact_governance(store)
    failures = scan_failures + gov_failures
    return result(CertificationType.AUTHORITY_BOUNDARY_CERTIFICATION, "authority_boundary_preservation", CertificationStatus.PASS if scan_ok and gov_ok else CertificationStatus.FAIL, "Research OS remains non-authoritative", details=failures, metadata=authority_boundary_metadata())


def _check_evidence_maturity_boundaries(root: Path, store: ArtifactStore):
    ok, failures = validate_evidence_maturity_boundaries(store, root)
    return result(CertificationType.GOVERNANCE_CERTIFICATION, "evidence_maturity_boundaries", CertificationStatus.PASS if ok else CertificationStatus.FAIL, "evidence maturity boundaries are preserved", details=failures)


def _check_operator_disposition_boundary(root: Path, store: ArtifactStore):
    ok, failures = validate_evidence_maturity_boundaries(store, root)
    operator_failures = [failure for failure in failures if "operator disposition" in failure]
    return result(CertificationType.GOVERNANCE_CERTIFICATION, "operator_disposition_not_evidence_level", CertificationStatus.PASS if ok or not operator_failures else CertificationStatus.FAIL, "operator disposition is not treated as an evidence level", details=operator_failures)


def _check_candidate_quality_optional():
    if not _candidate_quality_present():
        return result(CertificationType.GOVERNANCE_CERTIFICATION, "candidate_quality_optional_detection", CertificationStatus.SKIPPED_DEPENDENCY_MISSING, "candidate-quality certification dependency is not present")
    return result(CertificationType.GOVERNANCE_CERTIFICATION, "candidate_quality_optional_detection", CertificationStatus.WARNING, "candidate-quality modules detected; certification remains measurement-only and non-authoritative")


def _candidate_quality_present() -> bool:
    return (Path(__file__).with_name("candidate_quality_models.py")).exists()


def _status(failures: list[str]) -> CertificationStatus:
    return CertificationStatus.FAIL if failures else CertificationStatus.PASS


def _check_worker_connection_contracts():
    failures: list[str] = []
    connected_count = 0
    for adapter in create_default_worker_adapters():
        if adapter.adapter_status in CONNECTED_STATUSES:
            connected_count += 1
            if not is_worker_connection_allowed(adapter):
                failures.append(f"{adapter.worker_id}: worker connection rules failed")
            for output_type in adapter.supported_output_artifact_types:
                if not is_output_artifact_allowed(output_type):
                    failures.append(f"{adapter.worker_id}: output not allowed: {output_type}")
    if connected_count < 5:
        failures.append(f"expected at least 5 connected research workers, found {connected_count}")
    return result(
        CertificationType.WORKER_CONNECTION_CERTIFICATION,
        "connected_worker_contract_validity",
        _status(failures),
        "connected worker contracts are valid and fail closed",
        details=failures,
        metadata={"connected_worker_count": connected_count},
    )


def _check_worker_connection_preservation():
    failures: list[str] = []
    required_workers = {
        "atlas_v2_claim_worker_adapter",
        "atlas_v2_hypothesis_worker_adapter",
        "atlas_v2_experiment_design_worker_adapter",
        "atlas_v2_learning_worker_adapter",
        "atlas_v2_evaluation_worker_adapter",
    }
    seen = set()
    for adapter in create_default_worker_adapters():
        if adapter.adapter_status in CONNECTED_STATUSES:
            seen.add(adapter.worker_id)
            governance = validate_worker_connection(adapter)
            failures.extend(governance.violations)
    missing = sorted(required_workers - seen)
    if missing:
        failures.append(f"required connected workers missing: {missing}")
    return result(
        CertificationType.WORKER_CONNECTION_CERTIFICATION,
        "connected_worker_lineage_evidence_governance_preservation",
        _status(failures),
        "connected workers preserve lineage, evidence labels, and governance boundaries",
        details=failures,
        metadata={"connected_workers": sorted(seen)},
    )


def _check_worker_run_record_integrity(root: Path):
    try:
        ok, failures = validate_worker_run_record_integrity(root)
    except Exception as exc:
        return result(
            CertificationType.WORKER_CONNECTION_CERTIFICATION,
            "worker_run_record_integrity",
            CertificationStatus.FAIL,
            "worker run records are invalid",
            details=[str(exc)],
        )
    return result(
        CertificationType.WORKER_CONNECTION_CERTIFICATION,
        "worker_run_record_integrity",
        CertificationStatus.PASS if ok else CertificationStatus.FAIL,
        "worker run records are indexed and internally consistent",
        details=failures,
    )
