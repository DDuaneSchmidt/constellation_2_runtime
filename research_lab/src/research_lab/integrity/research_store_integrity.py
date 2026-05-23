from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

from jsonschema.exceptions import ValidationError

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import SCHEMAS, validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


GENERATED_AT = "1970-01-01T00:00:00Z"
SCHEMA_VERSION = "research_store_integrity_report.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"

Finding = dict[str, Any]
PathFactory = Callable[[Path, str], Path]


def _report_dir(store: Path) -> Path:
    return store / "integrity_reports"


def _report_path(store: Path, report_id: str) -> Path:
    return _report_dir(store) / f"{report_id}.json"


def _registry_path(store: Path) -> Path:
    return store / "registries" / "integrity_report_registry.json"


def _row_file(directory: str, filename: str) -> PathFactory:
    return lambda store, artifact_id: store / directory / artifact_id / filename


def _flat_file(directory: str) -> PathFactory:
    return lambda store, artifact_id: store / directory / f"{artifact_id}.json"


def _stability_file(family: str) -> PathFactory:
    return lambda store, artifact_id: store / "stability_reports" / family / f"{artifact_id}.json"


REGISTRY_SPECS: list[dict[str, Any]] = [
    {"registry": "challenger_variants.jsonl", "id": "challenger_variant_id", "path": _flat_file("challenger_variants"), "contract": "challenger_variant"},
    {"registry": "challenger_evidence_batches.jsonl", "id": "challenger_evidence_batch_id", "path": _flat_file("challenger_evidence_batches"), "contract": "challenger_evidence_batch"},
    {"registry": "challenger_comparison_reports.jsonl", "id": "challenger_comparison_report_id", "path": _flat_file("challenger_comparison_reports"), "contract": "challenger_comparison_report"},
    {"registry": "human_review_dossiers.jsonl", "id": "human_review_dossier_id", "path": _flat_file("human_review_dossiers"), "contract": "human_review_dossier"},
    {"registry": "human_review_decision_registry.json", "id": "human_review_decision_id", "path": _flat_file("human_review_decisions"), "contract": "human_review_decision"},
    {"registry": "candidate_batches.jsonl", "id": "candidate_batch_id", "path": _row_file("candidate_batches", "candidate_batch.json"), "contract": "candidate_batch"},
    {"registry": "evidence_packages.jsonl", "id": "evidence_package_id", "path": _row_file("evidence_packages", "evidence_manifest.json"), "contract": "evidence_package"},
    {"registry": "paper_trials.jsonl", "id": "paper_trial_id", "path": _row_file("paper_trials", "paper_trial.json"), "contract": "paper_trial"},
    {"registry": "expectancy_drift_reports.jsonl", "id": "expectancy_drift_report_id", "path": _stability_file("expectancy_drift"), "contract": "expectancy_drift_report"},
    {"registry": "regime_fragility_reports.jsonl", "id": "regime_fragility_report_id", "path": _stability_file("regime_fragility"), "contract": "regime_fragility_report"},
    {"registry": "sleeve_stability_reports.jsonl", "id": "sleeve_stability_report_id", "path": _stability_file("sleeve_stability"), "contract": "sleeve_stability_report"},
    {"registry": "challenger_event_studies.jsonl", "id": "event_study_id", "path": _flat_file("event_studies"), "contract": None},
    {"registry": "challenger_backtests.jsonl", "id": "backtest_id", "path": _flat_file("backtests"), "contract": None},
    {"registry": "longitudinal_candidate_runs.jsonl", "id": "longitudinal_run_id", "path": _row_file("longitudinal_runs", "longitudinal_run.json"), "contract": "longitudinal_candidate_run"},
]

AUDITED_ARTIFACT_DIRS = [
    "challenger_variants",
    "challenger_evidence_batches",
    "challenger_comparison_reports",
    "human_review_dossiers",
    "human_review_decisions",
    "event_studies",
    "backtests",
    "stability_reports/expectancy_drift",
    "stability_reports/regime_fragility",
    "stability_reports/sleeve_stability",
]

REQUIRED_LINEAGE_FIELDS = [
    "challenger_variant_id",
    "event_study_evidence_id",
    "backtest_evidence_id",
    "longitudinal_run_id",
    "expectancy_drift_report_id",
    "regime_fragility_report_id",
    "sleeve_stability_report_id",
]


def _finding(*, severity: str, source: str, message: str, **extra: Any) -> Finding:
    return {"severity": severity, "source": source, "message": message, **extra}


def _safe_json(path: Path, failures: list[Finding]) -> dict[str, Any] | None:
    try:
        return read_json(path)
    except Exception as exc:
        failures.append(_finding(severity="ERROR", source=str(path), message=str(exc), artifact_path=str(path)))
        return None


def _artifact_path_for(store: Path, field: str, artifact_id: str) -> Path:
    mappings = {
        "challenger_variant_id": store / "challenger_variants" / f"{artifact_id}.json",
        "event_study_evidence_id": store / "event_studies" / f"{artifact_id}.json",
        "backtest_evidence_id": store / "backtests" / f"{artifact_id}.json",
        "longitudinal_run_id": store / "longitudinal_runs" / artifact_id / "longitudinal_run.json",
        "expectancy_drift_report_id": store / "stability_reports" / "expectancy_drift" / f"{artifact_id}.json",
        "regime_fragility_report_id": store / "stability_reports" / "regime_fragility" / f"{artifact_id}.json",
        "sleeve_stability_report_id": store / "stability_reports" / "sleeve_stability" / f"{artifact_id}.json",
        "challenger_evidence_batch_id": store / "challenger_evidence_batches" / f"{artifact_id}.json",
        "challenger_comparison_report_id": store / "challenger_comparison_reports" / f"{artifact_id}.json",
        "human_review_dossier_id": store / "human_review_dossiers" / f"{artifact_id}.json",
    }
    return mappings[field]


def _expected_hash(payload: dict[str, Any]) -> str:
    if payload.get("human_review_decision_id"):
        return content_hash(payload, exclude={"immutable_hash", "content_hash"}, sort_lists=True)
    if payload.get("challenger_variant_id") or payload.get("integrity_report_id"):
        return content_hash(payload, exclude={"immutable_hash", "content_hash", "generated_at"}, sort_lists=True)
    return content_hash(payload, exclude={"immutable_hash", "content_hash", "generated_at", "created_at"}, sort_lists=True)


def _check_hash(path: Path, payload: dict[str, Any], failures: list[Finding]) -> None:
    stored = str(payload.get("immutable_hash") or "")
    if not stored:
        return
    actual = _expected_hash(payload)
    if stored != actual:
        failures.append(
            _finding(
                severity="ERROR",
                source=str(path),
                message="immutable_hash mismatch",
                artifact_id=payload.get("human_review_decision_id") or payload.get("challenger_variant_id") or path.stem,
                stored_hash=stored,
                recomputed_hash=actual,
            )
        )


def _check_schema(path: Path, payload: dict[str, Any], contract: str | None, failures: list[Finding]) -> None:
    if not contract or contract not in SCHEMAS:
        return
    try:
        validate_contract(contract, payload)
    except ValidationError as exc:
        severity = "WARNING" if contract in {"expectancy_drift_report", "regime_fragility_report", "sleeve_stability_report"} and "created_at" in exc.message else "ERROR"
        failures.append(_finding(severity=severity, source=str(path), message=f"schema validation failed: {exc.message}", contract=contract))


def _registry_rows(store: Path, registry: str) -> list[dict[str, Any]]:
    return read_jsonl(store / "registries" / registry)


def _collect_registry_state(store: Path) -> tuple[dict[str, list[dict[str, Any]]], set[str], list[Finding], list[Finding]]:
    rows_by_registry: dict[str, list[dict[str, Any]]] = {}
    referenced_paths: set[str] = set()
    missing: list[Finding] = []
    duplicates: list[Finding] = []
    for spec in REGISTRY_SPECS:
        registry = str(spec["registry"])
        id_field = str(spec["id"])
        rows = _registry_rows(store, registry)
        rows_by_registry[registry] = rows
        ids = [str(row.get(id_field) or "") for row in rows if row.get(id_field)]
        for artifact_id in ids:
            path = spec["path"](store, artifact_id)
            referenced_paths.add(str(path.resolve()))
            if not path.exists():
                missing.append(_finding(severity="ERROR", source=registry, message="registry entry artifact missing", registry_name=registry, registry_entry_id=artifact_id, missing_artifact_path=str(path)))
        for artifact_id, count in Counter(ids).items():
            if count > 1:
                duplicates.append(_finding(severity="ERROR", source=registry, message="duplicate registry entry id", registry_name=registry, artifact_id=artifact_id, duplicate_count=count))
    return rows_by_registry, referenced_paths, missing, duplicates


def _audit_coverage(store: Path, rows_by_registry: dict[str, list[dict[str, Any]]]) -> tuple[Counter[str], list[Finding]]:
    audit_rows = read_jsonl(store / "audit_log" / "audit_events.jsonl")
    by_action = Counter(str(row.get("action") or "") for row in audit_rows)
    by_entity = {(str(row.get("entity_type") or ""), str(row.get("entity_id") or ""), str(row.get("action") or "")) for row in audit_rows}
    findings: list[Finding] = []
    required = [
        ("challenger_variants.jsonl", "challenger_variant_id", "challenger_variant", "challenger_variant_created"),
        ("challenger_evidence_batches.jsonl", "challenger_evidence_batch_id", "challenger_evidence_batch", "challenger_evidence_batch_created"),
        ("challenger_comparison_reports.jsonl", "challenger_comparison_report_id", "challenger_comparison_report", "challenger_comparison_report_created"),
        ("human_review_dossiers.jsonl", "human_review_dossier_id", "human_review_dossier", "human_review_dossier_created"),
        ("human_review_decision_registry.json", "human_review_decision_id", "human_review_decision", "human_review_decision_recorded"),
    ]
    for registry, id_field, entity_type, action in required:
        for row in rows_by_registry.get(registry, []):
            artifact_id = str(row.get(id_field) or "")
            if artifact_id and (entity_type, artifact_id, action) not in by_entity:
                findings.append(_finding(severity="WARNING", source=registry, message="audit coverage missing for modern artifact", artifact_id=artifact_id, required_action=action))
    return by_action, findings


def _lineage_checks(store: Path, referenced_paths: set[str]) -> tuple[list[Finding], list[Finding], list[Finding]]:
    missing: list[Finding] = []
    breaks: list[Finding] = []
    blockers: list[Finding] = []
    for batch_path in sorted((store / "challenger_evidence_batches").glob("*.json")):
        batch = _safe_json(batch_path, breaks)
        if not batch:
            continue
        legacy_projection_batch = any(
            item.get("status") == "generated"
            and item.get("materialization_method") != "fully_materialized_rule_delta_evidence_chain"
            and not item.get("artifact_lineage")
            for item in batch.get("challenger_evidence_items") or []
        )
        lineage_severity = "WARNING" if legacy_projection_batch else "ERROR"
        for item in batch.get("challenger_evidence_items") or []:
            source = f"{batch.get('challenger_evidence_batch_id')}:{item.get('challenger_hypothesis_id')}"
            if item.get("status") == "blocked":
                reason = str(item.get("failure_reason") or "")
                severity = "INFO" if reason else "ERROR"
                blockers.append(_finding(severity=severity, source=source, message="blocked challenger hypothesis", reason=reason, recoverable=(item.get("blocker") or {}).get("recoverable")))
                if not reason:
                    breaks.append(_finding(severity="ERROR", source=source, message="blocked challenger missing failure reason"))
                continue
            for field in REQUIRED_LINEAGE_FIELDS:
                artifact_id = str(item.get(field) or "")
                if not artifact_id:
                    breaks.append(_finding(severity=lineage_severity, source=source, message="challenger evidence lineage field missing", field=field))
                    continue
                path = _artifact_path_for(store, field, artifact_id)
                referenced_paths.add(str(path.resolve()))
                if not path.exists():
                    missing.append(_finding(severity=lineage_severity, source=source, message="challenger evidence referenced artifact missing", field=field, artifact_id=artifact_id, expected_path=str(path)))
    for report_path in sorted((store / "challenger_comparison_reports").glob("*.json")):
        report = _safe_json(report_path, breaks)
        if not report:
            continue
        batch_id = str(report.get("challenger_evidence_batch_id") or "")
        if batch_id:
            path = _artifact_path_for(store, "challenger_evidence_batch_id", batch_id)
            referenced_paths.add(str(path.resolve()))
            if not path.exists():
                missing.append(_finding(severity="ERROR", source=str(report_path), message="comparison evidence batch missing", artifact_id=batch_id, expected_path=str(path)))
    for dossier_path in sorted((store / "human_review_dossiers").glob("*.json")):
        dossier = _safe_json(dossier_path, breaks)
        if not dossier:
            continue
        report_id = str(dossier.get("challenger_comparison_report_id") or "")
        if report_id:
            path = _artifact_path_for(store, "challenger_comparison_report_id", report_id)
            referenced_paths.add(str(path.resolve()))
            if not path.exists():
                missing.append(_finding(severity="ERROR", source=str(dossier_path), message="dossier comparison report missing", artifact_id=report_id, expected_path=str(path)))
    for decision_path in sorted((store / "human_review_decisions").glob("*.json")):
        decision = _safe_json(decision_path, breaks)
        if not decision:
            continue
        dossier_id = str(decision.get("human_review_dossier_id") or "")
        path = _artifact_path_for(store, "human_review_dossier_id", dossier_id)
        referenced_paths.add(str(path.resolve()))
        if not path.exists():
            missing.append(_finding(severity="ERROR", source=str(decision_path), message="decision dossier missing", artifact_id=dossier_id, expected_path=str(path)))
        else:
            dossier = read_json(path)
            actual = content_hash(dossier, exclude={"generated_at"}, sort_lists=True)
            if actual != decision.get("source_dossier_hash"):
                breaks.append(_finding(severity="ERROR", source=str(decision_path), message="decision source dossier hash mismatch", artifact_id=decision.get("human_review_decision_id"), stored_hash=decision.get("source_dossier_hash"), recomputed_hash=actual))
        lineage = (decision.get("source_artifact_ids") or {}).get("artifact_lineage") or {}
        for field in REQUIRED_LINEAGE_FIELDS:
            artifact_id = str(lineage.get(field) or "")
            if not artifact_id:
                missing.append(_finding(severity="ERROR", source=str(decision_path), message="decision source artifact lineage missing", field=field))
                continue
            ref_path = _artifact_path_for(store, field, artifact_id)
            referenced_paths.add(str(ref_path.resolve()))
            if not ref_path.exists():
                missing.append(_finding(severity="ERROR", source=str(decision_path), message="decision source artifact missing", field=field, artifact_id=artifact_id, expected_path=str(ref_path)))
    return missing, breaks, blockers


def _mutation_boundary_checks(store: Path) -> list[Finding]:
    findings: list[Finding] = []
    for path in sorted((store / "human_review_decisions").glob("*.json")):
        payload = read_json(path)
        constraints = payload.get("governance_constraints") or {}
        forbidden = {
            "paper_trial_created": False,
            "sleeve_mutated": False,
            "challenger_lifecycle_mutated": False,
            "candidate_ledger_mutated": False,
            "capital_allocation_allowed": False,
            "broker_execution_allowed": False,
            "order_execution_allowed": False,
            "automatic_promotion_allowed": False,
        }
        for field, expected in forbidden.items():
            if constraints.get(field) is not expected:
                findings.append(_finding(severity="ERROR", source=str(path), message="mutation boundary violation", field=field, value=constraints.get(field)))
    return findings


def _artifact_and_schema_checks(store: Path) -> tuple[int, list[Finding], list[Finding], set[str]]:
    artifact_count = 0
    hash_mismatches: list[Finding] = []
    schema_failures: list[Finding] = []
    artifact_paths: set[str] = set()
    contract_by_dir = {
        "challenger_variants": "challenger_variant",
        "challenger_evidence_batches": "challenger_evidence_batch",
        "challenger_comparison_reports": "challenger_comparison_report",
        "human_review_dossiers": "human_review_dossier",
        "human_review_decisions": "human_review_decision",
        "stability_reports/expectancy_drift": "expectancy_drift_report",
        "stability_reports/regime_fragility": "regime_fragility_report",
        "stability_reports/sleeve_stability": "sleeve_stability_report",
    }
    for rel in AUDITED_ARTIFACT_DIRS:
        root = store / rel
        if not root.exists():
            continue
        for path in sorted(root.glob("*.json")):
            artifact_count += 1
            artifact_paths.add(str(path.resolve()))
            payload = _safe_json(path, schema_failures)
            if not payload:
                continue
            _check_hash(path, payload, hash_mismatches)
            _check_schema(path, payload, contract_by_dir.get(rel), schema_failures)
    return artifact_count, hash_mismatches, schema_failures, artifact_paths


def _orphan_checks(artifact_paths: set[str], referenced_paths: set[str], audit_rows: list[dict[str, Any]]) -> list[Finding]:
    audit_text = "\n".join(str(row.get("entity_id") or "") for row in audit_rows)
    findings = []
    for path in sorted(artifact_paths - referenced_paths):
        artifact_id = Path(path).stem
        if artifact_id in audit_text:
            continue
        findings.append(_finding(severity="WARNING", source=path, message="orphan artifact not referenced by registry, lineage, or audit event", artifact_id=artifact_id))
    return findings


def build_research_store_integrity_report(
    *,
    store_root: Path | None = None,
    scope: str = "full",
    generated_at: str = GENERATED_AT,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    rows_by_registry, referenced_paths, registry_missing, duplicate_entries = _collect_registry_state(store)
    artifact_count, hash_mismatches, schema_failures, artifact_paths = _artifact_and_schema_checks(store)
    lineage_missing, lineage_breaks, blockers = _lineage_checks(store, referenced_paths)
    mutation_violations = _mutation_boundary_checks(store)
    audit_rows = read_jsonl(store / "audit_log" / "audit_events.jsonl")
    audit_counts, audit_findings = _audit_coverage(store, rows_by_registry)
    orphaned = _orphan_checks(artifact_paths, referenced_paths, audit_rows)
    missing_references = registry_missing + lineage_missing
    checked_groups = [missing_references, hash_mismatches, schema_failures, lineage_breaks, duplicate_entries, mutation_violations, audit_findings, orphaned]
    errors = [row for group in checked_groups for row in group if row.get("severity") == "ERROR"]
    warnings = [row for group in checked_groups for row in group if row.get("severity") == "WARNING"]
    status = "FAIL" if errors else ("PASS_WITH_WARNINGS" if warnings else "PASS")
    source_registry_ids = {name: [str(row.get(spec["id"]) or "") for row in rows_by_registry.get(name, []) if row.get(spec["id"])] for spec in REGISTRY_SPECS for name in [str(spec["registry"])]}
    payload = {
        "integrity_report_id": "",
        "generated_at": generated_at,
        "research_store_root": str(store),
        "report_scope": scope,
        "overall_status": status,
        "artifact_counts": {"audited_json_artifacts": artifact_count, "audited_artifact_paths": len(artifact_paths)},
        "registry_counts": {name: len(rows) for name, rows in sorted(rows_by_registry.items())},
        "audit_event_counts": dict(sorted(audit_counts.items())),
        "missing_references": missing_references,
        "orphaned_artifacts": orphaned,
        "hash_mismatches": hash_mismatches,
        "schema_validation_failures": schema_failures,
        "lineage_breaks": lineage_breaks,
        "duplicate_registry_entries": duplicate_entries,
        "mutation_boundary_violations": mutation_violations,
        "unresolved_blockers": blockers,
        "warnings": warnings,
        "source_registry_ids": source_registry_ids,
        "source_artifact_ids": sorted(Path(path).stem for path in artifact_paths),
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    seed = content_hash(payload, exclude={"integrity_report_id", "immutable_hash", "content_hash", "generated_at"}, sort_lists=True)
    payload["integrity_report_id"] = f"rsir_{short_hash(seed, 16)}"
    payload["immutable_hash"] = content_hash(payload, exclude={"immutable_hash", "content_hash", "generated_at"}, sort_lists=True)
    payload["content_hash"] = payload["immutable_hash"]
    validate_contract("research_store_integrity_report", payload)
    return payload


def write_research_store_integrity_report(
    *,
    store_root: Path | None = None,
    scope: str = "full",
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = build_research_store_integrity_report(store_root=store, scope=scope)
    path = _report_path(store, report["integrity_report_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable integrity report: {path}")
    write_json(path, report, overwrite=False)
    row = {
        "integrity_report_id": report["integrity_report_id"],
        "generated_at": report["generated_at"],
        "overall_status": report["overall_status"],
        "error_count": error_count(report),
        "warning_count": warning_count(report),
        "immutable_hash": report["immutable_hash"],
    }
    append_jsonl(_registry_path(store), row)
    if not read_jsonl(_registry_path(store)) or read_jsonl(_registry_path(store))[-1] != row:
        raise RuntimeError("integrity report registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="research_store_integrity_report",
        entity_id=report["integrity_report_id"],
        action="research_store_integrity_report_generated",
        new_state_hash=report["immutable_hash"],
        reason="Generated read-only Research Store integrity report.",
        metadata={"integrity_report_id": report["integrity_report_id"], "overall_status": report["overall_status"], "error_count": row["error_count"], "warning_count": row["warning_count"], "immutable_hash": report["immutable_hash"]},
        store_root=store,
    )
    return {"report": report, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def error_count(report: dict[str, Any]) -> int:
    fields = ["missing_references", "hash_mismatches", "schema_validation_failures", "lineage_breaks", "duplicate_registry_entries", "mutation_boundary_violations"]
    return sum(1 for field in fields for row in report.get(field, []) if row.get("severity") == "ERROR")


def warning_count(report: dict[str, Any]) -> int:
    return sum(1 for row in report.get("warnings", []) if row.get("severity") == "WARNING")


def load_integrity_report(integrity_report_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_report_path(store, integrity_report_id))


def list_integrity_reports(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))


def latest_integrity_report(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_integrity_reports(store_root=store_root)
    if not rows:
        return None
    latest = sorted(rows, key=lambda row: (str(row.get("generated_at") or ""), str(row.get("integrity_report_id") or "")))[-1]
    return load_integrity_report(str(latest["integrity_report_id"]), store_root=store_root)
