from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.challengers.challenger_comparison import list_challenger_comparison_reports, load_challenger_comparison_report
from research_lab.challengers.challenger_evidence import list_challenger_evidence_batches, load_challenger_evidence_batch
from research_lab.challengers.human_review_decision import list_human_review_decisions, load_human_review_decision
from research_lab.challengers.human_review_dossier import list_human_review_dossiers, load_human_review_dossier
from research_lab.contracts.schemas import validate_contract
from research_lab.integrity.research_store_integrity import error_count, latest_integrity_report, warning_count
from research_lab.stability.sleeve_stability import latest_expectancy_drift_report, latest_regime_fragility_report, latest_sleeve_stability_report
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "research_os_status_report.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"


def _report_dir(store: Path) -> Path:
    return store / "status_reports"


def _report_path(store: Path, report_id: str) -> Path:
    return _report_dir(store) / f"{report_id}.json"


def _registry_path(store: Path) -> Path:
    return store / "registries" / "research_os_status_report_registry.json"


def _latest_row(rows: list[dict[str, Any]], id_field: str, time_field: str = "generated_at") -> dict[str, Any] | None:
    if not rows:
        return None
    return rows[-1]


def _load_latest_challenger_evidence_batch(store: Path) -> dict[str, Any] | None:
    row = _latest_row(list_challenger_evidence_batches(store_root=store), "challenger_evidence_batch_id")
    return load_challenger_evidence_batch(str(row["challenger_evidence_batch_id"]), store_root=store) if row else None


def _load_latest_comparison_report(store: Path) -> dict[str, Any] | None:
    row = _latest_row(list_challenger_comparison_reports(store_root=store), "challenger_comparison_report_id")
    return load_challenger_comparison_report(str(row["challenger_comparison_report_id"]), store_root=store) if row else None


def _load_latest_human_review_dossier(store: Path) -> dict[str, Any] | None:
    row = _latest_row(list_human_review_dossiers(store_root=store), "human_review_dossier_id")
    return load_human_review_dossier(str(row["human_review_dossier_id"]), store_root=store) if row else None


def _load_latest_human_review_decision(store: Path) -> dict[str, Any] | None:
    row = _latest_row(list_human_review_decisions(store_root=store), "human_review_decision_id", "decided_at")
    return load_human_review_decision(str(row["human_review_decision_id"]), store_root=store) if row else None


def _all_findings(report: dict[str, Any], *, severity: str | None = None) -> list[dict[str, Any]]:
    fields = [
        "missing_references",
        "orphaned_artifacts",
        "hash_mismatches",
        "schema_validation_failures",
        "lineage_breaks",
        "duplicate_registry_entries",
        "mutation_boundary_violations",
        "unresolved_blockers",
        "warnings",
    ]
    rows: list[dict[str, Any]] = []
    for field in fields:
        for row in report.get(field) or []:
            if severity is None or row.get("severity") == severity:
                rows.append(dict(row) | {"finding_type": field})
    return rows


def _lineage_warning_count(report: dict[str, Any]) -> int:
    rows = list(report.get("warnings") or [])
    if not rows:
        rows = list(report.get("lineage_breaks") or []) + list(report.get("missing_references") or [])
    return sum(1 for row in rows if row.get("severity") == "WARNING" and ("lineage" in str(row.get("message") or "") or "referenced artifact missing" in str(row.get("message") or "")))


def _integrity_summary(report: dict[str, Any] | None) -> dict[str, Any]:
    if not report:
        return {
            "latest_integrity_report_id": "",
            "overall_status": "MISSING",
            "error_count": 1,
            "warning_count": 0,
            "hash_mismatch_count": 0,
            "schema_failure_count": 0,
            "missing_reference_count": 0,
            "lineage_break_count": 0,
            "duplicate_registry_entry_count": 0,
            "mutation_boundary_violation_count": 0,
            "known_blocker_count": 0,
            "lineage_warning_count": 0,
            "top_integrity_findings": [{"severity": "ERROR", "message": "integrity report missing"}],
        }
    error_findings = _all_findings(report, severity="ERROR")
    warning_findings = _all_findings(report, severity="WARNING")
    return {
        "latest_integrity_report_id": report.get("integrity_report_id"),
        "overall_status": report.get("overall_status"),
        "error_count": error_count(report),
        "warning_count": warning_count(report),
        "hash_mismatch_count": len(report.get("hash_mismatches") or []),
        "schema_failure_count": len(report.get("schema_validation_failures") or []),
        "missing_reference_count": len(report.get("missing_references") or []),
        "lineage_break_count": len(report.get("lineage_breaks") or []),
        "duplicate_registry_entry_count": len(report.get("duplicate_registry_entries") or []),
        "mutation_boundary_violation_count": len(report.get("mutation_boundary_violations") or []),
        "known_blocker_count": len(report.get("unresolved_blockers") or []),
        "lineage_warning_count": _lineage_warning_count(report),
        "top_integrity_findings": (error_findings + warning_findings)[:8],
    }


def _challenger_summary(batch: dict[str, Any] | None, comparison: dict[str, Any] | None) -> dict[str, Any]:
    items = batch.get("challenger_evidence_items") if batch else []
    generated = [item for item in items or [] if item.get("status") == "generated"]
    materialized = [item for item in generated if (item.get("evidence_completeness") or {}).get("complete") is True or item.get("materialization_method") == "fully_materialized_rule_delta_evidence_chain"]
    blocked = [item for item in items or [] if item.get("status") == "blocked"]
    active = [row for row in (comparison.get("comparison_table") if comparison else []) or [] if row.get("entity_type") == "challenger" and row.get("exclusion_status") == "included_for_human_review"]
    excluded = [row for row in (comparison.get("comparison_table") if comparison else []) or [] if row.get("entity_type") != "incumbent" and row.get("exclusion_status") == "excluded"]
    completeness = "complete" if generated and len(materialized) == len(generated) else ("partial" if materialized else "missing")
    return {
        "latest_challenger_evidence_batch_id": batch.get("challenger_evidence_batch_id") if batch else "",
        "latest_challenger_comparison_report_id": comparison.get("challenger_comparison_report_id") if comparison else "",
        "materialized_challenger_count": len(materialized),
        "blocked_challenger_count": len(blocked),
        "active_challenger_count": len(active),
        "excluded_challenger_count": len(excluded),
        "evidence_completeness_status": completeness,
        "top_review_candidate_id": ((comparison.get("deterministic_rank_order") or [{}])[0].get("challenger_hypothesis_id") if comparison else "") or "",
        "excluded_or_blocked_challengers": [
            {
                "challenger_hypothesis_id": row.get("challenger_hypothesis_id"),
                "variant_name": row.get("variant_name"),
                "status": row.get("status") or row.get("exclusion_status"),
                "reason": row.get("failure_reason") or row.get("exclusion_reason") or row.get("governance_notes"),
            }
            for row in blocked + excluded
        ],
        "comparison_ready": comparison.get("evidence_sufficiency") == "comparison_ready" if comparison else False,
    }


def _human_review_summary(dossier: dict[str, Any] | None, decision: dict[str, Any] | None, store: Path) -> dict[str, Any]:
    history = []
    if dossier:
        dossier_id = str(dossier.get("human_review_dossier_id") or "")
        history = [row for row in list_human_review_decisions(store_root=store) if row.get("human_review_dossier_id") == dossier_id]
    return {
        "latest_human_review_dossier_id": dossier.get("human_review_dossier_id") if dossier else "",
        "latest_human_review_decision_id": decision.get("human_review_decision_id") if decision else "",
        "latest_decision": decision.get("decision") if decision else "",
        "decision_challenger_id": decision.get("challenger_id") if decision else "",
        "decision_recorded_at": decision.get("decided_at") if decision else "",
        "allowed_next_decisions": ["request_more_challenger_evidence", "reject_challenger", "open_challenger_paper_trial"],
        "decision_history_count": len(history),
    }


def _paper_trial_summary(store: Path, integrity: dict[str, Any]) -> dict[str, Any]:
    rows = read_jsonl(store / "registries" / "paper_trials.jsonl")
    ids = [str(row.get("paper_trial_id") or "") for row in rows if row.get("paper_trial_id")]
    duplicates = [row for row in integrity.get("duplicate_registry_entries") or [] if row.get("registry_name") == "paper_trials.jsonl"]
    active = 0
    for paper_trial_id in sorted(set(ids)):
        path = store / "paper_trials" / paper_trial_id / "paper_trial.json"
        if path.exists():
            try:
                if read_json(path).get("status") == "active":
                    active += 1
            except Exception:
                pass
    blockers = [
        "Resolve duplicate paper-trial registry entry before opening proposal gates."
        for _ in duplicates
    ]
    return {
        "paper_trial_count": len(set(ids)),
        "active_paper_trial_count": active,
        "duplicate_registry_issue_count": len(duplicates),
        "paper_trial_integrity_status": "BLOCKED" if duplicates else "OK",
        "paper_trial_proposal_eligible": False if duplicates or error_count(integrity) else True,
        "paper_trial_proposal_blockers": blockers,
    }


def _candidate_evidence_summary(store: Path, integrity: dict[str, Any], batch: dict[str, Any] | None) -> dict[str, Any]:
    candidate_rows = read_jsonl(store / "registries" / "candidate_batches.jsonl")
    evidence_rows = read_jsonl(store / "registries" / "evidence_packages.jsonl")
    latest_candidate = _latest_row(candidate_rows, "candidate_batch_id", "created_at")
    legacy_warnings = [row for row in _all_findings(integrity, severity="WARNING") if "lineage" in str(row.get("message") or "") or "referenced artifact missing" in str(row.get("message") or "")]
    return {
        "candidate_generation_status": "available" if latest_candidate else "missing",
        "latest_candidate_batch_id": latest_candidate.get("candidate_batch_id") if latest_candidate else "",
        "evidence_package_count": len(evidence_rows),
        "evidence_materialization_status": "complete" if (batch and all((item.get("status") == "blocked") or (item.get("evidence_completeness") or {}).get("complete") is True for item in batch.get("challenger_evidence_items") or [])) else "partial_or_legacy",
        "missing_or_legacy_evidence_warnings": legacy_warnings[:8],
    }


def _stability_summary(store: Path) -> dict[str, Any]:
    drift = latest_expectancy_drift_report(store_root=store)
    fragility = latest_regime_fragility_report(store_root=store)
    stability = latest_sleeve_stability_report(store_root=store)
    status = str((stability or {}).get("overall_stability_status") or "UNKNOWN")
    blockers = []
    if not drift:
        blockers.append("latest expectancy drift report missing")
    if not fragility:
        blockers.append("latest regime fragility report missing")
    if not stability:
        blockers.append("latest sleeve stability report missing")
    return {
        "latest_expectancy_drift_report_id": (drift or {}).get("expectancy_drift_report_id", ""),
        "latest_regime_fragility_report_id": (fragility or {}).get("regime_fragility_report_id", ""),
        "latest_sleeve_stability_report_id": (stability or {}).get("sleeve_stability_report_id", ""),
        "stability_status": status,
        "stability_blockers": blockers,
    }


def _next_actions(integrity_summary: dict[str, Any], paper_summary: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    if integrity_summary["duplicate_registry_entry_count"]:
        actions.append("Resolve duplicate paper-trial registry entry manually or through a future governed repair packet.")
    if integrity_summary["lineage_warning_count"]:
        actions.append("Decide whether legacy pre-Packet-20 lineage warnings require migration or accepted-legacy classification.")
    if integrity_summary["overall_status"] == "FAIL":
        actions.append("Continue to Packet 24 only after acknowledging integrity RED/YELLOW status.")
    if not paper_summary["paper_trial_proposal_eligible"]:
        actions.append("Do not open paper-trial proposals until the integrity error is addressed or explicitly waived.")
    return actions or ["Continue research review under read-only governance."]


def _status_and_readiness(integrity_summary: dict[str, Any], current_blockers: list[dict[str, Any]]) -> tuple[str, str]:
    if integrity_summary["error_count"]:
        return "RED", "not_ready"
    if integrity_summary["warning_count"] or current_blockers:
        return "YELLOW", "research_ready_with_blockers"
    return "GREEN", "research_ready"


def build_research_os_status_report(
    *,
    store_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    generated_at = generated_at or utc_now_iso()
    integrity = latest_integrity_report(store_root=store) or {}
    integrity_summary = _integrity_summary(integrity if integrity else None)
    batch = _load_latest_challenger_evidence_batch(store)
    comparison = _load_latest_comparison_report(store)
    dossier = _load_latest_human_review_dossier(store)
    decision = _load_latest_human_review_decision(store)
    challenger_summary = _challenger_summary(batch, comparison)
    human_review = _human_review_summary(dossier, decision, store)
    paper_trials = _paper_trial_summary(store, integrity)
    candidate_evidence = _candidate_evidence_summary(store, integrity, batch)
    stability = _stability_summary(store)
    error_blockers = _all_findings(integrity, severity="ERROR")
    known_blockers = _all_findings(integrity, severity="INFO")
    current_blockers = error_blockers + known_blockers
    overall_status, readiness_level = _status_and_readiness(integrity_summary, current_blockers)
    actions = _next_actions(integrity_summary, paper_trials)
    source_artifact_ids = {
        "latest_integrity_report_id": integrity_summary["latest_integrity_report_id"],
        "latest_challenger_evidence_batch_id": challenger_summary["latest_challenger_evidence_batch_id"],
        "latest_challenger_comparison_report_id": challenger_summary["latest_challenger_comparison_report_id"],
        "latest_human_review_dossier_id": human_review["latest_human_review_dossier_id"],
        "latest_human_review_decision_id": human_review["latest_human_review_decision_id"],
        "latest_expectancy_drift_report_id": stability["latest_expectancy_drift_report_id"],
        "latest_regime_fragility_report_id": stability["latest_regime_fragility_report_id"],
        "latest_sleeve_stability_report_id": stability["latest_sleeve_stability_report_id"],
        "latest_candidate_batch_id": candidate_evidence["latest_candidate_batch_id"],
    }
    payload = {
        "research_os_status_report_id": "",
        "generated_at": generated_at,
        "overall_status": overall_status,
        "readiness_level": readiness_level,
        "latest_integrity_report_id": integrity_summary["latest_integrity_report_id"],
        "integrity_status": integrity_summary["overall_status"],
        "integrity_error_count": integrity_summary["error_count"],
        "integrity_warning_count": integrity_summary["warning_count"],
        "current_blockers": current_blockers,
        "latest_challenger_evidence_batch_id": challenger_summary["latest_challenger_evidence_batch_id"],
        "latest_challenger_comparison_report_id": challenger_summary["latest_challenger_comparison_report_id"],
        "latest_human_review_dossier_id": human_review["latest_human_review_dossier_id"],
        "latest_human_review_decision_id": human_review["latest_human_review_decision_id"],
        "active_challenger_count": challenger_summary["active_challenger_count"],
        "blocked_challenger_count": challenger_summary["blocked_challenger_count"],
        "materialized_challenger_count": challenger_summary["materialized_challenger_count"],
        "paper_trial_count": paper_trials["paper_trial_count"],
        "duplicate_registry_issue_count": paper_trials["duplicate_registry_issue_count"],
        "lineage_warning_count": integrity_summary["lineage_warning_count"],
        "candidate_generation_status": candidate_evidence["candidate_generation_status"],
        "evidence_materialization_status": candidate_evidence["evidence_materialization_status"],
        "human_review_status": "decision_recorded" if human_review["latest_human_review_decision_id"] else ("dossier_ready" if human_review["latest_human_review_dossier_id"] else "missing"),
        "paper_trial_status": paper_trials["paper_trial_integrity_status"],
        "stability_status": stability["stability_status"],
        "next_recommended_research_actions": actions,
        "integrity_summary": integrity_summary,
        "challenger_summary": challenger_summary,
        "human_review_summary": human_review,
        "paper_trial_summary": paper_trials,
        "candidate_evidence_summary": candidate_evidence,
        "stability_summary": stability,
        "source_artifact_ids": source_artifact_ids,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "governance_constraints": {
            "read_only": True,
            "automatic_repair_allowed": False,
            "paper_trial_created": False,
            "challenger_promoted": False,
            "capital_allocation_allowed": False,
            "broker_execution_allowed": False,
            "order_execution_allowed": False,
            "candidate_ledger_mutated": False,
            "evidence_mutated": False,
        },
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint_exclude = {"research_os_status_report_id", "generated_at", "immutable_hash", "content_hash"}
    fingerprint = content_hash(payload, exclude=fingerprint_exclude, sort_lists=True)
    payload["research_os_status_report_id"] = f"rsos_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("research_os_status_report", payload)
    return payload


def write_research_os_status_report(
    *,
    store_root: Path | None = None,
    actor: str = "Aegis",
    generated_at: str | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = build_research_os_status_report(store_root=store, generated_at=generated_at)
    path = _report_path(store, report["research_os_status_report_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable Research OS status report: {path}")
    write_json(path, report, overwrite=False)
    row = {
        "research_os_status_report_id": report["research_os_status_report_id"],
        "generated_at": report["generated_at"],
        "overall_status": report["overall_status"],
        "readiness_level": report["readiness_level"],
        "latest_integrity_report_id": report["latest_integrity_report_id"],
        "immutable_hash": report["immutable_hash"],
    }
    append_jsonl(_registry_path(store), row)
    if not read_jsonl(_registry_path(store)) or read_jsonl(_registry_path(store))[-1] != row:
        raise RuntimeError("Research OS status registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="research_os_status_report",
        entity_id=report["research_os_status_report_id"],
        action="research_os_status_report_generated",
        new_state_hash=report["immutable_hash"],
        reason="Generated read-only Research OS status projection.",
        metadata={
            "research_os_status_report_id": report["research_os_status_report_id"],
            "overall_status": report["overall_status"],
            "readiness_level": report["readiness_level"],
            "latest_integrity_report_id": report["latest_integrity_report_id"],
            "immutable_hash": report["immutable_hash"],
        },
        store_root=store,
    )
    return {"report": report, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_research_os_status_report(research_os_status_report_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_report_path(store, research_os_status_report_id))


def list_research_os_status_reports(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))


def latest_research_os_status_report(*, store_root: Path | None = None) -> dict[str, Any] | None:
    row = _latest_row(list_research_os_status_reports(store_root=store_root), "research_os_status_report_id")
    if not row:
        return None
    return load_research_os_status_report(str(row["research_os_status_report_id"]), store_root=store_root)


def report_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "research_os_status_report_id": report["research_os_status_report_id"],
        "overall_status": report["overall_status"],
        "readiness_level": report["readiness_level"],
        "latest_integrity_report_id": report["latest_integrity_report_id"],
        "integrity_status": report["integrity_status"],
        "current_blocker_count": len(report.get("current_blockers") or []),
        "duplicate_registry_issue_count": report.get("duplicate_registry_issue_count"),
        "lineage_warning_count": report.get("lineage_warning_count"),
        "next_recommended_research_actions": report.get("next_recommended_research_actions") or [],
    }
