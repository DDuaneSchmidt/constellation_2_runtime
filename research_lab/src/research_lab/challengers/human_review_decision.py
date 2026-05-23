from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.challengers.human_review_dossier import load_human_review_dossier
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


GENERATED_AT = "1970-01-01T00:00:00Z"
RESEARCH_LABEL = "RESEARCH_ONLY"
SCHEMA_VERSION = "human_review_decision.v1"

ALLOWED_DECISIONS = {
    "request_more_challenger_evidence",
    "reject_challenger",
    "open_challenger_paper_trial",
}

REJECTED_DECISIONS = {
    "promote_challenger",
    "retire_incumbent",
    "allocate_capital",
    "execute_trade",
    "create_live_sleeve",
    "mutate_paper_trial",
    "mutate_sleeve",
}


def _decision_dir(store: Path) -> Path:
    return store / "human_review_decisions"


def _decision_path(store: Path, decision_id: str) -> Path:
    return _decision_dir(store) / f"{decision_id}.json"


def _registry_path(store: Path) -> Path:
    return store / "registries" / "human_review_decision_registry.json"


def _verify_dossier_hash(dossier: dict[str, Any]) -> str:
    expected = str(dossier.get("content_hash") or "")
    actual = content_hash(dossier, exclude={"generated_at"}, sort_lists=True)
    if expected != actual:
        raise RuntimeError(f"source dossier hash mismatch: expected {expected} actual {actual}")
    return expected


def _candidate_by_id(dossier: dict[str, Any], challenger_id: str | None) -> dict[str, Any]:
    candidates = dossier.get("review_candidates") or []
    excluded = dossier.get("blocked_or_excluded_items") or []
    wanted = challenger_id or str((dossier.get("executive_summary") or {}).get("top_research_review_candidate_id") or "")
    for candidate in candidates:
        if str(candidate.get("challenger_hypothesis_id") or "") == wanted:
            row = dict(candidate)
            row["_review_source"] = "active_review_candidate"
            return row
    for candidate in excluded:
        if str(candidate.get("challenger_hypothesis_id") or "") == wanted:
            row = dict(candidate)
            row["_review_source"] = "blocked_or_excluded_item"
            return row
    raise RuntimeError(f"challenger review candidate missing: {wanted}")


def _source_artifacts_resolve(store: Path, source_artifact_ids: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    lineage = source_artifact_ids.get("artifact_lineage") or {}
    mappings = {
        "challenger_variant_id": lambda value: store / "challenger_variants" / f"{value}.json",
        "event_study_evidence_id": lambda value: store / "event_studies" / f"{value}.json",
        "backtest_evidence_id": lambda value: store / "backtests" / f"{value}.json",
        "longitudinal_run_id": lambda value: store / "longitudinal_runs" / str(value) / "outcome_index.parquet",
        "expectancy_drift_report_id": lambda value: store / "stability_reports" / "expectancy_drift" / f"{value}.json",
        "regime_fragility_report_id": lambda value: store / "stability_reports" / "regime_fragility" / f"{value}.json",
        "sleeve_stability_report_id": lambda value: store / "stability_reports" / "sleeve_stability" / f"{value}.json",
    }
    for key, factory in mappings.items():
        value = str(lineage.get(key) or "")
        if not value:
            refs.append({"field": key, "artifact_id": "", "status": "missing", "path": ""})
            continue
        path = factory(value)
        refs.append({"field": key, "artifact_id": value, "status": "available" if path.exists() else "missing", "path": str(path)})
    return refs


def _decision_readiness(candidate: dict[str, Any]) -> tuple[str, str]:
    completeness = candidate.get("evidence_completeness") or {}
    lineage = candidate.get("artifact_lineage") or {}
    required_lineage = [
        "challenger_variant_id",
        "event_study_evidence_id",
        "backtest_evidence_id",
        "longitudinal_run_id",
        "expectancy_drift_report_id",
        "regime_fragility_report_id",
        "sleeve_stability_report_id",
    ]
    status = "complete" if completeness.get("complete") is True or all(lineage.get(field) for field in required_lineage) else "incomplete"
    confidence = str(candidate.get("confidence_classification") or ("excluded_research_candidate" if candidate.get("_review_source") == "blocked_or_excluded_item" else "insufficient"))
    return status, confidence


def _validate_decision_inputs(*, decision: str, rationale: str, decided_by: str) -> None:
    if decision in REJECTED_DECISIONS or decision not in ALLOWED_DECISIONS:
        raise ValueError(f"decision not allowed for research-only ledger: {decision}")
    if not rationale.strip():
        raise ValueError("rationale is required")
    if not decided_by.strip():
        raise ValueError("decided_by is required")


def build_human_review_decision(
    *,
    human_review_dossier_id: str,
    decision: str,
    rationale: str,
    decided_by: str,
    challenger_id: str | None = None,
    decided_at: str = GENERATED_AT,
    created_at: str = GENERATED_AT,
    store_root: Path | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    decision = decision.strip()
    rationale = rationale.strip()
    decided_by = decided_by.strip()
    _validate_decision_inputs(decision=decision, rationale=rationale, decided_by=decided_by)
    dossier = load_human_review_dossier(human_review_dossier_id, store_root=store)
    source_dossier_hash = _verify_dossier_hash(dossier)
    candidate = _candidate_by_id(dossier, challenger_id)
    resolved_challenger_id = str(candidate.get("challenger_hypothesis_id") or challenger_id or "")
    evidence_completeness_status, confidence_classification = _decision_readiness(candidate)
    source_artifact_ids = {
        "source_report_ids": dossier.get("source_report_ids") or {},
        "artifact_lineage": candidate.get("artifact_lineage") or {},
        "supporting_evidence_ids": candidate.get("supporting_evidence_ids") or [],
    }
    refs = _source_artifacts_resolve(store, source_artifact_ids)
    missing_refs = [ref for ref in refs if ref["status"] != "available"]
    if missing_refs:
        raise RuntimeError(f"source artifact reference missing: {missing_refs[0]['field']}")
    if decision == "open_challenger_paper_trial":
        if candidate.get("_review_source") != "active_review_candidate":
            raise RuntimeError("open_challenger_paper_trial requires an active review candidate")
        if evidence_completeness_status != "complete":
            raise RuntimeError("open_challenger_paper_trial requires complete evidence")
        if confidence_classification == "insufficient":
            raise RuntimeError("open_challenger_paper_trial requires non-insufficient confidence")
    if decision == "request_more_challenger_evidence" and len(rationale) < 12:
        raise ValueError("request_more_challenger_evidence rationale must explain missing evidence or concern")
    payload = {
        "human_review_decision_id": "",
        "human_review_dossier_id": human_review_dossier_id,
        "challenger_track_id": str(dossier.get("challenger_track_id") or ""),
        "challenger_id": resolved_challenger_id,
        "incumbent_sleeve_id": str(dossier.get("incumbent_sleeve_id") or ""),
        "decision": decision,
        "rationale": rationale,
        "decided_by": decided_by,
        "decided_at": decided_at,
        "source_dossier_hash": source_dossier_hash,
        "source_artifact_ids": source_artifact_ids,
        "evidence_completeness_status": evidence_completeness_status,
        "confidence_classification": confidence_classification,
        "created_at": created_at,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "governance_constraints": {
            "research_only": True,
            "paper_trial_created": False,
            "sleeve_mutated": False,
            "challenger_lifecycle_mutated": False,
            "candidate_ledger_mutated": False,
            "capital_allocation_allowed": False,
            "broker_execution_allowed": False,
            "order_execution_allowed": False,
            "automatic_promotion_allowed": False,
        },
        "immutable_hash": "",
        "content_hash": "",
    }
    seed = content_hash(payload, exclude={"human_review_decision_id", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["human_review_decision_id"] = f"hrdec_{short_hash(seed, 16)}"
    payload["immutable_hash"] = content_hash(payload, exclude={"immutable_hash", "content_hash"}, sort_lists=True)
    payload["content_hash"] = payload["immutable_hash"]
    validate_contract("human_review_decision", payload)
    return payload


def write_human_review_decision(
    *,
    human_review_dossier_id: str,
    decision: str,
    rationale: str,
    decided_by: str,
    challenger_id: str | None = None,
    decided_at: str = GENERATED_AT,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    payload = build_human_review_decision(
        human_review_dossier_id=human_review_dossier_id,
        decision=decision,
        rationale=rationale,
        decided_by=decided_by,
        challenger_id=challenger_id,
        decided_at=decided_at,
        store_root=store,
    )
    path = _decision_path(store, payload["human_review_decision_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable human review decision: {path}")
    write_json(path, payload, overwrite=False)
    row = {
        "human_review_decision_id": payload["human_review_decision_id"],
        "human_review_dossier_id": payload["human_review_dossier_id"],
        "challenger_track_id": payload["challenger_track_id"],
        "challenger_id": payload["challenger_id"],
        "decision": payload["decision"],
        "decided_at": payload["decided_at"],
        "immutable_hash": payload["immutable_hash"],
        "source_dossier_hash": payload["source_dossier_hash"],
    }
    append_jsonl(_registry_path(store), row)
    if not read_jsonl(_registry_path(store)) or read_jsonl(_registry_path(store))[-1] != row:
        raise RuntimeError("human review decision registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="human_review_decision",
        entity_id=payload["human_review_decision_id"],
        action="human_review_decision_recorded",
        new_state_hash=payload["immutable_hash"],
        reason="Recorded append-only research-only human review dossier decision.",
        metadata={
            "human_review_decision_id": payload["human_review_decision_id"],
            "human_review_dossier_id": payload["human_review_dossier_id"],
            "challenger_track_id": payload["challenger_track_id"],
            "challenger_id": payload["challenger_id"],
            "decision": payload["decision"],
            "source_dossier_hash": payload["source_dossier_hash"],
            "immutable_hash": payload["immutable_hash"],
            "registry_row": row,
            "governance_constraints": payload["governance_constraints"],
        },
        store_root=store,
    )
    return {"decision": payload, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_human_review_decision(human_review_decision_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_decision_path(store, human_review_decision_id))


def list_human_review_decisions(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))


def decisions_for_dossier(human_review_dossier_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    rows = list_human_review_decisions(store_root=store_root)
    return [row for row in rows if row.get("human_review_dossier_id") == human_review_dossier_id]


def latest_decision_for_dossier(human_review_dossier_id: str, *, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = decisions_for_dossier(human_review_dossier_id, store_root=store_root)
    if not rows:
        return None
    return sorted(rows, key=lambda row: (str(row.get("decided_at") or ""), str(row.get("human_review_decision_id") or "")))[-1]


def human_review_decision_read_model(*, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    dossiers = read_jsonl(store / "registries" / "human_review_dossiers.jsonl")
    decisions = list_human_review_decisions(store_root=store)
    decisions_by_dossier: dict[str, list[dict[str, Any]]] = {}
    for row in decisions:
        decisions_by_dossier.setdefault(str(row.get("human_review_dossier_id") or ""), []).append(row)
    items = []
    for row in dossiers:
        dossier_id = str(row.get("human_review_dossier_id") or "")
        try:
            dossier = load_human_review_dossier(dossier_id, store_root=store)
        except FileNotFoundError:
            continue
        candidate = (dossier.get("review_candidates") or dossier.get("blocked_or_excluded_items") or [{}])[0]
        history = sorted(decisions_by_dossier.get(dossier_id, []), key=lambda item: (str(item.get("decided_at") or ""), str(item.get("human_review_decision_id") or "")))
        latest = history[-1] if history else None
        completeness = candidate.get("evidence_completeness") or {}
        blockers = dossier.get("blocked_or_excluded_items") or []
        next_action = "paper_trial_proposal_eligible" if completeness.get("complete") is True and latest and latest.get("decision") == "open_challenger_paper_trial" else dossier.get("recommended_next_action")
        items.append(
            {
                "human_review_dossier_id": dossier_id,
                "challenger_id": candidate.get("challenger_hypothesis_id"),
                "evidence_completeness": completeness,
                "allowed_decisions": sorted(ALLOWED_DECISIONS),
                "latest_recorded_decision": latest,
                "decision_history_count": len(history),
                "blockers": blockers,
                "next_allowed_research_action": next_action,
                "paper_trial_created": False,
            }
        )
    return {"ok": True, "read_only": True, "dossiers": items, "count": len(items)}
