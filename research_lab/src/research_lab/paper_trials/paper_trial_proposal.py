from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.challengers.challenger_comparison import load_challenger_comparison_report
from research_lab.challengers.human_review_decision import list_human_review_decisions, load_human_review_decision
from research_lab.challengers.human_review_dossier import load_human_review_dossier
from research_lab.contracts.schemas import validate_contract
from research_lab.integrity.research_store_integrity import latest_integrity_report
from research_lab.status.research_os_status import latest_research_os_status_report
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "paper_trial_proposal.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"
REQUIRED_EVIDENCE_FIELDS = [
    "challenger_variant_id",
    "event_study_evidence_id",
    "backtest_evidence_id",
    "longitudinal_run_id",
    "expectancy_drift_report_id",
    "regime_fragility_report_id",
    "sleeve_stability_report_id",
]


def _proposal_dir(store: Path) -> Path:
    return store / "paper_trial_proposals"


def _proposal_path(store: Path, proposal_id: str) -> Path:
    return _proposal_dir(store) / f"{proposal_id}.json"


def _registry_path(store: Path) -> Path:
    return store / "registries" / "paper_trial_proposal_registry.json"


def _latest_decision_id(store: Path) -> str:
    rows = list_human_review_decisions(store_root=store)
    return str(rows[-1].get("human_review_decision_id") or "") if rows else ""


def _blocker(code: str, message: str, *, severity: str = "ERROR", recoverable: bool = True, source_artifact_id: str = "") -> dict[str, Any]:
    return {
        "blocker_code": code,
        "blocker_message": message,
        "severity": severity,
        "recoverable": recoverable,
        "source_artifact_id": source_artifact_id,
    }


def _artifact_path(store: Path, field: str, artifact_id: str) -> Path:
    return {
        "challenger_variant_id": store / "challenger_variants" / f"{artifact_id}.json",
        "event_study_evidence_id": store / "event_studies" / f"{artifact_id}.json",
        "backtest_evidence_id": store / "backtests" / f"{artifact_id}.json",
        "longitudinal_run_id": store / "longitudinal_runs" / artifact_id / "longitudinal_run.json",
        "expectancy_drift_report_id": store / "stability_reports" / "expectancy_drift" / f"{artifact_id}.json",
        "regime_fragility_report_id": store / "stability_reports" / "regime_fragility" / f"{artifact_id}.json",
        "sleeve_stability_report_id": store / "stability_reports" / "sleeve_stability" / f"{artifact_id}.json",
    }[field]


def _stored_hash(path: Path) -> str:
    if not path.exists():
        return ""
    if path.suffix == ".json":
        payload = read_json(path)
        return str(payload.get("immutable_hash") or payload.get("content_hash") or content_hash(payload, exclude={"generated_at"}, sort_lists=True))
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _decision_challenger(dossier: dict[str, Any], challenger_id: str) -> dict[str, Any]:
    for row in (dossier.get("review_candidates") or []) + (dossier.get("blocked_or_excluded_items") or []):
        if row.get("challenger_hypothesis_id") == challenger_id:
            return row
    return {}


def _comparison_contains_materialized(comparison: dict[str, Any], challenger_id: str) -> bool:
    if not comparison:
        return False
    ids = {str(row.get("challenger_hypothesis_id") or "") for row in comparison.get("challenger_summaries") or []}
    rows = [row for row in comparison.get("challenger_summaries") or [] if row.get("challenger_hypothesis_id") == challenger_id]
    return challenger_id in ids and bool(rows) and bool((rows[0].get("evidence_completeness") or {}).get("complete"))


def build_paper_trial_proposal(
    *,
    human_review_decision_id: str | None = None,
    allow_red_status: bool = False,
    override_reason: str = "",
    generated_at: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    generated_at = generated_at or utc_now_iso()
    selected_decision_id = human_review_decision_id or _latest_decision_id(store)
    blockers: list[dict[str, Any]] = []
    decision: dict[str, Any] = {}
    dossier: dict[str, Any] = {}
    comparison: dict[str, Any] = {}
    source_artifact_ids: dict[str, Any] = {}
    source_artifact_hashes: dict[str, str] = {}

    if not selected_decision_id:
        blockers.append(_blocker("human_review_decision_missing", "No human review decision exists.", recoverable=True))
    else:
        try:
            decision = load_human_review_decision(selected_decision_id, store_root=store)
        except FileNotFoundError:
            blockers.append(_blocker("human_review_decision_missing", "Specified human review decision does not resolve.", source_artifact_id=selected_decision_id))

    if decision:
        if decision.get("decision") != "open_challenger_paper_trial":
            blockers.append(
                _blocker(
                    "decision_not_open_challenger_paper_trial",
                    f"Decision is {decision.get('decision')}; only open_challenger_paper_trial can pass the proposal gate.",
                    source_artifact_id=str(decision.get("human_review_decision_id") or ""),
                )
            )
        try:
            dossier = load_human_review_dossier(str(decision.get("human_review_dossier_id") or ""), store_root=store)
        except FileNotFoundError:
            blockers.append(_blocker("human_review_dossier_missing", "Decision dossier does not resolve.", source_artifact_id=str(decision.get("human_review_dossier_id") or "")))
        if dossier:
            expected_hash = content_hash(dossier, exclude={"generated_at"}, sort_lists=True)
            if expected_hash != decision.get("source_dossier_hash"):
                blockers.append(_blocker("source_dossier_hash_mismatch", "Decision source dossier hash does not match current dossier artifact.", source_artifact_id=str(dossier.get("human_review_dossier_id") or "")))
            if dossier.get("challenger_track_id") != decision.get("challenger_track_id"):
                blockers.append(_blocker("challenger_track_mismatch", "Decision challenger track does not match dossier.", source_artifact_id=str(dossier.get("human_review_dossier_id") or "")))
            candidate = _decision_challenger(dossier, str(decision.get("challenger_id") or ""))
            if not candidate:
                blockers.append(_blocker("challenger_id_missing_from_dossier", "Decision challenger id is not present in the dossier.", source_artifact_id=str(dossier.get("human_review_dossier_id") or "")))
            lineage = candidate.get("artifact_lineage") or (decision.get("source_artifact_ids") or {}).get("artifact_lineage") or {}
            for field in REQUIRED_EVIDENCE_FIELDS:
                artifact_id = str(lineage.get(field) or "")
                if not artifact_id:
                    blockers.append(_blocker(f"missing_{field}", f"Required evidence field {field} is missing.", source_artifact_id=str(decision.get("challenger_id") or "")))
                    continue
                path = _artifact_path(store, field, artifact_id)
                source_artifact_ids[field] = artifact_id
                source_artifact_hashes[field] = _stored_hash(path)
                if not path.exists():
                    blockers.append(_blocker(f"missing_{field}", f"Required evidence artifact {artifact_id} is missing.", source_artifact_id=artifact_id))
            comparison_id = str(dossier.get("challenger_comparison_report_id") or (dossier.get("source_report_ids") or {}).get("challenger_comparison_report_id") or "")
            if comparison_id:
                try:
                    comparison = load_challenger_comparison_report(comparison_id, store_root=store)
                except FileNotFoundError:
                    blockers.append(_blocker("challenger_comparison_missing", "Referenced challenger comparison report does not resolve.", source_artifact_id=comparison_id))
            else:
                blockers.append(_blocker("challenger_comparison_missing", "Dossier does not reference a challenger comparison report.", source_artifact_id=str(dossier.get("human_review_dossier_id") or "")))
            if comparison and not _comparison_contains_materialized(comparison, str(decision.get("challenger_id") or "")):
                blockers.append(_blocker("comparison_materialized_evidence_missing", "Challenger comparison does not show complete materialized evidence for this challenger.", source_artifact_id=str(comparison.get("challenger_comparison_report_id") or "")))

    integrity = latest_integrity_report(store_root=store) or {}
    status = latest_research_os_status_report(store_root=store) or {}
    if not integrity:
        blockers.append(_blocker("integrity_report_missing", "Latest Research Store integrity report is missing."))
    elif integrity.get("overall_status") == "FAIL":
        blockers.append(_blocker("integrity_status_fail", "Latest integrity report is FAIL.", source_artifact_id=str(integrity.get("integrity_report_id") or "")))
        for row in integrity.get("duplicate_registry_entries") or []:
            if row.get("registry_name") == "paper_trials.jsonl":
                blockers.append(_blocker("duplicate_paper_trial_registry_entry", "Paper-trial registry contains duplicate entries.", source_artifact_id=str(row.get("artifact_id") or "")))
    if not status:
        blockers.append(_blocker("research_os_status_missing", "Latest Research OS status report is missing."))
    elif status.get("overall_status") == "RED":
        blockers.append(_blocker("research_os_status_red", "Latest Research OS status is RED.", source_artifact_id=str(status.get("research_os_status_report_id") or "")))

    red_blockers = {"integrity_status_fail", "duplicate_paper_trial_registry_entry", "research_os_status_red"}
    has_red_blocker = any(row["blocker_code"] in red_blockers for row in blockers)
    if allow_red_status and has_red_blocker:
        if not override_reason.strip():
            blockers.append(_blocker("override_reason_missing", "allow-red-status requires an override reason.", recoverable=True))
        proposal_status = "needs_review"
    else:
        proposal_status = "blocked" if blockers else "eligible"
    if blockers and proposal_status != "needs_review":
        proposal_status = "blocked"
    eligibility_status = "passed" if proposal_status == "eligible" else ("override_needs_review" if proposal_status == "needs_review" else "blocked")
    eligibility_checks = {
        "human_decision_open_challenger_paper_trial": decision.get("decision") == "open_challenger_paper_trial",
        "dossier_hash_matches": bool(decision and dossier and content_hash(dossier, exclude={"generated_at"}, sort_lists=True) == decision.get("source_dossier_hash")),
        "required_evidence_complete": all(source_artifact_ids.get(field) and source_artifact_hashes.get(field) for field in REQUIRED_EVIDENCE_FIELDS),
        "comparison_materialized": bool(comparison and _comparison_contains_materialized(comparison, str(decision.get("challenger_id") or ""))),
        "integrity_not_fail": bool(integrity and integrity.get("overall_status") != "FAIL"),
        "research_os_not_red": bool(status and status.get("overall_status") != "RED"),
        "allow_red_status": allow_red_status,
        "override_reason": override_reason.strip(),
    }
    payload = {
        "paper_trial_proposal_id": "",
        "generated_at": generated_at,
        "proposal_status": proposal_status,
        "human_review_decision_id": str(decision.get("human_review_decision_id") or selected_decision_id),
        "human_review_dossier_id": str(decision.get("human_review_dossier_id") or ""),
        "challenger_track_id": str(decision.get("challenger_track_id") or ""),
        "challenger_id": str(decision.get("challenger_id") or ""),
        "incumbent_sleeve_id": str(decision.get("incumbent_sleeve_id") or ""),
        "proposed_paper_trial_type": "challenger_paper_trial",
        "proposal_reason": "Record governed human intent to open a challenger paper trial; no paper trial is created.",
        "eligibility_status": eligibility_status,
        "eligibility_checks": eligibility_checks,
        "blockers": blockers,
        "source_artifact_ids": source_artifact_ids,
        "source_artifact_hashes": source_artifact_hashes,
        "latest_integrity_report_id": str(integrity.get("integrity_report_id") or ""),
        "latest_research_os_status_report_id": str(status.get("research_os_status_report_id") or ""),
        "latest_challenger_evidence_batch_id": str((comparison.get("challenger_evidence_batch_id") if comparison else "") or ""),
        "latest_challenger_comparison_report_id": str((comparison.get("challenger_comparison_report_id") if comparison else "") or ""),
        "latest_human_review_dossier_id": str(dossier.get("human_review_dossier_id") or ""),
        "latest_human_review_decision_id": str(decision.get("human_review_decision_id") or ""),
        "no_lifecycle_mutation_assertion": {
            "paper_trial_created": False,
            "sleeve_created": False,
            "candidate_created": False,
            "trade_created": False,
            "capital_allocation_created": False,
            "challenger_promoted": False,
            "incumbent_mutated": False,
            "challenger_lifecycle_mutated": False,
            "candidate_ledger_mutated": False,
            "evidence_mutated": False,
            "integrity_repaired": False,
        },
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint = content_hash(payload, exclude={"paper_trial_proposal_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["paper_trial_proposal_id"] = f"ptp_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("paper_trial_proposal", payload)
    return payload


def write_paper_trial_proposal(
    *,
    human_review_decision_id: str | None = None,
    allow_red_status: bool = False,
    override_reason: str = "",
    generated_at: str | None = None,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    payload = build_paper_trial_proposal(
        human_review_decision_id=human_review_decision_id,
        allow_red_status=allow_red_status,
        override_reason=override_reason,
        generated_at=generated_at,
        store_root=store,
    )
    path = _proposal_path(store, payload["paper_trial_proposal_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable paper-trial proposal: {path}")
    write_json(path, payload, overwrite=False)
    row = {
        "paper_trial_proposal_id": payload["paper_trial_proposal_id"],
        "generated_at": payload["generated_at"],
        "proposal_status": payload["proposal_status"],
        "human_review_decision_id": payload["human_review_decision_id"],
        "human_review_dossier_id": payload["human_review_dossier_id"],
        "challenger_id": payload["challenger_id"],
        "challenger_track_id": payload["challenger_track_id"],
        "incumbent_sleeve_id": payload["incumbent_sleeve_id"],
        "immutable_hash": payload["immutable_hash"],
    }
    append_jsonl(_registry_path(store), row)
    if not read_jsonl(_registry_path(store)) or read_jsonl(_registry_path(store))[-1] != row:
        raise RuntimeError("paper-trial proposal registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="paper_trial_proposal",
        entity_id=payload["paper_trial_proposal_id"],
        action="paper_trial_proposal_generated",
        new_state_hash=payload["immutable_hash"],
        reason="Generated research-only paper-trial proposal gate artifact; no paper trial was created.",
        metadata={
            "paper_trial_proposal_id": payload["paper_trial_proposal_id"],
            "proposal_status": payload["proposal_status"],
            "human_review_decision_id": payload["human_review_decision_id"],
            "challenger_id": payload["challenger_id"],
            "latest_integrity_report_id": payload["latest_integrity_report_id"],
            "latest_research_os_status_report_id": payload["latest_research_os_status_report_id"],
            "immutable_hash": payload["immutable_hash"],
        },
        store_root=store,
    )
    return {"proposal": payload, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_paper_trial_proposal(paper_trial_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_proposal_path(store, paper_trial_proposal_id))


def list_paper_trial_proposals(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))


def latest_paper_trial_proposal(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_paper_trial_proposals(store_root=store_root)
    if not rows:
        return None
    return load_paper_trial_proposal(str(rows[-1]["paper_trial_proposal_id"]), store_root=store_root)


def paper_trial_proposals_for_decision(human_review_decision_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    return [row for row in list_paper_trial_proposals(store_root=store_root) if row.get("human_review_decision_id") == human_review_decision_id]

