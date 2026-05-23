from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_proposal import load_hypothesis_proposal
from research_lab.hypotheses.hypothesis_proposal_review import load_hypothesis_proposal_review
from research_lab.hypotheses.research_hypothesis import build_research_hypothesis
from research_lab.integrity.research_store_integrity import error_count, latest_integrity_report
from research_lab.observations.observation_candidate import load_observation_candidate
from research_lab.observations.observation_cluster import load_observation_cluster
from research_lab.status.research_os_status import latest_research_os_status_report
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "hypothesis_intake_decision.v1"
INTAKE_VERSION = "hypothesis_intake_gate.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"
ACTIONABILITY_STATUS = "non_actionable_research_only"

APPROVED_DECISION = "approve_for_future_hypothesis_activation"


def hypothesis_intake_decision_dir(store: Path) -> Path:
    return store / "hypothesis_intake_decisions"


def hypothesis_intake_decision_path(store: Path, hypothesis_intake_decision_id: str) -> Path:
    return hypothesis_intake_decision_dir(store) / f"{hypothesis_intake_decision_id}.json"


def hypothesis_intake_decision_registry_path(store: Path) -> Path:
    return store / "registries" / "hypothesis_intake_decision_registry.json"


def _blocker(code: str, message: str, *, severity: str = "ERROR", recoverable: bool = True, source_artifact_id: str = "") -> dict[str, Any]:
    return {
        "blocker_code": code,
        "blocker_message": message,
        "severity": severity,
        "recoverable": recoverable,
        "source_artifact_id": source_artifact_id,
    }


def _resolve_current_reports(store: Path) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    try:
        integrity = latest_integrity_report(store_root=store)
    except Exception:
        integrity = None
    try:
        status = latest_research_os_status_report(store_root=store)
    except Exception:
        status = None
    return integrity, status


def evaluate_intake_eligibility(
    *,
    store_root: Path | None,
    review: dict[str, Any],
    proposal: dict[str, Any],
    allow_red_status: bool = False,
    allow_unapproved_review: bool = False,
    override_reason: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], str, str]:
    store = ensure_store_layout(store_root)
    blockers: list[dict[str, Any]] = []
    checks: dict[str, Any] = {
        "review_exists": bool(review),
        "proposal_exists": bool(proposal),
        "review_decision": review.get("review_decision"),
        "proposal_status": proposal.get("proposal_status"),
        "allow_red_status": allow_red_status,
        "allow_unapproved_review": allow_unapproved_review,
        "override_reason": override_reason or "",
    }

    proposal_hash = str(proposal.get("immutable_hash") or proposal.get("content_hash") or content_hash(proposal, sort_lists=True))
    review_hash = str(review.get("source_proposal_hash") or "")
    checks["source_proposal_hash_matches"] = bool(proposal_hash and review_hash and proposal_hash == review_hash)
    if not checks["source_proposal_hash_matches"]:
        blockers.append(_blocker("source_proposal_hash_mismatch", "The review source proposal hash does not match the current proposal artifact.", source_artifact_id=str(review.get("hypothesis_proposal_review_id") or "")))

    approved = review.get("review_decision") == APPROVED_DECISION
    checks["approved_for_future_hypothesis_activation"] = approved
    if not approved:
        severity = "WARNING" if allow_unapproved_review else "ERROR"
        blockers.append(_blocker("review_not_approved_for_future_hypothesis_activation", "Source review is not approved for future hypothesis activation.", severity=severity, source_artifact_id=str(review.get("hypothesis_proposal_review_id") or "")))

    checks["proposal_status_proposed_for_review"] = proposal.get("proposal_status") == "proposed_for_review"
    if not checks["proposal_status_proposed_for_review"]:
        blockers.append(_blocker("proposal_not_proposed_for_review", "Source proposal is not in proposed_for_review status.", source_artifact_id=str(proposal.get("hypothesis_proposal_id") or "")))

    checks["proposal_actionability_research_only"] = proposal.get("actionability_status") == ACTIONABILITY_STATUS
    if not checks["proposal_actionability_research_only"]:
        blockers.append(_blocker("proposal_actionability_not_research_only", "Source proposal actionability status is not non_actionable_research_only.", source_artifact_id=str(proposal.get("hypothesis_proposal_id") or "")))

    cluster_id = str(proposal.get("source_observation_cluster_id") or review.get("source_observation_cluster_id") or "")
    checks["source_observation_cluster_id"] = cluster_id
    try:
        load_observation_cluster(cluster_id, store_root=store)
        checks["source_observation_cluster_resolves"] = True
    except Exception:
        checks["source_observation_cluster_resolves"] = False
        blockers.append(_blocker("source_observation_cluster_missing", "Source observation cluster does not resolve.", source_artifact_id=cluster_id))

    candidate_ids = list(proposal.get("source_observation_candidate_ids") or review.get("source_observation_candidate_ids") or [])
    missing_candidates = []
    for candidate_id in candidate_ids:
        try:
            load_observation_candidate(str(candidate_id), store_root=store)
        except Exception:
            missing_candidates.append(str(candidate_id))
    checks["source_observation_candidate_ids"] = candidate_ids
    checks["source_observation_candidates_resolve"] = not missing_candidates and bool(candidate_ids)
    if missing_candidates:
        blockers.append(_blocker("source_observation_candidate_missing", "One or more source observation candidates do not resolve.", source_artifact_id=",".join(missing_candidates)))
    elif not candidate_ids:
        blockers.append(_blocker("source_observation_candidates_missing", "Source proposal has no observation candidate references.", source_artifact_id=str(proposal.get("hypothesis_proposal_id") or "")))

    test_design = proposal.get("proposed_test_design")
    checks["proposed_test_design_present"] = isinstance(test_design, dict) and bool(test_design)
    if not checks["proposed_test_design_present"]:
        blockers.append(_blocker("proposed_test_design_missing", "Source proposal does not include a proposed test design.", source_artifact_id=str(proposal.get("hypothesis_proposal_id") or "")))

    integrity, status = _resolve_current_reports(store)
    integrity_id = str((integrity or {}).get("integrity_report_id") or proposal.get("latest_integrity_report_id") or review.get("latest_integrity_report_id") or "")
    status_id = str((status or {}).get("research_os_status_report_id") or proposal.get("latest_research_os_status_report_id") or review.get("latest_research_os_status_report_id") or "")
    integrity_status = str((integrity or {}).get("overall_status") or "")
    research_os_status = str((status or {}).get("overall_status") or "")
    checks["latest_integrity_report_id"] = integrity_id
    checks["latest_research_os_status_report_id"] = status_id
    checks["integrity_status"] = integrity_status or "MISSING"
    checks["research_os_status"] = research_os_status or "MISSING"

    if not integrity:
        blockers.append(_blocker("integrity_report_missing", "Latest integrity report is missing.", source_artifact_id=integrity_id))
    elif integrity_status == "FAIL" or error_count(integrity):
        blockers.append(_blocker("integrity_status_fail", "Latest integrity report is FAIL.", source_artifact_id=integrity_id))

    if not status:
        blockers.append(_blocker("research_os_status_missing", "Latest Research OS status report is missing.", source_artifact_id=status_id))
    elif research_os_status == "RED":
        blockers.append(_blocker("research_os_status_red", "Latest Research OS status is RED.", source_artifact_id=status_id))

    blockers_from_override = [row for row in blockers if row["blocker_code"] in {"integrity_status_fail", "research_os_status_red"}]
    accepted = approved and proposal.get("proposal_status") == "proposed_for_review" and checks["source_proposal_hash_matches"] and checks["proposal_actionability_research_only"] and checks["source_observation_cluster_resolves"] and checks["source_observation_candidates_resolve"] and checks["proposed_test_design_present"]
    if blockers_from_override and allow_red_status:
        accepted = False
    else:
        accepted = accepted and not blockers

    if accepted:
        return checks, blockers, "accepted_inactive_research_only", "create_inactive_research_hypothesis"
    if review.get("review_decision") in {"defer_until_more_observation", "mark_system_research_item", "request_data_enablement"} or allow_red_status or allow_unapproved_review:
        return checks, blockers, "deferred", "defer_intake"
    return checks, blockers, "blocked", "block_intake"


def build_hypothesis_intake_decision(
    *,
    review: dict[str, Any],
    proposal: dict[str, Any],
    generated_at: str,
    store_root: Path | None = None,
    allow_red_status: bool = False,
    allow_unapproved_review: bool = False,
    override_reason: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    checks, blockers, intake_status, intake_decision = evaluate_intake_eligibility(
        store_root=store_root,
        review=review,
        proposal=proposal,
        allow_red_status=allow_red_status,
        allow_unapproved_review=allow_unapproved_review,
        override_reason=override_reason,
    )
    research_hypothesis = None
    if intake_status == "accepted_inactive_research_only":
        research_hypothesis = build_research_hypothesis(
            review=review,
            proposal=proposal,
            generated_at=generated_at,
            blockers=[],
            latest_integrity_report_id=str(checks.get("latest_integrity_report_id") or ""),
            latest_research_os_status_report_id=str(checks.get("latest_research_os_status_report_id") or ""),
        )
    payload = {
        "hypothesis_intake_decision_id": "",
        "generated_at": generated_at,
        "intake_version": INTAKE_VERSION,
        "source_hypothesis_proposal_review_id": str(review.get("hypothesis_proposal_review_id") or ""),
        "source_hypothesis_proposal_id": str(proposal.get("hypothesis_proposal_id") or review.get("hypothesis_proposal_id") or ""),
        "source_hypothesis_proposal_batch_id": str(review.get("hypothesis_proposal_batch_id") or (proposal.get("source_artifact_ids") or {}).get("hypothesis_proposal_batch_id") or ""),
        "source_observation_cluster_id": str(proposal.get("source_observation_cluster_id") or review.get("source_observation_cluster_id") or ""),
        "intake_status": intake_status,
        "intake_decision": intake_decision,
        "intake_rationale": _intake_rationale(review, proposal, blockers, intake_status),
        "eligibility_checks": checks,
        "blockers": blockers,
        "created_research_hypothesis_id": (research_hypothesis or {}).get("research_hypothesis_id"),
        "latest_integrity_report_id": str(checks.get("latest_integrity_report_id") or ""),
        "latest_research_os_status_report_id": str(checks.get("latest_research_os_status_report_id") or ""),
        "non_actionable_research_only_assertion": {
            "research_only": True,
            "intake_gate_only": True,
            "active_hypothesis_created": False,
            "market_action_authorized": False,
            "buy_sell_hold_recommendations_allowed": False,
        },
        "no_lifecycle_mutation_assertion": {
            "active_hypothesis_created": False,
            "challenger_created": False,
            "sleeve_created": False,
            "paper_trial_created": False,
            "paper_trial_proposal_created": False,
            "candidate_ledger_mutated": False,
            "evidence_mutated": False,
            "integrity_repaired": False,
            "trade_order_created": False,
            "capital_allocation_created": False,
        },
        "actionability_status": ACTIONABILITY_STATUS,
        "source_artifact_ids": {
            "hypothesis_proposal_review_id": review.get("hypothesis_proposal_review_id"),
            "hypothesis_proposal_id": proposal.get("hypothesis_proposal_id") or review.get("hypothesis_proposal_id"),
            "hypothesis_proposal_batch_id": review.get("hypothesis_proposal_batch_id"),
            "source_observation_cluster_id": proposal.get("source_observation_cluster_id") or review.get("source_observation_cluster_id"),
            "source_observation_candidate_ids": list(proposal.get("source_observation_candidate_ids") or review.get("source_observation_candidate_ids") or []),
            "created_research_hypothesis_id": (research_hypothesis or {}).get("research_hypothesis_id"),
            "latest_integrity_report_id": checks.get("latest_integrity_report_id"),
            "latest_research_os_status_report_id": checks.get("latest_research_os_status_report_id"),
        },
        "source_artifact_hashes": {
            "source_proposal_hash": proposal.get("immutable_hash") or proposal.get("content_hash"),
            "source_review_hash": review.get("immutable_hash") or review.get("content_hash"),
            **(proposal.get("source_artifact_hashes") or {}),
        },
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint = content_hash(payload, exclude={"hypothesis_intake_decision_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["hypothesis_intake_decision_id"] = f"hint_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("hypothesis_intake_decision", payload)
    return payload, research_hypothesis


def _intake_rationale(review: dict[str, Any], proposal: dict[str, Any], blockers: list[dict[str, Any]], intake_status: str) -> str:
    if intake_status == "accepted_inactive_research_only":
        return "Review and source proposal satisfy the inactive research hypothesis intake gate."
    if review.get("review_decision") != APPROVED_DECISION:
        return f"Source review decision is {review.get('review_decision')}; Packet 29 cannot create an inactive research hypothesis."
    if proposal.get("proposal_status") != "proposed_for_review":
        return f"Source proposal status is {proposal.get('proposal_status')}; Packet 29 requires proposed_for_review."
    codes = ", ".join(str(row.get("blocker_code")) for row in blockers) or "eligibility_blocked"
    return f"Inactive research hypothesis intake is blocked by: {codes}."


def build_hypothesis_intake_for_review_id(
    *,
    hypothesis_proposal_review_id: str,
    generated_at: str,
    store_root: Path | None = None,
    allow_red_status: bool = False,
    allow_unapproved_review: bool = False,
    override_reason: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    store = ensure_store_layout(store_root)
    review = load_hypothesis_proposal_review(hypothesis_proposal_review_id, store_root=store)
    proposal = load_hypothesis_proposal(str(review.get("hypothesis_proposal_id") or ""), store_root=store)
    return build_hypothesis_intake_decision(
        review=review,
        proposal=proposal,
        generated_at=generated_at,
        store_root=store,
        allow_red_status=allow_red_status,
        allow_unapproved_review=allow_unapproved_review,
        override_reason=override_reason,
    )


def load_hypothesis_intake_decision(hypothesis_intake_decision_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(hypothesis_intake_decision_path(store, hypothesis_intake_decision_id))


def list_hypothesis_intake_decisions(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(hypothesis_intake_decision_registry_path(store))


def latest_hypothesis_intake_decision(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_hypothesis_intake_decisions(store_root=store_root)
    if not rows:
        return None
    return load_hypothesis_intake_decision(str(rows[-1]["hypothesis_intake_decision_id"]), store_root=store_root)
