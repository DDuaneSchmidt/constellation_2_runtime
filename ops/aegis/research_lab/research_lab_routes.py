from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.research_lab.research_console_v1 import (
    assess_hypothesis_v1 as research_console_assess_hypothesis_v1,
    blocked_evidence_v1 as research_console_blocked_evidence_v1,
    convert_hypothesis_v1 as research_console_convert_hypothesis_v1,
    evidence_v1 as research_console_evidence_v1,
    explicit_research_action_placeholder_v1,
    paper_trials_v1 as research_console_paper_trials_v1,
    research_backlog_v1 as research_console_backlog_v1,
    research_console_v1,
    research_hypothesis_queue_v1 as research_console_hypothesis_queue_v1,
    research_intake_dossier_v1 as research_console_dossier_v1,
    research_plans_v1 as research_console_research_plans_v1,
    research_action_failure_response as research_lab_console_action_failure_response_v1,
    review_hypothesis_v1 as research_console_review_hypothesis_v1,
    sleeve_review_center_v1 as research_console_sleeve_review_center_v1,
    start_research_options_v1 as research_console_start_research_options_v1,
    start_research_v1 as research_console_start_research_v1,
)
from ops.aegis.research_lab.research_store_reader import (
    build_edge_lab_projection,
    build_evidence_chain_view,
    evidence_summary,
    health_payload,
    list_sleeves,
    paper_trial_summary,
)
from research_lab.challengers.challenger_evidence import list_challenger_evidence_batches, load_challenger_evidence_batch
from research_lab.challengers.challenger_comparison import list_challenger_comparison_reports, load_challenger_comparison_report
from research_lab.challengers.challenger_track import list_challenger_research_tracks, load_challenger_research_track
from research_lab.challengers.challenger_variant import list_challenger_variants, load_challenger_variant
from research_lab.challengers.human_review_decision import (
    decisions_for_dossier,
    human_review_decision_read_model,
    latest_decision_for_dossier,
    list_human_review_decisions,
    load_human_review_decision,
)
from research_lab.challengers.human_review_dossier import list_human_review_dossiers, load_human_review_dossier
from research_lab.integrity.research_store_integrity import latest_integrity_report, list_integrity_reports, load_integrity_report, error_count, warning_count
from research_lab.observations.observation_batch import (
    latest_observation_candidate_batch,
    latest_observation_candidates_for_batch,
    list_observation_candidate_batches,
    load_observation_candidate_batch,
)
from research_lab.observations.observation_candidate import latest_observation_candidate, list_observation_candidates, load_observation_candidate
from research_lab.observations.observation_cluster import latest_observation_cluster, list_observation_clusters, load_observation_cluster
from research_lab.observations.observation_cluster_batch import (
    latest_observation_cluster_batch,
    latest_observation_clusters_for_batch,
    list_observation_cluster_batches,
    load_observation_cluster_batch,
)
from research_lab.hypotheses.hypothesis_proposal import (
    hypothesis_proposals_for_cluster,
    latest_hypothesis_proposal,
    list_hypothesis_proposals,
    load_hypothesis_proposal,
)
from research_lab.hypotheses.hypothesis_proposal_batch import (
    latest_hypothesis_proposal_batch,
    latest_hypothesis_proposals_for_batch,
    list_hypothesis_proposal_batches,
    load_hypothesis_proposal_batch,
)
from research_lab.hypotheses.hypothesis_proposal_review import (
    hypothesis_proposal_reviews_for_proposal,
    latest_hypothesis_proposal_review,
    list_hypothesis_proposal_reviews,
    load_hypothesis_proposal_review,
)
from research_lab.event_intake.event_intake_registry import (
    load_hypothesis_proposal as load_event_intake_hypothesis_proposal,
)
from research_lab.research_intake.intake_registry import (
    assess_hypothesis_readiness as research_intake_assess_hypothesis_readiness,
    build_and_store_research_intake_dossier as research_intake_build_dossier,
    latest_research_intake_dossier as research_intake_latest_dossier,
    review_hypothesis_proposal as research_intake_review_hypothesis_proposal,
)
from research_lab.research_intake.proposal_queue import hypothesis_proposal_queue as research_intake_hypothesis_proposal_queue
from research_lab.projections.projection_builder import rebuild_research_projections
from research_lab.projections.projection_health import projection_health as research_projection_health
from research_lab.projections.projection_validator import validate_research_projections
from research_lab.hypotheses.hypothesis_proposal_review_batch import (
    latest_hypothesis_proposal_review_batch,
    latest_hypothesis_proposal_reviews_for_batch,
    list_hypothesis_proposal_review_batches,
    load_hypothesis_proposal_review_batch,
)
from research_lab.hypotheses.hypothesis_intake import (
    latest_hypothesis_intake_decision,
    list_hypothesis_intake_decisions,
    load_hypothesis_intake_decision,
)
from research_lab.hypotheses.hypothesis_intake_batch import (
    latest_hypothesis_intake_batch,
    latest_hypothesis_intake_decisions_for_batch,
    list_hypothesis_intake_batches,
    load_hypothesis_intake_batch,
)
from research_lab.hypotheses.research_hypothesis import (
    latest_research_hypothesis,
    list_research_hypotheses,
    load_research_hypothesis,
)
from research_lab.paper_trials.paper_trial_proposal import (
    latest_paper_trial_proposal,
    list_paper_trial_proposals,
    load_paper_trial_proposal,
    paper_trial_proposals_for_decision,
)
from research_lab.portfolio.backlog_priority import latest_research_backlog_priority_report
from research_lab.portfolio.evidence_inventory import latest_evidence_inventory
from research_lab.portfolio.paper_trial_inventory import latest_paper_trial_inventory
from research_lab.portfolio.sleeve_comparison import latest_sleeve_comparison_report
from research_lab.status.research_os_status import latest_research_os_status_report, list_research_os_status_reports, load_research_os_status_report
from research_lab.stability.sleeve_stability import (
    latest_expectancy_drift_report,
    latest_regime_fragility_report,
    latest_sleeve_stability_report,
)


def research_lab_health_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return health_payload(store_root=store_root)


def _safe_empty_payload(error: str, exc: Exception | None = None, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"ok": False, "read_only": True, "error": error, **extra}
    if exc is not None:
        payload["error_detail"] = str(exc)
        payload["error_type"] = type(exc).__name__
    return payload


def research_lab_sleeves_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return list_sleeves(store_root=store_root)


def research_lab_sleeve_v1(*, sleeve_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return build_evidence_chain_view(sleeve_id=sleeve_id, store_root=store_root)


def research_lab_evidence_chain_v1(*, sleeve_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return build_evidence_chain_view(sleeve_id=sleeve_id, store_root=store_root)


def research_lab_edge_lab_projection_v1(*, sleeve_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return build_edge_lab_projection(sleeve_id=sleeve_id, store_root=store_root)


def research_lab_evidence_summary_v1(*, evidence_package_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return evidence_summary(evidence_package_id=evidence_package_id, store_root=store_root)


def research_lab_paper_trial_summary_v1(*, paper_trial_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return paper_trial_summary(paper_trial_id=paper_trial_id, store_root=store_root)


def research_lab_evidence_inventory_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = latest_evidence_inventory(store_root=store_root)
    return payload | {"read_only": True} if payload else {"ok": False, "read_only": True, "error": "evidence_inventory_report_missing"}


def research_lab_paper_trial_inventory_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = latest_paper_trial_inventory(store_root=store_root)
    return payload | {"read_only": True} if payload else {"ok": False, "read_only": True, "error": "paper_trial_inventory_report_missing"}


def research_lab_sleeve_comparison_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = latest_sleeve_comparison_report(store_root=store_root)
    return payload | {"read_only": True} if payload else {"ok": False, "read_only": True, "error": "sleeve_comparison_report_missing"}


def research_lab_backlog_priority_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = latest_research_backlog_priority_report(store_root=store_root)
    return payload | {"read_only": True} if payload else {"ok": False, "read_only": True, "error": "research_backlog_priority_report_missing"}


def research_lab_expectancy_drift_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = latest_expectancy_drift_report(store_root=store_root)
    return payload | {"read_only": True} if payload else {"ok": False, "read_only": True, "error": "expectancy_drift_report_missing"}


def research_lab_regime_fragility_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = latest_regime_fragility_report(store_root=store_root)
    return payload | {"read_only": True} if payload else {"ok": False, "read_only": True, "error": "regime_fragility_report_missing"}


def research_lab_sleeve_stability_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = latest_sleeve_stability_report(store_root=store_root)
    return payload | {"read_only": True} if payload else {"ok": False, "read_only": True, "error": "sleeve_stability_report_missing"}


def research_lab_sleeve_stability_v1(*, sleeve_id: str, store_root: Path | None = None) -> dict[str, Any]:
    stability = latest_sleeve_stability_report(store_root=store_root, sleeve_id=sleeve_id)
    drift = latest_expectancy_drift_report(store_root=store_root, sleeve_id=sleeve_id)
    fragility = latest_regime_fragility_report(store_root=store_root, sleeve_id=sleeve_id)
    if not stability and not drift and not fragility:
        return {"ok": False, "read_only": True, "sleeve_id": sleeve_id, "error": "stability_reports_missing"}
    return {
        "ok": True,
        "read_only": True,
        "sleeve_id": sleeve_id,
        "sleeve_stability": stability,
        "expectancy_drift": drift,
        "regime_fragility": fragility,
    }


def research_lab_challenger_tracks_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    rows = list_challenger_research_tracks(store_root=store_root)
    projected = [
        {
            "challenger_track_id": row.get("challenger_track_id"),
            "incumbent_sleeve_id": row.get("incumbent_sleeve_id"),
            "trigger_summary": {"sleeve_stability_report_id": row.get("sleeve_stability_report_id")},
            "status": row.get("status"),
            "hypothesis_count": row.get("hypothesis_count"),
            "recommended_next_action": row.get("recommended_next_action"),
            "research_label": row.get("research_label"),
        }
        for row in rows
    ]
    return {"ok": True, "read_only": True, "challenger_tracks": projected, "count": len(projected)}


def research_lab_challenger_track_v1(*, challenger_track_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        track = load_challenger_research_track(challenger_track_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "challenger_track_id": challenger_track_id, "error": "challenger_track_missing"}
    return {
        "ok": True,
        "read_only": True,
        "challenger_track": {
            "challenger_track_id": track.get("challenger_track_id"),
            "incumbent_sleeve_id": track.get("incumbent_sleeve_id"),
            "trigger_report_ids": track.get("trigger_report_ids"),
            "trigger_reason": track.get("trigger_reason"),
            "status": track.get("status"),
            "hypothesis_count": len(track.get("challenger_hypothesis_set") or []),
            "recommended_next_action": track.get("recommended_next_action"),
            "research_label": track.get("research_label"),
            "governance_constraints": track.get("governance_constraints"),
        },
    }


def research_lab_challenger_variants_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    rows = list_challenger_variants(store_root=store_root)
    projected = [
        {
            "challenger_variant_id": row.get("challenger_variant_id"),
            "challenger_track_id": row.get("challenger_track_id"),
            "incumbent_sleeve_id": row.get("incumbent_sleeve_id"),
            "hypothesis_id": row.get("hypothesis_id"),
            "variant_name": row.get("variant_name"),
            "status": row.get("status"),
            "research_label": row.get("research_label"),
        }
        for row in rows
    ]
    return {"ok": True, "read_only": True, "challenger_variants": projected, "count": len(projected)}


def research_lab_challenger_variant_v1(*, challenger_variant_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        variant = load_challenger_variant(challenger_variant_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "challenger_variant_id": challenger_variant_id, "error": "challenger_variant_missing"}
    return {
        "ok": True,
        "read_only": True,
        "challenger_variant": {
            "challenger_variant_id": variant.get("challenger_variant_id"),
            "challenger_track_id": variant.get("challenger_track_id"),
            "incumbent_sleeve_id": variant.get("incumbent_sleeve_id"),
            "hypothesis_id": variant.get("hypothesis_id"),
            "variant_name": variant.get("variant_name"),
            "parameter_deltas": variant.get("parameter_deltas"),
            "rule_deltas": variant.get("rule_deltas"),
            "regime_constraints": variant.get("regime_constraints"),
            "source_artifact_ids": variant.get("source_artifact_ids"),
            "status": variant.get("status"),
            "research_label": variant.get("research_label"),
            "governance_constraints": variant.get("governance_constraints"),
        },
    }


def research_lab_challenger_evidence_batches_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    rows = list_challenger_evidence_batches(store_root=store_root)
    projected = [
        {
            "challenger_evidence_batch_id": row.get("challenger_evidence_batch_id"),
            "challenger_track_id": row.get("challenger_track_id"),
            "incumbent_sleeve_id": row.get("incumbent_sleeve_id"),
            "generated_item_count": row.get("generated_item_count"),
            "blocked_hypothesis_count": row.get("blocked_hypothesis_count"),
            "evidence_quality_summary": row.get("evidence_quality_summary"),
            "recommended_next_action": row.get("recommended_next_action"),
            "research_label": row.get("research_label"),
        }
        for row in rows
    ]
    return {"ok": True, "read_only": True, "challenger_evidence_batches": projected, "count": len(projected)}


def research_lab_challenger_evidence_batch_v1(*, challenger_evidence_batch_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = load_challenger_evidence_batch(challenger_evidence_batch_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "challenger_evidence_batch_id": challenger_evidence_batch_id, "error": "challenger_evidence_batch_missing"}
    return {
        "ok": True,
        "read_only": True,
        "challenger_evidence_batch": {
            "challenger_evidence_batch_id": batch.get("challenger_evidence_batch_id"),
            "challenger_track_id": batch.get("challenger_track_id"),
            "incumbent_sleeve_id": batch.get("incumbent_sleeve_id"),
            "generated_item_count": sum(1 for item in batch.get("challenger_evidence_items") or [] if item.get("status") == "generated"),
            "blocked_hypothesis_count": len(batch.get("blocked_hypotheses") or []),
            "evidence_quality_summary": {
                quality: sum(1 for item in batch.get("challenger_evidence_items") or [] if item.get("evidence_quality") == quality)
                for quality in ["strong", "moderate", "weak", "insufficient", "blocked"]
            },
            "recommended_next_action": batch.get("recommended_next_action"),
            "research_label": batch.get("research_label"),
            "governance_constraints": batch.get("governance_constraints"),
            "evidence_generation_config": batch.get("evidence_generation_config"),
            "evidence_completeness": [
                {
                    "challenger_hypothesis_id": item.get("challenger_hypothesis_id"),
                    "challenger_variant_id": item.get("challenger_variant_id"),
                    "status": item.get("status"),
                    "evidence_completeness": item.get("evidence_completeness"),
                    "artifact_lineage": item.get("artifact_lineage"),
                    "failure_reason": item.get("failure_reason"),
                }
                for item in batch.get("challenger_evidence_items") or []
            ],
        },
    }


def research_lab_challenger_evidence_completeness_v1(*, challenger_evidence_batch_id: str | None = None, store_root: Path | None = None) -> dict[str, Any]:
    if challenger_evidence_batch_id:
        batch = load_challenger_evidence_batch(challenger_evidence_batch_id, store_root=store_root)
    else:
        rows = list_challenger_evidence_batches(store_root=store_root)
        if not rows:
            return {"ok": False, "read_only": True, "error": "challenger_evidence_batch_missing"}
        batch = load_challenger_evidence_batch(str(rows[-1]["challenger_evidence_batch_id"]), store_root=store_root)
    items = batch.get("challenger_evidence_items") or []
    return {
        "ok": True,
        "read_only": True,
        "challenger_evidence_batch_id": batch.get("challenger_evidence_batch_id"),
        "complete_count": sum(1 for item in items if (item.get("evidence_completeness") or {}).get("complete") is True),
        "blocked_count": sum(1 for item in items if item.get("status") == "blocked"),
        "incomplete_items": [
            {
                "challenger_hypothesis_id": item.get("challenger_hypothesis_id"),
                "status": item.get("status"),
                "missing_fields": (item.get("evidence_completeness") or {}).get("missing_fields") or [],
                "failure_reason": item.get("failure_reason"),
            }
            for item in items
            if item.get("status") == "blocked" or (item.get("evidence_completeness") or {}).get("complete") is False
        ],
    }


def research_lab_challenger_evidence_lineage_v1(*, challenger_evidence_batch_id: str, store_root: Path | None = None) -> dict[str, Any]:
    batch = load_challenger_evidence_batch(challenger_evidence_batch_id, store_root=store_root)
    return {
        "ok": True,
        "read_only": True,
        "challenger_evidence_batch_id": challenger_evidence_batch_id,
        "lineage": [
            {
                "challenger_hypothesis_id": item.get("challenger_hypothesis_id"),
                "artifact_lineage": item.get("artifact_lineage") or {},
                "generated_evidence_ids": item.get("generated_evidence_ids") or [],
                "status": item.get("status"),
            }
            for item in batch.get("challenger_evidence_items") or []
        ],
    }


def research_lab_challenger_blockers_v1(*, challenger_evidence_batch_id: str | None = None, store_root: Path | None = None) -> dict[str, Any]:
    if challenger_evidence_batch_id:
        batch = load_challenger_evidence_batch(challenger_evidence_batch_id, store_root=store_root)
    else:
        rows = list_challenger_evidence_batches(store_root=store_root)
        if not rows:
            return {"ok": False, "read_only": True, "error": "challenger_evidence_batch_missing"}
        batch = load_challenger_evidence_batch(str(rows[-1]["challenger_evidence_batch_id"]), store_root=store_root)
    return {"ok": True, "read_only": True, "challenger_evidence_batch_id": batch.get("challenger_evidence_batch_id"), "blockers": batch.get("blocked_hypotheses") or []}


def research_lab_challenger_comparison_reports_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    rows = list_challenger_comparison_reports(store_root=store_root)
    projected = [
        {
            "challenger_comparison_report_id": row.get("challenger_comparison_report_id"),
            "challenger_track_id": row.get("challenger_track_id"),
            "challenger_evidence_batch_id": row.get("challenger_evidence_batch_id"),
            "incumbent_sleeve_id": row.get("incumbent_sleeve_id"),
            "evidence_sufficiency": row.get("evidence_sufficiency"),
            "active_challenger_count": row.get("active_challenger_count"),
            "excluded_challenger_count": row.get("excluded_challenger_count"),
            "top_research_review_candidate_id": row.get("top_research_review_candidate_id"),
            "recommended_next_action": row.get("recommended_next_action"),
            "research_label": row.get("research_label"),
        }
        for row in rows
    ]
    return {"ok": True, "read_only": True, "challenger_comparison_reports": projected, "count": len(projected)}


def research_lab_challenger_comparison_report_v1(*, challenger_comparison_report_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        report = load_challenger_comparison_report(challenger_comparison_report_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "challenger_comparison_report_id": challenger_comparison_report_id, "error": "challenger_comparison_report_missing"}
    active = [row for row in report.get("comparison_table") or [] if row.get("entity_type") == "challenger" and row.get("exclusion_status") == "included_for_human_review"]
    excluded = [row for row in report.get("comparison_table") or [] if row.get("entity_type") != "incumbent" and row.get("exclusion_status") == "excluded"]
    return {
        "ok": True,
        "read_only": True,
        "challenger_comparison_report": {
            "challenger_comparison_report_id": report.get("challenger_comparison_report_id"),
            "challenger_track_id": report.get("challenger_track_id"),
            "challenger_evidence_batch_id": report.get("challenger_evidence_batch_id"),
            "incumbent_sleeve_id": report.get("incumbent_sleeve_id"),
            "evidence_sufficiency": report.get("evidence_sufficiency"),
            "active_challenger_count": len(active),
            "excluded_challenger_count": len(excluded),
            "top_research_review_candidate_id": (report.get("deterministic_rank_order") or [{}])[0].get("challenger_hypothesis_id", ""),
            "recommended_next_action": report.get("recommended_next_action"),
            "research_label": report.get("research_label"),
        },
    }


def research_lab_human_review_dossiers_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    rows = list_human_review_dossiers(store_root=store_root)
    projected = [
        {
            "human_review_dossier_id": row.get("human_review_dossier_id"),
            "dossier_type": row.get("dossier_type"),
            "incumbent_sleeve_id": row.get("incumbent_sleeve_id"),
            "challenger_track_id": row.get("challenger_track_id"),
            "challenger_comparison_report_id": row.get("challenger_comparison_report_id"),
            "top_research_review_candidate_id": row.get("top_research_review_candidate_id"),
            "active_challenger_count": row.get("active_challenger_count"),
            "excluded_challenger_count": row.get("excluded_challenger_count"),
            "recommended_next_action": row.get("recommended_next_action"),
            "research_label": row.get("research_label"),
        }
        for row in rows
    ]
    return {"ok": True, "read_only": True, "human_review_dossiers": projected, "count": len(projected)}


def research_lab_human_review_dossier_v1(*, human_review_dossier_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        dossier = load_human_review_dossier(human_review_dossier_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "human_review_dossier_id": human_review_dossier_id, "error": "human_review_dossier_missing"}
    return {
        "ok": True,
        "read_only": True,
        "human_review_dossier": {
            "human_review_dossier_id": dossier.get("human_review_dossier_id"),
            "dossier_type": dossier.get("dossier_type"),
            "incumbent_sleeve_id": dossier.get("incumbent_sleeve_id"),
            "challenger_track_id": dossier.get("challenger_track_id"),
            "challenger_comparison_report_id": dossier.get("challenger_comparison_report_id"),
            "top_research_review_candidate_id": (dossier.get("executive_summary") or {}).get("top_research_review_candidate_id"),
            "active_challenger_count": (dossier.get("executive_summary") or {}).get("active_challenger_count"),
            "excluded_challenger_count": (dossier.get("executive_summary") or {}).get("excluded_challenger_count"),
            "recommended_next_action": dossier.get("recommended_next_action"),
            "research_label": dossier.get("research_label"),
        },
    }


def research_lab_human_review_decisions_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    rows = list_human_review_decisions(store_root=store_root)
    return {"ok": True, "read_only": True, "human_review_decisions": rows, "count": len(rows)}


def research_lab_human_review_decision_latest_v1(*, human_review_dossier_id: str | None = None, store_root: Path | None = None) -> dict[str, Any]:
    if human_review_dossier_id:
        latest = latest_decision_for_dossier(human_review_dossier_id, store_root=store_root)
    else:
        rows = list_human_review_decisions(store_root=store_root)
        latest = sorted(rows, key=lambda row: (str(row.get("decided_at") or ""), str(row.get("human_review_decision_id") or "")))[-1] if rows else None
    return {"ok": bool(latest), "read_only": True, "latest_decision": latest, "error": "" if latest else "human_review_decision_missing"}


def research_lab_human_review_decision_v1(*, human_review_decision_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        decision = load_human_review_decision(human_review_decision_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "human_review_decision_id": human_review_decision_id, "error": "human_review_decision_missing"}
    return {"ok": True, "read_only": True, "human_review_decision": decision}


def research_lab_human_review_dossier_decisions_v1(*, human_review_dossier_id: str, store_root: Path | None = None) -> dict[str, Any]:
    rows = decisions_for_dossier(human_review_dossier_id, store_root=store_root)
    return {"ok": True, "read_only": True, "human_review_dossier_id": human_review_dossier_id, "decisions": rows, "count": len(rows)}


def research_lab_human_review_decision_read_model_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return human_review_decision_read_model(store_root=store_root)


def research_lab_integrity_reports_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    rows = list_integrity_reports(store_root=store_root)
    return {"ok": True, "read_only": True, "integrity_reports": rows, "count": len(rows)}


def research_lab_integrity_report_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        report = latest_integrity_report(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("integrity_report_unavailable", exc, integrity_report=None)
    if not report:
        return _safe_empty_payload("integrity_report_missing", integrity_report=None)
    return {"ok": True, "read_only": True, "integrity_report": report}


def research_lab_integrity_report_v1(*, integrity_report_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        report = load_integrity_report(integrity_report_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "integrity_report_id": integrity_report_id, "error": "integrity_report_missing"}
    return {"ok": True, "read_only": True, "integrity_report": report}


def research_lab_integrity_latest_summary_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        report = latest_integrity_report(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("integrity_report_unavailable", exc)
    if not report:
        return _safe_empty_payload("integrity_report_missing")
    return {
        "ok": True,
        "read_only": True,
        "integrity_report_id": report.get("integrity_report_id"),
        "overall_status": report.get("overall_status"),
        "artifact_count": (report.get("artifact_counts") or {}).get("audited_json_artifacts"),
        "registry_count": len(report.get("registry_counts") or {}),
        "audit_event_count": sum(int(value) for value in (report.get("audit_event_counts") or {}).values()),
        "error_count": error_count(report),
        "warning_count": warning_count(report),
        "missing_reference_count": len(report.get("missing_references") or []),
        "orphan_artifact_count": len(report.get("orphaned_artifacts") or []),
        "hash_mismatch_count": len(report.get("hash_mismatches") or []),
        "schema_failure_count": len(report.get("schema_validation_failures") or []),
        "lineage_break_count": len(report.get("lineage_breaks") or []),
        "duplicate_registry_entry_count": len(report.get("duplicate_registry_entries") or []),
        "mutation_boundary_violation_count": len(report.get("mutation_boundary_violations") or []),
        "known_blocker_count": len(report.get("unresolved_blockers") or []),
    }


def research_lab_status_reports_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_research_os_status_reports(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("research_os_status_reports_unavailable", exc, status_reports=[], research_os_status_reports=[], count=0)
    return {"ok": True, "read_only": True, "status_reports": rows, "research_os_status_reports": rows, "count": len(rows)}


def research_lab_status_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        report = latest_research_os_status_report(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("research_os_status_report_unavailable", exc, status_report=None, research_os_status_report=None)
    if not report:
        return _safe_empty_payload("research_os_status_report_missing", status_report=None, research_os_status_report=None)
    return {"ok": True, "read_only": True, "status_report": report, "research_os_status_report": report}


def research_lab_status_report_v1(*, research_os_status_report_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        report = load_research_os_status_report(research_os_status_report_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "research_os_status_report_id": research_os_status_report_id, "error": "research_os_status_report_missing"}
    return {"ok": True, "read_only": True, "status_report": report, "research_os_status_report": report}


def research_lab_paper_trial_proposals_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_paper_trial_proposals(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("paper_trial_proposals_unavailable", exc, paper_trial_proposals=[], count=0)
    return {"ok": True, "read_only": True, "paper_trial_proposals": rows, "count": len(rows)}


def research_lab_paper_trial_proposal_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        proposal = latest_paper_trial_proposal(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("paper_trial_proposal_unavailable", exc, paper_trial_proposal=None)
    if not proposal:
        return _safe_empty_payload("paper_trial_proposal_missing", paper_trial_proposal=None)
    return {"ok": True, "read_only": True, "paper_trial_proposal": proposal}


def research_lab_paper_trial_proposal_v1(*, paper_trial_proposal_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        proposal = load_paper_trial_proposal(paper_trial_proposal_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "paper_trial_proposal_id": paper_trial_proposal_id, "error": "paper_trial_proposal_missing"}
    return {"ok": True, "read_only": True, "paper_trial_proposal": proposal}


def research_lab_human_review_decision_paper_trial_proposals_v1(*, human_review_decision_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = paper_trial_proposals_for_decision(human_review_decision_id, store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("paper_trial_proposals_unavailable", exc, human_review_decision_id=human_review_decision_id, paper_trial_proposals=[], count=0)
    return {"ok": True, "read_only": True, "human_review_decision_id": human_review_decision_id, "paper_trial_proposals": rows, "count": len(rows)}


def _filter_observation_rows(rows: list[dict[str, Any]], *, observation_type: str | None = None, observation_family: str | None = None, severity: str | None = None, research_status: str | None = None) -> list[dict[str, Any]]:
    result = rows
    if observation_type:
        result = [row for row in result if row.get("observation_type") == observation_type]
    if observation_family:
        result = [row for row in result if row.get("observation_family") == observation_family]
    if severity:
        result = [row for row in result if row.get("severity") == severity]
    if research_status:
        result = [row for row in result if row.get("research_status") == research_status]
    return result


def research_lab_observation_candidates_v1(
    *,
    observation_type: str | None = None,
    observation_family: str | None = None,
    severity: str | None = None,
    research_status: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    try:
        rows = _filter_observation_rows(
            list_observation_candidates(store_root=store_root),
            observation_type=observation_type,
            observation_family=observation_family,
            severity=severity,
            research_status=research_status,
        )
    except Exception as exc:
        return _safe_empty_payload("observation_candidates_unavailable", exc, observation_candidates=[], count=0)
    return {"ok": True, "read_only": True, "observation_candidates": rows, "count": len(rows)}


def research_lab_observation_candidate_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        candidate = latest_observation_candidate(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("observation_candidate_unavailable", exc, observation_candidate=None)
    if not candidate:
        return _safe_empty_payload("observation_candidate_missing", observation_candidate=None)
    return {"ok": True, "read_only": True, "observation_candidate": candidate}


def research_lab_observation_candidate_v1(*, observation_candidate_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        candidate = load_observation_candidate(observation_candidate_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "observation_candidate_id": observation_candidate_id, "error": "observation_candidate_missing"}
    return {"ok": True, "read_only": True, "observation_candidate": candidate}


def research_lab_observation_candidate_batches_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_observation_candidate_batches(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("observation_candidate_batches_unavailable", exc, observation_candidate_batches=[], count=0)
    return {"ok": True, "read_only": True, "observation_candidate_batches": rows, "count": len(rows)}


def research_lab_observation_candidate_batch_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = latest_observation_candidate_batch(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("observation_candidate_batch_unavailable", exc, observation_candidate_batch=None, top_observations=[])
    if not batch:
        return _safe_empty_payload("observation_candidate_batch_missing", observation_candidate_batch=None, top_observations=[])
    try:
        observations = latest_observation_candidates_for_batch(store_root=store_root, limit=10)
    except Exception as exc:
        return _safe_empty_payload("observation_candidate_batch_children_unavailable", exc, observation_candidate_batch=batch, top_observations=[])
    return {"ok": True, "read_only": True, "observation_candidate_batch": batch, "top_observations": observations}


def research_lab_observation_candidate_batch_v1(*, observation_candidate_batch_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = load_observation_candidate_batch(observation_candidate_batch_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "observation_candidate_batch_id": observation_candidate_batch_id, "error": "observation_candidate_batch_missing"}
    return {"ok": True, "read_only": True, "observation_candidate_batch": batch}


def _filter_cluster_rows(rows: list[dict[str, Any]], *, cluster_family: str | None = None, cluster_status: str | None = None, research_status: str | None = None) -> list[dict[str, Any]]:
    result = rows
    if cluster_family:
        result = [row for row in result if row.get("cluster_family") == cluster_family]
    if cluster_status:
        result = [row for row in result if row.get("cluster_status") == cluster_status]
    if research_status:
        result = [row for row in result if row.get("research_status") == research_status]
    return result


def research_lab_observation_clusters_v1(
    *,
    cluster_family: str | None = None,
    cluster_status: str | None = None,
    research_status: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    try:
        rows = _filter_cluster_rows(list_observation_clusters(store_root=store_root), cluster_family=cluster_family, cluster_status=cluster_status, research_status=research_status)
    except Exception as exc:
        return _safe_empty_payload("observation_clusters_unavailable", exc, observation_clusters=[], count=0)
    return {"ok": True, "read_only": True, "observation_clusters": rows, "count": len(rows)}


def research_lab_observation_cluster_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        cluster = latest_observation_cluster(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("observation_cluster_unavailable", exc, observation_cluster=None)
    if not cluster:
        return _safe_empty_payload("observation_cluster_missing", observation_cluster=None)
    return {"ok": True, "read_only": True, "observation_cluster": cluster}


def research_lab_observation_cluster_v1(*, observation_cluster_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        cluster = load_observation_cluster(observation_cluster_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "observation_cluster_id": observation_cluster_id, "error": "observation_cluster_missing"}
    return {"ok": True, "read_only": True, "observation_cluster": cluster}


def research_lab_observation_cluster_batches_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_observation_cluster_batches(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("observation_cluster_batches_unavailable", exc, observation_cluster_batches=[], count=0)
    return {"ok": True, "read_only": True, "observation_cluster_batches": rows, "count": len(rows)}


def research_lab_observation_cluster_batch_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = latest_observation_cluster_batch(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("observation_cluster_batch_unavailable", exc, observation_cluster_batch=None, top_clusters=[])
    if not batch:
        return _safe_empty_payload("observation_cluster_batch_missing", observation_cluster_batch=None, top_clusters=[])
    try:
        clusters = latest_observation_clusters_for_batch(store_root=store_root, limit=10)
    except Exception as exc:
        return _safe_empty_payload("observation_cluster_batch_children_unavailable", exc, observation_cluster_batch=batch, top_clusters=[])
    return {"ok": True, "read_only": True, "observation_cluster_batch": batch, "top_clusters": clusters}


def research_lab_observation_cluster_batch_v1(*, observation_cluster_batch_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = load_observation_cluster_batch(observation_cluster_batch_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "observation_cluster_batch_id": observation_cluster_batch_id, "error": "observation_cluster_batch_missing"}
    return {"ok": True, "read_only": True, "observation_cluster_batch": batch}


def _filter_hypothesis_proposal_rows(rows: list[dict[str, Any]], *, proposal_status: str | None = None, proposal_family: str | None = None) -> list[dict[str, Any]]:
    result = rows
    if proposal_status:
        result = [row for row in result if row.get("proposal_status") == proposal_status]
    if proposal_family:
        result = [row for row in result if row.get("proposal_family") == proposal_family]
    return result


def research_lab_hypothesis_proposals_v1(
    *,
    proposal_status: str | None = None,
    proposal_family: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    try:
        rows = _filter_hypothesis_proposal_rows(list_hypothesis_proposals(store_root=store_root), proposal_status=proposal_status, proposal_family=proposal_family)
        intake = research_intake_hypothesis_proposal_queue(status=proposal_status, store_root=store_root, audit_view=False)
        event_rows = intake.get("hypothesis_proposals") or []
        if proposal_family:
            event_rows = [row for row in event_rows if row.get("proposal_family") == proposal_family]
        rows = [*rows, *event_rows]
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposals_unavailable", exc, hypothesis_proposals=[], count=0)
    return {"ok": True, "read_only": True, "hypothesis_proposals": rows, "count": len(rows)}


def research_lab_hypothesis_proposal_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        proposal = latest_hypothesis_proposal(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_unavailable", exc, hypothesis_proposal=None)
    if not proposal:
        return _safe_empty_payload("hypothesis_proposal_missing", hypothesis_proposal=None)
    return {"ok": True, "read_only": True, "hypothesis_proposal": proposal}


def research_lab_hypothesis_proposal_v1(*, hypothesis_proposal_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        proposal = load_hypothesis_proposal(hypothesis_proposal_id, store_root=store_root)
    except Exception:
        try:
            proposal = load_event_intake_hypothesis_proposal(hypothesis_proposal_id, store_root=store_root)
        except Exception:
            return {"ok": False, "read_only": True, "hypothesis_proposal_id": hypothesis_proposal_id, "error": "hypothesis_proposal_missing"}
    return {"ok": True, "read_only": True, "hypothesis_proposal": proposal}



def research_lab_research_intake_queue_v1(*, status: str | None = None, store_root: Path | None = None) -> dict[str, Any]:
    try:
        return research_intake_hypothesis_proposal_queue(status=status, store_root=store_root, audit_view=False)
    except Exception as exc:
        return _safe_empty_payload("research_intake_queue_unavailable", exc, hypothesis_proposals=[], queue=[], lanes={}, count=0)


def research_lab_hypothesis_proposal_dossier_v1(*, hypothesis_proposal_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        dossier = research_intake_latest_dossier(hypothesis_proposal_id, store_root=store_root)
        if not dossier:
            result = research_intake_build_dossier(hypothesis_proposal_id=hypothesis_proposal_id, store_root=store_root, actor="AegisAPI")
            dossier = result["research_intake_dossier"]
            markdown = result.get("markdown", "")
        else:
            markdown = ""
        return {"ok": True, "read_only": True, "hypothesis_proposal_id": hypothesis_proposal_id, "research_intake_dossier": dossier, "dossier": dossier, "markdown": markdown}
    except Exception as exc:
        return _safe_empty_payload("research_intake_dossier_unavailable", exc, hypothesis_proposal_id=hypothesis_proposal_id, research_intake_dossier=None)


def research_lab_hypothesis_proposal_assess_readiness_v1(*, hypothesis_proposal_id: str, store_root: Path | None = None, actor: str = "AegisAPI") -> dict[str, Any]:
    try:
        result = research_intake_assess_hypothesis_readiness(hypothesis_proposal_id=hypothesis_proposal_id, store_root=store_root, actor=actor)
        return {"ok": True, "read_only": False, "append_only": True, **result}
    except Exception as exc:
        return _safe_empty_payload("research_readiness_assessment_failed", exc, hypothesis_proposal_id=hypothesis_proposal_id)


def research_lab_hypothesis_proposal_review_append_v1(*, hypothesis_proposal_id: str, decision: str, reason: str, reviewed_by: str, store_root: Path | None = None, actor: str = "AegisAPI") -> dict[str, Any]:
    try:
        result = research_intake_review_hypothesis_proposal(hypothesis_proposal_id=hypothesis_proposal_id, decision=decision, reason=reason, reviewed_by=reviewed_by, store_root=store_root, actor=actor)
        return {"ok": True, "read_only": False, "append_only": True, **result}
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_review_failed", exc, hypothesis_proposal_id=hypothesis_proposal_id)

def research_lab_hypothesis_proposal_batches_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_hypothesis_proposal_batches(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_batches_unavailable", exc, hypothesis_proposal_batches=[], count=0)
    return {"ok": True, "read_only": True, "hypothesis_proposal_batches": rows, "count": len(rows)}


def research_lab_hypothesis_proposal_batch_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = latest_hypothesis_proposal_batch(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_batch_unavailable", exc, hypothesis_proposal_batch=None, top_hypothesis_proposals=[])
    if not batch:
        return _safe_empty_payload("hypothesis_proposal_batch_missing", hypothesis_proposal_batch=None, top_hypothesis_proposals=[])
    try:
        proposals = latest_hypothesis_proposals_for_batch(store_root=store_root, limit=10)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_batch_children_unavailable", exc, hypothesis_proposal_batch=batch, top_hypothesis_proposals=[])
    return {"ok": True, "read_only": True, "hypothesis_proposal_batch": batch, "top_hypothesis_proposals": proposals}


def research_lab_hypothesis_proposal_batch_v1(*, hypothesis_proposal_batch_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = load_hypothesis_proposal_batch(hypothesis_proposal_batch_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "hypothesis_proposal_batch_id": hypothesis_proposal_batch_id, "error": "hypothesis_proposal_batch_missing"}
    return {"ok": True, "read_only": True, "hypothesis_proposal_batch": batch}


def research_lab_observation_cluster_hypothesis_proposals_v1(*, observation_cluster_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = hypothesis_proposals_for_cluster(observation_cluster_id, store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposals_unavailable", exc, observation_cluster_id=observation_cluster_id, hypothesis_proposals=[], count=0)
    return {"ok": True, "read_only": True, "observation_cluster_id": observation_cluster_id, "hypothesis_proposals": rows, "count": len(rows)}


def _filter_hypothesis_review_rows(rows: list[dict[str, Any]], *, review_decision: str | None = None, review_status: str | None = None) -> list[dict[str, Any]]:
    result = rows
    if review_decision:
        result = [row for row in result if row.get("review_decision") == review_decision]
    if review_status:
        result = [row for row in result if row.get("review_status") == review_status]
    return result


def research_lab_hypothesis_proposal_reviews_v1(
    *,
    review_decision: str | None = None,
    review_status: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    try:
        rows = _filter_hypothesis_review_rows(list_hypothesis_proposal_reviews(store_root=store_root), review_decision=review_decision, review_status=review_status)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_reviews_unavailable", exc, hypothesis_proposal_reviews=[], count=0)
    return {"ok": True, "read_only": True, "hypothesis_proposal_reviews": rows, "count": len(rows)}


def research_lab_hypothesis_proposal_review_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        review = latest_hypothesis_proposal_review(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_review_unavailable", exc, hypothesis_proposal_review=None)
    if not review:
        return _safe_empty_payload("hypothesis_proposal_review_missing", hypothesis_proposal_review=None)
    return {"ok": True, "read_only": True, "hypothesis_proposal_review": review}


def research_lab_hypothesis_proposal_review_v1(*, hypothesis_proposal_review_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        review = load_hypothesis_proposal_review(hypothesis_proposal_review_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "hypothesis_proposal_review_id": hypothesis_proposal_review_id, "error": "hypothesis_proposal_review_missing"}
    return {"ok": True, "read_only": True, "hypothesis_proposal_review": review}


def research_lab_hypothesis_proposal_review_batches_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_hypothesis_proposal_review_batches(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_review_batches_unavailable", exc, hypothesis_proposal_review_batches=[], count=0)
    return {"ok": True, "read_only": True, "hypothesis_proposal_review_batches": rows, "count": len(rows)}


def research_lab_hypothesis_proposal_review_batch_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = latest_hypothesis_proposal_review_batch(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_review_batch_unavailable", exc, hypothesis_proposal_review_batch=None, top_hypothesis_proposal_reviews=[])
    if not batch:
        return _safe_empty_payload("hypothesis_proposal_review_batch_missing", hypothesis_proposal_review_batch=None, top_hypothesis_proposal_reviews=[])
    try:
        reviews = latest_hypothesis_proposal_reviews_for_batch(store_root=store_root, limit=10)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_review_batch_children_unavailable", exc, hypothesis_proposal_review_batch=batch, top_hypothesis_proposal_reviews=[])
    return {"ok": True, "read_only": True, "hypothesis_proposal_review_batch": batch, "top_hypothesis_proposal_reviews": reviews}


def research_lab_hypothesis_proposal_review_batch_v1(*, hypothesis_proposal_review_batch_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = load_hypothesis_proposal_review_batch(hypothesis_proposal_review_batch_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "hypothesis_proposal_review_batch_id": hypothesis_proposal_review_batch_id, "error": "hypothesis_proposal_review_batch_missing"}
    return {"ok": True, "read_only": True, "hypothesis_proposal_review_batch": batch}


def research_lab_hypothesis_proposal_reviews_for_proposal_v1(*, hypothesis_proposal_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = hypothesis_proposal_reviews_for_proposal(hypothesis_proposal_id, store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_proposal_reviews_unavailable", exc, hypothesis_proposal_id=hypothesis_proposal_id, hypothesis_proposal_reviews=[], count=0)
    return {"ok": True, "read_only": True, "hypothesis_proposal_id": hypothesis_proposal_id, "hypothesis_proposal_reviews": rows, "count": len(rows)}


def research_lab_hypothesis_intake_decisions_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_hypothesis_intake_decisions(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_intake_decisions_unavailable", exc, hypothesis_intake_decisions=[], count=0)
    return {"ok": True, "read_only": True, "hypothesis_intake_decisions": rows, "count": len(rows)}


def research_lab_hypothesis_intake_decision_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        decision = latest_hypothesis_intake_decision(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_intake_decision_unavailable", exc, hypothesis_intake_decision=None)
    if not decision:
        return _safe_empty_payload("hypothesis_intake_decision_missing", hypothesis_intake_decision=None)
    return {"ok": True, "read_only": True, "hypothesis_intake_decision": decision}


def research_lab_hypothesis_intake_decision_v1(*, hypothesis_intake_decision_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        decision = load_hypothesis_intake_decision(hypothesis_intake_decision_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "hypothesis_intake_decision_id": hypothesis_intake_decision_id, "error": "hypothesis_intake_decision_missing"}
    except Exception as exc:
        return _safe_empty_payload("hypothesis_intake_decision_unavailable", exc, hypothesis_intake_decision_id=hypothesis_intake_decision_id, hypothesis_intake_decision=None)
    return {"ok": True, "read_only": True, "hypothesis_intake_decision": decision}


def research_lab_hypothesis_intake_batches_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_hypothesis_intake_batches(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_intake_batches_unavailable", exc, hypothesis_intake_batches=[], count=0)
    return {"ok": True, "read_only": True, "hypothesis_intake_batches": rows, "count": len(rows)}


def research_lab_hypothesis_intake_batch_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = latest_hypothesis_intake_batch(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_intake_batch_unavailable", exc, hypothesis_intake_batch=None, top_hypothesis_intake_decisions=[])
    if not batch:
        return _safe_empty_payload("hypothesis_intake_batch_missing", hypothesis_intake_batch=None, top_hypothesis_intake_decisions=[])
    try:
        decisions = latest_hypothesis_intake_decisions_for_batch(store_root=store_root, limit=10)
    except Exception as exc:
        return _safe_empty_payload("hypothesis_intake_batch_children_unavailable", exc, hypothesis_intake_batch=batch, top_hypothesis_intake_decisions=[])
    return {"ok": True, "read_only": True, "hypothesis_intake_batch": batch, "top_hypothesis_intake_decisions": decisions}


def research_lab_hypothesis_intake_batch_v1(*, hypothesis_intake_batch_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        batch = load_hypothesis_intake_batch(hypothesis_intake_batch_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "hypothesis_intake_batch_id": hypothesis_intake_batch_id, "error": "hypothesis_intake_batch_missing"}
    except Exception as exc:
        return _safe_empty_payload("hypothesis_intake_batch_unavailable", exc, hypothesis_intake_batch_id=hypothesis_intake_batch_id, hypothesis_intake_batch=None)
    return {"ok": True, "read_only": True, "hypothesis_intake_batch": batch}


def research_lab_research_hypotheses_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        rows = list_research_hypotheses(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("research_hypotheses_unavailable", exc, research_hypotheses=[], count=0)
    return {"ok": True, "read_only": True, "research_hypotheses": rows, "count": len(rows)}


def research_lab_research_hypothesis_latest_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    try:
        hypothesis = latest_research_hypothesis(store_root=store_root)
    except Exception as exc:
        return _safe_empty_payload("research_hypothesis_unavailable", exc, research_hypothesis=None)
    if not hypothesis:
        return _safe_empty_payload("research_hypothesis_missing", research_hypothesis=None)
    return {"ok": True, "read_only": True, "research_hypothesis": hypothesis}


def research_lab_research_hypothesis_v1(*, research_hypothesis_id: str, store_root: Path | None = None) -> dict[str, Any]:
    try:
        hypothesis = load_research_hypothesis(research_hypothesis_id, store_root=store_root)
    except FileNotFoundError:
        return {"ok": False, "read_only": True, "research_hypothesis_id": research_hypothesis_id, "error": "research_hypothesis_missing"}
    except Exception as exc:
        return _safe_empty_payload("research_hypothesis_unavailable", exc, research_hypothesis_id=research_hypothesis_id, research_hypothesis=None)
    return {"ok": True, "read_only": True, "research_hypothesis": hypothesis}

# Operator-first Research Console routes. These translate plain operator intents into
# read-only queue views and explicit append-only governance/research artifacts.
def research_lab_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_v1(store_root=store_root)


def research_lab_start_research_v1(payload: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_start_research_v1(payload, store_root=store_root)


def research_lab_hypothesis_queue_v1(*, status: str | None = None, store_root: Path | None = None, rebuild: bool = False) -> dict[str, Any]:
    return research_console_hypothesis_queue_v1(status=status, store_root=store_root, rebuild=rebuild)


def research_lab_console_dossier_v1(*, hypothesis_proposal_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_dossier_v1(hypothesis_proposal_id=hypothesis_proposal_id, store_root=store_root)


def research_lab_console_review_hypothesis_v1(*, hypothesis_proposal_id: str, payload: dict[str, Any], store_root: Path | None = None) -> dict[str, Any]:
    return research_console_review_hypothesis_v1(hypothesis_proposal_id, payload, store_root=store_root)


def research_lab_console_assess_hypothesis_v1(*, hypothesis_proposal_id: str, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_assess_hypothesis_v1(hypothesis_proposal_id, store_root=store_root)


def research_lab_console_convert_hypothesis_v1(*, hypothesis_proposal_id: str, payload: dict[str, Any], store_root: Path | None = None) -> dict[str, Any]:
    return research_console_convert_hypothesis_v1(hypothesis_proposal_id, payload, store_root=store_root)


def research_lab_research_plans_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_research_plans_v1(store_root=store_root)


def research_lab_paper_trials_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_paper_trials_v1(store_root=store_root)


def research_lab_sleeve_review_center_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_sleeve_review_center_v1(store_root=store_root)


def research_lab_blocked_evidence_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_blocked_evidence_v1(store_root=store_root)


def research_lab_research_backlog_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_backlog_v1(store_root=store_root)


def research_lab_explicit_action_placeholder_v1(*, action: str, entity_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return explicit_research_action_placeholder_v1(action, entity_id, payload)

def research_lab_operator_home_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_v1(store_root=store_root)


def research_lab_start_research_options_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_start_research_options_v1()


def research_lab_evidence_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_evidence_v1(store_root=store_root)


def research_lab_blocked_work_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_console_blocked_evidence_v1(store_root=store_root)



def research_lab_projection_health_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_projection_health(store_root=store_root)


def research_lab_rebuild_projections_v1(*, projection: str = "all", strict: bool = False, store_root: Path | None = None, actor: str = "AegisAPI") -> dict[str, Any]:
    return rebuild_research_projections(projection=projection, strict=strict, store_root=store_root, actor=actor)


def research_lab_validate_projections_v1(*, projection: str = "all", strict: bool = False, store_root: Path | None = None, actor: str = "AegisAPI") -> dict[str, Any]:
    return validate_research_projections(projection=projection, strict=strict, store_root=store_root, actor=actor)
