from __future__ import annotations

from pathlib import Path
from typing import Any

from ..meta_governance.api import _store
from .adaptation_artifacts import bundle_id_for, write_bundle, write_records
from .adaptation_audit import emit_adaptation_event
from .adaptation_recommendations import build_recommendations
from .drift_detection import detect_drift_signals as _detect_drift_signals
from .impact_prioritization import assess_impacts as _assess_impacts
from .issue_ranking import rank_issues as _rank_issues
from .operator_summary import build_summary
from .proposal_candidates import build_proposal_candidates
from .regime_detection import detect_regime_signals as _detect_regime_signals
from .schemas import utc_now


ADAPTATION_SOURCE_KINDS = (
    "expectation_records",
    "realized_validation_results",
    "reconciliation_results",
    "drift_signals",
    "regime_signals",
    "impact_assessments",
    "ranked_issues",
    "adaptation_recommendations",
    "proposal_candidates",
    "operator_summaries",
    "verification_bundles",
    "reconciliation_bundles",
)


def _created_at_for_refs(store, refs: tuple[str, ...], fallback: str | None = None) -> str:
    timestamps: list[str] = []
    for ref in refs:
        for kind in ADAPTATION_SOURCE_KINDS:
            if store.exists(kind, ref):
                timestamps.append(store.read(kind, ref)["created_at"])
                break
    if timestamps:
        return max(timestamps)
    return fallback or utc_now()


def detect_drift_signals(
    *,
    realized_validation_refs: tuple[str, ...] = (),
    reconciliation_result_refs: tuple[str, ...] = (),
    thresholds: dict[str, Any],
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = _detect_drift_signals(
        store,
        realized_validation_refs=realized_validation_refs,
        reconciliation_result_refs=reconciliation_result_refs,
        thresholds=thresholds,
    )
    refs = write_records(
        store,
        kind="drift_signals",
        records=records,
        artifact_type="DriftSignal",
        id_field="drift_signal_id",
        created_at=_created_at_for_refs(store, tuple(sorted(realized_validation_refs + reconciliation_result_refs))),
    )
    for ref in refs:
        emit_adaptation_event(store, "drift_signal_detected", "system", artifact_refs=(ref,))
    return refs


def detect_regime_signals(
    *,
    drift_signal_refs: tuple[str, ...] = (),
    reconciliation_result_refs: tuple[str, ...] = (),
    requested_regime_families: tuple[str, ...],
    thresholds: dict[str, Any],
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = _detect_regime_signals(
        store,
        drift_signal_refs=drift_signal_refs,
        reconciliation_result_refs=reconciliation_result_refs,
        requested_regime_families=requested_regime_families,
        thresholds=thresholds,
    )
    refs = write_records(
        store,
        kind="regime_signals",
        records=records,
        artifact_type="RegimeSignal",
        id_field="regime_signal_id",
        created_at=_created_at_for_refs(store, tuple(sorted(drift_signal_refs + reconciliation_result_refs))),
    )
    for ref in refs:
        emit_adaptation_event(store, "regime_signal_detected", "system", artifact_refs=(ref,))
    return refs


def assess_impacts(
    *,
    source_refs: tuple[str, ...],
    thresholds: dict[str, Any],
    exposure_overrides: dict[str, dict[str, float]] | None = None,
    require_explicit_exposure: bool = False,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = _assess_impacts(
        store,
        source_refs=source_refs,
        thresholds=thresholds,
        exposure_overrides=exposure_overrides,
        require_explicit_exposure=require_explicit_exposure,
    )
    refs = write_records(
        store,
        kind="impact_assessments",
        records=records,
        artifact_type="ImpactAssessment",
        id_field="impact_assessment_id",
        created_at=_created_at_for_refs(store, source_refs),
    )
    for ref in refs:
        emit_adaptation_event(store, "impact_assessment_created", "system", artifact_refs=(ref,))
    return refs


def rank_adaptive_issues(
    *,
    impact_assessment_refs: tuple[str, ...],
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = _rank_issues(store, impact_assessment_refs=impact_assessment_refs)
    refs = write_records(
        store,
        kind="ranked_issues",
        records=records,
        artifact_type="RankedIssue",
        id_field="ranked_issue_id",
        created_at=_created_at_for_refs(store, impact_assessment_refs),
    )
    for ref in refs:
        emit_adaptation_event(store, "issue_ranked", "system", artifact_refs=(ref,))
    return refs


def generate_adaptation_recommendations(
    *,
    ranked_issue_refs: tuple[str, ...],
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = build_recommendations(store, ranked_issue_refs=ranked_issue_refs)
    refs = write_records(
        store,
        kind="adaptation_recommendations",
        records=records,
        artifact_type="AdaptationRecommendation",
        id_field="recommendation_id",
        created_at=_created_at_for_refs(store, ranked_issue_refs),
    )
    for ref in refs:
        emit_adaptation_event(store, "adaptation_recommendation_created", "system", artifact_refs=(ref,))
    return refs


def generate_proposal_candidates(
    *,
    recommendation_refs: tuple[str, ...],
    adaptation_bundle_ref: str,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = build_proposal_candidates(store, recommendation_refs=recommendation_refs, adaptation_bundle_ref=adaptation_bundle_ref)
    refs = write_records(
        store,
        kind="proposal_candidates",
        records=records,
        artifact_type="ProposalCandidate",
        id_field="proposal_candidate_id",
        created_at=_created_at_for_refs(store, recommendation_refs),
    )
    for ref in refs:
        emit_adaptation_event(store, "proposal_candidate_created", "system", artifact_refs=(ref,))
    return refs


def build_operator_summary(
    *,
    ranked_issue_refs: tuple[str, ...],
    drift_signal_refs: tuple[str, ...],
    regime_signal_refs: tuple[str, ...],
    recommendation_refs: tuple[str, ...],
    proposal_candidate_refs: tuple[str, ...],
    as_of: str | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    record = build_summary(
        as_of=as_of or utc_now(),
        ranked_issue_refs=ranked_issue_refs,
        drift_signal_refs=drift_signal_refs,
        regime_signal_refs=regime_signal_refs,
        recommendation_refs=recommendation_refs,
        proposal_candidate_refs=proposal_candidate_refs,
    )
    summary_created_at = record.as_of or _created_at_for_refs(store, tuple(sorted(ranked_issue_refs + recommendation_refs + proposal_candidate_refs)))
    store.write_immutable("operator_summaries", record.summary_id, record, artifact_type="OperatorSummary", created_at=summary_created_at)
    emit_adaptation_event(store, "operator_summary_created", "system", artifact_refs=(record.summary_id,))
    return record.summary_id


def create_adaptation_bundle(
    *,
    drift_signal_refs: tuple[str, ...],
    regime_signal_refs: tuple[str, ...],
    impact_assessment_refs: tuple[str, ...],
    ranked_issue_refs: tuple[str, ...],
    recommendation_refs: tuple[str, ...],
    proposal_candidate_refs: tuple[str, ...],
    operator_summary_ref: str,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    bundle_id = write_bundle(
        store,
        drift_signal_refs=drift_signal_refs,
        regime_signal_refs=regime_signal_refs,
        impact_assessment_refs=impact_assessment_refs,
        ranked_issue_refs=ranked_issue_refs,
        recommendation_refs=recommendation_refs,
        proposal_candidate_refs=proposal_candidate_refs,
        operator_summary_ref=operator_summary_ref,
        created_at=_created_at_for_refs(
            store,
            tuple(
                sorted(
                    drift_signal_refs
                    + regime_signal_refs
                    + impact_assessment_refs
                    + ranked_issue_refs
                    + recommendation_refs
                    + proposal_candidate_refs
                    + (operator_summary_ref,)
                )
            ),
        ),
    )
    emit_adaptation_event(store, "adaptation_bundle_created", "system", artifact_refs=(bundle_id, operator_summary_ref))
    return bundle_id


def run_adaptive_cycle(
    *,
    realized_validation_refs: tuple[str, ...] = (),
    reconciliation_result_refs: tuple[str, ...] = (),
    requested_regime_families: tuple[str, ...],
    drift_thresholds: dict[str, Any],
    regime_thresholds: dict[str, Any],
    impact_thresholds: dict[str, Any],
    exposure_overrides: dict[str, dict[str, float]] | None = None,
    require_explicit_exposure: bool = False,
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    drift_refs = detect_drift_signals(
        realized_validation_refs=realized_validation_refs,
        reconciliation_result_refs=reconciliation_result_refs,
        thresholds=drift_thresholds,
        store_root=store_root,
    )
    regime_refs = detect_regime_signals(
        drift_signal_refs=drift_refs,
        reconciliation_result_refs=reconciliation_result_refs,
        requested_regime_families=requested_regime_families,
        thresholds=regime_thresholds,
        store_root=store_root,
    )
    impact_refs = assess_impacts(
        source_refs=tuple(sorted(set(drift_refs + regime_refs + reconciliation_result_refs))),
        thresholds=impact_thresholds,
        exposure_overrides=exposure_overrides,
        require_explicit_exposure=require_explicit_exposure,
        store_root=store_root,
    )
    ranked_issue_refs = rank_adaptive_issues(
        impact_assessment_refs=impact_refs,
        store_root=store_root,
    )
    recommendation_refs = generate_adaptation_recommendations(
        ranked_issue_refs=ranked_issue_refs,
        store_root=store_root,
    )
    adaptation_bundle_ref = bundle_id_for(
        drift_signal_refs=drift_refs,
        regime_signal_refs=regime_refs,
        impact_assessment_refs=impact_refs,
        ranked_issue_refs=ranked_issue_refs,
        recommendation_refs=recommendation_refs,
        operator_summary_ref="pending",
    )
    proposal_candidate_refs = generate_proposal_candidates(
        recommendation_refs=recommendation_refs,
        adaptation_bundle_ref=adaptation_bundle_ref,
        store_root=store_root,
    )
    operator_summary_ref = build_operator_summary(
        ranked_issue_refs=ranked_issue_refs,
        drift_signal_refs=drift_refs,
        regime_signal_refs=regime_refs,
        recommendation_refs=recommendation_refs,
        proposal_candidate_refs=proposal_candidate_refs,
        store_root=store_root,
    )
    adaptation_bundle_ref = bundle_id_for(
        drift_signal_refs=drift_refs,
        regime_signal_refs=regime_refs,
        impact_assessment_refs=impact_refs,
        ranked_issue_refs=ranked_issue_refs,
        recommendation_refs=recommendation_refs,
        operator_summary_ref=operator_summary_ref,
    )
    if proposal_candidate_refs:
        store = _store(store_root)
        for candidate_ref in proposal_candidate_refs:
            candidate = store.read("proposal_candidates", candidate_ref)["record"]
            if adaptation_bundle_ref not in candidate["evidence_bundle_refs"]:
                raise ValueError("ADAPTATION_BUNDLE_LINK_MISSING")
    bundle_ref = create_adaptation_bundle(
        drift_signal_refs=drift_refs,
        regime_signal_refs=regime_refs,
        impact_assessment_refs=impact_refs,
        ranked_issue_refs=ranked_issue_refs,
        recommendation_refs=recommendation_refs,
        proposal_candidate_refs=proposal_candidate_refs,
        operator_summary_ref=operator_summary_ref,
        store_root=store_root,
    )
    return {
        "drift_signal_refs": drift_refs,
        "regime_signal_refs": regime_refs,
        "impact_assessment_refs": impact_refs,
        "ranked_issue_refs": ranked_issue_refs,
        "recommendation_refs": recommendation_refs,
        "proposal_candidate_refs": proposal_candidate_refs,
        "operator_summary_ref": operator_summary_ref,
        "bundle_ref": bundle_ref,
    }


def find_adaptation_bundles(
    *,
    snapshot_id: str | None = None,
    store_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    store = _store(store_root)
    if snapshot_id is None:
        return [store.read("adaptation_bundles", bundle_id) for bundle_id in store.list_ids("adaptation_bundles")]
    expectation_refs = {
        expectation_id
        for expectation_id in store.list_ids("expectation_records")
        if store.read("expectation_records", expectation_id)["record"]["snapshot_id"] == snapshot_id
    }
    realized_refs = {
        result_id
        for result_id in store.list_ids("realized_validation_results")
        if store.read("realized_validation_results", result_id)["record"]["expectation_id"] in expectation_refs
    }
    known_refs = expectation_refs | realized_refs | {snapshot_id}
    bundles: list[dict[str, Any]] = []
    for bundle_id in store.list_ids("adaptation_bundles"):
        bundle = store.read("adaptation_bundles", bundle_id)
        related = False
        for drift_ref in bundle["record"]["drift_signal_refs"]:
            if set(store.read("drift_signals", drift_ref)["record"]["source_refs"]) & known_refs:
                related = True
                break
        if not related:
            for regime_ref in bundle["record"]["regime_signal_refs"]:
                if set(store.read("regime_signals", regime_ref)["record"]["source_refs"]) & known_refs:
                    related = True
                    break
        if not related:
            for impact_ref in bundle["record"]["impact_assessment_refs"]:
                if set(store.read("impact_assessments", impact_ref)["record"]["source_refs"]) & known_refs:
                    related = True
                    break
        if not related:
            for issue_ref in bundle["record"]["ranked_issue_refs"]:
                if set(store.read("ranked_issues", issue_ref)["record"]["source_refs"]) & known_refs:
                    related = True
                    break
        if not related:
            for recommendation_ref in bundle["record"]["recommendation_refs"]:
                if set(store.read("adaptation_recommendations", recommendation_ref)["record"]["source_refs"]) & known_refs:
                    related = True
                    break
        if not related:
            for candidate_ref in bundle["record"]["proposal_candidate_refs"]:
                candidate = store.read("proposal_candidates", candidate_ref)["record"]
                if set(candidate["source_refs"]) & known_refs or set(candidate["evidence_bundle_refs"]) & known_refs:
                    related = True
                    break
        if related:
            bundles.append(bundle)
    return bundles
