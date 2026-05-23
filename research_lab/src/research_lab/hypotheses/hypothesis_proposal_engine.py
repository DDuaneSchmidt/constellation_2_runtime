from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.integrity.research_store_integrity import latest_integrity_report
from research_lab.observations.observation_candidate import load_observation_candidate
from research_lab.observations.observation_cluster import load_observation_cluster
from research_lab.observations.observation_cluster_batch import latest_observation_cluster_batch, load_observation_cluster_batch
from research_lab.status.research_os_status import latest_research_os_status_report
from research_lab.storage.hashing import content_hash
from research_lab.storage.paths import ensure_store_layout
from research_lab.hypotheses.hypothesis_proposal import build_hypothesis_proposal


PROPOSAL_ENGINE_VERSION = "observation_to_hypothesis_proposal_engine.v1"
MARKET_FAMILIES = {"market_price_volume", "cross_sectional_behavior", "regime_behavior"}
SYSTEM_FAMILIES = {"evidence_quality", "research_store_integrity"}


def _blocker(code: str, message: str, *, source_artifact_id: str = "", severity: str = "ERROR", recoverable: bool = True) -> dict[str, Any]:
    return {
        "blocker_code": code,
        "blocker_message": message,
        "severity": severity,
        "recoverable": recoverable,
        "source_artifact_id": source_artifact_id,
    }


def _status_values(integrity: dict[str, Any] | None, research_status: dict[str, Any] | None) -> tuple[str, str]:
    integrity_status = str((integrity or {}).get("overall_status") or (research_status or {}).get("integrity_status") or "UNKNOWN")
    research_os_status = str((research_status or {}).get("overall_status") or "UNKNOWN")
    return integrity_status, research_os_status


def _source_hashes(cluster: dict[str, Any], batch: dict[str, Any], candidates: list[dict[str, Any]], integrity: dict[str, Any] | None, research_status: dict[str, Any] | None) -> dict[str, Any]:
    payload = {
        "source_observation_cluster_hash": cluster.get("immutable_hash") or content_hash(cluster, sort_lists=True),
        "source_observation_cluster_batch_hash": batch.get("immutable_hash") or content_hash(batch, sort_lists=True),
        "source_observation_candidate_hashes": {
            str(row.get("observation_candidate_id")): row.get("immutable_hash") or content_hash(row, sort_lists=True)
            for row in candidates
        },
        "latest_integrity_report_hash": (integrity or {}).get("immutable_hash") or (content_hash(integrity, sort_lists=True) if integrity else ""),
        "latest_research_os_status_report_hash": (research_status or {}).get("immutable_hash") or (content_hash(research_status, sort_lists=True) if research_status else ""),
    }
    return payload


def _test_design(test_type: str, *, cluster: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    symbol_ids = sorted({str(row.get("symbol_or_asset_id") or "") for row in candidates if row.get("symbol_or_asset_id")})
    return {
        "test_type": test_type,
        "required_data": _required_data_for_test(test_type),
        "minimum_observation_count": 3 if test_type in {"event_study", "longitudinal_backtest"} else 1,
        "event_window": "research_defined_after_human_review",
        "benchmark": "SPY" if test_type in {"event_study", "longitudinal_backtest", "regime_fragility_review"} else "not_applicable",
        "expected_artifacts_if_approved": _expected_artifacts_for_test(test_type),
        "source_cluster_family": cluster.get("cluster_family"),
        "source_cluster_status": cluster.get("cluster_status"),
        "symbol_or_asset_ids": symbol_ids,
    }


def _required_data_for_test(test_type: str) -> list[str]:
    if test_type == "integrity_resolution_review":
        return ["latest_integrity_report", "latest_research_os_status_report", "registry_lineage_findings"]
    if test_type == "evidence_quality_review":
        return ["challenger_evidence_batch", "challenger_comparison_report", "human_review_dossier", "integrity_report"]
    if test_type == "regime_fragility_review":
        return ["stability_reports", "regime_fragility_report", "longitudinal_outcome_index"]
    return ["local_ohlcv", "event_dates", "benchmark_returns", "research_store_integrity_pass_or_waiver"]


def _expected_artifacts_for_test(test_type: str) -> list[str]:
    if test_type == "integrity_resolution_review":
        return ["integrity_review_plan", "lineage_resolution_report"]
    if test_type == "evidence_quality_review":
        return ["evidence_quality_review_report", "artifact_lineage_review"]
    if test_type == "regime_fragility_review":
        return ["regime_fragility_review_report", "event_study_design"]
    if test_type == "longitudinal_backtest":
        return ["research_plan", "event_study", "backtest", "longitudinal_run"]
    return ["research_plan", "event_study"]


def _proposal_text(cluster: dict[str, Any], family: str) -> tuple[str, str, str, str, str, str]:
    label = str(cluster.get("cluster_label") or "Observation cluster")
    if family == "research_store_integrity":
        return (
            f"System research proposal: {label}",
            "Duplicate registry entries and legacy lineage warnings are recurring integrity blockers that should be resolved or explicitly waived before paper-trial eligibility.",
            "Which Research Store integrity blockers are recurring, and what governed remediation or waiver review is required before later lifecycle proposals?",
            "integrity_resolution_review",
            "not_applicable",
            "research_store",
        )
    if family == "evidence_quality":
        return (
            f"Evidence quality proposal: {label}",
            "Materialized challenger evidence chains show recurring evidence quality observations that should be classified before later proposal eligibility.",
            "Which challenger evidence quality observations are recurring, and do they indicate missing lineage, weak evidence, or acceptable legacy limitations?",
            "evidence_quality_review",
            "not_applicable",
            "research_store",
        )
    if family == "regime_behavior":
        return (
            f"Regime behavior proposal: {label}",
            "Regime behavior observations may warrant a formal fragility review if source stability evidence is complete and governance status is acceptable.",
            "Do recurring regime observations represent durable fragility or a temporary artifact of the current evidence sample?",
            "regime_fragility_review",
            "research_defined_after_human_review",
            "SPY",
        )
    return (
        f"Market behavior proposal: {label}",
        "Market or cross-sectional observations may warrant a formal event study only after data availability and Research OS integrity are acceptable.",
        "Do the clustered market observations recur under validated local market data and pass a formal research-only event study design?",
        "event_study",
        "research_defined_after_human_review",
        "SPY",
    )


def _classify_cluster(
    *,
    cluster: dict[str, Any],
    batch: dict[str, Any],
    candidates: list[dict[str, Any]],
    integrity: dict[str, Any] | None,
    research_status: dict[str, Any] | None,
    min_priority_score: float,
    generated_at: str,
) -> dict[str, Any]:
    family = str(cluster.get("cluster_family") or "unknown")
    cluster_status = str(cluster.get("cluster_status") or "unknown")
    priority = float(cluster.get("research_priority_score") or 0.0)
    integrity_status, research_os_status = _status_values(integrity, research_status)
    blockers = list(cluster.get("blockers") or [])
    eligibility_checks = {
        "cluster_priority_met": priority >= min_priority_score,
        "cluster_not_blocked": cluster_status != "blocked",
        "market_data_available": not any(item.get("blocker_code") == "market_data_unavailable" for item in blockers),
        "research_os_not_red": research_os_status != "RED",
        "integrity_not_fail": integrity_status != "FAIL",
        "source_cluster_family": family,
        "source_cluster_status": cluster_status,
    }
    proposal_family = {
        "research_store_integrity": "research_store_integrity",
        "evidence_quality": "evidence_quality",
        "regime_behavior": "regime_behavior",
        "market_price_volume": "market_behavior",
        "cross_sectional_behavior": "market_behavior",
        "governance_blocker": "governance_blocker",
    }.get(family, "market_behavior")
    proposal_status = "needs_more_observation"
    eligibility_status = "not_eligible"

    if priority < min_priority_score:
        blockers.append(_blocker("priority_below_threshold", f"Cluster priority {priority:.2f} is below threshold {min_priority_score:.2f}.", source_artifact_id=str(cluster.get("observation_cluster_id") or ""), severity="WARNING"))

    if family in SYSTEM_FAMILIES:
        proposal_status = "system_research_only"
        eligibility_status = "system_research_only"
    elif cluster_status == "blocked":
        proposal_status = "blocked"
        eligibility_status = "blocked"
        if family in MARKET_FAMILIES and not any(item.get("blocker_code") == "market_data_unavailable" for item in blockers):
            blockers.append(_blocker("cluster_blocked", "Source observation cluster is blocked.", source_artifact_id=str(cluster.get("observation_cluster_id") or "")))
    elif family in MARKET_FAMILIES:
        if research_os_status == "RED":
            blockers.append(_blocker("research_os_status_red", "Latest Research OS status is RED; market behavior proposals are blocked or require more observation.", source_artifact_id=str((research_status or {}).get("research_os_status_report_id") or "")))
        if integrity_status == "FAIL":
            blockers.append(_blocker("integrity_status_fail", "Latest integrity report is FAIL; market behavior proposals are not eligible.", source_artifact_id=str((integrity or {}).get("integrity_report_id") or "")))
        if blockers or research_os_status == "RED" or integrity_status == "FAIL":
            proposal_status = "needs_more_observation" if cluster_status != "blocked" else "blocked"
            eligibility_status = "blocked" if cluster_status == "blocked" else "not_eligible"
        elif priority >= min_priority_score:
            proposal_status = "proposed_for_review"
            eligibility_status = "eligible_for_review"
        else:
            proposal_status = "needs_more_observation"
            eligibility_status = "needs_more_observation"
    else:
        proposal_status = "needs_more_observation"
        eligibility_status = "needs_more_observation"

    label, statement, question, test_type, event_window, benchmark = _proposal_text(cluster, family)
    required_next_evidence = _required_next_evidence(proposal_status, family, blockers, test_type)
    source_ids = {
        "observation_cluster_id": cluster.get("observation_cluster_id"),
        "observation_cluster_batch_id": batch.get("observation_cluster_batch_id"),
        "observation_candidate_ids": [row.get("observation_candidate_id") for row in candidates],
        "latest_integrity_report_id": (integrity or {}).get("integrity_report_id") or cluster.get("latest_integrity_report_id"),
        "latest_research_os_status_report_id": (research_status or {}).get("research_os_status_report_id") or cluster.get("latest_research_os_status_report_id"),
    }
    return build_hypothesis_proposal(
        generated_at=generated_at,
        proposal_status=proposal_status,
        proposal_family=proposal_family,
        proposal_label=label,
        proposed_hypothesis_statement=statement,
        proposed_research_question=question,
        source_observation_cluster_id=str(cluster.get("observation_cluster_id") or ""),
        source_observation_cluster_batch_id=str(batch.get("observation_cluster_batch_id") or ""),
        source_observation_candidate_ids=[str(row.get("observation_candidate_id") or "") for row in candidates if row.get("observation_candidate_id")],
        cluster_family=family,
        cluster_status=cluster_status,
        research_priority_score=priority,
        eligibility_status=eligibility_status,
        eligibility_checks=eligibility_checks,
        blockers=blockers,
        required_next_evidence=required_next_evidence,
        proposed_test_design=_test_design(test_type, cluster=cluster, candidates=candidates),
        proposed_event_window=event_window,
        proposed_universe=str(next((row.get("universe_id") for row in candidates if row.get("universe_id")), "research_store")),
        proposed_benchmark=benchmark,
        latest_integrity_report_id=str(source_ids["latest_integrity_report_id"] or ""),
        latest_research_os_status_report_id=str(source_ids["latest_research_os_status_report_id"] or ""),
        source_artifact_ids=source_ids,
        source_artifact_hashes=_source_hashes(cluster, batch, candidates, integrity, research_status),
    )


def _required_next_evidence(proposal_status: str, family: str, blockers: list[dict[str, Any]], test_type: str) -> list[str]:
    if any(item.get("blocker_code") == "market_data_unavailable" for item in blockers):
        return ["local OHLCV source or market data adapter required before market anomaly proposals can advance."]
    if proposal_status == "system_research_only" and family == "research_store_integrity":
        return ["integrity finding review", "duplicate registry resolution or explicit governed waiver analysis", "legacy lineage classification"]
    if proposal_status == "system_research_only":
        return ["evidence lineage review", "challenger evidence completeness review", "human review dossier consistency review"]
    if proposal_status == "proposed_for_review":
        return _required_data_for_design_name(test_type)
    return ["additional observation recurrence", "validated source artifact lineage", "non-RED Research OS status for market behavior proposals"]


def _required_data_for_design_name(test_type: str) -> list[str]:
    if test_type == "regime_fragility_review":
        return ["source stability artifacts", "regime fragility report", "longitudinal candidate outcomes"]
    if test_type == "event_study":
        return ["validated local OHLCV", "event definitions", "benchmark returns"]
    return ["source artifact lineage", "research governance review"]


def generate_hypothesis_proposals(
    *,
    store_root: Path | None = None,
    observation_cluster_batch_id: str | None = None,
    min_priority_score: float = 0.60,
    max_proposals: int = 25,
    generated_at: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    store = ensure_store_layout(store_root)
    batch = load_observation_cluster_batch(observation_cluster_batch_id, store_root=store) if observation_cluster_batch_id else latest_observation_cluster_batch(store_root=store)
    if not batch:
        raise FileNotFoundError("observation_cluster_batch_missing")
    integrity = latest_integrity_report(store_root=store)
    research_status = latest_research_os_status_report(store_root=store)
    proposals: list[dict[str, Any]] = []
    for cluster_id in list(batch.get("observation_cluster_ids") or [])[:max_proposals]:
        cluster = load_observation_cluster(str(cluster_id), store_root=store)
        candidates = [load_observation_candidate(str(candidate_id), store_root=store) for candidate_id in cluster.get("observation_candidate_ids") or []]
        proposals.append(
            _classify_cluster(
                cluster=cluster,
                batch=batch,
                candidates=candidates,
                integrity=integrity,
                research_status=research_status,
                min_priority_score=min_priority_score,
                generated_at=generated_at,
            )
        )
    proposals = sorted(proposals, key=lambda row: (-float(row.get("research_priority_score") or 0.0), str(row.get("proposal_family") or ""), str(row.get("source_observation_cluster_id") or "")))
    return proposals, batch, integrity, research_status

