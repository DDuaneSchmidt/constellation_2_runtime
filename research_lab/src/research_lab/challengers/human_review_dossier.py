from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.challengers.challenger_comparison import load_challenger_comparison_report
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


GENERATED_AT = "1970-01-01T00:00:00Z"
RESEARCH_LABEL = "RESEARCH_ONLY"
SCHEMA_VERSION = "human_review_dossier.v1"
DOSSIER_TYPE = "CHALLENGER_REVIEW"

ALLOWED_HUMAN_DECISION_OPTIONS = [
    "continue_observation",
    "request_more_challenger_evidence",
    "reject_challenger",
    "open_challenger_paper_trial",
    "investigate_incumbent_degradation",
    "archive_challenger_track",
    "prepare_manual_governance_review",
]


def _registry_path(store: Path) -> Path:
    return store / "registries" / "human_review_dossiers.jsonl"


def _dossier_dir(store: Path) -> Path:
    return store / "human_review_dossiers"


def _dossier_path(store: Path, dossier_id: str) -> Path:
    return _dossier_dir(store) / f"{dossier_id}.json"


def _active_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        row
        for row in report.get("comparison_table") or []
        if row.get("entity_type") == "challenger" and row.get("exclusion_status") == "included_for_human_review"
    ]


def _excluded_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        row
        for row in report.get("comparison_table") or []
        if row.get("entity_type") != "incumbent" and row.get("exclusion_status") == "excluded"
    ]


def _summary_by_id(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("challenger_hypothesis_id")): row for row in report.get("challenger_summaries") or []}


def _rank_by_id(report: dict[str, Any]) -> dict[str, int]:
    return {str(row.get("challenger_hypothesis_id")): int(row.get("rank") or 0) for row in report.get("deterministic_rank_order") or []}


def _source_report_ids(report: dict[str, Any]) -> dict[str, Any]:
    incumbent = report.get("source_incumbent_evidence_ids") or {}
    return {
        "challenger_comparison_report_id": report.get("challenger_comparison_report_id"),
        "challenger_track_id": report.get("challenger_track_id"),
        "challenger_evidence_batch_id": report.get("challenger_evidence_batch_id"),
        "event_study_evidence_id": incumbent.get("event_study_evidence_id"),
        "backtest_evidence_id": incumbent.get("backtest_evidence_id"),
        "longitudinal_run_id": incumbent.get("longitudinal_run_id"),
        "expectancy_drift_report_id": incumbent.get("expectancy_drift_report_id"),
        "regime_fragility_report_id": incumbent.get("regime_fragility_report_id"),
        "sleeve_stability_report_id": incumbent.get("sleeve_stability_report_id"),
    }


def _recommended_action(report: dict[str, Any], active: list[dict[str, Any]]) -> str:
    sufficiency = str(report.get("evidence_sufficiency") or "")
    usable = [row for row in active if row.get("evidence_quality") in {"strong", "moderate"}]
    if sufficiency == "comparison_ready" and usable:
        return "open_challenger_paper_trial"
    if sufficiency == "partially_ready":
        return "request_more_challenger_evidence"
    if sufficiency == "insufficient":
        return "continue_observation"
    if sufficiency == "blocked":
        return "investigate_incumbent_degradation"
    return "prepare_manual_governance_review"


def _executive_summary(report: dict[str, Any], active: list[dict[str, Any]], excluded: list[dict[str, Any]]) -> dict[str, Any]:
    incumbent = report.get("incumbent_summary") or {}
    rank = report.get("deterministic_rank_order") or []
    return {
        "incumbent_stability_status": incumbent.get("overall_stability_status"),
        "expectancy_drift_status": incumbent.get("drift_status"),
        "regime_fragility_status": incumbent.get("regime_fragility_status"),
        "comparison_sufficiency": report.get("evidence_sufficiency"),
        "active_challenger_count": len(active),
        "excluded_challenger_count": len(excluded),
        "top_research_review_candidate_id": rank[0].get("challenger_hypothesis_id") if rank else "",
        "reason_this_dossier_exists": "Incumbent sleeve stability is degrading and challenger comparison is ready for human review.",
        "lifecycle_action_authorized": False,
        "authorization_statement": "No promotion, retirement, mutation, or trading lifecycle action is authorized by this dossier.",
    }


def _incumbent_context(report: dict[str, Any]) -> dict[str, Any]:
    incumbent = report.get("incumbent_summary") or {}
    sources = report.get("source_incumbent_evidence_ids") or {}
    return {
        "sleeve_id": report.get("incumbent_sleeve_id"),
        "sleeve_version_id": report.get("incumbent_sleeve_version_id"),
        "event_study_evidence_id": sources.get("event_study_evidence_id"),
        "backtest_evidence_id": sources.get("backtest_evidence_id"),
        "longitudinal_run_id": sources.get("longitudinal_run_id"),
        "expectancy_drift_report_id": sources.get("expectancy_drift_report_id"),
        "regime_fragility_report_id": sources.get("regime_fragility_report_id"),
        "sleeve_stability_report_id": sources.get("sleeve_stability_report_id"),
        "current_governance_posture": "degrading_incumbent_under_research_review",
        "why_incumbent_is_not_automatically_retired": "The evidence is research-only and requires human governance assessment before any lifecycle decision.",
        "evidence_quality": incumbent.get("evidence_quality"),
        "observation_count": incumbent.get("observation_count"),
    }


def _challenger_context(report: dict[str, Any], active: list[dict[str, Any]], excluded: list[dict[str, Any]]) -> dict[str, Any]:
    summaries = report.get("challenger_summaries") or []
    return {
        "challenger_track_id": report.get("challenger_track_id"),
        "challenger_evidence_batch_id": report.get("challenger_evidence_batch_id"),
        "challenger_comparison_report_id": report.get("challenger_comparison_report_id"),
        "hypothesis_count": len(summaries),
        "generated_challenger_count": sum(1 for row in summaries if row.get("status") == "generated"),
        "blocked_challenger_count": sum(1 for row in summaries if row.get("status") == "blocked"),
        "active_challenger_count": len(active),
        "excluded_challenger_count": len(excluded),
        "deterministic_rank_order": report.get("deterministic_rank_order") or [],
        "ranking_method_label": "RULE_BASED_RESEARCH_REVIEW_ORDER",
        "optimizer_disclaimer": "NOT_AN_OPTIMIZER",
        "evidence_completeness": {
            "active_challengers_complete": sum(
                1
                for row in summaries
                if row.get("status") == "generated" and (row.get("evidence_completeness") or {}).get("complete") is True
            ),
            "missing_evidence_warnings": [
                {
                    "challenger_hypothesis_id": row.get("challenger_hypothesis_id"),
                    "missing": (row.get("evidence_completeness") or {}).get("missing_fields") or row.get("missing_evidence_flags") or [],
                }
                for row in summaries
                if (row.get("evidence_completeness") or {}).get("complete") is False or row.get("missing_evidence_flags")
            ],
        },
    }


def _review_candidates(report: dict[str, Any], active: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries = _summary_by_id(report)
    rank = _rank_by_id(report)
    result = []
    for row in sorted(active, key=lambda item: rank.get(str(item.get("entity_id")), 9999)):
        summary = summaries.get(str(row.get("entity_id")), {})
        result.append(
            {
                "challenger_hypothesis_id": row.get("entity_id"),
                "variant_name": row.get("variant_name"),
                "deterministic_rule_delta": summary.get("deterministic_rule_delta"),
                "evidence_quality": row.get("evidence_quality"),
                "comparison_rank": rank.get(str(row.get("entity_id"))),
                "post_cost_expectancy": row.get("post_cost_expectancy"),
                "expectancy_delta_vs_incumbent": row.get("expectancy_delta_vs_incumbent"),
                "win_rate_delta_vs_incumbent": row.get("win_rate_delta_vs_incumbent"),
                "downside_tail_comparison": row.get("downside_tail_metric"),
                "drift_status": row.get("drift_status"),
                "regime_fragility_status": row.get("regime_fragility_status"),
                "observation_count": row.get("observation_count"),
                "supporting_evidence_ids": [
                    item
                    for item in [
                        summary.get("challenger_variant_id"),
                        summary.get("event_study_evidence_id"),
                        summary.get("backtest_evidence_id"),
                        summary.get("longitudinal_run_id"),
                        summary.get("expectancy_drift_report_id"),
                        summary.get("regime_fragility_report_id"),
                        summary.get("sleeve_stability_report_id"),
                    ]
                    if item
                ],
                "artifact_lineage": summary.get("artifact_lineage") or {},
                "evidence_completeness": summary.get("evidence_completeness") or {},
                "confidence_classification": "evidence_backed" if (summary.get("evidence_completeness") or {}).get("complete") else "incomplete_evidence",
                "missing_evidence_warnings": summary.get("missing_evidence_flags") or [],
                "review_notes": "Candidate for review only; requires human assessment before any future lifecycle action.",
                "required_human_questions": [
                    "Is the observed improvement economically explainable?",
                    "Does this variant reduce fragility or merely shift it?",
                    "Is additional paper-trial observation required before a governance decision?",
                ],
            }
        )
    return result


def _blocked_or_excluded(report: dict[str, Any], excluded: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries = _summary_by_id(report)
    exclusion_by_id = {str(row.get("entity_id")): row for row in report.get("exclusion_results") or []}
    result = []
    for row in excluded:
        summary = summaries.get(str(row.get("entity_id")), {})
        exclusion = exclusion_by_id.get(str(row.get("entity_id")), {})
        reason = summary.get("failure_reason") or "; ".join(str(item) for item in exclusion.get("reasons", []))
        result.append(
            {
                "challenger_hypothesis_id": row.get("entity_id"),
                "variant_name": row.get("variant_name"),
                "status": summary.get("status") or "excluded",
                "exclusion_reason": reason,
                "failure_reason": summary.get("failure_reason") or "",
                "recoverable": reason == "cost_sensitivity_requires_supported_cost_model_snapshot",
                "required_remediation": "Create a supported deterministic cost model snapshot before retesting." if reason == "cost_sensitivity_requires_supported_cost_model_snapshot" else "Review missing or insufficient evidence before retesting.",
                "artifact_lineage": summary.get("artifact_lineage") or {},
                "missing_evidence_warnings": summary.get("missing_evidence_flags") or [],
            }
        )
    return result


def _key_findings(report: dict[str, Any]) -> list[str]:
    rank = report.get("deterministic_rank_order") or []
    incumbent = report.get("incumbent_summary") or {}
    return [
        f"comparison_sufficiency={report.get('evidence_sufficiency')}",
        f"incumbent_stability_status={incumbent.get('overall_stability_status')}",
        f"incumbent_drift_status={incumbent.get('drift_status')}",
        f"top_review_candidate={rank[0].get('challenger_hypothesis_id') if rank else ''}",
        "No selection is declared; this dossier prepares human review only.",
    ]


def _open_questions() -> list[str]:
    return [
        "Is the longer holding period economically explainable?",
        "Is the challenger improvement robust across regimes?",
        "Is the observed incumbent degradation temporary or structural?",
        "Is paper-trial extension required before lifecycle decision?",
        "Are transaction costs adequately represented?",
        "Does the challenger reduce fragility or merely shift it?",
    ]


def _risk_and_limitations() -> list[str]:
    return [
        "Evidence is historical and research-only.",
        "Backtest underperformed SPY for incumbent.",
        "Challenger evidence does not imply live viability.",
        "Regime coverage may be incomplete.",
        "Cost model limitations remain material.",
        "Observation count limitations may affect reliability.",
        "No guarantee of future performance.",
        "No trading authorization is created.",
    ]


def _governance_constraints() -> dict[str, bool]:
    return {
        "research_only": True,
        "advisory_only": True,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_trading_allowed": False,
        "order_management_allowed": False,
        "portfolio_optimizer_allowed": False,
        "ml_ranking_allowed": False,
        "capital_allocation_allowed": False,
        "automatic_promotion_allowed": False,
        "automatic_retirement_allowed": False,
        "sleeve_mutation_allowed": False,
        "paper_trial_mutation_allowed": False,
        "candidate_ledger_mutation_allowed": False,
        "human_review_required": True,
        "dossier_uses_materialized_challenger_evidence": True,
    }


def build_human_review_dossier(
    *,
    challenger_comparison_report_id: str,
    store_root: Path | None = None,
    generated_at: str = GENERATED_AT,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = load_challenger_comparison_report(challenger_comparison_report_id, store_root=store)
    active = _active_rows(report)
    excluded = _excluded_rows(report)
    recommended = _recommended_action(report, active)
    payload = {
        "human_review_dossier_id": "",
        "dossier_type": DOSSIER_TYPE,
        "incumbent_sleeve_id": report.get("incumbent_sleeve_id"),
        "incumbent_sleeve_version_id": report.get("incumbent_sleeve_version_id"),
        "challenger_track_id": report.get("challenger_track_id"),
        "challenger_evidence_batch_id": report.get("challenger_evidence_batch_id"),
        "challenger_comparison_report_id": report.get("challenger_comparison_report_id"),
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "source_report_ids": _source_report_ids(report),
        "executive_summary": _executive_summary(report, active, excluded),
        "incumbent_context": _incumbent_context(report),
        "challenger_context": _challenger_context(report, active, excluded),
        "key_evidence_findings": _key_findings(report),
        "review_candidates": _review_candidates(report, active),
        "blocked_or_excluded_items": _blocked_or_excluded(report, excluded),
        "open_questions": _open_questions(),
        "risk_and_limitations": _risk_and_limitations(),
        "governance_constraints": _governance_constraints(),
        "required_human_decision_options": ALLOWED_HUMAN_DECISION_OPTIONS,
        "recommended_next_action": recommended,
        "audit_refs": {
            "challenger_comparison_report_id": challenger_comparison_report_id,
            "source_comparison_content_hash": report.get("content_hash"),
            "append_only_registry": "human_review_dossiers.jsonl",
            "audit_action": "human_review_dossier_created",
        },
        "content_hash": "",
    }
    seed = content_hash(payload, exclude={"human_review_dossier_id", "content_hash", "generated_at"}, sort_lists=True)
    payload["human_review_dossier_id"] = f"hrd_{short_hash(seed, 16)}"
    payload["content_hash"] = content_hash(payload, exclude={"generated_at"}, sort_lists=True)
    validate_contract("human_review_dossier", payload)
    return payload


def write_human_review_dossier(
    *,
    challenger_comparison_report_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    dossier = build_human_review_dossier(challenger_comparison_report_id=challenger_comparison_report_id, store_root=store)
    path = _dossier_path(store, dossier["human_review_dossier_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable human review dossier: {path}")
    write_json(path, dossier, overwrite=False)
    row = {
        "human_review_dossier_id": dossier["human_review_dossier_id"],
        "dossier_type": dossier["dossier_type"],
        "incumbent_sleeve_id": dossier["incumbent_sleeve_id"],
        "challenger_track_id": dossier["challenger_track_id"],
        "challenger_comparison_report_id": dossier["challenger_comparison_report_id"],
        "top_research_review_candidate_id": dossier["executive_summary"]["top_research_review_candidate_id"],
        "active_challenger_count": dossier["executive_summary"]["active_challenger_count"],
        "excluded_challenger_count": dossier["executive_summary"]["excluded_challenger_count"],
        "recommended_next_action": dossier["recommended_next_action"],
        "research_label": dossier["research_label"],
        "content_hash": dossier["content_hash"],
        "generated_at": dossier["generated_at"],
        "schema_version": dossier["schema_version"],
    }
    append_jsonl(_registry_path(store), row)
    rows = read_jsonl(_registry_path(store))
    if not rows or rows[-1] != row:
        raise RuntimeError("human review dossier registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="human_review_dossier",
        entity_id=dossier["human_review_dossier_id"],
        action="human_review_dossier_created",
        new_state_hash=dossier["content_hash"],
        reason="Generated deterministic research-only human review dossier.",
        metadata={"registry_row": row, "governance_constraints": dossier["governance_constraints"]},
        store_root=store,
    )
    return {"dossier": dossier, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_human_review_dossier(human_review_dossier_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_dossier_path(store, human_review_dossier_id))


def list_human_review_dossiers(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))
