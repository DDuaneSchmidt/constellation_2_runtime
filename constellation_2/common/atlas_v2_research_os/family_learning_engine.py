from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .family_learning_governance import family_learning_authority_boundary, validate_family_learning_allowed
from .family_learning_models import ALLOWED_FAMILY_LEARNING_ACTIONS, FamilyLearningUpdate
from .family_observation_log import CSV_FIELDS

REPORT_DIRNAME = "family_learning"

SUPPORTING_OUTCOMES = {"supporting", "supported", "observed"}
INVALIDATING_OUTCOMES = {"invalidating", "invalidated"}
NEUTRAL_OUTCOMES = {"neutral", "no_setup", "inconclusive", ""}
NEEDS_DATA_OUTCOMES = {"needs_data", "need_data", "data_blocked"}
RETIRE_OUTCOMES = {"retire", "retired"}


def run_family_learning(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    from .family_learning_reports import write_family_learning_report

    report = build_family_learning_report(root=root, created_at=created_at)
    write_family_learning_report(report, root=root)
    return report


def build_family_learning_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    family_index = _family_index(sources)
    observation_stats = _observation_stats(root_path / "family_observation_log" / "latest_template.csv")
    paper_plan_stats = _paper_plan_stats(sources["family_paper_forward_observation"]["payload"])
    updates = [
        _build_update(family, observation_stats.get(family_id, _empty_stats()), paper_plan_stats.get(family_id, {}), sources)
        for family_id, family in sorted(family_index.items(), key=lambda item: _family_sort_key(item[1]))
    ]
    payload = {
        "schema_id": "atlas_v2_research_os_family_learning_report_v1",
        "schema_version": "1.0",
        "report_type": "FAMILY_LEARNING_REPORT",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        }
        | {
            "family_observation_log": {
                "path": str(root_path / "family_observation_log" / "latest_template.csv"),
                "exists": (root_path / "family_observation_log" / "latest_template.csv").exists(),
                "report_type": "FAMILY_OBSERVATION_LOG_CSV",
            }
        },
        "summary": _summary(updates),
        "family_learning_updates": [update.to_dict() for update in updates],
        "methodology": {
            "confidence_before": "Prior confidence comes from robustness classification, candidate-family classification, edge classification, and portfolio relevance context; it is bounded to [0, 1].",
            "confidence_after": "Manual family observations update confidence by +0.04 for support and -0.06 for invalidation, with direct-data blockers preventing confidence upgrade status.",
            "neutral_observations": "Neutral observations count logged rows that are neither supporting nor invalidating and are not direct-data or retirement signals.",
            "no_template_evidence": "Blank family_observation_log template rows are not counted as observations.",
        },
        "allowed_actions": sorted(ALLOWED_FAMILY_LEARNING_ACTIONS),
        "artifact_types": ["FamilyLearningReport", "ResearchMemoryUpdateProposal", "ObservationRecommendation"],
        "authority_boundary": family_learning_authority_boundary(),
        "guardrails": [
            "Research-memory and belief-update infrastructure only.",
            "No trade recommendation.",
            "No capital allocation.",
            "No position sizing.",
            "No broker execution.",
            "No automatic paper placement.",
            "No candidate production promotion.",
        ],
    }
    validate_family_learning_allowed(payload)
    return payload


def _build_update(
    family: dict[str, Any],
    stats: dict[str, Any],
    paper_plan: dict[str, Any],
    sources: dict[str, dict[str, Any]],
) -> FamilyLearningUpdate:
    before = _prior_confidence(family, sources)
    observations = int(stats["observations"])
    supporting = int(stats["supporting"])
    invalidating = int(stats["invalidating"])
    neutral = max(0, observations - supporting - invalidating - int(stats["needs_data"]) - int(stats["retire"]))
    direct_data_required = _direct_data_required(family)
    after = _clamp(before + 0.04 * supporting - 0.06 * invalidating)
    delta = round(after - before, 4)
    status, reason, next_evidence = _status_reason_next(
        family=family,
        observations=observations,
        supporting=supporting,
        invalidating=invalidating,
        neutral=neutral,
        needs_data=int(stats["needs_data"]),
        retire=int(stats["retire"]),
        direct_data_required=direct_data_required,
        delta=delta,
        paper_plan=paper_plan,
    )
    return FamilyLearningUpdate(
        family_id=str(family.get("family_id") or ""),
        family_name=str(family.get("family_name") or family.get("name") or family.get("family_id") or ""),
        prior_classification=str(family.get("classification") or "UNKNOWN"),
        observations_logged=observations,
        supporting_observations=supporting,
        invalidating_observations=invalidating,
        neutral_observations=neutral,
        sample_size=observations,
        confidence_before=round(before, 4),
        confidence_after=round(after, 4),
        confidence_delta=delta,
        status_after_update=status,
        reason_for_update=reason,
        next_required_evidence=next_evidence,
        metadata={
            "direct_data_required": direct_data_required,
            "needs_data_observations": int(stats["needs_data"]),
            "retirement_markers": int(stats["retire"]),
            "paper_forward_plan_id": paper_plan.get("plan_id", ""),
        },
    )


def _status_reason_next(
    *,
    family: dict[str, Any],
    observations: int,
    supporting: int,
    invalidating: int,
    neutral: int,
    needs_data: int,
    retire: int,
    direct_data_required: bool,
    delta: float,
    paper_plan: dict[str, Any],
) -> tuple[str, str, str]:
    if retire or (invalidating >= 2 and supporting == 0):
        return (
            "RETIRED",
            "Manual observation evidence recommends retirement due to explicit retirement marker or repeated invalidation without support.",
            "Document retirement review and avoid further family-level observation until reopened by evidence.",
        )
    if direct_data_required or needs_data:
        return (
            "NEEDS_DIRECT_DATA",
            "Family remains data-blocked; observation evidence cannot increase confidence without direct validation.",
            "Collect direct family-level data for the representative condition before increasing confidence.",
        )
    if observations < 3:
        return (
            "NEEDS_MORE_OBSERVATIONS",
            "Fewer than three logged observations; sample is too small for a confidence direction change.",
            paper_plan.get("observation_condition") or "Log at least three manual family-level observations.",
        )
    if delta >= 0.02:
        return ("STRENGTHENED", "Supporting observations exceeded invalidating observations.", "Continue manual observation until sample size is adequate across regimes.")
    if delta <= -0.02:
        return ("WEAKENED", "Invalidating observations exceeded supporting observations.", "Review failure modes and collect one direct counterexample check before further observation.")
    return ("UNCHANGED", "Logged observations did not materially change confidence.", "Continue observation or collect direct data if proxy dependence remains high.")


def _family_index(sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    family_index: dict[str, dict[str, Any]] = {}
    for family in sources["candidate_family_discovery"]["payload"].get("families") or []:
        family_id = family.get("family_id")
        if family_id:
            family_index[str(family_id)] = dict(family)
    for family in sources["family_robustness_review"]["payload"].get("family_reviews") or []:
        family_id = family.get("family_id")
        if family_id:
            family_index.setdefault(str(family_id), {}).update(family)
    for family in sources["edge_magnitude_estimation"]["payload"].get("families") or []:
        family_id = family.get("family_id")
        if family_id:
            family_index.setdefault(str(family_id), {}).update({"edge_magnitude": family})
    for plan in sources["family_paper_forward_observation"]["payload"].get("plans") or []:
        family_id = plan.get("family_id")
        if family_id:
            family_index.setdefault(str(family_id), {}).update({"family_id": str(family_id), "family_name": plan.get("family_name") or str(family_id)})
    return family_index


def _observation_stats(path: Path) -> dict[str, dict[str, Any]]:
    by_family: dict[str, dict[str, Any]] = defaultdict(_empty_stats)
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != CSV_FIELDS:
            raise ValueError("family observation log CSV fields do not match required fields")
        for row in reader:
            if not _is_logged_observation(row):
                continue
            family_id = str(row.get("family_id") or "")
            stats = by_family[family_id]
            stats["observations"] += 1
            if _is_supporting(row):
                stats["supporting"] += 1
            if _is_invalidating(row):
                stats["invalidating"] += 1
            if _needs_data(row):
                stats["needs_data"] += 1
            if _retire(row):
                stats["retire"] += 1
    return dict(by_family)


def _paper_plan_stats(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(plan.get("family_id")): plan for plan in report.get("plans") or [] if plan.get("family_id")}


def _prior_confidence(family: dict[str, Any], sources: dict[str, dict[str, Any]]) -> float:
    classification = str(family.get("classification") or "").upper()
    base = {
        "ROBUST_ENOUGH_TO_OBSERVE": 0.62,
        "PROMISING_BUT_DATA_BLOCKED": 0.50,
        "DUPLICATIVE": 0.42,
        "FRAGILE": 0.28,
        "REJECT_FOR_NOW": 0.20,
        "HIGH_PRIORITY_FAMILY": 0.56,
        "PROMISING_FAMILY": 0.48,
        "DUPLICATIVE_FAMILY": 0.40,
        "WEAK_FAMILY": 0.28,
        "INSUFFICIENT_DATA_FAMILY": 0.32,
    }.get(classification, 0.35)
    edge = family.get("edge_magnitude") or {}
    edge_classification = str(edge.get("classification") or "").upper()
    if edge_classification in {"HIGH", "MEANINGFUL", "PLAUSIBLE"}:
        base += 0.05
    if edge.get("direct_validation_status") == "NEEDS_DIRECT_DATA":
        base -= 0.03
    portfolio_summary = sources["portfolio_relevance_estimate"]["payload"].get("summary") or {}
    if portfolio_summary.get("base_signal_magnitude_could_plausibly_matter") is True:
        base += 0.03
    return round(_clamp(base), 4)


def _summary(updates: list[FamilyLearningUpdate]) -> dict[str, Any]:
    rows = [update.to_dict() for update in updates]
    return {
        "families_processed": len(rows),
        "families_strengthened": sum(1 for row in rows if row["status_after_update"] == "STRENGTHENED"),
        "families_weakened": sum(1 for row in rows if row["status_after_update"] == "WEAKENED"),
        "families_needing_data": sum(1 for row in rows if row["status_after_update"] == "NEEDS_DIRECT_DATA"),
        "families_needing_more_observations": sum(1 for row in rows if row["status_after_update"] == "NEEDS_MORE_OBSERVATIONS"),
        "families_retired": sum(1 for row in rows if row["status_after_update"] == "RETIRED"),
        "total_observations_logged": sum(row["observations_logged"] for row in rows),
        "total_supporting_observations": sum(row["supporting_observations"] for row in rows),
        "total_invalidating_observations": sum(row["invalidating_observations"] for row in rows),
    }


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "family_paper_forward_observation": root / "family_paper_forward_observation" / "latest.json",
        "candidate_family_discovery": root / "candidate_family_discovery" / "latest.json",
        "family_robustness_review": root / "family_robustness_review" / "latest.json",
        "edge_magnitude_estimation": root / "edge_magnitude_estimation" / "latest.json",
        "portfolio_relevance_estimate": root / "portfolio_relevance_estimate" / "latest.json",
    }
    return {name: {"path": str(path), "exists": path.exists(), "payload": _read_json(path, {})} for name, path in paths.items()}


def _empty_stats() -> dict[str, int]:
    return {"observations": 0, "supporting": 0, "invalidating": 0, "needs_data": 0, "retire": 0}


def _is_logged_observation(row: dict[str, str]) -> bool:
    if not str(row.get("family_id") or "").strip():
        return False
    return any(str(row.get(field) or "").strip() for field in ("date", "observation_condition_met", "invalidation_condition_met", "paper_outcome", "return_observed", "notes", "reviewer"))


def _is_supporting(row: dict[str, str]) -> bool:
    return _truthy(row.get("observation_condition_met", "")) or str(row.get("paper_outcome") or "").strip().lower() in SUPPORTING_OUTCOMES


def _is_invalidating(row: dict[str, str]) -> bool:
    return _truthy(row.get("invalidation_condition_met", "")) or str(row.get("paper_outcome") or "").strip().lower() in INVALIDATING_OUTCOMES


def _needs_data(row: dict[str, str]) -> bool:
    outcome = str(row.get("paper_outcome") or "").strip().lower()
    notes = str(row.get("notes") or "").lower()
    return outcome in NEEDS_DATA_OUTCOMES or "needs data" in notes or "direct data" in notes


def _retire(row: dict[str, str]) -> bool:
    return str(row.get("paper_outcome") or "").strip().lower() in RETIRE_OUTCOMES


def _truthy(value: str) -> bool:
    return str(value).strip().lower() in {"true", "yes", "y", "1"}


def _direct_data_required(family: dict[str, Any]) -> bool:
    if family.get("requires_direct_data_validation") is True:
        return True
    if str(family.get("classification") or "") == "PROMISING_BUT_DATA_BLOCKED":
        return True
    proxy = family.get("proxy_data_dependence") or {}
    if proxy.get("requires_direct_data_validation") is True:
        return True
    edge = family.get("edge_magnitude") or {}
    return bool(edge.get("data_blockers") or edge.get("proxy_dependence") == "HIGH")


def _family_sort_key(family: dict[str, Any]) -> tuple[int, int, str]:
    priority = {
        "ROBUST_ENOUGH_TO_OBSERVE": 0,
        "PROMISING_BUT_DATA_BLOCKED": 1,
        "HIGH_PRIORITY_FAMILY": 2,
        "PROMISING_FAMILY": 3,
        "DUPLICATIVE": 4,
        "FRAGILE": 5,
        "REJECT_FOR_NOW": 6,
    }
    try:
        rank = int(family.get("best_rank"))
    except (TypeError, ValueError):
        rank = 999999
    return priority.get(str(family.get("classification") or ""), 9), rank, str(family.get("family_id") or "")


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
