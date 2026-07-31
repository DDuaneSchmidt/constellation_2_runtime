from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import SUPPORTED_CLASSIFICATION
from .historical_replay_engine import now_utc
from .observation_expansion import AUTHORITY_BOUNDARY as EXPANSION_AUTHORITY_BOUNDARY
from .observation_expansion import build_observation_expansion_report
from .observation_trial import AUTHORITY_STATEMENT as TRIAL_AUTHORITY_STATEMENT
from .observation_trial import build_observation_trial_report

REPORT_DIRNAME = "expanded_search_trial"
PROFILE = "EXPANDED_OBSERVATION_TRIAL_5000"
BASELINE = {
    "hypotheses": 600,
    "backtest_supported": 228,
    "final_eligible": 110,
    "candidate_families": 30,
    "family_observation_plans": 7,
    "robust_families": 2,
    "promising_data_blocked_families": 5,
}

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "expanded_observation_trial_allowed": True,
    "claim_generation_allowed": True,
    "hypothesis_dry_run_allowed": True,
    "historical_replay_dry_run_allowed": True,
    "final_qualification_report_allowed": True,
    "family_discovery_report_allowed": True,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
}

GUARDRAIL = (
    "Expanded search trial is bounded research evidence only. It does not authorize live trading, broker execution, "
    "capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion."
)


def build_expanded_search_trial_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int | None = None,
    trial_limit: int | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    expansion = build_observation_expansion_report(
        root=root_path,
        profile=PROFILE,
        day=day_value,
        created_at=created,
        dry_run_limit=dry_run_limit or 25,
        write_import_report=True,
    )
    trial = build_observation_trial_report(
        root=root_path,
        day=day_value,
        created_at=created,
        limit=trial_limit,
        data_path=data_path,
    )
    import_payload = _load_latest_import(root_path)
    import_records = [row for row in import_payload.get("batch", {}).get("records", []) if isinstance(row, dict)]
    observation_dimensions = _observation_dimensions(import_records)
    cluster_dimensions = _cluster_dimensions(import_payload.get("clusters") or [])
    final_qualification = _final_qualification(trial.get("trials") or [])
    family_discovery = _family_discovery(trial.get("trials") or [], _record_index(import_records))
    family_robustness = _family_robustness_review(family_discovery)
    comparison = _compare_to_baseline(final_qualification, family_discovery, observation_dimensions, family_robustness)
    report = {
        "schema_id": "atlas_v2_research_os_expanded_search_trial_v1",
        "schema_version": "v1",
        "report_type": "EXPANDED_SEARCH_TRIAL",
        "created_at": created,
        "day": day_value,
        "profile": PROFILE,
        "bounded": True,
        "pipeline": [
            "expanded observations",
            "claims",
            "hypotheses",
            "historical replay",
            "backtest-aware final qualification",
            "candidate family discovery",
            "family robustness review",
        ],
        "requested_dimensions": [
            "regime",
            "market_structure",
            "session_context",
            "symbol",
            "timeframe",
            "mechanism",
            "source_type",
        ],
        "observation_expansion": {
            "metrics": expansion.get("metrics", {}),
            "coverage": expansion.get("coverage", {}),
            "dry_run_sample": expansion.get("dry_run_sample", [])[:25],
        },
        "observation_trial": {
            "metrics": trial.get("metrics", {}),
            "assessment": trial.get("assessment", {}),
            "source_reports": trial.get("source_reports", {}),
            "data_source": trial.get("data_source", {}),
            "data_limitations": trial.get("data_limitations", []),
        },
        "observation_dimensions": observation_dimensions,
        "cluster_dimensions": cluster_dimensions,
        "final_qualification": final_qualification,
        "family_discovery": family_discovery,
        "family_robustness_review": family_robustness,
        "baseline": dict(BASELINE),
        "comparison_against_prior_baseline": comparison,
        "source_reports": _source_reports(root_path),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [GUARDRAIL, TRIAL_AUTHORITY_STATEMENT],
        "limitations": [
            "Historical replay and candidate backtests are dry-run research evidence only.",
            "Family discovery is based on bounded generated observations and local prior reports; no candidate is promoted.",
            "Direct candidate-specific market data remains a separate requirement before any later workflow review.",
        ],
    }
    _validate_authority(report)
    return report


def write_expanded_search_trial_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    report_root: str | Path | None = None,
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int | None = None,
    trial_limit: int | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Path]:
    report = build_expanded_search_trial_report(
        root=root,
        day=day,
        created_at=created_at,
        dry_run_limit=dry_run_limit,
        trial_limit=trial_limit,
        data_path=data_path,
    )
    root_path = Path(report_root) if report_root else Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report.get("day"))
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "expanded_search_trial_report.json"
    summary_path = out_dir / "expanded_search_trial_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_expanded_search_trial_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_expanded_search_trial_summary(report: dict[str, Any]) -> str:
    obs = report.get("observation_expansion", {}).get("metrics", {})
    final = report.get("final_qualification", {}).get("summary", {})
    families = report.get("family_discovery", {}).get("summary", {})
    comparison = report.get("comparison_against_prior_baseline", {})
    lines = [
        "# Expanded Search Trial",
        "",
        f"Created: {report.get('created_at')}",
        f"Profile: {report.get('profile')}",
        "",
        "## Pipeline Counts",
        "",
        f"- Expanded observations: {obs.get('observations_generated', 0)}",
        f"- Claims: {obs.get('claims_generated', 0)}",
        f"- Hypotheses: {final.get('hypotheses_generated', 0)}",
        f"- Historical replays: {final.get('historical_replays_run', 0)}",
        f"- Positive replay rate: {report.get('observation_trial', {}).get('metrics', {}).get('positive_replay_rate', 0.0)}",
        f"- Backtest-supported: {final.get('backtest_supported_candidates', 0)}",
        f"- Final eligible: {final.get('final_eligible_candidates', 0)}",
        f"- Candidate families: {families.get('new_family_count', 0)}",
        f"- Supported diagnostic families: {families.get('backtest_supported_family_count', 0)}",
        f"- Robust families: {report.get('family_robustness_review', {}).get('summary', {}).get('robust_family_count', 0)}",
        f"- Promising but data-blocked families: {report.get('family_robustness_review', {}).get('summary', {}).get('promising_but_data_blocked_family_count', 0)}",
        "",
        "## Baseline Comparison",
        "",
        f"- New hypotheses: {comparison.get('new_hypotheses')}",
        f"- New eligible candidates: {comparison.get('new_eligible_candidates')}",
        f"- New families: {comparison.get('new_families')}",
        f"- Duplicate families: {comparison.get('duplicate_families')}",
        f"- Expansion increased diversity: {comparison.get('expansion_increased_diversity')}",
        f"- Quality vs baseline: {comparison.get('quality_vs_baseline')}",
        f"- Diversity vs baseline: {comparison.get('diversity_vs_baseline')}",
        f"- Expansion improved quality: {comparison.get('expansion_improved_quality')}",
        "",
        "## Best New Dimensions",
        "",
        f"- Best new regimes: {', '.join(comparison.get('best_new_regimes', [])) or 'none'}",
        f"- Best market structures: {', '.join(comparison.get('best_market_structures', [])) or 'none'}",
        f"- Best session contexts: {', '.join(comparison.get('best_session_contexts', [])) or 'none'}",
        "",
        "## Core Question",
        "",
        str(comparison.get('core_question_answer') or ''),
        "",
        "## Guardrails",
        "",
        f"- {GUARDRAIL}",
        "",
    ]
    return "\n".join(lines)


def _final_qualification(trials: list[dict[str, Any]]) -> dict[str, Any]:
    supported = [row for row in trials if row.get("candidate_backtest", {}).get("classification") == SUPPORTED_CLASSIFICATION]
    edge_eligible = [row for row in trials if row.get("edge_qualification", {}).get("eligible") is True]
    final_eligible = [row for row in trials if row.get("paper_forward_ready") is True]

    def avg(rows: list[dict[str, Any]], path: tuple[str, ...]) -> float:
        vals = []
        for row in rows:
            value: Any = row
            for key in path:
                value = value.get(key, {}) if isinstance(value, dict) else None
            if isinstance(value, (int, float)):
                vals.append(float(value))
        return round(sum(vals) / len(vals), 6) if vals else 0.0

    return {
        "summary": {
            "hypotheses_generated": len(trials),
            "historical_replays_run": len(trials),
            "backtest_supported_candidates": len(supported),
            "edge_eligible_candidates": len(edge_eligible),
            "final_eligible_candidates": len(final_eligible),
            "average_final_eligible_edge_score": avg(final_eligible, ("edge_qualification", "edge_score")),
            "average_supported_expectancy": avg(supported, ("candidate_backtest", "metrics", "expectancy")),
            "average_supported_profit_factor": avg(supported, ("candidate_backtest", "metrics", "profit_factor")),
        },
        "eligible_examples": [_trial_brief(row) for row in final_eligible[:25]],
        "backtest_supported_examples": [_trial_brief(row) for row in supported[:25]],
        "qualification_rule": "final eligible means dry-run paper candidate gate eligible and candidate backtest classification BACKTEST_SUPPORTED; no promotion authority is granted",
    }


def _family_discovery(trials: list[dict[str, Any]], records_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    eligible = [row for row in trials if row.get("paper_forward_ready") is True]
    supported = [row for row in trials if row.get("candidate_backtest", {}).get("classification") == SUPPORTED_CLASSIFICATION]
    families = _build_families(eligible, records_by_id)
    supported_families = _build_families(supported, records_by_id)
    duplicate_count = sum(1 for row in families if row["duplicate_or_distinct_assessment"] == "REPEATED_FAMILY_VARIANTS")
    supported_duplicate_count = sum(1 for row in supported_families if row["duplicate_or_distinct_assessment"] == "REPEATED_FAMILY_VARIANTS")
    return {
        "summary": {
            "new_family_count": len(families),
            "duplicate_family_count": duplicate_count,
            "distinct_family_count": len(families) - duplicate_count,
            "eligible_candidates_clustered": len(eligible),
            "backtest_supported_family_count": len(supported_families),
            "backtest_supported_duplicate_family_count": supported_duplicate_count,
        },
        "families": families[:100],
        "backtest_supported_families": supported_families[:100],
        "methodology": "New families are counted from final-eligible trial candidates. If none qualify, best dimensions are reported from backtest-supported families as non-promotional research diagnostics.",
    }


def _build_families(rows_in: list[dict[str, Any]], records_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    family_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows_in:
        dims = _trial_dimensions(row, records_by_id)
        enriched = dict(row)
        enriched["expanded_dimensions"] = dims
        family_rows[_family_id(dims)].append(enriched)
    families = []
    for family_id, rows in family_rows.items():
        dims = rows[0]["expanded_dimensions"]
        edge_scores = [float(row.get("edge_qualification", {}).get("edge_score") or 0.0) for row in rows]
        expectancies = [float(row.get("candidate_backtest", {}).get("metrics", {}).get("expectancy") or 0.0) for row in rows]
        families.append({
            "family_id": family_id,
            "family_name": " / ".join([dims.get("mechanism", "UNKNOWN"), dims.get("regime", "UNKNOWN"), dims.get("market_structure", "UNKNOWN"), dims.get("session_context", "UNKNOWN"), dims.get("timeframe", "UNKNOWN"), dims.get("source_type", "UNKNOWN")]),
            "candidate_count": len(rows),
            "dominant_mechanism": dims.get("mechanism"),
            "dominant_regime": dims.get("regime"),
            "dominant_market_structure": dims.get("market_structure"),
            "dominant_session_context": dims.get("session_context"),
            "dominant_timeframe": dims.get("timeframe"),
            "dominant_source_type": dims.get("source_type"),
            "average_edge_score": round(sum(edge_scores) / len(edge_scores), 6) if edge_scores else 0.0,
            "average_expectancy": round(sum(expectancies) / len(expectancies), 6) if expectancies else 0.0,
            "candidate_ids": [str(row.get("candidate_id")) for row in rows[:10]],
            "duplicate_or_distinct_assessment": "REPEATED_FAMILY_VARIANTS" if len(rows) > 1 else "GENUINELY_DISTINCT",
        })
    families.sort(key=lambda row: (-float(row.get("average_edge_score") or 0.0), -float(row.get("average_expectancy") or 0.0), str(row.get("family_id"))))
    return families


def _family_robustness_review(family: dict[str, Any]) -> dict[str, Any]:
    final_families = [row for row in family.get("families", []) if isinstance(row, dict)]
    supported_families = [row for row in family.get("backtest_supported_families", []) if isinstance(row, dict)]
    robust = [row for row in final_families if float(row.get("average_edge_score") or 0.0) > 0.0]
    robust_keys = {row.get("family_id") for row in robust}
    promising = [row for row in supported_families if row.get("family_id") not in robust_keys]
    supported_count = int((family.get("summary") or {}).get("backtest_supported_family_count") or len(supported_families))
    promising_count = max(len(promising), supported_count - len(robust))
    return {
        "schema_id": "atlas_v2_research_os_expanded_search_trial_family_robustness_review_v1",
        "classification_rule": "ROBUST requires final-eligible expanded-trial family evidence; backtest-supported families without final eligibility are promising but data-blocked diagnostics.",
        "summary": {
            "robust_family_count": len(robust),
            "promising_but_data_blocked_family_count": promising_count,
            "insufficient_data_family_count": 0 if final_families or supported_families else 1,
            "robust_family_baseline": BASELINE["robust_families"],
            "promising_data_blocked_family_baseline": BASELINE["promising_data_blocked_families"],
        },
        "robust_families": robust[:25],
        "promising_but_data_blocked_families": promising[:50],
        "authority": "Research-only robustness classification. No candidate promotion, paper placement, trade recommendation, sizing, capital allocation, broker execution, or live trading authority.",
    }


def _compare_to_baseline(final: dict[str, Any], family: dict[str, Any], dimensions: dict[str, Any], robustness: dict[str, Any]) -> dict[str, Any]:
    summary = final.get("summary", {})
    family_summary = family.get("summary", {})
    hypotheses = int(summary.get("hypotheses_generated") or 0)
    supported = int(summary.get("backtest_supported_candidates") or 0)
    eligible = int(summary.get("final_eligible_candidates") or 0)
    new_families = int(family_summary.get("new_family_count") or 0)
    supported_family_count = int(family_summary.get("backtest_supported_family_count") or 0)
    robustness_summary = robustness.get("summary", {})
    robust_family_count = int(robustness_summary.get("robust_family_count") or 0)
    promising_blocked_count = int(robustness_summary.get("promising_but_data_blocked_family_count") or 0)
    baseline_families = _prior_family_keys()
    trial_families = family.get("families", [])
    supported_families = family.get("backtest_supported_families", [])
    dimension_families = trial_families or supported_families
    duplicate_families = sum(1 for row in trial_families if _legacy_family_key(row) in baseline_families)
    supported_duplicate_families = sum(1 for row in supported_families if _legacy_family_key(row) in baseline_families)
    quality_vs_baseline = _quality_vs_baseline(supported, eligible, robust_family_count)
    diversity_vs_baseline = _diversity_vs_baseline(new_families, supported_family_count, supported_duplicate_families)
    core_answer = _core_question_answer(new_families, supported_family_count, supported_duplicate_families, robust_family_count)
    return {
        "baseline": dict(BASELINE),
        "new_hypotheses": hypotheses,
        "new_backtest_supported_candidates": supported,
        "new_eligible_candidates": eligible,
        "new_families": new_families,
        "supported_diagnostic_families": supported_family_count,
        "duplicate_families": duplicate_families,
        "supported_duplicate_families": supported_duplicate_families,
        "robust_families": robust_family_count,
        "promising_but_data_blocked_families": promising_blocked_count,
        "hypothesis_delta_vs_baseline": hypotheses - BASELINE["hypotheses"],
        "backtest_supported_delta_vs_baseline": supported - BASELINE["backtest_supported"],
        "eligible_delta_vs_baseline": eligible - BASELINE["final_eligible"],
        "family_delta_vs_baseline": new_families - BASELINE["candidate_families"],
        "supported_family_delta_vs_baseline": supported_family_count - BASELINE["candidate_families"],
        "robust_family_delta_vs_baseline": robust_family_count - BASELINE["robust_families"],
        "promising_data_blocked_delta_vs_baseline": promising_blocked_count - BASELINE["promising_data_blocked_families"],
        "best_new_regimes": _top_dimension(dimension_families, "dominant_regime"),
        "best_market_structures": _top_dimension(dimension_families, "dominant_market_structure"),
        "best_session_contexts": _top_dimension(dimension_families, "dominant_session_context"),
        "quality_vs_baseline": quality_vs_baseline,
        "diversity_vs_baseline": diversity_vs_baseline,
        "expansion_increased_diversity": bool(supported_family_count > BASELINE["candidate_families"] or dimensions.get("session_contexts_covered", 0) >= 5),
        "expansion_improved_quality": bool(eligible > BASELINE["final_eligible"] and robust_family_count >= BASELINE["robust_families"]),
        "quality_assessment": quality_vs_baseline,
        "core_question_answer": core_answer,
    }


def _quality_vs_baseline(supported: int, eligible: int, robust_family_count: int) -> str:
    if eligible > BASELINE["final_eligible"] and robust_family_count >= BASELINE["robust_families"]:
        return "IMPROVED: final-eligible candidates and robust family evidence exceed baseline."
    if supported > BASELINE["backtest_supported"] and eligible == 0:
        return "MIXED_DATA_BLOCKED: backtest support improved, but final eligibility and robust family evidence did not."
    if supported > BASELINE["backtest_supported"]:
        return "MIXED: backtest support improved, but final-eligible robustness remains below baseline."
    return "NOT_IMPROVED: expanded trial did not improve quality versus baseline."


def _diversity_vs_baseline(new_families: int, supported_family_count: int, supported_duplicate_families: int) -> str:
    if new_families > BASELINE["candidate_families"]:
        return "IMPROVED_FINAL_ELIGIBLE_DIVERSITY"
    if supported_family_count > BASELINE["candidate_families"] and supported_duplicate_families < supported_family_count:
        return "IMPROVED_DIAGNOSTIC_DIVERSITY_ONLY"
    return "NOT_IMPROVED"


def _core_question_answer(new_families: int, supported_family_count: int, supported_duplicate_families: int, robust_family_count: int) -> str:
    distinct_supported = max(0, supported_family_count - supported_duplicate_families)
    if robust_family_count > 0:
        return "Expanded context produced robust final-eligible families; treat them as genuinely new only after direct-data validation."
    if distinct_supported > 0:
        return "Expanded context produced many genuinely distinct backtest-supported diagnostic families, but no final-eligible or robust families; this is broader search evidence, not validated family readiness."
    if supported_duplicate_families > 0:
        return "Expanded context mostly produced variants of existing families and did not create robust new families."
    return "Expanded context did not produce enough qualifying family evidence to distinguish new families from variants."


def _observation_dimensions(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "regimes_covered": len({str(row.get("regime") or "UNKNOWN").upper() for row in records}),
        "market_structures_covered": len({str(row.get("market_structure") or "UNKNOWN").upper() for row in records}),
        "session_contexts_covered": len({str(row.get("metadata", {}).get("session_context") or "UNKNOWN").upper() for row in records}),
        "symbols_covered": len({str(row.get("symbol") or "").upper() for row in records}),
        "timeframes_covered": len({str(row.get("timeframe") or "").upper() for row in records}),
        "mechanisms_covered": len({str(row.get("mechanism") or "UNKNOWN").upper() for row in records}),
        "source_types_covered": len({str(row.get("metadata", {}).get("source_type") or "UNKNOWN").upper() for row in records}),
    }


def _cluster_dimensions(clusters: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "clusters": len(clusters),
        "market_structure_counts": dict(Counter(str(row.get("market_structure") or "UNKNOWN") for row in clusters)),
        "session_context_counts": dict(Counter(ctx for row in clusters for ctx in row.get("metadata", {}).get("session_contexts", ["UNKNOWN"]))),
    }


def _trial_dimensions(row: dict[str, Any], records_by_id: dict[str, dict[str, Any]]) -> dict[str, str]:
    obs_ids = row.get("claim", {}).get("source_observation_ids") or row.get("hypothesis", {}).get("source_observation_ids") or []
    records = [records_by_id.get(str(obs_id), {}) for obs_id in obs_ids]

    def mode(values: list[str], default: str = "UNKNOWN") -> str:
        clean = [value for value in values if value]
        return Counter(clean).most_common(1)[0][0] if clean else default

    return {
        "mechanism": str(row.get("mechanism") or "UNKNOWN").upper(),
        "regime": str(row.get("regime") or "UNKNOWN").upper(),
        "market_structure": mode([str(record.get("market_structure") or "").upper() for record in records]),
        "session_context": mode([str(record.get("metadata", {}).get("session_context") or "").upper() for record in records]),
        "symbol": mode([str(record.get("symbol") or "").upper() for record in records]),
        "timeframe": mode([str(record.get("timeframe") or "").upper() for record in records]),
        "source_type": mode([str(record.get("metadata", {}).get("source_type") or "").upper() for record in records]),
    }


def _trial_brief(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "hypothesis_id": row.get("hypothesis_id"),
        "mechanism": row.get("mechanism"),
        "regime": row.get("regime"),
        "replay_score": row.get("historical_replay", {}).get("score"),
        "edge_score": row.get("edge_qualification", {}).get("edge_score"),
        "backtest_classification": row.get("candidate_backtest", {}).get("classification"),
        "paper_forward_ready": row.get("paper_forward_ready"),
    }


def _record_index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("observation_id")): row for row in records if row.get("observation_id")}


def _load_latest_import(root: Path) -> dict[str, Any]:
    path = root / "observation_import" / "latest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _source_reports(root: Path) -> dict[str, dict[str, Any]]:
    names = ["observation_import", "backtest_aware_final_qualification", "final_candidate_ranking", "candidate_family_discovery", "winner_pattern_extraction"]
    return {name: {"path": str(root / name / "latest.json"), "exists": (root / name / "latest.json").exists()} for name in names}


def _top_dimension(families: list[dict[str, Any]], key: str) -> list[str]:
    counts: Counter[str] = Counter()
    weighted: defaultdict[str, float] = defaultdict(float)
    for family in families:
        value = str(family.get(key) or "UNKNOWN")
        counts[value] += int(family.get("candidate_count") or 1)
        weighted[value] += float(family.get("average_edge_score") or 0.0)
    return [value for value, _ in sorted(counts.items(), key=lambda item: (-item[1], -weighted[item[0]], item[0]))[:5]]


def _prior_family_keys() -> set[str]:
    path = Path(DEFAULT_STORE_ROOT) / "candidate_family_discovery" / "latest.json"
    if not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {_legacy_family_key(row) for row in payload.get("families", []) if isinstance(row, dict)}


def _legacy_family_key(row: dict[str, Any]) -> str:
    return "|".join([
        str(row.get("dominant_mechanism") or "UNKNOWN").upper(),
        str(row.get("dominant_regime") or "UNKNOWN").upper(),
        str(row.get("dominant_timeframe") or "UNKNOWN").upper(),
    ])


def _family_id(dimensions: dict[str, str]) -> str:
    material = json.dumps(dimensions, sort_keys=True)
    return "expanded_family_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _validate_authority(report: dict[str, Any]) -> None:
    boundary = report.get("authority_boundary", {})
    allowed_true = {
        "research_only",
        "expanded_observation_trial_allowed",
        "claim_generation_allowed",
        "hypothesis_dry_run_allowed",
        "historical_replay_dry_run_allowed",
        "final_qualification_report_allowed",
        "family_discovery_report_allowed",
    }
    forbidden_true = [key for key, value in boundary.items() if key not in allowed_true and value is True]
    if boundary.get("research_only") is not True or forbidden_true:
        raise ValueError(f"expanded search trial authority boundary failed: {forbidden_true}")
    text = json.dumps(report, sort_keys=True).lower()
    for phrase in [
        "live_trading_authorized\": true",
        "broker_execution_authorized\": true",
        "capital_authorized\": true",
        "position_sizing_authorized\": true",
        "trade_recommendation_authorized\": true",
        "automatic_paper_trade_placement_authorized\": true",
        "candidate_promotion_authorized\": true",
    ]:
        if phrase in text:
            raise ValueError(f"forbidden authority phrase present: {phrase}")
    if any(value is True for key, value in EXPANSION_AUTHORITY_BOUNDARY.items() if key not in {"research_only", "observation_import_allowed", "claim_seed_allowed", "hypothesis_dry_run_allowed", "historical_replay_dry_run_allowed"}):
        raise ValueError("upstream expansion authority boundary unexpectedly allows forbidden behavior")
