from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .expanded_search_trial import (
    AUTHORITY_BOUNDARY,
    GUARDRAIL as EXPANDED_GUARDRAIL,
    _cluster_dimensions,
    _family_discovery,
    _family_robustness_review,
    _final_qualification,
    _load_latest_import,
    _observation_dimensions,
    _record_index,
    _top_dimension,
    _validate_authority,
)
from .historical_replay_engine import now_utc
from .observation_expansion import build_observation_expansion_report
from .observation_trial import AUTHORITY_STATEMENT as TRIAL_AUTHORITY_STATEMENT
from .observation_trial import build_observation_trial_report

REPORT_DIRNAME = "focused_expanded_search_trial"
PROFILE = "EXPANDED_OBSERVATION_TRIAL_FOCUSED"

ORIGINAL_SPLIT_BASELINE = {
    "hypotheses": 600,
    "backtest_supported": 228,
    "final_eligible": 110,
    "candidate_families": 30,
    "robust_families": 2,
    "promising_data_blocked_families": 5,
}

BROAD_EXPANDED_BASELINE = {
    "hypotheses": 770,
    "backtest_supported": 427,
    "final_eligible": 0,
    "robust_families": 0,
    "promising_data_blocked_families": 427,
}

FOCUSED_DIMENSIONS = {
    "regime": ["CHOP", "TRENDING", "BEAR", "RISK_ON", "VOL_CONTRACTION"],
    "market_structure": ["COMPRESSION", "FAILED_BREAKOUT", "GAP_UP", "GAP_DOWN"],
    "session_context": [],
    "mechanism": ["BREAKOUT", "MEAN_REVERSION", "EVENT_REACTION", "REVERSAL"],
    "timeframe": ["30M", "15M", "1H", "5M"],
    "source_type": ["JOURNAL_EXTRACT", "MANUAL_REVIEW", "SCANNER_SNAPSHOT", "SCREEN_REPLAY"],
    "symbol_universe": ["QQQ", "DBC", "DIA", "USO", "GOOGL"],
}

GUARDRAIL = (
    "Focused expanded search trial is bounded research evidence only. It does not authorize live trading, broker "
    "execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate "
    "production promotion, or production promotion."
)


def build_focused_expanded_search_trial_report(
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
    trials = trial.get("trials") or []
    import_payload = _load_latest_import(root_path)
    import_records = [row for row in import_payload.get("batch", {}).get("records", []) if isinstance(row, dict)]
    final_qualification = _final_qualification(trials)
    family_discovery = _family_discovery(trials, _record_index(import_records))
    family_robustness = _family_robustness_review(family_discovery)
    metrics = _required_metrics(expansion, trial, final_qualification, family_discovery, family_robustness, trials)
    comparison = _compare_quality(metrics)
    conclusion = _conclusion(metrics, comparison)
    report = {
        "schema_id": "atlas_v2_research_os_focused_expanded_search_trial_v1",
        "schema_version": "v1",
        "report_type": "FOCUSED_EXPANDED_SEARCH_TRIAL",
        "created_at": created,
        "day": day_value,
        "profile": PROFILE,
        "bounded": True,
        "focused_dimensions": dict(FOCUSED_DIMENSIONS),
        "pipeline": [
            "focused observations",
            "claims",
            "hypotheses",
            "historical replay",
            "backtest-aware final qualification",
            "candidate family discovery",
            "family robustness review",
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
        "observation_dimensions": _observation_dimensions(import_records),
        "cluster_dimensions": _cluster_dimensions(import_payload.get("clusters") or []),
        "final_qualification": final_qualification,
        "family_discovery": family_discovery,
        "family_robustness_review": family_robustness,
        "required_metrics": metrics,
        "baselines": {
            "original_split_search": dict(ORIGINAL_SPLIT_BASELINE),
            "broad_expanded_search": dict(BROAD_EXPANDED_BASELINE),
        },
        "comparison": comparison,
        "required_conclusion": conclusion,
        "source_reports": _source_reports(root_path),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [GUARDRAIL, EXPANDED_GUARDRAIL, TRIAL_AUTHORITY_STATEMENT],
        "limitations": [
            "Historical replay and candidate backtests are dry-run research evidence only.",
            "Focused profile excludes session context as requested; session effects are not evaluated here.",
            "No candidate is promoted, placed, sized, or recommended.",
        ],
    }
    _validate_authority(report)
    return report


def write_focused_expanded_search_trial_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    report_root: str | Path | None = None,
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int | None = None,
    trial_limit: int | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Path]:
    report = build_focused_expanded_search_trial_report(
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
    json_path = out_dir / "focused_expanded_search_trial_report.json"
    summary_path = out_dir / "focused_expanded_search_trial_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_focused_expanded_search_trial_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_focused_expanded_search_trial_summary(report: dict[str, Any]) -> str:
    metrics = report.get("required_metrics", {})
    comparison = report.get("comparison", {})
    conclusion = report.get("required_conclusion", {})
    lines = [
        "# Focused Expanded Search Trial",
        "",
        f"Created: {report.get('created_at')}",
        f"Profile: {report.get('profile')}",
        "",
        "## Required Metrics",
        "",
        f"- Observations generated: {metrics.get('observations_generated')}",
        f"- Claims generated: {metrics.get('claims_generated')}",
        f"- Hypotheses generated: {metrics.get('hypotheses_generated')}",
        f"- Positive replay rate: {metrics.get('positive_replay_rate')}",
        f"- Backtest-supported candidates: {metrics.get('backtest_supported_candidates')}",
        f"- Final eligible candidates: {metrics.get('final_eligible_candidates')}",
        f"- Near-threshold candidates: {metrics.get('near_threshold_candidate_count')}",
        f"- Candidate families: {metrics.get('candidate_families')}",
        f"- New families: {metrics.get('new_families')}",
        f"- Duplicate families: {metrics.get('duplicate_families')}",
        f"- Robust families: {metrics.get('robust_families')}",
        f"- Promising-data-blocked families: {metrics.get('promising_data_blocked_families')}",
        "",
        "## Best Dimensions",
        "",
        f"- Best mechanisms: {', '.join(metrics.get('best_mechanisms', [])) or 'none'}",
        f"- Best regimes: {', '.join(metrics.get('best_regimes', [])) or 'none'}",
        f"- Best market structures: {', '.join(metrics.get('best_market_structures', [])) or 'none'}",
        f"- Best timeframes: {', '.join(metrics.get('best_timeframes', [])) or 'none'}",
        "",
        "## Comparison",
        "",
        f"- Quality vs broad expanded search: {comparison.get('quality_vs_broad_expanded_search')}",
        f"- Quality vs original split-search baseline: {comparison.get('quality_vs_original_split_search')}",
        "",
        "## Required Conclusion",
        "",
        f"- Did focusing improve final eligibility? {conclusion.get('did_focusing_improve_final_eligibility')}",
        f"- Did focusing improve family quality? {conclusion.get('did_focusing_improve_family_quality')}",
        f"- Did focusing reduce duplicate/noisy families? {conclusion.get('did_focusing_reduce_duplicate_or_noisy_families')}",
        f"- Decision: {conclusion.get('profile_decision')}",
        f"- Rationale: {conclusion.get('rationale')}",
        "",
        "## Guardrails",
        "",
        f"- {GUARDRAIL}",
        "",
    ]
    return "\n".join(lines)


def _required_metrics(
    expansion: dict[str, Any],
    trial: dict[str, Any],
    final: dict[str, Any],
    family: dict[str, Any],
    robustness: dict[str, Any],
    trials: list[dict[str, Any]],
) -> dict[str, Any]:
    obs = expansion.get("metrics", {})
    trial_metrics = trial.get("metrics", {})
    final_summary = final.get("summary", {})
    family_summary = family.get("summary", {})
    robustness_summary = robustness.get("summary", {})
    diagnostic_families = family.get("families") or family.get("backtest_supported_families") or []
    return {
        "observations_generated": int(obs.get("observations_generated") or 0),
        "claims_generated": int(obs.get("claims_generated") or 0),
        "hypotheses_generated": int(final_summary.get("hypotheses_generated") or 0),
        "positive_replay_rate": trial_metrics.get("positive_replay_rate", 0.0),
        "backtest_supported_candidates": int(final_summary.get("backtest_supported_candidates") or 0),
        "final_eligible_candidates": int(final_summary.get("final_eligible_candidates") or 0),
        "near_threshold_candidate_count": len(_near_threshold_candidates(trials)),
        "near_threshold_candidates": _near_threshold_candidates(trials)[:25],
        "candidate_families": int(family_summary.get("backtest_supported_family_count") or family_summary.get("new_family_count") or 0),
        "new_families": int(family_summary.get("new_family_count") or 0),
        "duplicate_families": int(family_summary.get("duplicate_family_count") or 0),
        "supported_duplicate_families": int(family_summary.get("backtest_supported_duplicate_family_count") or 0),
        "robust_families": int(robustness_summary.get("robust_family_count") or 0),
        "promising_data_blocked_families": int(robustness_summary.get("promising_but_data_blocked_family_count") or 0),
        "best_mechanisms": _top_dimension(diagnostic_families, "dominant_mechanism"),
        "best_regimes": _top_dimension(diagnostic_families, "dominant_regime"),
        "best_market_structures": _top_dimension(diagnostic_families, "dominant_market_structure"),
        "best_timeframes": _top_dimension(diagnostic_families, "dominant_timeframe"),
    }


def _near_threshold_candidates(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in trials:
        score = row.get("edge_qualification", {}).get("edge_score")
        if not isinstance(score, (int, float)):
            continue
        gap = round(0.7 - float(score), 6)
        if 0 <= gap <= 0.02:
            rows.append({
                "candidate_id": row.get("candidate_id"),
                "mechanism": row.get("mechanism"),
                "market_structure": row.get("market_structure"),
                "regime": row.get("regime"),
                "edge_score": round(float(score), 6),
                "edge_score_gap_to_0_70": gap,
                "backtest_classification": row.get("candidate_backtest", {}).get("classification"),
                "paper_forward_ready": row.get("paper_forward_ready"),
            })
    rows.sort(key=lambda item: (item["edge_score_gap_to_0_70"], str(item.get("candidate_id"))))
    return rows


def _compare_quality(metrics: dict[str, Any]) -> dict[str, Any]:
    broad = BROAD_EXPANDED_BASELINE
    original = ORIGINAL_SPLIT_BASELINE
    focused_final = int(metrics.get("final_eligible_candidates") or 0)
    focused_robust = int(metrics.get("robust_families") or 0)
    focused_supported = int(metrics.get("backtest_supported_candidates") or 0)
    focused_promising = int(metrics.get("promising_data_blocked_families") or 0)
    return {
        "quality_vs_broad_expanded_search": _quality_label(
            focused_final, focused_robust, focused_supported, focused_promising, broad, broad_name="broad expanded search"
        ),
        "quality_vs_original_split_search": _quality_label(
            focused_final, focused_robust, focused_supported, focused_promising, original, broad_name="original split-search"
        ),
        "final_eligible_delta_vs_broad": focused_final - broad["final_eligible"],
        "final_eligible_delta_vs_original": focused_final - original["final_eligible"],
        "robust_family_delta_vs_broad": focused_robust - broad["robust_families"],
        "robust_family_delta_vs_original": focused_robust - original["robust_families"],
        "backtest_supported_delta_vs_broad": focused_supported - broad["backtest_supported"],
        "backtest_supported_delta_vs_original": focused_supported - original["backtest_supported"],
        "promising_data_blocked_delta_vs_broad": focused_promising - broad["promising_data_blocked_families"],
        "promising_data_blocked_delta_vs_original": focused_promising - original["promising_data_blocked_families"],
    }


def _quality_label(final_eligible: int, robust: int, supported: int, promising: int, baseline: dict[str, int], *, broad_name: str) -> str:
    if final_eligible > baseline["final_eligible"] and robust >= baseline["robust_families"]:
        return f"IMPROVED_VS_{broad_name.upper().replace(' ', '_')}: final eligibility improved with robust family support."
    if final_eligible > baseline["final_eligible"]:
        return f"PARTIAL_IMPROVEMENT_VS_{broad_name.upper().replace(' ', '_')}: final eligibility improved but robust family quality did not."
    if supported > baseline["backtest_supported"] and final_eligible == 0:
        return f"DIAGNOSTIC_ONLY_VS_{broad_name.upper().replace(' ', '_')}: support improved but final eligibility remains blocked."
    if promising < baseline["promising_data_blocked_families"] and final_eligible == baseline["final_eligible"]:
        return f"NO_QUALITY_IMPROVEMENT_BUT_LESS_NOISE_VS_{broad_name.upper().replace(' ', '_')}"
    return f"NOT_IMPROVED_VS_{broad_name.upper().replace(' ', '_')}"


def _conclusion(metrics: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    improved_final = int(metrics.get("final_eligible_candidates") or 0) > BROAD_EXPANDED_BASELINE["final_eligible"]
    improved_family = int(metrics.get("robust_families") or 0) > BROAD_EXPANDED_BASELINE["robust_families"]
    reduced_noise = int(metrics.get("promising_data_blocked_families") or 0) < BROAD_EXPANDED_BASELINE["promising_data_blocked_families"]
    if improved_final and improved_family:
        decision = "KEEP_FOCUSED_PROFILE"
        rationale = "Focused search improved final eligibility and robust family quality versus broad expanded search."
    elif improved_final or reduced_noise:
        decision = "MODIFY_FOCUSED_PROFILE"
        rationale = "Focused search improved at least one quality/noise dimension but still needs refinement before broad reuse."
    else:
        decision = "REJECT_AS_CURRENTLY_CONFIGURED"
        rationale = "Focused search did not improve final eligibility, robust family quality, or noise versus broad expanded search."
    return {
        "did_focusing_improve_final_eligibility": improved_final,
        "did_focusing_improve_family_quality": improved_family,
        "did_focusing_reduce_duplicate_or_noisy_families": reduced_noise,
        "profile_decision": decision,
        "rationale": rationale,
        "quality_vs_broad_expanded_search": comparison.get("quality_vs_broad_expanded_search"),
        "quality_vs_original_split_search_baseline": comparison.get("quality_vs_original_split_search"),
    }


def _source_reports(root: Path) -> dict[str, dict[str, Any]]:
    names = [
        "observation_import",
        "backtest_aware_final_qualification",
        "final_candidate_ranking",
        "candidate_family_discovery",
        "family_robustness_review",
        "expanded_search_trial",
        "expanded_search_gate_audit",
    ]
    return {name: {"path": str(root / name / "latest.json"), "exists": (root / name / "latest.json").exists()} for name in names}


def main() -> int:
    paths = write_focused_expanded_search_trial_report()
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
