from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .regime_vocabulary_bridge import NO_EXECUTABLE_REGIME_EQUIVALENT, explain_regime_mapping, map_research_regime_to_replay_regime

REPORT_DIRNAME = "holdout_replay_validation"

CLASSIFICATIONS = (
    "HOLDOUT_SURVIVED",
    "HOLDOUT_WEAKENED",
    "HOLDOUT_FAILED",
    "INSUFFICIENT_HOLDOUT_DATA",
    "DATA_BLOCKED",
)

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "holdout_replay_validation_allowed": True,
    "confidence_increase_from_search_selection_allowed": False,
    "trade_recommendation_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}

GUARDRAILS = [
    "Use unseen or held-out historical periods when event-level outcome data is available.",
    "Do not retune thresholds after seeing holdout results.",
    "Do not change family definitions after holdout results.",
    "Do not use holdout results to automatically promote candidates.",
    "Search selection alone cannot increase confidence.",
    "No trade recommendations, capital allocation, position sizing, broker execution, automatic paper placement, or candidate production promotion.",
]


def run_holdout_replay_validation(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_replay_validation_report(root=root, created_at=created_at)
    write_holdout_replay_validation_report(report, root=root)
    return report


def build_holdout_replay_validation_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    target_families = _select_target_families(sources)
    event_rows = _load_holdout_event_rows(root_path)
    family_validations = [_validate_family(family, event_rows) for family in target_families]
    summary = _summary(family_validations)
    report = {
        "schema_id": "atlas_v2_research_os_holdout_replay_validation_v1",
        "schema_version": "1.0",
        "report_type": "HOLDOUT_REPLAY_VALIDATION",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        },
        "target_selection_policy": {
            "prioritized_groups": [
                "2 ROBUST_ENOUGH_TO_OBSERVE families",
                "5 PROMISING_BUT_DATA_BLOCKED families",
                "Top 8 campaign families",
            ],
            "family_definition_frozen": True,
            "threshold_retuning_on_holdout_allowed": False,
            "automatic_promotion_allowed": False,
        },
        "holdout_splits": [
            {
                "split_id": "EARLY_DISCOVERY_LATE_HOLDOUT",
                "discovery_rule": "Earliest 70 percent of dated event-level outcomes.",
                "holdout_rule": "Latest 30 percent of dated event-level outcomes.",
                "implemented": True,
            },
            {
                "split_id": "ROLLING_YEAR_HOLDOUT",
                "discovery_rule": "Prior years are discovery and the latest year is holdout when dated outcomes span at least two calendar years.",
                "holdout_rule": "Latest calendar year.",
                "implemented": True,
            },
            {
                "split_id": "REGIME_HOLDOUT",
                "discovery_rule": "All but one regime are discovery when event-level regime labels exist.",
                "holdout_rule": "Least represented eligible regime.",
                "implemented": True,
            },
        ],
        "data_availability": {
            "event_level_rows_loaded": len(event_rows),
            "event_level_rows_with_returns": sum(1 for row in event_rows if _return_value(row) is not None),
            "aggregate_backtest_rows_treated_as_holdout": False,
            "data_blocker": None
            if event_rows
            else "No event-level dated return_observed rows were available for the targeted families; aggregate expectancy/profit-factor rows are not treated as holdout evidence.",
        },
        "family_validations": family_validations,
        "summary": summary,
        "required_conclusion": {
            "did_any_family_survive_holdout": bool(summary["families_survived_holdout"]),
            "families_failed": summary["families_failed"],
            "families_remain_data_blocked": summary["families_data_blocked"],
            "methodology_confidence_impact": summary["methodology_confidence_impact"],
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": list(GUARDRAILS),
    }
    _validate_report(report)
    return report


def write_holdout_replay_validation_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report.get("day"))
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "holdout_replay_validation_report.json"
    summary_path = out_dir / "holdout_replay_validation_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_holdout_replay_validation_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_holdout_replay_validation_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Holdout Replay Validation",
        "",
        f"Created: {report.get('created_at')}",
        f"Families targeted: {summary.get('families_targeted', 0)}",
        f"Families survived holdout: {len(summary.get('families_survived_holdout', []))}",
        f"Families failed holdout: {len(summary.get('families_failed', []))}",
        f"Families data-blocked: {len(summary.get('families_data_blocked', []))}",
        f"Methodology confidence impact: {summary.get('methodology_confidence_impact')}",
        "",
        "## Required Conclusion",
        "",
        f"- Did any family survive holdout? {'yes' if summary.get('families_survived_holdout') else 'no'}",
        f"- Families failed: {', '.join(summary.get('families_failed', [])) or 'none'}",
        f"- Families data-blocked: {', '.join(summary.get('families_data_blocked', [])[:12]) or 'none'}",
        f"- Methodology confidence: {summary.get('methodology_confidence_impact')}",
        "",
        "## Family Results",
        "",
    ]
    for row in report.get("family_validations") or []:
        lines.append(
            f"- {row.get('family_id')} ({row.get('family_name')}): {row.get('classification')} / {row.get('confidence_impact')}"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Family definitions and thresholds are frozen before holdout evaluation.",
            "- Aggregate backtest summaries are not counted as holdout evidence.",
            "- Research-only. No trade, capital, broker, position-sizing, automatic paper-placement, or candidate production-promotion authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_family(family: dict[str, Any], event_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = _matching_rows(family, event_rows)
    split_results = _split_results(rows)
    primary = split_results.get("EARLY_DISCOVERY_LATE_HOLDOUT") or {}
    discovery = primary.get("discovery") or {}
    holdout = primary.get("holdout") or {}
    classification, reason, impact = _classify_holdout(family, rows, primary)
    return {
        "family_id": family["family_id"],
        "family_name": family["family_name"],
        "target_reasons": family.get("target_reasons", []),
        "prior_classification": family.get("prior_classification"),
        "candidate_ids": family.get("candidate_ids", []),
        "family_definition": {
            "mechanism": family.get("mechanism"),
            "regime": family.get("regime"),
            "regime_vocabulary_bridge": explain_regime_mapping(family.get("regime")),
            "timeframes": family.get("timeframes", []),
            "source_types": family.get("source_types", []),
            "symbols": family.get("symbols", []),
        },
        "discovery_sample_size": discovery.get("sample_size", 0),
        "holdout_sample_size": holdout.get("sample_size", 0),
        "discovery_expectancy": discovery.get("expectancy"),
        "holdout_expectancy": holdout.get("expectancy"),
        "discovery_profit_factor": discovery.get("profit_factor"),
        "holdout_profit_factor": holdout.get("profit_factor"),
        "performance_degradation": _performance_degradation(discovery.get("expectancy"), holdout.get("expectancy")),
        "holdout_survived": classification == "HOLDOUT_SURVIVED",
        "failure_reason": reason,
        "confidence_impact": impact,
        "classification": classification,
        "split_results": split_results,
        "rules_frozen": True,
        "thresholds_retuned_on_holdout": False,
        "automatic_promotion_authorized": False,
    }


def _split_results(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    dated = sorted([row for row in rows if _return_value(row) is not None], key=_sort_key)
    return {
        "EARLY_DISCOVERY_LATE_HOLDOUT": _early_late_split(dated),
        "ROLLING_YEAR_HOLDOUT": _rolling_year_split(dated),
        "REGIME_HOLDOUT": _regime_split(dated),
    }


def _early_late_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if len(rows) < 8:
        return _insufficient_split("Need at least 8 dated return rows for a non-trivial 70/30 holdout split.")
    cut = max(1, int(len(rows) * 0.7))
    if cut >= len(rows):
        cut = len(rows) - 1
    return {"status": "EVALUATED", "discovery": _metrics(rows[:cut]), "holdout": _metrics(rows[cut:])}


def _rolling_year_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    years = sorted({_year(row) for row in rows if _year(row)})
    if len(years) < 2:
        return _insufficient_split("Need dated return rows spanning at least two calendar years.")
    holdout_year = years[-1]
    discovery = [row for row in rows if _year(row) != holdout_year]
    holdout = [row for row in rows if _year(row) == holdout_year]
    if not discovery or not holdout:
        return _insufficient_split("Rolling-year split produced an empty side.")
    return {"status": "EVALUATED", "holdout_year": holdout_year, "discovery": _metrics(discovery), "holdout": _metrics(holdout)}


def _regime_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    regimes = Counter(str(row.get("regime") or "").upper() for row in rows if row.get("regime"))
    regimes.pop("", None)
    if len(regimes) < 2:
        return _insufficient_split("Need at least two event-level regime labels.")
    holdout_regime = min(regimes, key=lambda key: (regimes[key], key))
    discovery = [row for row in rows if str(row.get("regime") or "").upper() != holdout_regime]
    holdout = [row for row in rows if str(row.get("regime") or "").upper() == holdout_regime]
    if not discovery or not holdout:
        return _insufficient_split("Regime split produced an empty side.")
    return {"status": "EVALUATED", "holdout_regime": holdout_regime, "discovery": _metrics(discovery), "holdout": _metrics(holdout)}


def _classify_holdout(family: dict[str, Any], rows: list[dict[str, Any]], primary: dict[str, Any]) -> tuple[str, str, str]:
    if not rows:
        return (
            "DATA_BLOCKED",
            "No dated event-level return_observed rows match this frozen family definition; aggregate proxy backtest metrics were not counted as holdout.",
            "UNCHANGED_NEEDS_DIRECT_DATA",
        )
    if primary.get("status") != "EVALUATED":
        return ("INSUFFICIENT_HOLDOUT_DATA", primary.get("reason", "Insufficient holdout data."), "UNCHANGED_NEEDS_MORE_HOLDOUT")
    discovery = primary["discovery"]
    holdout = primary["holdout"]
    holdout_expectancy = holdout.get("expectancy")
    holdout_pf = holdout.get("profit_factor")
    if holdout_expectancy is None or holdout_pf is None:
        return ("INSUFFICIENT_HOLDOUT_DATA", "Holdout metrics could not be computed.", "UNCHANGED_NEEDS_MORE_HOLDOUT")
    if holdout_expectancy <= 0 or holdout_pf < 1.0:
        return ("HOLDOUT_FAILED", "Holdout expectancy was non-positive or profit factor fell below 1.0.", "DECREASE")
    degradation = _performance_degradation(discovery.get("expectancy"), holdout_expectancy)
    if degradation is not None and degradation > 0.5:
        return ("HOLDOUT_WEAKENED", "Holdout remained positive but expectancy degraded by more than 50 percent.", "DECREASE")
    if family.get("requires_direct_data"):
        return ("HOLDOUT_SURVIVED", "Holdout metrics survived, but direct-data blocker still prevents confidence increase.", "UNCHANGED_NEEDS_DIRECT_DATA")
    return ("HOLDOUT_SURVIVED", "Holdout expectancy and profit factor survived without retuning.", "INCREASE_FROM_HOLDOUT_ONLY")


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [_return_value(row) for row in rows]
    values = [value for value in returns if value is not None]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_loss = abs(sum(losses))
    profit_factor = None if gross_loss == 0 else round(sum(wins) / gross_loss, 6)
    return {
        "sample_size": len(values),
        "expectancy": round(mean(values), 6) if values else None,
        "profit_factor": profit_factor,
    }


def _matching_rows(family: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidate_ids = set(family.get("candidate_ids") or [])
    family_id = family.get("family_id")
    mechanism = _norm(family.get("mechanism"))
    regime = _norm(family.get("regime"))
    replay_regime = map_research_regime_to_replay_regime(regime) if regime else ""
    regime_equivalents = {value for value in {regime, replay_regime} if value and value != NO_EXECUTABLE_REGIME_EQUIVALENT}
    timeframes = {_norm(value) for value in family.get("timeframes") or [] if value}
    matched = []
    for row in rows:
        if family_id and row.get("family_id") == family_id:
            matched.append(row)
            continue
        if candidate_ids and row.get("candidate_id") in candidate_ids:
            matched.append(row)
            continue
        if mechanism and _norm(row.get("mechanism")) == mechanism and regime_equivalents and _norm(row.get("regime")) in regime_equivalents:
            row_timeframe = _norm(row.get("timeframe"))
            if not timeframes or row_timeframe in timeframes:
                matched.append(row)
    return matched


def _select_target_families(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    robustness_rows = sources["family_robustness_review"]["payload"].get("family_reviews") or []
    discovery_rows = sources["candidate_family_discovery"]["payload"].get("candidate_families") or []
    if not discovery_rows:
        discovery_rows = sources["candidate_family_discovery"]["payload"].get("families") or []
    by_id: dict[str, dict[str, Any]] = {}
    for row in discovery_rows:
        if row.get("family_id"):
            by_id[row["family_id"]] = row
    for row in robustness_rows:
        if row.get("family_id"):
            by_id.setdefault(row["family_id"], {}).update({k: v for k, v in row.items() if v not in (None, [], {})})

    selected: dict[str, dict[str, Any]] = {}
    robust = [row for row in robustness_rows if row.get("classification") == "ROBUST_ENOUGH_TO_OBSERVE"]
    promising = [row for row in robustness_rows if row.get("classification") == "PROMISING_BUT_DATA_BLOCKED"]
    for row in sorted(robust, key=lambda item: _as_int(item.get("best_rank"), 999999))[:2]:
        _add_family(selected, row, by_id, "ROBUST_ENOUGH_TO_OBSERVE_PRIORITY")
    for row in sorted(promising, key=lambda item: _as_int(item.get("best_rank"), 999999))[:5]:
        _add_family(selected, row, by_id, "PROMISING_BUT_DATA_BLOCKED_PRIORITY")

    campaign_ids = _campaign_candidate_ids(sources)
    candidate_to_family = _candidate_to_family_map(by_id.values())
    for candidate_id in campaign_ids[:8]:
        family_id = candidate_to_family.get(candidate_id)
        if family_id and family_id in by_id:
            _add_family(selected, by_id[family_id], by_id, "TOP_8_CAMPAIGN_FAMILY")
    return sorted(selected.values(), key=lambda item: _as_int(item.get("best_rank"), 999999))


def _add_family(selected: dict[str, dict[str, Any]], row: dict[str, Any], by_id: dict[str, dict[str, Any]], reason: str) -> None:
    family_id = row.get("family_id")
    if not family_id:
        return
    merged = dict(by_id.get(family_id, {}))
    merged.update({k: v for k, v in row.items() if v not in (None, [], {})})
    family = selected.setdefault(family_id, _normalize_family(merged))
    if reason not in family["target_reasons"]:
        family["target_reasons"].append(reason)


def _normalize_family(row: dict[str, Any]) -> dict[str, Any]:
    candidate_ids = set(row.get("candidate_ids") or [])
    candidate_ids.update(row.get("top_candidate_ids") or [])
    if isinstance(row.get("best_candidate"), dict) and row["best_candidate"].get("candidate_id"):
        candidate_ids.add(row["best_candidate"]["candidate_id"])
    if row.get("best_candidate_id"):
        candidate_ids.add(row["best_candidate_id"])
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    proxy = row.get("proxy_data_dependence") if isinstance(row.get("proxy_data_dependence"), dict) else {}
    sample = row.get("sample_size_summary") if isinstance(row.get("sample_size_summary"), dict) else {}
    return {
        "family_id": row.get("family_id"),
        "family_name": row.get("family_name") or "UNKNOWN_FAMILY",
        "prior_classification": row.get("classification"),
        "best_rank": row.get("best_rank"),
        "candidate_ids": sorted(candidate_ids),
        "mechanism": row.get("mechanism") or row.get("dominant_mechanism"),
        "regime": row.get("regime") or row.get("dominant_regime"),
        "timeframes": row.get("timeframes") or ([row.get("dominant_timeframe")] if row.get("dominant_timeframe") else []),
        "source_types": row.get("source_types") or ([row.get("dominant_source_type")] if row.get("dominant_source_type") else []),
        "symbols": row.get("symbols") or row.get("symbols/universes") or [],
        "requires_direct_data": bool(row.get("requires_direct_data_validation") or proxy.get("requires_direct_data_validation")),
        "aggregate_expectancy": metrics.get("average_expectancy", row.get("average_expectancy")),
        "aggregate_profit_factor": metrics.get("average_profit_factor", row.get("average_profit_factor")),
        "aggregate_sample_size": sample.get("average", row.get("average_sample_size")),
        "target_reasons": [],
    }


def _candidate_to_family_map(rows: Any) -> dict[str, str]:
    mapping = {}
    for row in rows:
        family_id = row.get("family_id")
        if not family_id:
            continue
        normalized = _normalize_family(row)
        for candidate_id in normalized["candidate_ids"]:
            mapping[candidate_id] = family_id
    return mapping


def _campaign_candidate_ids(sources: dict[str, dict[str, Any]]) -> list[str]:
    campaign = sources["focused_observation_campaign"]["payload"].get("campaign_candidates") or []
    if not campaign:
        campaign = sources["final_candidate_ranking"]["payload"].get("campaign_candidate_preview") or []
    return [row.get("candidate_id") for row in campaign if row.get("candidate_id")]


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    names = [
        "search_overfit_guardrail",
        "candidate_family_discovery",
        "family_robustness_review",
        "focused_observation_campaign",
        "backtest_aware_final_qualification",
        "final_candidate_ranking",
    ]
    return {name: _load_latest(root, name) for name in names}


def _load_latest(root: Path, dirname: str) -> dict[str, Any]:
    path = root / dirname / "latest.json"
    if not path.exists():
        return {"path": str(path), "exists": False, "payload": {}}
    return {"path": str(path), "exists": True, "payload": _read_json(path, {})}


def _load_holdout_event_rows(root: Path) -> list[dict[str, Any]]:
    rows = []
    for dirname in ("holdout_replay_events", "family_holdout_replay_events", "historical_replay"):
        latest = root / dirname / "latest.json"
        if latest.exists():
            payload = _read_json(latest, {})
            rows.extend(_extract_event_rows(payload))
    return [row for row in rows if _return_value(row) is not None]


def _extract_event_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("holdout_events", "event_rows", "observations", "results", "rows"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["classification"] for row in rows)
    survived = [row["family_id"] for row in rows if row["classification"] == "HOLDOUT_SURVIVED"]
    failed = [row["family_id"] for row in rows if row["classification"] == "HOLDOUT_FAILED"]
    data_blocked = [row["family_id"] for row in rows if row["classification"] == "DATA_BLOCKED"]
    insufficient = [row["family_id"] for row in rows if row["classification"] == "INSUFFICIENT_HOLDOUT_DATA"]
    if survived and not failed and not data_blocked and not insufficient:
        impact = "INCREASE_FROM_HOLDOUT_ONLY"
    elif failed:
        impact = "DECREASE"
    elif survived:
        impact = "MIXED_OR_UNCHANGED_PENDING_DIRECT_DATA"
    else:
        impact = "UNCHANGED_DATA_BLOCKED"
    return {
        "families_targeted": len(rows),
        "classification_counts": {name: counts.get(name, 0) for name in CLASSIFICATIONS},
        "families_survived_holdout": survived,
        "families_weakened": [row["family_id"] for row in rows if row["classification"] == "HOLDOUT_WEAKENED"],
        "families_failed": failed,
        "families_insufficient_holdout_data": insufficient,
        "families_data_blocked": data_blocked,
        "methodology_confidence_impact": impact,
    }


def _performance_degradation(discovery_expectancy: Any, holdout_expectancy: Any) -> float | None:
    try:
        discovery = float(discovery_expectancy)
        holdout = float(holdout_expectancy)
    except (TypeError, ValueError):
        return None
    if discovery <= 0:
        return None
    return round((discovery - holdout) / abs(discovery), 6)


def _return_value(row: dict[str, Any]) -> float | None:
    for key in ("return_observed", "return", "outcome_return", "pnl_pct", "net_return"):
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _sort_key(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("timestamp") or row.get("event_date") or "")


def _year(row: dict[str, Any]) -> int | None:
    value = _sort_key(row)
    if len(value) < 4:
        return None
    try:
        return int(value[:4])
    except ValueError:
        return None


def _insufficient_split(reason: str) -> dict[str, Any]:
    return {"status": "INSUFFICIENT_DATA", "reason": reason, "discovery": _metrics([]), "holdout": _metrics([])}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_report(report: dict[str, Any]) -> None:
    authority = report.get("authority_boundary", {})
    forbidden = [
        "trade_recommendation_authorized",
        "capital_authorized",
        "position_sizing_authorized",
        "broker_execution_authorized",
        "automatic_paper_trade_placement_authorized",
        "candidate_production_promotion_authorized",
        "confidence_increase_from_search_selection_allowed",
    ]
    if any(authority.get(key) for key in forbidden):
        raise ValueError("Holdout replay validation must remain research-only with no search-selection confidence increase.")
    for row in report.get("family_validations") or []:
        if row.get("thresholds_retuned_on_holdout") or row.get("automatic_promotion_authorized"):
            raise ValueError("Holdout validation cannot retune thresholds or authorize promotion.")
