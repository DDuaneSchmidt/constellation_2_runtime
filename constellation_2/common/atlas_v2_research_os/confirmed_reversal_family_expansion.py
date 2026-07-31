from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import run_candidate_backtest_spec
from .direct_candidate_data_validation import AUTHORITY_BOUNDARY, _direct_backtest_spec, _load_direct_symbol_data
from .local_market_data_import import discover_local_market_data_files

REPORT_DIRNAME = "confirmed_reversal_family_expansion"
TARGET_FAMILY_ID = "family_55443d63b32328bd"
TARGET_CANDIDATE_ID = "ptc_backtest_final_4df2e8e80685a054"


def run_confirmed_reversal_family_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_confirmed_reversal_family_expansion_report(root=root, created_at=created_at)
    write_confirmed_reversal_family_expansion_report(report, root=root)
    return report


def build_confirmed_reversal_family_expansion_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    direct = _read_json(root_path / "direct_candidate_data_validation" / "latest.json", {})
    families_payload = _read_json(root_path / "candidate_family_discovery" / "latest.json", {})
    robustness = _read_json(root_path / "family_robustness_review" / "latest.json", {})
    final_ranking = _read_json(root_path / "final_candidate_ranking" / "latest.json", {})
    bridge = _read_json(root_path / "regime_vocabulary_bridge" / "latest.json", {})
    holdout = _read_json(root_path / "holdout_replay_validation" / "latest.json", {})

    family_rows = list(families_payload.get("families") or families_payload.get("candidate_families") or [])
    target_family = next((row for row in family_rows if row.get("family_id") == TARGET_FAMILY_ID), {})
    candidate_rows = _candidate_rows(final_ranking, families_payload)
    direct_by_id = {row.get("candidate_id"): row for row in direct.get("candidate_validations", [])}
    local_index = _local_data_index()

    representative = _representative_detail(target_family, candidate_rows.get(TARGET_CANDIDATE_ID, {}), direct_by_id.get(TARGET_CANDIDATE_ID, {}), local_index)
    related_families = _related_families(family_rows)
    related_variants = [
        _variant_detail(family, candidate_rows, direct_by_id, root_path, local_index, created_at=created)
        for family in related_families
    ]
    holdout_feasibility = _holdout_feasibility(holdout, related_variants)
    repeatability = _repeatability_assessment(representative, related_variants)

    report = {
        "schema_id": "atlas_v2_research_os_confirmed_reversal_family_expansion",
        "schema_version": "1.0",
        "report_type": "CONFIRMED_REVERSAL_FAMILY_EXPANSION",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            "direct_candidate_data_validation": str(root_path / "direct_candidate_data_validation" / "latest.json"),
            "regime_vocabulary_bridge": str(root_path / "regime_vocabulary_bridge" / "latest.json"),
            "candidate_family_discovery": str(root_path / "candidate_family_discovery" / "latest.json"),
            "family_robustness_review": str(root_path / "family_robustness_review" / "latest.json"),
            "holdout_replay_validation": str(root_path / "holdout_replay_validation" / "latest.json"),
            "final_candidate_ranking": str(root_path / "final_candidate_ranking" / "latest.json"),
        },
        "target_family": representative,
        "family_robustness_classification": _family_classification(robustness),
        "regime_bridge_summary": bridge.get("summary", {}),
        "related_variant_search_policy": {
            "exact": "REVERSAL / TRENDING / 1H / SCREEN_REPLAY or related source",
            "near_variants": ["REVERSAL / TRENDING / 30M", "REVERSAL / RANGE_BOUND / 1H", "REVERSAL / CHOP / 1H"],
            "direct_validation_runs_only_when_local_data_exists": True,
        },
        "related_variants": related_variants,
        "holdout_feasibility": holdout_feasibility,
        "required_conclusion": {
            "isolated_or_repeatable": repeatability["classification"],
            "does_it_justify_priority_forward_observation": repeatability["priority_forward_observation"],
            "evidence_still_missing": holdout_feasibility["missing"] + repeatability["missing_evidence"],
            "what_would_invalidate_it": [
                "Related reversal/trending variants fail direct replay on adequately covered symbols/timeframes.",
                "Forward observations show non-positive expectancy or material drawdown concentration.",
                "Event-level holdout rows fail early/late or rolling-year splits.",
                "BAC-only confirmation disappears when exact 1H local data is available.",
            ],
        },
        "summary": {
            "target_family_id": TARGET_FAMILY_ID,
            "target_candidate_id": TARGET_CANDIDATE_ID,
            "target_direct_classification": representative.get("direct_validation_classification"),
            "target_direct_sample_size": representative.get("direct_result", {}).get("sample_size"),
            "related_variants_found": len(related_variants),
            "variant_classification_counts": dict(Counter(row["direct_validation_classification"] for row in related_variants)),
            "direct_confirmed_variants": [row["family_id"] for row in related_variants if row["direct_validation_classification"] == "DIRECT_CONFIRMED"],
            "direct_weak_variants": [row["family_id"] for row in related_variants if row["direct_validation_classification"] == "DIRECT_WEAK"],
            "holdout_can_run_now": holdout_feasibility["can_run_now"],
            "repeatability_assessment": repeatability["classification"],
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Research-only family expansion.",
            "No live trading.",
            "No broker execution.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }
    return report


def write_confirmed_reversal_family_expansion_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "confirmed_reversal_family_expansion_report.json"
    summary_path = out_dir / "confirmed_reversal_family_expansion_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_confirmed_reversal_family_expansion_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_confirmed_reversal_family_expansion_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    target = report.get("target_family", {})
    lines = [
        "# Confirmed Reversal Family Expansion",
        "",
        f"Target family: {summary.get('target_family_id')} / {target.get('family_name')}",
        f"Target candidate: {summary.get('target_candidate_id')}",
        f"Target direct result: {summary.get('target_direct_classification')} sample_size={summary.get('target_direct_sample_size')}",
        f"Related variants found: {summary.get('related_variants_found')}",
        f"Variant classifications: {json.dumps(summary.get('variant_classification_counts') or {}, sort_keys=True)}",
        f"Repeatability: {summary.get('repeatability_assessment')}",
        f"Holdout can run now: {summary.get('holdout_can_run_now')}",
        "",
        "## Related Variants",
    ]
    for row in report.get("related_variants") or []:
        lines.append(
            f"- {row.get('family_id')} {row.get('family_name')}: {row.get('direct_validation_classification')} "
            f"sample_size={row.get('direct_result', {}).get('sample_size')} symbol={row.get('direct_result', {}).get('symbol')}"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            f"- Isolated or repeatable: {report.get('required_conclusion', {}).get('isolated_or_repeatable')}",
            f"- Priority forward observation: {report.get('required_conclusion', {}).get('does_it_justify_priority_forward_observation')}",
            f"- Missing evidence: {json.dumps(report.get('required_conclusion', {}).get('evidence_still_missing') or [], sort_keys=True)}",
            "",
            "Authority: research-only; no live/capital/broker/position-sizing authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _representative_detail(family: dict[str, Any], candidate: dict[str, Any], direct: dict[str, Any], local_index: dict[tuple[str, str], list[dict[str, Any]]]) -> dict[str, Any]:
    symbols = _strings(family.get("symbols") or family.get("symbols/universes") or candidate.get("candidate_symbols"))
    timeframes = _strings(family.get("timeframes") or candidate.get("candidate_timeframes"), lower=True)
    return {
        "family_id": TARGET_FAMILY_ID,
        "family_name": family.get("family_name") or "REVERSAL / TRENDING / 1H / SCREEN_REPLAY",
        "representative_candidates": [TARGET_CANDIDATE_ID],
        "symbols_universe": symbols,
        "timeframe": timeframes,
        "mechanism": family.get("mechanism") or family.get("dominant_mechanism") or candidate.get("mechanism"),
        "regime": family.get("regime") or family.get("dominant_regime") or candidate.get("regime"),
        "market_structure": family.get("market_structure") or candidate.get("market_structure"),
        "session_context": family.get("session_context") or candidate.get("session_context"),
        "source_type": (family.get("source_types") or [family.get("dominant_source_type") or "UNKNOWN"])[0],
        "entry_exit_invalidation_rules": _rules(candidate),
        "direct_validation_classification": _classify_direct_row(direct),
        "direct_result": direct.get("direct_result") or {},
        "available_local_data": _available_local_data(symbols, timeframes, local_index),
        "missing_local_data": _missing_local_data(symbols, timeframes, local_index),
    }


def _variant_detail(family: dict[str, Any], candidates: dict[str, dict[str, Any]], direct_by_id: dict[str, dict[str, Any]], root: Path, local_index: dict[tuple[str, str], list[dict[str, Any]]], *, created_at: str) -> dict[str, Any]:
    candidate_ids = _strings(family.get("candidate_ids") or family.get("top_candidate_ids"))
    representative_id = candidate_ids[0] if candidate_ids else family.get("best_candidate_id")
    candidate = candidates.get(representative_id, {"candidate_id": representative_id, "mechanism": family.get("mechanism"), "regime": family.get("regime")})
    symbols = _strings(family.get("symbols") or family.get("symbols/universes") or candidate.get("candidate_symbols"))
    timeframes = _strings(family.get("timeframes") or candidate.get("candidate_timeframes"), lower=True)
    direct_row = direct_by_id.get(representative_id)
    replay = direct_row.get("direct_result") if direct_row else _run_variant_direct(candidate, family, symbols, timeframes, root, created_at=created_at)
    return {
        "family_id": family.get("family_id"),
        "family_name": family.get("family_name"),
        "representative_candidates": candidate_ids,
        "tested_candidate_id": representative_id,
        "symbols_universe": symbols,
        "timeframe": timeframes,
        "mechanism": family.get("mechanism") or family.get("dominant_mechanism") or candidate.get("mechanism"),
        "regime": family.get("regime") or family.get("dominant_regime") or candidate.get("regime"),
        "market_structure": family.get("market_structure") or candidate.get("market_structure"),
        "session_context": family.get("session_context") or candidate.get("session_context"),
        "source_type": (family.get("source_types") or [family.get("dominant_source_type") or "UNKNOWN"])[0],
        "entry_exit_invalidation_rules": _rules(candidate),
        "direct_validation_classification": _classify_direct_result(replay),
        "direct_result": replay or {},
        "available_local_data": _available_local_data(symbols, timeframes, local_index),
        "missing_local_data": _missing_local_data(symbols, timeframes, local_index),
    }


def _run_variant_direct(candidate: dict[str, Any], family: dict[str, Any], symbols: list[str], timeframes: list[str], root: Path, *, created_at: str) -> dict[str, Any]:
    runs = []
    plan = {
        "mechanism": family.get("mechanism") or family.get("dominant_mechanism"),
        "regime": family.get("regime") or family.get("dominant_regime"),
    }
    candidate_for_spec = dict(candidate)
    candidate_for_spec.setdefault("mechanism", plan["mechanism"])
    candidate_for_spec.setdefault("regime", plan["regime"])
    candidate_for_spec.setdefault("candidate_timeframes", timeframes)
    for symbol in symbols:
        rows, meta, searched = _load_direct_symbol_data(symbol, root, timeframes=timeframes)
        if not rows:
            runs.append({"symbol": symbol, "classification": "INSUFFICIENT_DATA", "sample_size": 0, "missing_data": searched})
            continue
        spec = _direct_backtest_spec(candidate_for_spec, plan, symbol=symbol, data_meta=meta)
        result = run_candidate_backtest_spec(spec, rows, created_at=created_at)
        metrics = result.get("metrics") or {}
        runs.append(
            {
                "symbol": symbol,
                "classification": result.get("classification"),
                "sample_size": metrics.get("sample_size", 0),
                "expectancy": metrics.get("expectancy"),
                "profit_factor": metrics.get("profit_factor"),
                "max_drawdown": metrics.get("max_drawdown"),
                "missing_data": result.get("missing_data") or [],
                "warnings": result.get("warnings") or [],
                "data_meta": meta,
            }
        )
    if not runs:
        return {"classification": "INSUFFICIENT_DATA", "sample_size": 0, "missing_data": ["no related symbols found"]}
    return sorted(runs, key=lambda row: (row.get("classification") == "BACKTEST_SUPPORTED", float(row.get("expectancy") or 0), int(row.get("sample_size") or 0)), reverse=True)[0]


def _related_families(families: list[dict[str, Any]]) -> list[dict[str, Any]]:
    related = []
    for family in families:
        if family.get("family_id") == TARGET_FAMILY_ID:
            continue
        mechanism = _norm(family.get("mechanism") or family.get("dominant_mechanism"))
        regime = _norm(family.get("regime") or family.get("dominant_regime"))
        timeframes = {_norm(tf) for tf in family.get("timeframes") or ([family.get("dominant_timeframe")] if family.get("dominant_timeframe") else [])}
        source = _norm((family.get("source_types") or [family.get("dominant_source_type") or ""])[0])
        if mechanism != "REVERSAL":
            continue
        exact = regime == "TRENDING" and "1H" in timeframes and source in {"SCREEN_REPLAY", "UNKNOWN", "MANUAL_REVIEW"}
        near = (regime == "TRENDING" and "30M" in timeframes) or (regime in {"RANGE_BOUND", "CHOP"} and "1H" in timeframes)
        if exact or near:
            row = dict(family)
            row["related_match_type"] = "EXACT_OR_SOURCE_RELATED" if exact else "NEAR_VARIANT"
            related.append(row)
    return sorted(related, key=lambda row: int(row.get("best_rank") or 999999))


def _candidate_rows(final_ranking: dict[str, Any], families_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = []
    for key in ("campaign_candidate_preview", "top_20_robust_candidates", "excluded_candidates", "recommended_human_review_order"):
        value = final_ranking.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    for key in ("candidate_family_assignments",):
        value = families_payload.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    return {row.get("candidate_id"): row for row in rows if row.get("candidate_id")}


def _holdout_feasibility(holdout: dict[str, Any], variants: list[dict[str, Any]]) -> dict[str, Any]:
    data = holdout.get("data_availability") or {}
    missing = []
    if not data.get("event_level_rows_with_returns"):
        missing.append("event-level holdout rows with return_observed")
    if not data.get("event_level_rows_loaded"):
        missing.append("date split support from dated event-level rows")
    if any(row.get("missing_local_data") for row in variants):
        missing.append("exact symbol/timeframe coverage for some related variants")
    missing.append("family-level rule parser coverage for exact entry/exit/invalidation semantics")
    return {
        "can_run_now": False if missing else True,
        "holdout_summary": holdout.get("summary", {}),
        "missing": missing,
        "event_level_rows_loaded": data.get("event_level_rows_loaded", 0),
        "event_level_rows_with_returns": data.get("event_level_rows_with_returns", 0),
    }


def _repeatability_assessment(representative: dict[str, Any], variants: list[dict[str, Any]]) -> dict[str, Any]:
    confirmed = [row for row in variants if row["direct_validation_classification"] == "DIRECT_CONFIRMED"]
    weak = [row for row in variants if row["direct_validation_classification"] == "DIRECT_WEAK"]
    if confirmed:
        classification = "REPEATABLE_ACROSS_RELATED_VARIANTS"
        priority = "YES_RESEARCH_ONLY_PRIORITY_FORWARD_OBSERVATION"
    elif weak:
        classification = "PARTIALLY_REPEATABLE_BUT_WEAK"
        priority = "YES_FOR_PAPER_FORWARD_OBSERVATION_PRIORITY_WITH_NO_CONFIDENCE_INCREASE"
    else:
        classification = "ISOLATED_CONFIRMED_RESULT"
        priority = "LIMITED_PRIORITY_FORWARD_OBSERVATION_ONLY"
    return {
        "classification": classification,
        "priority_forward_observation": priority,
        "missing_evidence": [
            "independent forward observations for BAC and related symbols",
            "event-level holdout rows",
            "exact 1H local data for all attributed symbols where absent",
        ],
    }


def _classify_direct_row(row: dict[str, Any]) -> str:
    return _classify_direct_result(row.get("direct_result") or {})


def _classify_direct_result(result: dict[str, Any] | None) -> str:
    result = result or {}
    classification = result.get("classification")
    sample_size = int(result.get("sample_size") or 0)
    if classification == "BACKTEST_SUPPORTED":
        return "DIRECT_CONFIRMED"
    if classification == "BACKTEST_WEAK":
        return "DIRECT_WEAK"
    if classification == "BACKTEST_FAILED":
        return "DIRECT_FAILED"
    if sample_size == 0:
        return "RULE_NO_SAMPLE"
    return "INSUFFICIENT_DATA"


def _available_local_data(symbols: list[str], timeframes: list[str], local_index: dict[tuple[str, str], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for symbol in symbols:
        for timeframe in timeframes:
            rows.extend(local_index.get((symbol.upper(), _tf(timeframe)), []))
        rows.extend(local_index.get((symbol.upper(), "daily"), []))
    return rows


def _missing_local_data(symbols: list[str], timeframes: list[str], local_index: dict[tuple[str, str], list[dict[str, Any]]]) -> list[str]:
    missing = []
    for symbol in symbols:
        for timeframe in timeframes:
            if not local_index.get((symbol.upper(), _tf(timeframe))):
                missing.append(f"{symbol}:{_tf(timeframe)}")
    return sorted(set(missing))


def _local_data_index() -> dict[tuple[str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for file in discover_local_market_data_files(base_dir=Path.cwd()):
        row = {"symbol": file.symbol, "timeframe": _tf(file.timeframe), "path": file.path}
        index.setdefault((file.symbol, _tf(file.timeframe)), []).append(row)
    return index


def _rules(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "hypothesis": candidate.get("hypothesis"),
        "entry": candidate.get("entry_observation_condition"),
        "exit": candidate.get("exit_observation_condition"),
        "invalidation": candidate.get("invalidation_condition") or candidate.get("invalidating_condition") or candidate.get("invalidating_conditions"),
        "parser_note": "Executable direct replay uses deterministic REVERSAL proxy trigger: three-bar down streak followed by close above open; exit uses fixed horizon.",
    }


def _family_classification(robustness: dict[str, Any]) -> str | None:
    for row in robustness.get("family_reviews") or []:
        if row.get("family_id") == TARGET_FAMILY_ID:
            return row.get("classification")
    return None


def _strings(value: Any, *, lower: bool = False) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]
    elif isinstance(value, list):
        parts = [str(part).strip() for part in value if str(part).strip()]
    else:
        parts = [str(value).strip()]
    if lower:
        return sorted({part.lower() for part in parts})
    return sorted({part.upper() if part.isalpha() else part for part in parts})


def _tf(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"1h": "1h", "1hr": "1h", "60m": "1h", "30m": "30m", "5m": "5m", "15m": "15m", "daily": "daily", "1d": "daily"}.get(text, text)


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
