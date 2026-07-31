from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "candidate_symbol_attribution"

METHOD_DIRECT_OBSERVATION_LINEAGE = "DIRECT_OBSERVATION_LINEAGE"
METHOD_CLUSTER_MAJOR_SYMBOL = "CLUSTER_MAJOR_SYMBOL"
METHOD_CLUSTER_SYMBOL_SET = "CLUSTER_SYMBOL_SET"
METHOD_UNKNOWN = "UNKNOWN"

ATTRIBUTION_METHODS = {
    METHOD_DIRECT_OBSERVATION_LINEAGE,
    METHOD_CLUSTER_MAJOR_SYMBOL,
    METHOD_CLUSTER_SYMBOL_SET,
    METHOD_UNKNOWN,
}

AUTHORITY_BOUNDARY = {
    "symbol_attribution_only": True,
    "direct_replay_eligibility_assessment_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def run_candidate_symbol_attribution(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_candidate_symbol_attribution_report(root=root, created_at=created_at)
    write_candidate_symbol_attribution_report(report, root=root)
    return report


def build_candidate_symbol_attribution_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    campaign = _read_json(root_path / "focused_observation_campaign" / "latest.json", {})
    ranking = _read_json(root_path / "final_candidate_ranking" / "latest.json", {})
    split = _read_json(root_path / "observation_cluster_split_experiment" / "latest.json", {})
    candidates = list(campaign.get("campaign_candidates") or ranking.get("campaign_candidate_preview") or [])
    split_by_candidate = {
        row.get("candidate_id"): row
        for row in [*list(split.get("sample_split_trials") or []), *list(split.get("trials") or [])]
        if row.get("candidate_id")
    }
    rows = []
    for candidate in candidates:
        enriched = dict(candidate)
        if candidate.get("candidate_id") in split_by_candidate:
            trial = split_by_candidate[candidate.get("candidate_id")]
            enriched.setdefault("source_lineage", _lineage_from_trial(trial))
            enriched.setdefault("claim", trial.get("claim"))
        rows.append(attribute_candidate_symbols(enriched))
    method_counts = Counter(row["symbol_attribution_method"] for row in rows)
    created = created_at or _now()
    return {
        "schema_id": "atlas_v2_research_os_candidate_symbol_attribution",
        "schema_version": "1.0",
        "report_type": "CANDIDATE_SYMBOL_ATTRIBUTION",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            "focused_observation_campaign": str(root_path / "focused_observation_campaign" / "latest.json"),
            "final_candidate_ranking": str(root_path / "final_candidate_ranking" / "latest.json"),
            "observation_cluster_split_experiment": str(root_path / "observation_cluster_split_experiment" / "latest.json"),
        },
        "summary": {
            "candidates_reviewed": len(rows),
            "symbols_attributed": sum(bool(row["candidate_symbols"]) for row in rows),
            "universe_level_candidates": sum(row["universe_level_candidate"] for row in rows),
            "method_counts": dict(method_counts),
            "unknown_attribution_count": method_counts.get(METHOD_UNKNOWN, 0),
            "direct_replay_eligible_candidates": sum(row["direct_replay_eligible"] for row in rows),
        },
        "candidate_symbol_attributions": rows,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Candidate symbol attribution only.",
            "No symbols are inferred from prose.",
            "No live trading.",
            "No broker execution.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }


def write_candidate_symbol_attribution_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "candidate_symbol_attribution_report.json"
    summary_path = out_dir / "candidate_symbol_attribution_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_candidate_symbol_attribution_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_candidate_symbol_attribution_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Candidate Symbol Attribution",
        "",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Symbols attributed: {summary.get('symbols_attributed')}",
        f"Universe-level candidates: {summary.get('universe_level_candidates')}",
        f"Direct replay eligible candidates: {summary.get('direct_replay_eligible_candidates')}",
        f"Method counts: {json.dumps(summary.get('method_counts', {}), sort_keys=True)}",
        "",
        "## Candidates",
    ]
    for row in report.get("candidate_symbol_attributions", []):
        lines.append(
            "- rank={rank} candidate={candidate_id} symbols={symbols} timeframes={timeframes} method={method} confidence={confidence}".format(
                rank=row.get("campaign_rank"),
                candidate_id=row.get("candidate_id"),
                symbols=",".join(row.get("candidate_symbols") or []) or "NONE",
                timeframes=",".join(row.get("candidate_timeframes") or []) or "NONE",
                method=row.get("symbol_attribution_method"),
                confidence=row.get("symbol_attribution_confidence"),
            )
        )
    lines.extend(["", "Authority: symbol attribution and direct replay eligibility assessment only; no live/capital/broker/position-sizing/trade authority.", ""])
    return "\n".join(lines)


def attribute_candidate_symbols(candidate: dict[str, Any]) -> dict[str, Any]:
    lineage = _merged_lineage(candidate)
    source_observation_ids = _string_set(
        candidate.get("candidate_source_observation_ids"),
        candidate.get("source_observation_ids"),
        lineage.get("source_observation_ids"),
        upper=False,
    )
    symbols = _string_set(
        candidate.get("candidate_symbols"),
        candidate.get("candidate_symbol"),
        candidate.get("symbol"),
        candidate.get("symbols"),
        candidate.get("universe_symbols"),
        lineage.get("symbols"),
    )
    timeframes = _string_set(candidate.get("candidate_timeframes"), candidate.get("timeframes"), candidate.get("timeframe"), lineage.get("timeframes"), upper=False)
    source_types = _string_set(candidate.get("source_types"), candidate.get("source_type"), lineage.get("source_types"), upper=False)
    mechanism = str(candidate.get("mechanism") or lineage.get("mechanism") or "UNKNOWN").upper()
    regime = str(candidate.get("regime") or lineage.get("regime") or "UNKNOWN").upper()
    symbol_counts = _counter_from_any(lineage.get("symbol_counts"))

    method = METHOD_UNKNOWN
    confidence = 0.0
    if symbols and source_observation_ids:
        method = METHOD_DIRECT_OBSERVATION_LINEAGE
        confidence = 0.95 if len(symbols) == 1 else 0.9
    elif symbols and len(symbols) == 1:
        method = METHOD_CLUSTER_MAJOR_SYMBOL
        confidence = 0.8
    elif symbols:
        major_symbol, major_count = _major_symbol(symbol_counts)
        total = sum(symbol_counts.values())
        if major_symbol and total and major_count / total >= 0.6:
            symbols = [major_symbol]
            method = METHOD_CLUSTER_MAJOR_SYMBOL
            confidence = round(0.72 + min(0.18, major_count / total * 0.18), 6)
        else:
            method = METHOD_CLUSTER_SYMBOL_SET
            confidence = 0.75

    universe_symbols = symbols if len(symbols) > 1 else []
    return {
        "candidate_id": candidate.get("candidate_id"),
        "campaign_rank": candidate.get("campaign_rank"),
        "mechanism": mechanism,
        "regime": regime,
        "candidate_symbols": symbols,
        "candidate_symbol": symbols[0] if len(symbols) == 1 else None,
        "candidate_universe_symbols": universe_symbols,
        "candidate_timeframes": timeframes,
        "candidate_source_observation_ids": source_observation_ids,
        "source_types": source_types,
        "source_type": source_types[0] if len(source_types) == 1 else ("MULTIPLE" if source_types else "UNKNOWN"),
        "symbol_attribution_confidence": confidence,
        "symbol_attribution_method": method,
        "universe_level_candidate": len(symbols) > 1,
        "direct_replay_eligible": bool(symbols and timeframes),
        "no_prose_symbol_inference": True,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def apply_symbol_attribution(candidate: dict[str, Any]) -> dict[str, Any]:
    row = dict(candidate)
    attribution = attribute_candidate_symbols(row)
    row.update(
        {
            "candidate_symbols": attribution["candidate_symbols"],
            "candidate_symbol": attribution["candidate_symbol"],
            "candidate_universe_symbols": attribution["candidate_universe_symbols"],
            "candidate_timeframes": attribution["candidate_timeframes"],
            "candidate_source_observation_ids": attribution["candidate_source_observation_ids"],
            "symbol_attribution_confidence": attribution["symbol_attribution_confidence"],
            "symbol_attribution_method": attribution["symbol_attribution_method"],
            "universe_level_candidate": attribution["universe_level_candidate"],
        }
    )
    return row


def _merged_lineage(candidate: dict[str, Any]) -> dict[str, Any]:
    lineage: dict[str, Any] = {}
    for key in ["source_lineage", "candidate_lineage", "observation_lineage"]:
        value = candidate.get(key)
        if isinstance(value, dict):
            lineage.update(value)
    claim = candidate.get("claim")
    if isinstance(claim, dict):
        lineage.setdefault("symbols", claim.get("symbols"))
        lineage.setdefault("timeframes", claim.get("timeframes"))
        lineage.setdefault("source_observation_ids", claim.get("source_observation_ids"))
        lineage.setdefault("mechanism", claim.get("mechanism"))
        lineage.setdefault("regime", claim.get("regime"))
        metadata = claim.get("metadata") if isinstance(claim.get("metadata"), dict) else {}
        lineage.setdefault("source_types", metadata.get("source_types"))
    return lineage


def _lineage_from_trial(trial: dict[str, Any]) -> dict[str, Any]:
    claim = trial.get("claim") if isinstance(trial.get("claim"), dict) else {}
    return {
        "symbols": trial.get("symbols") or claim.get("symbols"),
        "timeframes": trial.get("timeframes") or claim.get("timeframes"),
        "source_types": trial.get("source_types") or (claim.get("metadata") or {}).get("source_types"),
        "source_observation_ids": trial.get("source_observation_ids") or claim.get("source_observation_ids"),
        "mechanism": trial.get("mechanism") or claim.get("mechanism"),
        "regime": trial.get("regime") or claim.get("regime"),
    }


def _string_set(*values: Any, upper: bool = True) -> list[str]:
    rows: list[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            parts = value.replace(";", ",").split(",")
        elif isinstance(value, (list, tuple, set)):
            parts = list(value)
        else:
            parts = [value]
        for part in parts:
            text = str(part).strip()
            if not text or text.upper() == "UNKNOWN":
                continue
            rows.append(text.upper() if upper else text.lower())
    return sorted(set(rows))


def _counter_from_any(value: Any) -> Counter[str]:
    if isinstance(value, dict):
        return Counter({str(key).upper(): int(count or 0) for key, count in value.items()})
    return Counter()


def _major_symbol(counts: Counter[str]) -> tuple[str | None, int]:
    if not counts:
        return None, 0
    symbol, count = counts.most_common(1)[0]
    return symbol, count


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
