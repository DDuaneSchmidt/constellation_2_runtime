from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .observation_trial import AUTHORITY_BOUNDARY, AUTHORITY_STATEMENT
from .observation_cluster_splitting import symbol_group

REPORT_DIRNAME = "observation_source_breakdown"
DIMENSIONS = ["mechanism", "regime", "timeframe", "symbol", "symbol_group", "source_type", "confidence_bucket"]


def build_observation_source_breakdown(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    trial_report, trial_path = _load_json(root_path / "observation_trial" / "latest.json")
    import_report, import_path = _load_json(root_path / "observation_import" / "latest.json")
    observation_index = _observation_index(import_report)
    enriched = [_enrich_trial(row, observation_index) for row in trial_report.get("trials", [])]

    dimension_breakdowns = {
        dimension: _rank_groups(_group_rows(enriched, dimension=dimension))
        for dimension in DIMENSIONS
    }
    composite_groups = _rank_groups(_group_rows(enriched, dimension="composite"))
    all_groups = []
    for dimension, rows in dimension_breakdowns.items():
        all_groups.extend(rows)
    all_groups.extend(composite_groups)
    ranked_best = sorted(all_groups, key=_best_sort_key, reverse=True)[:20]
    ranked_worst = sorted(all_groups, key=_worst_sort_key)[:20]

    summary = _summary(enriched)
    return {
        "schema_id": "atlas_v2_research_os_observation_source_breakdown_v1",
        "schema_version": "1.0",
        "created_at": created_at or _now(),
        "source_reports": {
            "observation_trial": {"path": str(trial_path), "exists": trial_path.exists()},
            "observation_import": {"path": str(import_path), "exists": import_path.exists()},
        },
        "summary": summary,
        "dimension_breakdowns": dimension_breakdowns,
        "composite_groups": composite_groups,
        "ranked_best_groups": ranked_best,
        "ranked_worst_groups": ranked_worst,
        "candidate_rows": enriched,
        "ranking_metrics": [
            "positive_replay_rate",
            "eligible_candidate_rate",
            "backtest_support_rate",
            "paper_forward_readiness_rate",
            "average_expectancy",
            "average_profit_factor",
        ],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            AUTHORITY_STATEMENT,
            "Breakdown ranks research evidence only and does not create trade recommendations.",
            "No broker, capital, position-sizing, automatic paper placement, or production-promotion authority is granted.",
        ],
    }


def write_observation_source_breakdown_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    root_path = Path(root)
    report = build_observation_source_breakdown(root_path)
    day_value = day or _today()
    out_root = root_path / REPORT_DIRNAME
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "observation_source_breakdown.json"
    summary_path = out_dir / "observation_source_breakdown_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_observation_source_breakdown_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_observation_source_breakdown_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Observation Source Breakdown",
        "",
        f"Trials analyzed: {summary.get('trials_analyzed', 0)}",
        f"Positive replay rate: {summary.get('positive_replay_rate', 0.0)}",
        f"Eligible candidate rate: {summary.get('eligible_candidate_rate', 0.0)}",
        f"Backtest support rate: {summary.get('backtest_support_rate', 0.0)}",
        f"Paper-forward readiness rate: {summary.get('paper_forward_readiness_rate', 0.0)}",
        "",
        "## Best Groups",
    ]
    for row in report.get("ranked_best_groups", [])[:10]:
        lines.append(_group_line(row))
    lines.extend(["", "## Worst Groups"])
    for row in report.get("ranked_worst_groups", [])[:10]:
        lines.append(_group_line(row))
    lines.extend(["", "## By Mechanism"])
    for row in report.get("dimension_breakdowns", {}).get("mechanism", []):
        lines.append(_group_line(row))
    lines.extend(
        [
            "",
            "Authority: observation-derived research evidence only; no trade recommendations, broker execution, capital allocation, position sizing, automatic paper trade placement, or production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def _enrich_trial(row: dict[str, Any], observation_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    claim = row.get("claim") or {}
    source_observation_ids = list(claim.get("source_observation_ids") or [])
    source_records = [observation_index.get(obs_id, {}) for obs_id in source_observation_ids]
    source_types = sorted({str(((record.get("metadata") or {}).get("source_type")) or record.get("source") or "UNKNOWN") for record in source_records if record})
    symbols = sorted({str(value) for value in claim.get("symbols", [])} | {str(record.get("symbol")) for record in source_records if record.get("symbol")})
    timeframes = sorted({str(value) for value in claim.get("timeframes", [])} | {str(record.get("timeframe")) for record in source_records if record.get("timeframe")})
    confidence = _float(claim.get("confidence"))
    backtest = row.get("candidate_backtest") or {}
    metrics = backtest.get("metrics") or {}
    edge = row.get("edge_qualification") or {}
    replay = row.get("historical_replay") or {}
    return {
        "candidate_id": row.get("candidate_id"),
        "mechanism": str(row.get("mechanism") or claim.get("mechanism") or "UNKNOWN"),
        "regime": str(row.get("regime") or claim.get("regime") or "UNKNOWN"),
        "symbols": symbols or ["UNKNOWN"],
        "symbol_groups": sorted({symbol_group(symbol) for symbol in (symbols or ["UNKNOWN"])}),
        "timeframes": timeframes or ["UNKNOWN"],
        "source_types": source_types or ["UNKNOWN"],
        "confidence": round(confidence, 6),
        "confidence_bucket": _confidence_bucket(confidence),
        "positive_replay": replay.get("status") == "REPLAY_POSITIVE",
        "eligible_candidate": edge.get("eligible") is True,
        "backtest_supported": backtest.get("classification") == "BACKTEST_SUPPORTED",
        "paper_forward_ready": row.get("paper_forward_ready") is True,
        "expectancy": _nullable_float(metrics.get("expectancy")),
        "profit_factor": _nullable_float(metrics.get("profit_factor")),
        "replay_status": replay.get("status"),
        "backtest_classification": backtest.get("classification"),
        "edge_score": _nullable_float(edge.get("edge_score")),
    }


def _group_rows(rows: list[dict[str, Any]], *, dimension: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for value in _dimension_values(row, dimension):
            buckets[value].append(row)
    return [_metrics_for_group(dimension, key, values) for key, values in buckets.items()]


def _dimension_values(row: dict[str, Any], dimension: str) -> list[str]:
    if dimension == "symbol":
        return row.get("symbols", ["UNKNOWN"])
    if dimension == "timeframe":
        return row.get("timeframes", ["UNKNOWN"])
    if dimension == "symbol_group":
        return row.get("symbol_groups", ["unknown"])
    if dimension == "source_type":
        return row.get("source_types", ["UNKNOWN"])
    if dimension == "composite":
        values = []
        for symbol in row.get("symbols", ["UNKNOWN"]):
            for timeframe in row.get("timeframes", ["UNKNOWN"]):
                for source_type in row.get("source_types", ["UNKNOWN"]):
                    values.append("|".join([row["mechanism"], row["regime"], timeframe, symbol, source_type, row["confidence_bucket"]]))
        return values
    return [str(row.get(dimension) or "UNKNOWN")]


def _metrics_for_group(dimension: str, group_key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    expectancies = [row["expectancy"] for row in rows if row.get("expectancy") is not None]
    profit_factors = [row["profit_factor"] for row in rows if row.get("profit_factor") is not None]
    return {
        "dimension": dimension,
        "group": group_key,
        "candidate_count": count,
        "positive_replay_count": sum(bool(row.get("positive_replay")) for row in rows),
        "eligible_candidate_count": sum(bool(row.get("eligible_candidate")) for row in rows),
        "backtest_supported_count": sum(bool(row.get("backtest_supported")) for row in rows),
        "paper_forward_ready_count": sum(bool(row.get("paper_forward_ready")) for row in rows),
        "positive_replay_rate": _rate(sum(bool(row.get("positive_replay")) for row in rows), count),
        "eligible_candidate_rate": _rate(sum(bool(row.get("eligible_candidate")) for row in rows), count),
        "backtest_support_rate": _rate(sum(bool(row.get("backtest_supported")) for row in rows), count),
        "paper_forward_readiness_rate": _rate(sum(bool(row.get("paper_forward_ready")) for row in rows), count),
        "average_expectancy": _average(expectancies),
        "average_profit_factor": _average(profit_factors),
        "candidate_ids": sorted(str(row.get("candidate_id")) for row in rows),
    }


def _rank_groups(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(groups, key=_best_sort_key, reverse=True)


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    expectancies = [row["expectancy"] for row in rows if row.get("expectancy") is not None]
    profit_factors = [row["profit_factor"] for row in rows if row.get("profit_factor") is not None]
    return {
        "trials_analyzed": count,
        "positive_replay_rate": _rate(sum(row["positive_replay"] for row in rows), count),
        "eligible_candidate_rate": _rate(sum(row["eligible_candidate"] for row in rows), count),
        "backtest_support_rate": _rate(sum(row["backtest_supported"] for row in rows), count),
        "paper_forward_readiness_rate": _rate(sum(row["paper_forward_ready"] for row in rows), count),
        "average_expectancy": _average(expectancies),
        "average_profit_factor": _average(profit_factors),
    }


def _observation_index(import_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    records = (((import_report.get("batch") or {}).get("records")) or [])
    return {str(record.get("observation_id")): record for record in records if record.get("observation_id")}


def _best_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float, float, float, int]:
    return (
        _float(row.get("positive_replay_rate")),
        _float(row.get("eligible_candidate_rate")),
        _float(row.get("backtest_support_rate")),
        _float(row.get("paper_forward_readiness_rate")),
        _float(row.get("average_expectancy")),
        _float(row.get("average_profit_factor")),
        int(row.get("candidate_count") or 0),
    )


def _worst_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float, float, float, int]:
    return (
        _float(row.get("positive_replay_rate")),
        _float(row.get("eligible_candidate_rate")),
        _float(row.get("backtest_support_rate")),
        _float(row.get("paper_forward_readiness_rate")),
        _float(row.get("average_expectancy")),
        _float(row.get("average_profit_factor")),
        -int(row.get("candidate_count") or 0),
    )


def _confidence_bucket(confidence: float) -> str:
    if confidence < 0.6:
        return "LOW_<0.60"
    if confidence < 0.75:
        return "MEDIUM_0.60_0.75"
    if confidence < 0.9:
        return "HIGH_0.75_0.90"
    return "VERY_HIGH_>=0.90"


def _group_line(row: dict[str, Any]) -> str:
    return (
        f"- {row.get('dimension')}={row.get('group')}: n={row.get('candidate_count')} "
        f"positive_replay={row.get('positive_replay_rate')} eligible={row.get('eligible_candidate_rate')} "
        f"backtest_supported={row.get('backtest_support_rate')} paper_forward_ready={row.get('paper_forward_readiness_rate')} "
        f"expectancy={row.get('average_expectancy')} profit_factor={row.get('average_profit_factor')}"
    )


def _load_json(path: Path) -> tuple[dict[str, Any], Path]:
    if not path.exists():
        return {}, path
    return json.loads(path.read_text(encoding="utf-8")), path


def _nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    return round(_float(value), 6)


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
