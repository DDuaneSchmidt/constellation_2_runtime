from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import create_backtest_spec, run_candidate_backtest_spec
from .exact_replay_without_fallback import classify_candidate_exact_replay
from .market_data_schema_validation import normalize_market_data_csv
from .net_of_cost_evidence import classify_net_result
from .regime_vocabulary_bridge import mapped_replay_regimes

REPORT_DIRNAME = "timeframe_neighborhood_expansion"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"
TARGET_TIMEFRAMES = [
    ("15m", 15),
    ("30m", 30),
    ("60m", 60),
    ("2h", 120),
]
TARGET_CANDIDATES = [
    "ptc_backtest_final_651cd169dd508c4e",
    "ptc_backtest_final_8edabf7988a79611",
    "ptc_backtest_final_e962558456a60109",
]
COST_SCENARIOS_BPS = [0.0, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0]
DERIVED_BAR_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
BAR_MANIFEST_COLUMNS = [
    "symbol",
    "timeframe",
    "source_file",
    "derived_file",
    "source_rows",
    "derived_rows",
    "data_start",
    "data_end",
    "derivation_status",
]
EXACT_COLUMNS = [
    "candidate_id",
    "family_id",
    "symbol",
    "timeframe",
    "data_file",
    "sample_size",
    "expectancy",
    "profit_factor",
    "max_drawdown",
    "classification",
    "fallback_used",
    "notes",
]
COST_COLUMNS = [
    "timeframe",
    "candidate_id",
    "cost_bps",
    "sample_size",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "classification",
]
STABILITY_COLUMNS = [
    "timeframe",
    "bars",
    "candidates_tested",
    "exact_strong",
    "exact_weak",
    "exact_failed",
    "sample_size_total",
    "mean_expectancy",
    "mean_profit_factor",
    "net_survives_10bps",
    "net_failed_10bps",
    "break_even_cost_bps",
    "timeframe_classification",
    "relative_to_30m",
    "notes",
]

AUTHORITY_BOUNDARY = (
    "Research-only timeframe neighborhood expansion. No fallback data, no daily data, no alternate symbols, "
    "no trade recommendations, no candidate promotion, no broker execution, and no trading authority."
)


def run_timeframe_neighborhood_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    report = build_timeframe_neighborhood_expansion(root=root, created_at=created_at, repo_root=repo_root)
    write_timeframe_neighborhood_expansion(report, root=root)
    return report


def build_timeframe_neighborhood_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    out_dir = root_path / REPORT_DIRNAME
    derived_dir = out_dir / "derived_bars"
    created = created_at or _now()
    source_file = repo / "data" / "manual_intraday_import" / "TSLA_1m.csv"
    one_minute_rows = normalize_market_data_csv(source_file, symbol=TARGET_SYMBOL, timeframe="1m")

    derived_by_timeframe: dict[str, list[dict[str, Any]]] = {}
    manifest = []
    for timeframe, minutes in TARGET_TIMEFRAMES:
        rows = _resample_rows(one_minute_rows, minutes)
        derived_path = derived_dir / f"{TARGET_SYMBOL}_{timeframe}.csv"
        _write_csv(derived_path, DERIVED_BAR_COLUMNS, rows)
        derived_by_timeframe[timeframe] = rows
        manifest.append(
            {
                "symbol": TARGET_SYMBOL,
                "timeframe": timeframe,
                "source_file": str(source_file),
                "derived_file": str(derived_path),
                "source_rows": len(one_minute_rows),
                "derived_rows": len(rows),
                "data_start": rows[0]["timestamp"] if rows else "",
                "data_end": rows[-1]["timestamp"] if rows else "",
                "derivation_status": "DERIVED_FROM_VALIDATED_1M" if rows else "DERIVATION_FAILED",
            }
        )

    exact_rows = []
    for timeframe, _minutes in TARGET_TIMEFRAMES:
        for candidate_id in TARGET_CANDIDATES:
            exact_rows.append(_run_replay(candidate_id, timeframe, derived_by_timeframe[timeframe], derived_dir / f"{TARGET_SYMBOL}_{timeframe}.csv", created_at=created))

    cost_rows = [_cost_row(row, cost_bps) for row in exact_rows for cost_bps in COST_SCENARIOS_BPS]
    stability_rows = _stability_rows(exact_rows, cost_rows, {row["timeframe"]: row for row in manifest})
    answer = _key_question_answer(stability_rows)
    summary = {
        "target_symbol": TARGET_SYMBOL,
        "target_family_id": TARGET_FAMILY_ID,
        "target_mechanism": TARGET_MECHANISM,
        "target_regime": TARGET_REGIME,
        "timeframes_tested": [timeframe for timeframe, _minutes in TARGET_TIMEFRAMES],
        "source_1m_rows": len(one_minute_rows),
        "derived_timeframe_count": len(manifest),
        "exact_replays_run": len(exact_rows),
        "cost_rows_evaluated": len(cost_rows),
        "classification_counts": dict(Counter(row["timeframe_classification"] for row in stability_rows)),
        "key_question_answer": answer,
        "is_30m_special": answer == "30M_SPECIAL",
        "confidence_impact": "NONE",
        "no_trading_authority": True,
        "no_promotion_authority": True,
    }
    return {
        "schema_id": "atlas_v2_research_os_timeframe_neighborhood_expansion",
        "schema_version": "1.0",
        "report_type": "TIMEFRAME_NEIGHBORHOOD_EXPANSION",
        "build": "155-156",
        "created_at": created,
        "day": created[:10],
        "summary": summary,
        "source_inputs": {
            "validated_1m_source": str(source_file),
            "exact_replay_engine": "candidate_backtests.run_candidate_backtest_spec",
            "cost_model": "fixed_bps",
        },
        "fallback_policy": {
            "fallback_data_allowed": False,
            "daily_data_allowed": False,
            "alternate_symbols_allowed": False,
            "alternate_timeframes_allowed": False,
            "derived_from_validated_1m_only": True,
        },
        "generated_bar_manifest": manifest,
        "timeframe_exact_replay": exact_rows,
        "cost_robustness_by_timeframe": cost_rows,
        "timeframe_stability": stability_rows,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_timeframe_neighborhood_expansion(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "generated_bar_manifest": out_dir / "generated_bar_manifest.csv",
        "timeframe_exact_replay": out_dir / "timeframe_exact_replay.csv",
        "cost_robustness_by_timeframe": out_dir / "cost_robustness_by_timeframe.csv",
        "timeframe_stability": out_dir / "timeframe_stability.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_timeframe_neighborhood_expansion_summary(report), encoding="utf-8")
    _write_csv(paths["generated_bar_manifest"], BAR_MANIFEST_COLUMNS, report.get("generated_bar_manifest") or [])
    _write_csv(paths["timeframe_exact_replay"], EXACT_COLUMNS, report.get("timeframe_exact_replay") or [])
    _write_csv(paths["cost_robustness_by_timeframe"], COST_COLUMNS, report.get("cost_robustness_by_timeframe") or [])
    _write_csv(paths["timeframe_stability"], STABILITY_COLUMNS, report.get("timeframe_stability") or [])
    return paths


def render_timeframe_neighborhood_expansion_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 155-156 - Timeframe Neighborhood Expansion",
        "",
        f"Target: {summary.get('target_symbol')} {summary.get('target_mechanism')}/{summary.get('target_regime')}",
        f"Family: {summary.get('target_family_id')}",
        f"Timeframes tested: {', '.join(summary.get('timeframes_tested') or [])}",
        f"Exact replays run: {summary.get('exact_replays_run')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Key Question",
        "",
        f"Is 30m special, or stable nearby? {summary.get('key_question_answer')}",
        "",
        "## Timeframe Stability",
        "",
    ]
    for row in report.get("timeframe_stability") or []:
        lines.append(
            f"- {row.get('timeframe')}: {row.get('timeframe_classification')} "
            f"mean_expectancy={row.get('mean_expectancy')} mean_pf={row.get('mean_profit_factor')} "
            f"net_survives_10bps={row.get('net_survives_10bps')} break_even={row.get('break_even_cost_bps')}bps "
            f"relative_to_30m={row.get('relative_to_30m')}"
        )
    lines.extend(
        [
            "",
            "## Bar Generation",
            "",
        ]
    )
    for row in report.get("generated_bar_manifest") or []:
        lines.append(f"- {row.get('timeframe')}: {row.get('derivation_status')} rows={row.get('derived_rows')} file={row.get('derived_file')}")
    lines.extend(["", "## Authority Boundary", "", AUTHORITY_BOUNDARY, ""])
    return "\n".join(lines)


def _run_replay(candidate_id: str, timeframe: str, rows: list[dict[str, Any]], data_file: Path, *, created_at: str) -> dict[str, Any]:
    regime_original = TARGET_REGIME
    allowed_replay_regimes, mappings = mapped_replay_regimes([regime_original])
    data_rows = [
        {
            "date": str(row["timestamp"])[:10],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
        }
        for row in rows
    ]
    data_meta = {
        "available": True,
        "symbol": TARGET_SYMBOL,
        "path": str(data_file),
        "rows": len(data_rows),
        "bar_type": f"{timeframe}_ohlcv",
        "timeframe": timeframe,
        "start_date": data_rows[0]["date"] if data_rows else "",
        "end_date": data_rows[-1]["date"] if data_rows else "",
    }
    spec = create_backtest_spec(
        {
            "candidate_id": candidate_id,
            "mechanism": TARGET_MECHANISM,
            "regime_constraints": {"primary_regime": regime_original, "allowed_regimes": [regime_original]},
        },
        data_meta=data_meta,
    )
    spec["allowed_replay_regimes"] = allowed_replay_regimes or ["NO_EXECUTABLE_REGIME_EQUIVALENT"]
    spec["primary_replay_regime"] = spec["allowed_replay_regimes"][0]
    spec["regime_vocabulary_bridge"] = mappings
    spec["candidate_symbol"] = TARGET_SYMBOL
    spec["data_requirements"]["symbol_proxy"] = None
    spec["data_requirements"]["candidate_symbol_or_universe_required_for_direct_test"] = False
    result = run_candidate_backtest_spec(spec, data_rows, created_at=created_at)
    raw_notes = (result.get("warnings") or []) + (result.get("missing_data") or [])
    notes = [note for note in raw_notes if "SPY adjusted daily data" not in note and "daily bars cannot" not in note]
    notes.insert(0, f"replay rows supplied from derived {TARGET_SYMBOL} {timeframe} intraday bars; fallback_used=false")
    metrics = result.get("metrics") or {}
    sample_size = int(metrics.get("sample_size") or 0)
    expectancy = _round(metrics.get("expectancy"))
    profit_factor = _round(metrics.get("profit_factor"))
    max_drawdown = _round(metrics.get("max_drawdown"))
    return {
        "candidate_id": candidate_id,
        "family_id": TARGET_FAMILY_ID,
        "symbol": TARGET_SYMBOL,
        "timeframe": timeframe,
        "data_file": str(data_file),
        "sample_size": sample_size,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "classification": classify_candidate_exact_replay(sample_size, expectancy, profit_factor),
        "fallback_used": False,
        "notes": "; ".join(notes),
    }


def _resample_rows(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    buckets: dict[datetime, dict[str, Any]] = {}
    order: list[datetime] = []
    for row in rows:
        ts = row["timestamp"]
        if not isinstance(ts, datetime):
            ts = _parse_timestamp(str(ts))
        bucket = _bucket_start(ts, minutes)
        if bucket not in buckets:
            buckets[bucket] = {
                "timestamp": _format_timestamp(bucket),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
            order.append(bucket)
            continue
        bucket_row = buckets[bucket]
        bucket_row["high"] = max(float(bucket_row["high"]), float(row["high"]))
        bucket_row["low"] = min(float(bucket_row["low"]), float(row["low"]))
        bucket_row["close"] = float(row["close"])
        bucket_row["volume"] = float(bucket_row["volume"]) + float(row["volume"])
    return [_format_bar_row(buckets[bucket]) for bucket in sorted(order)]


def _cost_row(row: dict[str, Any], cost_bps: float) -> dict[str, Any]:
    sample = int(row.get("sample_size") or 0)
    gross = _float(row.get("expectancy"))
    gross_pf = _float(row.get("profit_factor"))
    cost = cost_bps / 10000.0
    net = None if gross is None else round(gross - cost, 6)
    net_pf = None if gross_pf is None else round(max(0.0, gross_pf - (cost_bps / 100.0)), 6)
    return {
        "timeframe": row.get("timeframe", ""),
        "candidate_id": row.get("candidate_id", ""),
        "cost_bps": cost_bps,
        "sample_size": sample,
        "gross_expectancy": gross,
        "net_expectancy": net,
        "gross_profit_factor": gross_pf,
        "net_profit_factor": net_pf,
        "classification": classify_net_result(sample, gross, net, net_pf),
    }


def _stability_rows(exact_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]], manifest_by_timeframe: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    by_tf: dict[str, list[dict[str, Any]]] = defaultdict(list)
    cost_by_tf: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in exact_rows:
        by_tf[str(row["timeframe"])].append(row)
    for row in cost_rows:
        cost_by_tf[str(row["timeframe"])].append(row)
    thirty = by_tf.get("30m") or []
    thirty_expectancy = _mean(_float(row.get("expectancy")) for row in thirty)
    out = []
    for timeframe, _minutes in TARGET_TIMEFRAMES:
        rows = by_tf.get(timeframe, [])
        costs = cost_by_tf.get(timeframe, [])
        costs_10 = [row for row in costs if float(row.get("cost_bps") or 0.0) == 10.0]
        break_even = _break_even_bps(rows)
        mean_expectancy = _mean(_float(row.get("expectancy")) for row in rows)
        mean_pf = _mean(_float(row.get("profit_factor")) for row in rows)
        classification = _classify_timeframe(rows, costs_10, break_even)
        out.append(
            {
                "timeframe": timeframe,
                "bars": manifest_by_timeframe.get(timeframe, {}).get("derived_rows", 0),
                "candidates_tested": len(rows),
                "exact_strong": sum(row.get("classification") == "EXACT_CONFIRMED_STRONG" for row in rows),
                "exact_weak": sum(row.get("classification") == "EXACT_CONFIRMED_WEAK" for row in rows),
                "exact_failed": sum(row.get("classification") == "EXACT_FAILED" for row in rows),
                "sample_size_total": sum(int(row.get("sample_size") or 0) for row in rows),
                "mean_expectancy": _round(mean_expectancy),
                "mean_profit_factor": _round(mean_pf),
                "net_survives_10bps": sum(row.get("classification") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"} for row in costs_10),
                "net_failed_10bps": sum(row.get("classification") in {"NET_FAILED", "COST_ERODED"} for row in costs_10),
                "break_even_cost_bps": _round(break_even),
                "timeframe_classification": classification,
                "relative_to_30m": _relative_to_30m(timeframe, mean_expectancy, thirty_expectancy),
                "notes": _timeframe_notes(classification, rows, costs_10),
            }
        )
    return out


def _classify_timeframe(rows: list[dict[str, Any]], costs_10: list[dict[str, Any]], break_even_bps: float | None) -> str:
    if not rows or any(int(row.get("sample_size") or 0) < 50 for row in rows):
        return "TIMEFRAME_FAILED"
    strong = sum(row.get("classification") == "EXACT_CONFIRMED_STRONG" for row in rows)
    weak = sum(row.get("classification") == "EXACT_CONFIRMED_WEAK" for row in rows)
    failed = sum(row.get("classification") == "EXACT_FAILED" for row in rows)
    net_survives_10 = sum(row.get("classification") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"} for row in costs_10)
    if failed == len(rows) or net_survives_10 == 0:
        return "TIMEFRAME_FAILED"
    if strong == len(rows) and net_survives_10 == len(rows) and (break_even_bps or 0.0) >= 15.0:
        return "TIMEFRAME_ROBUST"
    if strong + weak >= 2 and net_survives_10 >= 2:
        return "TIMEFRAME_PROMISING"
    return "TIMEFRAME_FRAGILE"


def _key_question_answer(rows: list[dict[str, Any]]) -> str:
    by_tf = {row["timeframe"]: row for row in rows}
    thirty = by_tf.get("30m", {})
    neighbors = [by_tf.get("15m", {}), by_tf.get("60m", {}), by_tf.get("2h", {})]
    supportive = [row for row in neighbors if row.get("timeframe_classification") in {"TIMEFRAME_ROBUST", "TIMEFRAME_PROMISING"}]
    if thirty.get("timeframe_classification") not in {"TIMEFRAME_ROBUST", "TIMEFRAME_PROMISING"}:
        return "30M_NOT_CONFIRMED"
    if len(supportive) >= 2:
        return "STABLE_NEARBY"
    if len(supportive) == 1:
        return "PARTIALLY_STABLE_NEARBY"
    return "30M_SPECIAL"


def _break_even_bps(rows: list[dict[str, Any]]) -> float | None:
    expectancies = [_float(row.get("expectancy")) for row in rows if _float(row.get("expectancy")) is not None]
    if not expectancies:
        return None
    return min(expectancies) * 10000.0


def _relative_to_30m(timeframe: str, expectancy: float | None, thirty_expectancy: float | None) -> str:
    if timeframe == "30m":
        return "BASELINE"
    if expectancy is None or thirty_expectancy in {None, 0.0}:
        return "UNKNOWN"
    ratio = expectancy / thirty_expectancy
    if ratio >= 0.75:
        return "SIMILAR_OR_STRONGER"
    if ratio > 0:
        return "WEAKER_POSITIVE"
    return "NEGATIVE"


def _timeframe_notes(classification: str, rows: list[dict[str, Any]], costs_10: list[dict[str, Any]]) -> str:
    counts = Counter(row.get("classification") for row in rows)
    cost_counts = Counter(row.get("classification") for row in costs_10)
    return f"exact={dict(counts)} 10bps={dict(cost_counts)} classification={classification}"


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _bucket_start(ts: datetime, minutes: int) -> datetime:
    ts = ts.astimezone(UTC)
    midnight = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    offset_minutes = int((ts - midnight).total_seconds() // 60)
    return midnight + timedelta(minutes=(offset_minutes // minutes) * minutes)


def _format_timestamp(ts: datetime) -> str:
    return ts.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_bar_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": row["timestamp"],
        "open": _round(row["open"]),
        "high": _round(row["high"]),
        "low": _round(row["low"]),
        "close": _round(row["close"]),
        "volume": int(row["volume"]),
    }


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column, "")) for column in columns})


def _csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _mean(values: Any) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present) / len(present)


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _round(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
