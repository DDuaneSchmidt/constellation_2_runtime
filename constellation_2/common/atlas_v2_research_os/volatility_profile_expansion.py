from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean, pstdev
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .exact_replay_without_fallback import _run_exact_requirement
from .market_data_schema_validation import normalize_market_data_csv
from .net_of_cost_evidence import classify_net_result

REPORT_DIRNAME = "volatility_profile_expansion"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_CANDIDATE_ID = "ptc_backtest_final_651cd169dd508c4e"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"
TARGET_TIMEFRAME = "30m"
PRIMARY_COST_BPS = 10.0
VALID_EXACT_STATUSES = {"VALID_READY", "VALID_WITH_WARNINGS"}

VOLATILITY_BUCKETS = {
    "HIGH_VOLATILITY": ["TSLA", "NVDA", "AMD"],
    "MEDIUM_VOLATILITY": ["META", "NFLX", "AMZN"],
    "LOWER_VOLATILITY": ["AAPL", "MSFT", "JPM"],
}

BUCKET_COLUMNS = [
    "volatility_bucket",
    "symbols_requested",
    "symbols_replayed",
    "symbols_blocked",
    "mean_realized_volatility",
    "mean_gross_expectancy",
    "mean_net_expectancy_10bps",
    "mean_gross_profit_factor",
    "mean_net_profit_factor_10bps",
    "exact_survivors",
    "net_survivors_10bps",
    "exact_failures",
    "bucket_classification",
    "notes",
]
SURVIVOR_COLUMNS = [
    "symbol",
    "volatility_bucket",
    "realized_volatility",
    "sample_size",
    "gross_expectancy",
    "gross_profit_factor",
    "net_expectancy_10bps",
    "net_profit_factor_10bps",
    "exact_classification",
    "net_classification_10bps",
    "volatility_classification",
    "data_file",
]
FAILURE_COLUMNS = [
    "symbol",
    "volatility_bucket",
    "failure_stage",
    "failure_reason",
    "required_file",
    "exact_classification",
    "net_classification_10bps",
]
COST_COLUMNS = [
    "symbol",
    "volatility_bucket",
    "cost_bps",
    "sample_size",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "cost_erosion",
    "net_classification",
]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "exact_replay_allowed": True,
    "net_of_cost_analysis_allowed": True,
    "fallback_allowed": False,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "candidate_promotion_authorized": False,
}


def run_volatility_profile_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    report = build_volatility_profile_expansion(root=root, created_at=created_at, repo_root=repo_root)
    write_volatility_profile_expansion(report, root=root)
    return report


def build_volatility_profile_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    exact_files = _validated_exact_files(root_path, repo)

    symbol_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    cost_rows: list[dict[str, Any]] = []
    for bucket, symbols in VOLATILITY_BUCKETS.items():
        for symbol in symbols:
            valid_file = exact_files.get((symbol, TARGET_TIMEFRAME))
            if not valid_file:
                failures.append(
                    {
                        "symbol": symbol,
                        "volatility_bucket": bucket,
                        "failure_stage": "EXACT_REPLAY",
                        "failure_reason": "No validated exact 30m intraday file; fallback is disabled.",
                        "required_file": f"data/manual_intraday_import/{symbol}_{TARGET_TIMEFRAME}.csv",
                        "exact_classification": "EXACT_BLOCKED",
                        "net_classification_10bps": "NET_BLOCKED",
                    }
                )
                continue
            row = _symbol_replay_row(symbol, bucket, valid_file, created_at=created)
            symbol_rows.append(row)
            cost_rows.append(_cost_row(row, PRIMARY_COST_BPS))
            if row["volatility_classification"] == "VOLATILITY_FAILED":
                failures.append(
                    {
                        "symbol": symbol,
                        "volatility_bucket": bucket,
                        "failure_stage": "NET_OF_COST" if row.get("net_classification_10bps") == "COST_ERODED" else "EXACT_REPLAY",
                        "failure_reason": _failure_reason(row),
                        "required_file": row.get("data_file", ""),
                        "exact_classification": row.get("exact_classification"),
                        "net_classification_10bps": row.get("net_classification_10bps"),
                    }
                )

    bucket_rows = _bucket_rows(symbol_rows, failures)
    summary = _summary(bucket_rows, symbol_rows, failures)
    return {
        "schema_id": "atlas_v2_research_os_volatility_profile_expansion",
        "schema_version": "1.0",
        "report_type": "VOLATILITY_PROFILE_EXPANSION",
        "builds": ["179", "180", "181", "182"],
        "created_at": created,
        "day": created[:10],
        "target": {
            "family_id": TARGET_FAMILY_ID,
            "candidate_id": TARGET_CANDIDATE_ID,
            "mechanism": TARGET_MECHANISM,
            "regime": TARGET_REGIME,
            "timeframe": TARGET_TIMEFRAME,
        },
        "volatility_buckets": VOLATILITY_BUCKETS,
        "cost_assumptions": {"primary_round_trip_cost_bps": PRIMARY_COST_BPS, "cost_model": "fixed_bps_net_of_expectancy", "position_sizing_applied": False},
        "source_inputs": {
            "exact_coverage_import_validator": str(root_path / "exact_coverage_import_validator" / "latest.json"),
            "verified_runtime_graph": "/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-06/verified_runtime_graph.v1.json",
        },
        "summary": summary,
        "bucket_results": bucket_rows,
        "volatility_survivors": [row for row in symbol_rows if row.get("volatility_classification") in {"VOLATILITY_CONFIRMED", "VOLATILITY_WEAK"}],
        "volatility_failures": failures,
        "bucket_cost_analysis": cost_rows,
        "overall_classification": summary["overall_classification"],
        "authority_boundary": AUTHORITY_BOUNDARY,
        "guardrails": [
            "Exact replay uses only validated 30m intraday files.",
            "No daily, alternate-symbol, or alternate-timeframe fallback is used.",
            "Bucket labels are the requested test buckets; realized volatility is measured from the validated exact input when present.",
            "Research-only output. No trading, broker execution, capital allocation, recommendations, automatic paper placement, or promotion.",
        ],
    }


def write_volatility_profile_expansion(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_root = Path(root) / REPORT_DIRNAME
    out_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_root / "latest.json",
        "latest_summary": out_root / "latest_summary.md",
        "bucket_results": out_root / "bucket_results.csv",
        "survivors": out_root / "volatility_survivors.csv",
        "failures": out_root / "volatility_failures.csv",
        "cost": out_root / "bucket_cost_analysis.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_volatility_profile_expansion_summary(report), encoding="utf-8")
    _write_csv(paths["bucket_results"], BUCKET_COLUMNS, report.get("bucket_results") or [])
    _write_csv(paths["survivors"], SURVIVOR_COLUMNS, report.get("volatility_survivors") or [])
    _write_csv(paths["failures"], FAILURE_COLUMNS, report.get("volatility_failures") or [])
    _write_csv(paths["cost"], COST_COLUMNS, report.get("bucket_cost_analysis") or [])
    return paths


def render_volatility_profile_expansion_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 179-182 - Volatility Profile Expansion",
        "",
        f"Created: {report.get('created_at')}",
        f"Overall classification: {report.get('overall_classification')}",
        f"Symbols replayed: {summary.get('symbols_replayed')}",
        f"Symbols blocked: {summary.get('symbols_blocked')}",
        "",
        "## Bucket Results",
        "",
    ]
    for row in report.get("bucket_results") or []:
        lines.append(
            f"- {row.get('volatility_bucket')}: {row.get('bucket_classification')} "
            f"replayed={row.get('symbols_replayed')} net_survivors={row.get('net_survivors_10bps')} "
            f"mean_net={row.get('mean_net_expectancy_10bps')}"
        )
    lines.extend(["", "## Guardrails", "", "Exact replay only; fallback disabled. Output is research-only.", ""])
    return "\n".join(lines)


def classify_volatility_symbol(row: dict[str, Any]) -> str:
    net = row.get("net_classification_10bps")
    exact = row.get("exact_classification")
    if net == "NET_SURVIVES_STRONG" and exact == "EXACT_CONFIRMED_STRONG":
        return "VOLATILITY_CONFIRMED"
    if net in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}:
        return "VOLATILITY_WEAK"
    return "VOLATILITY_FAILED"


def classify_volatility_bucket(row: dict[str, Any]) -> str:
    replayed = int(row.get("symbols_replayed") or 0)
    net_survivors = int(row.get("net_survivors_10bps") or 0)
    if replayed <= 0:
        return "VOLATILITY_FAILED"
    if net_survivors >= 2 and float(row.get("mean_net_expectancy_10bps") or 0.0) > 0:
        return "VOLATILITY_CONFIRMED"
    if net_survivors == 1:
        return "VOLATILITY_WEAK"
    return "VOLATILITY_FAILED"


def _symbol_replay_row(symbol: str, bucket: str, valid_file: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    requirement = {
        "candidate_id": TARGET_CANDIDATE_ID,
        "family_id": TARGET_FAMILY_ID,
        "symbol": symbol,
        "timeframe": TARGET_TIMEFRAME,
        "mechanism": TARGET_MECHANISM,
        "regime": TARGET_REGIME,
    }
    exact = _run_exact_requirement(requirement, valid_file, created_at=created_at)
    gross = exact.get("expectancy")
    gross_pf = exact.get("profit_factor")
    cost = PRIMARY_COST_BPS / 10000.0
    net = None if gross is None else round(float(gross) - cost, 6)
    net_pf = None if gross_pf is None else round(max(0.0, float(gross_pf) - (PRIMARY_COST_BPS / 100.0)), 6)
    row = {
        "symbol": symbol,
        "volatility_bucket": bucket,
        "realized_volatility": _realized_volatility(valid_file.get("data_file", ""), symbol),
        "sample_size": int(exact.get("sample_size") or 0),
        "gross_expectancy": gross,
        "gross_profit_factor": gross_pf,
        "net_expectancy_10bps": net,
        "net_profit_factor_10bps": net_pf,
        "exact_classification": exact.get("classification"),
        "net_classification_10bps": classify_net_result(int(exact.get("sample_size") or 0), gross, net, net_pf),
        "data_file": exact.get("data_file"),
    }
    return {**row, "volatility_classification": classify_volatility_symbol(row)}


def _cost_row(row: dict[str, Any], cost_bps: float) -> dict[str, Any]:
    cost = cost_bps / 10000.0
    return {
        "symbol": row.get("symbol"),
        "volatility_bucket": row.get("volatility_bucket"),
        "cost_bps": cost_bps,
        "sample_size": row.get("sample_size"),
        "gross_expectancy": row.get("gross_expectancy"),
        "net_expectancy": row.get("net_expectancy_10bps"),
        "gross_profit_factor": row.get("gross_profit_factor"),
        "net_profit_factor": row.get("net_profit_factor_10bps"),
        "cost_erosion": round(cost, 6),
        "net_classification": row.get("net_classification_10bps"),
    }


def _bucket_rows(symbol_rows: list[dict[str, Any]], failures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows_by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    failures_by_bucket = Counter(row["volatility_bucket"] for row in failures if row.get("exact_classification") == "EXACT_BLOCKED")
    for row in symbol_rows:
        rows_by_bucket[row["volatility_bucket"]].append(row)
    out = []
    for bucket, symbols in VOLATILITY_BUCKETS.items():
        rows = rows_by_bucket.get(bucket, [])
        bucket_row = {
            "volatility_bucket": bucket,
            "symbols_requested": len(symbols),
            "symbols_replayed": len(rows),
            "symbols_blocked": failures_by_bucket.get(bucket, 0),
            "mean_realized_volatility": _mean(row.get("realized_volatility") for row in rows),
            "mean_gross_expectancy": _mean(row.get("gross_expectancy") for row in rows),
            "mean_net_expectancy_10bps": _mean(row.get("net_expectancy_10bps") for row in rows),
            "mean_gross_profit_factor": _mean(row.get("gross_profit_factor") for row in rows),
            "mean_net_profit_factor_10bps": _mean(row.get("net_profit_factor_10bps") for row in rows),
            "exact_survivors": sum(row.get("exact_classification") in {"EXACT_CONFIRMED_STRONG", "EXACT_CONFIRMED_WEAK"} for row in rows),
            "net_survivors_10bps": sum(row.get("net_classification_10bps") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"} for row in rows),
            "exact_failures": sum(row.get("exact_classification") == "EXACT_FAILED" for row in rows),
            "notes": _bucket_notes(bucket, rows, failures),
        }
        out.append({**bucket_row, "bucket_classification": classify_volatility_bucket(bucket_row)})
    return out


def _summary(bucket_rows: list[dict[str, Any]], symbol_rows: list[dict[str, Any]], failures: list[dict[str, Any]]) -> dict[str, Any]:
    bucket_classifications = {row["volatility_bucket"]: row["bucket_classification"] for row in bucket_rows}
    high = next((row for row in bucket_rows if row["volatility_bucket"] == "HIGH_VOLATILITY"), {})
    others = [row for row in bucket_rows if row["volatility_bucket"] != "HIGH_VOLATILITY"]
    high_net = float(high.get("mean_net_expectancy_10bps") or 0.0)
    other_best = max((float(row.get("mean_net_expectancy_10bps") or 0.0) for row in others), default=0.0)
    if bucket_classifications.get("HIGH_VOLATILITY") == "VOLATILITY_CONFIRMED" and high_net > other_best:
        overall = "VOLATILITY_CONFIRMED"
    elif bucket_classifications.get("HIGH_VOLATILITY") in {"VOLATILITY_CONFIRMED", "VOLATILITY_WEAK"} and high_net > other_best:
        overall = "VOLATILITY_WEAK"
    else:
        overall = "VOLATILITY_FAILED"
    return {
        "symbols_requested": sum(len(symbols) for symbols in VOLATILITY_BUCKETS.values()),
        "symbols_replayed": len(symbol_rows),
        "symbols_blocked": sum(1 for row in failures if row.get("exact_classification") == "EXACT_BLOCKED"),
        "bucket_classification_counts": dict(Counter(row.get("bucket_classification") for row in bucket_rows)),
        "symbol_classification_counts": dict(Counter(row.get("volatility_classification") for row in symbol_rows)),
        "overall_classification": overall,
        "key_question_answer": _key_question(overall, high_net, other_best),
    }


def _validated_exact_files(root: Path, repo: Path) -> dict[tuple[str, str], dict[str, Any]]:
    payload = _read_json(root / "exact_coverage_import_validator" / "latest.json", {})
    rows = payload.get("import_validation_matrix") or []
    out = {}
    for row in rows:
        status = str(row.get("validation_status") or row.get("status") or "").upper()
        if status not in VALID_EXACT_STATUSES:
            continue
        symbol = str(row.get("symbol") or "").upper()
        timeframe = str(row.get("timeframe") or "").lower()
        data_file = row.get("normalized_file") or row.get("actual_file") or row.get("data_file")
        if not symbol or not timeframe or not data_file:
            continue
        path = Path(str(data_file))
        if not path.is_absolute():
            path = repo / path
        if path.exists():
            out[(symbol, timeframe)] = {**row, "symbol": symbol, "timeframe": timeframe, "data_file": str(path), "status": status}
    return out


def _realized_volatility(data_file: str, symbol: str) -> float | None:
    rows = normalize_market_data_csv(data_file, symbol=symbol, timeframe=TARGET_TIMEFRAME)
    closes = [float(row["close"]) for row in rows if float(row.get("close") or 0.0) > 0]
    returns = [math.log(closes[index] / closes[index - 1]) for index in range(1, len(closes)) if closes[index - 1] > 0]
    if len(returns) < 2:
        return None
    return round(pstdev(returns) * math.sqrt(13 * 252), 6)


def _failure_reason(row: dict[str, Any]) -> str:
    if row.get("net_classification_10bps") == "COST_ERODED":
        return "Positive gross edge did not survive 10 bps round-trip cost."
    if row.get("exact_classification") == "EXACT_FAILED":
        return "Exact replay failed before cost adjustment."
    return "Exact replay did not produce a net survivor."


def _bucket_notes(bucket: str, rows: list[dict[str, Any]], failures: list[dict[str, Any]]) -> str:
    missing = [row["symbol"] for row in failures if row.get("volatility_bucket") == bucket and row.get("exact_classification") == "EXACT_BLOCKED"]
    parts = []
    if missing:
        parts.append(f"blocked_missing_exact={','.join(missing)}")
    if rows:
        parts.append("exact_replay_without_fallback")
    return "; ".join(parts)


def _key_question(overall: str, high_net: float, other_best: float) -> str:
    if overall == "VOLATILITY_CONFIRMED":
        return "High-volatility bucket produced confirmed net-of-cost evidence across multiple available exact replays."
    if overall == "VOLATILITY_WEAK":
        return "Volatility may contribute, but high-bucket evidence is incomplete or not confirmed across multiple symbols."
    return "Available exact replay evidence does not support volatility as the true explanatory variable."


def _mean(values: Any) -> float | None:
    clean = [float(value) for value in values if value not in {None, ""}]
    if not clean:
        return None
    return round(fmean(clean), 6)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column, "")) for column in columns})


def _csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
