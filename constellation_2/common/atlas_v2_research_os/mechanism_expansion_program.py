from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import create_backtest_spec, run_candidate_backtest_spec
from .exact_replay_without_fallback import VALID_EXACT_STATUSES, classify_candidate_exact_replay
from .market_data_schema_validation import normalize_market_data_csv
from .net_of_cost_evidence import classify_net_result
from .regime_vocabulary_bridge import mapped_replay_regimes

REPORT_DIRNAME = "mechanism_expansion_program"
TARGET_MECHANISMS = [
    ("BREAKOUT", "CHOP"),
    ("MEAN_REVERSION", "TRENDING"),
    ("EVENT_REACTION", "CHOP"),
    ("REVERSAL", "TRENDING"),
]
TARGET_SYMBOLS = ["AAPL", "AMZN", "BAC", "IWM", "JPM", "META", "MSFT", "SPY", "TLT", "TSLA", "USO"]
BASE_TIMEFRAMES = ["30m", "1h"]
COST_SCENARIOS_BPS = [1.0, 2.0, 5.0, 10.0, 15.0, 25.0]
FORBIDDEN_ACTIONS = [
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
]

REPLAY_COLUMNS = [
    "candidate_id",
    "family_id",
    "mechanism",
    "regime",
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
FAMILY_COLUMNS = [
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "candidate_count",
    "symbols_tested",
    "exact_replays_run",
    "best_net_cost_bps",
    "survivor_count_at_best_cost",
    "strong_survivors_at_best_cost",
    "weak_survivors_at_best_cost",
    "survivor_density_at_best_cost",
    "family_classification",
]
SYMBOL_COLUMNS = [
    "symbol",
    "mechanism",
    "regime",
    "timeframe",
    "exact_replays_run",
    "best_net_cost_bps",
    "net_survivors",
    "net_survivor_density",
    "classification",
]
COST_COLUMNS = [
    "cost_bps",
    "candidate_id",
    "family_id",
    "mechanism",
    "regime",
    "symbol",
    "timeframe",
    "sample_size",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "classification",
]
BLOCKED_COLUMNS = ["candidate_id", "family_id", "mechanism", "regime", "symbol", "timeframe", "blocker", "required_file", "reason"]
DENSITY_COLUMNS = [
    "mechanism",
    "regime",
    "cost_bps",
    "families_tested",
    "exact_replays_run",
    "strong_survivors",
    "weak_survivors",
    "cost_eroded",
    "failed",
    "blocked",
    "insufficient_sample",
    "survivor_density",
    "mechanism_classification",
]
MATRIX_COLUMNS = ["mechanism", "regime", "timeframe", "families_tested", "exact_replays_run", "blocked", "best_cost_bps", "survivor_density", "mechanism_classification"]


def run_mechanism_expansion_program(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    report = build_mechanism_expansion_program(root=root, created_at=created_at, repo_root=repo_root)
    write_mechanism_expansion_program(report, root=root)
    return report


def build_mechanism_expansion_program(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    families = _selected_families(root_path)
    exact_index, validation_path = _valid_exact_index(root_path, repo)
    tested_timeframes = BASE_TIMEFRAMES + (["15m"] if _can_derive_15m(repo) else [])

    exact_rows: list[dict[str, Any]] = []
    blocked_rows: list[dict[str, Any]] = []
    replay_data_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for family in families:
        timeframe = _norm_timeframe(family.get("timeframe"))
        if timeframe not in tested_timeframes:
            continue
        candidate_ids = sorted(set(family.get("candidate_ids") or family.get("top_candidate_ids") or [family.get("candidate_id", "")]))
        for candidate_id in candidate_ids:
            if not candidate_id:
                continue
            for symbol in TARGET_SYMBOLS:
                requirement = {**family, "candidate_id": candidate_id, "symbol": symbol, "timeframe": timeframe}
                replay_input = _load_replay_input(symbol, timeframe, exact_index, repo)
                if replay_input.get("blocked"):
                    blocked_rows.append(_blocked_row(requirement, replay_input["blocker"], replay_input["required_file"], replay_input["reason"]))
                    continue
                cache_key = (symbol, timeframe)
                if cache_key not in replay_data_cache:
                    replay_data_cache[cache_key] = _load_replay_rows(symbol, timeframe, replay_input)
                exact_rows.append(_run_requirement(requirement, {**replay_input, "data_rows": replay_data_cache[cache_key]}, created_at=created))

    cost_rows = [_cost_row(row, cost_bps) for cost_bps in COST_SCENARIOS_BPS for row in exact_rows]
    family_rows = _family_rows(families, exact_rows, cost_rows, blocked_rows)
    symbol_rows = _symbol_rows(exact_rows, cost_rows)
    density_rows = _density_rows(families, exact_rows, cost_rows, blocked_rows)
    matrix_rows = _matrix_rows(families, exact_rows, cost_rows, blocked_rows)
    summary = _summary(families, exact_rows, cost_rows, blocked_rows, family_rows, density_rows)
    return {
        "schema_id": "atlas_v2_research_os_mechanism_expansion_program",
        "schema_version": "1.0",
        "report_type": "MECHANISM_EXPANSION_PROGRAM",
        "builds": "187-190",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "family_definitions": str(root_path / "candidate_family_discovery" / "latest.json"),
            "exact_data_validation": str(validation_path) if validation_path else "",
        },
        "target_mechanisms": [{"mechanism": mechanism, "regime": regime} for mechanism, regime in TARGET_MECHANISMS],
        "target_symbols": TARGET_SYMBOLS,
        "timeframes_tested": tested_timeframes,
        "cost_scenarios_bps": COST_SCENARIOS_BPS,
        "fallback_policy": {
            "daily_fallback_allowed": False,
            "alternate_symbol_fallback_allowed": False,
            "alternate_timeframe_fallback_allowed": False,
            "derived_15m_allowed_only_from_local_1m": True,
        },
        "summary": summary,
        "exact_replay_results": exact_rows,
        "family_mechanism_results": family_rows,
        "symbol_mechanism_results": symbol_rows,
        "cost_adjusted_mechanism_results": cost_rows,
        "blocked_mechanism_tests": blocked_rows,
        "survivor_density_report": density_rows,
        "mechanism_survivor_matrix": matrix_rows,
        "answers": _answers(summary, family_rows, symbol_rows, exact_rows, cost_rows),
        "authority_boundary": {
            "research_only": True,
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
    }


def write_mechanism_expansion_program(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "mechanism_survivor_matrix": out_dir / "mechanism_survivor_matrix.csv",
        "family_mechanism_results": out_dir / "family_mechanism_results.csv",
        "symbol_mechanism_results": out_dir / "symbol_mechanism_results.csv",
        "cost_adjusted_mechanism_results": out_dir / "cost_adjusted_mechanism_results.csv",
        "blocked_mechanism_tests": out_dir / "blocked_mechanism_tests.csv",
        "survivor_density_report": out_dir / "survivor_density_report.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_mechanism_expansion_summary(report), encoding="utf-8")
    _write_csv(paths["mechanism_survivor_matrix"], MATRIX_COLUMNS, report.get("mechanism_survivor_matrix") or [])
    _write_csv(paths["family_mechanism_results"], FAMILY_COLUMNS, report.get("family_mechanism_results") or [])
    _write_csv(paths["symbol_mechanism_results"], SYMBOL_COLUMNS, report.get("symbol_mechanism_results") or [])
    _write_csv(paths["cost_adjusted_mechanism_results"], COST_COLUMNS, report.get("cost_adjusted_mechanism_results") or [])
    _write_csv(paths["blocked_mechanism_tests"], BLOCKED_COLUMNS, report.get("blocked_mechanism_tests") or [])
    _write_csv(paths["survivor_density_report"], DENSITY_COLUMNS, report.get("survivor_density_report") or [])
    return paths


def render_mechanism_expansion_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    answers = report.get("answers", {})
    lines = [
        "# Builds 187-190 - Mechanism Expansion Program",
        "",
        f"Families tested: {summary.get('families_tested')}",
        f"Exact replays run: {summary.get('exact_replays_run')}",
        f"Blocked tests: {summary.get('blocked_tests')}",
        f"Cost scenarios: {', '.join(f'{cost:g} bps' for cost in report.get('cost_scenarios_bps', []))}",
        f"Best mechanism: {summary.get('best_mechanism') or 'none'}",
        f"Best family: {summary.get('best_family') or 'none'}",
        "",
        "## Key Questions",
        "",
        f"- Does Atlas find survivors outside TSLA reversal? {answers.get('survivors_outside_tsla_reversal')}",
        f"- Best net-of-cost survivor density: {answers.get('best_net_survivor_density')}",
        f"- Symbol concentration: {answers.get('symbol_concentration')}",
        f"- Stronger than TSLA 30m pocket: {answers.get('stronger_than_tsla_30m')}",
        "",
        "## Authority",
        "",
        "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
        "",
    ]
    return "\n".join(lines)


def classify_mechanism_candidate(sample_size: int, gross_expectancy: float | None, gross_profit_factor: float | None, cost_classifications: list[str]) -> str:
    if sample_size <= 0:
        return "MECHANISM_BLOCKED"
    if sample_size < 50:
        return "INSUFFICIENT_SAMPLE"
    highest = _highest_surviving_cost(cost_classifications)
    if highest >= 10.0:
        return "MECHANISM_SURVIVOR_STRONG"
    if highest >= 1.0:
        return "MECHANISM_SURVIVOR_WEAK"
    if (gross_expectancy or 0.0) > 0 and (gross_profit_factor or 0.0) > 1.0:
        return "MECHANISM_COST_ERODED"
    return "MECHANISM_FAILED"


def classify_cost_adjusted_candidate(sample_size: int, gross_expectancy: float | None, net_expectancy: float | None, net_profit_factor: float | None) -> str:
    net_classification = classify_net_result(sample_size, gross_expectancy, net_expectancy, net_profit_factor)
    if net_classification == "NET_SURVIVES_STRONG":
        return "MECHANISM_SURVIVOR_STRONG"
    if net_classification == "NET_SURVIVES_WEAK":
        return "MECHANISM_SURVIVOR_WEAK"
    if net_classification == "COST_ERODED":
        return "MECHANISM_COST_ERODED"
    if net_classification == "NET_INSUFFICIENT_SAMPLE":
        return "INSUFFICIENT_SAMPLE"
    if net_classification == "NET_BLOCKED":
        return "MECHANISM_BLOCKED"
    return "MECHANISM_FAILED"


def classify_mechanism_level(exact_count: int, blocked_count: int, survivor_density: float, strong_count: int) -> str:
    if exact_count <= 0 and blocked_count > 0:
        return "MECHANISM_INSUFFICIENT_DATA"
    if survivor_density > 0 and strong_count > 0:
        return "MECHANISM_PROMISING"
    if survivor_density > 0:
        return "MECHANISM_FRAGILE"
    if exact_count <= 0:
        return "MECHANISM_INSUFFICIENT_DATA"
    return "MECHANISM_FAILED"


def _selected_families(root: Path) -> list[dict[str, Any]]:
    payload = _read_json(root / "candidate_family_discovery" / "latest.json", [])
    raw = payload if isinstance(payload, list) else payload.get("families") or payload.get("candidate_families") or []
    selected = []
    target_pairs = set(TARGET_MECHANISMS)
    for row in raw:
        if not isinstance(row, dict):
            continue
        mechanism = _upper(row.get("mechanism") or row.get("dominant_mechanism"))
        regime = _upper(row.get("regime") or row.get("dominant_regime"))
        timeframe = _norm_timeframe(row.get("timeframe") or row.get("dominant_timeframe") or _first(row.get("timeframes")))
        if (mechanism, regime) not in target_pairs or timeframe not in {"15m", "30m", "1h"}:
            continue
        candidate_ids = sorted(set(row.get("candidate_ids") or row.get("top_candidate_ids") or []))
        selected.append(
            {
                "family_id": row.get("family_id", ""),
                "mechanism": mechanism,
                "regime": regime,
                "timeframe": timeframe,
                "candidate_ids": candidate_ids[:1],
                "candidate_variant_count": len(candidate_ids),
                "family_name": row.get("family_name", ""),
            }
        )
    return sorted(selected, key=lambda row: (row["mechanism"], row["regime"], row["timeframe"], row["family_id"]))


def _valid_exact_index(root: Path, repo: Path) -> tuple[dict[tuple[str, str], dict[str, Any]], Path | None]:
    payload, path = _load_validation(root)
    rows = []
    for key in ["revalidation_matrix", "file_validations", "validated_files", "exact_file_validations", "validations"]:
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    index = {}
    for row in rows:
        status = _upper(row.get("validation_status") or row.get("status") or row.get("readiness_status") or row.get("classification"))
        if status not in VALID_EXACT_STATUSES:
            continue
        symbol = _upper(row.get("symbol"))
        timeframe = _norm_timeframe(row.get("timeframe"))
        data_file = _first_present(row, ["revalidated_file", "data_file", "normalized_path", "source_file", "path", "file_path", "csv_path"])
        if not symbol or not timeframe or not data_file:
            continue
        file_path = Path(data_file)
        if not file_path.is_absolute():
            file_path = repo / file_path
        index[(symbol, timeframe)] = {**row, "symbol": symbol, "timeframe": timeframe, "data_file": str(file_path)}
    return index, path


def _load_validation(root: Path) -> tuple[dict[str, Any], Path | None]:
    for path in [
        root / "exact_data_repair_loop" / "latest.json",
        root / "exact_coverage_import_validator" / "latest.json",
        root / "exact_coverage_import_validation" / "latest.json",
    ]:
        if path.exists():
            return _read_json(path, {}), path
    return {}, None


def _can_derive_15m(repo: Path) -> bool:
    return any((repo / "data" / "manual_intraday_import" / f"{symbol}_1m.csv").exists() for symbol in TARGET_SYMBOLS)


def _load_replay_input(symbol: str, timeframe: str, exact_index: dict[tuple[str, str], dict[str, Any]], repo: Path) -> dict[str, Any]:
    if timeframe in BASE_TIMEFRAMES:
        valid = exact_index.get((symbol, timeframe))
        if not valid:
            return {"blocked": True, "blocker": "MISSING_EXACT_FILE", "required_file": f"{symbol}_{timeframe}.csv", "reason": "No validated exact local bar file exists; fallback disabled."}
        return {"blocked": False, "data_file": valid["data_file"], "timeframe": timeframe, "derived": False}
    if timeframe == "15m":
        one_minute = repo / "data" / "manual_intraday_import" / f"{symbol}_1m.csv"
        if not one_minute.exists():
            return {"blocked": True, "blocker": "MISSING_VALIDATED_1M_FILE", "required_file": str(one_minute), "reason": "15m replay is allowed only when derivable from local 1m data."}
        return {"blocked": False, "data_file": str(one_minute), "timeframe": timeframe, "derived": True}
    return {"blocked": True, "blocker": "UNVALIDATED_TIMEFRAME", "required_file": f"{symbol}_{timeframe}.csv", "reason": "Only 30m, 1h, and derivable 15m bars are in scope."}


def _load_replay_rows(symbol: str, timeframe: str, replay_input: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = normalize_market_data_csv(replay_input["data_file"], symbol=symbol, timeframe="1m" if replay_input.get("derived") else timeframe)
    bars = _derive_15m(normalized) if replay_input.get("derived") else normalized
    return [{"date": str(row["timestamp"])[:10], "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"], "volume": row["volume"]} for row in bars]


def _run_requirement(requirement: dict[str, Any], replay_input: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    symbol = _upper(requirement.get("symbol"))
    timeframe = _norm_timeframe(requirement.get("timeframe"))
    data_rows = replay_input.get("data_rows") or _load_replay_rows(symbol, timeframe, replay_input)
    mechanism = _upper(requirement.get("mechanism"))
    regime = _upper(requirement.get("regime"))
    allowed_replay_regimes, mappings = mapped_replay_regimes([regime])
    spec = create_backtest_spec(
        {
            "candidate_id": requirement.get("candidate_id"),
            "mechanism": mechanism,
            "regime_constraints": {"primary_regime": regime, "allowed_regimes": [regime]},
        },
        data_meta={
            "available": True,
            "symbol": symbol,
            "path": replay_input["data_file"],
            "rows": len(data_rows),
            "bar_type": f"{timeframe}_ohlcv",
            "timeframe": timeframe,
            "start_date": data_rows[0]["date"] if data_rows else "",
            "end_date": data_rows[-1]["date"] if data_rows else "",
        },
    )
    spec["allowed_replay_regimes"] = allowed_replay_regimes or ["NO_EXECUTABLE_REGIME_EQUIVALENT"]
    spec["primary_replay_regime"] = spec["allowed_replay_regimes"][0]
    spec["regime_vocabulary_bridge"] = mappings
    spec["candidate_symbol"] = symbol
    spec["data_requirements"]["symbol_proxy"] = None
    spec["data_requirements"]["candidate_symbol_or_universe_required_for_direct_test"] = False
    result = run_candidate_backtest_spec(spec, data_rows, created_at=created_at)
    metrics = result.get("metrics") or {}
    sample_size = int(metrics.get("sample_size") or 0)
    expectancy = _round(metrics.get("expectancy"))
    profit_factor = _round(metrics.get("profit_factor"))
    return {
        "candidate_id": requirement.get("candidate_id", ""),
        "family_id": requirement.get("family_id", ""),
        "mechanism": mechanism,
        "regime": regime,
        "symbol": symbol,
        "timeframe": timeframe,
        "data_file": replay_input["data_file"],
        "sample_size": sample_size,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "max_drawdown": _round(metrics.get("max_drawdown")),
        "classification": classify_candidate_exact_replay(sample_size, expectancy, profit_factor),
        "fallback_used": False,
        "notes": ("derived from validated local 1m bars; " if replay_input.get("derived") else "") + "; ".join((result.get("warnings") or []) + (result.get("missing_data") or [])),
    }


def _derive_15m(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        timestamp = _parse_timestamp(row["timestamp"])
        bucket = timestamp.replace(minute=(timestamp.minute // 15) * 15, second=0, microsecond=0)
        buckets[bucket].append(row)
    out = []
    for timestamp in sorted(buckets):
        group = buckets[timestamp]
        out.append({"timestamp": timestamp.isoformat().replace("+00:00", "Z"), "open": group[0]["open"], "high": max(row["high"] for row in group), "low": min(row["low"] for row in group), "close": group[-1]["close"], "volume": sum(row["volume"] for row in group)})
    return out


def _cost_row(row: dict[str, Any], cost_bps: float) -> dict[str, Any]:
    gross = _float(row.get("expectancy"))
    gross_pf = _float(row.get("profit_factor"))
    cost = round(cost_bps / 10000.0, 6)
    net = None if gross is None else round(gross - cost, 6)
    net_pf = None if gross_pf is None else round(max(0.0, gross_pf - (cost_bps / 100.0)), 6)
    sample_size = int(row.get("sample_size") or 0)
    classification = classify_cost_adjusted_candidate(sample_size, gross, net, net_pf)
    return {
        "cost_bps": cost_bps,
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "mechanism": row.get("mechanism", ""),
        "regime": row.get("regime", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "sample_size": sample_size,
        "gross_expectancy": gross,
        "net_expectancy": net,
        "gross_profit_factor": gross_pf,
        "net_profit_factor": net_pf,
        "classification": classification,
    }


def _family_rows(families: list[dict[str, Any]], exact_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]], blocked_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_family = _group(exact_rows, "family_id")
    cost_by_family = _group(cost_rows, "family_id")
    blocked_by_family = Counter(row["family_id"] for row in blocked_rows)
    out = []
    for family in families:
        family_id = family["family_id"]
        rows = by_family.get(family_id, [])
        costs = cost_by_family.get(family_id, [])
        best_cost, best_density, counts = _best_density(costs)
        out.append(
            {
                "family_id": family_id,
                "mechanism": family["mechanism"],
                "regime": family["regime"],
                "timeframe": family["timeframe"],
                "candidate_count": len(set(family.get("candidate_ids") or [])),
                "symbols_tested": len(set(row["symbol"] for row in rows)),
                "exact_replays_run": len(rows),
                "best_net_cost_bps": best_cost,
                "survivor_count_at_best_cost": counts["MECHANISM_SURVIVOR_STRONG"] + counts["MECHANISM_SURVIVOR_WEAK"],
                "strong_survivors_at_best_cost": counts["MECHANISM_SURVIVOR_STRONG"],
                "weak_survivors_at_best_cost": counts["MECHANISM_SURVIVOR_WEAK"],
                "survivor_density_at_best_cost": best_density,
                "family_classification": _classification_from_counts(counts, len(rows), blocked_by_family.get(family_id, 0)),
            }
        )
    return sorted(out, key=lambda row: (-float(row["survivor_density_at_best_cost"] or 0), -float(row["best_net_cost_bps"] or -1), row["mechanism"], row["family_id"]))


def _symbol_rows(exact_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    keys = sorted({(row["symbol"], row["mechanism"], row["regime"], row["timeframe"]) for row in exact_rows})
    for symbol, mechanism, regime, timeframe in keys:
        exact = [row for row in exact_rows if (row["symbol"], row["mechanism"], row["regime"], row["timeframe"]) == (symbol, mechanism, regime, timeframe)]
        costs = [row for row in cost_rows if (row["symbol"], row["mechanism"], row["regime"], row["timeframe"]) == (symbol, mechanism, regime, timeframe)]
        best_cost, density, counts = _best_density(costs)
        out.append({"symbol": symbol, "mechanism": mechanism, "regime": regime, "timeframe": timeframe, "exact_replays_run": len(exact), "best_net_cost_bps": best_cost, "net_survivors": counts["MECHANISM_SURVIVOR_STRONG"] + counts["MECHANISM_SURVIVOR_WEAK"], "net_survivor_density": density, "classification": _classification_from_counts(counts, len(exact), 0)})
    return sorted(out, key=lambda row: (row["mechanism"], row["regime"], row["symbol"], row["timeframe"]))


def _density_rows(families: list[dict[str, Any]], exact_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]], blocked_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for mechanism, regime in TARGET_MECHANISMS:
        fams = [row for row in families if row["mechanism"] == mechanism and row["regime"] == regime]
        for cost_bps in COST_SCENARIOS_BPS:
            costs = [row for row in cost_rows if row["mechanism"] == mechanism and row["regime"] == regime and float(row["cost_bps"]) == cost_bps]
            counts = _cost_classification_counts(costs)
            exact_count = len([row for row in exact_rows if row["mechanism"] == mechanism and row["regime"] == regime])
            blocked_count = len([row for row in blocked_rows if row["mechanism"] == mechanism and row["regime"] == regime])
            survivors = counts["MECHANISM_SURVIVOR_STRONG"] + counts["MECHANISM_SURVIVOR_WEAK"]
            density = round(survivors / len(costs), 6) if costs else 0.0
            out.append({"mechanism": mechanism, "regime": regime, "cost_bps": cost_bps, "families_tested": len(fams), "exact_replays_run": exact_count, "strong_survivors": counts["MECHANISM_SURVIVOR_STRONG"], "weak_survivors": counts["MECHANISM_SURVIVOR_WEAK"], "cost_eroded": counts["MECHANISM_COST_ERODED"], "failed": counts["MECHANISM_FAILED"], "blocked": blocked_count, "insufficient_sample": counts["INSUFFICIENT_SAMPLE"], "survivor_density": density, "mechanism_classification": classify_mechanism_level(exact_count, blocked_count, density, counts["MECHANISM_SURVIVOR_STRONG"])})
    return out


def _matrix_rows(families: list[dict[str, Any]], exact_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]], blocked_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    keys = sorted({(family["mechanism"], family["regime"], family["timeframe"]) for family in families})
    for mechanism, regime, timeframe in keys:
        fams = [row for row in families if (row["mechanism"], row["regime"], row["timeframe"]) == (mechanism, regime, timeframe)]
        exact = [row for row in exact_rows if (row["mechanism"], row["regime"], row["timeframe"]) == (mechanism, regime, timeframe)]
        costs = [row for row in cost_rows if (row["mechanism"], row["regime"], row["timeframe"]) == (mechanism, regime, timeframe)]
        blocked = [row for row in blocked_rows if (row["mechanism"], row["regime"], row["timeframe"]) == (mechanism, regime, timeframe)]
        best_cost, density, counts = _best_density(costs)
        out.append({"mechanism": mechanism, "regime": regime, "timeframe": timeframe, "families_tested": len(fams), "exact_replays_run": len(exact), "blocked": len(blocked), "best_cost_bps": best_cost, "survivor_density": density, "mechanism_classification": classify_mechanism_level(len(exact), len(blocked), density, counts["MECHANISM_SURVIVOR_STRONG"])})
    return out


def _summary(families: list[dict[str, Any]], exact_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]], blocked_rows: list[dict[str, Any]], family_rows: list[dict[str, Any]], density_rows: list[dict[str, Any]]) -> dict[str, Any]:
    best_density = max(density_rows, key=lambda row: (float(row["survivor_density"]), float(row["cost_bps"]), row["mechanism"]), default={})
    best_family = family_rows[0] if family_rows else {}
    counts = Counter(row["family_classification"] for row in family_rows)
    return {
        "families_tested": len(families),
        "mechanisms_tested": len(TARGET_MECHANISMS),
        "exact_replays_run": len(exact_rows),
        "blocked_tests": len(blocked_rows),
        "cost_rows_evaluated": len(cost_rows),
        "strong_survivors": counts["MECHANISM_SURVIVOR_STRONG"],
        "weak_survivors": counts["MECHANISM_SURVIVOR_WEAK"],
        "cost_eroded": counts["MECHANISM_COST_ERODED"],
        "failed": counts["MECHANISM_FAILED"],
        "insufficient_sample": counts["INSUFFICIENT_SAMPLE"],
        "blocked": counts["MECHANISM_BLOCKED"],
        "best_mechanism": f"{best_density.get('mechanism', '')}/{best_density.get('regime', '')}".strip("/"),
        "best_mechanism_density": best_density.get("survivor_density", 0.0),
        "best_family": best_family.get("family_id", ""),
        "best_family_density": best_family.get("survivor_density_at_best_cost", 0.0),
        "fallback_used_count": sum(1 for row in exact_rows if row.get("fallback_used")),
    }


def _answers(summary: dict[str, Any], family_rows: list[dict[str, Any]], symbol_rows: list[dict[str, Any]], exact_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]]) -> dict[str, str]:
    outside = [row for row in cost_rows if row["classification"] in {"MECHANISM_SURVIVOR_STRONG", "MECHANISM_SURVIVOR_WEAK"} and not (row["symbol"] == "TSLA" and row["mechanism"] == "REVERSAL" and row["timeframe"] == "30m")]
    symbol_survivors = Counter(row["symbol"] for row in cost_rows if row["classification"] in {"MECHANISM_SURVIVOR_STRONG", "MECHANISM_SURVIVOR_WEAK"})
    spread = "none"
    if symbol_survivors:
        top_symbol, top_count = symbol_survivors.most_common(1)[0]
        spread = f"{len(symbol_survivors)} symbols; top concentration {top_symbol}={top_count}/{sum(symbol_survivors.values())}"
    tsla_30m = [row for row in cost_rows if row["symbol"] == "TSLA" and row["mechanism"] == "REVERSAL" and row["timeframe"] == "30m"]
    tsla_best = max((_float(row["net_expectancy"]) or -999.0 for row in tsla_30m), default=-999.0)
    other_best = max((_float(row["net_expectancy"]) or -999.0 for row in cost_rows if not (row["symbol"] == "TSLA" and row["mechanism"] == "REVERSAL" and row["timeframe"] == "30m")), default=-999.0)
    return {
        "survivors_outside_tsla_reversal": "YES" if outside else "NO",
        "best_net_survivor_density": f"{summary.get('best_mechanism')} density={summary.get('best_mechanism_density')}",
        "symbol_concentration": spread,
        "stronger_than_tsla_30m": "YES" if other_best > tsla_best else "NO",
    }


def _best_density(costs: list[dict[str, Any]]) -> tuple[Any, float, Counter[str]]:
    best: tuple[float, float, Counter[str]] | None = None
    for cost_bps in COST_SCENARIOS_BPS:
        rows = [row for row in costs if float(row["cost_bps"]) == cost_bps]
        counts = _cost_classification_counts(rows)
        survivors = counts["MECHANISM_SURVIVOR_STRONG"] + counts["MECHANISM_SURVIVOR_WEAK"]
        density = round(survivors / len(rows), 6) if rows else 0.0
        candidate = (cost_bps, density, counts)
        if best is None or (density, cost_bps) > (best[1], best[0]):
            best = candidate
    if best is None:
        return "", 0.0, Counter()
    return best


def _cost_classification_counts(rows: list[dict[str, Any]]) -> Counter[str]:
    by_base: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
    for row in rows:
        key = (row["candidate_id"], row["family_id"], row["symbol"], row["timeframe"])
        by_base[key].append(row["classification"])
    counts: Counter[str] = Counter()
    for classes in by_base.values():
        if "MECHANISM_SURVIVOR_STRONG" in classes:
            counts["MECHANISM_SURVIVOR_STRONG"] += 1
        elif "MECHANISM_SURVIVOR_WEAK" in classes:
            counts["MECHANISM_SURVIVOR_WEAK"] += 1
        elif "MECHANISM_COST_ERODED" in classes:
            counts["MECHANISM_COST_ERODED"] += 1
        elif "INSUFFICIENT_SAMPLE" in classes:
            counts["INSUFFICIENT_SAMPLE"] += 1
        elif "MECHANISM_BLOCKED" in classes:
            counts["MECHANISM_BLOCKED"] += 1
        else:
            counts["MECHANISM_FAILED"] += 1
    return counts


def _classification_from_counts(counts: Counter[str], exact_count: int, blocked_count: int) -> str:
    if counts["MECHANISM_SURVIVOR_STRONG"]:
        return "MECHANISM_SURVIVOR_STRONG"
    if counts["MECHANISM_SURVIVOR_WEAK"]:
        return "MECHANISM_SURVIVOR_WEAK"
    if counts["MECHANISM_COST_ERODED"]:
        return "MECHANISM_COST_ERODED"
    if counts["INSUFFICIENT_SAMPLE"]:
        return "INSUFFICIENT_SAMPLE"
    if exact_count <= 0 and blocked_count:
        return "MECHANISM_BLOCKED"
    return "MECHANISM_FAILED"


def _highest_surviving_cost(classes: list[str]) -> float:
    costs = [cost for cost, classification in zip(COST_SCENARIOS_BPS, classes) if classification in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}]
    return max(costs) if costs else -1.0


def _blocked_row(requirement: dict[str, Any], blocker: str, required_file: str, reason: str) -> dict[str, Any]:
    return {"candidate_id": requirement.get("candidate_id", ""), "family_id": requirement.get("family_id", ""), "mechanism": requirement.get("mechanism", ""), "regime": requirement.get("regime", ""), "symbol": requirement.get("symbol", ""), "timeframe": requirement.get("timeframe", ""), "blocker": blocker, "required_file": required_file, "reason": reason}


def _group(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[str(row.get(key, ""))].append(row)
    return out


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


def _first_present(row: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        if row.get(key):
            return str(row[key])
    return ""


def _first(value: Any) -> Any:
    if isinstance(value, list) and value:
        return value[0]
    return value


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _norm_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"30m", "30min", "30"}:
        return "30m"
    if text in {"1h", "60m", "60min", "1hr"}:
        return "1h"
    if text in {"15m", "15min", "15"}:
        return "15m"
    return text


def _round(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return round(float(value), 6)


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return round(float(value), 6)


def _parse_timestamp(value: Any) -> datetime:
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
