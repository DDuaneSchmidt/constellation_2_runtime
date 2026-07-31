from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import create_backtest_spec, run_candidate_backtest_spec
from .market_data_schema_validation import normalize_market_data_csv
from .regime_vocabulary_bridge import mapped_replay_regimes

REPORT_DIRNAME = "exact_replay_without_fallback"
TARGET_FAMILY_IDS = {
    "family_55443d63b32328bd",
    "family_59cc928bca30cc44",
    "family_897af176175df5de",
}
VALID_EXACT_STATUSES = {"VALID_READY", "VALID_WITH_WARNINGS"}

RESULT_COLUMNS = [
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
    "regime_original",
    "regime_bridged",
    "notes",
]

FAMILY_COLUMNS = [
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "candidates_tested",
    "exact_confirmed_strong",
    "exact_confirmed_weak",
    "exact_failed",
    "exact_insufficient_sample",
    "exact_blocked",
    "family_classification",
    "confidence_impact",
]

BLOCKED_COLUMNS = [
    "candidate_id",
    "family_id",
    "symbol",
    "timeframe",
    "blocker",
    "required_file",
    "reason",
]

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


def run_exact_replay_without_fallback(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    report = build_exact_replay_without_fallback(root=root, created_at=created_at, repo_root=repo_root)
    write_exact_replay_without_fallback(report, root=root)
    return report


def build_exact_replay_without_fallback(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    validation_payload, validation_path = _load_build_098_validation(root_path)
    exact_index = _valid_exact_file_index(validation_payload, repo)
    requirements = _target_requirements(root_path)

    results: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for requirement in requirements:
        key = (_norm_symbol(requirement.get("symbol")), _norm_timeframe(requirement.get("timeframe")))
        valid_file = exact_index.get(key)
        if not key[0] or not key[1]:
            blocked.append(_blocked_row(requirement, "INSUFFICIENT_METADATA", "", "Candidate requirement lacks exact symbol/timeframe metadata."))
            continue
        if not valid_file:
            if requirement.get("available_exact_file"):
                blocker = "INVALID_EXACT_FILE"
                reason = "Exact file exists locally but Build 098 did not mark it VALID_READY or VALID_WITH_WARNINGS."
            elif _truthy(requirement.get("fallback_used")) or requirement.get("available_fallback_file"):
                blocker = "FALLBACK_REQUIRED"
                reason = "Exact replay would require daily/alternate fallback, which is disabled."
            else:
                blocker = "MISSING_EXACT_FILE"
                reason = "No Build 098 VALID_READY/VALID_WITH_WARNINGS exact file exists."
            blocked.append(_blocked_row(requirement, blocker, _expected_file(requirement), reason))
            continue
        if not _same_exact_file(requirement, valid_file):
            blocked.append(_blocked_row(requirement, "INVALID_EXACT_FILE", _expected_file(requirement), "Validated file does not match the exact candidate symbol/timeframe requirement."))
            continue
        try:
            results.append(_run_exact_requirement(requirement, valid_file, created_at=created))
        except Exception as exc:  # pragma: no cover - defensive report path
            blocked.append(_blocked_row(requirement, "REPLAY_ERROR", valid_file.get("data_file") or _expected_file(requirement), str(exc)))

    family_rows = _family_rows(requirements, results, blocked)
    summary = _summary(requirements, results, blocked, family_rows)
    return {
        "schema_id": "atlas_v2_research_os_exact_replay_without_fallback",
        "schema_version": "1.0",
        "report_type": "EXACT_REPLAY_WITHOUT_FALLBACK",
        "build": "108",
        "created_at": created,
        "day": created[:10],
        "target_family_ids": sorted(TARGET_FAMILY_IDS),
        "source_inputs": {
            "build_098_or_107_validation": str(validation_path) if validation_path else "",
            "reversal_trending_exact_coverage_plan": str(root_path / "reversal_trending_exact_coverage_plan" / "latest.json"),
            "coverage_matrix": str(root_path / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv"),
        },
        "fallback_policy": {
            "daily_fallback_allowed": False,
            "alternate_timeframe_fallback_allowed": False,
            "alternate_symbol_fallback_allowed": False,
        },
        "summary": summary,
        "candidate_results": results,
        "family_repeatability": family_rows,
        "blocked_exact_replays": blocked,
        "confidence_impact": summary["confidence_impact"],
        "recommended_next_build": "Acquire remaining exact symbol/timeframe files and rerun Build 108 until target families have complete non-fallback coverage.",
        "authority_boundary": {
            "research_only": True,
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
    }


def write_exact_replay_without_fallback(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "exact_replay_results": out_dir / "exact_replay_results.csv",
        "family_exact_repeatability": out_dir / "family_exact_repeatability.csv",
        "blocked_exact_replay": out_dir / "blocked_exact_replay.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_exact_replay_summary(report), encoding="utf-8")
    _write_csv(paths["exact_replay_results"], RESULT_COLUMNS, report.get("candidate_results") or [])
    _write_csv(paths["family_exact_repeatability"], FAMILY_COLUMNS, report.get("family_repeatability") or [])
    _write_csv(paths["blocked_exact_replay"], BLOCKED_COLUMNS, report.get("blocked_exact_replays") or [])
    return paths


def render_exact_replay_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Build 108 - Exact Replay Without Fallback",
        "",
        "## Executive Summary",
        "",
        f"Families reviewed: {summary.get('families_reviewed')}",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Exact replays run: {summary.get('exact_replays_run')}",
        f"Exact blocked: {summary.get('exact_blocked')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Inputs",
        "",
    ]
    for name, path in (report.get("source_inputs") or {}).items():
        lines.append(f"- {name}: {path or 'not present'}")
    lines.extend(
        [
            "",
            "## Fallback Disabled Policy",
            "",
            "- Daily fallback allowed: false",
            "- Alternate timeframe fallback allowed: false",
            "- Alternate symbol fallback allowed: false",
            "",
            "## Candidate Results",
            "",
        ]
    )
    for row in report.get("candidate_results") or []:
        lines.append(f"- {row.get('candidate_id')} {row.get('symbol')} {row.get('timeframe')}: {row.get('classification')} sample={row.get('sample_size')} pf={row.get('profit_factor')}")
    lines.extend(["", "## Family Repeatability Results", ""])
    for row in report.get("family_repeatability") or []:
        lines.append(f"- {row.get('family_id')}: {row.get('family_classification')} confidence={row.get('confidence_impact')}")
    lines.extend(["", "## Blocked Exact Replays", ""])
    for row in (report.get("blocked_exact_replays") or [])[:40]:
        lines.append(f"- {row.get('candidate_id')} {row.get('symbol')} {row.get('timeframe')}: {row.get('blocker')} - {row.get('reason')}")
    lines.extend(
        [
            "",
            "## Confidence Impact",
            "",
            str(summary.get("confidence_impact") or "NONE"),
            "",
            "## Authority Boundary",
            "",
            "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
            "",
            "## Recommended Next Build",
            "",
            str(report.get("recommended_next_build")),
            "",
        ]
    )
    return "\n".join(lines)


def classify_candidate_exact_replay(sample_size: int, expectancy: float | None, profit_factor: float | None) -> str:
    exp = float(expectancy or 0.0)
    pf = float(profit_factor or 0.0)
    if sample_size > 0 and sample_size < 50:
        return "EXACT_INSUFFICIENT_SAMPLE"
    if sample_size >= 50 and exp > 0 and pf >= 1.25:
        return "EXACT_CONFIRMED_STRONG"
    if sample_size >= 50 and exp > 0 and 1.0 <= pf < 1.2:
        return "EXACT_BACKTEST_WEAK"
    if sample_size >= 50 and exp > 0 and 1.2 <= pf < 1.25:
        return "EXACT_CONFIRMED_WEAK"
    if sample_size >= 50 and (exp <= 0 or pf <= 1.0):
        return "EXACT_FAILED"
    return "EXACT_REPLAY_ERROR"


def classify_family_repeatability(rows: list[dict[str, Any]], blocked_count: int) -> str:
    strong = sum(row.get("classification") == "EXACT_CONFIRMED_STRONG" for row in rows)
    weak_or_better = sum(row.get("classification") in {"EXACT_CONFIRMED_STRONG", "EXACT_CONFIRMED_WEAK"} for row in rows)
    failed = sum(row.get("classification") == "EXACT_FAILED" for row in rows)
    insufficient = sum(row.get("classification") == "EXACT_INSUFFICIENT_SAMPLE" for row in rows)
    if weak_or_better >= 2 and strong >= 1:
        return "EXACT_REPEATABLE_STRONG"
    if weak_or_better >= 2:
        return "EXACT_REPEATABLE_WEAK"
    if weak_or_better == 1 and (blocked_count or insufficient):
        return "EXACT_PARTIALLY_REPEATABLE"
    if rows and failed >= max(1, len(rows) - failed):
        return "EXACT_NOT_REPEATABLE"
    return "EXACT_BLOCKED_INSUFFICIENT_DATA"


def _run_exact_requirement(requirement: dict[str, Any], valid_file: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    symbol = _norm_symbol(requirement.get("symbol"))
    timeframe = _norm_timeframe(requirement.get("timeframe"))
    data_file = valid_file["data_file"]
    normalized = normalize_market_data_csv(data_file, symbol=symbol, timeframe=timeframe)
    data_rows = [
        {
            "date": str(row["timestamp"])[:10],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
        }
        for row in normalized
    ]
    regime_original = str(requirement.get("regime") or "UNKNOWN").upper()
    allowed_replay_regimes, mappings = mapped_replay_regimes([regime_original])
    data_meta = {
        "available": True,
        "symbol": symbol,
        "path": data_file,
        "rows": len(data_rows),
        "bar_type": f"{timeframe}_ohlcv",
        "timeframe": timeframe,
        "start_date": data_rows[0]["date"] if data_rows else "",
        "end_date": data_rows[-1]["date"] if data_rows else "",
    }
    spec = create_backtest_spec(
        {
            "candidate_id": requirement.get("candidate_id"),
            "mechanism": requirement.get("mechanism") or "REVERSAL",
            "regime_constraints": {"primary_regime": regime_original, "allowed_regimes": [regime_original]},
        },
        data_meta=data_meta,
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
    max_drawdown = _round(metrics.get("max_drawdown"))
    return {
        "candidate_id": requirement.get("candidate_id", ""),
        "family_id": requirement.get("family_id", ""),
        "symbol": symbol,
        "timeframe": timeframe,
        "data_file": data_file,
        "sample_size": sample_size,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "classification": classify_candidate_exact_replay(sample_size, expectancy, profit_factor),
        "fallback_used": False,
        "regime_original": regime_original,
        "regime_bridged": ",".join(allowed_replay_regimes) if allowed_replay_regimes else "NO_EXECUTABLE_REGIME_EQUIVALENT",
        "notes": "; ".join((result.get("warnings") or []) + (result.get("missing_data") or [])),
    }


def _family_rows(requirements: list[dict[str, Any]], results: list[dict[str, Any]], blocked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    req_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    result_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    blocked_by_family: Counter[str] = Counter()
    for row in requirements:
        req_by_family[str(row.get("family_id") or "")].append(row)
    for row in results:
        result_by_family[str(row.get("family_id") or "")].append(row)
    for row in blocked:
        blocked_by_family[str(row.get("family_id") or "")] += 1
    family_rows = []
    for family_id in sorted(TARGET_FAMILY_IDS):
        reqs = req_by_family.get(family_id, [])
        rows = result_by_family.get(family_id, [])
        classification = classify_family_repeatability(rows, blocked_by_family.get(family_id, 0))
        confidence = "SMALL_INCREASE" if classification in {"EXACT_REPEATABLE_STRONG", "EXACT_REPEATABLE_WEAK"} else ("DECREASE" if classification == "EXACT_NOT_REPEATABLE" else "NONE")
        family_rows.append(
            {
                "family_id": family_id,
                "mechanism": _first(reqs, "mechanism", "REVERSAL"),
                "regime": _first(reqs, "regime", "TRENDING"),
                "timeframe": _first(reqs, "timeframe", ""),
                "candidates_tested": len({row.get("candidate_id") for row in rows}),
                "exact_confirmed_strong": sum(row.get("classification") == "EXACT_CONFIRMED_STRONG" for row in rows),
                "exact_confirmed_weak": sum(row.get("classification") == "EXACT_CONFIRMED_WEAK" for row in rows),
                "exact_failed": sum(row.get("classification") == "EXACT_FAILED" for row in rows),
                "exact_insufficient_sample": sum(row.get("classification") == "EXACT_INSUFFICIENT_SAMPLE" for row in rows),
                "exact_blocked": blocked_by_family.get(family_id, 0),
                "family_classification": classification,
                "confidence_impact": confidence,
            }
        )
    return family_rows


def _summary(requirements: list[dict[str, Any]], results: list[dict[str, Any]], blocked: list[dict[str, Any]], family_rows: list[dict[str, Any]]) -> dict[str, Any]:
    confidence_values = {row.get("confidence_impact") for row in family_rows}
    confidence = "SMALL_INCREASE" if "SMALL_INCREASE" in confidence_values else ("DECREASE" if "DECREASE" in confidence_values else "NONE")
    return {
        "families_reviewed": len({row.get("family_id") for row in requirements}),
        "candidates_reviewed": len({row.get("candidate_id") for row in requirements}),
        "exact_replays_run": len(results),
        "exact_confirmed_strong": sum(row.get("classification") == "EXACT_CONFIRMED_STRONG" for row in results),
        "exact_confirmed_weak": sum(row.get("classification") == "EXACT_CONFIRMED_WEAK" for row in results),
        "exact_failed": sum(row.get("classification") == "EXACT_FAILED" for row in results),
        "exact_blocked": len(blocked),
        "confidence_impact": confidence,
        "classification_counts": dict(Counter(row.get("classification") for row in results)),
        "blocked_counts": dict(Counter(row.get("blocker") for row in blocked)),
    }


def _load_build_098_validation(root: Path) -> tuple[dict[str, Any], Path | None]:
    candidates = [
        root / "exact_data_repair_loop" / "latest.json",
        root / "exact_coverage_import_validator" / "latest.json",
        root / "exact_coverage_import_validation" / "latest.json",
        root / "exact_csv_validation" / "latest.json",
        root / "exact_coverage_validation" / "latest.json",
    ]
    for path in candidates:
        if path.exists():
            return _read_json(path, {}), path
    coverage_path = root / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv"
    rows = _read_csv(coverage_path)
    validations = []
    for row in rows:
        if row.get("coverage_status") == "EXACT_COVERAGE_AVAILABLE" and row.get("available_exact_file"):
            validations.append(
                {
                    "candidate_id": row.get("candidate_id"),
                    "family_id": row.get("family_id"),
                    "symbol": row.get("symbol"),
                    "timeframe": row.get("timeframe"),
                    "status": "VALID_READY",
                    "data_file": row.get("available_exact_file"),
                }
            )
    return {"file_validations": validations, "source": "derived_from_build_090_exact_coverage"}, None


def _valid_exact_file_index(payload: dict[str, Any], repo: Path) -> dict[tuple[str, str], dict[str, Any]]:
    rows = _validation_rows(payload)
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        status = str(row.get("status") or row.get("readiness_status") or row.get("validation_status") or row.get("classification") or "").upper()
        if status not in VALID_EXACT_STATUSES:
            continue
        symbol = _norm_symbol(row.get("symbol"))
        timeframe = _norm_timeframe(row.get("timeframe"))
        data_file = _first_present(row, ["data_file", "normalized_path", "normalized_file", "output_normalized_path", "revalidated_file", "path", "file_path", "csv_path"])
        if not data_file:
            continue
        path = Path(str(data_file))
        if not path.is_absolute():
            path = repo / path
        if symbol and timeframe:
            index[(symbol, timeframe)] = {**row, "status": status, "symbol": symbol, "timeframe": timeframe, "data_file": str(path)}
    return index


def _validation_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ["file_validations", "validated_files", "exact_file_validations", "readiness_rows", "files", "validations", "revalidation_matrix"]:
        value = payload.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    return rows


def _target_requirements(root: Path) -> list[dict[str, Any]]:
    coverage = _read_csv(root / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv")
    rows = [row for row in coverage if row.get("family_id") in TARGET_FAMILY_IDS]
    if rows:
        return sorted(rows, key=lambda row: (row.get("family_id", ""), row.get("candidate_id", ""), row.get("symbol", ""), row.get("timeframe", "")))
    latest = _read_json(root / "reversal_trending_exact_coverage_plan" / "latest.json", {})
    rows = [row for row in latest.get("coverage_matrix") or [] if row.get("family_id") in TARGET_FAMILY_IDS]
    return sorted(rows, key=lambda row: (row.get("family_id", ""), row.get("candidate_id", ""), row.get("symbol", ""), row.get("timeframe", "")))


def _same_exact_file(requirement: dict[str, Any], valid_file: dict[str, Any]) -> bool:
    return _norm_symbol(requirement.get("symbol")) == _norm_symbol(valid_file.get("symbol")) and _norm_timeframe(requirement.get("timeframe")) == _norm_timeframe(valid_file.get("timeframe"))


def _blocked_row(requirement: dict[str, Any], blocker: str, required_file: str, reason: str) -> dict[str, Any]:
    return {
        "candidate_id": requirement.get("candidate_id", ""),
        "family_id": requirement.get("family_id", ""),
        "symbol": _norm_symbol(requirement.get("symbol")),
        "timeframe": _norm_timeframe(requirement.get("timeframe")),
        "blocker": blocker,
        "required_file": required_file,
        "reason": reason,
    }


def _expected_file(requirement: dict[str, Any]) -> str:
    return str(requirement.get("available_exact_file") or f"{_norm_symbol(requirement.get('symbol'))}_{_norm_timeframe(requirement.get('timeframe'))}.csv")


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


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _first(rows: list[dict[str, Any]], key: str, default: str) -> str:
    for row in rows:
        if row.get(key):
            return str(row[key])
    return default


def _first_present(row: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        if row.get(key):
            return str(row[key])
    return ""


def _norm_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _norm_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"1d", "day", "daily"}:
        return "daily"
    return text


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _round(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
