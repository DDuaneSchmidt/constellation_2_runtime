from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .observation_deduplication import cluster_observations, deduplicate_observations, normalize_observation_text
from .observation_import_governance import validate_observation_import_allowed
from .observation_models import (
    DEFAULT_MARKET_STRUCTURE,
    DEFAULT_OBSERVATION_WORKLOAD_PROFILE,
    MARKET_STRUCTURES,
    OBSERVATION_WORKLOAD_PROFILES,
    REQUIRED_OBSERVATION_FIELDS,
    ObservationImportBatch,
    ObservationImportReport,
    ObservationRecord,
)
from .observation_to_claim import convert_observation_cluster_to_claim, create_backlog_items_from_observations
from .session_context import SESSION_CONTEXTS, normalize_session_context
from .regime_expansion import ATLAS_REGIMES, normalize_regime

MECHANISMS = [
    "BREAKOUT",
    "MEAN_REVERSION",
    "OPENING_RANGE",
    "SESSION_TIMING",
    "VWAP_OR_AVERAGE_RECLAIM",
    "VOLATILITY_EXPANSION",
    "LIQUIDITY_SWEEP",
    "TREND_CONTINUATION",
    "REVERSAL",
    "EVENT_REACTION",
]
REGIMES = list(ATLAS_REGIMES)
SYMBOLS = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA", "USO", "GLD", "TLT"]
TIMEFRAMES = ["5m", "15m", "30m", "1h"]
SOURCE_TYPES = ["manual_review", "screen_replay", "journal_extract", "scanner_snapshot", "market_note"]
EXPANDED_SYMBOLS = [
    "SPY", "QQQ", "IWM", "DIA", "AAPL", "MSFT", "NVDA", "TSLA", "AMD", "META",
    "GOOGL", "AMZN", "NFLX", "JPM", "BAC", "XLE", "USO", "GLD", "TLT", "DBC",
]

FOCUSED_EXPANDED_PROFILE = "EXPANDED_OBSERVATION_TRIAL_FOCUSED"
FOCUSED_REGIMES = ["CHOP", "TRENDING", "BEAR", "RISK_ON", "VOL_CONTRACTION"]
FOCUSED_MARKET_STRUCTURES = ["COMPRESSION", "FAILED_BREAKOUT", "GAP_UP", "GAP_DOWN"]
FOCUSED_MECHANISMS = ["BREAKOUT", "MEAN_REVERSION", "EVENT_REACTION", "REVERSAL"]
FOCUSED_TIMEFRAMES = ["30m", "15m", "1h", "5m"]
FOCUSED_SOURCE_TYPES = ["journal_extract", "manual_review", "scanner_snapshot", "screen_replay"]
FOCUSED_SYMBOLS = ["QQQ", "DBC", "DIA", "USO", "GOOGL"]


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(prefix: str, values: list[Any]) -> str:
    material = json.dumps(values, sort_keys=True, default=str)
    return f"{prefix}_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


def import_observations_from_csv(
    path: str | Path,
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    workload_profile: str = DEFAULT_OBSERVATION_WORKLOAD_PROFILE,
    created_at: str | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    rows = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(dict(row))
    return import_observation_rows(rows, source_path=str(path), input_format="CSV", root=root, workload_profile=workload_profile, created_at=created_at, write_report=write_report)


def import_observations_from_json(
    path: str | Path,
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    workload_profile: str = DEFAULT_OBSERVATION_WORKLOAD_PROFILE,
    created_at: str | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        rows = payload.get("observations", [])
    else:
        rows = payload
    if not isinstance(rows, list):
        raise ValueError("observation JSON must be a list or an object with observations")
    return import_observation_rows(list(rows), source_path=str(path), input_format="JSON", root=root, workload_profile=workload_profile, created_at=created_at, write_report=write_report)


def import_observation_rows(
    rows: list[dict[str, Any]],
    *,
    source_path: str,
    input_format: str,
    root: str | Path = DEFAULT_STORE_ROOT,
    workload_profile: str = DEFAULT_OBSERVATION_WORKLOAD_PROFILE,
    created_at: str | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    if workload_profile not in OBSERVATION_WORKLOAD_PROFILES:
        raise ValueError(f"invalid observation workload profile: {workload_profile}")
    created = created_at or now_utc()
    max_observations = OBSERVATION_WORKLOAD_PROFILES[workload_profile]
    limited_rows = list(rows[:max_observations])
    valid_records: list[dict[str, Any]] = []
    invalid_records: list[dict[str, Any]] = []
    for index, raw in enumerate(limited_rows):
        try:
            record = observation_record_from_raw(raw, index=index, created_at=created)
            validate_observation_record(record)
            validate_observation_import_allowed(record)
            valid_records.append(record)
        except Exception as exc:
            invalid_records.append({"row_index": index, "error": str(exc), "raw": raw})
    unique_records, duplicates = deduplicate_observations(valid_records)
    clusters = cluster_observations(unique_records)
    claim_seeds = [convert_observation_cluster_to_claim(cluster) for cluster in clusters]
    backlog_items = create_backlog_items_from_observations(claim_seeds, root=root, created_at=created)
    batch = ObservationImportBatch(
        batch_id=stable_id("obs_batch", [source_path, input_format, created, len(limited_rows)]),
        created_at=created,
        source_path=source_path,
        input_format=input_format,
        workload_profile=workload_profile,
        max_observations=max_observations,
        records=unique_records,
        invalid_records=invalid_records,
        metadata={
            "raw_rows_received": len(rows),
            "rows_processed_after_profile_limit": len(limited_rows),
            "duplicates_skipped": len(duplicates),
            "measurement_only": True,
        },
    ).to_dict()
    report = ObservationImportReport(
        report_id=stable_id("obs_import_report", [batch["batch_id"], len(unique_records), len(clusters), len(claim_seeds)]),
        created_at=created,
        raw_observations=len(limited_rows),
        valid_observations=len(unique_records),
        invalid_observations=len(invalid_records),
        duplicates_skipped=len(duplicates),
        clusters_created=len(clusters),
        claims_created=len(claim_seeds),
        backlog_items_created=len(backlog_items),
        batch=batch,
        clusters=clusters,
        claim_seeds=claim_seeds,
        backlog_items=backlog_items,
        metadata={"workload_profile": workload_profile, "source_path": source_path, "measurement_only": True},
    ).to_dict()
    validate_observation_import_allowed(report)
    if write_report:
        from .observation_import_reports import write_observation_import_report
        write_observation_import_report(report, root=Path(root) / "observation_import", day=created[:10])
    return report


def observation_record_from_raw(raw: dict[str, Any], *, index: int, created_at: str) -> dict[str, Any]:
    missing = [field for field in REQUIRED_OBSERVATION_FIELDS if raw.get(field) in {None, ""}]
    if missing:
        raise ValueError(f"missing required observation fields: {sorted(missing)}")
    market_structure = normalize_market_structure(raw.get("market_structure") or raw.get("structure"))
    session_context = normalize_session_context(raw.get("session_context") or raw.get("session"), timestamp=str(raw.get("timestamp") or ""))
    source_type = str(raw.get("source_type") or raw.get("source") or "unknown").strip()
    symbol_universe = raw.get("symbol_universe") or raw.get("universe") or "DEFAULT"
    metadata = {
        "market_context": raw.get("market_context"),
        "indicator_snapshot": raw.get("indicator_snapshot"),
        "notes": raw.get("notes"),
        "raw_index": index,
        "normalized_text": normalize_observation_text(str(raw.get("observation", ""))),
        "symbol_universe": symbol_universe,
        "source_type": source_type,
        "market_structure": market_structure,
        "session_context": session_context,
        "measurement_only": True,
    }
    timestamp = str(raw["timestamp"]).strip()
    symbol = str(raw["symbol"]).strip().upper()
    timeframe = str(raw["timeframe"]).strip().lower()
    mechanism = str(raw["mechanism"]).strip().upper()
    regime = normalize_regime(raw.get("regime"))
    observation_text = str(raw["observation"]).strip()
    source = str(raw["source"]).strip()
    confidence = float(raw.get("confidence") if raw.get("confidence") not in {None, ""} else 0.5)
    return ObservationRecord(
        observation_id=stable_id("obs", [timestamp, symbol, timeframe, mechanism, market_structure, regime, normalize_observation_text(observation_text), source]),
        timestamp=timestamp,
        symbol=symbol,
        timeframe=timeframe,
        mechanism=mechanism,
        observation_text=observation_text,
        market_structure=market_structure,
        regime=regime,
        source=source,
        confidence=confidence,
        metadata=metadata,
        created_at=created_at,
    ).to_dict()


def normalize_market_structure(value: Any) -> str:
    structure = str(value or DEFAULT_MARKET_STRUCTURE).strip().upper().replace(" ", "_").replace("-", "_")
    return structure if structure in MARKET_STRUCTURES else DEFAULT_MARKET_STRUCTURE


def validate_observation_record(record: dict[str, Any]) -> bool:
    ObservationRecord(**record).to_dict()
    return True


def import_observations(path: str | Path, *, root: str | Path = DEFAULT_STORE_ROOT, workload_profile: str = DEFAULT_OBSERVATION_WORKLOAD_PROFILE) -> dict[str, Any]:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return import_observations_from_csv(path, root=root, workload_profile=workload_profile)
    if suffix == ".json":
        return import_observations_from_json(path, root=root, workload_profile=workload_profile)
    raise ValueError("observation import path must end in .csv or .json")


def demo_observation_rows(count: int = 100) -> list[dict[str, Any]]:
    return structured_observation_rows(count=count, profile_name="OBSERVATION_IMPORT_100", symbol_universe=SYMBOLS)


def structured_observation_rows(
    *,
    count: int,
    profile_name: str,
    symbol_universe: list[str] | None = None,
    start: datetime | None = None,
) -> list[dict[str, Any]]:
    start_time = start or datetime(2026, 6, 5, 9, 30, tzinfo=timezone.utc)
    symbols = list(symbol_universe or EXPANDED_SYMBOLS)
    rows: list[dict[str, Any]] = []
    for index in range(count):
        mechanism = MECHANISMS[index % len(MECHANISMS)]
        regime_block = index // len(MECHANISMS)
        regime_index = 0 if regime_block == 5 else regime_block % len(REGIMES)
        regime = REGIMES[regime_index]
        symbol = symbols[(index * 7 + index // 11) % len(symbols)]
        timeframe = TIMEFRAMES[(index // 3) % len(TIMEFRAMES)]
        source_type = SOURCE_TYPES[(index // 5) % len(SOURCE_TYPES)]
        session_context = SESSION_CONTEXTS[(index // 9) % len(SESSION_CONTEXTS)]
        market_structure = MARKET_STRUCTURES[(index // 7) % len(MARKET_STRUCTURES)]
        timestamp = (start_time + timedelta(minutes=5 * index)).isoformat().replace("+00:00", "Z")
        rows.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "symbol_universe": "EXPANDED_CORE_ETF_EQUITY" if len(symbols) > len(SYMBOLS) else "DEMO_CORE",
            "timeframe": timeframe,
            "mechanism": mechanism,
            "market_structure": market_structure,
            "observation": demo_observation_text(mechanism, regime, index, market_structure=market_structure),
            "regime": regime,
            "source": f"{profile_name.lower()}_{source_type}",
            "source_type": source_type,
            "session_context": session_context,
            "confidence": round(0.50 + (index % 17) * 0.025, 3),
            "market_context": f"{regime} context, {session_context} session, {timeframe} replay, {symbol} in structured observation expansion",
            "indicator_snapshot": {
                "profile": profile_name,
                "demo_index": index,
                "mechanism": mechanism,
                "timeframe": timeframe,
                "source_type": source_type,
                "market_structure": market_structure,
                "session_context": session_context,
            },
            "notes": "Structured Research OS observation expansion; evidence only; no trading authority.",
        })
    return rows



def focused_expanded_observation_rows(count: int | None = None) -> list[dict[str, Any]]:
    combos: list[tuple[str, str, str, str, str, str]] = []
    for regime in FOCUSED_REGIMES:
        for market_structure in FOCUSED_MARKET_STRUCTURES:
            for mechanism in FOCUSED_MECHANISMS:
                for timeframe in FOCUSED_TIMEFRAMES:
                    for source_type in FOCUSED_SOURCE_TYPES:
                        for symbol in FOCUSED_SYMBOLS:
                            combos.append((regime, market_structure, mechanism, timeframe, source_type, symbol))
    target = count or OBSERVATION_WORKLOAD_PROFILES[FOCUSED_EXPANDED_PROFILE]
    rows: list[dict[str, Any]] = []
    for index in range(target):
        regime, market_structure, mechanism, timeframe, source_type, symbol = combos[index % len(combos)]
        cycle = index // len(combos)
        rows.append({
            "timestamp": f"focused-expanded-{index:05d}",
            "symbol": symbol,
            "symbol_universe": "FOCUSED_EXPANDED_CORE",
            "timeframe": timeframe,
            "mechanism": mechanism,
            "market_structure": market_structure,
            "observation": focused_observation_text(mechanism, regime, market_structure, timeframe, source_type, symbol, cycle),
            "regime": regime,
            "source": f"focused_expanded_{source_type}",
            "source_type": source_type,
            "session_context": "UNKNOWN",
            "confidence": round(0.66 + ((index + cycle) % 9) * 0.018, 3),
            "market_context": f"{regime} context, no session split, {market_structure}, {timeframe}, {symbol}",
            "indicator_snapshot": {
                "profile": FOCUSED_EXPANDED_PROFILE,
                "focused_dimension_set": True,
                "mechanism": mechanism,
                "regime": regime,
                "market_structure": market_structure,
                "timeframe": timeframe,
                "source_type": source_type,
                "symbol": symbol,
                "session_context": "NONE_REQUESTED",
                "cycle": cycle,
            },
            "notes": "Focused expanded Research OS observation; evidence only; no trading authority.",
        })
    return rows


def focused_observation_text(mechanism: str, regime: str, market_structure: str, timeframe: str, source_type: str, symbol: str, cycle: int) -> str:
    base = demo_observation_text(mechanism, regime, cycle, market_structure=market_structure)
    return f"{base}; focused profile {symbol} {timeframe} {source_type} cycle {cycle % 4}"


def observation_rows_for_profile(profile_name: str) -> list[dict[str, Any]]:
    if profile_name not in OBSERVATION_WORKLOAD_PROFILES:
        raise ValueError(f"invalid observation workload profile: {profile_name}")
    if profile_name == FOCUSED_EXPANDED_PROFILE:
        return focused_expanded_observation_rows()
    return structured_observation_rows(
        count=OBSERVATION_WORKLOAD_PROFILES[profile_name],
        profile_name=profile_name,
        symbol_universe=EXPANDED_SYMBOLS,
    )


def seed_observations_for_profile(
    profile_name: str,
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    output_path: str | Path | None = None,
    created_at: str | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    rows = observation_rows_for_profile(profile_name)
    path = Path(output_path or Path(root) / f"{profile_name.lower()}_observations.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"observations": rows}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = import_observations_from_json(path, root=root, workload_profile=profile_name, created_at=created_at, write_report=write_report)
    return {"profile": profile_name, "path": str(path), "observations_generated": len(rows), "import_report": report}


def demo_observation_text(mechanism: str, regime: str, index: int, *, market_structure: str = DEFAULT_MARKET_STRUCTURE) -> str:
    phrases = {
        "BREAKOUT": "broke above local range after compression",
        "MEAN_REVERSION": "reverted toward average after extended move",
        "OPENING_RANGE": "broke opening range high after compression",
        "SESSION_TIMING": "follow-through appeared during session timing window",
        "VWAP_OR_AVERAGE_RECLAIM": "reclaimed VWAP and held above average reference",
        "VOLATILITY_EXPANSION": "expanded volatility after narrow range compression",
        "LIQUIDITY_SWEEP": "swept liquidity then reversed back through trigger",
        "TREND_CONTINUATION": "continued trend after shallow pullback",
        "REVERSAL": "reversed after failed continuation attempt",
        "EVENT_REACTION": "reacted directionally after event timestamp",
    }
    structure_phrase = market_structure.lower().replace("_", " ") if market_structure != DEFAULT_MARKET_STRUCTURE else "unspecified structure"
    return f"{phrases.get(mechanism, 'showed mechanism behavior')} during {structure_phrase} in {regime} regime sample {index % 5}"


def seed_demo_observations(*, root: str | Path = DEFAULT_STORE_ROOT, output_path: str | Path | None = None, created_at: str | None = None) -> dict[str, Any]:
    path = Path(output_path or Path(root) / "demo_observations.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = demo_observation_rows(100)
    path.write_text(json.dumps({"observations": rows}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = import_observations_from_json(path, root=root, workload_profile="OBSERVATION_IMPORT_100", created_at=created_at)
    return {"demo_path": str(path), "observations_generated": len(rows), "import_report": report}
