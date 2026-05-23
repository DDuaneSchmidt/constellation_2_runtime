from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import write_file_immutable_v1
from ops.aegis.universe.canonical_universe_authority_v1 import (
    CanonicalUniverseAuthorityError,
    emit_canonical_universe_authority_from_manifest_v1,
)


getcontext().prec = 28


class RankedSymbolUniverseError(Exception):
    pass


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/ranked_symbol_universe.v1.schema.json"

ASSET_SCOPE = "US_EQUITY_ETF"
MIN_PRICE = Decimal("10")
MIN_MEDIAN_DAILY_DOLLAR_VOLUME = Decimal("25000000")
MIN_BAR_HISTORY_SESSIONS = 20
TARGET_SYMBOL_COUNT = 200
ALLOWED_SYMBOL_COUNT_RANGE = {"min": 150, "max": 300}
DETERMINISTIC_SORT = "liquidity_desc_then_symbol_asc"
DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_NUMERATOR = 4
DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR = 5


def minimum_required_dynamic_symbol_count(target_symbol_count: int = TARGET_SYMBOL_COUNT) -> int:
    target = int(target_symbol_count)
    return (
        target * DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_NUMERATOR
        + DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR
        - 1
    ) // DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR


def _truth_root(repo_root: Path, explicit_truth_root: Optional[Path]) -> Path:
    if explicit_truth_root is not None:
        return Path(explicit_truth_root).resolve()
    return resolve_truth_root(repo_root=repo_root)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise RankedSymbolUniverseError(f"MISSING_JSON:{path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RankedSymbolUniverseError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parse_decimal(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise RankedSymbolUniverseError(f"BAD_DECIMAL:{field}={value!r}") from exc


def _decimal_or_none(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _manifest_path(truth_root: Path) -> Path:
    return (truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()


def ranked_symbol_universe_artifact_path(*, truth_root: Path, day: str) -> Path:
    return (truth_root / "reports" / "ranked_symbol_universe_v1" / day / "ranked_symbol_universe.v1.json").resolve()


def _manifest(truth_root: Path) -> Dict[str, Any]:
    manifest = _read_json(_manifest_path(truth_root))
    files = manifest.get("files")
    if not isinstance(files, list):
        raise RankedSymbolUniverseError("MARKET_DATA_MANIFEST_FILES_NOT_LIST")
    return manifest


def _iter_rows(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                raise RankedSymbolUniverseError(f"JSONL_ROW_NOT_OBJECT:{path}:line={idx}")
            yield obj


def _symbol_files(truth_root: Path, manifest: Dict[str, Any], symbol: str) -> List[Path]:
    files = manifest.get("files")
    out: List[Path] = []
    for entry in files:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("symbol") or "").strip().upper() != symbol:
            continue
        rel = str(entry.get("file") or "").strip()
        if not rel:
            continue
        out.append((truth_root / "market_data_snapshot_v1" / rel).resolve())
    return sorted(out)


def _bars_up_to_day(truth_root: Path, manifest: Dict[str, Any], symbol: str, day_utc: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in _symbol_files(truth_root, manifest, symbol):
        if not path.exists():
            raise RankedSymbolUniverseError(f"MARKET_DATA_FILE_MISSING:{path}")
        for row in _iter_rows(path):
            ts = str(row.get("timestamp_utc") or "").strip()
            if len(ts) >= 10 and ts[:10] <= day_utc:
                rows.append(row)
    rows.sort(key=lambda item: str(item.get("timestamp_utc") or ""))
    return rows


def _available_symbols(manifest: Dict[str, Any]) -> List[str]:
    files = manifest.get("files")
    symbols = sorted({str(entry.get("symbol") or "").strip().upper() for entry in files if isinstance(entry, dict)})
    return [symbol for symbol in symbols if symbol]


def _median_dollar_volume(rows: List[Dict[str, Any]]) -> Decimal:
    series = [float(_parse_decimal(row.get("close"), field="close") * _parse_decimal(row.get("volume"), field="volume")) for row in rows]
    return Decimal(str(median(series)))


def _classify_symbol_metadata(symbol: str, metadata: Optional[Dict[str, Any]]) -> tuple[str, bool, List[str]]:
    if not isinstance(metadata, dict):
        return ("UNKNOWN", True, [])
    asset_type = str(metadata.get("asset_type") or "UNKNOWN").strip().upper() or "UNKNOWN"
    eligible = bool(metadata.get("eligible", True))
    reason_codes = [str(code).strip().upper() for code in metadata.get("reason_codes") or [] if str(code).strip()]
    return (asset_type, eligible, sorted(set(reason_codes)))


def build_ranked_symbol_universe_payload(
    *,
    day_utc: str,
    produced_utc: str,
    truth_root: Path,
    metadata_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None,
    dynamic_discovery_used: bool = False,
    canonical_universe_authority: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    manifest = _manifest(truth_root)
    authority_symbols = _available_symbols(manifest)
    if isinstance(canonical_universe_authority, dict) and isinstance(canonical_universe_authority.get("universe_symbols"), list):
        authority_symbols = [str(symbol).strip().upper() for symbol in canonical_universe_authority.get("universe_symbols") or [] if str(symbol).strip()]
    considered_symbols = sorted(set(authority_symbols))
    metrics: List[Dict[str, Any]] = []
    eligible_rows: List[tuple[Decimal, str]] = []

    for symbol in considered_symbols:
        rows = _bars_up_to_day(truth_root, manifest, symbol, day_utc)
        asset_type, metadata_eligible, metadata_reason_codes = _classify_symbol_metadata(symbol, (metadata_by_symbol or {}).get(symbol))
        reason_codes: List[str] = list(metadata_reason_codes)
        has_same_day_bar = bool(rows) and str(rows[-1].get("timestamp_utc") or "").strip()[:10] == day_utc
        latest_close: Optional[Decimal] = None
        median_daily_dollar_volume: Optional[Decimal] = None
        eligible = True

        if not metadata_eligible:
            eligible = False
        if len(rows) < MIN_BAR_HISTORY_SESSIONS:
            eligible = False
            reason_codes.append("INSUFFICIENT_BAR_HISTORY")
        if not has_same_day_bar:
            eligible = False
            reason_codes.append("MISSING_SAME_DAY_BAR")
        if rows:
            tail = rows[-MIN_BAR_HISTORY_SESSIONS:]
            latest_close = _parse_decimal(tail[-1].get("close"), field="close")
            if latest_close < MIN_PRICE:
                eligible = False
                reason_codes.append("PRICE_BELOW_MIN")
            median_daily_dollar_volume = _median_dollar_volume(tail)
            if median_daily_dollar_volume < MIN_MEDIAN_DAILY_DOLLAR_VOLUME:
                eligible = False
                reason_codes.append("MEDIAN_DOLLAR_VOLUME_BELOW_MIN")
        if eligible and latest_close is not None and median_daily_dollar_volume is not None:
            eligible_rows.append((median_daily_dollar_volume, symbol))

        metrics.append(
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "eligible": eligible,
                "latest_close": _decimal_or_none(latest_close),
                "median_daily_dollar_volume": _decimal_or_none(median_daily_dollar_volume),
                "bar_history_sessions": len(rows),
                "has_same_day_bar": has_same_day_bar,
                "reason_codes": sorted(set(reason_codes)),
            }
        )

    eligible_rows.sort(key=lambda row: (-row[0], row[1]))
    selected_symbols = [symbol for _, symbol in eligible_rows[:TARGET_SYMBOL_COUNT]]
    metrics.sort(key=lambda row: row["symbol"])

    reason_codes: List[str] = []
    if not selected_symbols:
        reason_codes.append("RANKED_SYMBOL_UNIVERSE_EMPTY")
        if considered_symbols and all("MISSING_SAME_DAY_BAR" in row.get("reason_codes", []) for row in metrics):
            reason_codes.append("ALL_CONSIDERED_SYMBOLS_MISSING_SAME_DAY_BAR")
    min_required_symbol_count = minimum_required_dynamic_symbol_count(TARGET_SYMBOL_COUNT)
    if len(selected_symbols) < ALLOWED_SYMBOL_COUNT_RANGE["min"]:
        reason_codes.append("SELECTED_SYMBOL_COUNT_BELOW_TARGET_RANGE")
    if len(selected_symbols) < min_required_symbol_count:
        reason_codes.append("UNIVERSE_BREADTH_FAILURE")
    if len(selected_symbols) > ALLOWED_SYMBOL_COUNT_RANGE["max"]:
        reason_codes.append("SELECTED_SYMBOL_COUNT_ABOVE_TARGET_RANGE")

    payload = {
        "schema_id": "ranked_symbol_universe",
        "schema_version": "v1",
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "status": "FAIL" if not selected_symbols or len(selected_symbols) < min_required_symbol_count else "PASS",
        "universe_type": "RANKED_DYNAMIC",
        "parent_canonical_universe_authority_id": str((canonical_universe_authority or {}).get("canonical_universe_authority_id") or ""),
        "parent_canonical_universe_authority_path": str((canonical_universe_authority or {}).get("artifact_path") or ""),
        "parent_universe_type": str((canonical_universe_authority or {}).get("universe_type") or "CANONICAL_DYNAMIC"),
        "universe_scope": "LIQUIDITY_RANKED_SYMBOLS/DYNAMIC_SAME_DAY",
        "selection_policy": {
            "asset_scope": ASSET_SCOPE,
            "min_price": _decimal_or_none(MIN_PRICE),
            "min_median_daily_dollar_volume": _decimal_or_none(MIN_MEDIAN_DAILY_DOLLAR_VOLUME),
            "min_bar_history_sessions": MIN_BAR_HISTORY_SESSIONS,
            "exclude_leveraged_inverse_etf": True,
            "target_symbol_count": TARGET_SYMBOL_COUNT,
            "allowed_symbol_count_range": dict(ALLOWED_SYMBOL_COUNT_RANGE),
            "deterministic_sort": DETERMINISTIC_SORT,
        },
        "target_symbol_count": TARGET_SYMBOL_COUNT,
        "considered_symbol_count": len(considered_symbols),
        "eligible_symbol_count": len(eligible_rows),
        "selected_symbol_count": len(selected_symbols),
        "symbols": selected_symbols,
        "selection_metrics_by_symbol": metrics,
        "source_evidence": {
            "market_data_manifest_path": str(_manifest_path(truth_root)),
            "market_data_manifest_sha256": _sha256_file(_manifest_path(truth_root)),
            "canonical_universe_authority_id": str((canonical_universe_authority or {}).get("canonical_universe_authority_id") or ""),
            "canonical_universe_authority_path": str((canonical_universe_authority or {}).get("artifact_path") or ""),
            "canonical_universe_symbol_count": int((canonical_universe_authority or {}).get("universe_symbol_count") or 0),
            "dynamic_discovery_used": bool(dynamic_discovery_used),
            "metadata_symbol_count": len(metadata_by_symbol or {}),
        },
        "reason_codes": reason_codes,
    }
    return payload


def emit_ranked_symbol_universe(
    *,
    day_utc: str,
    produced_utc: str,
    truth_root: Path,
    metadata_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None,
    dynamic_discovery_used: bool = False,
    refreshable: bool = False,
) -> Path:
    try:
        canonical_authority = emit_canonical_universe_authority_from_manifest_v1(
            truth_root=truth_root,
            source_day=day_utc,
            source_run_id=produced_utc,
            writer_process="ranked_symbol_universe_v1",
            generation_pipeline="ranked_symbol_universe_v1",
            discovery_targets={"target_symbol_count": TARGET_SYMBOL_COUNT},
            discovery_results={"dynamic_discovery_used": bool(dynamic_discovery_used)},
        )
    except CanonicalUniverseAuthorityError as exc:
        raise RankedSymbolUniverseError(str(exc)) from exc
    payload = build_ranked_symbol_universe_payload(
        day_utc=day_utc,
        produced_utc=produced_utc,
        truth_root=truth_root,
        metadata_by_symbol=metadata_by_symbol,
        dynamic_discovery_used=dynamic_discovery_used,
        canonical_universe_authority=canonical_authority,
    )
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)
    out_path = ranked_symbol_universe_artifact_path(truth_root=truth_root, day=day_utc)
    data = canonical_json_bytes_v1(payload) + b"\n"
    if refreshable:
        write_day_artifact_refreshable_v1(
            path=out_path,
            data=data,
            expected_day_utc=day_utc,
            expected_schema_id="ranked_symbol_universe",
            expected_schema_version="v1",
            preserve_statuses=(),
        )
    else:
        write_file_immutable_v1(path=out_path, data=data, create_dirs=True)
    return out_path


def load_ranked_symbol_universe(*, truth_root: Path, day: str, validate: bool = False) -> Dict[str, Any]:
    path = ranked_symbol_universe_artifact_path(truth_root=truth_root, day=day)
    payload = _read_json(path)
    if validate:
        validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)
    return payload

