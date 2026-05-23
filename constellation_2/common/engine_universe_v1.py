from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from constellation_2.common.ranked_symbol_universe_v1 import load_ranked_symbol_universe, minimum_required_dynamic_symbol_count
from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import write_file_immutable_v1

getcontext().prec = 28


class EngineUniverseError(Exception):
    pass


@dataclass(frozen=True)
class UniverseResolution:
    engine_id: str
    policy_id: str
    day_utc: str
    configured_rule: Dict[str, Any]
    symbols_considered: List[str]
    symbols_eligible: List[str]
    admitted_symbols: List[str]
    candidate_pairs: List[Tuple[str, str]]
    exclusions_by_reason: List[Dict[str, Any]]
    notes: List[str]
    universe_type: str = "ENGINE_FILTERED"
    parent_canonical_universe_authority_id: str = ""
    parent_canonical_universe_authority_path: str = ""
    parent_universe_type: str = "CANONICAL_DYNAMIC"


@dataclass(frozen=True)
class UniverseCandidateBasis:
    engine_id: str
    policy_id: str
    day_utc: str
    basis_day_utc: str
    basis_mode: str
    configured_rule: Dict[str, Any]
    symbols_considered: List[str]
    candidate_symbols: List[str]
    exclusions_by_reason: List[Dict[str, Any]]
    notes: List[str]
    universe_type: str = "ENGINE_FILTERED"
    parent_canonical_universe_authority_id: str = ""
    parent_canonical_universe_authority_path: str = ""
    parent_universe_type: str = "CANONICAL_DYNAMIC"


REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json").resolve()
POLICY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RISK/engine_universe_policy_registry.v1.schema.json"
REPORT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RISK/engine_universe_resolution.v1.schema.json"
CANDIDATE_BASIS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RISK/engine_universe_candidate_basis.v1.schema.json"


def _parse_decimal(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise EngineUniverseError(f"BAD_DECIMAL:{field}={value!r}") from exc


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise EngineUniverseError(f"MISSING_JSON:{path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise EngineUniverseError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _truth_root(repo_root: Path, explicit_truth_root: Optional[Path]) -> Path:
    if explicit_truth_root is not None:
        return Path(explicit_truth_root).resolve()
    return resolve_truth_root(repo_root=repo_root)


def _manifest(truth_root: Path) -> Dict[str, Any]:
    manifest_path = (truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
    obj = _read_json(manifest_path)
    files = obj.get("files")
    if not isinstance(files, list):
        raise EngineUniverseError("MARKET_DATA_MANIFEST_FILES_NOT_LIST")
    return obj


def _iter_rows(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                raise EngineUniverseError(f"JSONL_ROW_NOT_OBJECT:{path}:line={idx}")
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
            raise EngineUniverseError(f"MARKET_DATA_FILE_MISSING:{path}")
        for row in _iter_rows(path):
            ts = str(row.get("timestamp_utc") or "").strip()
            if len(ts) < 10:
                raise EngineUniverseError(f"BAD_TIMESTAMP:{symbol}:{ts!r}")
            if ts[:10] <= day_utc:
                rows.append(row)
    rows.sort(key=lambda item: str(item.get("timestamp_utc") or ""))
    return rows


def _has_same_day_bar(truth_root: Path, manifest: Dict[str, Any], symbol: str, day_utc: str) -> bool:
    for row in _bars_up_to_day(truth_root, manifest, symbol, day_utc):
        ts = str(row.get("timestamp_utc") or "").strip()
        if len(ts) >= 10 and ts[:10] == day_utc:
            return True
    return False


def _last_n_rows(rows: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    if n <= 0:
        raise EngineUniverseError(f"BAD_LOOKBACK:{n}")
    if len(rows) < n:
        raise EngineUniverseError(f"INSUFFICIENT_HISTORY:need={n}:have={len(rows)}")
    return rows[-n:]


def _close(row: Dict[str, Any]) -> Decimal:
    return _parse_decimal(row.get("close"), field="close")


def _high(row: Dict[str, Any]) -> Decimal:
    return _parse_decimal(row.get("high"), field="high")


def _low(row: Dict[str, Any]) -> Decimal:
    return _parse_decimal(row.get("low"), field="low")


def _volume(row: Dict[str, Any]) -> Decimal:
    return _parse_decimal(row.get("volume"), field="volume")


def _avg_volume(rows: List[Dict[str, Any]]) -> Decimal:
    return sum(_volume(row) for row in rows) / Decimal(len(rows))


def _avg_dollar_volume(rows: List[Dict[str, Any]]) -> Decimal:
    return sum((_close(row) * _volume(row)) for row in rows) / Decimal(len(rows))


def _atr(rows: List[Dict[str, Any]], lookback_sessions: int) -> Decimal:
    tail = _last_n_rows(rows, lookback_sessions + 1)
    trs: List[Decimal] = []
    for idx in range(1, len(tail)):
        prev_close = _close(tail[idx - 1])
        high_now = _high(tail[idx])
        low_now = _low(tail[idx])
        if prev_close <= Decimal("0"):
            raise EngineUniverseError("NON_POSITIVE_PREV_CLOSE_FOR_ATR")
        tr = max(high_now - low_now, abs(high_now - prev_close), abs(low_now - prev_close))
        trs.append(tr)
    if not trs:
        raise EngineUniverseError("ATR_EMPTY")
    return sum(trs) / Decimal(len(trs))


def _load_policy_registry() -> Dict[str, Any]:
    obj = _read_json(POLICY_PATH)
    validate_against_repo_schema_v1(obj, REPO_ROOT, POLICY_SCHEMA)
    return obj


def _policy_for_engine(engine_id: str) -> Dict[str, Any]:
    reg = _load_policy_registry()
    policies = reg.get("policies")
    if not isinstance(policies, list):
        raise EngineUniverseError("POLICIES_NOT_LIST")
    for policy in policies:
        if isinstance(policy, dict) and str(policy.get("engine_id") or "").strip() == engine_id:
            return policy
    raise EngineUniverseError(f"POLICY_NOT_FOUND:{engine_id}")


def _is_dynamic_ranked_policy(policy: Dict[str, Any]) -> bool:
    return (
        str(policy.get("universe_mode") or "").strip().upper() == "LIQUIDITY_RANKED_SYMBOLS"
        and str(policy.get("symbol_source_class") or "").strip().upper() == "DYNAMIC_SAME_DAY"
        and int(policy.get("target_symbol_count") or 0) >= 100
    )


def _required_dynamic_symbol_count(policy: Dict[str, Any]) -> int:
    return minimum_required_dynamic_symbol_count(int(policy.get("target_symbol_count") or 0))


def _require_dynamic_breadth(*, engine_id: str, policy: Dict[str, Any], symbol_count: int, source: str) -> None:
    if not _is_dynamic_ranked_policy(policy):
        return
    required = _required_dynamic_symbol_count(policy)
    if int(symbol_count) < required:
        target = int(policy.get("target_symbol_count") or 0)
        raise EngineUniverseError(
            f"UNIVERSE_BREADTH_FAILURE:{engine_id}:source={source}:symbol_count={int(symbol_count)}:"
            f"target_symbol_count={target}:minimum_required={required}"
        )


def _available_symbols(manifest: Dict[str, Any]) -> List[str]:
    files = manifest.get("files")
    symbols = sorted({str(entry.get("symbol") or "").strip().upper() for entry in files if isinstance(entry, dict)})
    return [symbol for symbol in symbols if symbol]


def _exclusion_rows(exclusions: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for reason_code in sorted(exclusions.keys()):
        examples = sorted(set(exclusions[reason_code]))[:5]
        rows.append({"reason_code": reason_code, "count": len(exclusions[reason_code]), "examples": examples})
    return rows


def _latest_day_before_target(
    truth_root: Path,
    manifest: Dict[str, Any],
    symbol: str,
    day_utc: str,
) -> Optional[str]:
    latest: Optional[str] = None
    for row in _bars_up_to_day(truth_root, manifest, symbol, day_utc):
        ts = str(row.get("timestamp_utc") or "").strip()
        if len(ts) < 10:
            continue
        row_day = ts[:10]
        if row_day >= day_utc:
            continue
        if latest is None or row_day > latest:
            latest = row_day
    return latest


def _prior_common_covered_day(truth_root: Path, manifest: Dict[str, Any], day_utc: str) -> str:
    latest_prior_by_symbol: Dict[str, str] = {}
    for symbol in _available_symbols(manifest):
        latest = _latest_day_before_target(truth_root, manifest, symbol, day_utc)
        if latest is None:
            continue
        latest_prior_by_symbol[symbol] = latest
    if not latest_prior_by_symbol:
        raise EngineUniverseError(f"CANDIDATE_BASIS_PRIOR_DAY_UNAVAILABLE:{day_utc}")
    return min(latest_prior_by_symbol.values())


def resolve_engine_universe(
    *,
    engine_id: str,
    day_utc: str,
    repo_root: Path = REPO_ROOT,
    truth_root: Optional[Path] = None,
    symbol_override: Optional[List[str]] = None,
) -> UniverseResolution:
    root = _truth_root(repo_root, truth_root)
    manifest = _manifest(root)
    policy = _policy_for_engine(engine_id)
    universe_mode = str(policy.get("universe_mode") or "").strip().upper()
    notes = list(policy.get("notes") or [])
    configured_rule = {
        "universe_mode": universe_mode,
        "strategy_type": str(policy.get("strategy_type") or ""),
        "security_scope": str(policy.get("security_scope") or ""),
        "liquidity_filters": policy.get("liquidity_filters"),
        "ranking_mode": policy.get("ranking_mode"),
        "target_symbol_count": int(policy.get("target_symbol_count") or 0),
        "sector_cap": policy.get("sector_cap"),
        "atr_filter": policy.get("atr_filter"),
        "symbol_override_applied": bool(symbol_override),
    }
    exclusions: Dict[str, List[str]] = defaultdict(list)

    if bool(policy.get("sector_cap", {}).get("enabled")):
        if bool(policy.get("sector_cap", {}).get("require_classification")):
            raise EngineUniverseError(f"SECTOR_CLASSIFICATION_REQUIRED_BUT_UNAVAILABLE:{engine_id}")
        notes.append("SECTOR_CAP_SKIPPED:NO_GOVERNED_SECTOR_CLASSIFICATION")

    if symbol_override:
        symbols = sorted({str(symbol).strip().upper() for symbol in symbol_override if str(symbol).strip()})
        return UniverseResolution(
            engine_id=engine_id,
            policy_id=str(policy.get("policy_id") or engine_id),
            day_utc=day_utc,
            configured_rule=configured_rule,
            symbols_considered=symbols,
            symbols_eligible=symbols,
            admitted_symbols=symbols,
            candidate_pairs=[],
            exclusions_by_reason=[],
            notes=notes,
            universe_type="TEMPORARY",
            parent_canonical_universe_authority_id="",
            parent_canonical_universe_authority_path="",
            parent_universe_type="",
        )

    if universe_mode == "CURATED_PAIRS":
        raw_pairs = policy.get("curated_pairs")
        if not isinstance(raw_pairs, list) or not raw_pairs:
            raise EngineUniverseError(f"CURATED_PAIRS_MISSING:{engine_id}")
        candidate_pairs: List[Tuple[str, str]] = []
        flattened: List[str] = []
        available = set(_available_symbols(manifest))
        for pair in raw_pairs:
            if not isinstance(pair, list) or len(pair) != 2:
                raise EngineUniverseError(f"BAD_CURATED_PAIR:{engine_id}:{pair!r}")
            leg_a = str(pair[0]).strip().upper()
            leg_b = str(pair[1]).strip().upper()
            if not leg_a or not leg_b:
                raise EngineUniverseError(f"EMPTY_CURATED_PAIR:{engine_id}:{pair!r}")
            missing = [symbol for symbol in [leg_a, leg_b] if symbol not in available]
            if missing and bool(policy.get("require_all_symbols_present")):
                raise EngineUniverseError(f"PAIR_SYMBOL_MISSING:{engine_id}:{','.join(missing)}")
            if missing:
                for symbol in missing:
                    exclusions["MISSING_MARKET_DATA"].append(symbol)
                continue
            candidate_pairs.append((leg_a, leg_b))
            flattened.extend([leg_a, leg_b])
        admitted = sorted(set(flattened))
        return UniverseResolution(
            engine_id=engine_id,
            policy_id=str(policy.get("policy_id") or engine_id),
            day_utc=day_utc,
            configured_rule=configured_rule,
            symbols_considered=sorted(set(flattened)),
            symbols_eligible=sorted(set(flattened)),
            admitted_symbols=admitted,
            candidate_pairs=candidate_pairs,
            exclusions_by_reason=_exclusion_rows(exclusions),
            notes=notes,
        )

    if universe_mode == "CURATED_SYMBOLS":
        curated = policy.get("curated_symbols")
        if not isinstance(curated, list) or not curated:
            raise EngineUniverseError(f"CURATED_SYMBOLS_MISSING:{engine_id}")
        available = set(_available_symbols(manifest))
        considered = []
        admitted = []
        for raw_symbol in curated:
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            considered.append(symbol)
            if symbol not in available:
                if bool(policy.get("require_all_symbols_present")):
                    raise EngineUniverseError(f"REQUIRED_SYMBOL_MISSING:{engine_id}:{symbol}")
                exclusions["MISSING_MARKET_DATA"].append(symbol)
                continue
            admitted.append(symbol)
        admitted = sorted(set(admitted))
        considered = sorted(set(considered))
        return UniverseResolution(
            engine_id=engine_id,
            policy_id=str(policy.get("policy_id") or engine_id),
            day_utc=day_utc,
            configured_rule=configured_rule,
            symbols_considered=considered,
            symbols_eligible=admitted,
            admitted_symbols=admitted[: int(policy.get("target_symbol_count") or len(admitted))],
            candidate_pairs=[],
            exclusions_by_reason=_exclusion_rows(exclusions),
            notes=notes,
        )

    if universe_mode != "LIQUIDITY_RANKED_SYMBOLS":
        raise EngineUniverseError(f"UNSUPPORTED_UNIVERSE_MODE:{engine_id}:{universe_mode}")

    liquidity_filters = policy.get("liquidity_filters")
    if not isinstance(liquidity_filters, dict):
        raise EngineUniverseError(f"LIQUIDITY_FILTERS_REQUIRED:{engine_id}")
    try:
        ranked_symbol_universe = load_ranked_symbol_universe(truth_root=root, day=day_utc, validate=False)
    except Exception as exc:
        raise EngineUniverseError(f"RANKED_SYMBOL_UNIVERSE_UNAVAILABLE:{day_utc}:{exc}") from exc
    ranked_status = str(ranked_symbol_universe.get("status") or "").strip().upper()
    source_symbols = sorted(
        {
            str(symbol).strip().upper()
            for symbol in ranked_symbol_universe.get("symbols") or []
            if str(symbol).strip()
        }
    )
    parent_authority_id = str(ranked_symbol_universe.get("parent_canonical_universe_authority_id") or "")
    parent_authority_path = str(ranked_symbol_universe.get("parent_canonical_universe_authority_path") or "")
    parent_universe_type = str(ranked_symbol_universe.get("parent_universe_type") or "")
    if _is_dynamic_ranked_policy(policy) and (not parent_authority_id or parent_universe_type != "CANONICAL_DYNAMIC"):
        raise EngineUniverseError(
            f"RANKED_SYMBOL_UNIVERSE_MISSING_CANONICAL_AUTHORITY_LINEAGE:{day_utc}:"
            f"parent_canonical_universe_authority_id={parent_authority_id}:parent_universe_type={parent_universe_type}"
        )
    if ranked_status not in {"PASS", "BOOTSTRAP_PASS"}:
        raise EngineUniverseError(
            f"RANKED_SYMBOL_UNIVERSE_UNUSABLE:{day_utc}:status={ranked_status}:"
            f"symbol_count={len(source_symbols)}:reason_codes={ranked_symbol_universe.get('reason_codes') or []}"
        )
    if not source_symbols:
        raise EngineUniverseError(f"RANKED_SYMBOL_UNIVERSE_EMPTY:{day_utc}")
    _require_dynamic_breadth(engine_id=engine_id, policy=policy, symbol_count=len(source_symbols), source="ranked_symbol_universe_v1")
    lookback = int(liquidity_filters.get("lookback_sessions") or 0)
    price_min = _parse_decimal(liquidity_filters.get("price_min"), field="price_min")
    volume_min = Decimal(int(liquidity_filters.get("avg_volume_min") or 0))
    dollar_volume_min = _parse_decimal(liquidity_filters.get("avg_dollar_volume_min"), field="avg_dollar_volume_min")
    ranked_rows: List[Tuple[Decimal, str, Decimal, Decimal]] = []
    atr_values: Dict[str, Decimal] = {}
    notes.append("CANONICAL_RANKED_SYMBOL_SOURCE:ranked_symbol_universe_v1")

    for symbol in source_symbols:
        considered_rows = _bars_up_to_day(root, manifest, symbol, day_utc)
        try:
            tail = _last_n_rows(considered_rows, lookback)
        except EngineUniverseError:
            exclusions["INSUFFICIENT_HISTORY"].append(symbol)
            continue
        latest_close = _close(tail[-1])
        avg_volume = _avg_volume(tail)
        avg_dollar_volume = _avg_dollar_volume(tail)
        if latest_close <= price_min:
            exclusions["PRICE_BELOW_MIN"].append(symbol)
            continue
        if avg_volume <= volume_min:
            exclusions["AVG_VOLUME_BELOW_MIN"].append(symbol)
            continue
        if avg_dollar_volume <= dollar_volume_min:
            exclusions["AVG_DOLLAR_VOLUME_BELOW_MIN"].append(symbol)
            continue
        ranked_rows.append((avg_dollar_volume, symbol, avg_volume, latest_close))

    ranked_rows.sort(key=lambda row: (-row[0], row[1]))
    eligible_symbols = [row[1] for row in ranked_rows]

    atr_filter = policy.get("atr_filter")
    if isinstance(atr_filter, dict) and bool(atr_filter.get("enabled")):
        atr_lookback = int(atr_filter.get("lookback_sessions") or 0)
        min_percentile = _parse_decimal(atr_filter.get("min_percentile"), field="atr_filter.min_percentile")
        eligible_after_atr: List[str] = []
        for symbol in eligible_symbols:
            rows = _bars_up_to_day(root, manifest, symbol, day_utc)
            try:
                atr_values[symbol] = _atr(rows, atr_lookback)
            except EngineUniverseError:
                exclusions["ATR_HISTORY_MISSING"].append(symbol)
                continue
        atr_series = sorted(atr_values.values())
        for symbol in eligible_symbols:
            if symbol not in atr_values:
                continue
            rank = Decimal(sum(1 for value in atr_series if value <= atr_values[symbol])) / Decimal(len(atr_series))
            if rank < min_percentile:
                exclusions["ATR_PERCENTILE_BELOW_MIN"].append(symbol)
                continue
            eligible_after_atr.append(symbol)
        eligible_symbols = eligible_after_atr

    if engine_id in {"C2_MEAN_REVERSION_EQ_V1", "C2_EVENT_DISLOCATION_V1"}:
        eligible_same_day: List[str] = []
        for symbol in eligible_symbols:
            if not _has_same_day_bar(root, manifest, symbol, day_utc):
                exclusions["MISSING_SAME_DAY_BAR"].append(symbol)
                continue
            eligible_same_day.append(symbol)
        eligible_symbols = eligible_same_day

    _require_dynamic_breadth(engine_id=engine_id, policy=policy, symbol_count=len(eligible_symbols), source="engine_universe_eligible_symbols")
    target_symbol_count = int(policy.get("target_symbol_count") or 0)
    admitted_symbols = eligible_symbols[:target_symbol_count]
    return UniverseResolution(
        engine_id=engine_id,
        policy_id=str(policy.get("policy_id") or engine_id),
        day_utc=day_utc,
        configured_rule=configured_rule,
        symbols_considered=source_symbols,
        symbols_eligible=eligible_symbols,
        admitted_symbols=admitted_symbols,
        candidate_pairs=[],
        exclusions_by_reason=_exclusion_rows(exclusions),
        notes=notes,
        universe_type="ENGINE_FILTERED",
        parent_canonical_universe_authority_id=parent_authority_id,
        parent_canonical_universe_authority_path=parent_authority_path,
        parent_universe_type=parent_universe_type or "CANONICAL_DYNAMIC",
    )


def resolve_engine_candidate_basis(
    *,
    engine_id: str,
    day_utc: str,
    repo_root: Path = REPO_ROOT,
    truth_root: Optional[Path] = None,
) -> UniverseCandidateBasis:
    root = _truth_root(repo_root, truth_root)
    manifest = _manifest(root)
    policy = _policy_for_engine(engine_id)
    universe_mode = str(policy.get("universe_mode") or "").strip().upper()
    if universe_mode != "LIQUIDITY_RANKED_SYMBOLS":
        raise EngineUniverseError(f"CANDIDATE_BASIS_UNSUPPORTED_UNIVERSE_MODE:{engine_id}:{universe_mode}")
    symbol_source_class = str(policy.get("symbol_source_class") or "").strip().upper()
    same_day_symbol_basis_required = bool(policy.get("same_day_symbol_basis_required") is True)
    fallback_allowed = bool(policy.get("fallback_allowed") is True)
    if (
        symbol_source_class == "DYNAMIC_SAME_DAY"
        and same_day_symbol_basis_required
        and not fallback_allowed
    ):
        try:
            resolution = resolve_engine_universe(
                engine_id=engine_id,
                day_utc=day_utc,
                repo_root=repo_root,
                truth_root=root,
            )
        except EngineUniverseError as exc:
            raise EngineUniverseError(
                f"CANDIDATE_BASIS_SAME_DAY_UNAVAILABLE:{engine_id}:{day_utc}:{str(exc)}"
            ) from exc
        candidate_symbols = sorted(
            {
                str(symbol).strip().upper()
                for symbol in resolution.symbols_eligible
                if str(symbol).strip()
            }
        )
        if not candidate_symbols:
            raise EngineUniverseError(f"CANDIDATE_BASIS_SAME_DAY_UNAVAILABLE:{engine_id}:{day_utc}")
        _require_dynamic_breadth(engine_id=engine_id, policy=policy, symbol_count=len(candidate_symbols), source="engine_universe_candidate_basis_v1")
        notes = list(resolution.notes)
        notes.append("CANDIDATE_BASIS_FROM_SAME_DAY_ELIGIBLE_SYMBOLS")
        notes.append(f"TARGET_DAY_UTC:{day_utc}")
        notes.append(f"BASIS_DAY_UTC:{day_utc}")
        return UniverseCandidateBasis(
            engine_id=engine_id,
            policy_id=resolution.policy_id,
            day_utc=day_utc,
            basis_day_utc=day_utc,
            basis_mode="SAME_DAY_ELIGIBLE_SYMBOLS",
            configured_rule=dict(resolution.configured_rule),
            symbols_considered=list(resolution.symbols_considered),
            candidate_symbols=candidate_symbols,
            exclusions_by_reason=list(resolution.exclusions_by_reason),
            notes=notes,
            universe_type=resolution.universe_type,
            parent_canonical_universe_authority_id=resolution.parent_canonical_universe_authority_id,
            parent_canonical_universe_authority_path=resolution.parent_canonical_universe_authority_path,
            parent_universe_type=resolution.parent_universe_type,
        )
    basis_day_utc = _prior_common_covered_day(root, manifest, day_utc)
    resolution = resolve_engine_universe(
        engine_id=engine_id,
        day_utc=basis_day_utc,
        repo_root=repo_root,
        truth_root=root,
    )
    candidate_symbols = sorted({str(symbol).strip().upper() for symbol in resolution.symbols_eligible if str(symbol).strip()})
    if not candidate_symbols:
        raise EngineUniverseError(f"CANDIDATE_BASIS_EMPTY:{engine_id}:{day_utc}:{basis_day_utc}")
    _require_dynamic_breadth(engine_id=engine_id, policy=policy, symbol_count=len(candidate_symbols), source="engine_universe_candidate_basis_v1")
    notes = list(resolution.notes)
    notes.append("CANDIDATE_BASIS_FROM_PRIOR_COMMON_COVERED_DAY")
    notes.append(f"TARGET_DAY_UTC:{day_utc}")
    notes.append(f"BASIS_DAY_UTC:{basis_day_utc}")
    return UniverseCandidateBasis(
        engine_id=engine_id,
        policy_id=resolution.policy_id,
        day_utc=day_utc,
        basis_day_utc=basis_day_utc,
        basis_mode="PRIOR_COMMON_COVERED_DAY_ELIGIBLE_SYMBOLS",
        configured_rule=dict(resolution.configured_rule),
        symbols_considered=list(resolution.symbols_considered),
        candidate_symbols=candidate_symbols,
        exclusions_by_reason=list(resolution.exclusions_by_reason),
        notes=notes,
        universe_type=resolution.universe_type,
        parent_canonical_universe_authority_id=resolution.parent_canonical_universe_authority_id,
        parent_canonical_universe_authority_path=resolution.parent_canonical_universe_authority_path,
        parent_universe_type=resolution.parent_universe_type,
    )


def _engine_universe_report_payload(
    *,
    engine_id: str,
    day_utc: str,
    produced_utc: str,
    resolution: UniverseResolution,
    signals_detected: int,
    signals_filtered: int,
    intents_created: int,
    selected_symbols: List[str],
) -> Dict[str, Any]:
    return {
      "schema_id": "engine_universe_resolution",
      "schema_version": "v1",
      "day_utc": day_utc,
      "produced_utc": produced_utc,
      "engine_id": engine_id,
      "policy_id": resolution.policy_id,
      "status": "PASS",
      "configured_rule": resolution.configured_rule,
      "symbols_considered": resolution.symbols_considered,
      "symbols_eligible": resolution.symbols_eligible,
      "admitted_symbols": resolution.admitted_symbols,
      "candidate_pairs": [list(pair) for pair in resolution.candidate_pairs],
      "exclusions_by_reason": resolution.exclusions_by_reason,
      "counters": {
          "symbols_scanned": len(resolution.admitted_symbols),
          "symbols_eligible": len(resolution.symbols_eligible),
          "signals_detected": int(signals_detected),
          "signals_filtered": int(signals_filtered),
          "intents_created": int(intents_created)
      },
      "selected_symbols": sorted(set(selected_symbols)),
      "notes": resolution.notes
    }


def _engine_universe_candidate_basis_payload(
    *,
    engine_id: str,
    day_utc: str,
    produced_utc: str,
    basis: UniverseCandidateBasis,
) -> Dict[str, Any]:
    return {
      "schema_id": "engine_universe_candidate_basis",
      "schema_version": "v1",
      "day_utc": day_utc,
      "basis_day_utc": basis.basis_day_utc,
      "produced_utc": produced_utc,
      "engine_id": engine_id,
      "policy_id": basis.policy_id,
      "status": "PASS",
      "universe_type": basis.universe_type,
      "parent_canonical_universe_authority_id": basis.parent_canonical_universe_authority_id,
      "parent_canonical_universe_authority_path": basis.parent_canonical_universe_authority_path,
      "parent_universe_type": basis.parent_universe_type,
      "basis_mode": basis.basis_mode,
      "configured_rule": basis.configured_rule,
      "symbols_considered": basis.symbols_considered,
      "candidate_symbols": basis.candidate_symbols,
      "exclusions_by_reason": basis.exclusions_by_reason,
      "notes": basis.notes,
    }


def emit_engine_universe_report(
    *,
    engine_id: str,
    day_utc: str,
    produced_utc: str,
    repo_root: Path = REPO_ROOT,
    truth_root: Optional[Path] = None,
    resolution: UniverseResolution,
    signals_detected: int,
    signals_filtered: int,
    intents_created: int,
    selected_symbols: List[str],
    refreshable: bool = False,
) -> Path:
    root = _truth_root(repo_root, truth_root)
    payload = _engine_universe_report_payload(
        engine_id=engine_id,
        day_utc=day_utc,
        produced_utc=produced_utc,
        resolution=resolution,
        signals_detected=signals_detected,
        signals_filtered=signals_filtered,
        intents_created=intents_created,
        selected_symbols=selected_symbols,
    )
    validate_against_repo_schema_v1(payload, repo_root, REPORT_SCHEMA)
    out_path = (root / "reports" / "engine_universe_resolution_v1" / day_utc / engine_id / "engine_universe_resolution.v1.json").resolve()
    data = canonical_json_bytes_v1(payload) + b"\n"
    if refreshable:
        write_day_artifact_refreshable_v1(
            path=out_path,
            data=data,
            expected_day_utc=day_utc,
            expected_schema_id="engine_universe_resolution",
            expected_schema_version="v1",
            preserve_statuses=(),
        )
    else:
        write_file_immutable_v1(path=out_path, data=data, create_dirs=True)
    return out_path


def emit_engine_candidate_basis_report(
    *,
    engine_id: str,
    day_utc: str,
    produced_utc: str,
    basis: UniverseCandidateBasis,
    repo_root: Path = REPO_ROOT,
    truth_root: Optional[Path] = None,
    refreshable: bool = False,
) -> Path:
    root = _truth_root(repo_root, truth_root)
    payload = _engine_universe_candidate_basis_payload(
        engine_id=engine_id,
        day_utc=day_utc,
        produced_utc=produced_utc,
        basis=basis,
    )
    validate_against_repo_schema_v1(payload, repo_root, CANDIDATE_BASIS_SCHEMA)
    out_path = (
        root / "reports" / "engine_universe_candidate_basis_v1" / day_utc / engine_id / "engine_universe_candidate_basis.v1.json"
    ).resolve()
    data = canonical_json_bytes_v1(payload) + b"\n"
    if refreshable:
        write_day_artifact_refreshable_v1(
            path=out_path,
            data=data,
            expected_day_utc=day_utc,
            expected_schema_id="engine_universe_candidate_basis",
            expected_schema_version="v1",
            preserve_statuses=(),
        )
    else:
        write_file_immutable_v1(path=out_path, data=data, create_dirs=True)
    return out_path
