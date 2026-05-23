from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.ranked_symbol_universe_v1 import minimum_required_dynamic_symbol_count
from ops.aegis.universe.canonical_universe_authority_v1 import (
    CanonicalUniverseAuthorityError,
    require_current_canonical_universe_authority_v1,
)
ENGINE_UNIVERSE_POLICY_RELPATH = Path("governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json")
ENGINE_MODEL_REGISTRY_RELPATH = Path("governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json")
DEPRECATED_SYMBOL_SOURCES_RELPATH = Path("governance/02_REGISTRIES/DEPRECATED_SYMBOL_SOURCES_V1.json")

SOURCE_ENGINE_BASIS = "engine_universe_candidate_basis_v1"
SOURCE_ENGINE_BASIS_OPERATIONAL_LATEST = "engine_universe_candidate_basis_v1.operational_latest_valid"
SOURCE_RANKED_UNIVERSE = "ranked_symbol_universe_v1"
SOURCE_MARKET_DATA_MANIFEST = "market_data_snapshot_v1.dataset_manifest"
SOURCE_ENGINE_POLICY_CURATED = "ENGINE_UNIVERSE_POLICY_V1.curated_symbols"
SOURCE_DEPRECATED_ENGINE_REGISTRY = "DEPRECATED:ENGINE_MODEL_REGISTRY_V1.allowed_symbols"
MIN_DYNAMIC_MARKET_DATA_SYMBOL_COUNT = 100
DYNAMIC_SOURCE_MIN_TARGET_COUNT = 100
OPERATIONAL_UNIVERSE_MAX_AGE_DAYS = 7
MINIMUM_VIABLE_ETF_SYMBOLS = {
    "DBC",
    "GLD",
    "HYG",
    "IEF",
    "IWM",
    "LQD",
    "QQQ",
    "SPY",
    "TLT",
    "UUP",
}


class CanonicalSymbolUniverseError(RuntimeError):
    pass


@dataclass(frozen=True)
class SymbolUniverseResolution:
    engine_id: str
    day_utc: str
    symbols: list[str]
    symbol_count: int
    source: str
    source_path: str
    source_rank: str
    deprecated_fallback_used: bool
    deprecated_fallback_warning: str
    policy_universe_mode: str
    policy_target_symbol_count: int
    blockers: list[dict[str, Any]]
    source_artifacts: list[str]


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CanonicalSymbolUniverseError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return payload


def _normalize_symbols(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return sorted({str(symbol).strip().upper() for symbol in raw if str(symbol).strip()})


def _engine_basis_path(*, truth_root: Path, day_utc: str, engine_id: str) -> Path:
    return (
        truth_root
        / "reports"
        / "engine_universe_candidate_basis_v1"
        / day_utc
        / engine_id.upper()
        / "engine_universe_candidate_basis.v1.json"
    ).resolve()


def _parse_day(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value or "").strip()[:10])
    except Exception:
        return None


def _day_age_days(*, newer_day: str, older_day: str) -> int | None:
    newer = _parse_day(newer_day)
    older = _parse_day(older_day)
    if newer is None or older is None:
        return None
    return (newer - older).days


def _is_intraday_operational_mode(market_data_mode: str) -> bool:
    return str(market_data_mode or "").strip().upper() in {"INTRADAY_OPERATIONAL", "PROVISIONAL_INTRADAY"}


def _latest_operational_basis_path(*, truth_root: Path, day_utc: str, engine_id: str) -> Path | None:
    base = Path(truth_root).resolve() / "reports" / "engine_universe_candidate_basis_v1"
    if not base.exists():
        return None
    requested = _parse_day(day_utc)
    if requested is None:
        return None
    candidates: list[tuple[date, Path]] = []
    for path in base.glob(f"*/{engine_id.upper()}/engine_universe_candidate_basis.v1.json"):
        path_day = _parse_day(path.parts[-3] if len(path.parts) >= 3 else "")
        if path_day is None or path_day > requested:
            continue
        candidates.append((path_day, path.resolve()))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _authority_status_from_basis(payload: dict[str, Any]) -> tuple[str, Path | None]:
    path_text = str(payload.get("parent_canonical_universe_authority_path") or "").strip()
    if not path_text:
        return ("MISSING_PARENT_CANONICAL_UNIVERSE_AUTHORITY", None)
    path = Path(path_text).expanduser().resolve()
    if not path.exists() or not path.is_file():
        return ("PARENT_CANONICAL_UNIVERSE_AUTHORITY_PATH_MISSING", path)
    try:
        authority = _read_json(path)
    except Exception:
        return ("PARENT_CANONICAL_UNIVERSE_AUTHORITY_UNREADABLE", path)
    if str(authority.get("authority_status") or "").upper() != "PASS":
        return (f"PARENT_CANONICAL_UNIVERSE_AUTHORITY_NOT_PASS:{authority.get('authority_status')}", path)
    if str(authority.get("universe_type") or "") != "CANONICAL_DYNAMIC":
        return (f"PARENT_CANONICAL_UNIVERSE_AUTHORITY_BAD_TYPE:{authority.get('universe_type')}", path)
    return ("PASS", path)


def _ranked_universe_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "ranked_symbol_universe_v1" / day_utc / "ranked_symbol_universe.v1.json").resolve()


def _manifest_path(*, truth_root: Path) -> Path:
    return (truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()


def _policy_path(repo_root: Path) -> Path:
    return (repo_root / ENGINE_UNIVERSE_POLICY_RELPATH).resolve()


def _engine_registry_path(repo_root: Path) -> Path:
    return (repo_root / ENGINE_MODEL_REGISTRY_RELPATH).resolve()


def _policy_for_engine(*, repo_root: Path, engine_id: str) -> dict[str, Any]:
    path = _policy_path(repo_root)
    if not path.exists():
        return {}
    registry = _read_json(path)
    for row in registry.get("policies") if isinstance(registry.get("policies"), list) else []:
        if isinstance(row, dict) and str(row.get("engine_id") or "").strip().upper() == engine_id.upper():
            return row
    return {}


def _is_dynamic_ranked_policy(policy: dict[str, Any]) -> bool:
    return (
        str(policy.get("universe_mode") or "").strip().upper() == "LIQUIDITY_RANKED_SYMBOLS"
        and str(policy.get("symbol_source_class") or "").strip().upper() == "DYNAMIC_SAME_DAY"
        and int(policy.get("target_symbol_count") or 0) >= DYNAMIC_SOURCE_MIN_TARGET_COUNT
    )


def _required_dynamic_symbol_count(policy: dict[str, Any]) -> int:
    return minimum_required_dynamic_symbol_count(int(policy.get("target_symbol_count") or 0))


def _dynamic_breadth_blocker(*, policy: dict[str, Any], path: Path, source: str, symbol_count: int) -> dict[str, str]:
    target = int(policy.get("target_symbol_count") or 0)
    required = _required_dynamic_symbol_count(policy)
    return _blocker(
        "universe_breadth_failure",
        path,
        f"source={source} symbol_count={int(symbol_count)} target_symbol_count={target} minimum_required={required}",
    )


def _meets_dynamic_breadth(*, policy: dict[str, Any], symbol_count: int) -> bool:
    if not _is_dynamic_ranked_policy(policy):
        return True
    return int(symbol_count) >= _required_dynamic_symbol_count(policy)


def _canonical_truth_root() -> Path | None:
    try:
        from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

        root = Path(resolve_canonical_truth_root()).resolve()
    except Exception:
        return None
    return root if root.exists() and root.is_dir() else None


def _dynamic_authority_root(*, requested_root: Path, policy: dict[str, Any]) -> Path:
    if not _is_dynamic_ranked_policy(policy):
        return requested_root
    canonical_root = _canonical_truth_root()
    if canonical_root is None:
        return requested_root
    if requested_root == canonical_root:
        return requested_root
    if "truth_sleeves" in requested_root.parts:
        return canonical_root
    return requested_root


def _fallback_engine_registry_symbols(*, repo_root: Path, engine_id: str) -> list[str]:
    path = _engine_registry_path(repo_root)
    if not path.exists():
        return []
    registry = _read_json(path)
    for row in registry.get("engines") if isinstance(registry.get("engines"), list) else []:
        if isinstance(row, dict) and str(row.get("engine_id") or "").strip().upper() == engine_id.upper():
            return _normalize_symbols(row.get("allowed_symbols"))
    return []


def _resolution(
    *,
    engine_id: str,
    day_utc: str,
    symbols: list[str],
    source: str,
    source_path: Path,
    source_rank: str,
    deprecated_fallback_used: bool = False,
    warning: str = "",
    policy: dict[str, Any] | None = None,
    blockers: list[dict[str, Any]] | None = None,
    source_artifacts: list[str] | None = None,
) -> SymbolUniverseResolution:
    policy = policy or {}
    normalized = _normalize_symbols(symbols)
    return SymbolUniverseResolution(
        engine_id=engine_id.upper(),
        day_utc=day_utc,
        symbols=normalized,
        symbol_count=len(normalized),
        source=source,
        source_path=str(source_path),
        source_rank=source_rank,
        deprecated_fallback_used=deprecated_fallback_used,
        deprecated_fallback_warning=warning,
        policy_universe_mode=str(policy.get("universe_mode") or "").strip().upper(),
        policy_target_symbol_count=int(policy.get("target_symbol_count") or 0),
        blockers=list(blockers or []),
        source_artifacts=list(source_artifacts or [str(source_path)]),
    )


def _blocker(blocker_type: str, path: Path, detail: str) -> dict[str, str]:
    return {"blocker_type": blocker_type, "path": str(path), "detail": detail}


def _deprecated_market_data_manifest_reason(symbols: list[str]) -> str:
    symbol_set = set(_normalize_symbols(symbols))
    if symbol_set == MINIMUM_VIABLE_ETF_SYMBOLS:
        return "minimum_viable_etf_contract_universe"
    if len(symbol_set) in {10, 12, 41, 43}:
        return f"fixed_etf_core_universe_count_{len(symbol_set)}"
    if 0 < len(symbol_set) < MIN_DYNAMIC_MARKET_DATA_SYMBOL_COUNT:
        return f"too_narrow_for_dynamic_market_universe_count_{len(symbol_set)}"
    return ""


def resolve_canonical_symbol_universe_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    truth_root: Path | str,
    day_utc: str,
    engine_id: str = "",
    allow_deprecated_fallback: bool = False,
    market_data_mode: str = "FINAL_EOD_CERTIFIED",
    operational_universe_max_age_days: int = OPERATIONAL_UNIVERSE_MAX_AGE_DAYS,
) -> SymbolUniverseResolution:
    repo = Path(repo_root).resolve()
    root = Path(truth_root).resolve()
    day = str(day_utc).strip()
    engine = str(engine_id or "").strip().upper()
    blockers: list[dict[str, Any]] = []
    policy = _policy_for_engine(repo_root=repo, engine_id=engine) if engine else {}
    universe_mode = str(policy.get("universe_mode") or "").strip().upper()
    source_root = _dynamic_authority_root(requested_root=root, policy=policy)
    if source_root != root and _is_dynamic_ranked_policy(policy):
        blockers.append(_blocker("dynamic_truth_root_repointed_to_canonical", source_root, f"requested_truth_root={root}"))
    current_authority_available = True
    if _is_dynamic_ranked_policy(policy):
        try:
            authority = require_current_canonical_universe_authority_v1(
                truth_root=source_root,
                day_utc=day,
                minimum_symbol_count=_required_dynamic_symbol_count(policy),
            )
            blockers.append(_blocker("canonical_universe_authority_verified", Path(str(authority.get("artifact_path") or source_root)), f"authority_id={authority.get('canonical_universe_authority_id')} symbol_count={authority.get('universe_symbol_count')}"))
        except CanonicalUniverseAuthorityError as exc:
            current_authority_available = False
            blockers.append(_blocker("canonical_universe_authority_failure", source_root, str(exc)))
            if not _is_intraday_operational_mode(market_data_mode):
                raise CanonicalSymbolUniverseError(
                    json.dumps(
                        {
                            "error": "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE",
                            "engine_id": engine,
                            "day_utc": day,
                            "market_data_mode": str(market_data_mode or ""),
                            "allow_deprecated_fallback": allow_deprecated_fallback,
                            "blockers": blockers,
                        },
                        sort_keys=True,
                    )
                ) from exc
            blockers.append(_blocker("intraday_operational_lkg_allowed", source_root, f"market_data_mode={market_data_mode} max_age_days={int(operational_universe_max_age_days)}"))

    if engine:
        basis_path = _engine_basis_path(truth_root=source_root, day_utc=day, engine_id=engine)
        if basis_path.exists() and basis_path.is_file():
            payload = _read_json(basis_path)
            symbols = _normalize_symbols(payload.get("candidate_symbols"))
            status = str(payload.get("status") or "").strip().upper()
            basis_day = str(payload.get("basis_day_utc") or payload.get("day_utc") or "").strip()
            if status in {"PASS", "BOOTSTRAP_PASS"} and basis_day == day and symbols:
                if _meets_dynamic_breadth(policy=policy, symbol_count=len(symbols)):
                    return _resolution(
                        engine_id=engine,
                        day_utc=day,
                        symbols=symbols,
                        source=SOURCE_ENGINE_BASIS,
                        source_path=basis_path,
                        source_rank="A",
                        policy=policy,
                        blockers=blockers,
                    )
                blockers.append(_dynamic_breadth_blocker(policy=policy, path=basis_path, source=SOURCE_ENGINE_BASIS, symbol_count=len(symbols)))
            else:
                blockers.append(_blocker("engine_candidate_basis_unusable", basis_path, f"status={status} basis_day={basis_day} symbol_count={len(symbols)}"))

        if _is_dynamic_ranked_policy(policy) and _is_intraday_operational_mode(market_data_mode) and not current_authority_available:
            latest_basis_path = _latest_operational_basis_path(truth_root=source_root, day_utc=day, engine_id=engine)
            if latest_basis_path is None:
                blockers.append(_blocker("operational_latest_valid_basis_missing", source_root, f"engine_id={engine} day_utc={day}"))
            elif latest_basis_path != basis_path:
                try:
                    payload = _read_json(latest_basis_path)
                    symbols = _normalize_symbols(payload.get("candidate_symbols"))
                    status = str(payload.get("status") or "").strip().upper()
                    basis_day = str(payload.get("basis_day_utc") or payload.get("day_utc") or latest_basis_path.parts[-3]).strip()
                    age_days = _day_age_days(newer_day=day, older_day=basis_day)
                    authority_status, authority_path = _authority_status_from_basis(payload)
                    if status in {"PASS", "BOOTSTRAP_PASS"} and symbols and age_days is not None and 0 <= age_days <= int(operational_universe_max_age_days) and authority_status == "PASS":
                        if _meets_dynamic_breadth(policy=policy, symbol_count=len(symbols)):
                            op_blockers = list(blockers)
                            op_blockers.append(_blocker("operational_latest_valid_basis_used", latest_basis_path, f"basis_day={basis_day} age_days={age_days} market_data_mode={market_data_mode}"))
                            if authority_path is not None:
                                op_blockers.append(_blocker("operational_parent_authority_verified", authority_path, "authority_status=PASS universe_type=CANONICAL_DYNAMIC"))
                            return _resolution(
                                engine_id=engine,
                                day_utc=day,
                                symbols=symbols,
                                source=SOURCE_ENGINE_BASIS_OPERATIONAL_LATEST,
                                source_path=latest_basis_path,
                                source_rank="A_OPERATIONAL_LATEST_VALID",
                                policy=policy,
                                blockers=op_blockers,
                                source_artifacts=[str(latest_basis_path), str(authority_path or "")],
                            )
                        blockers.append(_dynamic_breadth_blocker(policy=policy, path=latest_basis_path, source=SOURCE_ENGINE_BASIS_OPERATIONAL_LATEST, symbol_count=len(symbols)))
                    else:
                        blockers.append(_blocker("operational_latest_valid_basis_unusable", latest_basis_path, f"status={status} basis_day={basis_day} age_days={age_days} symbol_count={len(symbols)} authority_status={authority_status}"))
                except Exception as exc:
                    blockers.append(_blocker("operational_latest_valid_basis_unreadable", latest_basis_path, str(exc)))

    if universe_mode in {"CURATED_SYMBOLS", "CURATED_PAIRS"}:
        curated = _normalize_symbols(policy.get("curated_symbols"))
        if curated:
            return _resolution(
                engine_id=engine,
                day_utc=day,
                symbols=curated,
                source=SOURCE_ENGINE_POLICY_CURATED,
                source_path=_policy_path(repo),
                source_rank="A_POLICY_CURATED",
                policy=policy,
                blockers=blockers,
            )
        blockers.append(_blocker("curated_policy_symbols_missing", _policy_path(repo), f"engine_id={engine}"))

    ranked_path = _ranked_universe_path(truth_root=source_root, day_utc=day)
    if ranked_path.exists() and ranked_path.is_file():
        payload = _read_json(ranked_path)
        symbols = _normalize_symbols(payload.get("symbols"))
        status = str(payload.get("status") or "").strip().upper()
        if status in {"PASS", "BOOTSTRAP_PASS"} and symbols:
            if _meets_dynamic_breadth(policy=policy, symbol_count=len(symbols)):
                return _resolution(
                    engine_id=engine,
                    day_utc=day,
                    symbols=symbols,
                    source=SOURCE_RANKED_UNIVERSE,
                    source_path=ranked_path,
                    source_rank="B",
                    policy=policy,
                    blockers=blockers,
                )
            blockers.append(_dynamic_breadth_blocker(policy=policy, path=ranked_path, source=SOURCE_RANKED_UNIVERSE, symbol_count=len(symbols)))
        else:
            blockers.append(_blocker("ranked_symbol_universe_unusable", ranked_path, f"status={status} symbol_count={len(symbols)} reason_codes={payload.get('reason_codes') or []}"))

    manifest_path = _manifest_path(truth_root=source_root)
    if manifest_path.exists() and manifest_path.is_file():
        payload = _read_json(manifest_path)
        symbols = _normalize_symbols(payload.get("symbols"))
        if not symbols and isinstance(payload.get("files"), list):
            symbols = _normalize_symbols([row.get("symbol") for row in payload["files"] if isinstance(row, dict)])
        if symbols:
            deprecated_reason = _deprecated_market_data_manifest_reason(symbols)
            if not _meets_dynamic_breadth(policy=policy, symbol_count=len(symbols)):
                blockers.append(_dynamic_breadth_blocker(policy=policy, path=manifest_path, source=SOURCE_MARKET_DATA_MANIFEST, symbol_count=len(symbols)))
            elif deprecated_reason:
                blockers.append(_blocker("deprecated_market_data_manifest_unusable", manifest_path, deprecated_reason))
            else:
                return _resolution(
                    engine_id=engine,
                    day_utc=day,
                    symbols=symbols,
                    source=SOURCE_MARKET_DATA_MANIFEST,
                    source_path=manifest_path,
                    source_rank="C",
                    policy=policy,
                    blockers=blockers,
                )
        else:
            blockers.append(_blocker("market_data_manifest_empty", manifest_path, "symbol_count=0"))

    if allow_deprecated_fallback and engine:
        symbols = _fallback_engine_registry_symbols(repo_root=repo, engine_id=engine)
        if symbols:
            return _resolution(
                engine_id=engine,
                day_utc=day,
                symbols=symbols,
                source=SOURCE_DEPRECATED_ENGINE_REGISTRY,
                source_path=_engine_registry_path(repo),
                source_rank="D_EXPLICIT_DEPRECATED_FALLBACK",
                deprecated_fallback_used=True,
                warning="Deprecated engine registry symbols were used only because allow_deprecated_fallback=true.",
                policy=policy,
                blockers=blockers,
            )

    raise CanonicalSymbolUniverseError(
        json.dumps(
            {
                "error": "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE",
                "engine_id": engine,
                "day_utc": day,
                "allow_deprecated_fallback": allow_deprecated_fallback,
                "blockers": blockers,
            },
            sort_keys=True,
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="canonical_symbol_universe_resolver_v1")
    parser.add_argument("--truth-root", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--engine-id", default="")
    parser.add_argument("--allow-deprecated-fallback", action="store_true")
    parser.add_argument("--market-data-mode", "--market_data_mode", default="FINAL_EOD_CERTIFIED")
    args = parser.parse_args(argv)
    resolution = resolve_canonical_symbol_universe_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        engine_id=str(args.engine_id or ""),
        allow_deprecated_fallback=bool(args.allow_deprecated_fallback),
        market_data_mode=str(args.market_data_mode or "FINAL_EOD_CERTIFIED"),
    )
    print(json.dumps(asdict(resolution), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
