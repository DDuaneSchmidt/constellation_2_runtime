from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ops.aegis.universe.canonical_universe_authority_v1 import (
    CANONICAL_UNIVERSE_TYPE,
    DEFAULT_DISCOVERY_TARGET_SYMBOL_COUNT,
    build_canonical_universe_authority_v1,
    last_known_good_canonical_universe_path,
    load_last_known_good_canonical_universe_v1,
    minimum_required_dynamic_symbol_count,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/canonical_universe_discovery_report.v1.schema.json"
SEED_REGISTRY_RELPATH = Path("governance/02_REGISTRIES/CANONICAL_DYNAMIC_UNIVERSE_SEED_V1.json")
KNOWN_BROAD_MANIFEST_CANDIDATES = [
    Path("/home/node/constellation_2_runtime/constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json"),
    Path("/home/node/constellation_runtime_data/truth/market_data_snapshot_v1/dataset_manifest.json"),
]
PROHIBITED_SOURCE_PARTS = {"truth_sleeves"}


class CanonicalUniverseDiscoveryError(RuntimeError):
    pass


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise CanonicalUniverseDiscoveryError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() and path.is_file() else ""


def _normalize_symbols(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return sorted({str(symbol).strip().upper() for symbol in raw if str(symbol).strip()})


def _manifest_symbols(path: Path) -> list[str]:
    payload = _read_json(path)
    symbols = _normalize_symbols(payload.get("symbols"))
    if not symbols and isinstance(payload.get("files"), list):
        symbols = _normalize_symbols([row.get("symbol") for row in payload["files"] if isinstance(row, dict)])
    return symbols


def _artifact(path: Path, artifact_type: str) -> dict[str, str]:
    return {"artifact_type": artifact_type, "path": str(path.resolve()), "sha256": _sha256(path.resolve())}


def canonical_universe_discovery_report_path(*, truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / "canonical_universe_discovery_report_v1" / str(day_utc) / "canonical_universe_discovery_report.v1.json").resolve()


def _write_report(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    validate_against_repo_schema_v1(payload, REPO_ROOT, REPORT_SCHEMA_RELPATH)
    path = canonical_universe_discovery_report_path(truth_root=truth_root, day_utc=day_utc)
    write_day_artifact_refreshable_v1(
        path=path,
        data=canonical_json_bytes_v1(payload) + b"\n",
        expected_day_utc=day_utc,
        expected_schema_id="canonical_universe_discovery_report",
        expected_schema_version="v1",
        preserve_statuses=(),
    )
    return path


def _register_lkg_from_manifest(*, truth_root: Path, day_utc: str, manifest_path: Path, target_count: int) -> dict[str, Any]:
    symbols = _manifest_symbols(manifest_path)
    authority = build_canonical_universe_authority_v1(
        truth_root=truth_root,
        source_day=day_utc,
        source_run_id=f"last-known-good-from-{_sha256(manifest_path)[:16]}",
        universe_symbols=symbols,
        writer_process="ranked_symbol_universe_v1",
        source_data_artifacts=[_artifact(manifest_path, "market_data_snapshot_v1.dataset_manifest")],
        generation_pipeline="last_known_good_canonical_universe_registration_v1",
        generation_policy={"source": "prior_broad_market_data_manifest", "universe_type": CANONICAL_UNIVERSE_TYPE},
        discovery_targets={"target_symbol_count": target_count},
        discovery_results={"manifest_symbol_count": len(symbols), "registered_as_last_known_good": True},
    )
    if authority.get("authority_status") != "PASS":
        raise CanonicalUniverseDiscoveryError(f"LAST_KNOWN_GOOD_SOURCE_BELOW_FLOOR:{manifest_path}:count={len(symbols)}")
    lkg = {**authority, "freshness_status": "LAST_KNOWN_GOOD"}
    lkg_path = last_known_good_canonical_universe_path(truth_root=truth_root)
    lkg_path.parent.mkdir(parents=True, exist_ok=True)
    lkg_path.write_bytes(canonical_json_bytes_v1(lkg) + b"\n")
    return {**lkg, "artifact_path": str(lkg_path)}


def _load_seed_registry(repo_root: Path) -> tuple[list[str], list[dict[str, str]]]:
    path = (repo_root / SEED_REGISTRY_RELPATH).resolve()
    if not path.exists():
        return [], []
    payload = _read_json(path)
    symbols = _normalize_symbols(payload.get("symbols"))
    return symbols, [_artifact(path, "canonical_dynamic_universe_seed_registry_v1")]


def _find_prior_broad_manifest(*, floor: int) -> tuple[Path | None, list[str]]:
    candidates: list[Path] = []
    for path in KNOWN_BROAD_MANIFEST_CANDIDATES:
        candidates.append(path)
    for root in [Path("/home/node/constellation_runtime_data"), Path("/home/node/constellation_2_runtime")]:
        if root.exists():
            candidates.extend(root.glob("**/market_data_snapshot_v1/dataset_manifest.json"))
    seen: set[str] = set()
    best_path: Path | None = None
    best_symbols: list[str] = []
    for path in candidates:
        resolved = path.resolve()
        key = str(resolved)
        if key in seen or not resolved.exists() or not resolved.is_file():
            continue
        seen.add(key)
        if any(part in PROHIBITED_SOURCE_PARTS for part in resolved.parts):
            continue
        try:
            symbols = _manifest_symbols(resolved)
        except Exception:
            continue
        if len(symbols) >= floor and len(symbols) > len(best_symbols):
            best_path = resolved
            best_symbols = symbols
    return best_path, best_symbols


def resolve_canonical_universe_discovery_v1(
    *,
    truth_root: Path,
    day_utc: str,
    repo_root: Path = REPO_ROOT,
    requested_target_count: int = DEFAULT_DISCOVERY_TARGET_SYMBOL_COUNT,
) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    day = str(day_utc).strip()
    floor = minimum_required_dynamic_symbol_count(int(requested_target_count))
    source_artifacts: list[dict[str, str]] = []
    rejected: dict[str, int] = {}
    blocker_codes: list[str] = []
    notes: list[str] = ["IB_SCANNER_DYNAMIC_DISCOVERY_NOT_AVAILABLE_IN_CURRENT_PIPELINE"]

    seed_symbols, seed_artifacts = _load_seed_registry(repo_root)
    if seed_symbols:
        source_artifacts.extend(seed_artifacts)
        discovery_source = "GOVERNED_BROAD_SEED_UNIVERSE"
        symbols = seed_symbols
    else:
        lkg = load_last_known_good_canonical_universe_v1(truth_root=root)
        if int(lkg.get("universe_symbol_count") or 0) >= floor:
            discovery_source = "LAST_KNOWN_GOOD_CANONICAL_UNIVERSE"
            symbols = _normalize_symbols(lkg.get("universe_symbols"))
            source_artifacts.append({"artifact_type": "last_known_good_canonical_universe_v1", "path": str(last_known_good_canonical_universe_path(truth_root=root)), "sha256": _sha256(last_known_good_canonical_universe_path(truth_root=root))})
        else:
            prior_path, prior_symbols = _find_prior_broad_manifest(floor=floor)
            if prior_path is not None:
                lkg = _register_lkg_from_manifest(truth_root=root, day_utc=day, manifest_path=prior_path, target_count=requested_target_count)
                discovery_source = "PRIOR_BROAD_MANIFEST_REGISTERED_AS_LAST_KNOWN_GOOD"
                symbols = _normalize_symbols(lkg.get("universe_symbols"))
                source_artifacts.append(_artifact(prior_path, "prior_broad_market_data_manifest"))
                source_artifacts.append({"artifact_type": "last_known_good_canonical_universe_v1", "path": str(last_known_good_canonical_universe_path(truth_root=root)), "sha256": _sha256(last_known_good_canonical_universe_path(truth_root=root))})
            else:
                discovery_source = "NONE"
                symbols = []
                blocker_codes.append("CANONICAL_UNIVERSE_DISCOVERY_SOURCE_UNAVAILABLE")

    accepted = _normalize_symbols(symbols)
    if len(accepted) < floor:
        blocker_codes.append("UNIVERSE_BREADTH_FAILURE")
    status = "PASS" if not blocker_codes else "FAIL"
    rejected["BELOW_DISCOVERY_FLOOR"] = max(0, floor - len(accepted)) if len(accepted) < floor else 0
    payload = {
        "schema_id": "canonical_universe_discovery_report",
        "schema_version": "v1",
        "day_utc": day,
        "discovery_source": discovery_source,
        "requested_target_count": int(requested_target_count),
        "minimum_required_symbol_count": floor,
        "discovered_count": len(symbols),
        "accepted_count": len(accepted),
        "accepted_symbols": accepted,
        "rejected_count_by_reason": rejected,
        "last_known_good_used": discovery_source in {"LAST_KNOWN_GOOD_CANONICAL_UNIVERSE", "PRIOR_BROAD_MANIFEST_REGISTERED_AS_LAST_KNOWN_GOOD"},
        "current_refresh_success": False,
        "status": status,
        "blocker_codes": sorted(set(blocker_codes)),
        "notes": notes,
        "generated_at": _now_utc(),
        "source_artifacts": source_artifacts,
    }
    path = _write_report(truth_root=root, day_utc=day, payload=payload)
    return {**payload, "artifact_path": str(path)}
