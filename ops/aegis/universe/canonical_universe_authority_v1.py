from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import write_file_immutable_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/canonical_universe_authority.v1.schema.json"

ALLOWED_CANONICAL_AUTHORITY_WRITERS = {
    "canonical_market_data_refresh_v1",
    "ranked_symbol_universe_v1",
}
PROHIBITED_CANONICAL_AUTHORITY_WRITERS = {
    "sleeve_local_manifest",
    "allocation_v1",
    "capital_allocation_v1",
    "portfolio_gate",
    "alignment_refresh",
    "paper_sleeve_truth",
    "curated_sleeve_refresh",
    "active_registry_sync",
    "operator_state_rebuild",
}
UNIVERSE_TYPES = {
    "CANONICAL_DYNAMIC",
    "RANKED_DYNAMIC",
    "ENGINE_FILTERED",
    "CURATED",
    "SLEEVE_LOCAL",
    "TEMPORARY",
    "DEPRECATED",
}
CANONICAL_UNIVERSE_TYPE = "CANONICAL_DYNAMIC"
DEFAULT_DISCOVERY_TARGET_SYMBOL_COUNT = 200
MAX_UNIVERSE_SHRINK_WITHOUT_OVERRIDE_PCT = 20.0
DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_NUMERATOR = 4
DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR = 5


def minimum_required_dynamic_symbol_count(target_symbol_count: int = DEFAULT_DISCOVERY_TARGET_SYMBOL_COUNT) -> int:
    target = int(target_symbol_count)
    return (target * DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_NUMERATOR + DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR - 1) // DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR



class CanonicalUniverseAuthorityError(RuntimeError):
    pass


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_symbols(raw: Iterable[Any] | Any) -> list[str]:
    if not isinstance(raw, Iterable) or isinstance(raw, (str, bytes, dict)):
        return []
    return sorted({str(symbol).strip().upper() for symbol in raw if str(symbol).strip()})


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CanonicalUniverseAuthorityError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return payload


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() and path.is_file() else ""


def _artifact_ref(path: Path, artifact_type: str) -> dict[str, str]:
    return {"artifact_type": artifact_type, "path": str(path.resolve()), "sha256": _sha256_path(path.resolve())}


def canonical_universe_authority_path(*, truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / "canonical_universe_authority_v1" / str(day_utc) / "canonical_universe_authority.v1.json").resolve()


def last_known_good_canonical_universe_path(*, truth_root: Path) -> Path:
    return (Path(truth_root).resolve() / "reports" / "last_known_good_canonical_universe_v1" / "last_known_good_canonical_universe.v1.json").resolve()


def blocked_authority_write_path(*, truth_root: Path, day_utc: str, source_run_id: str) -> Path:
    safe_run = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(source_run_id or "run").strip())[:96] or "run"
    return (Path(truth_root).resolve() / "reports" / "canonical_universe_authority_blocked_write_v1" / str(day_utc) / f"{safe_run}.json").resolve()


def _authority_hash(payload: dict[str, Any]) -> str:
    tmp = dict(payload)
    tmp["immutable_hash"] = ""
    return hashlib.sha256(canonical_json_bytes_v1(tmp)).hexdigest()


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return _read_json(path)
    except Exception:
        return {}


def load_canonical_universe_authority_v1(*, truth_root: Path, day_utc: str, validate: bool = False) -> dict[str, Any]:
    path = canonical_universe_authority_path(truth_root=Path(truth_root), day_utc=str(day_utc))
    payload = _read_json(path)
    if validate:
        validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)
    return payload


def load_last_known_good_canonical_universe_v1(*, truth_root: Path) -> dict[str, Any]:
    return _load_json_if_exists(last_known_good_canonical_universe_path(truth_root=Path(truth_root)))


def latest_canonical_universe_authority_v1(*, truth_root: Path, day_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    if day_utc:
        path = canonical_universe_authority_path(truth_root=root, day_utc=str(day_utc))
        if path.exists():
            return _read_json(path)
        lkg = load_last_known_good_canonical_universe_v1(truth_root=root)
        if lkg:
            return {**lkg, "freshness_status": "STALE_LAST_KNOWN_GOOD", "requested_day_utc": str(day_utc)}
    base = root / "reports" / "canonical_universe_authority_v1"
    paths = sorted(base.glob("*/canonical_universe_authority.v1.json")) if base.exists() else []
    if paths:
        return _read_json(paths[-1])
    return load_last_known_good_canonical_universe_v1(truth_root=root)


def _latest_valid_authority_for_comparison(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    current = _load_json_if_exists(canonical_universe_authority_path(truth_root=truth_root, day_utc=day_utc))
    if str(current.get("authority_status") or "").upper() == "PASS":
        return current
    lkg = load_last_known_good_canonical_universe_v1(truth_root=truth_root)
    if str(lkg.get("authority_status") or "").upper() == "PASS":
        return lkg
    base = truth_root / "reports" / "canonical_universe_authority_v1"
    paths = sorted(base.glob("*/canonical_universe_authority.v1.json")) if base.exists() else []
    for path in reversed(paths):
        payload = _load_json_if_exists(path)
        if str(payload.get("authority_status") or "").upper() == "PASS":
            return payload
    return {}


def _override_allows_narrowing(path: str | Path | None) -> bool:
    if path is None or not str(path).strip():
        return False
    obj = _load_json_if_exists(Path(path).expanduser().resolve())
    return bool(obj.get("allow_universe_narrowing_override") is True and obj.get("override_scope") == "CANONICAL_DYNAMIC_UNIVERSE_NARROWING")


def _write_blocked_attempt(*, truth_root: Path, day_utc: str, source_run_id: str, reason_codes: list[str], payload: dict[str, Any], previous: dict[str, Any]) -> Path:
    out_path = blocked_authority_write_path(truth_root=truth_root, day_utc=day_utc, source_run_id=source_run_id)
    record = {
        "schema_id": "canonical_universe_authority_blocked_write",
        "schema_version": "v1",
        "source_day": day_utc,
        "source_run_id": source_run_id,
        "blocked_at": _now_utc(),
        "reason_codes": sorted(set(reason_codes)),
        "attempted_writer": payload.get("writer_process", ""),
        "attempted_universe_type": payload.get("universe_type", ""),
        "attempted_symbol_count": int(payload.get("universe_symbol_count") or 0),
        "previous_canonical_universe_authority_id": str(previous.get("canonical_universe_authority_id") or ""),
        "previous_universe_symbol_count": int(previous.get("universe_symbol_count") or 0),
        "attempted_authority_id": str(payload.get("canonical_universe_authority_id") or ""),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_file_immutable_v1(path=out_path, data=canonical_json_bytes_v1(record) + b"\n", create_dirs=True)
    return out_path


def build_canonical_universe_authority_v1(
    *,
    truth_root: Path,
    source_day: str,
    source_run_id: str,
    universe_symbols: list[str],
    writer_process: str,
    source_data_artifacts: list[dict[str, Any]] | None = None,
    generation_pipeline: str = "",
    generation_policy: dict[str, Any] | None = None,
    discovery_targets: dict[str, Any] | None = None,
    discovery_results: dict[str, Any] | None = None,
    universe_type: str = CANONICAL_UNIVERSE_TYPE,
) -> dict[str, Any]:
    symbols = _normalize_symbols(universe_symbols)
    target = int((discovery_targets or {}).get("target_symbol_count") or DEFAULT_DISCOVERY_TARGET_SYMBOL_COUNT)
    min_required = minimum_required_dynamic_symbol_count(target)
    status = "PASS" if len(symbols) >= min_required else "FAILED"
    reason_codes: list[str] = []
    if len(symbols) < min_required:
        reason_codes.append("UNIVERSE_BREADTH_FAILURE")
    if str(universe_type).strip().upper() != CANONICAL_UNIVERSE_TYPE:
        reason_codes.append("NON_CANONICAL_UNIVERSE_TYPE_REJECTED")
    previous = _latest_valid_authority_for_comparison(truth_root=Path(truth_root).resolve(), day_utc=source_day)
    previous_count = int(previous.get("universe_symbol_count") or 0)
    shrink_pct = "0"
    if previous_count > 0:
        shrink_value = max(0.0, (previous_count - len(symbols)) * 100.0 / previous_count)
        shrink_pct = f"{shrink_value:.6f}".rstrip("0").rstrip(".") or "0"
    payload = {
        "schema_id": "canonical_universe_authority",
        "schema_version": "v1",
        "canonical_universe_authority_id": "",
        "day_utc": source_day,
        "source_day": source_day,
        "source_run_id": source_run_id,
        "authority_status": status,
        "universe_type": CANONICAL_UNIVERSE_TYPE,
        "writer_process": writer_process,
        "universe_symbol_count": len(symbols),
        "universe_symbols": symbols,
        "source_data_artifacts": list(source_data_artifacts or []),
        "generation_pipeline": generation_pipeline or writer_process,
        "generation_policy": dict(generation_policy or {}),
        "discovery_targets": {**dict(discovery_targets or {}), "minimum_required_symbol_count": min_required},
        "discovery_results": dict(discovery_results or {}),
        "allowed_downstream_consumers": ["ranked_symbol_universe_v1", "engine_universe_candidate_basis_v1", "run_sleeve_evaluation_kernel_v1", "operator_state_read_models"],
        "prohibited_writers": sorted(PROHIBITED_CANONICAL_AUTHORITY_WRITERS),
        "previous_canonical_universe_authority_id": str(previous.get("canonical_universe_authority_id") or ""),
        "previous_universe_symbol_count": previous_count,
        "shrink_percent": shrink_pct,
        "freshness_status": "CURRENT",
        "last_known_good_canonical_universe_authority_id": str(previous.get("canonical_universe_authority_id") or ""),
        "blocked_reason_codes": sorted(set(reason_codes)),
        "immutable_hash": "",
        "generated_at": _now_utc(),
    }
    payload["immutable_hash"] = _authority_hash(payload)
    payload["canonical_universe_authority_id"] = f"cua_{source_day}_{hashlib.sha256((source_run_id + payload['immutable_hash']).encode()).hexdigest()[:16]}"
    payload["immutable_hash"] = _authority_hash(payload)
    return payload


def emit_canonical_universe_authority_v1(
    *,
    truth_root: Path,
    source_day: str,
    source_run_id: str,
    universe_symbols: list[str],
    writer_process: str,
    source_data_artifacts: list[dict[str, Any]] | None = None,
    generation_pipeline: str = "",
    generation_policy: dict[str, Any] | None = None,
    discovery_targets: dict[str, Any] | None = None,
    discovery_results: dict[str, Any] | None = None,
    universe_type: str = CANONICAL_UNIVERSE_TYPE,
    override_artifact_path: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    day = str(source_day).strip()
    run_id = str(source_run_id).strip() or "unknown_run"
    writer = str(writer_process).strip()
    universe_type = str(universe_type).strip().upper() or CANONICAL_UNIVERSE_TYPE
    if writer not in ALLOWED_CANONICAL_AUTHORITY_WRITERS or writer in PROHIBITED_CANONICAL_AUTHORITY_WRITERS:
        payload = build_canonical_universe_authority_v1(truth_root=root, source_day=day, source_run_id=run_id, universe_symbols=universe_symbols, writer_process=writer, source_data_artifacts=source_data_artifacts, generation_pipeline=generation_pipeline, generation_policy=generation_policy, discovery_targets=discovery_targets, discovery_results=discovery_results, universe_type=universe_type)
        _write_blocked_attempt(truth_root=root, day_utc=day, source_run_id=run_id, reason_codes=["PROHIBITED_CANONICAL_UNIVERSE_WRITER"], payload=payload, previous=_latest_valid_authority_for_comparison(truth_root=root, day_utc=day))
        raise CanonicalUniverseAuthorityError(f"PROHIBITED_CANONICAL_UNIVERSE_WRITER:{writer}")
    if universe_type != CANONICAL_UNIVERSE_TYPE:
        payload = build_canonical_universe_authority_v1(truth_root=root, source_day=day, source_run_id=run_id, universe_symbols=universe_symbols, writer_process=writer, source_data_artifacts=source_data_artifacts, generation_pipeline=generation_pipeline, generation_policy=generation_policy, discovery_targets=discovery_targets, discovery_results=discovery_results, universe_type=universe_type)
        _write_blocked_attempt(truth_root=root, day_utc=day, source_run_id=run_id, reason_codes=["NON_CANONICAL_UNIVERSE_TYPE_REJECTED"], payload=payload, previous=_latest_valid_authority_for_comparison(truth_root=root, day_utc=day))
        raise CanonicalUniverseAuthorityError(f"NON_CANONICAL_UNIVERSE_TYPE_REJECTED:{universe_type}")

    payload = build_canonical_universe_authority_v1(truth_root=root, source_day=day, source_run_id=run_id, universe_symbols=universe_symbols, writer_process=writer, source_data_artifacts=source_data_artifacts, generation_pipeline=generation_pipeline, generation_policy=generation_policy, discovery_targets=discovery_targets, discovery_results=discovery_results, universe_type=universe_type)
    previous = _latest_valid_authority_for_comparison(truth_root=root, day_utc=day)
    existing_path = canonical_universe_authority_path(truth_root=root, day_utc=day)
    existing = _load_json_if_exists(existing_path)
    if existing and str(existing.get("source_run_id") or "") == run_id and str(existing.get("immutable_hash") or "") != str(payload.get("immutable_hash") or ""):
        _write_blocked_attempt(truth_root=root, day_utc=day, source_run_id=run_id, reason_codes=["WRITE_ONCE_PER_RUN_VIOLATION"], payload=payload, previous=existing)
        raise CanonicalUniverseAuthorityError("WRITE_ONCE_PER_RUN_VIOLATION")
    if float(payload.get("shrink_percent") or 0.0) > MAX_UNIVERSE_SHRINK_WITHOUT_OVERRIDE_PCT and not _override_allows_narrowing(override_artifact_path):
        _write_blocked_attempt(truth_root=root, day_utc=day, source_run_id=run_id, reason_codes=["UNIVERSE_COLLAPSE_PROTECTED"], payload=payload, previous=previous)
        raise CanonicalUniverseAuthorityError(f"UNIVERSE_COLLAPSE_PROTECTED:previous={payload['previous_universe_symbol_count']}:new={payload['universe_symbol_count']}:shrink_percent={payload['shrink_percent']}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)
    data = canonical_json_bytes_v1(payload) + b"\n"
    write_day_artifact_refreshable_v1(path=existing_path, data=data, expected_day_utc=day, expected_schema_id="canonical_universe_authority", expected_schema_version="v1", preserve_statuses=())
    if str(payload.get("authority_status") or "") == "PASS":
        lkg_path = last_known_good_canonical_universe_path(truth_root=root)
        lkg_payload = {**payload, "freshness_status": "LAST_KNOWN_GOOD"}
        lkg_payload["immutable_hash"] = _authority_hash(lkg_payload)
        lkg_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = lkg_path.with_name(f".{lkg_path.name}.tmp")
        tmp_path.write_bytes(canonical_json_bytes_v1(lkg_payload) + b"\n")
        tmp_path.replace(lkg_path)
    return {**payload, "artifact_path": str(existing_path)}


def emit_canonical_universe_authority_from_manifest_v1(*, truth_root: Path, source_day: str, source_run_id: str, writer_process: str, generation_pipeline: str, discovery_targets: dict[str, Any] | None = None, discovery_results: dict[str, Any] | None = None, override_artifact_path: str | Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    manifest_path = (root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
    manifest = _read_json(manifest_path)
    symbols = _normalize_symbols(manifest.get("symbols"))
    if not symbols and isinstance(manifest.get("files"), list):
        symbols = _normalize_symbols([row.get("symbol") for row in manifest["files"] if isinstance(row, dict)])
    return emit_canonical_universe_authority_v1(
        truth_root=root,
        source_day=source_day,
        source_run_id=source_run_id,
        universe_symbols=symbols,
        writer_process=writer_process,
        source_data_artifacts=[_artifact_ref(manifest_path, "market_data_snapshot_v1.dataset_manifest")],
        generation_pipeline=generation_pipeline,
        generation_policy={"source": "market_data_snapshot_v1.dataset_manifest", "universe_type": CANONICAL_UNIVERSE_TYPE},
        discovery_targets=discovery_targets or {"target_symbol_count": DEFAULT_DISCOVERY_TARGET_SYMBOL_COUNT},
        discovery_results={**dict(discovery_results or {}), "manifest_symbol_count": len(symbols)},
        universe_type=CANONICAL_UNIVERSE_TYPE,
        override_artifact_path=override_artifact_path,
    )


def require_current_canonical_universe_authority_v1(*, truth_root: Path, day_utc: str, minimum_symbol_count: int | None = None, allow_stale_degraded: bool = False) -> dict[str, Any]:
    authority = latest_canonical_universe_authority_v1(truth_root=Path(truth_root), day_utc=day_utc)
    if not authority:
        raise CanonicalUniverseAuthorityError("CANONICAL_UNIVERSE_AUTHORITY_MISSING")
    if str(authority.get("universe_type") or "") != CANONICAL_UNIVERSE_TYPE:
        raise CanonicalUniverseAuthorityError(f"CANONICAL_UNIVERSE_AUTHORITY_BAD_TYPE:{authority.get('universe_type')}")
    if str(authority.get("freshness_status") or "") != "CURRENT" and not allow_stale_degraded:
        raise CanonicalUniverseAuthorityError(f"CANONICAL_UNIVERSE_AUTHORITY_STALE_DEGRADED:{authority.get('freshness_status')}")
    if str(authority.get("authority_status") or "").upper() != "PASS":
        raise CanonicalUniverseAuthorityError(f"CANONICAL_UNIVERSE_AUTHORITY_NOT_PASS:{authority.get('authority_status')}:symbol_count={authority.get('universe_symbol_count')}:reason_codes={authority.get('blocked_reason_codes') or []}")
    required = int(minimum_symbol_count or 0)
    if required and int(authority.get("universe_symbol_count") or 0) < required:
        raise CanonicalUniverseAuthorityError(f"CANONICAL_UNIVERSE_AUTHORITY_BREADTH_FAILURE:symbol_count={authority.get('universe_symbol_count')}:minimum_required={required}")
    return authority


def canonical_universe_health_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    current = latest_canonical_universe_authority_v1(truth_root=root, day_utc=day_utc)
    lkg = load_last_known_good_canonical_universe_v1(truth_root=root)
    blocked_dir = root / "reports" / "canonical_universe_authority_blocked_write_v1" / str(day_utc)
    blocked_paths = sorted(str(path.resolve()) for path in blocked_dir.glob("*.json")) if blocked_dir.exists() else []
    status = "MISSING"
    if current:
        status = "PASS" if str(current.get("authority_status") or "").upper() == "PASS" and str(current.get("freshness_status") or "") == "CURRENT" else "DEGRADED"
    return {
        "schema_id": "canonical_universe_health",
        "schema_version": "v1",
        "source_day": str(day_utc),
        "status": status,
        "current_canonical_universe_authority_id": str(current.get("canonical_universe_authority_id") or ""),
        "current_count": int(current.get("universe_symbol_count") or 0),
        "current_authority_status": str(current.get("authority_status") or ""),
        "previous_canonical_universe_authority_id": str(current.get("previous_canonical_universe_authority_id") or ""),
        "previous_count": int(current.get("previous_universe_symbol_count") or 0),
        "shrink_percent": float(current.get("shrink_percent") or 0.0),
        "universe_type": str(current.get("universe_type") or ""),
        "writer_process": str(current.get("writer_process") or ""),
        "freshness_status": str(current.get("freshness_status") or ""),
        "degraded_status": "NONE" if status == "PASS" else status,
        "last_known_good_canonical_universe_authority_id": str(lkg.get("canonical_universe_authority_id") or ""),
        "last_known_good_count": int(lkg.get("universe_symbol_count") or 0),
        "last_known_good_status": str(lkg.get("authority_status") or ""),
        "blocked_overwrite_attempt_count": len(blocked_paths),
        "blocked_overwrite_attempt_paths": blocked_paths,
        "allowed_downstream_consumers": list(current.get("allowed_downstream_consumers") or []),
        "prohibited_writers": list(current.get("prohibited_writers") or sorted(PROHIBITED_CANONICAL_AUTHORITY_WRITERS)),
        "reason_codes": list(current.get("blocked_reason_codes") or []),
    }
