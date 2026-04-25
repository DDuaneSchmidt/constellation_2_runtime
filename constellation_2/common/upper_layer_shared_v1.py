from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    parse_day_utc_v1,
    read_validated_surface_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


REPO_ROOT = Path(__file__).resolve().parents[2]

TRADE_IDENTITY_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/trade_identity.v1.schema.json'
INCORPORATED_STATE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/incorporated_broker_trade_state.v1.schema.json'
RECONCILED_DESCRIPTION_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciled_trade_description.v1.schema.json'
RECONCILIATION_HEALTH_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_health.v1.schema.json'
RECONCILIATION_PROVENANCE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_provenance.v1.schema.json'
CORE1_HEALTH_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_health.v1.schema.json'
CORE3_AUTHORITY_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/lifecycle_action_authority.v1.schema.json'
CORE4_BOUNDARY_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_submit_boundary.v1.schema.json'
CORE5_HEALTH_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_trade_health.v1.schema.json'


@dataclass(frozen=True)
class UpperCore2BundleV1:
    trade_dir: Path
    trade_identity: SurfaceRefV1
    incorporated_state: SurfaceRefV1
    reconciled_description: SurfaceRefV1
    reconciliation_health: SurfaceRefV1
    reconciliation_provenance: SurfaceRefV1


def read_json_object_v1(path: Path) -> Dict[str, Any]:
    obj = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT:path={path}')
    return obj


def coerce_utc_text_v1(text: str) -> str:
    raw = str(text or '').strip()
    if not raw:
        raise ValueError('UTC_TEXT_MISSING')
    normalized = raw[:-1] + '+00:00' if raw.endswith('Z') else raw
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def now_utc_text_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def day_utc_from_timestamp_v1(text: str) -> str:
    return coerce_utc_text_v1(text)[:10]


def stable_sha256_id_v1(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(payload))).hexdigest()


def sha256_file_v1(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def stable_unique_codes_v1(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = str(value or '').strip()
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def artifact_ref_from_surface_v1(ref: SurfaceRefV1) -> Dict[str, str]:
    return {'artifact_path': str(ref.path), 'artifact_sha256': str(ref.sha256)}


def artifact_ref_from_path_v1(path: Path) -> Dict[str, str]:
    resolved = Path(path).resolve()
    return {'artifact_path': str(resolved), 'artifact_sha256': sha256_file_v1(resolved)}


def logical_artifact_ref_v1(*, logical_name: str, path: Path) -> Dict[str, str]:
    ref = artifact_ref_from_path_v1(path)
    return {'logical_name': str(logical_name), **ref}


def repo_artifact_ref_v1(relpath: str) -> Dict[str, str]:
    return artifact_ref_from_path_v1((REPO_ROOT / relpath).resolve())


def load_core2_trade_bundle_v1(trade_dir: Path) -> UpperCore2BundleV1:
    root = Path(trade_dir).resolve()
    return UpperCore2BundleV1(
        trade_dir=root,
        trade_identity=read_validated_surface_v1(path=root / 'trade_identity.v1.json', schema_relpath=TRADE_IDENTITY_SCHEMA_RELPATH),
        incorporated_state=read_validated_surface_v1(path=root / 'incorporated_broker_trade_state.v1.json', schema_relpath=INCORPORATED_STATE_SCHEMA_RELPATH),
        reconciled_description=read_validated_surface_v1(path=root / 'reconciled_trade_description.v1.json', schema_relpath=RECONCILED_DESCRIPTION_SCHEMA_RELPATH),
        reconciliation_health=read_validated_surface_v1(path=root / 'reconciliation_health.v1.json', schema_relpath=RECONCILIATION_HEALTH_SCHEMA_RELPATH),
        reconciliation_provenance=read_validated_surface_v1(path=root / 'reconciliation_provenance.v1.json', schema_relpath=RECONCILIATION_PROVENANCE_SCHEMA_RELPATH),
    )


def core2_refs_v1(bundle: UpperCore2BundleV1) -> Dict[str, Dict[str, str]]:
    return {
        'trade_identity_ref': artifact_ref_from_surface_v1(bundle.trade_identity),
        'incorporated_state_ref': artifact_ref_from_surface_v1(bundle.incorporated_state),
        'reconciled_description_ref': artifact_ref_from_surface_v1(bundle.reconciled_description),
        'reconciliation_health_ref': artifact_ref_from_surface_v1(bundle.reconciliation_health),
        'reconciliation_provenance_ref': artifact_ref_from_surface_v1(bundle.reconciliation_provenance),
    }


def trade_identity_pointer_v1(bundle: UpperCore2BundleV1) -> Dict[str, str]:
    payload = bundle.trade_identity.payload
    return {
        'trade_identity_id': str(payload.get('trade_identity_id') or ''),
        'artifact_path': str(bundle.trade_identity.path),
        'artifact_sha256': str(bundle.trade_identity.sha256),
    }


def load_surface_from_path_v1(*, path: str | Path, schema_relpath: str) -> SurfaceRefV1:
    return read_validated_surface_v1(path=Path(path).expanduser().resolve(), schema_relpath=schema_relpath)


def optional_logical_ref_v1(*, logical_name: str, path: str | Path | None) -> list[Dict[str, str]]:
    if not path:
        return []
    resolved = Path(str(path)).expanduser().resolve()
    if not resolved.exists() or not resolved.is_file():
        return []
    return [logical_artifact_ref_v1(logical_name=logical_name, path=resolved)]


def resolve_core2_trade_dir_v1(*, execution_root: Path, day_utc: str, core2_materialization_set_id: str, trade_identity_id: str) -> Path:
    return (
        Path(execution_root).resolve()
        / 'reconciled_trade_state_v1'
        / 'materializations'
        / parse_day_utc_v1(day_utc)
        / str(core2_materialization_set_id).strip()
        / 'trades'
        / str(trade_identity_id).strip()
    ).resolve()


def resolve_core3_authority_path_v1(*, execution_root: Path, day_utc: str, core3_materialization_set_id: str, trade_identity_id: str) -> Path:
    return (
        Path(execution_root).resolve()
        / 'lifecycle_action_authority_v1'
        / 'materializations'
        / parse_day_utc_v1(day_utc)
        / str(core3_materialization_set_id).strip()
        / 'trades'
        / str(trade_identity_id).strip()
        / 'lifecycle_action_authority.v1.json'
    ).resolve()


def resolve_core1_health_path_v1(*, execution_root: Path, day_utc: str) -> Path:
    return (
        Path(execution_root).resolve()
        / 'reports'
        / 'broker_observation_health_v1'
        / parse_day_utc_v1(day_utc)
        / 'broker_observation_health.v1.json'
    ).resolve()


def resolve_upper_family_trade_dir_v1(*, execution_root: Path, family: str, day_utc: str, artifact_id: str, trade_identity_id: str) -> Path:
    return (
        Path(execution_root).resolve()
        / family
        / 'materializations'
        / parse_day_utc_v1(day_utc)
        / str(artifact_id).strip()
        / 'trades'
        / str(trade_identity_id).strip()
    ).resolve()


def resolve_upper_provenance_dir_v1(*, execution_root: Path, day_utc: str, artifact_family: str, artifact_id: str) -> Path:
    return (
        Path(execution_root).resolve()
        / 'upper_layer_provenance_spine_v1'
        / 'materializations'
        / parse_day_utc_v1(day_utc)
        / str(artifact_family).strip()
        / str(artifact_id).strip()
    ).resolve()


def write_validated_payload_v1(*, path: Path, payload: Dict[str, Any], schema_relpath: str) -> SurfaceRefV1:
    return atomic_write_validated_json_v1(path=Path(path).resolve(), payload=payload, schema_relpath=schema_relpath)


def cadence_window_v1(*, evaluated_at_utc: str, cadence_seconds: int) -> Dict[str, str | int]:
    if cadence_seconds <= 0:
        raise ValueError('CADENCE_SECONDS_INVALID')
    evaluation = datetime.fromisoformat(coerce_utc_text_v1(evaluated_at_utc).replace('Z', '+00:00'))
    epoch = int(evaluation.timestamp())
    window_start_epoch = epoch - (epoch % cadence_seconds)
    window_start = datetime.fromtimestamp(window_start_epoch, tz=UTC)
    window_end = window_start + timedelta(seconds=cadence_seconds)
    return {
        'cadence_class': 'FIXED_INTERVAL',
        'cadence_seconds': cadence_seconds,
        'window_started_at_utc': window_start.isoformat().replace('+00:00', 'Z'),
        'window_ends_at_utc': window_end.isoformat().replace('+00:00', 'Z'),
    }
