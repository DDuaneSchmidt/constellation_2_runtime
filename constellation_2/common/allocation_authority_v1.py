from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
EPOCH_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/sleeve_allocation_epoch.v1.schema.json"
ALLOCATION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _select_latest_epoch(day_utc: str, truth_root: Path, repo_root: Path) -> Dict[str, Any] | None:
    epoch_dir = (truth_root / "allocation_v1" / "sleeve_allocation_epoch_v1" / day_utc).resolve()
    if not epoch_dir.exists() or not epoch_dir.is_dir():
        return None
    matches = sorted(epoch_dir.glob("*.sleeve_allocation_epoch.v1.json"))
    latest_path: Path | None = None
    latest_obj: Dict[str, Any] | None = None
    latest_key: tuple[str, str] | None = None
    for path in matches:
        obj = _read_json_obj(path)
        validate_against_repo_schema_v1(obj, repo_root, EPOCH_SCHEMA_RELPATH)
        produced_utc = str(obj.get("produced_utc") or "").strip()
        epoch_id = str(obj.get("epoch_id") or "").strip()
        if not produced_utc or not epoch_id:
            raise ValueError(f"EPOCH_KEY_FIELDS_MISSING: {path}")
        key = (produced_utc, epoch_id)
        if latest_key is None or key > latest_key:
            latest_key = key
            latest_path = path
            latest_obj = obj
    if latest_path is None or latest_obj is None:
        return None
    return {
        "authority_source": "SLEEVE_ALLOCATION_EPOCH_V1",
        "path": latest_path,
        "obj": latest_obj,
        "epoch_id": str(latest_obj["epoch_id"]),
    }


def _fallback_authority(day_utc: str, truth_root: Path, repo_root: Path) -> Dict[str, Any]:
    allocation_path = (
        truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day_utc / "capital_authority_allocation.v1.json"
    ).resolve()
    if not allocation_path.exists() or not allocation_path.is_file():
        raise FileNotFoundError(f"ALLOCATION_FILE_MISSING: {allocation_path}")
    allocation_obj = _read_json_obj(allocation_path)
    validate_against_repo_schema_v1(allocation_obj, repo_root, ALLOCATION_SCHEMA_RELPATH)
    allocation_sha = _sha256_file(allocation_path)
    per_sleeve_raw = allocation_obj.get("per_sleeve")
    if not isinstance(per_sleeve_raw, list) or not per_sleeve_raw:
        raise ValueError(f"ALLOCATION_PER_SLEEVE_INVALID: {allocation_path}")
    per_sleeve: List[Dict[str, Any]] = []
    for item in per_sleeve_raw:
        if not isinstance(item, dict):
            raise ValueError(f"ALLOCATION_SLEEVE_NOT_OBJECT: {allocation_path}")
        per_sleeve.append(
            {
                "canonical_sleeve_id": str(item.get("sleeve_id") or "").strip(),
                "engine_ids": [str(engine_id or "").strip() for engine_id in item.get("engine_ids") or []],
                "assigned_budget_cents": int(item.get("allowed_capital_at_risk_cents") or 0),
                "used_budget_cents": int(item.get("used_capital_at_risk_cents") or 0),
                "remaining_budget_cents": int(item.get("headroom_cents") or 0),
            }
        )
    epoch_id = hashlib.sha256(
        "\n".join(
            [
                "C2_ALLOCATION_AUTHORITY_FALLBACK_V1",
                day_utc,
                allocation_sha,
            ]
        ).encode("utf-8")
    ).hexdigest()
    normalized = {
        "schema_id": "C2_ALLOCATION_AUTHORITY_FALLBACK_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": str(allocation_obj.get("produced_utc") or "").strip(),
        "epoch_id": epoch_id,
        "portfolio_snapshot_id": "",
        "policy_hash": "",
        "sleeve_registry_hash": "",
        "correlation_gate_hash": str(
            (allocation_obj.get("correlation_gate_binding") or {}).get("gate_artifact_sha256") or ""
        ).strip(),
        "portfolio_allowed_capital_at_risk_cents": int(
            (allocation_obj.get("portfolio") or {}).get("allowed_capital_at_risk_cents") or 0
        ),
        "portfolio_used_capital_at_risk_cents": int(
            (allocation_obj.get("portfolio") or {}).get("used_capital_at_risk_cents") or 0
        ),
        "portfolio_headroom_cents": int((allocation_obj.get("portfolio") or {}).get("headroom_cents") or 0),
        "per_sleeve": per_sleeve,
        "input_manifest": allocation_obj.get("input_manifest") or [],
    }
    return {
        "authority_source": "CAPITAL_AUTHORITY_ALLOCATION_V1_FALLBACK",
        "path": allocation_path,
        "obj": normalized,
        "epoch_id": epoch_id,
    }


def read_allocation_authority_v1(*, day_utc: str, truth_root: Path, repo_root: Path | None = None) -> Dict[str, Any]:
    root = (repo_root or REPO_ROOT).resolve()
    truth_root = truth_root.resolve()
    epoch_authority = _select_latest_epoch(day_utc, truth_root, root)
    if epoch_authority is not None:
        return epoch_authority
    return _fallback_authority(day_utc, truth_root, root)
