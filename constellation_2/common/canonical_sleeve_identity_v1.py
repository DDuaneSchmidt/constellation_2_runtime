from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_RELPATH = "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def engine_to_canonical_sleeve_map_from_policy_v1(policy_obj: Dict[str, Any]) -> Dict[str, str]:
    if str(policy_obj.get("schema_id") or "").strip() != "C2_CAPITAL_AUTHORITY_POLICY_V1":
        raise ValueError("POLICY_SCHEMA_ID_INVALID")
    sleeves = policy_obj.get("sleeves")
    if not isinstance(sleeves, list) or not sleeves:
        raise ValueError("POLICY_SLEEVES_INVALID")
    mapping: Dict[str, str] = {}
    for sleeve in sleeves:
        if not isinstance(sleeve, dict):
            raise ValueError("POLICY_SLEEVE_NOT_OBJECT")
        sleeve_id = str(sleeve.get("sleeve_id") or "").strip()
        if not sleeve_id:
            raise ValueError("POLICY_SLEEVE_ID_MISSING")
        engine_ids = sleeve.get("engine_ids")
        if not isinstance(engine_ids, list) or not engine_ids:
            raise ValueError(f"POLICY_ENGINE_IDS_INVALID: {sleeve_id}")
        for raw_engine_id in engine_ids:
            engine_id = str(raw_engine_id or "").strip()
            if not engine_id:
                raise ValueError(f"POLICY_ENGINE_ID_EMPTY: {sleeve_id}")
            existing = mapping.get(engine_id)
            if existing is not None and existing != sleeve_id:
                raise ValueError(f"ENGINE_ID_AMBIGUOUS: {engine_id}")
            mapping[engine_id] = sleeve_id
    if not mapping:
        raise ValueError("ENGINE_TO_SLEEVE_MAP_EMPTY")
    return dict(sorted(mapping.items()))


def load_engine_to_canonical_sleeve_map_v1(*, repo_root: Path | None = None) -> Dict[str, str]:
    root = (repo_root or REPO_ROOT).resolve()
    policy_path = (root / POLICY_RELPATH).resolve()
    return engine_to_canonical_sleeve_map_from_policy_v1(_read_json_obj(policy_path))


def canonical_sleeve_id_from_engine_id(engine_id: str, *, repo_root: Path | None = None) -> str:
    normalized_engine_id = str(engine_id or "").strip()
    if not normalized_engine_id:
        raise ValueError("ENGINE_ID_MISSING")
    mapping = load_engine_to_canonical_sleeve_map_v1(repo_root=repo_root)
    sleeve_id = mapping.get(normalized_engine_id)
    if not sleeve_id:
        raise ValueError(f"ENGINE_ID_NOT_MAPPED: {normalized_engine_id}")
    return sleeve_id


def canonical_sleeve_id_from_execution_scope(scope: str, engine_id: str, *, repo_root: Path | None = None) -> str:
    normalized_scope = str(scope or "").strip()
    if normalized_scope != "PRIMARY":
        raise ValueError(f"EXECUTION_SCOPE_UNSUPPORTED: {normalized_scope}")
    return canonical_sleeve_id_from_engine_id(engine_id, repo_root=repo_root)


def canonical_sleeve_id_for_engine_v1(engine_id: str, *, repo_root: Path | None = None) -> str:
    return canonical_sleeve_id_from_engine_id(engine_id, repo_root=repo_root)
