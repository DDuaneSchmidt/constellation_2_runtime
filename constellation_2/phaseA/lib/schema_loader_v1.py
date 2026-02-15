"""
schema_loader_v1.py

Constellation 2.0 Phase A
Offline schema loader (Design Pack authority: constellation_2/schemas/).

Hard constraints:
- Fail-closed if schema is missing or unreadable
- No network access
- Deterministic path resolution from repo root

This module intentionally does NOT cache across processes. Any in-process caching
must be deterministic and content-addressed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


class SchemaLoaderError(Exception):
    """Raised when schema lookup/load fails (fail-closed)."""


@dataclass(frozen=True)
class SchemaRef:
    schema_name: str
    filename: str


# Stable schema name mapping (Design Pack schema registry is authoritative).
# Names are the user-facing CLI identifiers.
SCHEMA_NAME_TO_FILE: Dict[str, str] = {
    "options_intent.v2": "options_intent.v2.schema.json",
    "options_chain_snapshot.v1": "options_chain_snapshot.v1.schema.json",
    "freshness_certificate.v1": "freshness_certificate.v1.schema.json",
    "order_plan.v1": "order_plan.v1.schema.json",
    "mapping_ledger_record.v1": "mapping_ledger_record.v1.schema.json",
    "binding_record.v1": "binding_record.v1.schema.json",
    "veto_record.v1": "veto_record.v1.schema.json",
    "broker_submission_record.v2": "broker_submission_record.v2.schema.json",
    "position_lifecycle.v1": "position_lifecycle.v1.schema.json",

    # Bundle H (Hybrid Equity primitives)
    "equity_intent.v1": "equity_intent.v1.schema.json",
    "equity_order_plan.v1": "equity_order_plan.v1.schema.json",
}


def repo_root_from_here() -> Path:
    """
    Derive repo root by walking up from this file:
    constellation_2/phaseA/lib/schema_loader_v1.py -> repo root is 4 parents up.
    Fail-closed if structure is unexpected.
    """
    here = Path(__file__).resolve()
    # .../constellation_2/phaseA/lib/schema_loader_v1.py
    # parents:
    # 0 = lib
    # 1 = phaseA
    # 2 = constellation_2
    # 3 = repo root
    try:
        root = here.parents[3]
    except Exception as e:  # noqa: BLE001
        raise SchemaLoaderError(f"Unable to derive repo root from {here}: {e}") from e
    if not (root / ".git").exists():
        # We require being inside a git repo for authority.
        raise SchemaLoaderError(f"Derived repo root does not look like a git repo: {root}")
    return root


def schemas_dir(repo_root: Optional[Path] = None) -> Path:
    root = repo_root or repo_root_from_here()
    d = root / "constellation_2" / "schemas"
    if not d.exists() or not d.is_dir():
        raise SchemaLoaderError(f"Schemas directory missing: {d}")
    return d


def schema_path(schema_name: str, repo_root: Optional[Path] = None) -> Path:
    if schema_name not in SCHEMA_NAME_TO_FILE:
        raise SchemaLoaderError(f"Unknown schema_name '{schema_name}'. Known: {sorted(SCHEMA_NAME_TO_FILE.keys())}")
    p = schemas_dir(repo_root) / SCHEMA_NAME_TO_FILE[schema_name]
    if not p.exists() or not p.is_file():
        raise SchemaLoaderError(f"Schema file missing: {p}")
    return p


def load_schema(schema_name: str, repo_root: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load schema JSON as dict. Fail-closed on any IO/parse error.
    """
    p = schema_path(schema_name, repo_root)
    try:
        raw = p.read_text(encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        raise SchemaLoaderError(f"Failed reading schema file {p}: {e}") from e
    try:
        obj = json.loads(raw)
    except Exception as e:  # noqa: BLE001
        raise SchemaLoaderError(f"Failed parsing schema JSON {p}: {e}") from e
    if not isinstance(obj, dict):
        raise SchemaLoaderError(f"Schema root is not an object: {p}")
    return obj
