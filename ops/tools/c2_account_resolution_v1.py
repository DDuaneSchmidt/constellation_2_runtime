#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def _load_sleeve_registry(repo_root: Path) -> Dict[str, Any]:
    path = (repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL: sleeve_registry_missing path={path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: sleeve_registry_parse_error path={path} err={type(e).__name__}:{e}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: sleeve_registry_not_object path={path}")
    if str(obj.get("schema_id") or "") != "c2_sleeve_registry" or str(obj.get("schema_version") or "") != "v1":
        raise SystemExit(
            "FAIL: sleeve_registry_schema_mismatch "
            f"path={path} schema_id={obj.get('schema_id')!r} schema_version={obj.get('schema_version')!r}"
        )
    return obj


def resolve_single_paper_ib_account_from_sleeve_registry(repo_root: Path) -> str:
    reg = _load_sleeve_registry(repo_root)
    sleeves = reg.get("sleeves")
    if not isinstance(sleeves, list):
        raise SystemExit("FAIL: sleeve_registry_invalid_sleeves")

    accounts: List[str] = []
    for sleeve in sleeves:
        if not isinstance(sleeve, dict):
            continue
        if not bool(sleeve.get("enabled")):
            continue
        if str(sleeve.get("mode") or "").strip().upper() != "PAPER":
            continue
        account = str(sleeve.get("ib_account") or "").strip()
        if not account:
            raise SystemExit(
                "FAIL: enabled_paper_sleeve_missing_ib_account "
                f"sleeve_id={sleeve.get('sleeve_id')!r}"
            )
        accounts.append(account)

    uniq = sorted(set(accounts))
    if not uniq:
        raise SystemExit("FAIL: no_enabled_paper_sleeve_accounts")
    if len(uniq) != 1:
        raise SystemExit(f"FAIL: multiple_enabled_paper_sleeve_accounts accounts={uniq}")
    return uniq[0]
