from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
REG_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json").resolve()


@dataclass(frozen=True)
class FamilyAuthority:
    family: str
    authoritative_root: str
    non_authoritative_roots: Tuple[str, ...]


def load_registry() -> Dict[str, FamilyAuthority]:
    if not REG_PATH.exists():
        raise RuntimeError(f"FAIL: missing truth authority registry: {REG_PATH}")
    obj = json.loads(REG_PATH.read_text(encoding="utf-8"))
    if str(obj.get("schema_id") or "") != "c2_truth_authority_registry":
        raise RuntimeError(f"FAIL: invalid registry schema_id: {REG_PATH}")
    if str(obj.get("schema_version") or "") != "v1":
        raise RuntimeError(f"FAIL: invalid registry schema_version: {REG_PATH}")

    out: Dict[str, FamilyAuthority] = {}
    fams = obj.get("families")
    if not isinstance(fams, list):
        raise RuntimeError(f"FAIL: families must be list: {REG_PATH}")
    for f in fams:
        if not isinstance(f, dict):
            continue
        fam = str(f.get("family") or "").strip()
        auth = str(f.get("authoritative_root") or "").strip()
        non = f.get("non_authoritative_roots")
        if (not fam) or (not auth) or (not isinstance(non, list)):
            raise RuntimeError(f"FAIL: invalid family entry: {f}")
        out[fam] = FamilyAuthority(family=fam, authoritative_root=auth, non_authoritative_roots=tuple(str(x) for x in non))
    return out


def require_authoritative_root(family: str, requested_root: str) -> None:
    reg = load_registry()
    fam = str(family).strip()
    req = str(requested_root).strip()
    if fam not in reg:
        raise RuntimeError(f"FAIL: family not registered: {fam}")
    fa = reg[fam]
    if req != fa.authoritative_root:
        raise RuntimeError(
            f"FAIL: non-authoritative root access blocked family={fam} requested={req} authoritative={fa.authoritative_root}"
        )
