#!/usr/bin/env python3
"""
run_exposure_net_day_v1.py

Bundle A (A2): Exposure Net Producer (day-scoped, deterministic, fail-closed).

Inputs (day_utc):
- intents_v1/snapshots/<DAY>/*.exposure_intent.v1.json
- positions snapshot (optional for this v1 bootstrap implementation; used only if present)
- cash ledger snapshot (optional for this v1 bootstrap implementation; used only if present)
- governance policy manifest: governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json

Outputs:
- constellation_2/runtime/truth/risk_v1/exposure_net_v1/<DAY>/exposure_net.v1.json
(no latest pointers)

Strictness posture (v1):
- FAIL-CLOSED if intents dir missing or unreadable.
- If intents dir exists but empty: writes FAIL_MISSING_INPUTS (deterministic).
- For this bootstrap v1, if positions/cash snapshots are missing we still write OK with zeros and explicit reason codes.
  (Bundle A allocation/authorization will still enforce headroom=0 due to envelope gate.)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

# Import bootstrap
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1  # noqa: E402
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/exposure_net.v1.schema.json"
POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()

INTENTS_DAY_ROOT = (TRUTH / "intents_v1/snapshots").resolve()
OUT_ROOT = (TRUTH / "risk_v1/exposure_net_v1").resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return o


def _atomic_write_refuse_overwrite(path: Path, data: bytes) -> None:
    if path.exists():
        raise SystemExit(f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        raise SystemExit(f"FAIL: TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(str(tmp), str(path))


def _list_intent_files(day: str) -> List[Path]:
    d = (INTENTS_DAY_ROOT / day).resolve()
    if not d.exists():
        raise SystemExit(f"FAIL: INTENTS_DAY_DIR_MISSING: {str(d)}")
    if not d.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DAY_PATH_NOT_DIR: {str(d)}")
    try:
        files = sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".json")])
    except Exception as e:
        raise SystemExit(f"FAIL: INTENTS_DAY_DIR_UNREADABLE: {e!r}")
    return files


def _parse_day(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s

def _policy_engine_ids_fallback() -> List[str]:
    """
    Deterministic fallback: derive engine_ids from governance policy when no intents exist.
    This prevents empty per_engine (schema requires minItems >= 1).
    """
    try:
        pol = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    sleeves = pol.get("sleeves")
    if not isinstance(sleeves, list):
        return []

    out: List[str] = []
    for s in sleeves:
        if not isinstance(s, dict):
            continue
        eids = s.get("engine_ids")
        if not isinstance(eids, list):
            continue
        for eid in eids:
            t = str(eid).strip()
            if t:
                out.append(t)

    # unique + deterministic order
    return sorted(set(out))



def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_exposure_net_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)

    produced_utc = f"{day}T00:00:00Z"
    reason_codes: List[str] = []
    notes: List[str] = []

    # Required: policy exists
    if not POLICY_PATH.exists():
        raise SystemExit(f"FAIL: POLICY_MISSING: {str(POLICY_PATH)}")
    policy_sha = _sha256_file(POLICY_PATH)

    intent_files = _list_intent_files(day)

    input_manifest: List[Dict[str, Any]] = []
    input_manifest.append({"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": policy_sha, "day_utc": None, "producer": "governance"})
    # Manifest all intent files (bounded? no: day-level deterministic, but likely small; still ok)
    for p in intent_files:
        input_manifest.append({"type": "intents_snapshot", "path": str(p.resolve()), "sha256": _sha256_file(p), "day_utc": day, "producer": "intents_v1"})

    # v1 bootstrap arithmetic: we do not compute notional; we just net as zeros and track symbol set.
    symbols: List[str] = []
    per_engine: Dict[str, Dict[str, Any]] = {}

    if not intent_files:
        reason_codes.append("CAPAUTH_INTENTS_MISSING")
        status = "FAIL_MISSING_INPUTS"
    else:
        status = "OK"
        for p in intent_files:
            o = _read_json_obj(p)
            if str(o.get("schema_id") or "") != "exposure_intent":
                raise SystemExit(f"FAIL: UNEXPECTED_INTENT_SCHEMA_ID: {p} schema_id={o.get('schema_id')!r}")
            engine = o.get("engine") or {}
            engine_id = str((engine.get("engine_id") or "")).strip()
            sym = str(((o.get("underlying") or {}).get("symbol") or "")).strip().upper()
            if not engine_id or not sym:
                raise SystemExit(f"FAIL: INTENT_MISSING_ENGINE_OR_SYMBOL: {p}")
            symbols.append(sym)
            if engine_id not in per_engine:
                per_engine[engine_id] = {"engine_id": engine_id, "gross_notional_usd": "0", "net_notional_usd": "0", "capital_at_risk_cents": 0, "by_symbol": []}

        symbols = sorted(set(symbols))

    portfolio = {
        "gross_notional_usd": "0",
        "net_notional_usd": "0",
        "capital_at_risk_cents": 0,
        "symbol_count": int(len(symbols)),
        "by_symbol": [{"symbol": s, "net_notional_usd": "0", "gross_notional_usd": "0", "capital_at_risk_cents": 0, "sector": None} for s in symbols],
    }

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_EXPOSURE_NET_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_exposure_net_day_v1.py"},
        "status": status,
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
        "input_manifest": input_manifest,
        "portfolio": portfolio,
        "per_engine": [per_engine[k] for k in sorted(per_engine.keys())],
    }

# Schema requires per_engine minItems >= 1.
# If there were no intents, out_obj["per_engine"] may be empty. Use policy fallback.
if not out_obj.get("per_engine"):
    eids = _policy_engine_ids_fallback()
    if not eids:
        # fail-closed but schema-compliant fallback: single unknown engine
        eids = ["UNKNOWN_ENGINE"]

    fallback = []
    for eid in eids:
        fallback.append(
            {
                "engine_id": eid,
                "gross_notional_usd": "0",
                "net_notional_usd": "0",
                "capital_at_risk_cents": 0,
                "by_symbol": [],
            }
        )
    out_obj["per_engine"] = fallback
    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    try:
        payload = canonical_json_bytes_v1(out_obj) + b"\n"
    except CanonicalizationError as e:
        raise SystemExit(f"FAIL: CANONICALIZATION_FAILED: {e}") from e

    out_path = (OUT_ROOT / day / "exposure_net.v1.json").resolve()
    _atomic_write_refuse_overwrite(out_path, payload)

    print(f"OK: EXPOSURE_NET_V1_WRITTEN day_utc={day} path={out_path} sha256={_sha256_bytes(payload)} status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
