#!/usr/bin/env python3
"""
run_capital_authority_allocation_day_v1.py

Bundle A (A1): Capital Authority Allocation Producer (day-scoped, deterministic, fail-closed).

Inputs:
- exposure_net_v1/<DAY>/exposure_net.v1.json
- reports/capital_risk_envelope_v2/<DAY>/capital_risk_envelope.v2.json (authoritative headroom)
- policy manifest: governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json
- intents_v1/snapshots/<DAY>/*.exposure_intent.v1.json

Outputs:
- allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json

Determinism:
- stable ordering by sleeve_id then intent_hash.
Fail-closed:
- missing required inputs => FAIL_* (no output rewrite).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1  # noqa: E402
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json"

POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
EXPOSURE_NET_PATH = lambda day: (TRUTH / "risk_v1/exposure_net_v1" / day / "exposure_net.v1.json").resolve()
ENVELOPE_V2_PATH = lambda day: (TRUTH / "reports/capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json").resolve()
INTENTS_DAY_DIR = lambda day: (TRUTH / "intents_v1/snapshots" / day).resolve()

OUT_ROOT = (TRUTH / "allocation_v1/capital_authority_allocation_v1").resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


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


def _parse_day(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _load_policy() -> Dict[str, Any]:
    if not POLICY_PATH.exists():
        raise SystemExit(f"FAIL: POLICY_MISSING: {str(POLICY_PATH)}")
    return _read_json_obj(POLICY_PATH)


def _headroom_from_envelope_v2(day: str) -> int:
    p = ENVELOPE_V2_PATH(day)
    if not p.exists():
        raise SystemExit(f"FAIL: ENVELOPE_V2_MISSING: {str(p)}")
    o = _read_json_obj(p)
    if str(o.get("schema_id") or "") != "capital_risk_envelope":
        raise SystemExit("FAIL: ENVELOPE_SCHEMA_ID_MISMATCH")
    if str(o.get("schema_version") or "") != "v2":
        raise SystemExit("FAIL: ENVELOPE_SCHEMA_VERSION_MISMATCH")
    env = o.get("envelope") or {}
    headroom = env.get("headroom_cents")
    if not isinstance(headroom, int):
        raise SystemExit("FAIL: ENVELOPE_HEADROOM_NOT_INT")
    return int(headroom)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_capital_authority_allocation_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    # Required inputs
    p_ex = EXPOSURE_NET_PATH(day)
    if not p_ex.exists():
        raise SystemExit(f"FAIL: EXPOSURE_NET_MISSING: {str(p_ex)}")
    ex_sha = _sha256_file(p_ex)

    policy = _load_policy()
    pol_sha = _sha256_file(POLICY_PATH)

    headroom_cents = _headroom_from_envelope_v2(day)
    env_sha = _sha256_file(ENVELOPE_V2_PATH(day))

    intents_dir = INTENTS_DAY_DIR(day)
    if not intents_dir.exists() or not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DIR_MISSING: {str(intents_dir)}")
    intents = sorted([p for p in intents_dir.iterdir() if p.is_file() and p.name.endswith(".json")], key=lambda p: p.name)

    # Build sleeve lookup from policy
    sleeves = policy.get("sleeves")
    if not isinstance(sleeves, list) or not sleeves:
        raise SystemExit("FAIL: POLICY_SLEEVES_INVALID_OR_EMPTY")

    engine_to_sleeve: Dict[str, str] = {}
    for s in sleeves:
        if not isinstance(s, dict):
            continue
        sleeve_id = str(s.get("sleeve_id") or "").strip()
        eids = s.get("engine_ids")
        if not sleeve_id or not isinstance(eids, list):
            continue
        for eid in eids:
            engine_to_sleeve[str(eid).strip()] = sleeve_id

    # Deterministic per-intent decisions: with headroom==0, reject all with reason code.
    per_intent: List[Dict[str, Any]] = []
    for p in intents:
        o = _read_json_obj(p)
        engine_id = str(((o.get("engine") or {}).get("engine_id") or "")).strip()
        intent_id = str(o.get("intent_id") or "").strip()
        if not engine_id or not intent_id:
            raise SystemExit(f"FAIL: INTENT_MISSING_ENGINE_OR_INTENT_ID: {str(p)}")
        sleeve_id = engine_to_sleeve.get(engine_id, "UNKNOWN_SLEEVE")
        intent_sha = _sha256_file(p)  # deterministic sha of file bytes
        decision = "REJECTED"
        authorized_qty = 0
        rc = ["CAPAUTH_REJECTED", "CAPAUTH_PORTFOLIO_LIMIT_BREACH"]
        if headroom_cents > 0:
            # still conservative: bootstrap v1 does not size; allow zero only unless policy is changed later.
            decision = "REJECTED"
            authorized_qty = 0
            rc = ["CAPAUTH_REJECTED", "CAPAUTH_FAIL_CLOSED_REQUIRED"]
        per_intent.append(
            {
                "intent_hash": intent_sha,
                "intent_id": intent_id,
                "engine_id": engine_id,
                "sleeve_id": sleeve_id,
                "decision": decision,
                "authorized_quantity": authorized_qty,
                "reason_codes": rc,
            }
        )

    per_intent.sort(key=lambda r: (r["sleeve_id"], r["intent_hash"]))

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_capital_authority_allocation_day_v1.py"},
        "status": "BLOCK" if headroom_cents <= 0 else "OK",
        "reason_codes": ["CAPAUTH_PORTFOLIO_LIMIT_BREACH"] if headroom_cents <= 0 else [],
        "input_manifest": [
            {"type": "exposure_net", "path": str(p_ex), "sha256": ex_sha, "day_utc": day, "producer": "risk_v1"},
            {"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha, "day_utc": None, "producer": "governance"},
            {"type": "other", "path": str(ENVELOPE_V2_PATH(day)), "sha256": env_sha, "day_utc": day, "producer": "capital_risk_envelope_v2"},
        ],
        "portfolio": {
            "allowed_capital_at_risk_cents": int(max(headroom_cents, 0)),
            "used_capital_at_risk_cents": 0,
            "headroom_cents": int(headroom_cents),
        },
        "per_sleeve": [
            {
                "sleeve_id": str(s.get("sleeve_id") or "").strip(),
                "engine_ids": list(s.get("engine_ids") or []),
                "allowed_capital_at_risk_cents": 0,
                "used_capital_at_risk_cents": 0,
                "headroom_cents": 0,
            }
            for s in sorted([x for x in sleeves if isinstance(x, dict)], key=lambda x: str(x.get("sleeve_id") or ""))
        ],
        "per_intent": per_intent,
    }

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    try:
        payload = canonical_json_bytes_v1(out_obj) + b"\n"
    except CanonicalizationError as e:
        raise SystemExit(f"FAIL: CANONICALIZATION_FAILED: {e}") from e

    out_path = (OUT_ROOT / day / "capital_authority_allocation.v1.json").resolve()
    _atomic_write_refuse_overwrite(out_path, payload)

    print(f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN day_utc={day} path={out_path} sha256={_sha256_bytes(payload)} status={out_obj['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
