#!/usr/bin/env python3
"""
run_capital_authority_allocation_day_v1.py

Bundle A (A1): Capital Authority Allocation Producer (day-scoped, deterministic, fail-closed).

Writes:
  constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json

Inputs (required):
  - risk_v1/exposure_net_v1/<DAY>/exposure_net.v1.json
  - governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json
  - reports/capital_risk_envelope_v2/<DAY>/capital_risk_envelope.v2.json
  - intents_v1/snapshots/<DAY>/*.json

Policy (v1 behavior):
  - Deterministic sleeve headroom allocation using:
      portfolio_headroom_cents from capital_risk_envelope_v2.envelope.headroom_cents
      plus per-sleeve caps from policy.sleeves[].limits.max_capital_at_risk_cents
  - Conservative sizing in v1: authorized_quantity remains 0 (fail-closed required)
    even when headroom exists, until sizing logic is introduced under governance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json"

OUT_ROOT = (TRUTH_ROOT / "allocation_v1" / "capital_authority_allocation_v1").resolve()

AUTHORITY_HEAD_PATH = (TRUTH_ROOT / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()


def _require_authority_head_pass_authoritative(day: str) -> Dict[str, Any]:
    p = AUTHORITY_HEAD_PATH
    ah = _read_json_obj(p)
    schema_id = str(ah.get("schema_id") or "").strip()
    schema_ver = str(ah.get("schema_version") or "").strip()
    status = str(ah.get("status") or "").strip().upper()
    authoritative = bool(ah.get("authoritative") is True)
    day_utc = str(ah.get("day_utc") or "").strip()

    if schema_id != "c2_run_pointer_canonical_authority_head" or schema_ver != "v1":
        raise SystemExit("FAIL: AUTHORITY_HEAD_SCHEMA_MISMATCH")
    if day_utc != day:
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_DAY_MISMATCH head_day={day_utc!r} expected_day={day!r}")
    if status != "PASS":
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_NOT_PASS status={status!r}")
    if not authoritative:
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORITATIVE")
    return ah



def _parse_day(day_utc: str) -> str:
    d = str(day_utc).strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {d!r}")
    return d


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL: missing_or_not_file: {str(path)}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: json_parse_failed: {str(path)}: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _atomic_write_refuse_overwrite(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise SystemExit(f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    path.write_bytes(data)


def _git_sha_failclosed() -> str:
    """
    Deterministic git sha resolution without shelling out:
      - Read .git/HEAD
      - If it is a ref, read that ref file
      - Return the hash string (must be 7..40 lowercase hex in schema)
    """
    head = (REPO_ROOT / ".git" / "HEAD").resolve()
    if not head.exists():
        raise SystemExit("FAIL: GIT_HEAD_MISSING_FAILCLOSED")

    s = head.read_text(encoding="utf-8").strip()
    if s.startswith("ref:"):
        ref = s.split(" ", 1)[1].strip()
        refp = (REPO_ROOT / ".git" / ref).resolve()
        if not refp.exists():
            raise SystemExit(f"FAIL: GIT_REF_MISSING_FAILCLOSED: {ref}")
        return refp.read_text(encoding="utf-8").strip()

    return s


def EXPOSURE_NET_PATH(day: str) -> Path:
    return (TRUTH_ROOT / "risk_v1" / "exposure_net_v1" / day / "exposure_net.v1.json").resolve()


def ENVELOPE_V2_PATH(day: str) -> Path:
    return (TRUTH_ROOT / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json").resolve()


def INTENTS_DAY_DIR(day: str) -> Path:
    return (TRUTH_ROOT / "intents_v1" / "snapshots" / day).resolve()


@dataclass(frozen=True)
class SleeveLimit:
    sleeve_id: str
    priority_rank: int
    engine_ids: List[str]
    max_capital_at_risk_cents: int


def _load_policy() -> Dict[str, Any]:
    return _read_json_obj(POLICY_PATH)


def _headroom_from_envelope_v2(day: str) -> int:
    p = ENVELOPE_V2_PATH(day)
    env = _read_json_obj(p)
    envelope = env.get("envelope")
    if not isinstance(envelope, dict):
        raise SystemExit("FAIL: ENVELOPE_V2_MISSING_envelope_OBJECT")
    headroom = envelope.get("headroom_cents")
    if not isinstance(headroom, int):
        raise SystemExit("FAIL: ENVELOPE_V2_MISSING_headroom_cents_INT")
    return int(headroom)


def _parse_sleeves(policy: Dict[str, Any]) -> List[SleeveLimit]:
    sleeves_raw = policy.get("sleeves")
    if not isinstance(sleeves_raw, list) or not sleeves_raw:
        raise SystemExit("FAIL: POLICY_SLEEVES_INVALID_OR_EMPTY")

    out: List[SleeveLimit] = []
    for s in sleeves_raw:
        if not isinstance(s, dict):
            continue
        sleeve_id = str(s.get("sleeve_id") or "").strip()
        if not sleeve_id:
            raise SystemExit("FAIL: POLICY_SLEEVE_ID_MISSING")
        pr = s.get("priority_rank")
        if not isinstance(pr, int):
            raise SystemExit(f"FAIL: POLICY_PRIORITY_RANK_NOT_INT sleeve_id={sleeve_id}")

        eids = s.get("engine_ids")
        if not isinstance(eids, list) or not eids:
            raise SystemExit(f"FAIL: POLICY_ENGINE_IDS_INVALID_OR_EMPTY sleeve_id={sleeve_id}")
        engine_ids = [str(x).strip() for x in eids if str(x).strip()]
        if not engine_ids:
            raise SystemExit(f"FAIL: POLICY_ENGINE_IDS_EMPTY_AFTER_STRIP sleeve_id={sleeve_id}")

        limits = s.get("limits")
        if not isinstance(limits, dict):
            raise SystemExit(f"FAIL: POLICY_LIMITS_MISSING_OR_INVALID sleeve_id={sleeve_id}")
        mcar = limits.get("max_capital_at_risk_cents")
        if not isinstance(mcar, int) or mcar < 0:
            raise SystemExit(f"FAIL: POLICY_MAX_CAPITAL_AT_RISK_CENTS_INVALID sleeve_id={sleeve_id}")

        out.append(
            SleeveLimit(
                sleeve_id=sleeve_id,
                priority_rank=int(pr),
                engine_ids=engine_ids,
                max_capital_at_risk_cents=int(mcar),
            )
        )

    if not out:
        raise SystemExit("FAIL: POLICY_SLEEVES_EMPTY_AFTER_PARSE")

    out.sort(key=lambda x: (x.priority_rank, x.sleeve_id))
    return out


def _build_engine_to_sleeve(sleeves: List[SleeveLimit]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for s in sleeves:
        for eid in s.engine_ids:
            m[eid] = s.sleeve_id
    return m


def _allocate_sleeve_headroom(portfolio_headroom_cents: int, sleeves: List[SleeveLimit]) -> Dict[str, int]:
    remaining = int(max(portfolio_headroom_cents, 0))
    allowed_by_sleeve: Dict[str, int] = {}
    for s in sleeves:
        cap = int(max(s.max_capital_at_risk_cents, 0))
        allow = min(cap, remaining)
        allowed_by_sleeve[s.sleeve_id] = int(allow)
        remaining -= int(allow)
    return allowed_by_sleeve


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_capital_authority_allocation_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    # Fail-closed: allocation can only be produced on authority PASS+authoritative days.
    _require_authority_head_pass_authoritative(day)

    # Required inputs
    p_ex = EXPOSURE_NET_PATH(day)
    if not p_ex.exists():
        raise SystemExit(f"FAIL: EXPOSURE_NET_MISSING: {str(p_ex)}")
    ex_sha = _sha256_file(p_ex)

    envp = ENVELOPE_V2_PATH(day)
    if not envp.exists():
        raise SystemExit(f"FAIL: ENVELOPE_V2_MISSING: {str(envp)}")
    env_sha = _sha256_file(envp)

    intents_dir = INTENTS_DAY_DIR(day)
    if not intents_dir.exists() or not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DIR_MISSING: {str(intents_dir)}")
    intents = sorted([p for p in intents_dir.iterdir() if p.is_file() and p.name.endswith(".json")], key=lambda p: p.name)

    policy = _load_policy()
    pol_sha = _sha256_file(POLICY_PATH)

    sleeves = _parse_sleeves(policy)
    engine_to_sleeve = _build_engine_to_sleeve(sleeves)

    portfolio_headroom_cents = _headroom_from_envelope_v2(day)
    allowed_by_sleeve = _allocate_sleeve_headroom(portfolio_headroom_cents, sleeves)

    # Deterministic per-intent decisions (v1 sizing intentionally fail-closed)
    per_intent: List[Dict[str, Any]] = []
    for p in intents:
        o = _read_json_obj(p)
        engine_id = str(((o.get("engine") or {}).get("engine_id") or "")).strip()
        intent_id = str(o.get("intent_id") or "").strip()
        if not engine_id or not intent_id:
            raise SystemExit(f"FAIL: INTENT_MISSING_ENGINE_OR_INTENT_ID: {str(p)}")

        sleeve_id = engine_to_sleeve.get(engine_id, "UNKNOWN_SLEEVE")
        intent_sha = _sha256_file(p)

        # v1: Even when headroom exists, we do not size yet -> explicit fail-closed.
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

    per_sleeve = []
    for s in sorted(sleeves, key=lambda x: x.sleeve_id):
        allow = int(allowed_by_sleeve.get(s.sleeve_id, 0))
        per_sleeve.append(
            {
                "sleeve_id": s.sleeve_id,
                "engine_ids": list(s.engine_ids),
                "allowed_capital_at_risk_cents": allow,
                "used_capital_at_risk_cents": 0,
                "headroom_cents": allow,
            }
        )

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": _git_sha_failclosed(),
            "module": "ops/tools/run_capital_authority_allocation_day_v1.py",
        },
        "status": "OK",
        "reason_codes": [],
        "input_manifest": [
            {"type": "exposure_net", "path": str(p_ex), "sha256": ex_sha, "day_utc": day, "producer": "risk_v1"},
            {"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha, "day_utc": None, "producer": "governance"},
            {"type": "other", "path": str(envp), "sha256": env_sha, "day_utc": day, "producer": "capital_risk_envelope_v2"},
        ],
        "portfolio": {
            "allowed_capital_at_risk_cents": int(max(portfolio_headroom_cents, 0)),
            "used_capital_at_risk_cents": 0,
            "headroom_cents": int(portfolio_headroom_cents),
        },
        "per_sleeve": per_sleeve,
        "per_intent": per_intent,
    }

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    payload = canonical_json_bytes_v1(out_obj) + b"\n"
    out_path = (OUT_ROOT / day / "capital_authority_allocation.v1.json").resolve()
    _atomic_write_refuse_overwrite(out_path, payload)

    print(
        f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN day_utc={day} path={out_path} "
        f"sha256={_sha256_bytes(payload)} status={out_obj['status']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
