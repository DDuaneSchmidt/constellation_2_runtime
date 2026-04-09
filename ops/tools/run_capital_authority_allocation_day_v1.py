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
      plus correlation cap multipliers from correlation_envelope_gate_v1.caps.multiplier_bp_by_sleeve
  - Deterministic sizing in v1:
      * if any required upstream gate is non-passing, reject with authorized_quantity=0
      * if portfolio or sleeve headroom is non-positive, reject with authorized_quantity=0
      * options_intent.v2 may authorize contracts from risk.max_contracts bounded by risk.max_risk_usd
      * equity_intent.v1 may authorize at most one share-sized unit, using max_risk_pct * nav_total_cents as a conservative per-unit risk proxy
      * unsupported or under-specified intents remain fail-closed REJECTED with authorized_quantity=0
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json"


def _resolve_truth_root(truth_root_arg: Optional[str]) -> Path:
    if truth_root_arg:
        return Path(truth_root_arg).resolve()
    return resolve_truth_root(REPO_ROOT)


def _authority_head_path(truth_root: Path) -> Path:
    return (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()


def _out_root(truth_root: Path) -> Path:
    return (truth_root / "allocation_v1" / "capital_authority_allocation_v1").resolve()


def _require_authority_head_pass_authoritative(day: str, truth_root: Path) -> Dict[str, Any]:
    p = _authority_head_path(truth_root)
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
    if status not in ("PASS", "BOOTSTRAP_PASS"):
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_NOT_EXECUTION_AUTHORIZED status={status!r}")
    if not authoritative:
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORITATIVE")
    points_to = str(ah.get("points_to") or "").strip()
    verdict_path = Path(points_to).resolve() if Path(points_to).is_absolute() else (truth_root / points_to).resolve()
    verdict = _read_json_obj(verdict_path)
    verdict_schema_id = str(verdict.get("schema_id") or "").strip()
    verdict_schema_version = str(verdict.get("schema_version") or "").strip()
    verdict_day = str(verdict.get("day_utc") or "").strip()
    verdict_status = str(verdict.get("status") or "").strip().upper()

    if verdict_day != day:
        raise SystemExit("FAIL: AUTHORITY_HEAD_POINTS_TO_DAY_MISMATCH")
    if verdict_status not in ("PASS", "BOOTSTRAP_PASS"):
        raise SystemExit("FAIL: AUTHORITY_HEAD_POINTS_TO_NOT_EXECUTION_AUTHORIZED")

    if "authorization_gate_verdict_v1" in points_to:
        if verdict_schema_id != "authorization_gate_verdict_v1" or verdict_schema_version not in {"1", "v1"}:
            raise SystemExit("FAIL: AUTHORIZATION_VERDICT_SCHEMA_MISMATCH")
    elif "gate_stack_verdict_v1" in points_to:
        if verdict_schema_id != "gate_stack_verdict" or verdict_schema_version != "v1":
            raise SystemExit("FAIL: GATE_STACK_VERDICT_SCHEMA_MISMATCH")
    else:
        raise SystemExit("FAIL: AUTHORITY_HEAD_POINTS_TO_UNSUPPORTED_VERDICT")
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


def _atomic_write_replace_if_changed(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate_sha = _sha256_bytes(data)
    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {str(path)}")
        existing = path.read_bytes()
        existing_sha = _sha256_bytes(existing)
        if existing == data:
            print(
                f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN path={path} "
                f"sha256={existing_sha} action=EXISTS_IDENTICAL"
            )
            return existing_sha
        tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
        tmp.write_bytes(data)
        fd = os.open(str(tmp), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(str(tmp), str(path))
        dfd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        print(
            f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN path={path} "
            f"sha256={candidate_sha} action=REPLACED_STALE existing_sha={existing_sha}"
        )
        return candidate_sha
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(data)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    print(
        f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN path={path} "
        f"sha256={candidate_sha} action=WROTE"
    )
    return candidate_sha


def _git_sha_failclosed() -> str:
    """
    Deterministic git sha resolution without shelling out:
      - Read .git/HEAD
      - If it is a ref, read that ref file
      - Return the hash string (must be 7..40 lowercase hex in schema)
    """
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        s = out.decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        s = "0" * 40
    if len(s) < 7:
        raise SystemExit(f"FAIL: GIT_SHA_INVALID_FAILCLOSED: {s!r}")
    return s


def EXPOSURE_NET_PATH(truth_root: Path, day: str) -> Path:
    return (truth_root / "risk_v1" / "exposure_net_v1" / day / "exposure_net.v1.json").resolve()


def ENVELOPE_V2_PATH(truth_root: Path, day: str) -> Path:
    return (truth_root / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json").resolve()


def INTENTS_DAY_DIR(truth_root: Path, day: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day).resolve()

def CORRELATION_ENVELOPE_GATE_PATH(truth_root: Path, day: str) -> Path:
    return (truth_root / "reports" / "correlation_envelope_gate_v1" / day / "correlation_envelope_gate.v1.json").resolve()

@dataclass(frozen=True)
class SleeveLimit:
    sleeve_id: str
    priority_rank: int
    engine_ids: List[str]
    max_capital_at_risk_cents: int


def _load_policy() -> Dict[str, Any]:
    return _read_json_obj(POLICY_PATH)


def _headroom_from_envelope_v2(truth_root: Path, day: str) -> int:
    p = ENVELOPE_V2_PATH(truth_root, day)
    env = _read_json_obj(p)
    envelope = env.get("envelope")
    if not isinstance(envelope, dict):
        raise SystemExit("FAIL: ENVELOPE_V2_MISSING_envelope_OBJECT")
    headroom = envelope.get("headroom_cents")
    if not isinstance(headroom, int):
        raise SystemExit("FAIL: ENVELOPE_V2_MISSING_headroom_cents_INT")
    return int(headroom)


def _status_is_passing(status: Any) -> bool:
    value = str(status or "").strip().upper()
    return value in {"PASS", "OK", "BOOTSTRAP_PASS", "AUTHORIZED"}


def _decimal_usd_to_cents(value: Any, *, field_name: str) -> int:
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise SystemExit(f"FAIL: INVALID_DECIMAL_{field_name}: {value!r}") from exc
    if dec <= 0:
        raise SystemExit(f"FAIL: NONPOSITIVE_DECIMAL_{field_name}: {value!r}")
    cents = (dec * Decimal("100")).to_integral_value(rounding=ROUND_CEILING)
    return int(cents)


def _extract_quantity_and_risk_per_unit_cents(intent_obj: Dict[str, Any], *, nav_total_cents: int) -> Optional[Tuple[int, int]]:
    schema_id = str(intent_obj.get("schema_id") or "").strip()
    schema_version = str(intent_obj.get("schema_version") or "").strip()
    if schema_id == "options_intent" and schema_version == "v2":
        risk = intent_obj.get("risk")
        if not isinstance(risk, dict):
            return None
        max_contracts = risk.get("max_contracts")
        if not isinstance(max_contracts, int) or max_contracts <= 0:
            return None
        max_risk_cents = _decimal_usd_to_cents(risk.get("max_risk_usd"), field_name="MAX_RISK_USD")
        risk_per_unit_cents = (max_risk_cents + max_contracts - 1) // max_contracts
        if risk_per_unit_cents <= 0:
            return None
        return int(max_contracts), int(risk_per_unit_cents)
    if schema_id == "equity_intent" and schema_version == "v1":
        sizing = intent_obj.get("sizing")
        if not isinstance(sizing, dict):
            return None
        target_notional_pct = Decimal(str(sizing.get("target_notional_pct") or "").strip())
        max_risk_pct = Decimal(str(sizing.get("max_risk_pct") or "").strip())
        if target_notional_pct <= 0 or max_risk_pct <= 0:
            return None
        if nav_total_cents <= 0:
            return None
        risk_per_unit_cents = int((Decimal(nav_total_cents) * max_risk_pct).to_integral_value(rounding=ROUND_CEILING))
        if risk_per_unit_cents <= 0:
            return None
        # EquityIntent v1 does not carry a deterministic share count. Authorize at most one unit when caps are positive.
        return 1, risk_per_unit_cents
    if schema_id == "exposure_intent" and schema_version == "v1":
        exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
        if exposure_type != "LONG_EQUITY":
            return None
        constraints = intent_obj.get("constraints")
        if not isinstance(constraints, dict):
            return None
        target_notional_pct = Decimal(str(intent_obj.get("target_notional_pct") or "").strip())
        max_risk_pct = Decimal(str(constraints.get("max_risk_pct") or "").strip())
        if target_notional_pct <= 0 or max_risk_pct <= 0:
            return None
        if nav_total_cents <= 0:
            return None
        risk_per_unit_cents = int((Decimal(nav_total_cents) * max_risk_pct).to_integral_value(rounding=ROUND_CEILING))
        if risk_per_unit_cents <= 0:
            return None
        # ExposureIntent v1 is pre-transform sizing; keep authorization conservative at one unit.
        return 1, risk_per_unit_cents
    return None


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


def _select_effective_intents(intents_dir: Path) -> List[Path]:
    # Same-day corrected snapshots may coexist with stale prior snapshots for the
    # same intent_id. Keep exactly one effective file per intent_id by selecting
    # the latest file mtime; break ties by lexicographically larger filename.
    by_intent_id: Dict[str, Tuple[int, str, Path]] = {}
    passthrough: List[Path] = []
    for p in sorted([p for p in intents_dir.iterdir() if p.is_file() and p.name.endswith(".json")], key=lambda p: p.name):
        try:
            obj = _read_json_obj(p)
        except Exception:
            passthrough.append(p)
            continue
        intent_id = str(obj.get("intent_id") or "").strip()
        if not intent_id:
            passthrough.append(p)
            continue
        stat = p.stat()
        candidate = (int(stat.st_mtime_ns), p.name, p)
        prior = by_intent_id.get(intent_id)
        if prior is None or candidate[:2] >= prior[:2]:
            by_intent_id[intent_id] = candidate
    return sorted(passthrough + [item[2] for item in by_intent_id.values()], key=lambda p: p.name)


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
    ap.add_argument("--truth_root", required=False, default=None)
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"
    truth_root = _resolve_truth_root(args.truth_root)

    intents_dir = INTENTS_DAY_DIR(truth_root, day)
    if not intents_dir.exists() or not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DIR_MISSING: {str(intents_dir)}")
    intents = _select_effective_intents(intents_dir)

    # Fail-closed authority check:
    # Required when there is intent activity; no-intent/no-submission clean roots can still emit
    # deterministic no-activity allocation evidence without an authority-day head.
    if intents:
        _require_authority_head_pass_authoritative(day, truth_root)

    # Required inputs
    p_ex = EXPOSURE_NET_PATH(truth_root, day)
    if not p_ex.exists():
        raise SystemExit(f"FAIL: EXPOSURE_NET_MISSING: {str(p_ex)}")
    ex_sha = _sha256_file(p_ex)

    envp = ENVELOPE_V2_PATH(truth_root, day)
    if not envp.exists():
        raise SystemExit(f"FAIL: ENVELOPE_V2_MISSING: {str(envp)}")
    env_sha = _sha256_file(envp)
    env_obj = _read_json_obj(envp)
    env_status_ok = _status_is_passing(env_obj.get("status"))

    cegp = CORRELATION_ENVELOPE_GATE_PATH(truth_root, day)
    if not cegp.exists():
        raise SystemExit(f"FAIL: CORRELATION_ENVELOPE_GATE_MISSING: {str(cegp)}")
    ceg_sha = _sha256_file(cegp)
    ceg_obj = _read_json_obj(cegp)
    caps = ceg_obj.get("caps")
    if not isinstance(caps, dict):
        raise SystemExit("FAIL: CORRELATION_ENVELOPE_GATE_MISSING_caps_OBJECT")
    mult = caps.get("multiplier_bp_by_sleeve")
    if not isinstance(mult, dict):
        raise SystemExit("FAIL: CORRELATION_ENVELOPE_GATE_MISSING_multiplier_bp_by_sleeve_OBJECT")
    corr_status_ok = _status_is_passing(ceg_obj.get("status"))

    policy = _load_policy()
    pol_sha = _sha256_file(POLICY_PATH)

    sleeves = _parse_sleeves(policy)
    engine_to_sleeve = _build_engine_to_sleeve(sleeves)

    portfolio_headroom_cents = _headroom_from_envelope_v2(truth_root, day)
    allowed_by_sleeve_raw = _allocate_sleeve_headroom(portfolio_headroom_cents, sleeves)

    # HARD BINDING: apply correlation envelope cap multipliers (basis points) per sleeve
    allowed_by_sleeve: Dict[str, int] = {}
    for sid, allow in sorted(allowed_by_sleeve_raw.items(), key=lambda kv: kv[0]):
        bp_any = mult.get(sid)
        if not isinstance(bp_any, int):
            raise SystemExit(f"FAIL: CORRELATION_GATE_MISSING_MULTIPLIER_FOR_SLEEVE: {sid}")
        bp = int(bp_any)
        if bp < 0 or bp > 10000:
            raise SystemExit(f"FAIL: CORRELATION_GATE_MULTIPLIER_OUT_OF_RANGE sleeve={sid} bp={bp}")
        allowed_by_sleeve[sid] = (int(allow) * bp) // 10000


    sleeve_priority = {s.sleeve_id: s.priority_rank for s in sleeves}
    initial_allowed_by_sleeve = {sid: int(v) for sid, v in allowed_by_sleeve.items()}
    remaining_by_sleeve = {sid: int(v) for sid, v in allowed_by_sleeve.items()}
    remaining_portfolio_headroom_cents = int(max(portfolio_headroom_cents, 0))

    per_intent_rows: List[Tuple[int, str, Dict[str, Any]]] = []
    for p in intents:
        o = _read_json_obj(p)
        engine_id = str(((o.get("engine") or {}).get("engine_id") or "")).strip()
        intent_id = str(o.get("intent_id") or "").strip()
        if not engine_id or not intent_id:
            raise SystemExit(f"FAIL: INTENT_MISSING_ENGINE_OR_INTENT_ID: {str(p)}")

        sleeve_id = engine_to_sleeve.get(engine_id, "UNKNOWN_SLEEVE")
        intent_sha = _sha256_file(p)
        decision = "REJECTED"
        authorized_qty = 0
        rc = ["CAPAUTH_REJECTED", "CAPAUTH_FAIL_CLOSED_REQUIRED"]

        risk_budget = _extract_quantity_and_risk_per_unit_cents(o, nav_total_cents=int(env_obj.get("envelope", {}).get("nav_total_cents") or 0))
        if (
            env_status_ok
            and corr_status_ok
            and remaining_portfolio_headroom_cents > 0
            and int(remaining_by_sleeve.get(sleeve_id, 0)) > 0
            and risk_budget is not None
        ):
            requested_qty, risk_per_unit_cents = risk_budget
            max_by_sleeve = int(remaining_by_sleeve.get(sleeve_id, 0)) // int(risk_per_unit_cents)
            max_by_portfolio = int(remaining_portfolio_headroom_cents) // int(risk_per_unit_cents)
            authorized_qty = int(min(requested_qty, max_by_sleeve, max_by_portfolio))
            if authorized_qty > 0:
                used_cents = int(authorized_qty * risk_per_unit_cents)
                remaining_by_sleeve[sleeve_id] = int(remaining_by_sleeve.get(sleeve_id, 0)) - used_cents
                remaining_portfolio_headroom_cents -= used_cents
                decision = "AUTHORIZED"
                rc = ["CAPAUTH_AUTHORIZED"]

        per_intent_rows.append(
            (
                int(sleeve_priority.get(sleeve_id, 999999)),
                intent_sha,
                {
                    "intent_hash": intent_sha,
                    "intent_id": intent_id,
                    "engine_id": engine_id,
                    "sleeve_id": sleeve_id,
                    "decision": decision,
                    "authorized_quantity": authorized_qty,
                    "reason_codes": rc,
                },
            )
        )

    per_intent_rows.sort(key=lambda row: (row[0], row[1]))
    per_intent = [row[2] for row in per_intent_rows]
    per_intent.sort(key=lambda r: (r["sleeve_id"], r["intent_hash"]))

    per_sleeve = []
    for s in sorted(sleeves, key=lambda x: x.sleeve_id):
        initial_allow = int(initial_allowed_by_sleeve.get(s.sleeve_id, 0))
        remaining_allow = int(remaining_by_sleeve.get(s.sleeve_id, 0))
        used_allow = int(initial_allow - remaining_allow)
        per_sleeve.append(
            {
                "sleeve_id": s.sleeve_id,
                "engine_ids": list(s.engine_ids),
                "allowed_capital_at_risk_cents": initial_allow,
                "used_capital_at_risk_cents": used_allow,
                "headroom_cents": remaining_allow,
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
        "input_manifest": (
            [
                {"type": "exposure_net", "path": str(p_ex), "sha256": ex_sha, "day_utc": day, "producer": "risk_v1"},
                {"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha, "day_utc": None, "producer": "governance"},
                {"type": "other", "path": str(envp), "sha256": env_sha, "day_utc": day, "producer": "capital_risk_envelope_v2"},
                {"type": "other", "path": str(cegp), "sha256": ceg_sha, "day_utc": day, "producer": "correlation_envelope_gate_v1"},
            ]
            + [
                {"type": "intents_snapshot", "path": str(p), "sha256": _sha256_file(p), "day_utc": day, "producer": "intents_v1"}
                for p in intents
            ]
        ),
        "portfolio": {
            "allowed_capital_at_risk_cents": int(max(portfolio_headroom_cents, 0)),
            "used_capital_at_risk_cents": int(max(portfolio_headroom_cents, 0)) - int(remaining_portfolio_headroom_cents),
            "headroom_cents": int(remaining_portfolio_headroom_cents),
        },
        "correlation_gate_binding": {
            "gate_artifact_path": str(cegp),
            "gate_artifact_sha256": ceg_sha,
            "policy_id": "C2_CORRELATION_ENVELOPE_POLICY_V1",
            "policy_sha256": _sha256_file(REPO_ROOT / "governance/02_REGISTRIES/C2_CORRELATION_ENVELOPE_POLICY_V1.json"),
            "binding_mode": "ALLOCATION_CAP_CONSTRAINT",
            "applied": True
        },
        "per_sleeve": per_sleeve,
        "per_intent": per_intent,
    }

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    payload = canonical_json_bytes_v1(out_obj) + b"\n"
    out_path = (_out_root(truth_root) / day / "capital_authority_allocation.v1.json").resolve()
    wrote_sha = _atomic_write_replace_if_changed(out_path, payload)

    print(f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_DAY day_utc={day} path={out_path} sha256={wrote_sha} status={out_obj['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
