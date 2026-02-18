#!/usr/bin/env python3
"""
run_c2_capital_risk_envelope_gate_v2.py

Forward-only Capital Risk Envelope Gate v2.

- Writes to reports/capital_risk_envelope_v2/<OUT_DAY>/capital_risk_envelope.v2.json
- Deterministic, fail-closed, audit-grade.
- SAFE_IDLE: empty positions list can PASS if required inputs exist and drawdown present.
- Never overwrites immutable day-keyed truth artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_OUT = (REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json").resolve()
SCHEMA_ALLOC_SUMMARY = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_summary.v1.schema.json"
SCHEMA_NAV = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v1.schema.json"
SCHEMA_POS_V3 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json"
SCHEMA_POS_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json"

DRAWDOWN_CONTRACT = (REPO_ROOT / "governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md").resolve()
CAP_RISK_CONTRACT_V2 = (REPO_ROOT / "governance/05_CONTRACTS/C2/capital_risk_envelope_v2.contract.md").resolve()

BASE_ENVELOPE_PCT = Decimal("0.020000")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _read_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return obj


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    # Deterministic JSON: sorted keys, compact separators, newline terminated.
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _validate_against_repo_schema(obj: Dict[str, Any], schema_relpath: str) -> None:
    # Minimal local validator to avoid adding deps; reuse existing repo validator module if present.
    # Use constellation_2.phaseD.lib.validate_against_schema_v1 which is already in repo.
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore

    validate_against_repo_schema_v1(obj, REPO_ROOT, schema_relpath)


def _write_immutable(path: Path, data: bytes) -> str:
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    cand_sha = _sha256_bytes(data)

    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {str(path)}")
        existing = path.read_bytes()
        ex_sha = _sha256_bytes(existing)
        if ex_sha == cand_sha:
            return cand_sha
        raise SystemExit(f"FAIL: ATTEMPTED_REWRITE_IMMUTABLE: {str(path)} existing_sha={ex_sha} candidate_sha={cand_sha}")

    path.write_bytes(data)
    return cand_sha


def _git_sha() -> str:
    head = (REPO_ROOT / ".git" / "HEAD").resolve()
    if not head.exists():
        raise RuntimeError("GIT_HEAD_MISSING_FAILCLOSED")
    s = head.read_text(encoding="utf-8").strip()
    if s.startswith("ref:"):
        ref = s.split(" ", 1)[1].strip()
        refp = (REPO_ROOT / ".git" / ref).resolve()
        if not refp.exists():
            raise RuntimeError(f"GIT_REF_MISSING_FAILCLOSED: {ref}")
        return refp.read_text(encoding="utf-8").strip()
    return s


def _quant6(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.000001"))


def _quant2(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.01"))


def _multiplier_from_drawdown(drawdown_pct: Decimal) -> Decimal:
    # Canonical table (matches drawdown contract semantics)
    if drawdown_pct <= Decimal("-0.150000"):
        return Decimal("0.25")
    if drawdown_pct <= Decimal("-0.100000"):
        return Decimal("0.50")
    if drawdown_pct <= Decimal("-0.050000"):
        return Decimal("0.75")
    return Decimal("1.00")


def _table() -> List[Dict[str, str]]:
    return [
        {"threshold_drawdown_pct": "0.000000", "multiplier": "1.00"},
        {"threshold_drawdown_pct": "-0.050000", "multiplier": "0.75"},
        {"threshold_drawdown_pct": "-0.100000", "multiplier": "0.50"},
        {"threshold_drawdown_pct": "-0.150000", "multiplier": "0.25"},
    ]


@dataclass(frozen=True)
class Inputs:
    alloc_path: Path
    nav_path: Path
    pos_path: Path
    pos_schema: str
    truth_root: Path


def _resolve_inputs(day: str, truth_root: Path) -> Inputs:
    alloc = (truth_root / "allocation_v1/summary" / day / "summary.json").resolve()
    nav = (truth_root / "accounting_v1/nav" / day / "nav.json").resolve()
    pos_v3 = (truth_root / "positions_v1/snapshots" / day / "positions_snapshot.v3.json").resolve()
    pos_v2 = (truth_root / "positions_v1/snapshots" / day / "positions_snapshot.v2.json").resolve()

    if not alloc.exists():
        raise FileNotFoundError(f"ALLOC_SUMMARY_MISSING: {str(alloc)}")
    if not nav.exists():
        raise FileNotFoundError(f"NAV_MISSING: {str(nav)}")

    if pos_v3.exists():
        return Inputs(alloc_path=alloc, nav_path=nav, pos_path=pos_v3, pos_schema=SCHEMA_POS_V3, truth_root=truth_root)
    if pos_v2.exists():
        return Inputs(alloc_path=alloc, nav_path=nav, pos_path=pos_v2, pos_schema=SCHEMA_POS_V2, truth_root=truth_root)

    raise FileNotFoundError(f"POSITIONS_SNAPSHOT_MISSING: {str(pos_v3)} and {str(pos_v2)}")


def _compute(out_day: str, produced_utc: str, inp: Inputs) -> Dict[str, Any]:
    reason_codes: List[str] = []
    notes: List[str] = []

    checks: Dict[str, Any] = {
        "allocation_summary_present": True,
        "nav_present": True,
        "positions_present": True,
        "drawdown_present": False,
        "positions_all_have_max_loss": False,
        "portfolio_within_envelope": False,
    }

    alloc_obj = _read_json(inp.alloc_path)
    nav_obj = _read_json(inp.nav_path)
    pos_obj = _read_json(inp.pos_path)

    try:
        _validate_against_repo_schema(alloc_obj, SCHEMA_ALLOC_SUMMARY)
    except Exception as e:  # noqa: BLE001
        reason_codes.append("B2_ALLOC_SUMMARY_SCHEMA_INVALID")
        notes.append(f"allocation_summary schema invalid: {e}")
        checks["allocation_summary_present"] = False

    try:
        _validate_against_repo_schema(nav_obj, SCHEMA_NAV)
    except Exception as e:  # noqa: BLE001
        reason_codes.append("B2_ACCOUNTING_NAV_SCHEMA_INVALID")
        notes.append(f"accounting_nav schema invalid: {e}")
        checks["nav_present"] = False

    try:
        _validate_against_repo_schema(pos_obj, inp.pos_schema)
    except Exception as e:  # noqa: BLE001
        reason_codes.append("B2_POSITIONS_SNAPSHOT_SCHEMA_INVALID")
        notes.append(f"positions_snapshot schema invalid: {e}")
        checks["positions_present"] = False

    input_manifest = [
        {"type": "allocation_summary", "path": str(inp.alloc_path), "sha256": _sha256_file(inp.alloc_path)},
        {"type": "accounting_nav", "path": str(inp.nav_path), "sha256": _sha256_file(inp.nav_path)},
        {"type": "positions_snapshot", "path": str(inp.pos_path), "sha256": _sha256_file(inp.pos_path)},
        {"type": "drawdown_contract", "path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
        {"type": "capital_risk_envelope_contract_v2", "path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
        {"type": "output_schema", "path": str(SCHEMA_OUT), "sha256": _sha256_file(SCHEMA_OUT)},
    ]

    nav_total = nav_obj.get("nav", {}).get("nav_total")
    hist = nav_obj.get("history", {}) if isinstance(nav_obj.get("history"), dict) else {}
    peak_nav = hist.get("peak_nav")
    drawdown_abs = hist.get("drawdown_abs")
    drawdown_pct_raw = hist.get("drawdown_pct")

    if not isinstance(nav_total, int):
        reason_codes.append("B2_NAV_TOTAL_MISSING_OR_INVALID")
        checks["nav_present"] = False
        nav_total = 0

    nav_total_cents = int(nav_total) * 100

    multiplier: Optional[Decimal] = None
    drawdown_pct_q: Optional[Decimal] = None

    if isinstance(drawdown_pct_raw, str) and drawdown_pct_raw.strip() != "":
        drawdown_pct_q = _quant6(Decimal(drawdown_pct_raw))
        multiplier = _quant2(_multiplier_from_drawdown(drawdown_pct_q))
        checks["drawdown_present"] = True
    else:
        reason_codes.append("B2_DRAWDOWN_MISSING_FAILCLOSED")
        notes.append("drawdown_pct missing/null at enforcement time -> FAIL-CLOSED per drawdown_convention_v1.contract.md")

    items = pos_obj.get("positions", {}).get("items")
    if not isinstance(items, list):
        reason_codes.append("B2_POSITIONS_ITEMS_INVALID_OR_MISSING")
        checks["positions_present"] = False
        items = []

    # SAFE_IDLE: empty list is valid and implies risk_sum=0.
    breakdown: List[Dict[str, Any]] = []
    risk_sum: Optional[int] = 0
    all_have_max = True

    def _pid(it: Any) -> str:
        if not isinstance(it, dict):
            return ""
        return str(it.get("position_id") or "")

    for it in sorted(items, key=_pid):
        if not isinstance(it, dict):
            continue
        position_id = str(it.get("position_id") or "unknown").strip()
        engine_id = str(it.get("engine_id") or "unknown").strip()
        st = str(it.get("status") or "unknown").strip()
        met = str(it.get("market_exposure_type") or "unknown").strip()
        ml = it.get("max_loss_cents")

        included = False
        if st == "OPEN":
            if isinstance(ml, int) and ml >= 0:
                included = True
                assert risk_sum is not None
                risk_sum += int(ml)
            else:
                all_have_max = False

        breakdown.append(
            {
                "position_id": position_id,
                "engine_id": engine_id,
                "market_exposure_type": met,
                "status": st,
                "max_loss_cents": (int(ml) if isinstance(ml, int) else None),
                "included_in_risk_sum": bool(included),
            }
        )

    checks["positions_all_have_max_loss"] = bool(all_have_max)
    if not all_have_max:
        reason_codes.append("B2_OPEN_POSITION_MISSING_MAX_LOSS_FAILCLOSED")
        notes.append("At least one OPEN position lacks max_loss_cents; cannot compute capital-at-risk -> FAIL-CLOSED")
        risk_sum = None

    allowed: Optional[int] = None
    headroom: Optional[int] = None

    if multiplier is not None and all_have_max and isinstance(risk_sum, int):
        allowed_dec = (Decimal(nav_total_cents) * BASE_ENVELOPE_PCT * multiplier)
        allowed = int(allowed_dec.to_integral_value(rounding="ROUND_FLOOR"))
        headroom = int(allowed - risk_sum)
        checks["portfolio_within_envelope"] = bool(risk_sum <= allowed)
        if risk_sum > allowed:
            reason_codes.append("B2_PORTFOLIO_CAPITAL_AT_RISK_EXCEEDS_ENVELOPE")

    status = "PASS" if not reason_codes else "FAIL"

    out = {
        "schema_id": "capital_risk_envelope",
        "schema_version": "v2",
        "day_utc": out_day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "checks": checks,
        "envelope": {
            "contracts": {
                "drawdown_contract": {"path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
                "capital_risk_envelope_contract": {"path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            },
            "drawdown_multiplier_table": _table(),
            "base_envelope_pct": f"{BASE_ENVELOPE_PCT:.6f}",
            "nav_total": int(nav_total),
            "nav_total_cents": int(nav_total_cents),
            "peak_nav": (int(peak_nav) if isinstance(peak_nav, int) else None),
            "drawdown_abs": (int(drawdown_abs) if isinstance(drawdown_abs, int) else None),
            "drawdown_pct": (f"{drawdown_pct_q:.6f}" if drawdown_pct_q is not None else None),
            "multiplier": (f"{multiplier:.2f}" if multiplier is not None else None),
            "allowed_capital_at_risk_cents": (int(allowed) if isinstance(allowed, int) else None),
            "portfolio_capital_at_risk_cents": (int(risk_sum) if isinstance(risk_sum, int) else None),
            "headroom_cents": (int(headroom) if isinstance(headroom, int) else None),
            "positions": breakdown,
        },
    }

    _validate_against_repo_schema(out, "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json")
    return out


def _minimal_missing_inputs_report(out_day: str, in_day: str, produced_utc: str, missing_err: str) -> Dict[str, Any]:
    out = {
        "schema_id": "capital_risk_envelope",
        "schema_version": "v2",
        "day_utc": out_day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py", "git_sha": _git_sha()},
        "status": "FAIL",
        "reason_codes": ["B2_INPUTS_MISSING_FAILCLOSED"],
        "notes": [f"inputs missing for input_day_utc={in_day}: {missing_err}"],
        "input_manifest": [
            {"type": "drawdown_contract", "path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
            {"type": "capital_risk_envelope_contract_v2", "path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            {"type": "output_schema", "path": str(SCHEMA_OUT), "sha256": _sha256_file(SCHEMA_OUT)},
        ],
        "checks": {
            "allocation_summary_present": False,
            "nav_present": False,
            "positions_present": False,
            "drawdown_present": False,
            "positions_all_have_max_loss": False,
            "portfolio_within_envelope": False,
        },
        "envelope": {
            "contracts": {
                "drawdown_contract": {"path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
                "capital_risk_envelope_contract": {"path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            },
            "drawdown_multiplier_table": _table(),
            "base_envelope_pct": f"{BASE_ENVELOPE_PCT:.6f}",
            "nav_total": 0,
            "nav_total_cents": 0,
            "peak_nav": None,
            "drawdown_abs": None,
            "drawdown_pct": None,
            "multiplier": None,
            "allowed_capital_at_risk_cents": None,
            "portfolio_capital_at_risk_cents": None,
            "headroom_cents": None,
            "positions": [],
        },
    }
    _validate_against_repo_schema(out, "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_capital_risk_envelope_gate_v2")
    ap.add_argument("--out_day_utc", required=True, help="YYYY-MM-DD (output day key for report path)")
    ap.add_argument("--input_day_utc", required=True, help="YYYY-MM-DD (input truth day key to read)")
    ap.add_argument("--produced_utc", required=True, help="UTC ISO-8601 Z timestamp (deterministic, operator/orchestrator provided)")
    ap.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT), help="Override truth root (tests only)")
    args = ap.parse_args()

    out_day = str(args.out_day_utc).strip()
    in_day = str(args.input_day_utc).strip()
    produced_utc = str(args.produced_utc).strip()
    truth_root = Path(str(args.truth_root)).resolve()

    out_dir = (truth_root / "reports" / "capital_risk_envelope_v2" / out_day).resolve()
    out_path = (out_dir / "capital_risk_envelope.v2.json").resolve()

    # immutable day-keyed output; allow skip-identical only
    if out_path.exists():
        existing_sha = _sha256_file(out_path)
        existing = _read_json(out_path)
        schema_id = str(existing.get("schema_id") or "").strip()
        day_utc = str(existing.get("day_utc") or "").strip()
        schema_version = str(existing.get("schema_version") or "").strip()
        status = str(existing.get("status") or "").strip().upper()
        if schema_id != "capital_risk_envelope":
            raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
        if schema_version != "v2":
            raise SystemExit(f"FAIL: EXISTING_REPORT_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}")
        if day_utc != out_day:
            raise SystemExit(f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={out_day!r} path={out_path}")
        if status not in ("PASS", "FAIL", "DEGRADED"):
            raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_INVALID: status={status!r} path={out_path}")

        print(f"CAPITAL_RISK_ENVELOPE_V2_WRITTEN day_utc={out_day} path={str(out_path)} sha256={existing_sha} action=EXISTS")
        if status != "PASS":
            print(f"FAIL: CAPITAL_RISK_ENVELOPE_GATE_V2 status={status} reason_codes={existing.get('reason_codes')}", file=sys.stderr)
            return 2
        print("OK: CAPITAL_RISK_ENVELOPE_GATE_V2 PASS")
        return 0

    inp: Optional[Inputs] = None
    missing_err: Optional[str] = None
    try:
        inp = _resolve_inputs(day=in_day, truth_root=truth_root)
    except Exception as e:  # noqa: BLE001
        missing_err = str(e)

    if inp is not None:
        out = _compute(out_day=out_day, produced_utc=produced_utc, inp=inp)
    else:
        out = _minimal_missing_inputs_report(out_day=out_day, in_day=in_day, produced_utc=produced_utc, missing_err=str(missing_err))

    sha = _write_immutable(out_path, _canonical_json_bytes(out))

    print(f"CAPITAL_RISK_ENVELOPE_V2_WRITTEN day_utc={out_day} path={str(out_path)} sha256={sha} action=WROTE")
    if out.get("status") != "PASS":
        print(f"FAIL: CAPITAL_RISK_ENVELOPE_GATE_V2 status={out.get('status')} reason_codes={out.get('reason_codes')}", file=sys.stderr)
        return 2
    print("OK: CAPITAL_RISK_ENVELOPE_GATE_V2 PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
