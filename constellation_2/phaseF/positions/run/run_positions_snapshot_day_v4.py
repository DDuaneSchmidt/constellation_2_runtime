from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import day_paths_v1 as exec_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_v4 import REPO_ROOT, day_paths_v4
from constellation_2.phaseF.positions.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1


SCHEMA_POSITIONS_SNAPSHOT_V4 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json"

ORDER_PLAN_SCHEMA = "constellation_2/schemas/order_plan.v1.schema.json"
EQUITY_ORDER_PLAN_SCHEMA = "constellation_2/schemas/equity_order_plan.v1.schema.json"
EXEC_EVENT_SCHEMA = "constellation_2/schemas/execution_event_record.v1.schema.json"


def _parse_price_to_cents(price_str: str) -> int:
    if not isinstance(price_str, str):
        raise ValueError("AVG_PRICE_NOT_STRING")
    s = price_str.strip()
    if not s:
        raise ValueError("AVG_PRICE_EMPTY")
    if s.count(".") > 1:
        raise ValueError("AVG_PRICE_INVALID_DECIMAL")
    if "." in s:
        whole, frac = s.split(".", 1)
    else:
        whole, frac = s, ""
    if not whole.isdigit():
        raise ValueError("AVG_PRICE_INVALID_WHOLE")
    if frac and not frac.isdigit():
        raise ValueError("AVG_PRICE_INVALID_FRAC")
    if len(frac) > 2:
        raise ValueError("AVG_PRICE_TOO_MANY_DECIMALS")
    frac2 = (frac + "00")[:2]
    return int(whole) * 100 + int(frac2)


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _producer_sha_lock_if_existing_snapshot(snapshot_path: Path, producer_sha: str) -> int:
    if snapshot_path.exists() and snapshot_path.is_file():
        try:
            ex = _read_json_obj(snapshot_path)
            ex_prod = ex.get("producer") if isinstance(ex, dict) else None
            ex_sha = ex_prod.get("git_sha") if isinstance(ex_prod, dict) else None
            if isinstance(ex_sha, str) and ex_sha.strip():
                if ex_sha.strip() != producer_sha:
                    print(
                        f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha.strip()} provided={producer_sha}",
                        file=sys.stderr,
                    )
                    return 4
        except Exception:
            print("FAIL: EXISTING_SNAPSHOT_UNREADABLE_FOR_SHA_LOCK", file=sys.stderr)
            return 4
    return 0


def _normalize_right_v1(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("RIGHT_EMPTY")
    u = raw.upper()
    if u in ("C", "CALL"):
        return "C"
    if u in ("P", "PUT"):
        return "P"
    raise ValueError(f"RIGHT_INVALID: {raw!r}")


def _instrument_from_order_plan_options_v1(op: Dict[str, Any]) -> Dict[str, Any]:
    u = op.get("underlying")
    underlying = ""
    if isinstance(u, str):
        underlying = u.strip()
    elif isinstance(u, dict):
        sym = u.get("symbol")
        if isinstance(sym, str):
            underlying = sym.strip()
    if not underlying:
        raise ValueError("ORDER_PLAN_UNDERLYING_MISSING")

    legs = op.get("legs")
    if not isinstance(legs, list) or not legs:
        raise ValueError("ORDER_PLAN_LEGS_MISSING_OR_EMPTY")

    out_legs: List[Dict[str, Any]] = []
    for i, leg in enumerate(legs):
        if not isinstance(leg, dict):
            raise ValueError(f"ORDER_PLAN_LEG_NOT_OBJECT: idx={i}")
        action = str(leg.get("action") or "").strip().upper()
        expiry_utc = str(leg.get("expiry_utc") or "").strip()
        strike = str(leg.get("strike") or "").strip()

        right_raw = str(leg.get("right") or "")
        right = _normalize_right_v1(right_raw)

        ratio = leg.get("ratio")
        ib_conid = leg.get("ib_conId")
        ib_lsym = str(leg.get("ib_localSymbol") or "").strip()

        if action not in ("BUY", "SELL"):
            raise ValueError(f"ORDER_PLAN_LEG_ACTION_INVALID: idx={i}")
        if not expiry_utc:
            raise ValueError(f"ORDER_PLAN_LEG_EXPIRY_MISSING: idx={i}")
        if not strike:
            raise ValueError(f"ORDER_PLAN_LEG_STRIKE_MISSING: idx={i}")
        if not isinstance(ratio, int) or ratio < 1:
            raise ValueError(f"ORDER_PLAN_LEG_RATIO_INVALID: idx={i}")
        if not isinstance(ib_conid, int):
            raise ValueError(f"ORDER_PLAN_LEG_IB_CONID_INVALID: idx={i}")
        if not ib_lsym:
            raise ValueError(f"ORDER_PLAN_LEG_IB_LOCALSYMBOL_MISSING: idx={i}")

        out_legs.append(
            {
                "action": action,
                "expiry_utc": expiry_utc,
                "strike": strike,
                "right": right,
                "ratio": ratio,
                "ib_conId": ib_conid,
                "ib_localSymbol": ib_lsym,
            }
        )

    kind = "OPTION_SINGLE" if len(out_legs) == 1 else "OPTION_MULTI"
    summary = {"expiry_utc": None, "strike": None, "right": None}
    if len(out_legs) == 1:
        summary = {
            "expiry_utc": out_legs[0]["expiry_utc"],
            "strike": out_legs[0]["strike"],
            "right": out_legs[0]["right"],
        }

    return {"kind": kind, "underlying": underlying, "legs": out_legs, "summary": summary}


def _instrument_from_equity_order_plan_v1(ep: Dict[str, Any]) -> Dict[str, Any]:
    sym = str(ep.get("symbol") or "").strip()
    ccy = str(ep.get("currency") or "").strip()
    if not sym:
        raise ValueError("EQUITY_PLAN_SYMBOL_MISSING")
    if not ccy:
        raise ValueError("EQUITY_PLAN_CURRENCY_MISSING")
    # No legs. Equity is its own primitive.
    return {"kind": "EQUITY", "symbol": sym, "currency": ccy, "ib_conId": None, "ib_localSymbol": None}


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_positions_snapshot_day_v4",
        description="C2 Positions Snapshot Truth Spine v4 (equity-native + option-native; no synthetic legs).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp_exec = exec_day_paths_v1(day_utc)
    dp_pos = day_paths_v4(day_utc)

    rc = _producer_sha_lock_if_existing_snapshot(dp_pos.snapshot_path, producer_sha)
    if rc != 0:
        return rc

    if not dp_exec.submissions_day_dir.exists():
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["EXECUTION_EVIDENCE_DAY_DIR_MISSING"],
            input_manifest=[
                {
                    "type": "execution_evidence",
                    "path": str(dp_exec.submissions_day_dir),
                    "sha256": "0" * 64,
                    "day_utc": day_utc,
                    "producer": "execution_evidence_v1",
                }
            ],
            code="FAIL_CORRUPT_INPUTS",
            message=f"Missing execution evidence day directory: {str(dp_exec.submissions_day_dir)}",
            details={"missing_path": str(dp_exec.submissions_day_dir)},
            attempted_outputs=[{"path": str(dp_pos.snapshot_path), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=dp_pos.failure_path, failure_obj=failure)
        print("FAIL: EXECUTION_EVIDENCE_DAY_DIR_MISSING (failure artifact written)")
        return 2

    items: List[Dict[str, Any]] = []
    reason_codes: List[str] = ["INSTRUMENT_IDENTITY_FROM_EXECUTION_EVIDENCE_V4"]
    status = "OK"

    sub_dirs = sorted([p for p in dp_exec.submissions_day_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
    for sd in sub_dirs:
        submission_id = sd.name.strip()
        p_exec = sd / "execution_event_record.v1.json"
        if not p_exec.exists():
            continue

        evt = _read_json_obj(p_exec)
        validate_against_repo_schema_v1(evt, REPO_ROOT, EXEC_EVENT_SCHEMA)

        # Prefer equity plan if present, else options plan.
        p_ep = sd / "equity_order_plan.v1.json"
        p_op = sd / "order_plan.v1.json"

        instr: Optional[Dict[str, Any]] = None
        if p_ep.exists():
            ep = _read_json_obj(p_ep)
            validate_against_repo_schema_v1(ep, REPO_ROOT, EQUITY_ORDER_PLAN_SCHEMA)
            instr = _instrument_from_equity_order_plan_v1(ep)
        elif p_op.exists():
            op = _read_json_obj(p_op)
            validate_against_repo_schema_v1(op, REPO_ROOT, ORDER_PLAN_SCHEMA)
            instr = _instrument_from_order_plan_options_v1(op)
        else:
            raise ValueError(f"NO_PLAN_FOUND_FOR_SUBMISSION: {submission_id}")

        qty = int(evt["filled_qty"])
        avg_cents = _parse_price_to_cents(str(evt["avg_price"]))

        pos_id = str(evt.get("binding_hash") or submission_id).strip()
        if not pos_id:
            raise ValueError(f"POSITION_ID_MISSING: {submission_id}")

        items.append(
            {
                "position_id": pos_id,
                "engine_id": "unknown",
                "instrument": instr,
                "qty": qty,
                "avg_cost_cents": avg_cents,
                "market_exposure_type": "UNDEFINED_RISK",
                "max_loss_cents": None,
                "opened_day_utc": day_utc,
                "status": "OPEN",
            }
        )

    snapshot_obj: Dict[str, Any] = {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V4",
        "schema_version": 4,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_sha,
            "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py",
        },
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": [
            {
                "type": "execution_evidence",
                "path": str(dp_exec.submissions_day_dir),
                "sha256": "0" * 64,
                "day_utc": day_utc,
                "producer": "execution_evidence_v1",
            }
        ],
        "positions": {
            "currency": "USD",
            "asof_utc": f"{day_utc}T00:00:00Z",
            "items": items,
            "notes": ["instrument identity derived deterministically from execution evidence; equity-native + option-native"],
        },
    }

    validate_against_repo_schema_v1(snapshot_obj, REPO_ROOT, SCHEMA_POSITIONS_SNAPSHOT_V4)
    try:
        snap_bytes = canonical_json_bytes_v1(snapshot_obj) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: SNAPSHOT_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=dp_pos.snapshot_path, data=snap_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    print("OK: POSITIONS_SNAPSHOT_V4_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
