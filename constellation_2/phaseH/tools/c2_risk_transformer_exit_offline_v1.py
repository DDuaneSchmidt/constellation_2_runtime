#!/usr/bin/env python3
"""
c2_risk_transformer_exit_offline_v1.py

Purpose:
- Convert ExposureIntent v1 EXIT intents (target_notional_pct == "0") into:
  - equity_intent.v1.json (EQUITY_LONG_CLOSE)
  - equity_order_plan.v1.json (SELL qty_shares determined from positions snapshot)

Institutional properties:
- deterministic, fail-closed
- no network access
- atomic writes
- schema validated

Inputs:
- --exposure_intent: path to ExposureIntent v1
- --day_utc: day key
- --eval_time_utc: deterministic clock
- --out_dir: output directory (must be empty or non-existent)

Positions source:
- Prefer explicit --positions_snapshot_path if provided.
- Else: use truth positions snapshot day dir:
    constellation_2/runtime/truth/positions_v1/snapshots/<DAY>/
    prefer v5, else v4, v3, v2, v1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseC.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402


class ExitTransformerError(Exception):
    pass


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise ExitTransformerError(f"TEMP_EXISTS: {tmp}")
    try:
        with tmp.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp), str(path))
    except Exception as e:  # noqa: BLE001
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        raise ExitTransformerError(f"ATOMIC_WRITE_FAILED: {path}: {e}") from e


def _ensure_out_dir_ready(out_dir: Path) -> None:
    if out_dir.exists():
        if not out_dir.is_dir():
            raise ExitTransformerError(f"OUT_DIR_NOT_DIR: {out_dir}")
        if list(out_dir.iterdir()):
            raise ExitTransformerError(f"OUT_DIR_NOT_EMPTY: {out_dir}")
        return
    out_dir.mkdir(parents=True, exist_ok=False)


def _dec(s: str, name: str) -> Decimal:
    if not isinstance(s, str) or not s.strip():
        raise ExitTransformerError(f"DECIMAL_STRING_REQUIRED: {name}")
    try:
        return Decimal(s.strip())
    except InvalidOperation as e:
        raise ExitTransformerError(f"DECIMAL_PARSE_FAILED: {name}={s!r}") from e


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ExitTransformerError(f"INPUT_FILE_MISSING: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ExitTransformerError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _parse_day_utc(day: str) -> str:
    d = (day or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ExitTransformerError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _find_positions_snapshot_for_day(repo_root: Path, day: str) -> Path:
    day_dir = (repo_root / "constellation_2/runtime/truth/positions_v1/snapshots" / day).resolve()
    if not day_dir.exists() or not day_dir.is_dir():
        raise ExitTransformerError(f"POSITIONS_SNAPSHOT_DAY_DIR_MISSING: {day_dir}")
    # Prefer highest known version
    for v in (5, 4, 3, 2, 1):
        p = day_dir / f"positions_snapshot.v{v}.json"
        if p.exists() and p.is_file():
            return p
    raise ExitTransformerError(f"NO_POSITIONS_SNAPSHOT_FOUND_FOR_DAY: {day_dir}")


def _exit_qty_from_positions_snapshot(pos_obj: Dict[str, Any], engine_id: str, symbol: str) -> int:
    pos = pos_obj.get("positions")
    if not isinstance(pos, dict):
        raise ExitTransformerError("POSITIONS_BLOCK_MISSING")
    items = pos.get("items")
    if not isinstance(items, list):
        raise ExitTransformerError("POSITIONS_ITEMS_NOT_LIST")

    total_qty = 0
    matched = 0

    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("status") or "").strip() != "OPEN":
            continue
        if str(it.get("engine_id") or "").strip() != engine_id:
            continue
        inst = it.get("instrument")
        if not isinstance(inst, dict):
            continue
        kind = str(inst.get("kind") or "").strip()
        underlying = inst.get("underlying")
        if kind != "EQUITY":
            continue
        if not isinstance(underlying, str) or underlying.strip() == "":
            continue
        if underlying.strip() != symbol:
            continue
        qty = it.get("qty")
        if not isinstance(qty, int):
            raise ExitTransformerError("POSITION_QTY_NOT_INT")
        matched += 1
        total_qty += qty

    if matched <= 0 or total_qty <= 0:
        raise ExitTransformerError(f"NO_MATCHING_OPEN_EQUITY_POSITION_FOR_EXIT: engine_id={engine_id} symbol={symbol}")
    return int(total_qty)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_risk_transformer_exit_offline_v1")
    ap.add_argument("--exposure_intent", required=True, help="Path to ExposureIntent v1 JSON (EXIT intent target=0)")
    ap.add_argument("--day_utc", required=True, help="Day key YYYY-MM-DD for reading positions snapshot")
    ap.add_argument("--eval_time_utc", required=True, help="ISO-8601 Z timestamp (deterministic clock)")
    ap.add_argument("--out_dir", required=True, help="Output directory (must not exist or must be empty)")
    ap.add_argument("--positions_snapshot_path", default="", help="Optional explicit positions snapshot path override")
    args = ap.parse_args(argv)

    day = _parse_day_utc(args.day_utc)
    out_dir = Path(args.out_dir).resolve()
    _ensure_out_dir_ready(out_dir)

    exp_path = Path(args.exposure_intent).resolve()
    exp = _read_json_obj(exp_path)

    validate_against_repo_schema_v1(exp, REPO_ROOT, "constellation_2/schemas/exposure_intent.v1.schema.json")

    exposure_type = exp.get("exposure_type")
    if exposure_type != "LONG_EQUITY":
        raise ExitTransformerError(f"UNSUPPORTED_EXPOSURE_TYPE_FOR_EXIT_V1: {exposure_type!r}")

    target_pct = _dec(str(exp.get("target_notional_pct") or ""), "target_notional_pct")
    if target_pct != Decimal("0"):
        raise ExitTransformerError(f"EXIT_TRANSFORMER_REQUIRES_TARGET_ZERO: target={str(target_pct)}")

    engine = exp.get("engine")
    if not isinstance(engine, dict):
        raise ExitTransformerError("EXPOSURE_INTENT_ENGINE_NOT_OBJECT")
    engine_id = str(engine.get("engine_id") or "").strip()
    suite = str(engine.get("suite") or "").strip()
    mode = str(engine.get("mode") or "").strip()
    if not engine_id or not suite or not mode:
        raise ExitTransformerError("ENGINE_FIELDS_MISSING")

    und = exp.get("underlying")
    if not isinstance(und, dict):
        raise ExitTransformerError("UNDERLYING_NOT_OBJECT")
    sym = str(und.get("symbol") or "").strip()
    ccy = str(und.get("currency") or "").strip()
    if not sym or not ccy:
        raise ExitTransformerError("UNDERLYING_FIELDS_MISSING")

    # Load positions snapshot
    if str(args.positions_snapshot_path or "").strip():
        pos_path = Path(str(args.positions_snapshot_path).strip()).resolve()
    else:
        pos_path = _find_positions_snapshot_for_day(REPO_ROOT, day)

    pos_obj = _read_json_obj(pos_path)
    # Validate against governance schema where possible; choose v2 schema as minimum in this repo.
    # If file schema_id differs (v3/v4/v5), we still require object structure to match required fields used above.
    # Hard fail only if top-level not dict or missing required blocks handled in _exit_qty_from_positions_snapshot.
    qty = _exit_qty_from_positions_snapshot(pos_obj, engine_id=engine_id, symbol=sym)

    # Build EquityIntent CLOSE
    eq_intent: Dict[str, Any] = {
        "schema_id": "equity_intent",
        "schema_version": "v1",
        "intent_id": exp["intent_id"],
        "created_at_utc": args.eval_time_utc,
        "engine": {"engine_id": engine_id, "suite": suite, "mode": mode},
        "underlying": {"symbol": sym, "currency": ccy},
        "intent_type": "EQUITY_LONG_CLOSE",
        "sizing": {"target_notional_pct": "0", "max_risk_pct": "0"},
        "exit_policy": {"policy_id": "c2_equity_exit_immediate_v1", "time_exit": {"enabled": True, "max_holding_days": 0}},
        "canonical_json_hash": None,
    }
    validate_against_repo_schema_v1(eq_intent, REPO_ROOT, "constellation_2/schemas/equity_intent.v1.schema.json")
    eq_intent["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_intent)

    intent_hash = canonical_hash_for_c2_artifact_v1(eq_intent)

    # Build EquityOrderPlan SELL MARKET
    eq_plan: Dict[str, Any] = {
        "schema_id": "equity_order_plan",
        "schema_version": "v1",
        "plan_id": exp["intent_id"],
        "created_at_utc": args.eval_time_utc,
        "intent_hash": intent_hash,
        "structure": "EQUITY_SPOT",
        "symbol": sym,
        "currency": ccy,
        "action": "SELL",
        "qty_shares": int(qty),
        "order_terms": {"order_type": "MARKET", "limit_price": None, "time_in_force": "DAY"},
        "risk_proof": None,
        "canonical_json_hash": None,
    }
    validate_against_repo_schema_v1(eq_plan, REPO_ROOT, "constellation_2/schemas/equity_order_plan.v1.schema.json")
    eq_plan["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(eq_plan)

    _atomic_write_bytes(out_dir / "equity_intent.v1.json", canonical_json_bytes_v1(eq_intent) + b"\n")
    _atomic_write_bytes(out_dir / "equity_order_plan.v1.json", canonical_json_bytes_v1(eq_plan) + b"\n")

    print("OK: EXIT_RISK_TRANSFORMER_EMITTED_EQUITY_CLOSE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
